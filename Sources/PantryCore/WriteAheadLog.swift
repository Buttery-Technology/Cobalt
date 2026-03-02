import Foundation
import Compression

/// Configuration for group commit batching
public struct GroupCommitConfig: Sendable {
    public let maxDelayMicroseconds: Int
    public let maxBatchSize: Int

    public init(maxDelayMicroseconds: Int = 1000, maxBatchSize: Int = 32) {
        self.maxDelayMicroseconds = maxDelayMicroseconds
        self.maxBatchSize = maxBatchSize
    }
}

/// Write-Ahead Log for transaction durability and crash recovery
public actor WriteAheadLog: Sendable {
    private let logFilePath: String
    private var logFileHandle: FileHandle?
    private var currentLogPosition: UInt64 = 0
    private var nextLSN: UInt64 = 1
    private var logCache: [UInt64: LogRecord] = [:]
    private var logCacheFIFO: [UInt64] = []  // insertion-order LSN list for O(1) eviction
    private let logCacheLimit = 1000
    private let storageManager: StorageManager
    private let groupCommitConfig: GroupCommitConfig
    private let encryptionProvider: EncryptionProvider?

    /// Pending commit continuations waiting for the next fsync batch
    private var pendingCommits: [(txID: UInt64, continuation: CheckedContinuation<Void, Error>)] = []

    /// Adaptive batch sizing: track commits per flush cycle to scale batch size
    private var adaptiveBatchSize: Int = 4
    private var recentBatchSizes: [Int] = []  // rolling window of last 8 batch sizes

    // MARK: - Types

    public enum LogRecordType: UInt8, Sendable {
        case transactionBegin = 1
        case transactionCommit = 2
        case transactionRollback = 3
        case pageBeforeUpdate = 4
        case pageAfterUpdate = 5
        case checkpoint = 6
        case pageBeforeDelta = 7
        case pageAfterDelta = 8
        case pageBeforeUpdateCompressed = 9
        case pageAfterUpdateCompressed = 10
        case recordInsert = 11  // record-level redo: insert
        case recordDelete = 12  // record-level redo: delete
    }

    public enum LogContent: Sendable {
        case transaction(IsolationLevel)
        case pageImage(Int, UInt64, Data) // pageID, timestamp, data
        case pageDelta(Int, UInt64, Data) // pageID, timestamp, delta data
        case checkpoint([UInt64]) // active transaction IDs
        case recordOp(pageID: Int, recordID: UInt64, data: Data?) // record-level: data is nil for delete
    }

    public struct LogRecord: Sendable {
        public let lsn: UInt64
        public let type: LogRecordType
        public let transactionID: UInt64
        public let timestamp: UInt64
        public let content: LogContent
    }

    // MARK: - Initialization

    public init(databasePath: String, storageManager: StorageManager, groupCommitConfig: GroupCommitConfig = GroupCommitConfig(), encryptionProvider: EncryptionProvider? = nil) async throws {
        self.logFilePath = databasePath + ".wal"
        self.storageManager = storageManager
        self.groupCommitConfig = groupCommitConfig
        self.encryptionProvider = encryptionProvider

        if !FileManager.default.fileExists(atPath: logFilePath) {
            FileManager.default.createFile(atPath: logFilePath, contents: nil)
        }

        logFileHandle = try FileHandle(forUpdating: URL(fileURLWithPath: logFilePath))
        currentLogPosition = try logFileHandle?.seekToEnd() ?? 0

        if currentLogPosition == 0 {
            try writeLogHeader()
        } else {
            // Recover nextLSN from existing WAL to prevent LSN collisions after restart
            try recoverNextLSN()
        }
    }

    // MARK: - Log Header

    private func writeLogHeader() throws {
        var headerData = Data()
        let magic = "SWIFTDB-WAL"
        headerData.append(magic.data(using: .utf8)!)
        let version: UInt32 = 1
        withUnsafeBytes(of: version) { headerData.append(contentsOf: $0) }
        let timestamp = UInt64(Date().timeIntervalSince1970)
        withUnsafeBytes(of: timestamp) { headerData.append(contentsOf: $0) }
        let padding = [UInt8](repeating: 0, count: 64 - headerData.count)
        headerData.append(contentsOf: padding)

        try logFileHandle?.write(contentsOf: headerData)
        currentLogPosition += UInt64(headerData.count)
    }

    // MARK: - Transaction Logging

    public func logTransactionBegin(txID: UInt64, isolationLevel: IsolationLevel) throws {
        let lsn = nextLSN
        nextLSN += 1

        var logData = Data()
        logData.append(LogRecordType.transactionBegin.rawValue)
        withUnsafeBytes(of: lsn) { logData.append(contentsOf: $0) }
        withUnsafeBytes(of: txID) { logData.append(contentsOf: $0) }
        let timestamp = UInt64(Date().timeIntervalSince1970)
        withUnsafeBytes(of: timestamp) { logData.append(contentsOf: $0) }
        let isolationValue = UInt8(isolationLevel.rawValue)
        logData.append(isolationValue)

        cacheLogRecord(lsn: lsn, type: .transactionBegin, txID: txID, timestamp: timestamp, content: .transaction(isolationLevel))

        try writeLogRecord(logData)
    }

    public func logTransactionCommit(txID: UInt64, isolationLevel: IsolationLevel = .readCommitted) async throws {
        let lsn = nextLSN
        nextLSN += 1

        var logData = Data()
        logData.append(LogRecordType.transactionCommit.rawValue)
        withUnsafeBytes(of: lsn) { logData.append(contentsOf: $0) }
        withUnsafeBytes(of: txID) { logData.append(contentsOf: $0) }
        let timestamp = UInt64(Date().timeIntervalSince1970)
        withUnsafeBytes(of: timestamp) { logData.append(contentsOf: $0) }

        cacheLogRecord(lsn: lsn, type: .transactionCommit, txID: txID, timestamp: timestamp, content: .transaction(isolationLevel))

        try writeLogRecord(logData)

        // Join the pending batch for fsync (adaptive batch size)
        try await withCheckedThrowingContinuation { (cont: CheckedContinuation<Void, Error>) in
            pendingCommits.append((txID: txID, continuation: cont))
            let effectiveBatchSize = min(adaptiveBatchSize, groupCommitConfig.maxBatchSize)
            if pendingCommits.count >= effectiveBatchSize {
                flushBatch()
            } else if pendingCommits.count == 1 {
                // First in batch — schedule delayed flush
                Task { [weak self] in
                    try? await Task.sleep(for: .microseconds(self?.groupCommitConfig.maxDelayMicroseconds ?? 1000))
                    await self?.flushBatchIfNeeded()
                }
            }
        }
    }

    private func flushBatch() {
        guard !pendingCommits.isEmpty else { return }
        let batch = pendingCommits
        pendingCommits = []

        // Adapt batch size based on recent history: grow toward larger batches under load
        recentBatchSizes.append(batch.count)
        if recentBatchSizes.count > 8 { recentBatchSizes.removeFirst() }
        let avgBatch = recentBatchSizes.reduce(0, +) / max(1, recentBatchSizes.count)
        // Scale: if recent batches are large, grow adaptive size; if small, shrink
        adaptiveBatchSize = max(4, min(groupCommitConfig.maxBatchSize, avgBatch * 2))

        do {
            try logFileHandle?.synchronize()
            for item in batch { item.continuation.resume() }
        } catch {
            for item in batch { item.continuation.resume(throwing: error) }
        }
    }

    private func flushBatchIfNeeded() {
        if !pendingCommits.isEmpty {
            flushBatch()
        }
    }

    public func logTransactionRollback(txID: UInt64, isolationLevel: IsolationLevel = .readCommitted) throws {
        let lsn = nextLSN
        nextLSN += 1

        var logData = Data()
        logData.append(LogRecordType.transactionRollback.rawValue)
        withUnsafeBytes(of: lsn) { logData.append(contentsOf: $0) }
        withUnsafeBytes(of: txID) { logData.append(contentsOf: $0) }
        let timestamp = UInt64(Date().timeIntervalSince1970)
        withUnsafeBytes(of: timestamp) { logData.append(contentsOf: $0) }

        cacheLogRecord(lsn: lsn, type: .transactionRollback, txID: txID, timestamp: timestamp, content: .transaction(isolationLevel))

        try writeLogRecord(logData)
    }

    // MARK: - Page Image Logging

    public func logPageBeforeImage(txID: UInt64, page: DatabasePage) throws {
        let lsn = nextLSN
        nextLSN += 1
        let timestamp = UInt64(Date().timeIntervalSince1970)

        var logData = Data()

        // Try LZ4 compression on page data
        if let compressed = compressLZ4(page.data) {
            logData.append(LogRecordType.pageBeforeUpdateCompressed.rawValue)
            withUnsafeBytes(of: lsn) { logData.append(contentsOf: $0) }
            withUnsafeBytes(of: txID) { logData.append(contentsOf: $0) }
            withUnsafeBytes(of: timestamp) { logData.append(contentsOf: $0) }
            withUnsafeBytes(of: page.pageID) { logData.append(contentsOf: $0) }
            let originalSize = UInt32(page.data.count)
            withUnsafeBytes(of: originalSize) { logData.append(contentsOf: $0) }
            let compressedLength = UInt32(compressed.count)
            withUnsafeBytes(of: compressedLength) { logData.append(contentsOf: $0) }
            logData.append(compressed)
        } else {
            logData.append(LogRecordType.pageBeforeUpdate.rawValue)
            withUnsafeBytes(of: lsn) { logData.append(contentsOf: $0) }
            withUnsafeBytes(of: txID) { logData.append(contentsOf: $0) }
            withUnsafeBytes(of: timestamp) { logData.append(contentsOf: $0) }
            withUnsafeBytes(of: page.pageID) { logData.append(contentsOf: $0) }
            let dataLength = UInt32(page.data.count)
            withUnsafeBytes(of: dataLength) { logData.append(contentsOf: $0) }
            logData.append(page.data)
        }

        cacheLogRecord(lsn: lsn, type: .pageBeforeUpdate, txID: txID, timestamp: timestamp, content: .pageImage(page.pageID, timestamp, page.data))

        try writeLogRecord(logData)
    }

    public func logPageAfterImage(txID: UInt64, page: DatabasePage) throws {
        let lsn = nextLSN
        nextLSN += 1
        let timestamp = UInt64(Date().timeIntervalSince1970)

        var logData = Data()

        // Try LZ4 compression on page data
        if let compressed = compressLZ4(page.data) {
            logData.append(LogRecordType.pageAfterUpdateCompressed.rawValue)
            withUnsafeBytes(of: lsn) { logData.append(contentsOf: $0) }
            withUnsafeBytes(of: txID) { logData.append(contentsOf: $0) }
            withUnsafeBytes(of: timestamp) { logData.append(contentsOf: $0) }
            withUnsafeBytes(of: page.pageID) { logData.append(contentsOf: $0) }
            let originalSize = UInt32(page.data.count)
            withUnsafeBytes(of: originalSize) { logData.append(contentsOf: $0) }
            let compressedLength = UInt32(compressed.count)
            withUnsafeBytes(of: compressedLength) { logData.append(contentsOf: $0) }
            logData.append(compressed)
        } else {
            logData.append(LogRecordType.pageAfterUpdate.rawValue)
            withUnsafeBytes(of: lsn) { logData.append(contentsOf: $0) }
            withUnsafeBytes(of: txID) { logData.append(contentsOf: $0) }
            withUnsafeBytes(of: timestamp) { logData.append(contentsOf: $0) }
            withUnsafeBytes(of: page.pageID) { logData.append(contentsOf: $0) }
            let dataLength = UInt32(page.data.count)
            withUnsafeBytes(of: dataLength) { logData.append(contentsOf: $0) }
            logData.append(page.data)
        }

        cacheLogRecord(lsn: lsn, type: .pageAfterUpdate, txID: txID, timestamp: timestamp, content: .pageImage(page.pageID, timestamp, page.data))

        try writeLogRecord(logData)
    }

    // MARK: - Delta Page Logging

    /// Compute a binary delta between two page data buffers and log it.
    /// Delta format: [2B range count][per range: [2B offset][2B length][N bytes data]]
    /// Returns true if delta was used, false if full image was logged instead.
    @discardableResult
    public func logPageDelta(txID: UInt64, pageID: Int, oldData: Data, newData: Data, type: LogRecordType) throws -> Bool {
        let delta = Self.computeDelta(oldData: oldData, newData: newData)

        // Threshold: use delta only when it's smaller than 4KB (half a page)
        if delta.count >= 4096 {
            let fullType: LogRecordType = (type == .pageBeforeDelta) ? .pageBeforeUpdate : .pageAfterUpdate
            let lsn = nextLSN
            nextLSN += 1
            let timestamp = UInt64(Date().timeIntervalSince1970)

            var logData = Data()
            logData.append(fullType.rawValue)
            withUnsafeBytes(of: lsn) { logData.append(contentsOf: $0) }
            withUnsafeBytes(of: txID) { logData.append(contentsOf: $0) }
            withUnsafeBytes(of: timestamp) { logData.append(contentsOf: $0) }
            withUnsafeBytes(of: pageID) { logData.append(contentsOf: $0) }
            let imageData = (type == .pageBeforeDelta) ? oldData : newData
            let dataLength = UInt32(imageData.count)
            withUnsafeBytes(of: dataLength) { logData.append(contentsOf: $0) }
            logData.append(imageData)

            cacheLogRecord(lsn: lsn, type: fullType, txID: txID, timestamp: timestamp, content: .pageImage(pageID, timestamp, imageData))
            try writeLogRecord(logData)
            return false
        }

        let lsn = nextLSN
        nextLSN += 1
        let timestamp = UInt64(Date().timeIntervalSince1970)

        var logData = Data()
        logData.append(type.rawValue)
        withUnsafeBytes(of: lsn) { logData.append(contentsOf: $0) }
        withUnsafeBytes(of: txID) { logData.append(contentsOf: $0) }
        withUnsafeBytes(of: timestamp) { logData.append(contentsOf: $0) }
        withUnsafeBytes(of: pageID) { logData.append(contentsOf: $0) }
        let deltaLength = UInt32(delta.count)
        withUnsafeBytes(of: deltaLength) { logData.append(contentsOf: $0) }
        logData.append(delta)

        cacheLogRecord(lsn: lsn, type: type, txID: txID, timestamp: timestamp, content: .pageDelta(pageID, timestamp, delta))
        try writeLogRecord(logData)
        return true
    }

    // MARK: - Record-Level WAL Entries

    /// Log a record insert operation. Much smaller than a full page image for single-row inserts.
    /// Format: [1B type][8B lsn][8B txID][8B timestamp][8B pageID][8B recordID][4B dataLength][N bytes data]
    public func logRecordInsert(txID: UInt64, pageID: Int, recordID: UInt64, data: Data) throws {
        let lsn = nextLSN
        nextLSN += 1
        let timestamp = UInt64(Date().timeIntervalSince1970)

        var logData = Data(capacity: 1 + 8 + 8 + 8 + 8 + 8 + 4 + data.count)
        logData.append(LogRecordType.recordInsert.rawValue)
        withUnsafeBytes(of: lsn) { logData.append(contentsOf: $0) }
        withUnsafeBytes(of: txID) { logData.append(contentsOf: $0) }
        withUnsafeBytes(of: timestamp) { logData.append(contentsOf: $0) }
        withUnsafeBytes(of: pageID) { logData.append(contentsOf: $0) }
        withUnsafeBytes(of: recordID) { logData.append(contentsOf: $0) }
        let dataLength = UInt32(data.count)
        withUnsafeBytes(of: dataLength) { logData.append(contentsOf: $0) }
        logData.append(data)

        cacheLogRecord(lsn: lsn, type: .recordInsert, txID: txID, timestamp: timestamp, content: .recordOp(pageID: pageID, recordID: recordID, data: data))
        try writeLogRecord(logData)
    }

    /// Log a record delete operation. Stores the record data for undo capability.
    /// Format: [1B type][8B lsn][8B txID][8B timestamp][8B pageID][8B recordID][4B dataLength][N bytes data]
    public func logRecordDelete(txID: UInt64, pageID: Int, recordID: UInt64, data: Data) throws {
        let lsn = nextLSN
        nextLSN += 1
        let timestamp = UInt64(Date().timeIntervalSince1970)

        var logData = Data(capacity: 1 + 8 + 8 + 8 + 8 + 8 + 4 + data.count)
        logData.append(LogRecordType.recordDelete.rawValue)
        withUnsafeBytes(of: lsn) { logData.append(contentsOf: $0) }
        withUnsafeBytes(of: txID) { logData.append(contentsOf: $0) }
        withUnsafeBytes(of: timestamp) { logData.append(contentsOf: $0) }
        withUnsafeBytes(of: pageID) { logData.append(contentsOf: $0) }
        withUnsafeBytes(of: recordID) { logData.append(contentsOf: $0) }
        let dataLength = UInt32(data.count)
        withUnsafeBytes(of: dataLength) { logData.append(contentsOf: $0) }
        logData.append(data)

        cacheLogRecord(lsn: lsn, type: .recordDelete, txID: txID, timestamp: timestamp, content: .recordOp(pageID: pageID, recordID: recordID, data: data))
        try writeLogRecord(logData)
    }

    /// Compute binary delta between old and new data.
    /// Format: [2B range count][per range: [2B offset][2B length][N bytes changed data]]
    public static func computeDelta(oldData: Data, newData: Data) -> Data {
        let len = min(oldData.count, newData.count)
        var ranges: [(offset: UInt16, data: Data)] = []
        var i = 0

        while i < len {
            if oldData[oldData.startIndex + i] == newData[newData.startIndex + i] {
                i += 1
                continue
            }
            let start = i
            while i < len && oldData[oldData.startIndex + i] != newData[newData.startIndex + i] {
                i += 1
            }
            let rangeData = newData.subdata(in: (newData.startIndex + start)..<(newData.startIndex + i))
            ranges.append((offset: UInt16(start), data: rangeData))
        }

        var delta = Data()
        var rangeCount = UInt16(ranges.count)
        withUnsafeBytes(of: &rangeCount) { delta.append(contentsOf: $0) }
        for range in ranges {
            var off = range.offset.littleEndian
            withUnsafeBytes(of: &off) { delta.append(contentsOf: $0) }
            var length = UInt16(range.data.count).littleEndian
            withUnsafeBytes(of: &length) { delta.append(contentsOf: $0) }
            delta.append(range.data)
        }

        return delta
    }

    /// Apply a delta to page data to produce the modified version.
    public static func applyDelta(to baseData: Data, delta: Data) -> Data? {
        guard delta.count >= 2 else { return nil }
        var result = baseData
        var pos = 0

        let rangeCount = delta.withUnsafeBytes { UInt16(littleEndian: $0.loadUnaligned(fromByteOffset: pos, as: UInt16.self)) }
        pos += 2

        for _ in 0..<rangeCount {
            guard pos + 4 <= delta.count else { return nil }
            let offset = delta.withUnsafeBytes { Int(UInt16(littleEndian: $0.loadUnaligned(fromByteOffset: pos, as: UInt16.self))) }
            pos += 2
            let length = delta.withUnsafeBytes { Int(UInt16(littleEndian: $0.loadUnaligned(fromByteOffset: pos, as: UInt16.self))) }
            pos += 2
            guard pos + length <= delta.count else { return nil }
            guard offset + length <= result.count else { return nil }
            let rangeData = delta.subdata(in: pos..<(pos + length))
            result.replaceSubrange(offset..<(offset + length), with: rangeData)
            pos += length
        }

        return result
    }

    // MARK: - Recovery

    public func recoverPage(pageID: Int) throws -> DatabasePage {
        let logRecords = try getAllLogRecords()

        let pageImages = logRecords.filter { record in
            if case let .pageImage(recordPageID, _, _) = record.content, recordPageID == pageID {
                return true
            }
            return false
        }.sorted { $0.lsn > $1.lsn }

        if let latestImage = pageImages.first {
            if case let .pageImage(_, _, pageData) = latestImage.content {
                var page = DatabasePage(pageID: pageID, data: pageData)
                page.loadRecords()
                return page
            }
        }

        throw PantryError.pageNotInLog
    }

    /// Undo a transaction by restoring before-images (or applying before-deltas) via StorageManager
    public func undoTransaction(txID: UInt64) async throws {
        let logRecords = try getAllLogRecords()

        let txRecords = logRecords.filter { $0.transactionID == txID }
            .sorted { $0.lsn > $1.lsn }

        // Collect pages that have full before-image entries — record-level undo is
        // redundant for these pages since the before-image will restore them completely.
        var pagesWithBeforeImage = Set<Int>()
        for record in txRecords {
            switch record.type {
            case .pageBeforeUpdate:
                if case let .pageImage(pageID, _, _) = record.content {
                    pagesWithBeforeImage.insert(pageID)
                }
            case .pageBeforeDelta:
                if case let .pageDelta(pageID, _, _) = record.content {
                    pagesWithBeforeImage.insert(pageID)
                }
            default:
                break
            }
        }

        for record in txRecords {
            switch record.type {
            case .pageBeforeUpdate:
                if case let .pageImage(pageID, _, pageData) = record.content {
                    var restoredPage = DatabasePage(pageID: pageID, data: pageData)
                    restoredPage.loadRecords()
                    try await storageManager.writePage(&restoredPage)
                }
            case .pageBeforeDelta:
                if case let .pageDelta(pageID, _, delta) = record.content {
                    // Read current on-disk page and apply the before-delta to reconstruct pre-modification state
                    let currentPage = try await storageManager.readPage(pageID: pageID)
                    if let restoredData = Self.applyDelta(to: currentPage.data, delta: delta) {
                        var restoredPage = DatabasePage(pageID: pageID, data: restoredData)
                        restoredPage.loadRecords()
                        try await storageManager.writePage(&restoredPage)
                    }
                }
            case .recordInsert:
                // Undo insert: delete the record from the page (skip if before-image covers it)
                if case let .recordOp(pageID, recordID, _) = record.content,
                   !pagesWithBeforeImage.contains(pageID) {
                    var page = try await storageManager.readPage(pageID: pageID)
                    page.loadRecords()
                    _ = page.deleteRecord(id: recordID)
                    try page.saveRecords()
                    try await storageManager.writePage(&page)
                }
            case .recordDelete:
                // Undo delete: re-insert the record into the page (skip if before-image covers it)
                if case let .recordOp(pageID, recordID, data) = record.content, let data = data,
                   !pagesWithBeforeImage.contains(pageID) {
                    var page = try await storageManager.readPage(pageID: pageID)
                    page.loadRecords()
                    let restoredRecord = Record(id: recordID, data: data)
                    _ = page.addRecord(restoredRecord)
                    try page.saveRecords()
                    try await storageManager.writePage(&page)
                }
            default:
                break
            }
        }
    }

    // MARK: - Checkpointing

    /// LSN at or before which all records have been checkpointed (skip during recovery)
    private var checkpointLSN: UInt64 = 0

    /// Set the checkpoint LSN (called during init from persisted metadata)
    public func setCheckpointLSN(_ lsn: UInt64) {
        checkpointLSN = lsn
    }

    /// Get the current checkpoint LSN
    public func getCheckpointLSN() -> UInt64 {
        checkpointLSN
    }

    @discardableResult
    public func createCheckpoint(activeTransactionCount: UInt32) throws -> UInt64 {
        let lsn = nextLSN
        nextLSN += 1

        var logData = Data()
        logData.append(LogRecordType.checkpoint.rawValue)
        withUnsafeBytes(of: lsn) { logData.append(contentsOf: $0) }
        let timestamp = UInt64(Date().timeIntervalSince1970)
        withUnsafeBytes(of: timestamp) { logData.append(contentsOf: $0) }
        withUnsafeBytes(of: activeTransactionCount) { logData.append(contentsOf: $0) }

        try writeLogRecord(logData)
        try logFileHandle?.synchronize()
        checkpointLSN = lsn
        return lsn
    }

    // MARK: - Log Record I/O

    private func writeLogRecord(_ data: Data) throws {
        guard let fh = logFileHandle else {
            throw PantryError.walWriteError(description: "WAL file handle is closed")
        }

        // Encrypt the record payload if encryption is configured
        let payload: Data
        if let provider = encryptionProvider {
            payload = try provider.encrypt(data)
        } else {
            payload = data
        }

        let crc = CRC32.checksum(payload)

        // Single write: combine header (8B) + payload into one buffer to reduce syscalls
        let totalLength = UInt32(payload.count + 8)
        var buf = Data(capacity: 8 + payload.count)
        withUnsafeBytes(of: totalLength) { buf.append(contentsOf: $0) }
        withUnsafeBytes(of: crc) { buf.append(contentsOf: $0) }
        buf.append(payload)

        try fh.seek(toOffset: currentLogPosition)
        try fh.write(contentsOf: buf)

        currentLogPosition += UInt64(buf.count)
    }

    /// Parse the actual log file to retrieve all log records
    private func getAllLogRecords() throws -> [LogRecord] {
        guard let handle = logFileHandle else {
            throw PantryError.logReadError
        }

        try handle.seek(toOffset: 0)
        guard let allData = try handle.availableData() else {
            return Array(logCache.values)
        }

        if allData.count <= 64 {
            return Array(logCache.values)
        }

        var records: [LogRecord] = []
        var pos = 64 // skip header

        while pos + 8 <= allData.count {
            let totalLength = allData.withUnsafeBytes { $0.loadUnaligned(fromByteOffset: pos, as: UInt32.self) }
            let storedCRC = allData.withUnsafeBytes { $0.loadUnaligned(fromByteOffset: pos + 4, as: UInt32.self) }
            pos += 8

            let dataLength = Int(totalLength) - 8
            guard dataLength > 0, pos + dataLength <= allData.count else { break }

            let payload = allData.subdata(in: pos..<(pos + dataLength))
            let computedCRC = CRC32.checksum(payload)
            guard computedCRC == storedCRC else {
                pos += dataLength
                continue
            }

            // Decrypt if encryption is configured
            let recordData: Data
            if let provider = encryptionProvider {
                do { recordData = try provider.decrypt(payload) } catch { pos += dataLength; continue }
            } else {
                recordData = payload
            }

            if let record = parseLogRecord(recordData) {
                records.append(record)
            }
            pos += dataLength
        }

        // Merge with cache for any records not yet flushed
        let parsedLSNs = Set(records.map { $0.lsn })
        for (_, cachedRecord) in logCache where !parsedLSNs.contains(cachedRecord.lsn) {
            records.append(cachedRecord)
        }

        // Filter out records at or before the checkpoint LSN (already checkpointed)
        if checkpointLSN > 0 {
            records = records.filter { $0.lsn > checkpointLSN }
        }

        return records
    }

    private func parseLogRecord(_ data: Data) -> LogRecord? {
        guard !data.isEmpty else { return nil }

        let typeRaw = data[data.startIndex]
        guard let type = LogRecordType(rawValue: typeRaw) else { return nil }

        var pos = 1
        guard pos + 8 <= data.count else { return nil }
        let lsn = data.withUnsafeBytes { $0.loadUnaligned(fromByteOffset: pos, as: UInt64.self) }
        pos += 8

        switch type {
        case .transactionBegin:
            guard pos + 8 + 8 + 1 <= data.count else { return nil }
            let txID = data.withUnsafeBytes { $0.loadUnaligned(fromByteOffset: pos, as: UInt64.self) }
            pos += 8
            let timestamp = data.withUnsafeBytes { $0.loadUnaligned(fromByteOffset: pos, as: UInt64.self) }
            pos += 8
            let isoRaw = Int(data[data.startIndex.advanced(by: pos)])
            let level = IsolationLevel(rawValue: isoRaw) ?? .readCommitted
            return LogRecord(lsn: lsn, type: type, transactionID: txID, timestamp: timestamp, content: .transaction(level))

        case .transactionCommit, .transactionRollback:
            guard pos + 8 + 8 <= data.count else { return nil }
            let txID = data.withUnsafeBytes { $0.loadUnaligned(fromByteOffset: pos, as: UInt64.self) }
            pos += 8
            let timestamp = data.withUnsafeBytes { $0.loadUnaligned(fromByteOffset: pos, as: UInt64.self) }
            return LogRecord(lsn: lsn, type: type, transactionID: txID, timestamp: timestamp, content: .transaction(.readCommitted))

        case .pageBeforeUpdate, .pageAfterUpdate:
            guard pos + 8 + 8 + 8 + 4 <= data.count else { return nil }
            let txID = data.withUnsafeBytes { $0.loadUnaligned(fromByteOffset: pos, as: UInt64.self) }
            pos += 8
            let timestamp = data.withUnsafeBytes { $0.loadUnaligned(fromByteOffset: pos, as: UInt64.self) }
            pos += 8
            let pageID = data.withUnsafeBytes { $0.loadUnaligned(fromByteOffset: pos, as: Int.self) }
            pos += 8
            let dataLength = data.withUnsafeBytes { Int($0.loadUnaligned(fromByteOffset: pos, as: UInt32.self)) }
            pos += 4
            guard pos + dataLength <= data.count else { return nil }
            let pageData = data.subdata(in: pos..<(pos + dataLength))
            return LogRecord(lsn: lsn, type: type, transactionID: txID, timestamp: timestamp, content: .pageImage(pageID, timestamp, pageData))

        case .pageBeforeUpdateCompressed, .pageAfterUpdateCompressed:
            // Compressed format: [8B txID][8B timestamp][8B pageID][4B originalSize][4B compressedLength][N bytes compressed data]
            guard pos + 8 + 8 + 8 + 4 + 4 <= data.count else { return nil }
            let txID = data.withUnsafeBytes { $0.loadUnaligned(fromByteOffset: pos, as: UInt64.self) }
            pos += 8
            let timestamp = data.withUnsafeBytes { $0.loadUnaligned(fromByteOffset: pos, as: UInt64.self) }
            pos += 8
            let pageID = data.withUnsafeBytes { $0.loadUnaligned(fromByteOffset: pos, as: Int.self) }
            pos += 8
            let originalSize = data.withUnsafeBytes { Int($0.loadUnaligned(fromByteOffset: pos, as: UInt32.self)) }
            pos += 4
            let compressedLength = data.withUnsafeBytes { Int($0.loadUnaligned(fromByteOffset: pos, as: UInt32.self)) }
            pos += 4
            guard pos + compressedLength <= data.count else { return nil }
            let compressedData = data.subdata(in: pos..<(pos + compressedLength))
            guard let pageData = decompressLZ4(compressedData, originalSize: originalSize) else { return nil }
            // Map compressed types back to uncompressed for downstream consumers
            let mappedType: LogRecordType = (type == .pageBeforeUpdateCompressed) ? .pageBeforeUpdate : .pageAfterUpdate
            return LogRecord(lsn: lsn, type: mappedType, transactionID: txID, timestamp: timestamp, content: .pageImage(pageID, timestamp, pageData))

        case .checkpoint:
            guard pos + 8 <= data.count else { return nil }
            let timestamp = data.withUnsafeBytes { $0.loadUnaligned(fromByteOffset: pos, as: UInt64.self) }
            return LogRecord(lsn: lsn, type: type, transactionID: 0, timestamp: timestamp, content: .checkpoint([]))

        case .pageBeforeDelta, .pageAfterDelta:
            guard pos + 8 + 8 + 8 + 4 <= data.count else { return nil }
            let txID = data.withUnsafeBytes { $0.loadUnaligned(fromByteOffset: pos, as: UInt64.self) }
            pos += 8
            let timestamp = data.withUnsafeBytes { $0.loadUnaligned(fromByteOffset: pos, as: UInt64.self) }
            pos += 8
            let pageID = data.withUnsafeBytes { $0.loadUnaligned(fromByteOffset: pos, as: Int.self) }
            pos += 8
            let deltaLength = data.withUnsafeBytes { Int($0.loadUnaligned(fromByteOffset: pos, as: UInt32.self)) }
            pos += 4
            guard pos + deltaLength <= data.count else { return nil }
            let deltaData = data.subdata(in: pos..<(pos + deltaLength))
            return LogRecord(lsn: lsn, type: type, transactionID: txID, timestamp: timestamp, content: .pageDelta(pageID, timestamp, deltaData))

        case .recordInsert, .recordDelete:
            // Format: [8B txID][8B timestamp][8B pageID][8B recordID][4B dataLength][N bytes data]
            guard pos + 8 + 8 + 8 + 8 + 4 <= data.count else { return nil }
            let txID = data.withUnsafeBytes { $0.loadUnaligned(fromByteOffset: pos, as: UInt64.self) }
            pos += 8
            let timestamp = data.withUnsafeBytes { $0.loadUnaligned(fromByteOffset: pos, as: UInt64.self) }
            pos += 8
            let pageID = data.withUnsafeBytes { $0.loadUnaligned(fromByteOffset: pos, as: Int.self) }
            pos += 8
            let recordID = data.withUnsafeBytes { $0.loadUnaligned(fromByteOffset: pos, as: UInt64.self) }
            pos += 8
            let dataLength = data.withUnsafeBytes { Int($0.loadUnaligned(fromByteOffset: pos, as: UInt32.self)) }
            pos += 4
            guard pos + dataLength <= data.count else { return nil }
            let recordData = data.subdata(in: pos..<(pos + dataLength))
            return LogRecord(lsn: lsn, type: type, transactionID: txID, timestamp: timestamp, content: .recordOp(pageID: pageID, recordID: recordID, data: recordData))
        }
    }

    private func cacheLogRecord(lsn: UInt64, type: LogRecordType, txID: UInt64, timestamp: UInt64, content: LogContent) {
        let record = LogRecord(lsn: lsn, type: type, transactionID: txID, timestamp: timestamp, content: content)
        logCache[lsn] = record
        logCacheFIFO.append(lsn)

        if logCache.count > logCacheLimit {
            // FIFO eviction: remove oldest entries from front
            let excess = logCache.count - logCacheLimit
            for lsnToRemove in logCacheFIFO.prefix(excess) {
                logCache.removeValue(forKey: lsnToRemove)
            }
            logCacheFIFO.removeFirst(excess)
        }
    }

    // MARK: - Recovery Helpers

    /// Scan only the persisted WAL file to recover nextLSN (ignores logCache to avoid stale entries)
    private func recoverNextLSN() throws {
        let records = try parsePersistedRecords()
        if let maxLSN = records.map({ $0.lsn }).max() {
            nextLSN = maxLSN + 1
        }
    }

    /// Return the maximum transaction ID found in the persisted WAL (for TransactionManager recovery)
    public func recoverMaxTransactionID() throws -> UInt64 {
        let records = try parsePersistedRecords()
        return records.map { $0.transactionID }.max() ?? 0
    }

    /// Parse only the on-disk WAL records without merging logCache
    private func parsePersistedRecords() throws -> [LogRecord] {
        guard let handle = logFileHandle else {
            throw PantryError.logReadError
        }

        try handle.seek(toOffset: 0)
        guard let allData = try handle.availableData() else {
            return []
        }

        if allData.count <= 64 {
            return []
        }

        var records: [LogRecord] = []
        var pos = 64

        while pos + 8 <= allData.count {
            let totalLength = allData.withUnsafeBytes { $0.loadUnaligned(fromByteOffset: pos, as: UInt32.self) }
            let storedCRC = allData.withUnsafeBytes { $0.loadUnaligned(fromByteOffset: pos + 4, as: UInt32.self) }
            pos += 8

            let dataLength = Int(totalLength) - 8
            guard dataLength > 0, pos + dataLength <= allData.count else { break }

            let payload = allData.subdata(in: pos..<(pos + dataLength))
            let computedCRC = CRC32.checksum(payload)
            guard computedCRC == storedCRC else {
                pos += dataLength
                continue
            }

            // Decrypt if encryption is configured
            let recordData: Data
            if let provider = encryptionProvider {
                do { recordData = try provider.decrypt(payload) } catch { pos += dataLength; continue }
            } else {
                recordData = payload
            }

            if let record = parseLogRecord(recordData) {
                records.append(record)
            }
            pos += dataLength
        }

        return records
    }

    // MARK: - Truncation

    /// Truncate the WAL by replacing it with a header-only file
    public func truncate() throws {
        try logFileHandle?.synchronize()
        try logFileHandle?.close()
        logFileHandle = nil

        // Replace file with empty WAL (header only)
        try Data().write(to: URL(fileURLWithPath: logFilePath))
        logFileHandle = try FileHandle(forUpdating: URL(fileURLWithPath: logFilePath))
        currentLogPosition = 0
        logCache.removeAll()
        logCacheFIFO.removeAll()
        nextLSN = 1
        try writeLogHeader()
    }

    /// Current WAL file size in bytes
    public var walSize: UInt64 { currentLogPosition }

    // MARK: - Compression Helpers

    /// Compress data using LZ4. Returns nil if compression doesn't save space.
    private nonisolated func compressLZ4(_ data: Data) -> Data? {
        let sourceSize = data.count
        // LZ4 worst case: input size + overhead
        let destCapacity = sourceSize + (sourceSize / 255) + 16
        var dest = Data(count: destCapacity)
        let compressedSize = data.withUnsafeBytes { srcPtr in
            dest.withUnsafeMutableBytes { dstPtr in
                compression_encode_buffer(
                    dstPtr.baseAddress!.assumingMemoryBound(to: UInt8.self),
                    destCapacity,
                    srcPtr.baseAddress!.assumingMemoryBound(to: UInt8.self),
                    sourceSize,
                    nil,
                    COMPRESSION_LZ4
                )
            }
        }
        guard compressedSize > 0 && compressedSize < sourceSize else { return nil }
        dest.count = compressedSize
        return dest
    }

    /// Decompress LZ4-compressed data. `originalSize` must be the uncompressed size.
    private nonisolated func decompressLZ4(_ data: Data, originalSize: Int) -> Data? {
        var dest = Data(count: originalSize)
        let decompressedSize = data.withUnsafeBytes { srcPtr in
            dest.withUnsafeMutableBytes { dstPtr in
                compression_decode_buffer(
                    dstPtr.baseAddress!.assumingMemoryBound(to: UInt8.self),
                    originalSize,
                    srcPtr.baseAddress!.assumingMemoryBound(to: UInt8.self),
                    data.count,
                    nil,
                    COMPRESSION_LZ4
                )
            }
        }
        guard decompressedSize == originalSize else { return nil }
        return dest
    }

    // MARK: - Cleanup

    public func close() throws {
        try logFileHandle?.synchronize()
        try logFileHandle?.close()
        logFileHandle = nil
    }
}

// MARK: - FileHandle helper

extension FileHandle {
    func availableData() throws -> Data? {
        try seek(toOffset: 0)
        let data = try readToEnd()
        return data
    }
}

/// Transaction isolation levels
public enum IsolationLevel: Int, Sendable {
    case readUncommitted = 0
    case readCommitted = 1
    case repeatableRead = 2
    case serializable = 3
}
