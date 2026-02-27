import Foundation

/// Write-Ahead Log for transaction durability and crash recovery
public actor WriteAheadLog: Sendable {
    private let logFilePath: String
    private var logFileHandle: FileHandle?
    private var currentLogPosition: UInt64 = 0
    private var nextLSN: UInt64 = 1
    private var logCache: [UInt64: LogRecord] = [:]
    private let logCacheLimit = 1000
    private let storageManager: StorageManager

    // MARK: - Types

    public enum LogRecordType: UInt8, Sendable {
        case transactionBegin = 1
        case transactionCommit = 2
        case transactionRollback = 3
        case pageBeforeUpdate = 4
        case pageAfterUpdate = 5
        case checkpoint = 6
    }

    public enum LogContent: Sendable {
        case transaction(IsolationLevel)
        case pageImage(Int, UInt64, Data) // pageID, timestamp, data
        case checkpoint([UInt64]) // active transaction IDs
    }

    public struct LogRecord: Sendable {
        public let lsn: UInt64
        public let type: LogRecordType
        public let transactionID: UInt64
        public let timestamp: UInt64
        public let content: LogContent
    }

    // MARK: - Initialization

    public init(databasePath: String, storageManager: StorageManager) async throws {
        self.logFilePath = databasePath + ".wal"
        self.storageManager = storageManager

        if !FileManager.default.fileExists(atPath: logFilePath) {
            FileManager.default.createFile(atPath: logFilePath, contents: nil)
        }

        logFileHandle = try FileHandle(forUpdating: URL(fileURLWithPath: logFilePath))
        currentLogPosition = try logFileHandle?.seekToEnd() ?? 0

        if currentLogPosition == 0 {
            try writeLogHeader()
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

    public func logTransactionCommit(txID: UInt64) throws {
        let lsn = nextLSN
        nextLSN += 1

        var logData = Data()
        logData.append(LogRecordType.transactionCommit.rawValue)
        withUnsafeBytes(of: lsn) { logData.append(contentsOf: $0) }
        withUnsafeBytes(of: txID) { logData.append(contentsOf: $0) }
        let timestamp = UInt64(Date().timeIntervalSince1970)
        withUnsafeBytes(of: timestamp) { logData.append(contentsOf: $0) }

        cacheLogRecord(lsn: lsn, type: .transactionCommit, txID: txID, timestamp: timestamp, content: .transaction(.readCommitted))

        try writeLogRecord(logData)
        try logFileHandle?.synchronize()
    }

    public func logTransactionRollback(txID: UInt64) throws {
        let lsn = nextLSN
        nextLSN += 1

        var logData = Data()
        logData.append(LogRecordType.transactionRollback.rawValue)
        withUnsafeBytes(of: lsn) { logData.append(contentsOf: $0) }
        withUnsafeBytes(of: txID) { logData.append(contentsOf: $0) }
        let timestamp = UInt64(Date().timeIntervalSince1970)
        withUnsafeBytes(of: timestamp) { logData.append(contentsOf: $0) }

        cacheLogRecord(lsn: lsn, type: .transactionRollback, txID: txID, timestamp: timestamp, content: .transaction(.readCommitted))

        try writeLogRecord(logData)
    }

    // MARK: - Page Image Logging

    public func logPageBeforeImage(txID: UInt64, page: DatabasePage) throws {
        let lsn = nextLSN
        nextLSN += 1

        var logData = Data()
        logData.append(LogRecordType.pageBeforeUpdate.rawValue)
        withUnsafeBytes(of: lsn) { logData.append(contentsOf: $0) }
        withUnsafeBytes(of: txID) { logData.append(contentsOf: $0) }
        withUnsafeBytes(of: page.pageID) { logData.append(contentsOf: $0) }
        let dataLength = UInt32(page.data.count)
        withUnsafeBytes(of: dataLength) { logData.append(contentsOf: $0) }
        logData.append(page.data)

        let timestamp = UInt64(Date().timeIntervalSince1970)
        cacheLogRecord(lsn: lsn, type: .pageBeforeUpdate, txID: txID, timestamp: timestamp, content: .pageImage(page.pageID, timestamp, page.data))

        try writeLogRecord(logData)
    }

    public func logPageAfterImage(txID: UInt64, page: DatabasePage) throws {
        let lsn = nextLSN
        nextLSN += 1

        var logData = Data()
        logData.append(LogRecordType.pageAfterUpdate.rawValue)
        withUnsafeBytes(of: lsn) { logData.append(contentsOf: $0) }
        withUnsafeBytes(of: txID) { logData.append(contentsOf: $0) }
        withUnsafeBytes(of: page.pageID) { logData.append(contentsOf: $0) }
        let dataLength = UInt32(page.data.count)
        withUnsafeBytes(of: dataLength) { logData.append(contentsOf: $0) }
        logData.append(page.data)

        let timestamp = UInt64(Date().timeIntervalSince1970)
        cacheLogRecord(lsn: lsn, type: .pageAfterUpdate, txID: txID, timestamp: timestamp, content: .pageImage(page.pageID, timestamp, page.data))

        try writeLogRecord(logData)
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

    /// Undo a transaction by restoring before-images via StorageManager
    public func undoTransaction(txID: UInt64) async throws {
        let logRecords = try getAllLogRecords()

        let txRecords = logRecords.filter { $0.transactionID == txID }
            .sorted { $0.lsn > $1.lsn }

        for record in txRecords {
            if case let .pageImage(pageID, _, pageData) = record.content,
               record.type == .pageBeforeUpdate {
                var restoredPage = DatabasePage(pageID: pageID, data: pageData)
                restoredPage.loadRecords()
                try await storageManager.writePage(&restoredPage)
            }
        }
    }

    // MARK: - Checkpointing

    public func createCheckpoint(activeTransactionCount: UInt32) throws {
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
    }

    // MARK: - Log Record I/O

    private func writeLogRecord(_ data: Data) throws {
        let crc = CRC32.checksum(data)

        var headerData = Data()
        let totalLength = UInt32(data.count + 8)
        withUnsafeBytes(of: totalLength) { headerData.append(contentsOf: $0) }
        withUnsafeBytes(of: crc) { headerData.append(contentsOf: $0) }

        try logFileHandle?.seek(toOffset: currentLogPosition)
        try logFileHandle?.write(contentsOf: headerData)
        try logFileHandle?.write(contentsOf: data)

        currentLogPosition += UInt64(headerData.count + data.count)
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

            let recordData = allData.subdata(in: pos..<(pos + dataLength))
            let computedCRC = CRC32.checksum(recordData)
            guard computedCRC == storedCRC else {
                pos += dataLength
                continue
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
            guard pos + 8 + 8 + 4 <= data.count else { return nil }
            let txID = data.withUnsafeBytes { $0.loadUnaligned(fromByteOffset: pos, as: UInt64.self) }
            pos += 8
            let pageID = data.withUnsafeBytes { $0.loadUnaligned(fromByteOffset: pos, as: Int.self) }
            pos += 8
            let dataLength = data.withUnsafeBytes { Int($0.loadUnaligned(fromByteOffset: pos, as: UInt32.self)) }
            pos += 4
            guard pos + dataLength <= data.count else { return nil }
            let pageData = data.subdata(in: pos..<(pos + dataLength))
            let timestamp = UInt64(Date().timeIntervalSince1970)
            return LogRecord(lsn: lsn, type: type, transactionID: txID, timestamp: timestamp, content: .pageImage(pageID, timestamp, pageData))

        case .checkpoint:
            guard pos + 8 <= data.count else { return nil }
            let timestamp = data.withUnsafeBytes { $0.loadUnaligned(fromByteOffset: pos, as: UInt64.self) }
            return LogRecord(lsn: lsn, type: type, transactionID: 0, timestamp: timestamp, content: .checkpoint([]))
        }
    }

    private func cacheLogRecord(lsn: UInt64, type: LogRecordType, txID: UInt64, timestamp: UInt64, content: LogContent) {
        let record = LogRecord(lsn: lsn, type: type, transactionID: txID, timestamp: timestamp, content: content)
        logCache[lsn] = record

        if logCache.count > logCacheLimit {
            let sortedKeys = logCache.keys.sorted()
            let keysToRemove = sortedKeys.prefix(logCache.count - logCacheLimit)
            for key in keysToRemove {
                logCache.removeValue(forKey: key)
            }
        }
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
