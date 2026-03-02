import Foundation
import Synchronization

/// Manages page-level file I/O with optional encryption at the disk boundary.
/// Uses POSIX pread/pwrite for thread-safe concurrent I/O without seek serialization.
/// For unencrypted databases, mmap provides zero-syscall read access to clean pages.
public final class StorageManager: Sendable {
    private let fileURL: URL
    private let fd: Mutex<Int32?>
    private let encryptionProvider: EncryptionProvider?
    private let pageSize: Int
    public let diskPageSize: Int

    // MARK: - mmap State

    private struct MmapState {
        var baseAddress: UnsafeRawPointer? = nil
        var mappedLength: Int = 0
    }
    private let mmapState: PantryRWLock<MmapState>
    /// True when mmap is available (unencrypted databases only).
    public let mmapAvailable: Bool

    /// Initialize the storage manager
    /// - Parameters:
    ///   - databasePath: Explicit file path (no Bundle.main default)
    ///   - encryptionProvider: Optional encryption provider for at-rest encryption
    public init(databasePath: String, encryptionProvider: EncryptionProvider? = nil) throws {
        self.fileURL = URL(fileURLWithPath: databasePath)
        self.encryptionProvider = encryptionProvider
        self.pageSize = PantryConstants.PAGE_SIZE
        self.diskPageSize = encryptionProvider != nil ? PantryConstants.ENCRYPTED_PAGE_SIZE : PantryConstants.PAGE_SIZE

        // Create parent directory if needed
        let directory = fileURL.deletingLastPathComponent()
        if !FileManager.default.fileExists(atPath: directory.path) {
            try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        }

        // Create the database file if it doesn't exist
        if !FileManager.default.fileExists(atPath: fileURL.path) {
            FileManager.default.createFile(atPath: fileURL.path, contents: nil)
        }

        let fileFD = open(fileURL.path, O_RDWR)
        guard fileFD >= 0 else {
            throw PantryError.fileOpenError(description: "Failed to open database at \(databasePath)")
        }
        self.fd = Mutex(fileFD)

        // Set up mmap for unencrypted databases
        if encryptionProvider == nil {
            self.mmapAvailable = true
            let fileSize = lseek(fileFD, 0, SEEK_END)
            if fileSize > 0 {
                let length = Int(fileSize)
                let ptr = mmap(nil, length, PROT_READ, MAP_SHARED, fileFD, 0)
                if ptr != MAP_FAILED {
                    self.mmapState = PantryRWLock(MmapState(baseAddress: UnsafeRawPointer(ptr), mappedLength: length))
                } else {
                    self.mmapState = PantryRWLock(MmapState())
                }
            } else {
                self.mmapState = PantryRWLock(MmapState())
            }
        } else {
            self.mmapAvailable = false
            self.mmapState = PantryRWLock(MmapState())
        }
    }

    private func requireFD() throws -> Int32 {
        let fileFD = fd.withLock { $0 }
        guard let fileFD, fileFD >= 0 else {
            throw PantryError.databaseClosed
        }
        return fileFD
    }

    /// Write a page to disk using pwrite (thread-safe, no seek needed)
    /// Pass `alreadySerialized: true` to skip redundant saveRecords()
    public func writePage(_ page: inout DatabasePage, alreadySerialized: Bool = false) throws {
        let fileFD = try requireFD()
        if !alreadySerialized {
            try page.saveRecords()
        }

        let offset = off_t(page.pageID) * off_t(diskPageSize)

        let dataToWrite: Data
        if let provider = encryptionProvider {
            dataToWrite = try provider.encrypt(page.data)
        } else {
            dataToWrite = page.data
        }

        try dataToWrite.withUnsafeBytes { rawBuf in
            guard let baseAddr = rawBuf.baseAddress else { return }
            var written = 0
            while written < dataToWrite.count {
                let result = pwrite(fileFD, baseAddr + written, dataToWrite.count - written, offset + off_t(written))
                if result < 0 {
                    throw PantryError.pageWriteError(description: "pwrite failed for page \(page.pageID): errno \(errno)")
                }
                written += result
            }
        }
    }

    /// Read a page from disk using pread (thread-safe, no seek needed)
    public func readPage(pageID: Int) throws -> DatabasePage {
        let fileFD = try requireFD()
        let offset = off_t(pageID) * off_t(diskPageSize)

        var rawData = Data(count: diskPageSize)
        let bytesRead = rawData.withUnsafeMutableBytes { rawBuf -> Int in
            guard let baseAddr = rawBuf.baseAddress else { return 0 }
            return pread(fileFD, baseAddr, diskPageSize, offset)
        }
        guard bytesRead == diskPageSize else {
            throw PantryError.pageReadError(description: "Failed to read page \(pageID)")
        }

        let pageData: Data
        if let provider = encryptionProvider {
            pageData = try provider.decrypt(rawData)
        } else {
            pageData = rawData
        }

        var page = DatabasePage(
            pageID: pageID,
            nextPageID: 0,
            recordCount: 0,
            freeSpaceOffset: 0,
            flags: 0,
            data: pageData
        )
        page.loadRecords()
        return page
    }

    /// Create a new blank page and extend the database file.
    /// NOTE: This method is NOT thread-safe for concurrent callers — must be called
    /// from a serialized context (e.g., StorageEngine actor) to prevent duplicate page IDs.
    public func createNewPage() throws -> DatabasePage {
        let fileFD = try requireFD()

        // Get current file size to determine new page ID
        let currentSize = lseek(fileFD, 0, SEEK_END)
        guard currentSize >= 0 else {
            throw PantryError.pageWriteError(description: "lseek failed: errno \(errno)")
        }
        let newPageID = Int(currentSize) / diskPageSize

        var page = DatabasePage(
            pageID: newPageID,
            nextPageID: 0,
            recordCount: 0,
            freeSpaceOffset: pageSize,
            flags: 0,
            data: Data(count: pageSize)
        )
        try page.saveRecords()

        let dataToWrite: Data
        if let provider = encryptionProvider {
            dataToWrite = try provider.encrypt(page.data)
        } else {
            dataToWrite = page.data
        }

        // Append at end of file
        let offset = off_t(newPageID) * off_t(diskPageSize)
        try dataToWrite.withUnsafeBytes { rawBuf in
            guard let baseAddr = rawBuf.baseAddress else { return }
            var written = 0
            while written < dataToWrite.count {
                let result = pwrite(fileFD, baseAddr + written, dataToWrite.count - written, offset + off_t(written))
                if result < 0 {
                    throw PantryError.pageWriteError(description: "pwrite failed for new page \(newPageID): errno \(errno)")
                }
                written += result
            }
        }

        remapMmap()
        return page
    }

    /// Create multiple new pages in one operation, extending the file once.
    /// Pages are NOT written to disk — they exist only in memory until flushed via the buffer pool.
    public func createNewPages(count: Int) throws -> [DatabasePage] {
        guard count > 0 else { return [] }
        let fileFD = try requireFD()

        let currentSize = lseek(fileFD, 0, SEEK_END)
        guard currentSize >= 0 else {
            throw PantryError.pageWriteError(description: "lseek failed: errno \(errno)")
        }
        let firstPageID = Int(currentSize) / diskPageSize

        // Extend the file to accommodate all new pages in one operation
        let newSize = off_t(firstPageID + count) * off_t(diskPageSize)
        if ftruncate(fileFD, newSize) < 0 {
            throw PantryError.pageWriteError(description: "ftruncate failed: errno \(errno)")
        }

        var pages = [DatabasePage]()
        pages.reserveCapacity(count)
        for i in 0..<count {
            var page = DatabasePage(
                pageID: firstPageID + i,
                nextPageID: 0,
                recordCount: 0,
                freeSpaceOffset: pageSize,
                flags: 0,
                data: Data(count: pageSize)
            )
            try page.saveRecords()
            pages.append(page)
        }

        remapMmap()
        return pages
    }

    /// Read multiple pages in a single I/O operation when they are contiguous.
    /// Groups page IDs into contiguous runs and issues one pread per run.
    public func readPages(pageIDs: [Int]) throws -> [DatabasePage] {
        guard !pageIDs.isEmpty else { return [] }
        let fileFD = try requireFD()

        // Sort and group into contiguous runs
        let sorted = pageIDs.sorted()
        var pages = [DatabasePage]()
        pages.reserveCapacity(sorted.count)

        var runStart = sorted[0]
        var runCount = 1

        func readRun(start: Int, count: Int) throws {
            let totalBytes = count * diskPageSize
            let offset = off_t(start) * off_t(diskPageSize)
            var rawData = Data(count: totalBytes)
            let bytesRead = rawData.withUnsafeMutableBytes { rawBuf -> Int in
                guard let baseAddr = rawBuf.baseAddress else { return 0 }
                return pread(fileFD, baseAddr, totalBytes, offset)
            }
            guard bytesRead == totalBytes else {
                // Fallback to individual reads on partial read
                for i in 0..<count {
                    pages.append(try readPage(pageID: start + i))
                }
                return
            }
            for i in 0..<count {
                let pageStart = i * diskPageSize
                let pageEnd = pageStart + diskPageSize
                let pageSlice = rawData.subdata(in: pageStart..<pageEnd)

                let pageData: Data
                if let provider = encryptionProvider {
                    pageData = try provider.decrypt(pageSlice)
                } else {
                    pageData = pageSlice
                }

                var page = DatabasePage(pageID: start + i, data: pageData)
                page.loadRecords()
                pages.append(page)
            }
        }

        for idx in 1..<sorted.count {
            if sorted[idx] == sorted[idx - 1] + 1 {
                runCount += 1
            } else {
                try readRun(start: runStart, count: runCount)
                runStart = sorted[idx]
                runCount = 1
            }
        }
        try readRun(start: runStart, count: runCount)

        return pages
    }

    // MARK: - mmap Read Path

    /// Read a page directly from mmap'd memory — no syscall, no buffer pool lock.
    /// Returns nil if mmap is not active or the page is beyond the mapped region.
    public func readPageMmap(pageID: Int) -> DatabasePage? {
        mmapState.withReadLock { state in
            guard let base = state.baseAddress else { return nil }
            let offset = pageID * pageSize
            let end = offset + pageSize
            guard end <= state.mappedLength else { return nil }

            let pageData = Data(bytes: base + offset, count: pageSize)
            var page = DatabasePage(pageID: pageID, data: pageData)
            page.loadRecords()
            return page
        }
    }

    /// Read a single record by ID from mmap'd page data without deserializing all records.
    /// Parses only the page header + slot directory, then scans record headers for matching ID.
    /// Returns the record's raw data payload (excluding the 12-byte record header) on match.
    public func readRecordMmap(pageID: Int, recordID: UInt64) -> Data? {
        mmapState.withReadLock { state in
            guard let base = state.baseAddress else { return nil }
            let pageOffset = pageID * pageSize
            let pageEnd = pageOffset + pageSize
            guard pageEnd <= state.mappedLength else { return nil }

            let ptr = base + pageOffset

            // Read record count from header (offset 16, Int32)
            let recordCount = Int(ptr.loadUnaligned(fromByteOffset: 16, as: Int32.self))
            guard recordCount > 0, recordCount <= (pageSize - 28) / 6 else { return nil }

            // Scan slot directory (starts at offset 28, 6 bytes per slot)
            var slotPos = 28
            for _ in 0..<recordCount {
                let slotOffset = Int(ptr.loadUnaligned(fromByteOffset: slotPos, as: UInt16.self))
                let slotLength = Int(ptr.loadUnaligned(fromByteOffset: slotPos + 2, as: UInt32.self))
                slotPos += 6

                // Validate slot bounds
                guard slotOffset + slotLength <= pageSize, slotLength >= 12 else { continue }

                // Read record ID from record header (first 8 bytes)
                let rid = ptr.loadUnaligned(fromByteOffset: slotOffset, as: UInt64.self)
                if rid == recordID {
                    // Read payload length (next 4 bytes)
                    let payloadLen = Int(ptr.loadUnaligned(fromByteOffset: slotOffset + 8, as: UInt32.self))
                    let payloadStart = slotOffset + 12
                    guard payloadStart + payloadLen <= pageSize else { return nil }

                    // Check for overflow flag
                    if payloadLen > 0 {
                        let firstByte = ptr.load(fromByteOffset: payloadStart, as: UInt8.self)
                        if firstByte == 0x01 && payloadLen >= 9 {
                            // Overflow record — return nil to fall back to full page read
                            return nil
                        }
                    }

                    // Copy just this record's payload from mmap
                    return Data(bytes: ptr + payloadStart, count: payloadLen)
                }
            }
            return nil
        }
    }

    /// Iterate over records in an mmap'd page without allocating Record/DatabasePage arrays.
    /// Calls the visitor closure with (recordID, recordPayloadData) for each record.
    /// Returns false if mmap is unavailable or page is out of range.
    @discardableResult
    public func forEachRecordMmap(pageID: Int, _ visitor: (UInt64, Data) -> Bool) -> Bool {
        mmapState.withReadLock { state in
            guard let base = state.baseAddress else { return false }
            let pageOffset = pageID * pageSize
            let pageEnd = pageOffset + pageSize
            guard pageEnd <= state.mappedLength else { return false }

            let ptr = base + pageOffset

            let recordCount = Int(ptr.loadUnaligned(fromByteOffset: 16, as: Int32.self))
            guard recordCount > 0, recordCount <= (pageSize - 28) / 6 else { return true }

            var slotPos = 28
            for _ in 0..<recordCount {
                let slotOffset = Int(ptr.loadUnaligned(fromByteOffset: slotPos, as: UInt16.self))
                let slotLength = Int(ptr.loadUnaligned(fromByteOffset: slotPos + 2, as: UInt32.self))
                slotPos += 6

                guard slotOffset + slotLength <= pageSize, slotLength >= 12 else { continue }

                let rid = ptr.loadUnaligned(fromByteOffset: slotOffset, as: UInt64.self)
                let payloadLen = Int(ptr.loadUnaligned(fromByteOffset: slotOffset + 8, as: UInt32.self))
                let payloadStart = slotOffset + 12
                guard payloadStart + payloadLen <= pageSize else { continue }

                // Check for overflow — skip (caller must handle via full page load)
                if payloadLen > 0 {
                    let firstByte = ptr.load(fromByteOffset: payloadStart, as: UInt8.self)
                    if firstByte == 0x01 && payloadLen >= 9 { continue }
                }

                let recordData = Data(bytes: ptr + payloadStart, count: payloadLen)
                if !visitor(rid, recordData) { return true } // early exit
            }
            return true
        }
    }

    /// Re-map the file after growth (new pages created).
    /// Uses munmap + mmap since macOS has no mremap.
    public func remapMmap() {
        guard mmapAvailable else { return }
        guard let fileFD = fd.withLock({ $0 }), fileFD >= 0 else { return }
        let currentFileSize = lseek(fileFD, 0, SEEK_END)
        guard currentFileSize > 0 else { return }
        let newLength = Int(currentFileSize)

        mmapState.withWriteLock { state in
            guard newLength > state.mappedLength else { return }
            if let oldBase = state.baseAddress, state.mappedLength > 0 {
                munmap(UnsafeMutableRawPointer(mutating: oldBase), state.mappedLength)
            }
            let ptr = mmap(nil, newLength, PROT_READ, MAP_SHARED, fileFD, 0)
            if ptr != MAP_FAILED {
                state.baseAddress = UnsafeRawPointer(ptr)
                state.mappedLength = newLength
            } else {
                state.baseAddress = nil
                state.mappedLength = 0
            }
        }
    }

    /// Flush pending writes to disk
    public func sync() throws {
        let fileFD = try requireFD()
        fsync(fileFD)
    }

    /// Get total number of pages in the file
    public func totalPageCount() throws -> Int {
        let fileFD = try requireFD()
        let size = lseek(fileFD, 0, SEEK_END)
        guard size >= 0 else {
            throw PantryError.pageReadError(description: "lseek failed: errno \(errno)")
        }
        return Int(size) / diskPageSize
    }

    /// Close the database file
    public func close() throws {
        mmapState.withWriteLock { state in
            if let base = state.baseAddress, state.mappedLength > 0 {
                munmap(UnsafeMutableRawPointer(mutating: base), state.mappedLength)
            }
            state.baseAddress = nil
            state.mappedLength = 0
        }
        fd.withLock { fileFD in
            if let fileFD, fileFD >= 0 {
                Darwin.close(fileFD)
            }
            fileFD = nil
        }
    }
}
