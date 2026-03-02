import Foundation
import Synchronization

/// Manages page-level file I/O with optional encryption at the disk boundary.
/// Uses POSIX pread/pwrite for thread-safe concurrent I/O without seek serialization.
public final class StorageManager: Sendable {
    private let fileURL: URL
    private let fd: Mutex<Int32?>
    private let encryptionProvider: EncryptionProvider?
    private let pageSize: Int
    public let diskPageSize: Int

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
        fd.withLock { fileFD in
            if let fileFD, fileFD >= 0 {
                Darwin.close(fileFD)
            }
            fileFD = nil
        }
    }
}
