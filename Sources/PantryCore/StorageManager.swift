import Foundation

/// Manages page-level file I/O with optional encryption at the disk boundary
public actor StorageManager: Sendable {
    private let fileURL: URL
    private var fileHandle: FileHandle?
    private let encryptionProvider: EncryptionProvider?
    private let pageSize: Int
    private let diskPageSize: Int

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

        fileHandle = try FileHandle(forUpdating: fileURL)
    }

    private func requireHandle() throws -> FileHandle {
        guard let fh = fileHandle else {
            throw PantryError.databaseClosed
        }
        return fh
    }

    /// Write a page to disk, encrypting if configured (does NOT fsync — call sync() at durability points)
    public func writePage(_ page: inout DatabasePage) throws {
        let fh = try requireHandle()
        try page.saveRecords()

        let offset = UInt64(page.pageID) * UInt64(diskPageSize)

        let dataToWrite: Data
        if let provider = encryptionProvider {
            dataToWrite = try provider.encrypt(page.data)
        } else {
            dataToWrite = page.data
        }

        try fh.seek(toOffset: offset)
        try fh.write(contentsOf: dataToWrite)
    }

    /// Read a page from disk, decrypting if configured
    public func readPage(pageID: Int) throws -> DatabasePage {
        let fh = try requireHandle()
        let offset = UInt64(pageID) * UInt64(diskPageSize)

        try fh.seek(toOffset: offset)
        guard let rawData = try fh.read(upToCount: diskPageSize),
              rawData.count == diskPageSize else {
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

    /// Create a new blank page and extend the database file
    public func createNewPage() throws -> DatabasePage {
        let fh = try requireHandle()
        let currentSize = try fh.seekToEnd()
        let newPageID = Int(currentSize) / diskPageSize

        // Build the page struct and serialize its header into the data buffer
        // so that if this page is evicted and re-read, loadRecords() sees the correct pageID
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

        try fh.write(contentsOf: dataToWrite)

        return page
    }

    /// Flush pending writes to disk. Call at durability points: WAL commit, checkpoint, close.
    public func sync() throws {
        try requireHandle().synchronize()
    }

    /// Get total number of pages in the file
    public func totalPageCount() throws -> Int {
        let fh = try requireHandle()
        let size = try fh.seekToEnd()
        return Int(size) / diskPageSize
    }

    /// Close the database file
    public func close() throws {
        try fileHandle?.synchronize()
        try fileHandle?.close()
        fileHandle = nil
    }
}
