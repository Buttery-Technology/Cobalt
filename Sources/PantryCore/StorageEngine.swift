import Foundation

/// Protocol that PantryIndex implements to provide index operations to the storage engine
public protocol IndexHook: Sendable {
    func lookupRecord(id: UInt64, tableName: String) async throws -> Int?
    func updateIndexes(record: Record, row: Row, tableName: String) async throws
    func removeFromIndexes(id: UInt64, row: Row, tableName: String) async throws
}

/// Main storage engine coordinating pages, buffer pool, WAL, transactions, and table registry
public actor StorageEngine: Sendable {
    public let bufferPoolManager: BufferPoolManager
    public let storageManager: StorageManager
    public let transactionManager: TransactionManager
    public let tableRegistry: TableRegistry
    private let logManager: WriteAheadLog

    /// Optional index hook wired by PantryIndex
    public var indexHook: (any IndexHook)?

    public init(databasePath: String, bufferPoolCapacity: Int = 1000, encryptionProvider: EncryptionProvider? = nil) async throws {
        let sm = try StorageManager(databasePath: databasePath, encryptionProvider: encryptionProvider)
        self.storageManager = sm
        self.bufferPoolManager = BufferPoolManager(capacity: bufferPoolCapacity, storageManager: sm)
        self.logManager = try await WriteAheadLog(databasePath: databasePath, storageManager: sm)
        self.transactionManager = TransactionManager(logManager: logManager)
        self.tableRegistry = TableRegistry(storageManager: sm, bufferPool: bufferPoolManager)

        // Wire page flusher to break circular reference
        await transactionManager.setPageFlusher { [bufferPoolManager, storageManager] modifiedPages in
            for pageID in modifiedPages {
                if let page = await bufferPoolManager.getCachedPage(pageID: pageID) {
                    var pageToWrite = page
                    try await storageManager.writePage(&pageToWrite)
                    await bufferPoolManager.clearDirtyFlag(pageID: pageID)
                }
            }
        }

        // Ensure system pages exist (page 0 = DB metadata, page 1 = table registry)
        let totalPages = try await sm.totalPageCount()
        if totalPages == 0 {
            var metaPage = try await sm.createNewPage()
            metaPage.pageFlags = [.system]
            try await sm.writePage(&metaPage)
            // Reserve page 1 for table registry so data pages start at page 2+
            var registryPage = try await sm.createNewPage()
            registryPage.pageFlags = [.system, .tableRegistry]
            try await sm.writePage(&registryPage)
        } else if totalPages == 1 {
            // Database has only page 0; reserve page 1 for registry
            var registryPage = try await sm.createNewPage()
            registryPage.pageFlags = [.system, .tableRegistry]
            try await sm.writePage(&registryPage)
        }

        // Load table registry
        try await tableRegistry.load()
    }

    // MARK: - Page Operations

    public func getPage(pageID: Int, transactionContext: TransactionContext? = nil) async throws -> DatabasePage {
        if let cachedPage = await bufferPoolManager.getCachedPage(pageID: pageID) {
            if let txContext = transactionContext {
                await txContext.recordAccess(pageID: pageID, isWrite: false)
            }
            return cachedPage
        }

        let loadedPage = try await storageManager.readPage(pageID: pageID)
        await bufferPoolManager.cachePage(loadedPage)

        if let txContext = transactionContext {
            await txContext.recordAccess(pageID: pageID, isWrite: false)
        }
        return loadedPage
    }

    public func savePage(_ page: DatabasePage, transactionContext: TransactionContext? = nil) async throws {
        if let txContext = transactionContext {
            await txContext.markModified(pageID: page.pageID)
            await txContext.recordAccess(pageID: page.pageID, isWrite: true)
        }

        // Serialize before caching so the data buffer is current
        var serializedPage = page
        try serializedPage.saveRecords()

        await bufferPoolManager.updatePage(serializedPage)
        await bufferPoolManager.markDirty(pageID: page.pageID)

        if transactionContext == nil {
            try await storageManager.writePage(&serializedPage)
            await bufferPoolManager.clearDirtyFlag(pageID: page.pageID)
        }
    }

    // MARK: - Record Operations

    @discardableResult
    public func insertRecord(_ record: Record, tableName: String, row: Row? = nil, transactionContext: TransactionContext? = nil) async throws -> UInt64 {
        guard await tableRegistry.getTableInfo(name: tableName) != nil else {
            throw PantryError.tableNotFound(name: tableName)
        }

        let recordSize = record.serialize().count
        // findTargetPageForInsert may create new pages and update the registry
        guard let currentInfo = await tableRegistry.getTableInfo(name: tableName) else {
            throw PantryError.tableNotFound(name: tableName)
        }
        let targetPageID = try await findTargetPageForInsert(tableInfo: currentInfo, recordSize: recordSize)
        var page = try await getPage(pageID: targetPageID, transactionContext: transactionContext)

        if !page.addRecord(record) {
            // Re-read table info since findTargetPageForInsert may have updated it
            guard var freshInfo = await tableRegistry.getTableInfo(name: tableName) else {
                throw PantryError.tableNotFound(name: tableName)
            }
            // createNewPageForTable links the new page at the tail of the chain;
            // do NOT splice it after `page` or it creates an infinite cycle
            var newPage = try await createNewPageForTable(tableInfo: &freshInfo)
            if !newPage.addRecord(record) {
                throw PantryError.recordTooLarge(size: recordSize)
            }

            try await savePage(newPage, transactionContext: transactionContext)
            await tableRegistry.updateTableInfo(freshInfo)
        } else {
            try await savePage(page, transactionContext: transactionContext)
        }

        // Re-read table info to avoid overwriting changes from page allocation
        guard var updatedInfo = await tableRegistry.getTableInfo(name: tableName) else {
            throw PantryError.tableNotFound(name: tableName)
        }
        updatedInfo.recordCount += 1
        await tableRegistry.updateTableInfo(updatedInfo)

        // Update indexes
        if let row = row {
            try await indexHook?.updateIndexes(record: record, row: row, tableName: tableName)
        }

        return record.id
    }

    public func getRecord(id: UInt64, tableName: String, transactionContext: TransactionContext? = nil) async throws -> Record {
        // Try index lookup first
        if let pageID = try await indexHook?.lookupRecord(id: id, tableName: tableName) {
            let page = try await getPage(pageID: pageID, transactionContext: transactionContext)
            if let record = page.records.first(where: { $0.id == id }) {
                return record
            }
        }

        return try await findRecordBySequentialScan(id: id, tableName: tableName, transactionContext: transactionContext)
    }

    public func deleteRecord(id: UInt64, tableName: String, transactionContext: TransactionContext? = nil) async throws {
        guard await tableRegistry.getTableInfo(name: tableName) != nil else {
            throw PantryError.tableNotFound(name: tableName)
        }

        let pageID = try await findPageContainingRecord(id: id, tableName: tableName)
        var page = try await getPage(pageID: pageID, transactionContext: transactionContext)

        // Decode the row before deletion so indexes can be updated
        let deletedRow: Row?
        if let record = page.records.first(where: { $0.id == id }) {
            let dec = JSONDecoder()
            dec.nonConformingFloatDecodingStrategy = .convertFromString(positiveInfinity: "+inf", negativeInfinity: "-inf", nan: "nan")
            deletedRow = try? dec.decode(Row.self, from: record.data)
        } else {
            deletedRow = nil
        }

        if !page.deleteRecord(id: id) {
            throw PantryError.recordNotFound(id: id)
        }

        try await savePage(page, transactionContext: transactionContext)

        if let row = deletedRow {
            try await indexHook?.removeFromIndexes(id: id, row: row, tableName: tableName)
        }

        // Re-read tableInfo to avoid lost-update race with concurrent operations
        guard var freshInfo = await tableRegistry.getTableInfo(name: tableName) else {
            throw PantryError.tableNotFound(name: tableName)
        }
        if freshInfo.recordCount > 0 {
            freshInfo.recordCount -= 1
        }
        await tableRegistry.updateTableInfo(freshInfo)
    }

    /// Scan all records in a table
    public func scanTable(_ tableName: String, transactionContext: TransactionContext? = nil) async throws -> [(Record, Row)] {
        guard let tableInfo = await tableRegistry.getTableInfo(name: tableName) else {
            throw PantryError.tableNotFound(name: tableName)
        }

        var results: [(Record, Row)] = []
        var currentPageID = tableInfo.firstPageID
        var visited: Set<Int> = []
        let decoder = JSONDecoder()
        decoder.nonConformingFloatDecodingStrategy = .convertFromString(positiveInfinity: "+inf", negativeInfinity: "-inf", nan: "nan")

        while currentPageID != 0 {
            guard visited.insert(currentPageID).inserted else {
                throw PantryError.corruptPage(pageID: currentPageID)
            }
            let page = try await getPage(pageID: currentPageID, transactionContext: transactionContext)
            for record in page.records {
                if let row = try? decoder.decode(Row.self, from: record.data) {
                    results.append((record, row))
                }
            }
            currentPageID = page.nextPageID
        }

        return results
    }

    /// Check if a table exists
    public func tableExists(_ name: String) async -> Bool {
        await tableRegistry.getTableInfo(name: name) != nil
    }

    /// List all table names
    public func listTables() async -> [String] {
        await tableRegistry.allTables().map { $0.name }
    }

    // MARK: - Table Management

    public func createTable(_ schema: PantryTableSchema) async throws {
        // Create the first data page for this table
        let firstPage = try await storageManager.createNewPage()
        await bufferPoolManager.cachePage(firstPage)

        _ = try await tableRegistry.registerTable(name: schema.name, schema: schema, firstPageID: firstPage.pageID)
        try await tableRegistry.save()
    }

    public func dropTable(_ name: String) async throws {
        guard let tableInfo = await tableRegistry.getTableInfo(name: name) else {
            throw PantryError.tableNotFound(name: name)
        }

        // Remove from registry first so a crash won't leave a dangling reference
        // to zeroed pages (orphaned pages are a space leak, not data corruption)
        await tableRegistry.removeTable(name: name)
        try await tableRegistry.save()

        // Then clear the data pages
        var currentPageID = tableInfo.firstPageID
        var visited: Set<Int> = []
        while currentPageID != 0 {
            guard visited.insert(currentPageID).inserted else { break }
            let page = try await getPage(pageID: currentPageID)
            let nextID = page.nextPageID
            let clearedPage = DatabasePage(pageID: currentPageID)
            try await savePage(clearedPage)
            currentPageID = nextID
        }
    }

    // MARK: - Transaction Passthrough

    public func beginTransaction(isolationLevel: IsolationLevel? = nil) async throws -> TransactionContext {
        try await transactionManager.beginTransaction(isolationLevel: isolationLevel)
    }

    public func commitTransaction(_ txContext: TransactionContext) async throws {
        try await transactionManager.commitTransaction(txContext)
    }

    public func rollbackTransaction(_ txContext: TransactionContext) async throws {
        try await transactionManager.rollbackTransaction(txContext)
    }

    // MARK: - Helpers

    private func findTargetPageForInsert(tableInfo: TableInfo, recordSize: Int) async throws -> Int {
        var currentPageID = tableInfo.firstPageID
        var visited: Set<Int> = []

        while currentPageID != 0 {
            guard visited.insert(currentPageID).inserted else {
                throw PantryError.corruptPage(pageID: currentPageID)
            }
            let page = try await getPage(pageID: currentPageID)
            if page.getFreeSpace() > recordSize + PantryConstants.SLOT_SIZE {
                return currentPageID
            }
            currentPageID = page.nextPageID
        }

        // Need a new page — caller will handle linking
        var info = tableInfo
        let newPage = try await createNewPageForTable(tableInfo: &info)
        await tableRegistry.updateTableInfo(info)
        return newPage.pageID
    }

    private func createNewPageForTable(tableInfo: inout TableInfo) async throws -> DatabasePage {
        let newPage = try await storageManager.createNewPage()
        await bufferPoolManager.cachePage(newPage)

        if tableInfo.firstPageID == 0 {
            tableInfo.firstPageID = newPage.pageID
            tableInfo.lastPageID = newPage.pageID
        } else {
            // Walk the chain to find the true last page — the cached lastPageID may be
            // stale after a crash (registry not persisted on every page allocation)
            var currentPageID = tableInfo.lastPageID
            var visited: Set<Int> = []
            while true {
                guard visited.insert(currentPageID).inserted else { break }
                let page = try await getPage(pageID: currentPageID)
                if page.nextPageID == 0 {
                    // Found the real tail
                    var tailPage = page
                    tailPage.nextPageID = newPage.pageID
                    try await savePage(tailPage)
                    break
                }
                currentPageID = page.nextPageID
            }
            tableInfo.lastPageID = newPage.pageID
        }

        return newPage
    }

    private func findRecordBySequentialScan(id: UInt64, tableName: String, transactionContext: TransactionContext?) async throws -> Record {
        guard let tableInfo = await tableRegistry.getTableInfo(name: tableName) else {
            throw PantryError.tableNotFound(name: tableName)
        }

        var currentPageID = tableInfo.firstPageID
        var visited: Set<Int> = []
        while currentPageID != 0 {
            guard visited.insert(currentPageID).inserted else {
                throw PantryError.corruptPage(pageID: currentPageID)
            }
            let page = try await getPage(pageID: currentPageID, transactionContext: transactionContext)
            if let record = page.records.first(where: { $0.id == id }) {
                return record
            }
            currentPageID = page.nextPageID
        }

        throw PantryError.recordNotFound(id: id)
    }

    private func findPageContainingRecord(id: UInt64, tableName: String) async throws -> Int {
        guard let tableInfo = await tableRegistry.getTableInfo(name: tableName) else {
            throw PantryError.tableNotFound(name: tableName)
        }

        var currentPageID = tableInfo.firstPageID
        var visited: Set<Int> = []
        while currentPageID != 0 {
            guard visited.insert(currentPageID).inserted else {
                throw PantryError.corruptPage(pageID: currentPageID)
            }
            let page = try await getPage(pageID: currentPageID)
            if page.records.contains(where: { $0.id == id }) {
                return currentPageID
            }
            currentPageID = page.nextPageID
        }

        throw PantryError.recordNotFound(id: id)
    }

    // MARK: - Lifecycle

    public func close() async throws {
        try await bufferPoolManager.flushAllDirtyPages()
        try await tableRegistry.save()
        try await logManager.close()
        try await storageManager.close()
    }

    public func getBufferPoolStats() async -> BufferPoolStats {
        await bufferPoolManager.getStats()
    }
}

// Extension on TransactionManager to wire the page flusher
extension TransactionManager {
    public func setPageFlusher(_ flusher: @escaping @Sendable (Set<Int>) async throws -> Void) {
        self.pageFlusher = flusher
    }
}
