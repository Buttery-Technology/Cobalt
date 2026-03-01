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

    /// Head of the free page list (0 = empty)
    private var freeListHead: Int = 0

    /// Page ID storing the index registry (0 = not allocated)
    private var indexRegistryPageID: Int = 0

    /// Free space map: table name → set of page IDs known to have free space.
    /// Lazily populated as pages are accessed; provides O(1) insert targeting.
    private var freeSpaceMap: [String: Set<Int>] = [:]

    public init(databasePath: String, bufferPoolCapacity: Int = 1000, encryptionProvider: EncryptionProvider? = nil) async throws {
        let sm = try StorageManager(databasePath: databasePath, encryptionProvider: encryptionProvider)
        self.storageManager = sm
        self.bufferPoolManager = BufferPoolManager(capacity: bufferPoolCapacity, storageManager: sm)
        self.logManager = try await WriteAheadLog(databasePath: databasePath, storageManager: sm)
        self.transactionManager = try await TransactionManager(logManager: logManager)
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

        // Wire page invalidator so rollback evicts stale pages from buffer pool
        await transactionManager.setPageInvalidator { [bufferPoolManager] modifiedPages in
            for pageID in modifiedPages {
                await bufferPoolManager.evictPage(pageID: pageID)
            }
        }

        // Wire all-page flusher for checkpoint
        await transactionManager.setAllPageFlusher { [bufferPoolManager] in
            try await bufferPoolManager.flushAllDirtyPages()
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

        // Load free list head and index registry page ID from page 0 metadata record
        let metaPage = try await sm.readPage(pageID: PantryConstants.SYSTEM_PAGE_DB_METADATA)
        if let metaRecord = metaPage.records.first,
           let meta = try? JSONDecoder().decode(DBMetadata.self, from: metaRecord.data) {
            freeListHead = meta.freeListHead
            indexRegistryPageID = meta.indexRegistryPageID
        }
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

            // Capture before-image for WAL rollback (only once per page per transaction)
            let alreadyLogged = await txContext.beforeImagePages.contains(page.pageID)
            if !alreadyLogged {
                // Read the current on-disk version of this page
                let totalPages = try await storageManager.totalPageCount()
                if page.pageID < totalPages {
                    let beforePage = try await storageManager.readPage(pageID: page.pageID)
                    try await logManager.logPageBeforeImage(txID: txContext.transactionID, page: beforePage)
                }
                await txContext.recordBeforeImage(pageID: page.pageID)
            }
        }

        // Serialize before caching so the data buffer is current
        var serializedPage = page
        try serializedPage.saveRecords()

        await bufferPoolManager.updatePage(serializedPage)
        await bufferPoolManager.markDirty(pageID: page.pageID)

        if let txContext = transactionContext {
            // Log after-image
            try await logManager.logPageAfterImage(txID: txContext.transactionID, page: serializedPage)
        }

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

        // Page now has more free space — add back to free space map
        freeSpaceMap[tableName, default: []].insert(pageID)

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

    /// Stream records from a table page-at-a-time, yielding (Record, Row) pairs.
    public func scanTableStream(_ tableName: String) async throws -> AsyncStream<(Record, Row)> {
        guard let tableInfo = await tableRegistry.getTableInfo(name: tableName) else {
            throw PantryError.tableNotFound(name: tableName)
        }

        let firstPageID = tableInfo.firstPageID
        let engine = self

        return AsyncStream { continuation in
            Task {
                var currentPageID = firstPageID
                var visited: Set<Int> = []
                let decoder = JSONDecoder()
                decoder.nonConformingFloatDecodingStrategy = .convertFromString(positiveInfinity: "+inf", negativeInfinity: "-inf", nan: "nan")

                while currentPageID != 0 {
                    guard visited.insert(currentPageID).inserted else { break }
                    do {
                        let page = try await engine.getPage(pageID: currentPageID)
                        for record in page.records {
                            if let row = try? decoder.decode(Row.self, from: record.data) {
                                continuation.yield((record, row))
                            }
                        }
                        currentPageID = page.nextPageID
                    } catch {
                        break
                    }
                }
                continuation.finish()
            }
        }
    }

    /// Check if a table exists
    public func tableExists(_ name: String) async -> Bool {
        await tableRegistry.getTableInfo(name: name) != nil
    }

    /// List all table names
    public func listTables() async -> [String] {
        await tableRegistry.allTables().map { $0.name }
    }

    /// Get a table's schema
    public func getTableSchema(_ name: String) async -> PantryTableSchema? {
        await tableRegistry.getTableInfo(name: name)?.schema
    }

    /// Update a table's schema in the registry (does not modify existing rows)
    public func updateTableSchema(_ name: String, schema: PantryTableSchema) async throws {
        guard var info = await tableRegistry.getTableInfo(name: name) else {
            throw PantryError.tableNotFound(name: name)
        }
        info.schema = schema
        await tableRegistry.updateTableInfo(info)
        try await tableRegistry.save()
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

        // Remove from registry and free space map first
        await tableRegistry.removeTable(name: name)
        freeSpaceMap.removeValue(forKey: name)
        try await tableRegistry.save()

        // Chain freed data pages into the free list
        var currentPageID = tableInfo.firstPageID
        var visited: Set<Int> = []
        while currentPageID != 0 {
            guard visited.insert(currentPageID).inserted else { break }
            let page = try await getPage(pageID: currentPageID)
            let nextDataPageID = page.nextPageID

            // Push this page onto the free list
            var freedPage = DatabasePage(pageID: currentPageID)
            freedPage.nextPageID = freeListHead
            try await savePage(freedPage)
            freeListHead = currentPageID

            currentPageID = nextDataPageID
        }

        // Persist the updated free list head to page 0
        try await saveFreeListHead()
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
        let requiredSpace = recordSize + PantryConstants.SLOT_SIZE

        // O(1) path: check free space map first
        if var candidates = freeSpaceMap[tableInfo.name] {
            while let pageID = candidates.first {
                let page = try await getPage(pageID: pageID)
                if page.getFreeSpace() > requiredSpace {
                    return pageID
                }
                // Page is full — remove from map
                candidates.remove(pageID)
                freeSpaceMap[tableInfo.name]?.remove(pageID)
            }
        }

        // Fallback: walk the page chain (populates free space map for future calls)
        var currentPageID = tableInfo.firstPageID
        var visited: Set<Int> = []

        while currentPageID != 0 {
            guard visited.insert(currentPageID).inserted else {
                throw PantryError.corruptPage(pageID: currentPageID)
            }
            let page = try await getPage(pageID: currentPageID)
            if page.getFreeSpace() > requiredSpace {
                // Add to free space map for future lookups
                freeSpaceMap[tableInfo.name, default: []].insert(currentPageID)
                return currentPageID
            }
            currentPageID = page.nextPageID
        }

        // Need a new page — caller will handle linking
        var info = tableInfo
        let newPage = try await createNewPageForTable(tableInfo: &info)
        await tableRegistry.updateTableInfo(info)
        // New page has full free space
        freeSpaceMap[tableInfo.name, default: []].insert(newPage.pageID)
        return newPage.pageID
    }

    private func createNewPageForTable(tableInfo: inout TableInfo) async throws -> DatabasePage {
        // Pop from free list if available, otherwise extend file
        let newPage: DatabasePage
        if freeListHead != 0 {
            let freePage = try await storageManager.readPage(pageID: freeListHead)
            freeListHead = freePage.nextPageID
            var reusedPage = DatabasePage(pageID: freePage.pageID)
            try reusedPage.saveRecords()
            await bufferPoolManager.updatePage(reusedPage)
            try await saveFreeListHead()
            newPage = reusedPage
        } else {
            newPage = try await storageManager.createNewPage()
        }
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

    // MARK: - Index Registry Persistence

    /// Load index registry entries from the dedicated page chain
    public func loadIndexRegistry() async throws -> Data? {
        guard indexRegistryPageID != 0 else { return nil }
        // Follow the page chain to collect all chunks
        var allData = Data()
        var currentPageID = indexRegistryPageID
        var visited: Set<Int> = []
        while currentPageID != 0 {
            guard visited.insert(currentPageID).inserted else { break }
            let page = try await storageManager.readPage(pageID: currentPageID)
            guard page.pageFlags.contains(.system) else { break }
            if let record = page.records.first {
                allData.append(record.data)
            }
            currentPageID = page.nextPageID
        }
        return allData.isEmpty ? nil : allData
    }

    /// Save index registry entries across a chain of dedicated pages
    public func saveIndexRegistry(_ data: Data) async throws {
        let maxRecordDataSize = PantryConstants.PAGE_SIZE - PantryConstants.PAGE_HEADER_SIZE - PantryConstants.SLOT_SIZE - 12 - 1 // 12 = record header (id + length), -1 for strict >= check in addRecord

        // Split data into chunks that fit in one page
        var chunks: [Data] = []
        var offset = 0
        while offset < data.count {
            let end = min(offset + maxRecordDataSize, data.count)
            chunks.append(data.subdata(in: offset..<end))
            offset = end
        }
        if chunks.isEmpty {
            chunks.append(Data())
        }

        // Allocate or reuse the first page
        if indexRegistryPageID == 0 {
            let firstPage = try await storageManager.createNewPage()
            indexRegistryPageID = firstPage.pageID
            try await saveDBMetadata()
        }

        var currentPageID = indexRegistryPageID
        for (i, chunk) in chunks.enumerated() {
            var page = DatabasePage(pageID: currentPageID)
            page.pageFlags = [.system]
            let record = Record(id: UInt64(i + 1), data: chunk)
            guard page.addRecord(record) else {
                throw PantryError.schemaSerializationError
            }

            if i < chunks.count - 1 {
                // Need a next page
                let nextPage = try await storageManager.createNewPage()
                page.nextPageID = nextPage.pageID
                currentPageID = nextPage.pageID
            } else {
                page.nextPageID = 0
            }

            try await storageManager.writePage(&page)
            await bufferPoolManager.updatePage(page)
        }
    }

    // MARK: - DB Metadata Persistence (page 0 record)

    /// Save freeListHead and indexRegistryPageID as a record on page 0
    private func saveDBMetadata() async throws {
        let meta = DBMetadata(freeListHead: freeListHead, indexRegistryPageID: indexRegistryPageID)
        let data = try JSONEncoder().encode(meta)
        let record = Record(id: 1, data: data)

        var metaPage = try await storageManager.readPage(pageID: PantryConstants.SYSTEM_PAGE_DB_METADATA)
        metaPage.records = [record]
        metaPage.recordCount = 1
        metaPage.freeSpaceOffset = PantryConstants.PAGE_SIZE - record.serialize().count
        try await storageManager.writePage(&metaPage)
        await bufferPoolManager.updatePage(metaPage)
    }

    private func saveFreeListHead() async throws {
        try await saveDBMetadata()
    }

    // MARK: - Checkpoint

    /// Flush all dirty pages and truncate the WAL
    public func checkpoint() async throws {
        try await transactionManager.createCheckpoint()
    }

    // MARK: - Lifecycle

    public func close() async throws {
        var firstError: Error?

        do { try await bufferPoolManager.flushAllDirtyPages() } catch { if firstError == nil { firstError = error } }
        do { try await tableRegistry.save() } catch { if firstError == nil { firstError = error } }
        // Checkpoint WAL (flush + truncate) — ignore failure if active txns exist
        do { try await transactionManager.createCheckpoint() } catch { /* checkpoint is best-effort on close */ }
        do { try await logManager.close() } catch { if firstError == nil { firstError = error } }
        do { try await storageManager.close() } catch { if firstError == nil { firstError = error } }

        if let error = firstError { throw error }
    }

    public func getBufferPoolStats() async -> BufferPoolStats {
        await bufferPoolManager.getStats()
    }
}

// Extension on TransactionManager to wire closures
extension TransactionManager {
    public func setPageFlusher(_ flusher: @escaping @Sendable (Set<Int>) async throws -> Void) {
        self.pageFlusher = flusher
    }

    public func setPageInvalidator(_ invalidator: @escaping @Sendable (Set<Int>) async -> Void) {
        self.pageInvalidator = invalidator
    }

    public func setAllPageFlusher(_ flusher: @escaping @Sendable () async throws -> Void) {
        self.allPageFlusher = flusher
    }
}
