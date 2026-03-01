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

    /// Page ID storing the free space bitmap (0 = not allocated)
    private var freeSpaceBitmapPageID: Int = 0

    /// Persisted free space bitmap for O(1) page selection
    private let freeSpaceBitmap = FreeSpaceBitmap()

    /// Set of system/reserved page IDs that should not be used for data
    private var systemPageIDs: Set<Int> = [0, 1]

    public init(databasePath: String, bufferPoolCapacity: Int = 1000, encryptionProvider: EncryptionProvider? = nil) async throws {
        let sm = try StorageManager(databasePath: databasePath, encryptionProvider: encryptionProvider)
        self.storageManager = sm
        self.bufferPoolManager = BufferPoolManager(capacity: bufferPoolCapacity, storageManager: sm)
        self.logManager = try await WriteAheadLog(databasePath: databasePath, storageManager: sm, encryptionProvider: encryptionProvider)
        self.transactionManager = try await TransactionManager(logManager: logManager)
        self.tableRegistry = TableRegistry(storageManager: sm, bufferPool: bufferPoolManager)

        // Wire page flusher to break circular reference
        await transactionManager.setPageFlusher { [bufferPoolManager, storageManager] modifiedPages in
            for pageID in modifiedPages {
                if let page = bufferPoolManager.getCachedPage(pageID: pageID) {
                    var pageToWrite = page
                    try await storageManager.writePage(&pageToWrite)
                    bufferPoolManager.clearDirtyFlag(pageID: pageID)
                }
            }
        }

        // Wire page invalidator so rollback evicts stale pages from buffer pool
        await transactionManager.setPageInvalidator { [bufferPoolManager] modifiedPages in
            for pageID in modifiedPages {
                bufferPoolManager.evictPage(pageID: pageID)
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
            freeSpaceBitmapPageID = meta.freeSpaceBitmapPageID
        }

        // Load or initialize the free space bitmap
        if freeSpaceBitmapPageID != 0 {
            try await freeSpaceBitmap.load(storageManager: sm, bitmapPageID: freeSpaceBitmapPageID)
            systemPageIDs.insert(freeSpaceBitmapPageID)
        } else {
            // One-time migration: scan all data pages to populate bitmap
            try await populateBitmapFromPages()
        }

        if indexRegistryPageID != 0 {
            systemPageIDs.insert(indexRegistryPageID)
        }
    }

    // MARK: - Page Operations

    public func getPage(pageID: Int, transactionContext: TransactionContext? = nil) async throws -> DatabasePage {
        if let cachedPage = bufferPoolManager.getCachedPage(pageID: pageID) {
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
        var beforePageData: Data? = nil

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
                    beforePageData = beforePage.data
                    try await logManager.logPageBeforeImage(txID: txContext.transactionID, page: beforePage)
                }
                await txContext.recordBeforeImage(pageID: page.pageID)
            }
        }

        // Serialize before caching so the data buffer is current
        var serializedPage = page
        try serializedPage.saveRecords()

        bufferPoolManager.updatePage(serializedPage)
        bufferPoolManager.markDirty(pageID: page.pageID)

        if let txContext = transactionContext {
            // Log after-image: use delta if we have the before-image data
            if let beforeData = beforePageData {
                try await logManager.logPageDelta(
                    txID: txContext.transactionID,
                    pageID: serializedPage.pageID,
                    oldData: beforeData,
                    newData: serializedPage.data,
                    type: .pageAfterDelta
                )
            } else {
                try await logManager.logPageAfterImage(txID: txContext.transactionID, page: serializedPage)
            }
        }

        if transactionContext == nil {
            try await storageManager.writePage(&serializedPage)
            bufferPoolManager.clearDirtyFlag(pageID: page.pageID)
        }

        // Update free space bitmap for data pages
        if !systemPageIDs.contains(page.pageID) {
            freeSpaceBitmap.setCategory(pageID: page.pageID, category: serializedPage.spaceCategory())
        }
    }

    // MARK: - Record Operations

    @discardableResult
    public func insertRecord(_ record: Record, tableName: String, row: Row? = nil, transactionContext: TransactionContext? = nil) async throws -> UInt64 {
        guard tableRegistry.getTableInfo(name: tableName) != nil else {
            throw PantryError.tableNotFound(name: tableName)
        }

        let serializedSize = record.serialize().count
        let maxInlineSize = PantryConstants.MAX_INLINE_RECORD_SIZE

        // Check if this record needs overflow pages
        if serializedSize > maxInlineSize {
            let insertedRecord = try await insertOverflowRecord(record, tableName: tableName, transactionContext: transactionContext)

            // Re-read table info to avoid overwriting changes from page allocation
            guard var updatedInfo = tableRegistry.getTableInfo(name: tableName) else {
                throw PantryError.tableNotFound(name: tableName)
            }
            updatedInfo.recordCount += 1
            tableRegistry.updateTableInfo(updatedInfo)

            if let row = row {
                try await indexHook?.updateIndexes(record: insertedRecord, row: row, tableName: tableName)
            }
            return insertedRecord.id
        }

        // Normal (non-overflow) insert path
        guard let currentInfo = tableRegistry.getTableInfo(name: tableName) else {
            throw PantryError.tableNotFound(name: tableName)
        }
        let targetPageID = try await findTargetPageForInsert(tableInfo: currentInfo, recordSize: serializedSize)
        var page = try await getPage(pageID: targetPageID, transactionContext: transactionContext)

        if !page.addRecord(record) {
            guard var freshInfo = tableRegistry.getTableInfo(name: tableName) else {
                throw PantryError.tableNotFound(name: tableName)
            }
            var newPage = try await createNewPageForTable(tableInfo: &freshInfo)
            if !newPage.addRecord(record) {
                throw PantryError.recordTooLarge(size: serializedSize)
            }

            try await savePage(newPage, transactionContext: transactionContext)
            tableRegistry.updateTableInfo(freshInfo)
        } else {
            try await savePage(page, transactionContext: transactionContext)
        }

        guard var updatedInfo = tableRegistry.getTableInfo(name: tableName) else {
            throw PantryError.tableNotFound(name: tableName)
        }
        updatedInfo.recordCount += 1
        tableRegistry.updateTableInfo(updatedInfo)

        if let row = row {
            try await indexHook?.updateIndexes(record: record, row: row, tableName: tableName)
        }

        return record.id
    }

    public func getRecord(id: UInt64, tableName: String, transactionContext: TransactionContext? = nil) async throws -> Record {
        // Try index lookup first
        if let pageID = try await indexHook?.lookupRecord(id: id, tableName: tableName) {
            let page = try await getPage(pageID: pageID, transactionContext: transactionContext)
            if var record = page.records.first(where: { $0.id == id }) {
                if record.isOverflow {
                    record = try await reassembleOverflowRecord(record)
                }
                return record
            }
        }

        var record = try await findRecordBySequentialScan(id: id, tableName: tableName, transactionContext: transactionContext)
        if record.isOverflow {
            record = try await reassembleOverflowRecord(record)
        }
        return record
    }

    public func deleteRecord(id: UInt64, tableName: String, transactionContext: TransactionContext? = nil) async throws {
        guard tableRegistry.getTableInfo(name: tableName) != nil else {
            throw PantryError.tableNotFound(name: tableName)
        }

        let pageID = try await findPageContainingRecord(id: id, tableName: tableName)
        var page = try await getPage(pageID: pageID, transactionContext: transactionContext)

        // Decode the row before deletion so indexes can be updated
        // Also check for overflow pages that need to be freed
        let deletedRow: Row?
        var overflowPageIDToFree: Int? = nil
        if let record = page.records.first(where: { $0.id == id }) {
            if record.isOverflow {
                // Reassemble the full record to get the row, then free overflow pages
                let fullRecord = try await reassembleOverflowRecord(record)
                deletedRow = Row.fromBytes(fullRecord.data)
                overflowPageIDToFree = record.overflowPageID
            } else {
                deletedRow = Row.fromBytes(record.data)
            }
        } else {
            deletedRow = nil
        }

        if !page.deleteRecord(id: id) {
            throw PantryError.recordNotFound(id: id)
        }

        try await savePage(page, transactionContext: transactionContext)

        // Free overflow pages
        if let overflowStart = overflowPageIDToFree {
            try await freeOverflowPages(startingAt: overflowStart)
        }

        if let row = deletedRow {
            try await indexHook?.removeFromIndexes(id: id, row: row, tableName: tableName)
        }

        guard var freshInfo = tableRegistry.getTableInfo(name: tableName) else {
            throw PantryError.tableNotFound(name: tableName)
        }
        if freshInfo.recordCount > 0 {
            freshInfo.recordCount -= 1
        }
        tableRegistry.updateTableInfo(freshInfo)
    }

    /// Scan all records in a table with sequential readahead
    public func scanTable(_ tableName: String, transactionContext: TransactionContext? = nil) async throws -> [(Record, Row)] {
        guard let tableInfo = tableRegistry.getTableInfo(name: tableName) else {
            throw PantryError.tableNotFound(name: tableName)
        }

        var results: [(Record, Row)] = []
        var currentPageID = tableInfo.firstPageID
        var visited: Set<Int> = []
        var prefetchTask: Task<Void, Never>?

        while currentPageID != 0 {
            guard visited.insert(currentPageID).inserted else {
                throw PantryError.corruptPage(pageID: currentPageID)
            }
            let page = try await getPage(pageID: currentPageID, transactionContext: transactionContext)

            // Issue readahead for next pages in the chain
            if page.nextPageID != 0 {
                let nextID = page.nextPageID
                let bp = bufferPoolManager
                prefetchTask?.cancel()
                prefetchTask = Task { await bp.prefetchPages([nextID]) }
            }

            for var record in page.records {
                if record.isOverflow {
                    record = try await reassembleOverflowRecord(record)
                }
                if let row = Row.fromBytes(record.data) {
                    results.append((record, row))
                }
            }
            currentPageID = page.nextPageID
        }

        prefetchTask?.cancel()
        return results
    }

    /// Scan all records in a table, returning raw record data without Row decoding.
    /// Callers are responsible for decoding rows from record.data.
    public func scanTableRaw(_ tableName: String, transactionContext: TransactionContext? = nil) async throws -> [(Record, Data)] {
        guard let tableInfo = tableRegistry.getTableInfo(name: tableName) else {
            throw PantryError.tableNotFound(name: tableName)
        }

        var results: [(Record, Data)] = []
        var currentPageID = tableInfo.firstPageID
        var visited: Set<Int> = []
        var prefetchTask: Task<Void, Never>?

        while currentPageID != 0 {
            guard visited.insert(currentPageID).inserted else {
                throw PantryError.corruptPage(pageID: currentPageID)
            }
            let page = try await getPage(pageID: currentPageID, transactionContext: transactionContext)

            if page.nextPageID != 0 {
                let nextID = page.nextPageID
                let bp = bufferPoolManager
                prefetchTask?.cancel()
                prefetchTask = Task { await bp.prefetchPages([nextID]) }
            }

            for var record in page.records {
                if record.isOverflow {
                    record = try await reassembleOverflowRecord(record)
                }
                results.append((record, record.data))
            }
            currentPageID = page.nextPageID
        }

        prefetchTask?.cancel()
        return results
    }

    /// Batch lookup: fetch full (Record, Row) pairs for a set of record IDs in a single table scan.
    /// Much more efficient than N individual getRecord calls.
    public func getRecordsByIDs(_ ids: Set<UInt64>, tableName: String, transactionContext: TransactionContext? = nil) async throws -> [(Record, Row)] {
        guard let tableInfo = tableRegistry.getTableInfo(name: tableName) else {
            throw PantryError.tableNotFound(name: tableName)
        }

        var remaining = ids
        var results: [(Record, Row)] = []
        var currentPageID = tableInfo.firstPageID
        var visited: Set<Int> = []

        while currentPageID != 0 && !remaining.isEmpty {
            guard visited.insert(currentPageID).inserted else {
                throw PantryError.corruptPage(pageID: currentPageID)
            }
            let page = try await getPage(pageID: currentPageID, transactionContext: transactionContext)
            for var record in page.records {
                guard remaining.contains(record.id) else { continue }
                if record.isOverflow {
                    record = try await reassembleOverflowRecord(record)
                }
                if let row = Row.fromBytes(record.data) {
                    results.append((record, row))
                    remaining.remove(record.id)
                }
            }
            currentPageID = page.nextPageID
        }

        return results
    }

    /// Walk the page chain for a table and return just the page IDs (lightweight).
    public func getPageChain(tableName: String, transactionContext: TransactionContext? = nil) async throws -> [Int] {
        guard let tableInfo = tableRegistry.getTableInfo(name: tableName) else {
            throw PantryError.tableNotFound(name: tableName)
        }

        var pageIDs: [Int] = []
        var currentPageID = tableInfo.firstPageID
        var visited: Set<Int> = []

        while currentPageID != 0 {
            guard visited.insert(currentPageID).inserted else {
                throw PantryError.corruptPage(pageID: currentPageID)
            }
            pageIDs.append(currentPageID)
            let page = try await getPage(pageID: currentPageID, transactionContext: transactionContext)
            currentPageID = page.nextPageID
        }

        return pageIDs
    }

    /// Stream records from a table page-at-a-time, yielding (Record, Row) pairs.
    public func scanTableStream(_ tableName: String) async throws -> AsyncStream<(Record, Row)> {
        guard let tableInfo = tableRegistry.getTableInfo(name: tableName) else {
            throw PantryError.tableNotFound(name: tableName)
        }

        let firstPageID = tableInfo.firstPageID
        let engine = self

        return AsyncStream { continuation in
            Task {
                var currentPageID = firstPageID
                var visited: Set<Int> = []

                while currentPageID != 0 {
                    guard visited.insert(currentPageID).inserted else { break }
                    do {
                        let page = try await engine.getPage(pageID: currentPageID)
                        for var record in page.records {
                            if record.isOverflow {
                                record = try await engine.reassembleOverflowRecord(record)
                            }
                            if let row = Row.fromBytes(record.data) {
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

    /// Collect per-column distinct value counts for a table by sampling.
    /// Updates the table's columnStats in the registry.
    public func analyzeTable(_ tableName: String) async throws {
        guard var info = tableRegistry.getTableInfo(name: tableName) else {
            throw PantryError.tableNotFound(name: tableName)
        }

        let columnNames = info.schema.columns.map { $0.name }
        var distinctSets: [String: Set<String>] = [:]
        for col in columnNames {
            distinctSets[col] = Set()
        }

        // Single scan to collect distinct values
        var currentPageID = info.firstPageID
        var visited: Set<Int> = []
        while currentPageID != 0 {
            guard visited.insert(currentPageID).inserted else { break }
            let page = try await getPage(pageID: currentPageID)
            for record in page.records {
                if let row = Row.fromBytes(record.data) {
                    for col in columnNames {
                        if let value = row.values[col], value != .null {
                            distinctSets[col]?.insert(value.statsKey)
                        }
                    }
                }
            }
            currentPageID = page.nextPageID
        }

        // Update stats
        for col in columnNames {
            let existing = info.columnStats[col] ?? ColumnStats()
            info.columnStats[col] = ColumnStats(
                distinctCount: distinctSets[col]?.count ?? 0,
                isIndexed: existing.isIndexed
            )
        }
        tableRegistry.updateTableInfo(info)
    }

    /// Get column statistics for query optimization (nonisolated — reads from Mutex-protected registry)
    public nonisolated func getColumnStats(_ tableName: String, column: String) -> ColumnStats? {
        tableRegistry.getTableInfo(name: tableName)?.columnStats[column]
    }

    /// Check if a table exists
    public func tableExists(_ name: String) async -> Bool {
        tableRegistry.getTableInfo(name: name) != nil
    }

    /// List all table names
    public func listTables() async -> [String] {
        tableRegistry.allTables().map { $0.name }
    }

    /// Get a table's schema
    public func getTableSchema(_ name: String) async -> PantryTableSchema? {
        tableRegistry.getTableInfo(name: name)?.schema
    }

    /// Update a table's schema in the registry (does not modify existing rows)
    public func updateTableSchema(_ name: String, schema: PantryTableSchema) async throws {
        guard var info = tableRegistry.getTableInfo(name: name) else {
            throw PantryError.tableNotFound(name: name)
        }
        info.schema = schema
        tableRegistry.updateTableInfo(info)
        try await tableRegistry.save()
    }

    // MARK: - Table Management

    public func createTable(_ schema: PantryTableSchema) async throws {
        // Create the first data page for this table
        let firstPage = try await storageManager.createNewPage()
        await bufferPoolManager.cachePage(firstPage)

        _ = try tableRegistry.registerTable(name: schema.name, schema: schema, firstPageID: firstPage.pageID)
        try await tableRegistry.save()
    }

    public func dropTable(_ name: String) async throws {
        guard let tableInfo = tableRegistry.getTableInfo(name: name) else {
            throw PantryError.tableNotFound(name: name)
        }

        // Remove from registry first
        tableRegistry.removeTable(name: name)
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
            freeSpaceBitmap.setCategory(pageID: currentPageID, category: .empty)

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

        // Determine minimum space category needed
        let minCategory: SpaceCategory
        if requiredSpace > 6144 {
            minCategory = .empty
        } else if requiredSpace > 2048 {
            minCategory = .available
        } else if requiredSpace >= 256 {
            minCategory = .low
        } else {
            minCategory = .low
        }

        // Walk the table's page chain, using bitmap to skip known-full pages
        var currentPageID = tableInfo.firstPageID
        var visited: Set<Int> = []

        while currentPageID != 0 {
            guard visited.insert(currentPageID).inserted else {
                throw PantryError.corruptPage(pageID: currentPageID)
            }

            // Check bitmap first to skip known-full pages without loading them
            let category = freeSpaceBitmap.getCategory(pageID: currentPageID)
            if category.rawValue >= minCategory.rawValue {
                let page = try await getPage(pageID: currentPageID)
                let actualCategory = page.spaceCategory()
                if actualCategory != category {
                    freeSpaceBitmap.setCategory(pageID: currentPageID, category: actualCategory)
                }
                if page.getFreeSpace() > requiredSpace {
                    return currentPageID
                }
            }

            // Need to load page to follow chain
            let page = try await getPage(pageID: currentPageID)
            currentPageID = page.nextPageID
        }

        // Need a new page
        var info = tableInfo
        let newPage = try await createNewPageForTable(tableInfo: &info)
        tableRegistry.updateTableInfo(info)
        freeSpaceBitmap.setCategory(pageID: newPage.pageID, category: .empty)
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
            bufferPoolManager.updatePage(reusedPage)
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
        guard let tableInfo = tableRegistry.getTableInfo(name: tableName) else {
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
        guard let tableInfo = tableRegistry.getTableInfo(name: tableName) else {
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

    // MARK: - Overflow Record Helpers

    /// Insert a record that is too large for a single page by splitting it across overflow pages.
    private func insertOverflowRecord(_ record: Record, tableName: String, transactionContext: TransactionContext? = nil) async throws -> Record {
        let fullData = record.data
        let overflowPayload = PantryConstants.OVERFLOW_PAGE_PAYLOAD

        // Calculate how much data fits inline on the primary page
        // Inline portion: record header (12) + overflow header (1 flag + 4 total len + 4 overflow pageID) + inline bytes
        // Must fit in page alongside header + slot
        let maxInlineData = PantryConstants.PAGE_SIZE - PantryConstants.PAGE_HEADER_SIZE - PantryConstants.SLOT_SIZE - 12 - 9
        let inlineSize = min(fullData.count, maxInlineData)
        let inlineData = fullData.prefix(inlineSize)
        var remainingData = fullData.suffix(from: fullData.startIndex + inlineSize)

        // Allocate overflow pages and chain them
        var overflowPages: [(pageID: Int, data: Data)] = []
        while !remainingData.isEmpty {
            let chunkSize = min(remainingData.count, overflowPayload)
            let chunk = remainingData.prefix(chunkSize)
            remainingData = remainingData.suffix(from: remainingData.startIndex + chunkSize)
            overflowPages.append((pageID: 0, data: Data(chunk)))
        }

        // Allocate pages for overflow chain
        for i in 0..<overflowPages.count {
            let newPage: DatabasePage
            if freeListHead != 0 {
                let freePage = try await storageManager.readPage(pageID: freeListHead)
                freeListHead = freePage.nextPageID
                newPage = DatabasePage(pageID: freePage.pageID)
                try await saveFreeListHead()
            } else {
                newPage = try await storageManager.createNewPage()
            }
            await bufferPoolManager.cachePage(newPage)
            overflowPages[i].pageID = newPage.pageID
        }

        // Write overflow pages (chained via next-overflow-pageID stored in first 4 bytes of data area)
        for i in 0..<overflowPages.count {
            var page = DatabasePage(pageID: overflowPages[i].pageID)
            page.pageFlags = [.overflow]

            // Build overflow page data: [4B next overflow pageID][payload bytes]
            let nextOverflowPageID: Int32 = (i + 1 < overflowPages.count) ? Int32(overflowPages[i + 1].pageID) : 0
            var pageData = Data(count: PantryConstants.PAGE_SIZE)

            // Write page header
            var pos = 0
            withUnsafeBytes(of: page.pageID) { pageData.replaceSubrange(pos..<pos+8, with: $0) }
            pos += 8
            withUnsafeBytes(of: 0 as Int) { pageData.replaceSubrange(pos..<pos+8, with: $0) } // nextPageID = 0 (not a table chain page)
            pos += 8
            var rc = Int32(0)
            withUnsafeBytes(of: &rc) { pageData.replaceSubrange(pos..<pos+4, with: $0) }
            pos += 4
            var fso = Int32(PantryConstants.PAGE_SIZE)
            withUnsafeBytes(of: &fso) { pageData.replaceSubrange(pos..<pos+4, with: $0) }
            pos += 4
            withUnsafeBytes(of: page.pageFlags.rawValue) { pageData.replaceSubrange(pos..<pos+4, with: $0) }
            pos += 4  // pos = PAGE_HEADER_SIZE (28)

            // Write next overflow page ID pointer (4 bytes after header)
            var nextID = nextOverflowPageID
            withUnsafeBytes(of: &nextID) { pageData.replaceSubrange(pos..<pos+4, with: $0) }
            pos += 4

            // Write continuation data
            let chunk = overflowPages[i].data
            pageData.replaceSubrange(pos..<pos+chunk.count, with: chunk)

            page.data = pageData
            try await storageManager.writePage(&page)
            bufferPoolManager.updatePage(page)
            freeSpaceBitmap.setCategory(pageID: page.pageID, category: .full)
        }

        // Create the inline record pointing to the first overflow page
        let firstOverflowPageID = overflowPages.first!.pageID
        let inlineRecord = Record(id: record.id, data: Data(inlineData), overflowPageID: firstOverflowPageID)

        // Insert the inline record on a normal table page
        guard let currentInfo = tableRegistry.getTableInfo(name: tableName) else {
            throw PantryError.tableNotFound(name: tableName)
        }
        let serializedSize = inlineRecord.serialize().count
        let targetPageID = try await findTargetPageForInsert(tableInfo: currentInfo, recordSize: serializedSize)
        var page = try await getPage(pageID: targetPageID, transactionContext: transactionContext)

        if !page.addRecord(inlineRecord) {
            guard var freshInfo = tableRegistry.getTableInfo(name: tableName) else {
                throw PantryError.tableNotFound(name: tableName)
            }
            var newPage = try await createNewPageForTable(tableInfo: &freshInfo)
            if !newPage.addRecord(inlineRecord) {
                throw PantryError.recordTooLarge(size: serializedSize)
            }
            try await savePage(newPage, transactionContext: transactionContext)
            tableRegistry.updateTableInfo(freshInfo)
        } else {
            try await savePage(page, transactionContext: transactionContext)
        }

        // Return a record with the full data for index updates
        return Record(id: record.id, data: fullData, overflowPageID: firstOverflowPageID)
    }

    /// Follow the overflow page chain to reassemble a full record from its inline portion and overflow pages.
    private func reassembleOverflowRecord(_ record: Record) async throws -> Record {
        guard let firstOverflowPageID = record.overflowPageID else {
            return record
        }

        var fullData = Data(record.data)
        var currentOverflowPageID = firstOverflowPageID
        var visited: Set<Int> = []

        while currentOverflowPageID != 0 {
            guard visited.insert(currentOverflowPageID).inserted else {
                throw PantryError.corruptPage(pageID: currentOverflowPageID)
            }

            let page = try await storageManager.readPage(pageID: currentOverflowPageID)
            let headerSize = PantryConstants.PAGE_HEADER_SIZE

            // Read next overflow page ID (4 bytes after header)
            let nextOverflowPageID = Int(page.data.withUnsafeBytes {
                $0.loadUnaligned(fromByteOffset: headerSize, as: Int32.self)
            })

            // Read continuation data (after header + 4B pointer)
            let dataStart = headerSize + 4
            let dataEnd = PantryConstants.PAGE_SIZE
            // Trim trailing zeros to get actual data length
            var actualEnd = dataEnd
            while actualEnd > dataStart && page.data[actualEnd - 1] == 0 {
                actualEnd -= 1
            }
            if actualEnd > dataStart {
                fullData.append(page.data.subdata(in: dataStart..<actualEnd))
            }

            currentOverflowPageID = nextOverflowPageID
        }

        return Record(id: record.id, data: fullData)
    }

    /// Free all overflow pages in a chain, pushing them onto the free list.
    private func freeOverflowPages(startingAt firstPageID: Int) async throws {
        var currentPageID = firstPageID
        var visited: Set<Int> = []

        while currentPageID != 0 {
            guard visited.insert(currentPageID).inserted else { break }

            let page = try await storageManager.readPage(pageID: currentPageID)
            let headerSize = PantryConstants.PAGE_HEADER_SIZE
            let nextOverflowPageID = Int(page.data.withUnsafeBytes {
                $0.loadUnaligned(fromByteOffset: headerSize, as: Int32.self)
            })

            // Push this page onto the free list
            var freedPage = DatabasePage(pageID: currentPageID)
            freedPage.nextPageID = freeListHead
            try freedPage.saveRecords()
            try await storageManager.writePage(&freedPage)
            bufferPoolManager.updatePage(freedPage)
            freeListHead = currentPageID
            freeSpaceBitmap.setCategory(pageID: currentPageID, category: .empty)

            currentPageID = nextOverflowPageID
        }

        try await saveFreeListHead()
    }

    // MARK: - Free Space Bitmap Initialization

    /// One-time migration: scan all data pages to populate the bitmap
    private func populateBitmapFromPages() async throws {
        let totalPages = try await storageManager.totalPageCount()
        for pageID in 0..<totalPages {
            guard !systemPageIDs.contains(pageID) else { continue }
            let page = try await storageManager.readPage(pageID: pageID)
            if page.isSystemPage { continue }
            freeSpaceBitmap.setCategory(pageID: pageID, category: page.spaceCategory())
        }
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
            bufferPoolManager.updatePage(page)
        }
    }

    // MARK: - DB Metadata Persistence (page 0 record)

    /// Save freeListHead, indexRegistryPageID, and freeSpaceBitmapPageID as a record on page 0
    private func saveDBMetadata() async throws {
        let meta = DBMetadata(freeListHead: freeListHead, indexRegistryPageID: indexRegistryPageID, freeSpaceBitmapPageID: freeSpaceBitmapPageID)
        let data = try JSONEncoder().encode(meta)
        let record = Record(id: 1, data: data)

        var metaPage = try await storageManager.readPage(pageID: PantryConstants.SYSTEM_PAGE_DB_METADATA)
        metaPage.records = [record]
        metaPage.recordCount = 1
        metaPage.freeSpaceOffset = PantryConstants.PAGE_SIZE - record.serialize().count
        try await storageManager.writePage(&metaPage)
        bufferPoolManager.updatePage(metaPage)
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
        // Save free space bitmap
        do {
            let newPageID = try await freeSpaceBitmap.save(storageManager: storageManager, bufferPool: bufferPoolManager, existingPageID: freeSpaceBitmapPageID)
            if newPageID != freeSpaceBitmapPageID {
                freeSpaceBitmapPageID = newPageID
                systemPageIDs.insert(newPageID)
                try await saveDBMetadata()
            }
        } catch { if firstError == nil { firstError = error } }
        // Checkpoint WAL (flush + truncate) — ignore failure if active txns exist
        do { try await transactionManager.createCheckpoint() } catch { /* checkpoint is best-effort on close */ }
        do { try await logManager.close() } catch { if firstError == nil { firstError = error } }
        do { try await storageManager.close() } catch { if firstError == nil { firstError = error } }

        if let error = firstError { throw error }
    }

    public func getBufferPoolStats() async -> BufferPoolStats {
        bufferPoolManager.getStats()
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
