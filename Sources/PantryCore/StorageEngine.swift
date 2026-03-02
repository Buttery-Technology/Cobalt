import Foundation

/// Protocol that PantryIndex implements to provide index operations to the storage engine
public protocol IndexHook: Sendable {
    func lookupRecord(id: UInt64, tableName: String) async throws -> Int?
    func updateIndexes(record: Record, row: Row, tableName: String) async throws
    func removeFromIndexes(id: UInt64, row: Row, tableName: String) async throws
    func removeFromIndexesBatch(records: [(id: UInt64, row: Row)], tableName: String) async throws
    func removeFromIndexesBatchRaw(records: [(id: UInt64, data: Data)], tableName: String, schema: PantryTableSchema) async throws
}

/// Main storage engine coordinating pages, buffer pool, WAL, transactions, and table registry
public final class StorageEngine: @unchecked Sendable {
    public let bufferPoolManager: BufferPoolManager
    public let storageManager: StorageManager
    public let transactionManager: TransactionManager
    public let tableRegistry: TableRegistry
    private let logManager: WriteAheadLog

    /// Persisted free space bitmap for O(1) page selection
    private let freeSpaceBitmap = FreeSpaceBitmap()
    private let extentSize = 8

    private struct MutableState {
        var indexHook: (any IndexHook)?
        var freeListHead: Int = 0
        var indexRegistryPageID: Int = 0
        var freeSpaceBitmapPageID: Int = 0
        var checkpointLSN: UInt64 = 0
        var extentReserve: [String: [DatabasePage]] = [:]
        var autoCheckpointThreshold: Int = 0
        var systemPageIDs: Set<Int> = [0, 1]
        var ridPageMap: [String: [UInt64: Int]] = [:]
    }
    private let _state: PantryLock<MutableState>

    public func setAutoCheckpointThreshold(_ threshold: Int) {
        _state.withLock { $0.autoCheckpointThreshold = threshold }
    }

    public func setIndexHook(_ hook: any IndexHook) {
        _state.withLock { $0.indexHook = hook }
    }

    public init(databasePath: String, bufferPoolCapacity: Int = 1000, encryptionProvider: EncryptionProvider? = nil, bufferPoolStripeCount: Int = 8, bgWriterIntervalMs: Int = 100) async throws {
        let sm = try StorageManager(databasePath: databasePath, encryptionProvider: encryptionProvider)
        self.storageManager = sm
        self.bufferPoolManager = BufferPoolManager(capacity: bufferPoolCapacity, storageManager: sm, stripeCount: bufferPoolStripeCount, bgWriterConfig: BackgroundWriterConfig(intervalMilliseconds: bgWriterIntervalMs))
        self.logManager = try await WriteAheadLog(databasePath: databasePath, storageManager: sm, encryptionProvider: encryptionProvider)
        self.transactionManager = try await TransactionManager(logManager: logManager)
        self.tableRegistry = TableRegistry(storageManager: sm, bufferPool: bufferPoolManager)
        self._state = PantryLock(MutableState())

        // Wire page flusher to break circular reference
        await transactionManager.setPageFlusher { [bufferPoolManager, storageManager] modifiedPages in
            for pageID in modifiedPages {
                if let page = bufferPoolManager.getCachedPage(pageID: pageID) {
                    var pageToWrite = page
                    try await storageManager.writePage(&pageToWrite, alreadySerialized: true)
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
            _state.withLock { s in
                s.freeListHead = meta.freeListHead
                s.indexRegistryPageID = meta.indexRegistryPageID
                s.freeSpaceBitmapPageID = meta.freeSpaceBitmapPageID
                s.checkpointLSN = meta.checkpointLSN
            }
            // Pass checkpoint LSN to WAL so recovery skips already-checkpointed records
            await logManager.setCheckpointLSN(_state.withLock { $0.checkpointLSN })
        }

        // Load or initialize the free space bitmap
        let bitmapPageID = _state.withLock { $0.freeSpaceBitmapPageID }
        if bitmapPageID != 0 {
            try await freeSpaceBitmap.load(storageManager: sm, bitmapPageID: bitmapPageID)
            _state.withLock { $0.systemPageIDs.insert(bitmapPageID) }
        } else {
            // One-time migration: scan all data pages to populate bitmap
            try await populateBitmapFromPages()
        }

        if _state.withLock({ $0.indexRegistryPageID }) != 0 {
            _state.withLock { $0.systemPageIDs.insert($0.indexRegistryPageID) }
        }

        // Start background dirty page writer
        bufferPoolManager.startBackgroundWriter()
    }

    // MARK: - Page Operations

    public func getPage(pageID: Int, transactionContext: TransactionContext? = nil) async throws -> DatabasePage {
        if let cachedPage = bufferPoolManager.getCachedPage(pageID: pageID) {
            if let txContext = transactionContext {
                txContext.recordAccess(pageID: pageID, isWrite: false)
            }
            return cachedPage
        }

        let loadedPage = try await storageManager.readPage(pageID: pageID)
        await bufferPoolManager.cachePage(loadedPage)

        if let txContext = transactionContext {
            txContext.recordAccess(pageID: pageID, isWrite: false)
        }
        return loadedPage
    }

    /// Non-actor read path: reads from buffer pool or storage manager directly.
    /// No actor hop needed — both bufferPoolManager and storageManager are Sendable with internal locking.
    /// Use for read-only parallel scans where transaction tracking is not needed.
    public func getPageConcurrent(pageID: Int) async throws -> DatabasePage {
        if let cachedPage = bufferPoolManager.getCachedPage(pageID: pageID) {
            return cachedPage
        }
        // Read directly from storage manager (pread is thread-safe, no actor hop needed)
        let page = try storageManager.readPage(pageID: pageID)
        await bufferPoolManager.cachePage(page)
        return page
    }

    /// Batch read pages: checks cache, batch-reads misses in contiguous I/O.
    /// Nonisolated for parallel scan use.
    public func getPagesConcurrent(pageIDs: [Int]) async throws -> [DatabasePage] {
        try await bufferPoolManager.getPages(pageIDs: pageIDs)
    }

    public func savePage(_ page: DatabasePage, transactionContext: TransactionContext? = nil, skipAfterImage: Bool = false) async throws {
        var beforePageData: Data? = nil

        if let txContext = transactionContext {
            txContext.markModified(pageID: page.pageID)
            txContext.recordAccess(pageID: page.pageID, isWrite: true)

            // Capture before-image for WAL rollback (only once per page per transaction)
            let alreadyLogged = txContext.hasBeforeImage(pageID: page.pageID)
            if !alreadyLogged {
                // Try buffer pool first (avoids disk I/O on hot path), fall back to disk
                if let cachedBefore = bufferPoolManager.getCachedPage(pageID: page.pageID) {
                    beforePageData = cachedBefore.data
                    try await logManager.logPageBeforeImage(txID: txContext.transactionID, page: cachedBefore)
                } else {
                    let totalPages = try await storageManager.totalPageCount()
                    if page.pageID < totalPages {
                        let beforePage = try await storageManager.readPage(pageID: page.pageID)
                        beforePageData = beforePage.data
                        try await logManager.logPageBeforeImage(txID: txContext.transactionID, page: beforePage)
                    }
                }
                txContext.recordBeforeImage(pageID: page.pageID)
            }
        }

        // Serialize before caching so the data buffer is current
        var serializedPage = page
        try serializedPage.saveRecords()

        bufferPoolManager.updatePage(serializedPage)
        bufferPoolManager.markDirty(pageID: page.pageID)

        if let txContext = transactionContext, !skipAfterImage {
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
            try await storageManager.writePage(&serializedPage, alreadySerialized: true)
            bufferPoolManager.clearDirtyFlag(pageID: page.pageID)
        }

        // Update free space bitmap for data pages
        if !_state.withLock({ $0.systemPageIDs.contains(page.pageID) }) {
            freeSpaceBitmap.setCategory(pageID: page.pageID, category: serializedPage.spaceCategory())
        }
    }

    /// Save a page without flushing to disk — only serializes and marks dirty in buffer pool.
    /// Call `flushDirtyPages` after a batch of deferred saves to write them all at once.
    public func savePageDeferred(_ page: DatabasePage) async throws {
        var serializedPage = page
        // Fast path: if all modifications were same-size patches, data buffer is already correct
        if !serializedPage.allPatched {
            // Try single-record patch, else full serialization
            if !serializedPage.saveRecordPatch() {
                try serializedPage.saveRecords()
            }
        }
        serializedPage.allPatched = false
        serializedPage.patchInvalidated = false
        serializedPage.lastPatchIndex = nil
        bufferPoolManager.updatePage(serializedPage)
        bufferPoolManager.markDirty(pageID: page.pageID)
        if !_state.withLock({ $0.systemPageIDs.contains(page.pageID) }) {
            freeSpaceBitmap.setCategory(pageID: page.pageID, category: serializedPage.spaceCategory())
        }
    }

    /// Batch save: serializes and marks dirty multiple pages at once.
    /// Groups buffer pool operations by stripe for fewer lock acquisitions.
    public func savePagesDeferred(_ pages: [DatabasePage]) throws {
        guard !pages.isEmpty else { return }
        var serializedPages = [DatabasePage]()
        serializedPages.reserveCapacity(pages.count)
        for var page in pages {
            if !page.allPatched {
                if !page.saveRecordPatch() {
                    try page.saveRecords()
                }
            }
            page.allPatched = false
            page.patchInvalidated = false
            page.lastPatchIndex = nil
            serializedPages.append(page)
        }
        // Batch update + mark dirty in buffer pool (groups by stripe internally)
        for page in serializedPages {
            bufferPoolManager.updatePage(page)
            bufferPoolManager.markDirty(pageID: page.pageID)
            if !_state.withLock({ $0.systemPageIDs.contains(page.pageID) }) {
                freeSpaceBitmap.setCategory(pageID: page.pageID, category: page.spaceCategory())
            }
        }
    }

    /// Flush specific dirty pages to disk.
    public func flushDirtyPages(_ pageIDs: Set<Int>) async throws {
        for pageID in pageIDs {
            if let page = bufferPoolManager.getCachedPage(pageID: pageID) {
                var writablePage = page
                try await storageManager.writePage(&writablePage, alreadySerialized: true)
                bufferPoolManager.clearDirtyFlag(pageID: pageID)
            }
        }
    }

    // MARK: - Record Operations

    @discardableResult
    public func insertRecord(_ record: Record, tableName: String, row: Row? = nil, transactionContext: TransactionContext? = nil) async throws -> UInt64 {
        guard tableRegistry.getTableInfo(name: tableName) != nil else {
            throw PantryError.tableNotFound(name: tableName)
        }

        let serializedSize = record.serializedSize
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
                let hook = _state.withLock { $0.indexHook }
                try await hook?.updateIndexes(record: insertedRecord, row: row, tableName: tableName)
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

            // Use record-level WAL entry instead of full page after-image
            if let txContext = transactionContext {
                try await logManager.logRecordInsert(txID: txContext.transactionID, pageID: newPage.pageID, recordID: record.id, data: record.data)
                try await savePage(newPage, transactionContext: transactionContext, skipAfterImage: true)
            } else {
                try await savePage(newPage, transactionContext: transactionContext)
            }
            tableRegistry.updateTableInfo(freshInfo)
        } else {
            if let txContext = transactionContext {
                try await logManager.logRecordInsert(txID: txContext.transactionID, pageID: page.pageID, recordID: record.id, data: record.data)
                try await savePage(page, transactionContext: transactionContext, skipAfterImage: true)
            } else {
                try await savePage(page, transactionContext: transactionContext)
            }
        }

        guard var updatedInfo = tableRegistry.getTableInfo(name: tableName) else {
            throw PantryError.tableNotFound(name: tableName)
        }
        updatedInfo.recordCount += 1
        tableRegistry.updateTableInfo(updatedInfo)

        // Update RID-to-page cache
        _state.withLock { $0.ridPageMap[tableName, default: [:]][record.id] = page.pageID }

        if let row = row {
            let hook = _state.withLock { $0.indexHook }
            try await hook?.updateIndexes(record: record, row: row, tableName: tableName)
        }

        return record.id
    }

    /// Insert a record without triggering index updates — caller is responsible for indexing.
    /// Used by bulk insert to defer index updates until all records are inserted.
    public func insertRecordSkipIndex(_ record: Record, tableName: String, transactionContext: TransactionContext? = nil) async throws -> UInt64 {
        guard tableRegistry.getTableInfo(name: tableName) != nil else {
            throw PantryError.tableNotFound(name: tableName)
        }

        let serializedSize = record.serializedSize
        let maxInlineSize = PantryConstants.MAX_INLINE_RECORD_SIZE

        if serializedSize > maxInlineSize {
            let insertedRecord = try await insertOverflowRecord(record, tableName: tableName, transactionContext: transactionContext)
            guard var updatedInfo = tableRegistry.getTableInfo(name: tableName) else {
                throw PantryError.tableNotFound(name: tableName)
            }
            updatedInfo.recordCount += 1
            tableRegistry.updateTableInfo(updatedInfo)
            return insertedRecord.id
        }

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
            if let txContext = transactionContext {
                try await logManager.logRecordInsert(txID: txContext.transactionID, pageID: newPage.pageID, recordID: record.id, data: record.data)
                try await savePage(newPage, transactionContext: transactionContext, skipAfterImage: true)
            } else {
                try await savePage(newPage, transactionContext: transactionContext)
            }
            tableRegistry.updateTableInfo(freshInfo)
        } else {
            if let txContext = transactionContext {
                try await logManager.logRecordInsert(txID: txContext.transactionID, pageID: page.pageID, recordID: record.id, data: record.data)
                try await savePage(page, transactionContext: transactionContext, skipAfterImage: true)
            } else {
                try await savePage(page, transactionContext: transactionContext)
            }
        }

        guard var updatedInfo = tableRegistry.getTableInfo(name: tableName) else {
            throw PantryError.tableNotFound(name: tableName)
        }
        updatedInfo.recordCount += 1
        tableRegistry.updateTableInfo(updatedInfo)

        return record.id
    }

    /// Bulk insert records with batched page writes: fills pages to capacity before writing.
    /// Returns the number of records inserted. Much faster than per-record insertRecordSkipIndex.
    public func bulkInsertRecordsBatched(_ records: [Record], tableName: String, transactionContext: TransactionContext? = nil) async throws -> Int {
        guard !records.isEmpty else { return 0 }
        guard var tableInfo = tableRegistry.getTableInfo(name: tableName) else {
            throw PantryError.tableNotFound(name: tableName)
        }

        let maxInlineSize = PantryConstants.MAX_INLINE_RECORD_SIZE
        var inserted = 0

        // Load the last page in the chain (likely has free space)
        var currentPage: DatabasePage
        let lastPageID = tableInfo.pageList?.last ?? tableInfo.firstPageID
        if lastPageID != 0 {
            currentPage = try await getPage(pageID: lastPageID, transactionContext: transactionContext)
        } else {
            currentPage = try await createNewPageForTable(tableInfo: &tableInfo)
        }
        var pageDirty = false

        for record in records {
            let serializedSize = record.serializedSize

            // Handle overflow records individually
            if serializedSize > maxInlineSize {
                // Flush current page first
                if pageDirty {
                    try await savePage(currentPage, transactionContext: transactionContext)
                    freeSpaceBitmap.setCategory(pageID: currentPage.pageID, category: currentPage.spaceCategory())
                    pageDirty = false
                }
                _ = try await insertOverflowRecord(record, tableName: tableName, transactionContext: transactionContext)
                // Re-read tableInfo (insertOverflowRecord may have allocated pages)
                if let refreshed = tableRegistry.getTableInfo(name: tableName) {
                    tableInfo = refreshed
                }
                inserted += 1
                continue
            }

            // Try to add to current page (pass pre-computed size to avoid double serialization)
            if currentPage.addRecord(record, knownSerializedSize: serializedSize) {
                pageDirty = true
                inserted += 1
                // Populate RID-to-page cache for fast lookups later
                _state.withLock { $0.ridPageMap[tableName, default: [:]][record.id] = currentPage.pageID }
            } else {
                // Page full — write it with skipAfterImage (before-image only for undo)
                if pageDirty {
                    if transactionContext != nil {
                        try await savePage(currentPage, transactionContext: transactionContext, skipAfterImage: true)
                    } else {
                        try await savePage(currentPage, transactionContext: transactionContext)
                    }
                    freeSpaceBitmap.setCategory(pageID: currentPage.pageID, category: currentPage.spaceCategory())
                }

                currentPage = try await createNewPageForTable(tableInfo: &tableInfo)
                if !currentPage.addRecord(record, knownSerializedSize: serializedSize) {
                    throw PantryError.recordTooLarge(size: serializedSize)
                }
                pageDirty = true
                inserted += 1
                _state.withLock { $0.ridPageMap[tableName, default: [:]][record.id] = currentPage.pageID }
            }
        }

        // Flush the last page
        if pageDirty {
            if transactionContext != nil {
                try await savePage(currentPage, transactionContext: transactionContext, skipAfterImage: true)
            } else {
                try await savePage(currentPage, transactionContext: transactionContext)
            }
            freeSpaceBitmap.setCategory(pageID: currentPage.pageID, category: currentPage.spaceCategory())
        }

        // Update record count once
        tableInfo.recordCount += inserted
        tableRegistry.updateTableInfo(tableInfo)

        return inserted
    }

    public func getRecord(id: UInt64, tableName: String, transactionContext: TransactionContext? = nil) async throws -> Record {
        // Try index lookup first
        let hook = _state.withLock { $0.indexHook }
        if let pageID = try await hook?.lookupRecord(id: id, tableName: tableName) {
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

    /// Delete a record, optionally skipping the page scan if pageID is known.
    public func deleteRecord(id: UInt64, tableName: String, transactionContext: TransactionContext? = nil, knownPageID: Int? = nil) async throws {
        guard tableRegistry.getTableInfo(name: tableName) != nil else {
            throw PantryError.tableNotFound(name: tableName)
        }

        let pageID: Int
        let foundPage: DatabasePage
        if let known = knownPageID {
            pageID = known
            foundPage = try await getPage(pageID: known, transactionContext: transactionContext)
        } else {
            (pageID, foundPage) = try await findPageContainingRecord(id: id, tableName: tableName)
        }
        var page = foundPage

        // Decode the row before deletion so indexes can be updated
        // Also check for overflow pages that need to be freed
        let deleteSchema = tableRegistry.getTableInfo(name: tableName)?.schema
        let deletedRow: Row?
        var overflowPageIDToFree: Int? = nil
        if let record = page.records.first(where: { $0.id == id }) {
            if record.isOverflow {
                // Reassemble the full record to get the row, then free overflow pages
                let fullRecord = try await reassembleOverflowRecord(record)
                deletedRow = Row.fromBytesAuto(fullRecord.data, schema: deleteSchema)
                overflowPageIDToFree = record.overflowPageID
            } else {
                deletedRow = Row.fromBytesAuto(record.data, schema: deleteSchema)
            }
        } else {
            deletedRow = nil
        }

        // Capture record data for WAL before deletion
        let deletedRecordData: Data?
        if transactionContext != nil, let record = page.records.first(where: { $0.id == id }) {
            deletedRecordData = record.data
        } else {
            deletedRecordData = nil
        }

        if !page.deleteRecord(id: id) {
            throw PantryError.recordNotFound(id: id)
        }

        if let txContext = transactionContext, let recordData = deletedRecordData {
            try await logManager.logRecordDelete(txID: txContext.transactionID, pageID: pageID, recordID: id, data: recordData)
            try await savePage(page, transactionContext: transactionContext, skipAfterImage: true)
        } else {
            try await savePage(page, transactionContext: transactionContext)
        }

        // Free overflow pages
        if let overflowStart = overflowPageIDToFree {
            try await freeOverflowPages(startingAt: overflowStart)
        }

        if let row = deletedRow {
            let hook = _state.withLock { $0.indexHook }
            try await hook?.removeFromIndexes(id: id, row: row, tableName: tableName)
        }

        // Remove from RID cache
        _state.withLock { $0.ridPageMap[tableName]?.removeValue(forKey: id) }

        guard var freshInfo = tableRegistry.getTableInfo(name: tableName) else {
            throw PantryError.tableNotFound(name: tableName)
        }
        if freshInfo.recordCount > 0 {
            freshInfo.recordCount -= 1
        }
        tableRegistry.updateTableInfo(freshInfo)
    }

    /// Batch-delete multiple records grouped by page. Loads each page once, removes
    /// all matching records, saves once. Much faster than N individual deleteRecord calls.
    /// - Parameter records: Array of (recordID, pageID, decodedRow) tuples.
    public func deleteRecordsBatch(_ records: [(id: UInt64, pageID: Int, row: Row?)], tableName: String, transactionContext: TransactionContext? = nil) async throws {
        guard !records.isEmpty else { return }

        // Group by pageID for single-load-per-page
        var byPage = [Int: [(id: UInt64, row: Row?)]]()
        for r in records {
            byPage[r.pageID, default: []].append((id: r.id, row: r.row))
        }

        let deleteSchema = tableRegistry.getTableInfo(name: tableName)?.schema
        var totalDeleted = 0
        var indexRemovals: [(id: UInt64, row: Row)] = []

        // Process pages in sorted order for sequential I/O
        for pageID in byPage.keys.sorted() {
            let group = byPage[pageID]!
            var page = try await getPage(pageID: pageID, transactionContext: transactionContext)
            var deletedFromPage = 0

            for (recordID, row) in group {
                // Check for overflow pages to free
                var overflowPageIDToFree: Int? = nil
                if let record = page.records.first(where: { $0.id == recordID }), record.isOverflow {
                    overflowPageIDToFree = record.overflowPageID
                }

                // WAL logging
                if let txContext = transactionContext,
                   let record = page.records.first(where: { $0.id == recordID }) {
                    try await logManager.logRecordDelete(txID: txContext.transactionID, pageID: pageID, recordID: recordID, data: record.data)
                }

                if page.deleteRecord(id: recordID) {
                    deletedFromPage += 1

                    if let overflowStart = overflowPageIDToFree {
                        try await freeOverflowPages(startingAt: overflowStart)
                    }

                    // Collect for batch index removal
                    if let row = row {
                        indexRemovals.append((id: recordID, row: row))
                    }

                    // Remove from RID cache
                    _state.withLock { $0.ridPageMap[tableName]?.removeValue(forKey: recordID) }
                }
            }

            if deletedFromPage > 0 {
                if transactionContext != nil {
                    try await savePage(page, transactionContext: transactionContext, skipAfterImage: true)
                } else {
                    try await savePageDeferred(page)
                }
                totalDeleted += deletedFromPage
            }
        }

        // Batch index removal — one pass per index instead of per record
        if !indexRemovals.isEmpty {
            let hook = _state.withLock { $0.indexHook }
            try await hook?.removeFromIndexesBatch(records: indexRemovals, tableName: tableName)
        }

        // Flush all dirty pages at once for non-transactional deletes
        if transactionContext == nil && totalDeleted > 0 {
            try await flushDirtyPages(Set(byPage.keys))
        }

        // Update registry once
        if totalDeleted > 0 {
            guard var freshInfo = tableRegistry.getTableInfo(name: tableName) else { return }
            freshInfo.recordCount = max(0, freshInfo.recordCount - totalDeleted)
            tableRegistry.updateTableInfo(freshInfo)
        }
    }

    /// Batch-delete using raw data — avoids full Row deserialization.
    /// Extracts only indexed column values via O(1) positional access.
    public func deleteRecordsBatchRaw(_ records: [(id: UInt64, pageID: Int, data: Data?)], tableName: String, transactionContext: TransactionContext? = nil) async throws {
        guard !records.isEmpty else { return }

        var byPage = [Int: [(id: UInt64, data: Data?)]]()
        for r in records {
            byPage[r.pageID, default: []].append((id: r.id, data: r.data))
        }

        let schema = tableRegistry.getTableInfo(name: tableName)?.schema
        var totalDeleted = 0
        var rawIndexRemovals: [(id: UInt64, data: Data)] = []

        for pageID in byPage.keys.sorted() {
            let group = byPage[pageID]!
            var page = try await getPage(pageID: pageID, transactionContext: transactionContext)
            var deletedFromPage = 0

            for (recordID, rawData) in group {
                var overflowPageIDToFree: Int? = nil
                if let record = page.records.first(where: { $0.id == recordID }), record.isOverflow {
                    overflowPageIDToFree = record.overflowPageID
                }

                if let txContext = transactionContext,
                   let record = page.records.first(where: { $0.id == recordID }) {
                    try await logManager.logRecordDelete(txID: txContext.transactionID, pageID: pageID, recordID: recordID, data: record.data)
                }

                if page.deleteRecord(id: recordID) {
                    deletedFromPage += 1
                    if let overflowStart = overflowPageIDToFree {
                        try await freeOverflowPages(startingAt: overflowStart)
                    }
                    if let data = rawData {
                        rawIndexRemovals.append((id: recordID, data: data))
                    }
                    _state.withLock { $0.ridPageMap[tableName]?.removeValue(forKey: recordID) }
                }
            }

            if deletedFromPage > 0 {
                if transactionContext != nil {
                    try await savePage(page, transactionContext: transactionContext, skipAfterImage: true)
                } else {
                    try await savePageDeferred(page)
                }
                totalDeleted += deletedFromPage
            }
        }

        if !rawIndexRemovals.isEmpty, let schema = schema {
            let hook = _state.withLock { $0.indexHook }
            try await hook?.removeFromIndexesBatchRaw(records: rawIndexRemovals, tableName: tableName, schema: schema)
        }

        if transactionContext == nil && totalDeleted > 0 {
            try await flushDirtyPages(Set(byPage.keys))
        }

        if totalDeleted > 0 {
            guard var freshInfo = tableRegistry.getTableInfo(name: tableName) else { return }
            freshInfo.recordCount = max(0, freshInfo.recordCount - totalDeleted)
            tableRegistry.updateTableInfo(freshInfo)
        }
    }

    /// Single-pass delete by RIDs: finds records, deletes from pages, collects raw data for index removal.
    /// Avoids the double page-load of getRecordDataByIDs + deleteRecordsBatchRaw.
    public func deleteByRIDs(_ ids: Set<UInt64>, tableName: String, transactionContext: TransactionContext? = nil) async throws -> (deleted: Int, rawData: [(id: UInt64, data: Data)]) {
        guard !ids.isEmpty else { return (0, []) }

        let schema = tableRegistry.getTableInfo(name: tableName)?.schema
        var remaining = ids
        var totalDeleted = 0
        var rawIndexRemovals: [(id: UInt64, data: Data)] = []
        rawIndexRemovals.reserveCapacity(ids.count)
        var modifiedPageIDs: [Int] = []
        // Collect modified pages for batch save (non-transactional)
        var deferredPages: [DatabasePage] = []

        // Group RIDs by page using the cache
        var idsByPage = [Int: [UInt64]]()
        var uncached = [UInt64]()
        let ridCache = _state.withLock { $0.ridPageMap[tableName] }
        if let cache = ridCache, !cache.isEmpty {
            for id in ids {
                if let pageID = cache[id] { idsByPage[pageID, default: []].append(id) }
                else { uncached.append(id) }
            }
        } else {
            uncached = Array(ids)
        }

        // Process cached pages — single load for both data extraction and deletion
        for pageID in idsByPage.keys.sorted() {
            let pageIds = idsByPage[pageID]!
            var page = try await getPage(pageID: pageID, transactionContext: transactionContext)
            let idSet = Set(pageIds)
            var deletedFromPage = 0

            for record in page.records where idSet.contains(record.id) {
                var data = record.data
                if record.isOverflow {
                    let reassembled = try await reassembleOverflowRecord(record)
                    data = reassembled.data
                }

                if let txContext = transactionContext {
                    try await logManager.logRecordDelete(txID: txContext.transactionID, pageID: pageID, recordID: record.id, data: data)
                }

                if page.deleteRecord(id: record.id) {
                    deletedFromPage += 1
                    if record.isOverflow, let overflowStart = record.overflowPageID {
                        try await freeOverflowPages(startingAt: overflowStart)
                    }
                    rawIndexRemovals.append((id: record.id, data: data))
                    _state.withLock { $0.ridPageMap[tableName]?.removeValue(forKey: record.id) }
                    remaining.remove(record.id)
                }
            }

            if deletedFromPage > 0 {
                if transactionContext != nil {
                    try await savePage(page, transactionContext: transactionContext, skipAfterImage: true)
                } else {
                    deferredPages.append(page)
                }
                totalDeleted += deletedFromPage
                modifiedPageIDs.append(pageID)
            }
        }

        // Process uncached RIDs — scan pages
        if !remaining.isEmpty {
            let pageList = tableRegistry.getTableInfo(name: tableName)?.pageList ?? []
            for currentPageID in pageList {
                guard !remaining.isEmpty else { break }
                var page = try await getPage(pageID: currentPageID, transactionContext: transactionContext)
                var deletedFromPage = 0

                for record in page.records where remaining.contains(record.id) {
                    var data = record.data
                    if record.isOverflow {
                        let reassembled = try await reassembleOverflowRecord(record)
                        data = reassembled.data
                    }
                    _state.withLock { $0.ridPageMap[tableName, default: [:]][record.id] = currentPageID }

                    if let txContext = transactionContext {
                        try await logManager.logRecordDelete(txID: txContext.transactionID, pageID: currentPageID, recordID: record.id, data: data)
                    }

                    if page.deleteRecord(id: record.id) {
                        deletedFromPage += 1
                        if record.isOverflow, let overflowStart = record.overflowPageID {
                            try await freeOverflowPages(startingAt: overflowStart)
                        }
                        rawIndexRemovals.append((id: record.id, data: data))
                        _state.withLock { $0.ridPageMap[tableName]?.removeValue(forKey: record.id) }
                        remaining.remove(record.id)
                    }
                }

                if deletedFromPage > 0 {
                    if transactionContext != nil {
                        try await savePage(page, transactionContext: transactionContext, skipAfterImage: true)
                    } else {
                        deferredPages.append(page)
                    }
                    totalDeleted += deletedFromPage
                    modifiedPageIDs.append(currentPageID)
                }
            }
        }

        // Batch save all deferred pages at once, then flush to disk
        if transactionContext == nil && !deferredPages.isEmpty {
            try savePagesDeferred(deferredPages)
            try await flushDirtyPages(Set(modifiedPageIDs))
        }

        if totalDeleted > 0 {
            guard var freshInfo = tableRegistry.getTableInfo(name: tableName) else { return (totalDeleted, rawIndexRemovals) }
            freshInfo.recordCount = max(0, freshInfo.recordCount - totalDeleted)
            tableRegistry.updateTableInfo(freshInfo)
        }

        return (totalDeleted, rawIndexRemovals)
    }

    /// Try to replace a record in-place on its current page. Returns true if successful.
    /// Falls back to false when: record not found, new data doesn't fit, or record is overflow.
    /// Pass `knownPageID` to skip the sequential scan for the record's page.
    public func replaceRecordInPlace(id: UInt64, newRecord: Record, tableName: String, transactionContext: TransactionContext? = nil, knownPageID: Int? = nil) async throws -> Bool {
        let pageID: Int
        let foundPage: DatabasePage
        if let known = knownPageID {
            pageID = known
            foundPage = try await getPage(pageID: known, transactionContext: transactionContext)
        } else {
            (pageID, foundPage) = try await findPageContainingRecord(id: id, tableName: tableName)
        }
        var page = foundPage

        // Don't attempt in-place for overflow records
        if let existing = page.records.first(where: { $0.id == id }), existing.isOverflow {
            return false
        }

        guard page.replaceRecord(id: id, with: newRecord) else {
            return false
        }

        // Fast path: if the record size didn't change, patch the data buffer directly
        // instead of full saveRecords() reserialization
        if page.saveRecordPatch() {
            bufferPoolManager.updatePage(page)
            bufferPoolManager.markDirty(pageID: pageID)
            if let txContext = transactionContext {
                try await logManager.logRecordDelete(txID: txContext.transactionID, pageID: pageID, recordID: id, data: newRecord.data)
                try await logManager.logRecordInsert(txID: txContext.transactionID, pageID: pageID, recordID: id, data: newRecord.data)
            } else {
                var writablePage = page
                try storageManager.writePage(&writablePage, alreadySerialized: true)
            }
        } else {
            if let txContext = transactionContext {
                try await logManager.logRecordDelete(txID: txContext.transactionID, pageID: pageID, recordID: id, data: newRecord.data)
                try await logManager.logRecordInsert(txID: txContext.transactionID, pageID: pageID, recordID: id, data: newRecord.data)
                try await savePage(page, transactionContext: transactionContext, skipAfterImage: true)
            } else {
                try await savePage(page, transactionContext: transactionContext)
            }
        }

        return true
    }

    /// Scan all records in a table with sequential readahead.
    /// Pass `neededColumns` to enable lazy overflow loading: if an overflow record's inline data
    /// contains all needed columns, the overflow pages are skipped entirely.
    public func scanTable(_ tableName: String, transactionContext: TransactionContext? = nil, neededColumns: Set<String>? = nil) async throws -> [(Record, Row)] {
        guard let tableInfo = tableRegistry.getTableInfo(name: tableName) else {
            throw PantryError.tableNotFound(name: tableName)
        }

        let scanSchema = tableInfo.schema
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
                    // Lazy overflow: try decoding needed columns from inline data first
                    if let needed = neededColumns,
                       let partialRow = Row.fromBytesPartial(record.data, neededColumns: needed) {
                        results.append((record, partialRow))
                        continue
                    }
                    record = try await reassembleOverflowRecord(record)
                }
                if let row = Row.fromBytesAuto(record.data, schema: scanSchema) {
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

    /// Batch lookup: fetch full (Record, Row) pairs for a set of record IDs.
    /// Uses RID-to-page cache for O(1) lookup when available, falls back to page scan.
    /// Pass `neededColumns` to enable lazy overflow loading.
    public func getRecordsByIDs(_ ids: Set<UInt64>, tableName: String, transactionContext: TransactionContext? = nil, neededColumns: Set<String>? = nil) async throws -> [(Record, Row)] {
        guard let tableInfo = tableRegistry.getTableInfo(name: tableName) else {
            throw PantryError.tableNotFound(name: tableName)
        }

        let idsSchema = tableInfo.schema
        var remaining = ids
        var results: [(Record, Row)] = []

        // Fast path: use RID-to-page cache for direct page lookups
        let ridCache1 = _state.withLock { $0.ridPageMap[tableName] }
        if let cache = ridCache1, !cache.isEmpty {
            // Group IDs by page for efficient batch loading
            var idsByPage = [Int: [UInt64]]()
            var uncached = [UInt64]()
            for id in ids {
                if let pageID = cache[id] {
                    idsByPage[pageID, default: []].append(id)
                } else {
                    uncached.append(id)
                }
            }

            for (pageID, pageIds) in idsByPage {
                let page = transactionContext == nil
                    ? try await getPageConcurrent(pageID: pageID)
                    : try await getPage(pageID: pageID, transactionContext: transactionContext)
                let idSet = Set(pageIds)
                for var record in page.records {
                    guard idSet.contains(record.id) else { continue }
                    if record.isOverflow {
                        if let needed = neededColumns,
                           let partialRow = Row.fromBytesPartial(record.data, neededColumns: needed) {
                            results.append((record, partialRow))
                            remaining.remove(record.id)
                            continue
                        }
                        record = try await reassembleOverflowRecord(record)
                    }
                    if let row = Row.fromBytesAuto(record.data, schema: idsSchema) {
                        results.append((record, row))
                        remaining.remove(record.id)
                    }
                }
            }

            // If all found, return early
            if remaining.isEmpty || (remaining.isSubset(of: Set(uncached)) && uncached.isEmpty) {
                return results
            }
            // Fall through for any remaining IDs not in cache
            remaining = remaining.intersection(Set(uncached))
            if remaining.isEmpty { return results }
        }

        // Fallback: scan pages to find remaining IDs (also populates the RID cache)
        let pageIDs = tableInfo.pageList ?? []
        if !pageIDs.isEmpty {
            for currentPageID in pageIDs {
                guard !remaining.isEmpty else { break }
                let page = transactionContext == nil
                    ? try await getPageConcurrent(pageID: currentPageID)
                    : try await getPage(pageID: currentPageID, transactionContext: transactionContext)
                for var record in page.records {
                    // Populate RID cache for all records we see
                    _state.withLock { $0.ridPageMap[tableName, default: [:]][record.id] = currentPageID }
                    guard remaining.contains(record.id) else { continue }
                    if record.isOverflow {
                        if let needed = neededColumns,
                           let partialRow = Row.fromBytesPartial(record.data, neededColumns: needed) {
                            results.append((record, partialRow))
                            remaining.remove(record.id)
                            continue
                        }
                        record = try await reassembleOverflowRecord(record)
                    }
                    if let row = Row.fromBytesAuto(record.data, schema: idsSchema) {
                        results.append((record, row))
                        remaining.remove(record.id)
                    }
                }
            }
        } else {
            var currentPageID = tableInfo.firstPageID
            var visited: Set<Int> = []
            while currentPageID != 0 && !remaining.isEmpty {
                guard visited.insert(currentPageID).inserted else {
                    throw PantryError.corruptPage(pageID: currentPageID)
                }
                let page = try await getPage(pageID: currentPageID, transactionContext: transactionContext)
                for var record in page.records {
                    _state.withLock { $0.ridPageMap[tableName, default: [:]][record.id] = currentPageID }
                    guard remaining.contains(record.id) else { continue }
                    if record.isOverflow {
                        if let needed = neededColumns,
                           let partialRow = Row.fromBytesPartial(record.data, neededColumns: needed) {
                            results.append((record, partialRow))
                            remaining.remove(record.id)
                            continue
                        }
                        record = try await reassembleOverflowRecord(record)
                    }
                    if let row = Row.fromBytesAuto(record.data, schema: idsSchema) {
                        results.append((record, row))
                        remaining.remove(record.id)
                    }
                }
                currentPageID = page.nextPageID
            }
        }

        return results
    }

    /// Fetch records by ordered RID list, returning results in the SAME order as input.
    /// Groups RIDs by page for efficient batch loading, then restores original order.
    public func getRecordsByIDsOrdered(_ orderedIDs: [UInt64], tableName: String, transactionContext: TransactionContext? = nil) async throws -> [Row] {
        guard !orderedIDs.isEmpty else { return [] }
        guard let tableInfo = tableRegistry.getTableInfo(name: tableName) else {
            throw PantryError.tableNotFound(name: tableName)
        }

        let schema = tableInfo.schema
        var result = [Row?](repeating: nil, count: orderedIDs.count)

        // Build RID → position(s) mapping and group by page using RID cache
        var ridToPositions = [UInt64: [Int]](minimumCapacity: orderedIDs.count)
        for (i, rid) in orderedIDs.enumerated() {
            ridToPositions[rid, default: []].append(i)
        }

        var idsByPage = [Int: [UInt64]]()
        var uncachedIDs = [UInt64]()
        let ridCache = _state.withLock { $0.ridPageMap[tableName] }
        if let cache = ridCache, !cache.isEmpty {
            for rid in orderedIDs {
                if let pageID = cache[rid] {
                    idsByPage[pageID, default: []].append(rid)
                } else {
                    uncachedIDs.append(rid)
                }
            }
        } else {
            uncachedIDs = orderedIDs
        }

        // Load cached pages
        for (pageID, pageRIDs) in idsByPage {
            let page = transactionContext == nil
                ? try await getPageConcurrent(pageID: pageID)
                : try await getPage(pageID: pageID, transactionContext: transactionContext)
            let idSet = Set(pageRIDs)
            for record in page.records where idSet.contains(record.id) {
                if let row = Row.fromBytesAuto(record.data, schema: schema),
                   let positions = ridToPositions[record.id] {
                    for pos in positions {
                        result[pos] = row
                    }
                }
            }
        }

        // Scan for uncached RIDs
        if !uncachedIDs.isEmpty {
            let uncachedSet = Set(uncachedIDs)
            var remaining = uncachedSet
            let pageList = tableInfo.pageList ?? []
            for currentPageID in pageList {
                guard !remaining.isEmpty else { break }
                let page = transactionContext == nil
                    ? try await getPageConcurrent(pageID: currentPageID)
                    : try await getPage(pageID: currentPageID, transactionContext: transactionContext)
                for record in page.records {
                    _state.withLock { $0.ridPageMap[tableName, default: [:]][record.id] = currentPageID }
                    guard remaining.contains(record.id) else { continue }
                    if let row = Row.fromBytesAuto(record.data, schema: schema),
                       let positions = ridToPositions[record.id] {
                        for pos in positions {
                            result[pos] = row
                        }
                    }
                    remaining.remove(record.id)
                }
            }
        }

        return result.compactMap { $0 }
    }

    /// Single-record fetch by RID: avoids Set overhead of getRecordsByIDs.
    public func getRecordByID(_ id: UInt64, tableName: String, transactionContext: TransactionContext? = nil) async throws -> Row? {
        let schema = getTableSchema(tableName)
        let cachedPageID: Int? = _state.withLock { $0.ridPageMap[tableName]?[id] }
        if let pageID = cachedPageID {
            let page = transactionContext == nil
                ? try await getPageConcurrent(pageID: pageID)
                : try await getPage(pageID: pageID, transactionContext: transactionContext)
            for record in page.records where record.id == id {
                return Row.fromBytesAuto(record.data, schema: schema)
            }
        }
        // Fallback to general path
        let records = try await getRecordsByIDs(Set([id]), tableName: tableName, transactionContext: transactionContext)
        return records.first?.1
    }

    /// Batch lookup returning (Record, Row, pageID) tuples for direct page access in DELETE/UPDATE.
    /// Uses cached page list and concurrent reads when available.
    public func getRecordsByIDsWithPages(_ ids: Set<UInt64>, tableName: String, transactionContext: TransactionContext? = nil, neededColumns: Set<String>? = nil) async throws -> [(Record, Row, Int)] {
        guard let tableInfo = tableRegistry.getTableInfo(name: tableName) else {
            throw PantryError.tableNotFound(name: tableName)
        }

        let idsSchema = tableInfo.schema
        var remaining = ids
        var results: [(Record, Row, Int)] = []

        // Fast path: use RID-to-page cache
        let ridCache2 = _state.withLock { $0.ridPageMap[tableName] }
        if let cache = ridCache2, !cache.isEmpty {
            var idsByPage = [Int: [UInt64]]()
            var uncached = [UInt64]()
            for id in ids {
                if let pageID = cache[id] { idsByPage[pageID, default: []].append(id) }
                else { uncached.append(id) }
            }
            for (pageID, pageIds) in idsByPage {
                let page = transactionContext == nil
                    ? try await getPageConcurrent(pageID: pageID)
                    : try await getPage(pageID: pageID, transactionContext: transactionContext)
                let idSet = Set(pageIds)
                for var record in page.records {
                    guard idSet.contains(record.id) else { continue }
                    if record.isOverflow {
                        if let needed = neededColumns,
                           let partialRow = Row.fromBytesPartial(record.data, neededColumns: needed) {
                            results.append((record, partialRow, pageID))
                            remaining.remove(record.id)
                            continue
                        }
                        record = try await reassembleOverflowRecord(record)
                    }
                    if let row = Row.fromBytesAuto(record.data, schema: idsSchema) {
                        results.append((record, row, pageID))
                        remaining.remove(record.id)
                    }
                }
            }
            if remaining.isEmpty { return results }
            remaining = remaining.intersection(Set(uncached))
            if remaining.isEmpty { return results }
        }

        // Fallback: scan pages
        let pageIDs = tableInfo.pageList ?? []
        if !pageIDs.isEmpty {
            for currentPageID in pageIDs {
                guard !remaining.isEmpty else { break }
                let page = transactionContext == nil
                    ? try await getPageConcurrent(pageID: currentPageID)
                    : try await getPage(pageID: currentPageID, transactionContext: transactionContext)
                for var record in page.records {
                    _state.withLock { $0.ridPageMap[tableName, default: [:]][record.id] = currentPageID }
                    guard remaining.contains(record.id) else { continue }
                    if record.isOverflow {
                        if let needed = neededColumns,
                           let partialRow = Row.fromBytesPartial(record.data, neededColumns: needed) {
                            results.append((record, partialRow, currentPageID))
                            remaining.remove(record.id)
                            continue
                        }
                        record = try await reassembleOverflowRecord(record)
                    }
                    if let row = Row.fromBytesAuto(record.data, schema: idsSchema) {
                        results.append((record, row, currentPageID))
                        remaining.remove(record.id)
                    }
                }
            }
        } else {
            var currentPageID = tableInfo.firstPageID
            var visited: Set<Int> = []
            while currentPageID != 0 && !remaining.isEmpty {
                guard visited.insert(currentPageID).inserted else {
                    throw PantryError.corruptPage(pageID: currentPageID)
                }
                let page = try await getPage(pageID: currentPageID, transactionContext: transactionContext)
                for var record in page.records {
                    _state.withLock { $0.ridPageMap[tableName, default: [:]][record.id] = currentPageID }
                    guard remaining.contains(record.id) else { continue }
                    if record.isOverflow {
                        if let needed = neededColumns,
                           let partialRow = Row.fromBytesPartial(record.data, neededColumns: needed) {
                            results.append((record, partialRow, currentPageID))
                            remaining.remove(record.id)
                            continue
                        }
                        record = try await reassembleOverflowRecord(record)
                    }
                    if let row = Row.fromBytesAuto(record.data, schema: idsSchema) {
                        results.append((record, row, currentPageID))
                        remaining.remove(record.id)
                    }
                }
                currentPageID = page.nextPageID
            }
        }

        return results
    }

    /// Get RID-to-page mapping from cache, partitioned into cached and uncached RIDs.
    /// Enables single-pass page access without loading pages just to find records.
    public func getRIDPageMapping(_ ids: Set<UInt64>, tableName: String) -> (cached: [Int: [UInt64]], uncached: [UInt64]) {
        var byPage = [Int: [UInt64]]()
        var uncached = [UInt64]()
        let ridCache3 = _state.withLock { $0.ridPageMap[tableName] }
        if let cache = ridCache3, !cache.isEmpty {
            for id in ids {
                if let pageID = cache[id] { byPage[pageID, default: []].append(id) }
                else { uncached.append(id) }
            }
        } else {
            uncached = Array(ids)
        }
        return (cached: byPage, uncached: uncached)
    }

    /// Like getRecordsByIDsWithPages but returns raw Data instead of decoded Row.
    /// Skips full Row deserialization for callers that only need raw bytes.
    public func getRecordDataByIDs(_ ids: Set<UInt64>, tableName: String, transactionContext: TransactionContext? = nil) async throws -> [(Record, Int)] {
        guard let tableInfo = tableRegistry.getTableInfo(name: tableName) else {
            throw PantryError.tableNotFound(name: tableName)
        }

        var remaining = ids
        var results: [(Record, Int)] = []
        results.reserveCapacity(ids.count)

        // Fast path: use RID-to-page cache
        let ridCache4 = _state.withLock { $0.ridPageMap[tableName] }
        if let cache = ridCache4, !cache.isEmpty {
            var idsByPage = [Int: [UInt64]]()
            var uncached = [UInt64]()
            for id in ids {
                if let pageID = cache[id] { idsByPage[pageID, default: []].append(id) }
                else { uncached.append(id) }
            }
            for (pageID, pageIds) in idsByPage {
                let page = transactionContext == nil
                    ? try await getPageConcurrent(pageID: pageID)
                    : try await getPage(pageID: pageID, transactionContext: transactionContext)
                let idSet = Set(pageIds)
                for var record in page.records {
                    guard idSet.contains(record.id) else { continue }
                    if record.isOverflow {
                        record = try await reassembleOverflowRecord(record)
                    }
                    results.append((record, pageID))
                    remaining.remove(record.id)
                }
            }
            if remaining.isEmpty { return results }
            remaining = remaining.intersection(Set(uncached))
            if remaining.isEmpty { return results }
        }

        // Fallback: scan pages
        let pageIDs = tableInfo.pageList ?? []
        if !pageIDs.isEmpty {
            for currentPageID in pageIDs {
                guard !remaining.isEmpty else { break }
                let page = transactionContext == nil
                    ? try await getPageConcurrent(pageID: currentPageID)
                    : try await getPage(pageID: currentPageID, transactionContext: transactionContext)
                for var record in page.records {
                    _state.withLock { $0.ridPageMap[tableName, default: [:]][record.id] = currentPageID }
                    guard remaining.contains(record.id) else { continue }
                    if record.isOverflow {
                        record = try await reassembleOverflowRecord(record)
                    }
                    results.append((record, currentPageID))
                    remaining.remove(record.id)
                }
            }
        } else {
            var currentPageID = tableInfo.firstPageID
            var visited: Set<Int> = []
            while currentPageID != 0 && !remaining.isEmpty {
                guard visited.insert(currentPageID).inserted else {
                    throw PantryError.corruptPage(pageID: currentPageID)
                }
                let page = try await getPage(pageID: currentPageID, transactionContext: transactionContext)
                for var record in page.records {
                    _state.withLock { $0.ridPageMap[tableName, default: [:]][record.id] = currentPageID }
                    guard remaining.contains(record.id) else { continue }
                    if record.isOverflow {
                        record = try await reassembleOverflowRecord(record)
                    }
                    results.append((record, currentPageID))
                    remaining.remove(record.id)
                }
                currentPageID = page.nextPageID
            }
        }

        return results
    }

    /// Walk the page chain for a table and return just the page IDs (lightweight).
    public func getPageChain(tableName: String, transactionContext: TransactionContext? = nil) async throws -> [Int] {
        guard var tableInfo = tableRegistry.getTableInfo(name: tableName) else {
            throw PantryError.tableNotFound(name: tableName)
        }

        // Fast path: return cached page list if available
        if let cached = tableInfo.pageList, !cached.isEmpty {
            return cached
        }

        // Slow path: walk linked list and cache the result
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

        tableInfo.pageList = pageIDs
        tableRegistry.updateTableInfo(tableInfo)
        return pageIDs
    }

    /// Non-actor page chain read: reads from registry cache or walks pages via getPageConcurrent.
    /// No actor hop needed — all accessed components are Sendable with internal locking.
    public func getPageChainConcurrent(tableName: String) async throws -> [Int] {
        guard var tableInfo = tableRegistry.getTableInfo(name: tableName) else {
            throw PantryError.tableNotFound(name: tableName)
        }

        // Fast path: return cached page list if available
        if let cached = tableInfo.pageList, !cached.isEmpty {
            return cached
        }

        // Slow path: walk linked list using concurrent page reads
        var pageIDs: [Int] = []
        var currentPageID = tableInfo.firstPageID
        var visited: Set<Int> = []

        while currentPageID != 0 {
            guard visited.insert(currentPageID).inserted else {
                throw PantryError.corruptPage(pageID: currentPageID)
            }
            pageIDs.append(currentPageID)
            let page = try await getPageConcurrent(pageID: currentPageID)
            currentPageID = page.nextPageID
        }

        tableInfo.pageList = pageIDs
        tableRegistry.updateTableInfo(tableInfo)
        return pageIDs
    }

    /// Stream records from a table page-at-a-time, yielding (Record, Row) pairs.
    public func scanTableStream(_ tableName: String) async throws -> AsyncStream<(Record, Row)> {
        guard let tableInfo = tableRegistry.getTableInfo(name: tableName) else {
            throw PantryError.tableNotFound(name: tableName)
        }

        let firstPageID = tableInfo.firstPageID
        let streamSchema = tableInfo.schema
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
                            if let row = Row.fromBytesAuto(record.data, schema: streamSchema) {
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
    /// Analyze table using pre-scanned raw records (avoids duplicate scan)
    public func analyzeTableFromRaw(_ tableName: String, rawRecords: [(Record, Data)]) async throws {
        guard var info = tableRegistry.getTableInfo(name: tableName) else {
            throw PantryError.tableNotFound(name: tableName)
        }
        analyzeRecords(rawRecords, info: &info)
        tableRegistry.updateTableInfo(info)
    }

    public func analyzeTable(_ tableName: String) async throws {
        guard var info = tableRegistry.getTableInfo(name: tableName) else {
            throw PantryError.tableNotFound(name: tableName)
        }
        let rawRecords = try await scanTableRaw(tableName)
        analyzeRecords(rawRecords, info: &info)
        tableRegistry.updateTableInfo(info)
    }

    /// Core analysis logic — collects column statistics from raw records
    private func analyzeRecords(_ rawRecords: [(Record, Data)], info: inout TableInfo) {
        let schema = info.schema
        let columnNames = schema.columns.map { $0.name }
        let colCount = columnNames.count
        let colOrdinals = columnNames.map { schema.columnOrdinals[$0] }

        var distinctSets = Array(repeating: Set<String>(), count: colCount)
        var nullCounts = Array(repeating: 0, count: colCount)
        var minValues = Array<String?>(repeating: nil, count: colCount)
        var maxValues = Array<String?>(repeating: nil, count: colCount)
        var sampleValues = (0..<colCount).map { _ in [String]() }
        let totalRows = rawRecords.count

        for (_, data) in rawRecords {
            for i in 0..<colCount {
                guard let ordinal = colOrdinals[i] else { continue }
                guard let value = Row.extractColumnValue(from: data, columnIndex: ordinal) else {
                    nullCounts[i] += 1
                    continue
                }
                if value == .null {
                    nullCounts[i] += 1
                } else {
                    let key = value.statsKey
                    distinctSets[i].insert(key)
                    if minValues[i] == nil || key < minValues[i]! { minValues[i] = key }
                    if maxValues[i] == nil || key > maxValues[i]! { maxValues[i] = key }
                    if sampleValues[i].count < 10_000 {
                        sampleValues[i].append(key)
                    }
                }
            }
        }

        for i in 0..<colCount {
            let col = columnNames[i]
            let existing = info.columnStats[col] ?? ColumnStats()
            var boundaries: [String] = []
            if sampleValues[i].count >= 64 {
                let sorted = sampleValues[i].sorted()
                let bucketSize = sorted.count / 64
                for j in 1..<64 {
                    boundaries.append(sorted[j * bucketSize])
                }
            }
            info.columnStats[col] = ColumnStats(
                distinctCount: distinctSets[i].count,
                isIndexed: existing.isIndexed,
                nullCount: nullCounts[i],
                totalCount: totalRows,
                minValue: minValues[i],
                maxValue: maxValues[i],
                histogramBoundaries: boundaries
            )
        }
    }

    /// Mark a column as indexed in the table stats (for query planner)
    public func markColumnIndexed(_ tableName: String, column: String) {
        guard var info = tableRegistry.getTableInfo(name: tableName) else { return }
        var stats = info.columnStats[column] ?? ColumnStats()
        stats.isIndexed = true
        info.columnStats[column] = stats
        tableRegistry.updateTableInfo(info)
    }

    /// Mark a column as no longer indexed
    public func markColumnNotIndexed(_ tableName: String, column: String) {
        guard var info = tableRegistry.getTableInfo(name: tableName) else { return }
        if var stats = info.columnStats[column] {
            stats.isIndexed = false
            info.columnStats[column] = stats
            tableRegistry.updateTableInfo(info)
        }
    }

    /// Get column statistics for query optimization (nonisolated — reads from Mutex-protected registry)
    public func getColumnStats(_ tableName: String, column: String) -> ColumnStats? {
        tableRegistry.getTableInfo(name: tableName)?.columnStats[column]
    }

    /// Check if a table exists (nonisolated — reads from Mutex-protected registry)
    public func tableExists(_ name: String) -> Bool {
        tableRegistry.getTableInfo(name: name) != nil
    }

    /// List all table names (nonisolated — reads from Mutex-protected registry)
    public func listTables() -> [String] {
        tableRegistry.allTables().map { $0.name }
    }

    /// Get a table's schema (nonisolated — reads from Mutex-protected registry)
    public func getTableSchema(_ name: String) -> PantryTableSchema? {
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

        // Remove from registry and RID cache
        tableRegistry.removeTable(name: name)
        _state.withLock { $0.ridPageMap.removeValue(forKey: name) }
        try await tableRegistry.save()

        // Chain freed data pages into the free list
        let pagesToFree: [Int]
        if let cached = tableInfo.pageList, !cached.isEmpty {
            pagesToFree = cached
        } else {
            var list: [Int] = []
            var currentPageID = tableInfo.firstPageID
            var visited: Set<Int> = []
            while currentPageID != 0 {
                guard visited.insert(currentPageID).inserted else { break }
                let page = try await getPage(pageID: currentPageID)
                list.append(currentPageID)
                currentPageID = page.nextPageID
            }
            pagesToFree = list
        }
        for pageID in pagesToFree {
            var freedPage = DatabasePage(pageID: pageID)
            freedPage.nextPageID = _state.withLock { $0.freeListHead }
            try await savePage(freedPage)
            _state.withLock { $0.freeListHead = pageID }
            freeSpaceBitmap.setCategory(pageID: pageID, category: .empty)
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

        // Auto-checkpoint: if WAL exceeds threshold and no active transactions, checkpoint
        let threshold = _state.withLock { $0.autoCheckpointThreshold }
        if threshold > 0 {
            let walBytes = await logManager.walSize
            if walBytes >= UInt64(threshold),
               await transactionManager.getActiveTransactionCount() == 0 {
                try? await transactionManager.createCheckpoint()
            }
        }
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

        // Iterate page list (or walk chain), using bitmap to skip known-full pages
        let pageIDs = tableInfo.pageList ?? []
        if !pageIDs.isEmpty {
            for pageID in pageIDs {
                let category = freeSpaceBitmap.getCategory(pageID: pageID)
                if category.rawValue >= minCategory.rawValue {
                    let page = try await getPage(pageID: pageID)
                    let actualCategory = page.spaceCategory()
                    if actualCategory != category {
                        freeSpaceBitmap.setCategory(pageID: pageID, category: actualCategory)
                    }
                    if page.getFreeSpace() > requiredSpace {
                        return pageID
                    }
                }
            }
        } else {
            // Legacy fallback: walk linked page chain
            var currentPageID = tableInfo.firstPageID
            var visited: Set<Int> = []
            while currentPageID != 0 {
                guard visited.insert(currentPageID).inserted else {
                    throw PantryError.corruptPage(pageID: currentPageID)
                }
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
                let page = try await getPage(pageID: currentPageID)
                currentPageID = page.nextPageID
            }
        }

        // Need a new page
        var info = tableInfo
        let newPage = try await createNewPageForTable(tableInfo: &info)
        tableRegistry.updateTableInfo(info)
        freeSpaceBitmap.setCategory(pageID: newPage.pageID, category: .empty)
        return newPage.pageID
    }

    private func createNewPageForTable(tableInfo: inout TableInfo) async throws -> DatabasePage {
        // Pop from free list if available, check extent reserve, or extend file with extent allocation
        let newPage: DatabasePage
        let currentFreeListHead = _state.withLock { $0.freeListHead }
        if currentFreeListHead != 0 {
            let freePage = try await storageManager.readPage(pageID: currentFreeListHead)
            _state.withLock { $0.freeListHead = freePage.nextPageID }
            var reusedPage = DatabasePage(pageID: freePage.pageID)
            try reusedPage.saveRecords()
            bufferPoolManager.updatePage(reusedPage)
            try await saveFreeListHead()
            newPage = reusedPage
        } else if var reserve = _state.withLock({ $0.extentReserve[tableInfo.name] }), !reserve.isEmpty {
            // Use pre-allocated page from extent reserve
            newPage = reserve.removeFirst()
            _state.withLock { s in
                if reserve.isEmpty {
                    s.extentReserve.removeValue(forKey: tableInfo.name)
                } else {
                    s.extentReserve[tableInfo.name] = reserve
                }
            }
        } else {
            // Allocate a contiguous extent of pages, use the first, reserve the rest
            newPage = try await storageManager.createNewPage()
            var reserved: [DatabasePage] = []
            for _ in 1..<extentSize {
                let extraPage = try await storageManager.createNewPage()
                await bufferPoolManager.cachePage(extraPage)
                reserved.append(extraPage)
            }
            if !reserved.isEmpty {
                _state.withLock { $0.extentReserve[tableInfo.name] = reserved }
            }
        }
        await bufferPoolManager.cachePage(newPage)

        if tableInfo.firstPageID == 0 {
            tableInfo.firstPageID = newPage.pageID
            tableInfo.lastPageID = newPage.pageID
            tableInfo.pageList = [newPage.pageID]
        } else {
            // Link to the current tail page
            let tailPageID = tableInfo.pageList?.last ?? tableInfo.lastPageID
            var tailPage = try await getPage(pageID: tailPageID)
            if tailPage.nextPageID == 0 {
                tailPage.nextPageID = newPage.pageID
                try await savePage(tailPage)
            } else {
                // Fallback: walk chain to find true tail (recovery after crash)
                var currentPageID = tailPage.nextPageID
                var visited: Set<Int> = [tailPageID]
                while true {
                    guard visited.insert(currentPageID).inserted else { break }
                    let page = try await getPage(pageID: currentPageID)
                    if page.nextPageID == 0 {
                        var lastPage = page
                        lastPage.nextPageID = newPage.pageID
                        try await savePage(lastPage)
                        break
                    }
                    currentPageID = page.nextPageID
                }
            }
            tableInfo.lastPageID = newPage.pageID
            if tableInfo.pageList != nil {
                tableInfo.pageList!.append(newPage.pageID)
            }
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

    private func findPageContainingRecord(id: UInt64, tableName: String) async throws -> (Int, DatabasePage) {
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
                return (currentPageID, page)
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
            let currentFLH = _state.withLock { $0.freeListHead }
            if currentFLH != 0 {
                let freePage = try await storageManager.readPage(pageID: currentFLH)
                _state.withLock { $0.freeListHead = freePage.nextPageID }
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
        let serializedSize = inlineRecord.serializedSize
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
    public func reassembleOverflowRecord(_ record: Record) async throws -> Record {
        guard let firstOverflowPageID = record.overflowPageID else {
            return record
        }

        var fullData = Data(record.data)
        var currentOverflowPageID = firstOverflowPageID
        var visited: Set<Int> = []
        var prefetchTask: Task<Void, Never>?

        while currentOverflowPageID != 0 {
            guard visited.insert(currentOverflowPageID).inserted else {
                prefetchTask?.cancel()
                throw PantryError.corruptPage(pageID: currentOverflowPageID)
            }

            let page = try await storageManager.readPage(pageID: currentOverflowPageID)
            let headerSize = PantryConstants.PAGE_HEADER_SIZE

            // Read next overflow page ID (4 bytes after header)
            let nextOverflowPageID = Int(page.data.withUnsafeBytes {
                $0.loadUnaligned(fromByteOffset: headerSize, as: Int32.self)
            })

            // Prefetch next overflow page while processing current
            if nextOverflowPageID != 0 {
                let bp = bufferPoolManager
                let nextID = nextOverflowPageID
                prefetchTask?.cancel()
                prefetchTask = Task { await bp.prefetchPages([nextID]) }
            }

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

        prefetchTask?.cancel()
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
            freedPage.nextPageID = _state.withLock { $0.freeListHead }
            try freedPage.saveRecords()
            try await storageManager.writePage(&freedPage)
            bufferPoolManager.updatePage(freedPage)
            _state.withLock { $0.freeListHead = currentPageID }
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
            guard !_state.withLock({ $0.systemPageIDs.contains(pageID) }) else { continue }
            let page = try await storageManager.readPage(pageID: pageID)
            if page.isSystemPage { continue }
            freeSpaceBitmap.setCategory(pageID: pageID, category: page.spaceCategory())
        }
    }

    // MARK: - Index Registry Persistence

    /// Load index registry entries from the dedicated page chain
    public func loadIndexRegistry() async throws -> Data? {
        let irPageID = _state.withLock { $0.indexRegistryPageID }
        guard irPageID != 0 else { return nil }
        // Follow the page chain to collect all chunks
        var allData = Data()
        var currentPageID = irPageID
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
        if _state.withLock({ $0.indexRegistryPageID }) == 0 {
            let firstPage = try await storageManager.createNewPage()
            _state.withLock { $0.indexRegistryPageID = firstPage.pageID }
            try await saveDBMetadata()
        }

        var currentPageID = _state.withLock { $0.indexRegistryPageID }
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
        let meta = _state.withLock { s in
            DBMetadata(freeListHead: s.freeListHead, indexRegistryPageID: s.indexRegistryPageID, freeSpaceBitmapPageID: s.freeSpaceBitmapPageID, checkpointLSN: s.checkpointLSN)
        }
        let data = try JSONEncoder().encode(meta)
        let record = Record(id: 1, data: data)

        var metaPage = try await storageManager.readPage(pageID: PantryConstants.SYSTEM_PAGE_DB_METADATA)
        metaPage.records = [record]
        metaPage.recordCount = 1
        metaPage.freeSpaceOffset = PantryConstants.PAGE_SIZE - record.serializedSize
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
        // Persist the checkpoint LSN so recovery skips already-checkpointed records
        let newLSN = await transactionManager.lastCheckpointLSN
        if newLSN > _state.withLock({ $0.checkpointLSN }) {
            _state.withLock { $0.checkpointLSN = newLSN }
            try await saveDBMetadata()
        }
    }

    // MARK: - Lifecycle

    public func close() async throws {
        // Stop background writer before final flush
        bufferPoolManager.stopBackgroundWriter()

        var firstError: Error?

        do { try await bufferPoolManager.flushAllDirtyPages() } catch { if firstError == nil { firstError = error } }
        do { try await tableRegistry.save() } catch { if firstError == nil { firstError = error } }
        // Save free space bitmap
        do {
            let existingBitmapPageID = _state.withLock { $0.freeSpaceBitmapPageID }
            let newPageID = try await freeSpaceBitmap.save(storageManager: storageManager, bufferPool: bufferPoolManager, existingPageID: existingBitmapPageID)
            if newPageID != existingBitmapPageID {
                _state.withLock { s in
                    s.freeSpaceBitmapPageID = newPageID
                    s.systemPageIDs.insert(newPageID)
                }
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
