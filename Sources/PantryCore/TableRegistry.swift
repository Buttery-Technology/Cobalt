import Foundation

/// Metadata about a single table stored in the registry
public struct TableInfo: Codable, Sendable {
    public var tableID: Int
    public var name: String
    public var firstPageID: Int
    public var lastPageID: Int
    public var recordCount: Int
    public var schema: PantryTableSchema

    public init(tableID: Int, name: String, firstPageID: Int, lastPageID: Int, recordCount: Int, schema: PantryTableSchema) {
        self.tableID = tableID
        self.name = name
        self.firstPageID = firstPageID
        self.lastPageID = lastPageID
        self.recordCount = recordCount
        self.schema = schema
    }
}

/// Persists table metadata on system pages.
/// Page 0 = DB metadata, page 1+ = table registry entries.
public actor TableRegistry: Sendable {
    private var tables: [String: TableInfo] = [:]
    private var nextTableID: Int = 1
    private let storageManager: StorageManager
    private let bufferPool: BufferPoolManager

    public init(storageManager: StorageManager, bufferPool: BufferPoolManager) {
        self.storageManager = storageManager
        self.bufferPool = bufferPool
    }

    /// Load the table registry from system pages (follows overflow chain)
    public func load() async throws {
        let totalPages = try await storageManager.totalPageCount()
        guard totalPages > PantryConstants.SYSTEM_PAGE_TABLE_REGISTRY_START else {
            return
        }

        // Follow the registry page chain starting from page 1
        var currentPageID = PantryConstants.SYSTEM_PAGE_TABLE_REGISTRY_START
        var visited: Set<Int> = []

        while currentPageID != 0 && currentPageID < totalPages {
            guard visited.insert(currentPageID).inserted else { break }

            let page: DatabasePage
            do {
                page = try await storageManager.readPage(pageID: currentPageID)
            } catch {
                break
            }
            guard PageFlags(rawValue: page.flags).contains(.tableRegistry) else {
                break
            }

            // Each record on a registry page is a JSON-encoded TableInfo
            for record in page.records {
                if let info = try? JSONDecoder().decode(TableInfo.self, from: record.data) {
                    tables[info.name] = info
                    if info.tableID >= nextTableID {
                        nextTableID = info.tableID + 1
                    }
                }
            }

            currentPageID = page.nextPageID
        }
    }

    /// Persist the entire table registry to system pages, chaining overflow pages
    public func save() async throws {
        let encoder = JSONEncoder()
        var records: [Record] = []

        for (_, info) in tables.sorted(by: { $0.value.tableID < $1.value.tableID }) {
            let data = try encoder.encode(info)
            records.append(Record(id: UInt64(info.tableID), data: data))
        }

        // Write records across registry pages starting at page 1, chaining overflow pages
        var currentPageID = PantryConstants.SYSTEM_PAGE_TABLE_REGISTRY_START
        var page = DatabasePage(
            pageID: currentPageID,
            flags: PageFlags.system.rawValue | PageFlags.tableRegistry.rawValue
        )
        var writtenPageIDs: [Int] = []

        for record in records {
            if !page.addRecord(record) {
                // Current page is full — allocate overflow page and chain
                let overflowPage = try await storageManager.createNewPage()
                page.nextPageID = overflowPage.pageID
                try await writePage(&page)
                writtenPageIDs.append(currentPageID)

                currentPageID = overflowPage.pageID
                page = DatabasePage(
                    pageID: currentPageID,
                    flags: PageFlags.system.rawValue | PageFlags.tableRegistry.rawValue
                )
                if !page.addRecord(record) {
                    throw PantryError.schemaSerializationError
                }
            }
        }

        // Write the last page (no next pointer)
        page.nextPageID = 0
        try await writePage(&page)
        writtenPageIDs.append(currentPageID)

        // Clear any remaining old registry pages to prevent stale data on reload
        let lastWrittenPageID = writtenPageIDs.last ?? PantryConstants.SYSTEM_PAGE_TABLE_REGISTRY_START
        let totalPages = try await storageManager.totalPageCount()
        var nextPage = lastWrittenPageID + 1
        while nextPage < totalPages {
            do {
                let oldPage = try await storageManager.readPage(pageID: nextPage)
                guard PageFlags(rawValue: oldPage.flags).contains(.tableRegistry) else {
                    break
                }
                var clearPage = DatabasePage(pageID: nextPage)
                try await storageManager.writePage(&clearPage)
            } catch {
                break
            }
            nextPage += 1
        }
    }

    private func writePage(_ page: inout DatabasePage) async throws {
        try await storageManager.writePage(&page)
        await bufferPool.updatePage(page)
    }

    // MARK: - Table CRUD

    public func registerTable(name: String, schema: PantryTableSchema, firstPageID: Int) throws -> TableInfo {
        guard tables[name] == nil else {
            throw PantryError.tableAlreadyExists(name: name)
        }

        let info = TableInfo(
            tableID: nextTableID,
            name: name,
            firstPageID: firstPageID,
            lastPageID: firstPageID,
            recordCount: 0,
            schema: schema
        )
        nextTableID += 1
        tables[name] = info
        return info
    }

    public func getTableInfo(name: String) -> TableInfo? {
        tables[name]
    }

    public func updateTableInfo(_ info: TableInfo) {
        tables[info.name] = info
    }

    public func removeTable(name: String) {
        tables.removeValue(forKey: name)
    }

    public func allTables() -> [TableInfo] {
        Array(tables.values)
    }
}
