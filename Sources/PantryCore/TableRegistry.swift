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

    /// Load the table registry from system pages
    public func load() async throws {
        let totalPages = try await storageManager.totalPageCount()
        guard totalPages > PantryConstants.SYSTEM_PAGE_TABLE_REGISTRY_START else {
            return
        }

        // Read registry pages starting from page 1
        for pageID in PantryConstants.SYSTEM_PAGE_TABLE_REGISTRY_START..<totalPages {
            do {
                let page = try await storageManager.readPage(pageID: pageID)
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
            } catch {
                break
            }
        }
    }

    /// Persist the entire table registry to system pages
    public func save() async throws {
        let encoder = JSONEncoder()
        var records: [Record] = []

        for (_, info) in tables.sorted(by: { $0.value.tableID < $1.value.tableID }) {
            let data = try encoder.encode(info)
            records.append(Record(id: UInt64(info.tableID), data: data))
        }

        // Write records across registry pages starting at page 1
        let pageID = PantryConstants.SYSTEM_PAGE_TABLE_REGISTRY_START
        var page = DatabasePage(
            pageID: pageID,
            flags: PageFlags.system.rawValue | PageFlags.tableRegistry.rawValue
        )

        for record in records {
            if !page.addRecord(record) {
                // Guard: registry must fit in reserved pages to avoid overwriting data pages
                throw PantryError.schemaSerializationError
            }
        }

        // Write the last page
        try await writePage(&page)

        // Clear any remaining old registry pages to prevent stale data on reload
        let lastWrittenPageID = pageID
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
