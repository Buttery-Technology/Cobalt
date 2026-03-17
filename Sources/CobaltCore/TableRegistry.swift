import Foundation
import Synchronization

/// Per-column statistics for query optimization
public struct ColumnStats: Codable, Sendable {
    /// Approximate number of distinct values in this column
    public var distinctCount: Int
    /// Whether this column has an index
    public var isIndexed: Bool
    /// Number of NULL values in this column
    public var nullCount: Int
    /// Total number of rows sampled
    public var totalCount: Int
    /// Minimum value (as comparable string key)
    public var minValue: String?
    /// Maximum value (as comparable string key)
    public var maxValue: String?
    /// Equi-depth histogram bucket boundaries (up to 64 buckets)
    /// Each entry is a value boundary; rows are roughly evenly distributed between boundaries
    public var histogramBoundaries: [String]

    public init(distinctCount: Int = 0, isIndexed: Bool = false, nullCount: Int = 0, totalCount: Int = 0, minValue: String? = nil, maxValue: String? = nil, histogramBoundaries: [String] = []) {
        self.distinctCount = distinctCount
        self.isIndexed = isIndexed
        self.nullCount = nullCount
        self.totalCount = totalCount
        self.minValue = minValue
        self.maxValue = maxValue
        self.histogramBoundaries = histogramBoundaries
    }

    /// Selectivity estimate for an equality predicate (lower = more selective)
    public var equalitySelectivity: Double {
        distinctCount > 0 ? 1.0 / Double(distinctCount) : 1.0
    }

    /// Selectivity estimate for a NULL predicate
    public var nullSelectivity: Double {
        totalCount > 0 ? Double(nullCount) / Double(totalCount) : 0.05
    }

    /// Selectivity estimate for a range predicate using histogram
    /// Estimates fraction of rows between low and high boundaries
    public func rangeSelectivity(low: String?, high: String?) -> Double {
        guard !histogramBoundaries.isEmpty else { return 0.3 }
        let buckets = histogramBoundaries.count + 1
        var lowBucket = 0
        var highBucket = buckets

        if let low = low {
            lowBucket = histogramBoundaries.firstIndex(where: { $0 >= low }) ?? buckets
        }
        if let high = high {
            highBucket = (histogramBoundaries.firstIndex(where: { $0 > high }) ?? buckets)
        }

        let coveredBuckets = max(0, highBucket - lowBucket)
        return max(1.0 / Double(max(1, totalCount)), Double(coveredBuckets) / Double(buckets))
    }
}

/// Metadata about a single table stored in the registry
public struct TableInfo: Codable, Sendable {
    public var tableID: Int
    public var name: String
    public var firstPageID: Int
    public var lastPageID: Int
    public var recordCount: Int
    public var schema: CobaltTableSchema
    /// Per-column statistics for query optimization (column name → stats)
    public var columnStats: [String: ColumnStats]
    /// Cached list of all page IDs belonging to this table (nil = not yet populated)
    public var pageList: [Int]?

    enum CodingKeys: String, CodingKey {
        case tableID, name, firstPageID, lastPageID, recordCount, schema, columnStats, pageList
    }

    public init(tableID: Int, name: String, firstPageID: Int, lastPageID: Int, recordCount: Int, schema: CobaltTableSchema, columnStats: [String: ColumnStats] = [:]) {
        self.tableID = tableID
        self.name = name
        self.firstPageID = firstPageID
        self.lastPageID = lastPageID
        self.recordCount = recordCount
        self.schema = schema
        self.columnStats = columnStats
        self.pageList = firstPageID != 0 ? [firstPageID] : []
    }

    public init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        tableID = try container.decode(Int.self, forKey: .tableID)
        name = try container.decode(String.self, forKey: .name)
        firstPageID = try container.decode(Int.self, forKey: .firstPageID)
        lastPageID = try container.decode(Int.self, forKey: .lastPageID)
        recordCount = try container.decode(Int.self, forKey: .recordCount)
        schema = try container.decode(CobaltTableSchema.self, forKey: .schema)
        columnStats = try container.decodeIfPresent([String: ColumnStats].self, forKey: .columnStats) ?? [:]
        pageList = try container.decodeIfPresent([Int].self, forKey: .pageList)
    }
}

/// Mutable state protected by Mutex
private struct TableRegistryState: ~Copyable {
    var tables: [String: TableInfo] = [:]
    var nextTableID: Int = 1
}

/// Persists table metadata on system pages.
/// Page 0 = DB metadata, page 1+ = table registry entries.
public final class TableRegistry: Sendable {
    private let state = Mutex(TableRegistryState())
    private let storageManager: StorageManager
    private let bufferPool: BufferPoolManager

    public init(storageManager: StorageManager, bufferPool: BufferPoolManager) {
        self.storageManager = storageManager
        self.bufferPool = bufferPool
    }

    /// Load the table registry from system pages (follows overflow chain)
    public func load() async throws {
        let totalPages = try await storageManager.totalPageCount()
        guard totalPages > CobaltConstants.SYSTEM_PAGE_TABLE_REGISTRY_START else {
            return
        }

        // Follow the registry page chain starting from page 1
        var currentPageID = CobaltConstants.SYSTEM_PAGE_TABLE_REGISTRY_START
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
                    state.withLock { s in
                        s.tables[info.name] = info
                        if info.tableID >= s.nextTableID {
                            s.nextTableID = info.tableID + 1
                        }
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

        let tablesList = state.withLock { s in
            s.tables.values.sorted(by: { $0.tableID < $1.tableID })
        }

        for info in tablesList {
            let data = try encoder.encode(info)
            records.append(Record(id: UInt64(info.tableID), data: data))
        }

        // Write records across registry pages starting at page 1, chaining overflow pages
        var currentPageID = CobaltConstants.SYSTEM_PAGE_TABLE_REGISTRY_START
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
                    throw CobaltError.schemaSerializationError
                }
            }
        }

        // Write the last page (no next pointer)
        page.nextPageID = 0
        try await writePage(&page)
        writtenPageIDs.append(currentPageID)

        // Clear any remaining old registry pages to prevent stale data on reload
        let lastWrittenPageID = writtenPageIDs.last ?? CobaltConstants.SYSTEM_PAGE_TABLE_REGISTRY_START
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
        bufferPool.updatePage(page)
    }

    // MARK: - Table CRUD (synchronous — Mutex-protected)

    public func registerTable(name: String, schema: CobaltTableSchema, firstPageID: Int) throws -> TableInfo {
        try state.withLock { s in
            guard s.tables[name] == nil else {
                throw CobaltError.tableAlreadyExists(name: name)
            }

            let info = TableInfo(
                tableID: s.nextTableID,
                name: name,
                firstPageID: firstPageID,
                lastPageID: firstPageID,
                recordCount: 0,
                schema: schema
            )
            s.nextTableID += 1
            s.tables[name] = info
            return info
        }
    }

    public func getTableInfo(name: String) -> TableInfo? {
        state.withLock { $0.tables[name] }
    }

    public func updateTableInfo(_ info: TableInfo) {
        state.withLock { $0.tables[info.name] = info }
    }

    public func removeTable(name: String) {
        _ = state.withLock { $0.tables.removeValue(forKey: name) }
    }

    public func allTables() -> [TableInfo] {
        state.withLock { Array($0.tables.values) }
    }
}
