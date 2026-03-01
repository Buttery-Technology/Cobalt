import Foundation
import PantryCore

/// Per-index state: B-tree + bloom filter for a single or compound column index
public actor ColumnIndex: Sendable {
    public let tableName: String
    public let columnName: String
    /// For compound indexes, the ordered list of columns that form the key
    public let compoundColumns: [String]?
    public let btree: BTree
    private var bloomFilter: BloomFilter
    public let nodeStore: PageBackedNodeStore

    public init(tableName: String, columnName: String, compoundColumns: [String]? = nil, btree: BTree, nodeStore: PageBackedNodeStore, expectedElements: Int = 10000) {
        self.tableName = tableName
        self.columnName = columnName
        self.compoundColumns = compoundColumns
        self.btree = btree
        self.nodeStore = nodeStore
        self.bloomFilter = BloomFilter(expectedElements: expectedElements)
    }

    /// Whether this is a compound (multi-column) index
    public var isCompound: Bool { compoundColumns != nil }

    /// Insert a key-row pair into this index
    public func insert(key: DBValue, row: Row) async throws {
        bloomFilter.add(key.indexKey)
        try await btree.insert(key: key, row: row)
    }

    /// Search for rows matching a key, using bloom filter for fast negatives
    public func search(key: DBValue) async throws -> [Row]? {
        if !bloomFilter.contains(key.indexKey) {
            return [] // Definitive miss
        }
        return try await btree.search(key: key)
    }

    /// Range query on this index
    public func searchRange(from startKey: DBValue?, to endKey: DBValue?) async throws -> [Row] {
        try await btree.searchRange(from: startKey, to: endKey)
    }

    /// Delete a specific (key, row) entry from this index
    public func delete(key: DBValue, row: Row? = nil) async throws {
        try await btree.delete(key: key, row: row)
    }

    /// Add a value to the bloom filter only (used during index rebuild on load)
    public func addToBloomFilter(_ value: DBValue) {
        bloomFilter.add(value.indexKey)
    }
}

/// Coordinates per-table indexes; implements IndexHook for StorageEngine
public actor IndexManager: IndexHook, Sendable {
    /// table name -> column name -> ColumnIndex
    private var indexes: [String: [String: ColumnIndex]] = [:]
    private let bufferPool: BufferPoolManager
    private let storageManager: StorageManager

    public init(bufferPool: BufferPoolManager, storageManager: StorageManager) {
        self.bufferPool = bufferPool
        self.storageManager = storageManager
    }

    /// Create a new index for a table column
    public func createIndex(tableName: String, columnName: String) async throws -> ColumnIndex {
        let nodeStore = PageBackedNodeStore(bufferPool: bufferPool, storageManager: storageManager)
        let btree = BTree(order: 16, nodeStore: nodeStore)
        let columnIndex = ColumnIndex(tableName: tableName, columnName: columnName, btree: btree, nodeStore: nodeStore)

        if indexes[tableName] == nil {
            indexes[tableName] = [:]
        }
        indexes[tableName]![columnName] = columnIndex
        return columnIndex
    }

    /// Create a compound index over multiple columns
    public func createCompoundIndex(tableName: String, columns: [String]) async throws -> ColumnIndex {
        guard columns.count >= 2 else {
            throw PantryError.invalidQuery(description: "Compound index requires at least 2 columns")
        }
        let indexName = columns.joined(separator: "+")
        let nodeStore = PageBackedNodeStore(bufferPool: bufferPool, storageManager: storageManager)
        let btree = BTree(order: 16, nodeStore: nodeStore)
        let columnIndex = ColumnIndex(tableName: tableName, columnName: indexName, compoundColumns: columns, btree: btree, nodeStore: nodeStore)

        if indexes[tableName] == nil {
            indexes[tableName] = [:]
        }
        indexes[tableName]![indexName] = columnIndex
        return columnIndex
    }

    /// Get an existing index
    public func getIndex(tableName: String, columnName: String) -> ColumnIndex? {
        indexes[tableName]?[columnName]
    }

    /// Get all indexes for a table
    public func getIndexes(tableName: String) -> [String: ColumnIndex] {
        indexes[tableName] ?? [:]
    }

    /// Remove all indexes for a table
    public func removeIndexes(tableName: String) {
        indexes.removeValue(forKey: tableName)
    }

    // MARK: - Persistence

    /// Save all index metadata to a registry page
    public func saveIndexRegistry() async throws -> [IndexRegistryEntry] {
        var entries: [IndexRegistryEntry] = []
        for (tableName, tableIndexes) in indexes {
            for (columnName, columnIndex) in tableIndexes {
                let rootId = await columnIndex.btree.getRootId()
                let nodePageMap = await columnIndex.nodeStore.getNodePageMap()
                let compoundCols = await columnIndex.compoundColumns
                entries.append(IndexRegistryEntry(
                    tableName: tableName,
                    columnName: columnName,
                    compoundColumns: compoundCols,
                    rootNodeId: rootId,
                    nodePageMap: nodePageMap
                ))
            }
        }
        return entries
    }

    /// Restore indexes from persisted registry entries
    public func loadIndexRegistry(entries: [IndexRegistryEntry]) async throws {
        for entry in entries {
            let nodeStore = PageBackedNodeStore(bufferPool: bufferPool, storageManager: storageManager)
            await nodeStore.restoreNodePageMap(entry.decodedNodePageMap)

            let btree = BTree(order: 16, nodeStore: nodeStore)
            await btree.setRootId(entry.rootNodeId)

            let columnIndex = ColumnIndex(
                tableName: entry.tableName,
                columnName: entry.columnName,
                compoundColumns: entry.compoundColumns,
                btree: btree,
                nodeStore: nodeStore
            )

            // Rebuild bloom filter from B-tree data
            if entry.rootNodeId != nil {
                let allRows = try await btree.searchRange(from: nil, to: nil)
                for row in allRows {
                    if let compoundCols = entry.compoundColumns {
                        // Compound index: reconstruct compound key for bloom filter
                        let keyValues = compoundCols.map { row.values[$0] ?? .null }
                        await columnIndex.addToBloomFilter(.compound(keyValues))
                    } else if let value = row.values[entry.columnName] {
                        await columnIndex.addToBloomFilter(value)
                    }
                }
            }

            if indexes[entry.tableName] == nil {
                indexes[entry.tableName] = [:]
            }
            indexes[entry.tableName]![entry.columnName] = columnIndex
        }
    }

    // MARK: - IndexHook conformance

    public func lookupRecord(id: UInt64, tableName: String) async throws -> Int? {
        // Index lookup by record ID is not directly supported by column indexes
        // This would require a separate primary key index
        return nil
    }

    public func updateIndexes(record: Record, row: Row, tableName: String) async throws {
        guard let tableIndexes = indexes[tableName] else { return }

        // Embed record ID so index-accelerated update/delete can skip table scan
        var indexValues = row.values
        indexValues["__rid"] = .integer(Int64(bitPattern: record.id))
        let indexRow = Row(values: indexValues)

        for (_, columnIndex) in tableIndexes {
            if let columns = await columnIndex.compoundColumns {
                // Compound index: build compound key from multiple columns
                let keyValues = columns.map { indexRow.values[$0] ?? .null }
                let compoundKey = DBValue.compound(keyValues)
                try await columnIndex.insert(key: compoundKey, row: indexRow)
            } else {
                let columnName = await columnIndex.columnName
                if let value = indexRow.values[columnName] {
                    try await columnIndex.insert(key: value, row: indexRow)
                }
            }
        }
    }

    public func removeFromIndexes(id: UInt64, row: Row, tableName: String) async throws {
        guard let tableIndexes = indexes[tableName] else { return }

        var indexValues = row.values
        indexValues["__rid"] = .integer(Int64(bitPattern: id))
        let indexRow = Row(values: indexValues)

        for (_, columnIndex) in tableIndexes {
            if let columns = await columnIndex.compoundColumns {
                let keyValues = columns.map { indexRow.values[$0] ?? .null }
                let compoundKey = DBValue.compound(keyValues)
                try await columnIndex.delete(key: compoundKey, row: indexRow)
            } else {
                let columnName = await columnIndex.columnName
                if let value = indexRow.values[columnName] {
                    try await columnIndex.delete(key: value, row: indexRow)
                }
            }
        }
    }

    /// Attempt to use indexes for a query condition
    public func attemptIndexLookup(tableName: String, condition: WhereCondition) async throws -> [Row]? {
        guard let tableIndexes = indexes[tableName] else { return nil }

        switch condition {
        case .equals(let column, let value):
            // SQL NULL: NULL = NULL is false, so .equals with .null always returns empty
            if value == .null { return [] }
            if let index = tableIndexes[column] {
                return try await index.search(key: value)
            }
            return nil

        case .greaterThan(let column, let value):
            if let index = tableIndexes[column] {
                // searchRange uses closed-interval; exclude boundary for strict >
                let rows = try await index.searchRange(from: value, to: nil)
                return rows.filter { $0.values[column] != value }
            }
            return nil

        case .lessThan(let column, let value):
            if let index = tableIndexes[column] {
                // searchRange uses closed-interval; exclude boundary for strict <
                let rows = try await index.searchRange(from: nil, to: value)
                return rows.filter { $0.values[column] != value }
            }
            return nil

        case .greaterThanOrEqual(let column, let value):
            if let index = tableIndexes[column] {
                return try await index.searchRange(from: value, to: nil)
            }
            return nil

        case .lessThanOrEqual(let column, let value):
            if let index = tableIndexes[column] {
                return try await index.searchRange(from: nil, to: value)
            }
            return nil

        case .between(let column, let min, let max):
            if let index = tableIndexes[column] {
                return try await index.searchRange(from: min, to: max)
            }
            return nil

        case .and(let conditions):
            // Check compound indexes first: if all sub-conditions are .equals,
            // try to find a compound index matching those columns
            let equalsMap = extractEqualsMap(from: conditions)
            if !equalsMap.isEmpty {
                for (_, columnIndex) in tableIndexes {
                    guard let compoundColumns = await columnIndex.compoundColumns else { continue }
                    // Full compound key match: all columns present
                    if compoundColumns.allSatisfy({ equalsMap[$0] != nil }) {
                        let keyValues = compoundColumns.map { equalsMap[$0]! }
                        let compoundKey = DBValue.compound(keyValues)
                        if let results = try await columnIndex.search(key: compoundKey) {
                            return results
                        }
                    }
                    // Prefix match: first N columns present (range scan on prefix)
                    let prefixLen = compoundColumns.prefix(while: { equalsMap[$0] != nil }).count
                    if prefixLen > 0 && prefixLen < compoundColumns.count {
                        let prefixValues = compoundColumns.prefix(prefixLen).map { equalsMap[$0]! }
                        // Scan all entries and filter by prefix columns in memory
                        let allRows = try await columnIndex.searchRange(from: nil, to: nil)
                        let filtered = allRows.filter { row in
                            for (i, col) in compoundColumns.prefix(prefixLen).enumerated() {
                                guard let val = row.values[col], val == prefixValues[i] else { return false }
                            }
                            return true
                        }
                        return filtered
                    }
                }
            }

            // Fallback: try single-column index on first indexable condition
            for sub in conditions {
                if let result = try await attemptIndexLookup(tableName: tableName, condition: sub) {
                    return result
                }
            }
            return nil

        default:
            return nil
        }
    }
}

// MARK: - Helpers

/// Extract a column→value map from a list of .equals conditions
private func extractEqualsMap(from conditions: [WhereCondition]) -> [String: DBValue] {
    var map: [String: DBValue] = [:]
    for cond in conditions {
        if case .equals(let column, let value) = cond, value != .null {
            map[column] = value
        }
    }
    return map
}

// MARK: - DBValue index key helper

extension DBValue {
    /// Convert to a string suitable for bloom filter hashing
    var indexKey: String {
        switch self {
        case .null: return "__null__"
        case .integer(let v): return "n:\(Double(v))"
        case .double(let v):
            // Normalize -0.0 to 0.0 and canonicalize NaN
            let normalized = v.isNaN ? Double.nan : (v == 0.0 ? 0.0 : v)
            return "n:\(normalized)"
        case .string(let v): return "s:\(v)"
        case .boolean(let v): return "b:\(v)"
        case .blob(let v): return "x:\(v.base64EncodedString())"
        case .compound(let values): return "c:[\(values.map { $0.indexKey }.joined(separator: ","))]"
        }
    }
}
