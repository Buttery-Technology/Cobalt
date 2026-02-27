import Foundation
import PantryCore

/// Per-index state: B-tree + bloom filter for a single column index
public actor ColumnIndex: Sendable {
    public let tableName: String
    public let columnName: String
    private let btree: BTree
    private var bloomFilter: BloomFilter

    public init(tableName: String, columnName: String, btree: BTree, expectedElements: Int = 10000) {
        self.tableName = tableName
        self.columnName = columnName
        self.btree = btree
        self.bloomFilter = BloomFilter(expectedElements: expectedElements)
    }

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

    /// Delete a key from this index
    public func delete(key: DBValue) async throws {
        try await btree.delete(key: key)
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
        let columnIndex = ColumnIndex(tableName: tableName, columnName: columnName, btree: btree)

        if indexes[tableName] == nil {
            indexes[tableName] = [:]
        }
        indexes[tableName]![columnName] = columnIndex
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

    // MARK: - IndexHook conformance

    public func lookupRecord(id: UInt64, tableName: String) async throws -> Int? {
        // Index lookup by record ID is not directly supported by column indexes
        // This would require a separate primary key index
        return nil
    }

    public func updateIndexes(record: Record, row: Row, tableName: String) async throws {
        guard let tableIndexes = indexes[tableName] else { return }

        for (columnName, columnIndex) in tableIndexes {
            if let value = row.values[columnName] {
                try await columnIndex.insert(key: value, row: row)
            }
        }
    }

    public func removeFromIndexes(id: UInt64, row: Row, tableName: String) async throws {
        guard let tableIndexes = indexes[tableName] else { return }

        for (columnName, columnIndex) in tableIndexes {
            if let value = row.values[columnName] {
                try await columnIndex.delete(key: value)
            }
        }
    }

    /// Attempt to use indexes for a query condition
    public func attemptIndexLookup(tableName: String, condition: WhereCondition) async throws -> [Row]? {
        guard let tableIndexes = indexes[tableName] else { return nil }

        switch condition {
        case .equals(let column, let value):
            if let index = tableIndexes[column] {
                return try await index.search(key: value)
            }
            return nil

        case .greaterThan(let column, let value):
            if let index = tableIndexes[column] {
                return try await index.searchRange(from: value, to: nil)
            }
            return nil

        case .lessThan(let column, let value):
            if let index = tableIndexes[column] {
                return try await index.searchRange(from: nil, to: value)
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

        case .and(let conditions):
            // Try index on first indexable condition
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

// MARK: - DBValue index key helper

extension DBValue {
    /// Convert to a string suitable for bloom filter hashing
    var indexKey: String {
        switch self {
        case .null: return "__null__"
        case .integer(let v): return "i:\(v)"
        case .double(let v): return "d:\(v)"
        case .string(let v): return "s:\(v)"
        case .boolean(let v): return "b:\(v)"
        case .blob(let v): return "x:\(v.base64EncodedString())"
        }
    }
}
