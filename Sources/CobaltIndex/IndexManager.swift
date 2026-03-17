import Foundation
import CobaltCore

/// Per-index state: B-tree + bloom filter for a single or compound column index.
/// Converted from actor to class with internal locking to eliminate actor hops.
public final class ColumnIndex: @unchecked Sendable {
    public let tableName: String
    public let columnName: String
    /// For compound indexes, the ordered list of columns that form the key
    public let compoundColumns: [String]?
    /// Extra non-key columns stored in index for covering index scans (INCLUDE columns)
    public let includeColumns: [String]?
    /// Optional WHERE condition for partial indexes — only rows matching this are indexed
    public let partialCondition: WhereCondition?
    public let btree: BTree
    public let nodeStore: PageBackedNodeStore

    private struct MutableState {
        var bloomFilter: BloomFilter
        var keyHashSet: Set<Int> = []
    }
    private let _mutable: CobaltRWLock<MutableState>
    /// Maximum size for hash set before it stops growing (memory bound)
    private static let maxHashSetSize = 1_000_000

    public init(tableName: String, columnName: String, compoundColumns: [String]? = nil, includeColumns: [String]? = nil, partialCondition: WhereCondition? = nil, btree: BTree, nodeStore: PageBackedNodeStore, expectedElements: Int = 10000) {
        self.tableName = tableName
        self.columnName = columnName
        self.compoundColumns = compoundColumns
        self.includeColumns = includeColumns
        self.partialCondition = partialCondition
        self.btree = btree
        self.nodeStore = nodeStore
        self._mutable = CobaltRWLock(MutableState(bloomFilter: BloomFilter(expectedElements: expectedElements, falsePositiveRate: 0.001)))
    }

    /// Whether this index has no entries (B-tree root is nil)
    public var isEmpty: Bool { btree.isEmpty }

    /// Whether this is a compound (multi-column) index
    public var isCompound: Bool { compoundColumns != nil }

    /// All columns available from this index (key columns + include columns)
    public var coveredColumns: Set<String> {
        var cols = Set<String>()
        if let compound = compoundColumns {
            cols.formUnion(compound)
        } else {
            cols.insert(columnName)
        }
        if let include = includeColumns {
            cols.formUnion(include)
        }
        return cols
    }

    /// Insert a key-row pair into this index
    public func insert(key: DBValue, row: Row) async throws {
        _mutable.withWriteLock { s in
            s.bloomFilter.add(key.indexKey)
            if s.keyHashSet.count < Self.maxHashSetSize {
                s.keyHashSet.insert(key.hashValue)
            }
        }
        try await btree.insert(key: key, row: row)
    }

    /// Batch insert: sorts keys for sequential B-tree traversal, flushes dirty nodes once at end.
    public func insertBatch(pairs: [(key: DBValue, row: Row)], deferFlush: Bool = false) async throws {
        let sorted = pairs.sorted { $0.key < $1.key }
        _mutable.withWriteLock { s in
            for (key, _) in sorted {
                s.bloomFilter.add(key.indexKey)
                if s.keyHashSet.count < Self.maxHashSetSize {
                    s.keyHashSet.insert(key.hashValue)
                }
            }
        }
        for (key, row) in sorted {
            try await btree.insert(key: key, row: row)
        }
        if !deferFlush {
            try await nodeStore.flushDirtyNodes()
        }
    }

    /// Bulk load: builds B-tree bottom-up from sorted pairs. O(n) vs O(n log n) for insertBatch.
    /// Only valid for initial population of an empty index.
    public func bulkLoad(pairs: [(key: DBValue, row: Row)]) async throws {
        let sorted = pairs.sorted { $0.key < $1.key }
        _mutable.withWriteLock { s in
            for (key, _) in sorted {
                s.bloomFilter.add(key.indexKey)
                if s.keyHashSet.count < Self.maxHashSetSize {
                    s.keyHashSet.insert(key.hashValue)
                }
            }
        }
        try await btree.bulkLoad(sortedPairs: sorted)
    }

    /// Synchronous search using only cached B-tree nodes. Returns nil on cache miss.
    /// Returns .some([]) for definitive bloom/hash misses, .some(rows) for hits, nil for cache miss.
    public func searchCached(key: DBValue) -> [Row]? {
        if !bloomCheckCached(key: key) { return [] }
        guard let results = btree.searchCached(key: key) else { return nil }
        return results.map { reconstructRow($0, key: key) }
    }

    /// Synchronous search returning only RIDs. Avoids Row/dictionary allocation entirely.
    /// Returns .some(Set) on hit/miss, nil on cache miss.
    public func searchCachedRIDs(key: DBValue) -> Set<UInt64>? {
        if !bloomCheckCached(key: key) { return Set() }
        guard let results = btree.searchCached(key: key) else { return nil }
        var rids = Set<UInt64>(minimumCapacity: results.count)
        for row in results {
            if case .integer(let ridSigned) = row.values["__rid"] {
                rids.insert(UInt64(bitPattern: ridSigned))
            }
        }
        return rids
    }

    /// Async search returning only RIDs. Avoids Row/dictionary allocation.
    public func searchRIDs(key: DBValue) async throws -> Set<UInt64> {
        if !bloomCheckCached(key: key) { return Set() }
        let results = try await btree.search(key: key)
        var rids = Set<UInt64>(minimumCapacity: results.count)
        for row in results {
            if case .integer(let ridSigned) = row.values["__rid"] {
                rids.insert(UInt64(bitPattern: ridSigned))
            }
        }
        return rids
    }

    /// Fused synchronous lookup returning just the first RID. Avoids Row allocation entirely.
    /// Returns .some(rid) on hit, .some(nil) on definitive miss, nil on cache miss.
    public func searchCachedFirstRID(key: DBValue) -> UInt64?? {
        if !bloomCheckCached(key: key) { return .some(nil) }
        guard let results = btree.searchCached(key: key) else { return nil } // cache miss
        guard !results.isEmpty else { return .some(nil) } // no match
        // Extract __rid directly without Row reconstruction
        if case .integer(let ridSigned) = results[0].values["__rid"] {
            return .some(UInt64(bitPattern: ridSigned))
        }
        return .some(nil)
    }

    /// PK-optimized variant: skips bloom filter entirely since PK values are always indexed.
    /// Use only when the caller knows this is a primary key index where every lookup key
    /// is guaranteed to have been inserted. Returns .some(rid) on hit, .some(nil) on miss,
    /// nil on cache miss.
    public func searchCachedFirstRIDNoBloom(key: DBValue) -> UInt64?? {
        guard let results = btree.searchCached(key: key) else { return nil } // cache miss
        guard !results.isEmpty else { return .some(nil) } // no match
        if case .integer(let ridSigned) = results[0].values["__rid"] {
            return .some(UInt64(bitPattern: ridSigned))
        }
        return .some(nil)
    }

    /// Check bloom filter + hash set under a single lock. Returns true if key might be present.
    /// Uses DBValue.hashValue for the hash set (no String allocation) and only falls through
    /// to the bloom filter (String-based) when the hash set is full.
    private func bloomCheckCached(key: DBValue) -> Bool {
        _mutable.withReadLock { s -> Bool in
            if s.keyHashSet.count < Self.maxHashSetSize {
                // Hash set is populated — use DBValue.hashValue directly (no String alloc)
                return s.keyHashSet.contains(key.hashValue)
            }
            // Hash set is full — fall through to bloom filter
            return s.bloomFilter.contains(key.indexKey)
        }
    }

    /// Search for rows matching a key, using hash set + bloom filter for fast negatives
    public func search(key: DBValue) async throws -> [Row]? {
        if !bloomCheckCached(key: key) { return [] }
        let results = try await btree.search(key: key)
        return results.map { reconstructRow($0, key: key) }
    }

    /// Synchronous batch search using only cached B-tree nodes. Returns nil on any cache miss.
    public func searchBatchCached(keys: [DBValue]) -> [DBValue: [Row]]? {
        let filteredKeys: [DBValue] = _mutable.withReadLock { s in
            keys.filter { key in
                if s.keyHashSet.count < Self.maxHashSetSize {
                    return s.keyHashSet.contains(key.hashValue)
                }
                return s.bloomFilter.contains(key.indexKey)
            }
        }
        guard !filteredKeys.isEmpty else { return [:] }

        // Sort keys and use single leaf-chain traversal instead of N individual searches
        let sortedKeys = filteredKeys.sorted()
        guard let rawResults = btree.searchBatchSortedCached(sortedKeys: sortedKeys) else { return nil }

        var results = [DBValue: [Row]]()
        for (key, rows) in rawResults where !rows.isEmpty {
            results[key] = rows.map { reconstructRow($0, key: key) }
        }
        return results
    }

    /// Batch search for multiple keys. Returns a dictionary of key -> [Row].
    /// Uses sorted leaf-chain traversal: O(log N + K + L) vs O(K * log N) for individual searches.
    public func searchBatch(keys: [DBValue]) async throws -> [DBValue: [Row]] {
        // Pre-filter keys through bloom filter + hash set under a single lock acquisition
        let filteredKeys: [DBValue] = _mutable.withReadLock { s in
            keys.filter { key in
                if s.keyHashSet.count < Self.maxHashSetSize {
                    return s.keyHashSet.contains(key.hashValue)
                }
                return s.bloomFilter.contains(key.indexKey)
            }
        }
        guard !filteredKeys.isEmpty else { return [:] }

        // Sort keys and use single leaf-chain traversal
        let sortedKeys = filteredKeys.sorted()
        let rawResults = try await btree.searchBatchSorted(sortedKeys: sortedKeys)

        var results = [DBValue: [Row]]()
        for (key, rows) in rawResults where !rows.isEmpty {
            results[key] = rows.map { reconstructRow($0, key: key) }
        }
        return results
    }

    /// Range query on this index (returns rows with column values reconstructed from keys)
    public func searchRange(from startKey: DBValue?, to endKey: DBValue?) async throws -> [Row] {
        let keyed = try await btree.searchRangeKeyed(from: startKey, to: endKey)
        return keyed.map { reconstructRow($0.1, key: $0.0) }
    }

    /// Range query with early termination after `limit` rows, in ascending or descending order
    public func searchRangeWithLimit(from startKey: DBValue?, to endKey: DBValue?, limit: Int, ascending: Bool = true) async throws -> [Row] {
        try await btree.searchRangeWithLimit(from: startKey, to: endKey, limit: limit, ascending: ascending)
    }

    /// Synchronous cache-only range query with limit. Returns nil on cache miss.
    public func searchRangeWithLimitCached(from startKey: DBValue?, to endKey: DBValue?, limit: Int, ascending: Bool = true) -> [Row]? {
        btree.searchRangeWithLimitCached(from: startKey, to: endKey, limit: limit, ascending: ascending)
    }

    /// Range query returning only RIDs in index order (no Row allocation)
    public func searchRangeWithLimitRIDs(from startKey: DBValue?, to endKey: DBValue?, limit: Int, ascending: Bool = true) async throws -> [UInt64] {
        try await btree.searchRangeWithLimitRIDs(from: startKey, to: endKey, limit: limit, ascending: ascending)
    }

    /// Synchronous cache-only RID range query. Returns nil on cache miss.
    public func searchRangeWithLimitRIDsCached(from startKey: DBValue?, to endKey: DBValue?, limit: Int, ascending: Bool = true) -> [UInt64]? {
        btree.searchRangeWithLimitRIDsCached(from: startKey, to: endKey, limit: limit, ascending: ascending)
    }

    /// Range scan from a lower bound, collecting while predicate holds on keys
    public func searchRangeWhile(from startKey: DBValue?, predicate: @Sendable (DBValue) -> Bool) async throws -> [Row] {
        try await btree.searchRangeWhile(from: startKey, predicate: predicate)
    }

    /// Count entries in a range without materializing Row objects
    public func countRange(from startKey: DBValue?, to endKey: DBValue?) async throws -> Int64 {
        try await btree.countRange(from: startKey, to: endKey)
    }

    /// Reconstruct a slim row from a TID-only row by adding column values from the B-tree key
    private func reconstructRow(_ row: Row, key: DBValue) -> Row {
        // If row already has column values (legacy data), return as-is
        if row.values.count > 1 { return row }
        var vals = row.values
        if let cols = compoundColumns {
            if case .compound(let parts) = key {
                for (i, col) in cols.enumerated() where i < parts.count {
                    vals[col] = parts[i]
                }
            }
        } else {
            vals[columnName] = key
        }
        return Row(values: vals)
    }

    /// Delete a specific (key, rid) entry from this index
    public func delete(key: DBValue, rid: DBValue? = nil) async throws {
        try await btree.delete(key: key, rid: rid)
    }

    /// Batch delete: sorts keys for cache locality and avoids per-delete root save overhead
    public func deleteBatch(pairs: [(key: DBValue, rid: DBValue)]) async throws {
        try await btree.deleteBatch(pairs: pairs)
    }

    /// Add a value to the bloom filter and hash set (used during index rebuild on load)
    public func addToBloomFilter(_ value: DBValue) {
        _mutable.withWriteLock { s in
            s.bloomFilter.add(value.indexKey)
            if s.keyHashSet.count < Self.maxHashSetSize {
                s.keyHashSet.insert(value.hashValue)
            }
        }
    }

    /// Get the current bloom filter snapshot for persistence
    public var bloomFilterSnapshot: BloomFilterSnapshot {
        _mutable.withReadLock { $0.bloomFilter.snapshot }
    }

    /// Restore bloom filter from a persisted snapshot
    public func restoreBloomFilter(_ snap: BloomFilterSnapshot) {
        _mutable.withWriteLock { s in
            if let restored = BloomFilter.fromSnapshot(snap) {
                s.bloomFilter = restored
            }
        }
    }
}

/// Coordinates per-table indexes; implements IndexHook for StorageEngine.
/// Converted from actor to class with internal locking to eliminate actor hops.
public final class IndexManager: IndexHook, @unchecked Sendable {
    /// table name -> column name -> ColumnIndex
    private let _indexes: CobaltLock<[String: [String: ColumnIndex]]>
    private let bufferPool: BufferPoolManager
    private let storageManager: StorageManager

    public init(bufferPool: BufferPoolManager, storageManager: StorageManager) {
        self.bufferPool = bufferPool
        self.storageManager = storageManager
        self._indexes = CobaltLock([:])
    }

    /// Create a new index for a table column, optionally with INCLUDE columns for covering scans
    public func createIndex(tableName: String, columnName: String, includeColumns: [String]? = nil, partialCondition: WhereCondition? = nil) -> ColumnIndex {
        let nodeStore = PageBackedNodeStore(bufferPool: bufferPool, storageManager: storageManager)
        let btree = BTree(order: 64, nodeStore: nodeStore)
        let columnIndex = ColumnIndex(tableName: tableName, columnName: columnName, includeColumns: includeColumns, partialCondition: partialCondition, btree: btree, nodeStore: nodeStore)

        _indexes.withLock { indexes in
            if indexes[tableName] == nil {
                indexes[tableName] = [:]
            }
            indexes[tableName]![columnName] = columnIndex
        }
        return columnIndex
    }

    /// Create a compound index over multiple columns
    public func createCompoundIndex(tableName: String, columns: [String]) throws -> ColumnIndex {
        guard columns.count >= 2 else {
            throw CobaltError.invalidQuery(description: "Compound index requires at least 2 columns")
        }
        let indexName = columns.joined(separator: "+")
        let nodeStore = PageBackedNodeStore(bufferPool: bufferPool, storageManager: storageManager)
        let btree = BTree(order: 64, nodeStore: nodeStore)
        let columnIndex = ColumnIndex(tableName: tableName, columnName: indexName, compoundColumns: columns, btree: btree, nodeStore: nodeStore)

        _indexes.withLock { indexes in
            if indexes[tableName] == nil {
                indexes[tableName] = [:]
            }
            indexes[tableName]![indexName] = columnIndex
        }
        return columnIndex
    }

    /// Get an existing index
    public func getIndex(tableName: String, columnName: String) -> ColumnIndex? {
        _indexes.withLock { $0[tableName]?[columnName] }
    }

    /// Get all indexes for a table
    public func getIndexes(tableName: String) -> [String: ColumnIndex] {
        _indexes.withLock { $0[tableName] ?? [:] }
    }

    /// Remove all indexes for a table
    public func removeIndexes(tableName: String) {
        _indexes.withLock { $0.removeValue(forKey: tableName) }
    }

    /// List all indexes on a table as (column, isCompound) pairs
    public func listIndexes(tableName: String) -> [(column: String, isCompound: Bool)] {
        _indexes.withLock { indexes in
            guard let tableIndexes = indexes[tableName] else { return [] }
            return tableIndexes.map { (column: $0.key, isCompound: $0.value.compoundColumns != nil) }
        }
    }

    /// Check if an index exists on a table column
    public func hasIndex(tableName: String, columnName: String) -> Bool {
        _indexes.withLock { $0[tableName]?[columnName] != nil }
    }

    /// Drop a single index by table and column name
    public func dropIndex(tableName: String, columnName: String) {
        _indexes.withLock { $0[tableName]?.removeValue(forKey: columnName) }
    }

    // MARK: - Persistence

    /// Save all index metadata to a registry page
    public func saveIndexRegistry() async throws -> [IndexRegistryEntry] {
        // Snapshot current indexes under lock
        let allIndexes: [(String, String, ColumnIndex)] = _indexes.withLock { indexes in
            var result = [(String, String, ColumnIndex)]()
            for (tableName, tableIndexes) in indexes {
                for (columnName, columnIndex) in tableIndexes {
                    result.append((tableName, columnName, columnIndex))
                }
            }
            return result
        }
        // Flush all dirty B-tree nodes to pages before saving registry
        for (_, _, columnIndex) in allIndexes {
            try await columnIndex.nodeStore.flushDirtyNodes()
        }
        var entries: [IndexRegistryEntry] = []
        for (tableName, columnName, columnIndex) in allIndexes {
            let rootId = columnIndex.btree.getRootId()
            let nodePageMap = columnIndex.nodeStore.getNodePageMap()
            let compoundCols = columnIndex.compoundColumns
            let bloomSnap = columnIndex.bloomFilterSnapshot
            entries.append(IndexRegistryEntry(
                tableName: tableName,
                columnName: columnName,
                compoundColumns: compoundCols,
                rootNodeId: rootId,
                nodePageMap: nodePageMap,
                bloomFilterSnapshot: bloomSnap
            ))
        }
        return entries
    }

    /// Restore indexes from persisted registry entries
    public func loadIndexRegistry(entries: [IndexRegistryEntry]) async throws {
        for entry in entries {
            let nodeStore = PageBackedNodeStore(bufferPool: bufferPool, storageManager: storageManager)
            nodeStore.restoreNodePageMap(entry.decodedNodePageMap)

            let btree = BTree(order: 64, nodeStore: nodeStore)
            btree.setRootId(entry.rootNodeId)

            let columnIndex = ColumnIndex(
                tableName: entry.tableName,
                columnName: entry.columnName,
                compoundColumns: entry.compoundColumns,
                btree: btree,
                nodeStore: nodeStore
            )

            // Restore bloom filter from persisted snapshot, or rebuild from B-tree data
            if let snap = entry.bloomFilterSnapshot {
                columnIndex.restoreBloomFilter(snap)
            } else if entry.rootNodeId != nil {
                let allRows = try await btree.searchRange(from: nil, to: nil)
                for row in allRows {
                    if let compoundCols = entry.compoundColumns {
                        let keyValues = compoundCols.map { row.values[$0] ?? .null }
                        columnIndex.addToBloomFilter(.compound(keyValues))
                    } else if let value = row.values[entry.columnName] {
                        columnIndex.addToBloomFilter(value)
                    }
                }
            }

            _indexes.withLock { indexes in
                if indexes[entry.tableName] == nil {
                    indexes[entry.tableName] = [:]
                }
                indexes[entry.tableName]![entry.columnName] = columnIndex
            }
        }
    }

    // MARK: - IndexHook conformance

    public func lookupRecord(id: UInt64, tableName: String) async throws -> Int? {
        // Index lookup by record ID is not directly supported by column indexes
        // This would require a separate primary key index
        return nil
    }

    public func updateIndexes(record: Record, row: Row, tableName: String) async throws {
        let tableIndexes: [String: ColumnIndex]? = _indexes.withLock { $0[tableName] }
        guard let tableIndexes else { return }

        let rid: DBValue = .integer(Int64(bitPattern: record.id))

        for (_, columnIndex) in tableIndexes {
            // Skip row if partial index condition is not satisfied
            if let condition = columnIndex.partialCondition {
                if !evaluateConditionForIndex(condition, row: row) { continue }
            }

            if let columns = columnIndex.compoundColumns {
                // Compound index: key contains all indexed column values
                let keyValues = columns.map { col -> DBValue in row.values[col] ?? .null }
                let compoundKey = DBValue.compound(keyValues)
                let tidRow = Row(values: ["__rid": rid])
                try await columnIndex.insert(key: compoundKey, row: tidRow)
            } else {
                let columnName = columnIndex.columnName
                if let value = row.values[columnName] {
                    var vals: [String: DBValue] = ["__rid": rid, columnName: value]
                    if let include = columnIndex.includeColumns {
                        for incCol in include { vals[incCol] = row.values[incCol] ?? .null }
                    }
                    try await columnIndex.insert(key: value, row: Row(values: vals))
                }
            }
        }
    }

    /// Batch update all indexes for a set of inserted records.
    /// Groups keys per index, sorts for sequential B-tree traversal, flushes once at end.
    public func updateIndexesBatch(records: [(Record, Row)], tableName: String) async throws {
        let tableIndexes: [String: ColumnIndex]? = _indexes.withLock { $0[tableName] }
        guard let tableIndexes else { return }

        let multipleIndexes = tableIndexes.count > 1
        var indexesToFlush: [ColumnIndex] = multipleIndexes ? [] : []

        for (_, columnIndex) in tableIndexes {
            let condition = columnIndex.partialCondition
            let compoundCols = columnIndex.compoundColumns
            let colName = columnIndex.columnName
            let includeCols = columnIndex.includeColumns

            var pairs: [(key: DBValue, row: Row)] = []
            pairs.reserveCapacity(records.count)

            for (record, row) in records {
                if let condition = condition {
                    if !evaluateConditionForIndex(condition, row: row) { continue }
                }
                let rid: DBValue = .integer(Int64(bitPattern: record.id))

                if let columns = compoundCols {
                    let keyValues = columns.map { col -> DBValue in row.values[col] ?? .null }
                    pairs.append((key: .compound(keyValues), row: Row(values: ["__rid": rid])))
                } else if let value = row.values[colName] {
                    var vals: [String: DBValue] = ["__rid": rid, colName: value]
                    if let include = includeCols {
                        for incCol in include { vals[incCol] = row.values[incCol] ?? .null }
                    }
                    pairs.append((key: value, row: Row(values: vals)))
                }
            }

            if !pairs.isEmpty {
                // Use bulkLoad (O(n)) for empty indexes, insertBatch (O(n log n)) otherwise
                if columnIndex.isEmpty {
                    try await columnIndex.bulkLoad(pairs: pairs)
                } else {
                    // Defer flush when multiple indexes — single batch flush at end
                    try await columnIndex.insertBatch(pairs: pairs, deferFlush: multipleIndexes)
                    if multipleIndexes { indexesToFlush.append(columnIndex) }
                }
            }
        }

        // Single batch flush for all deferred indexes
        if multipleIndexes {
            for ci in indexesToFlush {
                try await ci.nodeStore.flushDirtyNodes()
            }
        }
    }

    public func removeFromIndexes(id: UInt64, row: Row, tableName: String) async throws {
        let tableIndexes: [String: ColumnIndex]? = _indexes.withLock { $0[tableName] }
        guard let tableIndexes else { return }

        let rid: DBValue = .integer(Int64(bitPattern: id))

        for (_, columnIndex) in tableIndexes {
            if let condition = columnIndex.partialCondition {
                if !evaluateConditionForIndex(condition, row: row) { continue }
            }

            if let columns = columnIndex.compoundColumns {
                let keyValues = columns.map { col -> DBValue in row.values[col] ?? .null }
                let compoundKey = DBValue.compound(keyValues)
                try await columnIndex.delete(key: compoundKey, rid: rid)
            } else {
                let columnName = columnIndex.columnName
                if let value = row.values[columnName] {
                    try await columnIndex.delete(key: value, rid: rid)
                }
            }
        }
    }

    /// Batch remove from indexes — collects all deletes per index, processes once.
    public func removeFromIndexesBatch(records: [(id: UInt64, row: Row)], tableName: String) async throws {
        let tableIndexes: [String: ColumnIndex]? = _indexes.withLock { $0[tableName] }
        guard let tableIndexes else { return }

        // Build pairs for each index, then delete in parallel
        var indexWork: [(ColumnIndex, [(key: DBValue, rid: DBValue)])] = []
        for (_, columnIndex) in tableIndexes {
            let condition = columnIndex.partialCondition
            let compoundCols = columnIndex.compoundColumns
            let colName = columnIndex.columnName

            var pairs: [(key: DBValue, rid: DBValue)] = []
            pairs.reserveCapacity(records.count)
            for (id, row) in records {
                if let condition = condition {
                    if !evaluateConditionForIndex(condition, row: row) { continue }
                }
                let rid: DBValue = .integer(Int64(bitPattern: id))
                if let columns = compoundCols {
                    let keyValues = columns.map { col -> DBValue in row.values[col] ?? .null }
                    pairs.append((key: .compound(keyValues), rid: rid))
                } else if let value = row.values[colName] {
                    pairs.append((key: value, rid: rid))
                }
            }
            if !pairs.isEmpty {
                indexWork.append((columnIndex, pairs))
            }
        }

        try await withThrowingTaskGroup(of: Void.self) { group in
            for (columnIndex, pairs) in indexWork {
                group.addTask {
                    try await columnIndex.deleteBatch(pairs: pairs)
                }
            }
            try await group.waitForAll()
        }
    }

    /// Batch remove from indexes using raw positional data — avoids full Row deserialization.
    /// Extracts only the indexed column value via O(1) positional access, then sorts by key for cache locality.
    public func removeFromIndexesBatchRaw(records: [(id: UInt64, data: Data)], tableName: String, schema: CobaltTableSchema) async throws {
        let tableIndexes: [String: ColumnIndex]? = _indexes.withLock { $0[tableName] }
        guard let tableIndexes else { return }

        // Build pairs for each index (CPU-bound), then delete in parallel
        var indexWork: [(ColumnIndex, [(key: DBValue, rid: DBValue)])] = []
        for (_, columnIndex) in tableIndexes {
            let condition = columnIndex.partialCondition
            let compoundCols = columnIndex.compoundColumns
            let colName = columnIndex.columnName

            // Extract keys from raw data using O(1) positional access
            var pairs: [(key: DBValue, rid: DBValue)] = []
            pairs.reserveCapacity(records.count)

            if let columns = compoundCols {
                // Compound index: extract multiple columns
                let colIndices = columns.compactMap { schema.columnOrdinals[$0] }
                guard colIndices.count == columns.count else { continue }
                for (id, data) in records {
                    if condition != nil {
                        guard let row = Row.fromBytesAuto(data, schema: schema),
                              evaluateConditionForIndex(condition!, row: row) else { continue }
                    }
                    let keyValues = colIndices.map { idx -> DBValue in
                        Row.extractColumnValue(from: data, columnIndex: idx) ?? .null
                    }
                    let compoundKey = DBValue.compound(keyValues)
                    let rid: DBValue = .integer(Int64(bitPattern: id))
                    pairs.append((key: compoundKey, rid: rid))
                }
            } else if let colIdx = schema.columnOrdinals[colName] {
                // Single column index: extract one column
                for (id, data) in records {
                    if condition != nil {
                        guard let row = Row.fromBytesAuto(data, schema: schema),
                              evaluateConditionForIndex(condition!, row: row) else { continue }
                    }
                    guard let value = Row.extractColumnValue(from: data, columnIndex: colIdx),
                          value != .null else { continue }
                    let rid: DBValue = .integer(Int64(bitPattern: id))
                    pairs.append((key: value, rid: rid))
                }
            }

            if !pairs.isEmpty {
                indexWork.append((columnIndex, pairs))
            }
        }

        // Delete from all indexes concurrently
        try await withThrowingTaskGroup(of: Void.self) { group in
            for (columnIndex, pairs) in indexWork {
                group.addTask {
                    try await columnIndex.deleteBatch(pairs: pairs)
                }
            }
            try await group.waitForAll()
        }
    }

    /// Synchronous cache-only RID lookup for .equals conditions. Skips Row allocation entirely.
    public func attemptIndexLookupCachedRIDs(tableName: String, condition: WhereCondition) -> Set<UInt64>? {
        guard case .equals(let column, let value) = condition else { return nil }
        if value == .null { return Set() }
        let index: ColumnIndex? = _indexes.withLock { $0[tableName]?[column] }
        guard let index else { return nil }
        return index.searchCachedRIDs(key: value)
    }

    /// Async RID-only lookup for .equals conditions. Skips Row allocation entirely.
    public func attemptIndexLookupRIDs(tableName: String, condition: WhereCondition) async throws -> Set<UInt64>? {
        guard case .equals(let column, let value) = condition else { return nil }
        if value == .null { return Set() }
        let index: ColumnIndex? = _indexes.withLock { $0[tableName]?[column] }
        guard let index else { return nil }
        return try await index.searchRIDs(key: value)
    }

    /// Synchronous cache-only index lookup for .equals conditions. Returns nil on cache miss.
    public func attemptIndexLookupCached(tableName: String, condition: WhereCondition) -> [Row]? {
        guard case .equals(let column, let value) = condition else { return nil }
        if value == .null { return [] }
        let index: ColumnIndex? = _indexes.withLock { $0[tableName]?[column] }
        guard let index else { return nil }
        return index.searchCached(key: value)
    }

    /// Attempt to use indexes for a query condition
    public func attemptIndexLookup(tableName: String, condition: WhereCondition) async throws -> [Row]? {
        let tableIndexes: [String: ColumnIndex]? = _indexes.withLock { $0[tableName] }
        guard let tableIndexes else { return nil }

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
                    guard let compoundColumns = columnIndex.compoundColumns else { continue }
                    // Full compound key match: all columns present
                    if compoundColumns.allSatisfy({ equalsMap[$0] != nil }) {
                        let keyValues = compoundColumns.map { equalsMap[$0]! }
                        let compoundKey = DBValue.compound(keyValues)
                        if let results = try await columnIndex.search(key: compoundKey) {
                            return results
                        }
                    }
                    // Prefix match: first N columns present (bounded range scan on prefix)
                    let prefixLen = compoundColumns.prefix(while: { equalsMap[$0] != nil }).count
                    if prefixLen > 0 && prefixLen < compoundColumns.count {
                        let prefixValues = compoundColumns.prefix(prefixLen).map { equalsMap[$0]! }
                        let lowerBound = DBValue.compound(prefixValues)
                        let results = try await columnIndex.searchRangeWhile(from: lowerBound) { key in
                            guard case .compound(let keyParts) = key, keyParts.count >= prefixLen else { return false }
                            for i in 0..<prefixLen {
                                if keyParts[i] != prefixValues[i] { return false }
                            }
                            return true
                        }
                        return results
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

/// Lightweight WHERE condition evaluator for partial index filtering (no QueryExecutor dependency)
private func evaluateConditionForIndex(_ condition: WhereCondition, row: Row) -> Bool {
    switch condition {
    case let .equals(column, value):
        if value == .null { return false }
        guard let rv = row.values[column], rv != .null else { return false }
        return rv == value
    case let .notEquals(column, value):
        if value == .null { return false }
        guard let rv = row.values[column], rv != .null else { return false }
        return rv != value
    case let .lessThan(column, value):
        guard let rv = row.values[column], rv != .null, value != .null else { return false }
        return rv < value
    case let .greaterThan(column, value):
        guard let rv = row.values[column], rv != .null, value != .null else { return false }
        return rv > value
    case let .lessThanOrEqual(column, value):
        guard let rv = row.values[column], rv != .null, value != .null else { return false }
        return rv <= value
    case let .greaterThanOrEqual(column, value):
        guard let rv = row.values[column], rv != .null, value != .null else { return false }
        return rv >= value
    case let .isNull(column):
        return row.values[column] == nil || row.values[column] == .null
    case let .isNotNull(column):
        return row.values[column] != nil && row.values[column] != .null
    case let .and(conditions):
        return conditions.allSatisfy { evaluateConditionForIndex($0, row: row) }
    case let .or(conditions):
        return conditions.contains { evaluateConditionForIndex($0, row: row) }
    case let .in(column, values):
        guard let rv = row.values[column], rv != .null else { return false }
        return values.contains(rv)
    case let .between(column, min, max):
        guard let rv = row.values[column], rv != .null else { return false }
        return rv >= min && rv <= max
    case let .like(column, pattern):
        guard let rv = row.values[column], case .string(let str) = rv else { return false }
        return matchLikeForIndex(str, pattern: pattern)
    }
}

/// Simple LIKE pattern match for partial index evaluation
private func matchLikeForIndex(_ string: String, pattern: String) -> Bool {
    let s = Array(string), p = Array(pattern)
    var si = 0, pi = 0, starSi = -1, starPi = -1
    while si < s.count {
        if pi < p.count && p[pi] == "%" { starPi = pi; starSi = si; pi += 1 }
        else if pi < p.count && (p[pi] == "_" || p[pi] == s[si]) { si += 1; pi += 1 }
        else if starPi >= 0 { pi = starPi + 1; starSi += 1; si = starSi }
        else { return false }
    }
    while pi < p.count && p[pi] == "%" { pi += 1 }
    return pi == p.count
}

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
