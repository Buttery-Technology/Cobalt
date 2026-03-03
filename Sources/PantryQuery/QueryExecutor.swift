import Foundation
import PantryCore
import PantryIndex

/// Aggregate functions supported by the query executor.
public enum AggregateFunction: Sendable {
    case count(column: String?)
    case sum(column: String)
    case avg(column: String)
    case min(column: String)
    case max(column: String)
}

/// Key for query result cache
private struct QueryResultCacheKey: Hashable {
    let table: String
    let conditionSignature: String
    let columns: [String]?
    let modifiersSignature: String
    /// Additional tables involved (e.g., join targets). Empty for single-table queries.
    let joinTables: [String]
    init(table: String, conditionSignature: String, columns: [String]?, modifiersSignature: String, joinTables: [String] = []) {
        self.table = table
        self.conditionSignature = conditionSignature
        self.columns = columns
        self.modifiersSignature = modifiersSignature
        self.joinTables = joinTables
    }
}

/// Cached query result with generation tracking and page-level invalidation
private struct CachedQueryResult {
    let rows: [Row]
    let generation: UInt64
    var lastAccess: UInt64
    /// Pages accessed to produce this result. nil = unknown (conservative invalidation).
    let accessedPages: Set<Int>?
    /// Generations of additional tables at cache time (for join queries).
    let joinGenerations: [String: UInt64]
}

/// Executes SELECT, INSERT, UPDATE, DELETE queries against the storage engine.
/// Uses cost-based QueryPlanner for index vs scan decisions and join ordering.
/// Converted from actor to class with internal locking to eliminate actor hops.
public final class QueryExecutor: @unchecked Sendable {
    private let storageEngine: StorageEngine
    private let indexManager: IndexManager
    private let planner: QueryPlanner

    private struct CacheState {
        var resultCache: [QueryResultCacheKey: CachedQueryResult] = [:]
        var tableGenerations: [String: UInt64] = [:]
        var resultCacheCounter: UInt64 = 0
    }
    private let _cache: PantryLock<CacheState>
    /// Maximum cached query results
    private let maxResultCacheSize = 128

    public init(storageEngine: StorageEngine, indexManager: IndexManager, tableRegistry: TableRegistry, costWeights: CostModelWeights = .default) {
        self.storageEngine = storageEngine
        self.indexManager = indexManager
        self.planner = QueryPlanner(storageEngine: storageEngine, indexManager: indexManager, registry: tableRegistry, costWeights: costWeights)
        self._cache = PantryLock(CacheState())
    }

    /// Convenience init — resolves tableRegistry from storageEngine
    public init(storageEngine: StorageEngine, indexManager: IndexManager, costWeights: CostModelWeights = .default) {
        self.storageEngine = storageEngine
        self.indexManager = indexManager
        let reg = storageEngine.tableRegistry
        self.planner = QueryPlanner(storageEngine: storageEngine, indexManager: indexManager, registry: reg, costWeights: costWeights)
        self._cache = PantryLock(CacheState())
    }

    /// Invalidate the query plan cache (call after index create/drop or schema changes)
    public func invalidatePlanCache() {
        planner.invalidateCache()
    }

    /// Invalidate cached plans for a specific table only
    public func invalidatePlanCache(forTable table: String) {
        planner.invalidateCache(forTable: table)
    }

    /// Invalidate result cache for a specific table (called after mutations).
    /// If modifiedPages is provided, only entries whose accessed pages overlap are invalidated.
    private func invalidateResultCache(forTable table: String, modifiedPages: Set<Int>? = nil) {
        _cache.withLock { cs in
            cs.tableGenerations[table, default: 0] += 1
            if let modified = modifiedPages {
                cs.resultCache = cs.resultCache.filter { entry in
                    // Invalidate if primary table matches or if this table is a join target
                    let involves = entry.key.table == table || entry.key.joinTables.contains(table)
                    guard involves else { return true }
                    guard let accessed = entry.value.accessedPages else { return false }
                    return accessed.isDisjoint(with: modified)
                }
            } else {
                cs.resultCache = cs.resultCache.filter { entry in
                    entry.key.table != table && !entry.key.joinTables.contains(table)
                }
            }
        }
    }

    /// Look up a cached query result, returns nil on miss or stale generation
    private func lookupResultCache(key: QueryResultCacheKey) -> [Row]? {
        _cache.withLock { cs in
            guard let cached = cs.resultCache[key],
                  cached.generation == cs.tableGenerations[key.table, default: 0] else {
                return nil
            }
            // For join queries, also verify all join table generations are still valid
            for (jt, gen) in cached.joinGenerations {
                guard gen == cs.tableGenerations[jt, default: 0] else { return nil }
            }
            cs.resultCacheCounter += 1
            cs.resultCache[key]?.lastAccess = cs.resultCacheCounter
            return cached.rows
        }
    }

    /// Store a query result in the cache with LRU eviction
    private func storeResultCache(key: QueryResultCacheKey, rows: [Row], accessedPages: Set<Int>? = nil) {
        // Don't cache very large result sets (>10K rows)
        guard rows.count <= 10_000 else { return }

        _cache.withLock { cs in
            if cs.resultCache.count >= maxResultCacheSize {
                let evictCount = maxResultCacheSize / 4
                var evictKeys = [QueryResultCacheKey]()
                evictKeys.reserveCapacity(evictCount)
                var evictAccesses = [UInt64]()
                evictAccesses.reserveCapacity(evictCount)
                var maxInSet: UInt64 = 0

                for (key, value) in cs.resultCache {
                    if evictKeys.count < evictCount {
                        evictKeys.append(key)
                        evictAccesses.append(value.lastAccess)
                        if value.lastAccess > maxInSet { maxInSet = value.lastAccess }
                    } else if value.lastAccess < maxInSet {
                        if let maxIdx = evictAccesses.firstIndex(of: maxInSet) {
                            evictKeys[maxIdx] = key
                            evictAccesses[maxIdx] = value.lastAccess
                            maxInSet = evictAccesses.max() ?? 0
                        }
                    }
                }
                for key in evictKeys {
                    cs.resultCache.removeValue(forKey: key)
                }
            }

            // Capture generations for all join tables
            var joinGens = [String: UInt64]()
            for jt in key.joinTables {
                joinGens[jt] = cs.tableGenerations[jt, default: 0]
            }

            cs.resultCacheCounter += 1
            cs.resultCache[key] = CachedQueryResult(
                rows: rows,
                generation: cs.tableGenerations[key.table, default: 0],
                lastAccess: cs.resultCacheCounter,
                accessedPages: accessedPages,
                joinGenerations: joinGens
            )
        }
    }

    // MARK: - Synchronous Point Lookup

    /// Fully synchronous point lookup: index bloom check → B-tree → mmap record read.
    /// Returns nil if the fast path cannot handle this query (caller should fall back to async).
    /// No actor hops, no async overhead, no result cache.
    public func executeSelectSync(from table: String, columns: [String]? = nil, where condition: WhereCondition, limit: Int = 1) -> [Row]? {
        guard limit == 1 else { return nil }
        guard case .equals(let column, let value) = condition, value != .null else { return nil }
        guard let index = indexManager.getIndex(tableName: table, columnName: column) else { return nil }
        guard let ridResult = index.searchCachedFirstRID(key: value) else { return nil } // cache miss
        guard let rid = ridResult else { return [] } // definitive miss
        guard let syncRow = storageEngine.getRecordByIDSync(rid, tableName: table) else { return nil }
        if let columns = columns, !columns.isEmpty {
            let projected = columns.reduce(into: [String: DBValue]()) { r, c in r[c] = syncRow.values[c] ?? .null }
            return [Row(values: projected)]
        }
        return [syncRow]
    }

    // MARK: - SELECT

    public func executeSelect(from table: String, columns: [String]? = nil, where condition: WhereCondition? = nil, modifiers: QueryModifiers? = nil, transactionContext: TransactionContext? = nil) async throws -> [Row] {
        // Ultra-fast path: equality lookup on indexed column with LIMIT 1 (e.g., WHERE pk = ? LIMIT 1)
        // Runs BEFORE cache key computation to avoid cache overhead when sync mmap succeeds.
        if transactionContext == nil,
           let cond = condition, let mods = modifiers, mods.limit == 1, !mods.distinct,
           (mods.orderBy == nil || mods.orderBy!.isEmpty) {
            if case .equals(let column, let value) = cond, value != .null {
                if let index = indexManager.getIndex(tableName: table, columnName: column) {
                    if let ridResult = index.searchCachedFirstRID(key: value) {
                        guard let rid = ridResult else {
                            // Definitive bloom/B-tree miss — no such key
                            return []
                        }
                        // Try fully synchronous mmap path (no async, no cache overhead)
                        if let syncRow = storageEngine.getRecordByIDSync(rid, tableName: table) {
                            if let columns = columns, !columns.isEmpty {
                                let projected = columns.reduce(into: [String: DBValue]()) { r, c in r[c] = syncRow.values[c] ?? .null }
                                return [Row(values: projected)]
                            }
                            return [syncRow]
                        }
                        // Sync mmap unavailable (dirty page, encrypted, etc.) — fall through to cached path
                    }
                    // Fall through on index cache miss — normal path will handle it
                }
            }
        }

        // Query result cache: skip for transactional queries (they need fresh data)
        let cacheKey: QueryResultCacheKey?
        if transactionContext == nil {
            let key = QueryResultCacheKey(
                table: table,
                conditionSignature: condition.map { queryConditionSignature($0) } ?? "",
                columns: columns,
                modifiersSignature: modifiers.map { queryModifiersSignature($0) } ?? ""
            )
            if let cached = lookupResultCache(key: key) {
                return cached
            }
            cacheKey = key
        } else {
            cacheKey = nil
        }

        var rows: [Row]

        // Fast path: ORDER BY + LIMIT on a single indexed column → index-ordered scan with early exit
        // Works with or without WHERE condition. For WHERE, overfetches and filters.
        if let mods = modifiers,
           let orderBy = mods.orderBy, orderBy.count == 1,
           let limit = mods.limit, limit > 0,
           !mods.distinct {
            let orderCol = orderBy[0].column
            let ascending = orderBy[0].direction == .ascending
            if let index = indexManager.getIndex(tableName: table, columnName: orderCol) {
                let totalNeeded = limit + (mods.offset ?? 0)

                // Check if this is a covering index (has all needed columns)
                let coveredCols = index.coveredColumns
                let neededCols: Set<String>
                if let columns = columns, !columns.isEmpty {
                    var needed = Set(columns)
                    if let cond = condition { needed.formUnion(columnsReferenced(in: cond)) }
                    neededCols = needed
                } else {
                    neededCols = [] // SELECT * — not coverable unless we know all table columns
                }
                let isCovering = !neededCols.isEmpty && neededCols.isSubset(of: coveredCols)

                // Adaptive over-fetch: start at 2x, expand to 4x only if insufficient
                var fetchLimit = condition != nil ? totalNeeded * 2 : totalNeeded

                var orderedRows: [Row] = []
                for attempt in 0..<2 {
                    if isCovering {
                        let indexRows: [Row]
                        if let cached = index.searchRangeWithLimitCached(from: nil, to: nil, limit: fetchLimit, ascending: ascending) {
                            indexRows = cached
                        } else {
                            indexRows = try await index.searchRangeWithLimit(from: nil, to: nil, limit: fetchLimit, ascending: ascending)
                        }
                        orderedRows = indexRows
                    } else {
                        let orderedRIDs: [UInt64]
                        if let cached = index.searchRangeWithLimitRIDsCached(from: nil, to: nil, limit: fetchLimit, ascending: ascending) {
                            orderedRIDs = cached
                        } else {
                            orderedRIDs = try await index.searchRangeWithLimitRIDs(from: nil, to: nil, limit: fetchLimit, ascending: ascending)
                        }
                        // Sync mmap fast path: resolve RIDs without async overhead
                        if transactionContext == nil && orderedRIDs.count <= 64 {
                            var syncRows = [Row]()
                            syncRows.reserveCapacity(orderedRIDs.count)
                            var allSync = true
                            for rid in orderedRIDs {
                                if let row = storageEngine.getRecordByIDSync(rid, tableName: table) {
                                    syncRows.append(row)
                                } else {
                                    allSync = false
                                    break
                                }
                            }
                            if allSync {
                                orderedRows = syncRows
                            } else {
                                orderedRows = try await storageEngine.getRecordsByIDsOrdered(orderedRIDs, tableName: table, transactionContext: transactionContext)
                            }
                        } else {
                            orderedRows = try await storageEngine.getRecordsByIDsOrdered(orderedRIDs, tableName: table, transactionContext: transactionContext)
                        }
                    }

                    if let condition = condition {
                        orderedRows = orderedRows.filter { evaluateCondition(condition, row: $0) }
                    }

                    // Enough results or no WHERE (exact fetch) → done
                    if orderedRows.count >= totalNeeded || condition == nil { break }
                    // First attempt with 2x wasn't enough — retry with 4x
                    if attempt == 0 { fetchLimit = totalNeeded * 4 } else { break }
                }

                if orderedRows.count >= totalNeeded || condition == nil {
                    // Apply OFFSET then LIMIT (already sorted by index)
                    if let offset = mods.offset, offset > 0 {
                        orderedRows = Array(orderedRows.dropFirst(offset))
                    }
                    rows = Array(orderedRows.prefix(limit))

                    // Project columns
                    if let columns = columns, !columns.isEmpty {
                        let projected = rows.map { row in
                            let p = columns.reduce(into: [String: DBValue]()) { r, c in r[c] = row.values[c] ?? .null }
                            return Row(values: p)
                        }
                        if let key = cacheKey { storeResultCache(key: key, rows: projected) }
                        return projected
                    }
                    if let key = cacheKey { storeResultCache(key: key, rows: rows) }
                    return rows
                }
                // Not enough rows from overfetch — fall through to full scan
            }
        }

        // Fast path: ORDER BY on indexed column without LIMIT → full index scan in sorted order
        // Avoids materializing all rows and sorting in memory
        if let mods = modifiers,
           let orderBy = mods.orderBy, orderBy.count == 1,
           (mods.limit == nil || mods.limit == 0),
           !mods.distinct {
            let orderCol = orderBy[0].column
            let ascending = orderBy[0].direction == .ascending
            if let index = indexManager.getIndex(tableName: table, columnName: orderCol) {
                // Full index scan in sorted order — no LIMIT cap
                let indexRows = try await index.searchRangeWithLimit(from: nil, to: nil, limit: Int.max, ascending: ascending)

                // Check covering index
                let coveredCols = index.coveredColumns
                let neededCols: Set<String>
                if let columns = columns, !columns.isEmpty {
                    var needed = Set(columns)
                    if let cond = condition { needed.formUnion(columnsReferenced(in: cond)) }
                    neededCols = needed
                } else {
                    neededCols = []
                }
                let isCovering = !neededCols.isEmpty && neededCols.isSubset(of: coveredCols)

                var orderedRows: [Row]
                if isCovering {
                    orderedRows = indexRows
                } else {
                    // Use RID-only scan + ordered fetch to preserve index order
                    let orderedRIDs = try await index.searchRangeWithLimitRIDs(from: nil, to: nil, limit: Int.max, ascending: ascending)
                    orderedRows = try await storageEngine.getRecordsByIDsOrdered(orderedRIDs, tableName: table, transactionContext: transactionContext)
                }

                // Apply WHERE filter if present
                if let condition = condition {
                    orderedRows = orderedRows.filter { evaluateCondition(condition, row: $0) }
                }

                // Apply OFFSET (already sorted by index)
                if let offset = mods.offset, offset > 0 {
                    orderedRows = Array(orderedRows.dropFirst(offset))
                }

                // Project columns
                if let columns = columns, !columns.isEmpty {
                    let projected = orderedRows.map { row in
                        let projectedValues = columns.reduce(into: [String: DBValue]()) { result, column in
                            result[column] = row.values[column] ?? .null
                        }
                        return Row(values: projectedValues)
                    }
                    if let key = cacheKey { storeResultCache(key: key, rows: projected) }
                    return projected
                }
                if let key = cacheKey { storeResultCache(key: key, rows: orderedRows) }
                return orderedRows
            }
        }

        // Get page chain for cost estimation and schema for positional row decoding
        let pageIDs = transactionContext == nil
            ? try await storageEngine.getPageChainConcurrent(tableName: table)
            : try await storageEngine.getPageChain(tableName: table, transactionContext: transactionContext)
        let selectSchema = storageEngine.getTableSchema(table)

        // Build index coverage map for cost-based planner
        let indexCoverage = await buildIndexCoverage(table: table)

        // Use cost-based planner to choose access strategy
        let plan = planner.chooseAccessPlan(table: table, condition: condition, pageCount: pageIDs.count, requestedColumns: columns, indexCoverage: indexCoverage)

        // Compute scan limit: push LIMIT down when no ORDER BY or DISTINCT requires full materialization
        let scanLimit: Int?
        if let mods = modifiers, let limit = mods.limit, limit >= 0,
           (mods.orderBy == nil || mods.orderBy!.isEmpty), !mods.distinct {
            scanLimit = limit + (mods.offset ?? 0)
        } else {
            scanLimit = nil
        }

        // Compute needed columns for lazy overflow loading:
        // Union of requested columns + columns referenced in WHERE condition
        let overflowNeededColumns: Set<String>? = {
            var needed = Set<String>()
            if let cols = columns { needed.formUnion(cols) }
            if let cond = condition { needed.formUnion(columnsReferenced(in: cond)) }
            // Also include ORDER BY and DISTINCT columns
            if let mods = modifiers {
                if let orderBy = mods.orderBy { needed.formUnion(orderBy.map { $0.column }) }
            }
            return needed.isEmpty ? nil : needed
        }()

        // Projected decode: only when explicit columns are requested (not SELECT *)
        // so we don't lose unselected columns that downstream code might need.
        let projectCols: Set<String>? = (columns != nil) ? overflowNeededColumns : nil

        switch plan {
        case .indexOnlyScan:
            if let condition = condition {
                // Try synchronous cache-only path first, fall back to async
                let indexed: [Row]?
                if let cached = indexManager.attemptIndexLookupCached(tableName: table, condition: condition) {
                    indexed = cached
                } else {
                    indexed = try await indexManager.attemptIndexLookup(tableName: table, condition: condition)
                }
                if let indexed {
                    rows = indexed.map { row in
                        var vals = row.values
                        vals.removeValue(forKey: "__rid")
                        return Row(values: vals)
                    }
                    if let scanLimit = scanLimit { rows = Array(rows.prefix(scanLimit)) }
                } else {
                    rows = try await parallelTableScan(pageIDs: pageIDs, condition: condition, limit: scanLimit, neededColumns: overflowNeededColumns, projectColumns: projectCols, schema: selectSchema, transactionContext: transactionContext)
                }
            } else {
                rows = try await parallelTableScan(pageIDs: pageIDs, condition: condition, limit: scanLimit, neededColumns: overflowNeededColumns, projectColumns: projectCols, schema: selectSchema, transactionContext: transactionContext)
            }

        case .indexScan:
            if let condition = condition {
                // Try synchronous cache-only path first, fall back to async
                let indexed: [Row]?
                if let cached = indexManager.attemptIndexLookupCached(tableName: table, condition: condition) {
                    indexed = cached
                } else {
                    indexed = try await indexManager.attemptIndexLookup(tableName: table, condition: condition)
                }
                if let indexed {
                    let matchingRIDs = Set(indexed.compactMap { row -> UInt64? in
                        guard case .integer(let ridSigned) = row.values["__rid"] else { return nil }
                        return UInt64(bitPattern: ridSigned)
                    })
                    let fullRecords = try await storageEngine.getRecordsByIDs(matchingRIDs, tableName: table, transactionContext: transactionContext, neededColumns: overflowNeededColumns)
                    rows = fullRecords
                        .map { $0.1 }
                        .filter { evaluateCondition(condition, row: $0) }
                    if let scanLimit = scanLimit { rows = Array(rows.prefix(scanLimit)) }
                } else {
                    rows = try await parallelTableScan(pageIDs: pageIDs, condition: condition, limit: scanLimit, neededColumns: overflowNeededColumns, projectColumns: projectCols, schema: selectSchema, transactionContext: transactionContext)
                }
            } else {
                rows = try await parallelTableScan(pageIDs: pageIDs, condition: condition, limit: scanLimit, neededColumns: overflowNeededColumns, projectColumns: projectCols, schema: selectSchema, transactionContext: transactionContext)
            }

        case .tableScan:
            rows = try await parallelTableScan(pageIDs: pageIDs, condition: condition, limit: scanLimit, neededColumns: overflowNeededColumns, projectColumns: projectCols, schema: selectSchema, transactionContext: transactionContext)
        }

        // Apply query modifiers: DISTINCT, ORDER BY, OFFSET, LIMIT
        if let mods = modifiers {
            if mods.distinct {
                rows = deduplicateRows(rows, columns: columns)
            }

            if let orderBy = mods.orderBy, !orderBy.isEmpty {
                let comparator: (Row, Row) -> Bool = { a, b in
                    for clause in orderBy {
                        let aVal = a.values[clause.column] ?? .null
                        let bVal = b.values[clause.column] ?? .null
                        if aVal == bVal { continue }
                        let less = aVal < bVal
                        return clause.direction == .ascending ? less : !less
                    }
                    return false
                }

                // Top-N optimization: when LIMIT is set, use bounded heap instead of full sort
                let totalNeeded = (mods.limit ?? Int.max) + (mods.offset ?? 0)
                if totalNeeded < rows.count && totalNeeded < Int.max {
                    rows = topN(rows, n: totalNeeded, by: comparator)
                } else {
                    rows.sort(by: comparator)
                }
            }

            if let offset = mods.offset, offset > 0 {
                rows = Array(rows.dropFirst(offset))
            }

            if let limit = mods.limit, limit >= 0 {
                rows = Array(rows.prefix(limit))
            }
        }

        if let columns = columns, !columns.isEmpty {
            let projected = rows.map { row in
                let projectedValues = columns.reduce(into: [String: DBValue]()) { result, column in
                    result[column] = row.values[column] ?? .null
                }
                return Row(values: projectedValues)
            }
            let scannedPages = Set(pageIDs)
            if let key = cacheKey { storeResultCache(key: key, rows: projected, accessedPages: scannedPages) }
            return projected
        }

        let scannedPages = Set(pageIDs)
        if let key = cacheKey { storeResultCache(key: key, rows: rows, accessedPages: scannedPages) }
        return rows
    }

    // MARK: - JOIN

    public func executeJoin(from table: String, joins: [JoinClause], columns: [String]? = nil, where condition: WhereCondition? = nil, modifiers: QueryModifiers? = nil, transactionContext: TransactionContext? = nil) async throws -> [Row] {
        // Query result cache for JOINs: skip for transactional queries
        let joinCacheKey: QueryResultCacheKey?
        if transactionContext == nil {
            let joinSig = joins.map { "\($0.type):\($0.table).\($0.rightColumn)=\($0.leftColumn)" }.joined(separator: "|")
            let key = QueryResultCacheKey(
                table: table,
                conditionSignature: (condition.map { queryConditionSignature($0) } ?? "") + "J:" + joinSig,
                columns: columns,
                modifiersSignature: modifiers.map { queryModifiersSignature($0) } ?? "",
                joinTables: joins.map { $0.table }
            )
            if let cached = lookupResultCache(key: key) {
                return cached
            }
            joinCacheKey = key
        } else {
            joinCacheKey = nil
        }

        // Fast path: single INNER JOIN + LIMIT + index on right join column
        // Streams through left table and probes right index, stopping at LIMIT.
        // Avoids scanning the right table entirely and can short-circuit left scan.
        if joins.count == 1, let join = joins.first, join.type == .inner,
           let mods = modifiers, let limit = mods.limit, limit > 0,
           !mods.distinct {
            let rightIndex = indexManager.getIndex(tableName: join.table, columnName: join.rightColumn)
            if let rightIndex = rightIndex {
                let hasWhere = condition != nil
                let hasOrderBy = mods.orderBy != nil && !mods.orderBy!.isEmpty
                // Overfetch when WHERE or ORDER BY will reduce/reorder the result set
                let totalNeeded = hasWhere || hasOrderBy ? Int.max : limit + (mods.offset ?? 0)
                let pageIDs = transactionContext == nil
                    ? try await storageEngine.getPageChainConcurrent(tableName: table)
                    : try await storageEngine.getPageChain(tableName: table, transactionContext: transactionContext)
                let leftSchema = storageEngine.getTableSchema(table)

                var result = [Row]()
                if totalNeeded < Int.max { result.reserveCapacity(totalNeeded) }
                let leftJoinColIdx: Int? = leftSchema?.columns.firstIndex { $0.name == join.leftColumn }

                // Pre-compute prefixed column names to avoid string interpolation in hot loop
                let leftPrefix = table + "."
                let rightPrefix = join.table + "."

                // Streaming approach: collect batches of left keys, batch-probe index, batch-fetch right records
                var pendingKeys = [(key: DBValue, recordData: Data)]()
                let batchSize = totalNeeded < Int.max ? max(totalNeeded, 128) : 256

                for pageID in pageIDs {
                    if result.count >= totalNeeded { break }
                    let page = transactionContext == nil
                        ? try await storageEngine.getPageConcurrent(pageID: pageID)
                        : try await storageEngine.getPage(pageID: pageID, transactionContext: transactionContext)
                    for record in page.records {
                        if result.count >= totalNeeded { break }
                        let leftKey: DBValue?
                        if let colIdx = leftJoinColIdx {
                            leftKey = Row.extractColumnValue(from: record.data, columnIndex: colIdx)
                        } else {
                            let row = Row.fromBytesAuto(record.data, schema: leftSchema)
                            leftKey = row?.values[join.leftColumn]
                        }
                        guard let leftKey = leftKey, leftKey != .null else { continue }
                        pendingKeys.append((key: leftKey, recordData: record.data))

                        // Flush batch when full
                        if pendingKeys.count >= batchSize {
                            try await _flushJoinBatch(pendingKeys: &pendingKeys, rightIndex: rightIndex, join: join, leftPrefix: leftPrefix, rightPrefix: rightPrefix, leftSchema: leftSchema, result: &result, totalNeeded: totalNeeded, transactionContext: transactionContext)
                        }
                    }
                }
                // Flush remaining
                if !pendingKeys.isEmpty && result.count < totalNeeded {
                    try await _flushJoinBatch(pendingKeys: &pendingKeys, rightIndex: rightIndex, join: join, leftPrefix: leftPrefix, rightPrefix: rightPrefix, leftSchema: leftSchema, result: &result, totalNeeded: totalNeeded, transactionContext: transactionContext)
                }

                // Apply WHERE post-filter on joined rows
                if let condition = condition {
                    result = result.filter { evaluateCondition(condition, row: $0) }
                }

                // Apply ORDER BY on joined rows
                if let orderBy = mods.orderBy, !orderBy.isEmpty {
                    result.sort { a, b in
                        for clause in orderBy {
                            let aVal = a.values[clause.column] ?? .null
                            let bVal = b.values[clause.column] ?? .null
                            if aVal == bVal { continue }
                            let less = aVal < bVal
                            return clause.direction == .ascending ? less : !less
                        }
                        return false
                    }
                }

                // Apply OFFSET
                if let offset = mods.offset, offset > 0 {
                    result = Array(result.dropFirst(offset))
                }
                result = Array(result.prefix(limit))

                // Project columns
                if let columns = columns, !columns.isEmpty {
                    result = result.map { row in
                        let projected = columns.reduce(into: [String: DBValue]()) { r, c in r[c] = row.values[c] ?? .null }
                        return Row(values: projected)
                    }
                }
                if let key = joinCacheKey { storeResultCache(key: key, rows: result) }
                return result
            }
        }

        // Optimize join order using cost-based planner before scanning
        let optimizedJoins = planner.optimizeJoinOrder(
            primaryTable: table,
            joins: joins,
            primaryRowCount: planner.registry.getTableInfo(name: table)?.recordCount ?? 100
        )

        // Predicate pushdown: decompose WHERE into per-table filters applied during scan
        let allTables = [table] + optimizedJoins.map { $0.table }
        var pushedPredicates: [String: WhereCondition] = [:]
        var residualCondition: WhereCondition? = condition

        if let condition = condition {
            // Build table column map from schemas
            var tableColumns: [String: Set<String>] = [:]
            for t in allTables {
                if let schema = storageEngine.getTableSchema(t) {
                    tableColumns[t] = Set(schema.columns.map { $0.name })
                }
            }

            let (perTable, residual) = decomposePredicates(condition, tables: allTables, tableColumns: tableColumns)
            pushedPredicates = perTable
            residualCondition = residual
        }

        // Check if first right table has an index on the join column — if so, skip scanning it
        let firstJoin = optimizedJoins.first
        let firstRightIndex = firstJoin.flatMap { indexManager.getIndex(tableName: $0.table, columnName: $0.rightColumn) }
        let skipFirstRightScan = firstRightIndex != nil && firstJoin?.type == .inner

        // Determine if we can use lazy (deferred deserialization) scan for the first INNER join
        let useFirstLazyJoin = firstJoin?.type == .inner && !skipFirstRightScan

        // Parallel scan: load left table and (conditionally) first right table concurrently
        // When lazy join is possible, scan raw records instead of full Rows
        let firstRightTable = optimizedJoins.first?.table
        let leftFilter = pushedPredicates[table]
        let firstRightFilter = firstRightTable.flatMap { pushedPredicates[$0] }

        var leftRows = [Row]()
        var firstRightRows = [Row]()
        var leftJoinRecords = [(key: DBValue, data: Data)]()
        var firstRightJoinRecords = [(key: DBValue, data: Data)]()

        if useFirstLazyJoin, let fj = firstJoin {
            // Lazy path: scan raw records with join column extraction
            let (lr, rr) = try await withThrowingTaskGroup(of: (String, [(key: DBValue, data: Data)]).self) { group -> ([(key: DBValue, data: Data)], [(key: DBValue, data: Data)]) in
                group.addTask { ("left", try await self.scanJoinRecords(table: table, joinColumn: fj.leftColumn, filter: leftFilter, transactionContext: transactionContext)) }
                if let rightTable = firstRightTable {
                    group.addTask { ("right", try await self.scanJoinRecords(table: rightTable, joinColumn: fj.rightColumn, filter: firstRightFilter, transactionContext: transactionContext)) }
                }
                var left = [(key: DBValue, data: Data)](), right = [(key: DBValue, data: Data)]()
                for try await (tag, records) in group {
                    if tag == "left" { left = records } else { right = records }
                }
                return (left, right)
            }
            leftJoinRecords = lr
            firstRightJoinRecords = rr
        } else {
            // Standard path: full Row scan
            let (lr, rr) = try await withThrowingTaskGroup(of: (String, [Row]).self) { group -> ([Row], [Row]) in
                group.addTask { ("left", try await self.scanAllRows(table: table, filter: leftFilter, transactionContext: transactionContext)) }
                if let rightTable = firstRightTable, !skipFirstRightScan {
                    group.addTask { ("right", try await self.scanAllRows(table: rightTable, filter: firstRightFilter, transactionContext: transactionContext)) }
                }
                var left = [Row](), right = [Row]()
                for try await (tag, rows) in group {
                    if tag == "left" { left = rows } else { right = rows }
                }
                return (left, right)
            }
            leftRows = lr
            firstRightRows = rr
        }

        let leftTablePrefix = table + "."
        var result: [Row]
        // Track whether the first join was already handled via lazy path
        var firstJoinHandled = false

        if useFirstLazyJoin, let fj = firstJoin {
            // Handle first INNER join with deferred deserialization
            let leftSchema = storageEngine.getTableSchema(table)
            let rightSchema = storageEngine.getTableSchema(fj.table)

            // Compute join limit for first join
            let joinLimit: Int?
            if let mods = modifiers, let limit = mods.limit, limit > 0,
               (mods.orderBy == nil || mods.orderBy!.isEmpty), !mods.distinct {
                joinLimit = limit + (mods.offset ?? 0)
            } else {
                joinLimit = nil
            }

            let strategy = planner.chooseJoinStrategy(
                leftRows: leftJoinRecords.count,
                rightRows: firstRightJoinRecords.count,
                join: fj
            )

            result = hashInnerJoinLazy(
                left: leftJoinRecords, right: firstRightJoinRecords,
                join: fj, leftTable: table,
                leftSchema: leftSchema, rightSchema: rightSchema,
                strategy: strategy, limit: joinLimit
            )
            firstJoinHandled = true
        } else {
            result = leftRows.map { row in
                // Prefix left table columns with "tableName."
                var prefixed = [String: DBValue](minimumCapacity: row.values.count * 2)
                for (k, v) in row.values { prefixed[leftTablePrefix + k] = v; prefixed[k] = v }
                return Row(values: prefixed)
            }
        }

        // Process each join in optimized order, using planner's join strategy
        for (i, join) in optimizedJoins.enumerated() {
            // Skip first join if already handled via lazy path
            if i == 0 && firstJoinHandled { continue }

            // Check if right table join column has an index for index nested loop join
            let rightIndex = indexManager.getIndex(tableName: join.table, columnName: join.rightColumn)

            // Compute join limit: push LIMIT down for inner joins when no ORDER BY/DISTINCT
            let joinLimit: Int?
            if let mods = modifiers, let limit = mods.limit, limit > 0,
               (mods.orderBy == nil || mods.orderBy!.isEmpty), !mods.distinct,
               join.type == .inner {
                joinLimit = limit + (mods.offset ?? 0)
            } else {
                joinLimit = nil
            }

            // Use index nested loop join when: index exists on right column AND type is INNER
            // Batch-probe all unique left keys at once instead of individual lookups
            if let rightIndex = rightIndex, join.type == .inner {
                var newResult = [Row]()
                // Collect unique left keys to avoid redundant index lookups
                var keyToLeftRows = [DBValue: [Row]]()
                for leftRow in result {
                    let leftKey = leftRow.values[join.leftColumn] ?? .null
                    if leftKey != .null {
                        keyToLeftRows[leftKey, default: []].append(leftRow)
                    }
                }

                // Batch-probe index for all unique keys at once
                let uniqueKeys = Array(keyToLeftRows.keys)
                let indexResults: [DBValue: [Row]]
                if let cached = rightIndex.searchBatchCached(keys: uniqueKeys) {
                    indexResults = cached
                } else {
                    indexResults = try await rightIndex.searchBatch(keys: uniqueKeys)
                }

                // Collect all RIDs and batch-fetch full right records
                var allRIDs = Set<UInt64>()
                var ridsByKey = [DBValue: [UInt64]]()
                for (key, rows) in indexResults {
                    let rids = rows.compactMap { row -> UInt64? in
                        guard case .integer(let ridSigned) = row.values["__rid"] else { return nil }
                        return UInt64(bitPattern: ridSigned)
                    }
                    ridsByKey[key] = rids
                    allRIDs.formUnion(rids)
                }

                if !allRIDs.isEmpty {
                    let fullRightRecords = try await storageEngine.getRecordsByIDs(allRIDs, tableName: join.table, transactionContext: transactionContext)
                    var rightRowsByRID = [UInt64: Row](minimumCapacity: fullRightRecords.count)
                    for (record, row) in fullRightRecords {
                        rightRowsByRID[record.id] = row
                    }

                    let rightPfx = join.table + "."
                    outerLoop: for (key, leftRows) in keyToLeftRows {
                        guard let rids = ridsByKey[key], !rids.isEmpty else { continue }
                        for rid in rids {
                            guard let rightRow = rightRowsByRID[rid] else { continue }
                            for leftRow in leftRows {
                                var combined = leftRow.values
                                for (k, v) in rightRow.values { combined[rightPfx + k] = v; combined[k] = v }
                                newResult.append(Row(values: combined))
                                if let jl = joinLimit, newResult.count >= jl { break outerLoop }
                            }
                        }
                    }
                }

                result = newResult
                continue
            }

            let rightRows: [Row]
            if i == 0 {
                rightRows = firstRightRows
            } else {
                let rightFilter = pushedPredicates[join.table]
                rightRows = try await scanAllRows(table: join.table, filter: rightFilter, transactionContext: transactionContext)
            }

            // Consult planner for join strategy (hash join with build-side selection)
            let strategy = planner.chooseJoinStrategy(
                leftRows: result.count,
                rightRows: rightRows.count,
                join: join
            )

            switch join.type {
            case .inner:
                result = hashInnerJoin(left: result, right: rightRows, join: join, strategy: strategy, limit: joinLimit)
            case .left:
                result = hashLeftJoin(left: result, right: rightRows, join: join, strategy: strategy)
            case .right:
                result = hashRightJoin(left: result, right: rightRows, join: join, strategy: strategy)
            case .cross:
                result = crossJoin(left: result, right: rightRows, join: join)
            }
        }

        // Apply residual WHERE filter (cross-table predicates not pushed down)
        if let residual = residualCondition {
            result = result.filter { evaluateCondition(residual, row: $0) }
        }

        // Apply modifiers (DISTINCT, ORDER BY, OFFSET, LIMIT)
        if let mods = modifiers {
            if mods.distinct { result = deduplicateRows(result, columns: columns) }
            if let orderBy = mods.orderBy, !orderBy.isEmpty {
                result.sort { a, b in
                    for clause in orderBy {
                        let aVal = a.values[clause.column] ?? .null
                        let bVal = b.values[clause.column] ?? .null
                        if aVal == bVal { continue }
                        let less = aVal < bVal
                        return clause.direction == .ascending ? less : !less
                    }
                    return false
                }
            }
            if let offset = mods.offset, offset > 0 { result = Array(result.dropFirst(offset)) }
            if let limit = mods.limit, limit >= 0 { result = Array(result.prefix(limit)) }
        }

        // Project columns
        if let columns = columns, !columns.isEmpty {
            let projected = result.map { row in
                let projected = columns.reduce(into: [String: DBValue]()) { r, c in r[c] = row.values[c] ?? .null }
                return Row(values: projected)
            }
            if let key = joinCacheKey { storeResultCache(key: key, rows: projected) }
            return projected
        }

        if let key = joinCacheKey { storeResultCache(key: key, rows: result) }
        return result
    }

    // MARK: - GROUP BY

    public func executeGroupBy(from table: String, select expressions: [SelectExpression], where condition: WhereCondition? = nil, groupBy: GroupByClause, modifiers: QueryModifiers? = nil, transactionContext: TransactionContext? = nil) async throws -> [Row] {
        // Streaming hash aggregate: scan pages directly, accumulate per-group state
        let pageIDs = transactionContext == nil
            ? try await storageEngine.getPageChainConcurrent(tableName: table)
            : try await storageEngine.getPageChain(tableName: table, transactionContext: transactionContext)
        let gbSchema = storageEngine.getTableSchema(table)

        // Per-group accumulators: key → (count, per-expression state)
        struct GroupAccum {
            var rowCount: Int64 = 0
            var firstRow: [String: DBValue]? = nil
            var sums: [String: Double] = [:]
            var sumHasValue: [String: Bool] = [:]
            var avgSums: [String: Double] = [:]
            var avgCounts: [String: Int] = [:]
            var mins: [String: DBValue] = [:]
            var maxs: [String: DBValue] = [:]
            var countNonNull: [String: Int64] = [:]
        }
        var groups = [[DBValue]: GroupAccum]()

        // Pre-compute column ordinals for positional extraction (avoids full Row decode per record)
        let groupColIndices: [Int]? = gbSchema.map { schema in
            groupBy.columns.compactMap { schema.columnOrdinals[$0] }
        }
        let groupColIndicesValid = groupColIndices?.count == groupBy.columns.count

        // Collect aggregate column ordinals
        var aggColIndices = [String: Int]()
        // Track if any .column expression exists (needs firstRow via full decode)
        var hasColumnExpr = false
        // Collect all column names referenced by .column expressions
        var columnExprNames = [String]()
        for expr in expressions {
            let colName: String?
            switch expr {
            case .column(let n): colName = n; hasColumnExpr = true; columnExprNames.append(n)
            case .count(let c): colName = c
            case .sum(let c), .avg(let c), .min(let c), .max(let c): colName = c
            }
            if let name = colName, let schema = gbSchema, let idx = schema.columnOrdinals[name] {
                aggColIndices[name] = idx
            }
        }
        // Column expression ordinals for positional first-row extraction
        let columnExprIndices: [(String, Int)]? = gbSchema.map { schema in
            columnExprNames.compactMap { name in
                schema.columnOrdinals[name].map { (name, $0) }
            }
        }

        // Build column index map for lazy WHERE evaluation
        let gbColumnIndexMap: [String: Int]? = gbSchema?.columnOrdinals

        // Check if positional fast path is viable
        let usePositional = groupColIndicesValid && gbSchema != nil

        for pageID in pageIDs {
            let page = try await storageEngine.getPage(pageID: pageID, transactionContext: transactionContext)
            for var record in page.records {
                if record.isOverflow {
                    record = try await storageEngine.reassembleOverflowRecord(record)
                }

                // Try lazy WHERE evaluation first (avoids full Row decode for filtered-out rows)
                if let cond = condition {
                    if let lazyResult = evaluateConditionLazy(cond, data: record.data, columnIndexMap: gbColumnIndexMap) {
                        if !lazyResult { continue }
                    } else {
                        // Lazy eval can't handle this — fall back to full decode
                        guard let row = Row.fromBytesAuto(record.data, schema: gbSchema) else { continue }
                        if !evaluateCondition(cond, row: row) { continue }
                    }
                }

                if usePositional, let gci = groupColIndices {
                    // Fast path: positional extraction of group key + aggregate columns
                    let key = gci.map { Row.extractColumnValue(from: record.data, columnIndex: $0) ?? .null }
                    var accum = groups[key] ?? GroupAccum()
                    accum.rowCount += 1

                    // Extract first-row values for .column expressions positionally
                    if accum.firstRow == nil && hasColumnExpr, let cei = columnExprIndices {
                        var firstVals = [String: DBValue]()
                        for (name, idx) in cei {
                            firstVals[name] = Row.extractColumnValue(from: record.data, columnIndex: idx) ?? .null
                        }
                        // Also include group columns in firstRow
                        for (i, col) in groupBy.columns.enumerated() {
                            firstVals[col] = key[i]
                        }
                        accum.firstRow = firstVals
                    }

                    for expr in expressions {
                        switch expr {
                        case .column: break
                        case .count(let col):
                            if let col = col {
                                if let idx = aggColIndices[col],
                                   let v = Row.extractColumnValue(from: record.data, columnIndex: idx), v != .null {
                                    accum.countNonNull[col, default: 0] += 1
                                }
                            }
                        case .sum(let col):
                            if let idx = aggColIndices[col],
                               let v = Row.extractColumnValue(from: record.data, columnIndex: idx) {
                                if let n = numericValue(v) {
                                    accum.sums[col, default: 0] += n
                                    accum.sumHasValue[col] = true
                                }
                            }
                        case .avg(let col):
                            if let idx = aggColIndices[col],
                               let v = Row.extractColumnValue(from: record.data, columnIndex: idx) {
                                if let n = numericValue(v) {
                                    accum.avgSums[col, default: 0] += n
                                    accum.avgCounts[col, default: 0] += 1
                                }
                            }
                        case .min(let col):
                            if let idx = aggColIndices[col],
                               let v = Row.extractColumnValue(from: record.data, columnIndex: idx), v != .null {
                                if accum.mins[col] == nil || v < accum.mins[col]! { accum.mins[col] = v }
                            }
                        case .max(let col):
                            if let idx = aggColIndices[col],
                               let v = Row.extractColumnValue(from: record.data, columnIndex: idx), v != .null {
                                if accum.maxs[col] == nil || v > accum.maxs[col]! { accum.maxs[col] = v }
                            }
                        }
                    }
                    groups[key] = accum
                } else {
                    // Fallback: full Row decode (non-positional format or missing schema)
                    guard let row = Row.fromBytesAuto(record.data, schema: gbSchema) else { continue }
                    // Re-check condition if lazy eval was used (it passed above, so no re-check needed
                    // unless lazy eval returned nil and we already fell back — but that case continues above)

                    let key = groupBy.columns.map { row.values[$0] ?? .null }
                    var accum = groups[key] ?? GroupAccum()
                    accum.rowCount += 1
                    if accum.firstRow == nil { accum.firstRow = row.values }

                    for expr in expressions {
                        switch expr {
                        case .column: break
                        case .count(let col):
                            if let col = col {
                                if let v = row.values[col], v != .null {
                                    accum.countNonNull[col, default: 0] += 1
                                }
                            }
                        case .sum(let col):
                            if let v = numericValue(row.values[col]) {
                                accum.sums[col, default: 0] += v
                                accum.sumHasValue[col] = true
                            }
                        case .avg(let col):
                            if let v = numericValue(row.values[col]) {
                                accum.avgSums[col, default: 0] += v
                                accum.avgCounts[col, default: 0] += 1
                            }
                        case .min(let col):
                            if let v = row.values[col], v != .null {
                                if accum.mins[col] == nil || v < accum.mins[col]! { accum.mins[col] = v }
                            }
                        case .max(let col):
                            if let v = row.values[col], v != .null {
                                if accum.maxs[col] == nil || v > accum.maxs[col]! { accum.maxs[col] = v }
                            }
                        }
                    }
                    groups[key] = accum
                }
            }
        }

        // Finalize results from accumulators
        var resultRows = [Row]()
        for (key, accum) in groups {
            var values = [String: DBValue]()
            for (i, col) in groupBy.columns.enumerated() { values[col] = key[i] }

            for expr in expressions {
                switch expr {
                case .column(let name):
                    values[name] = accum.firstRow?[name] ?? .null
                case .count(let col):
                    let alias = col.map { "COUNT(\($0))" } ?? "COUNT(*)"
                    if let col = col {
                        values[alias] = .integer(accum.countNonNull[col] ?? 0)
                    } else {
                        values[alias] = .integer(accum.rowCount)
                    }
                case .sum(let col):
                    let alias = "SUM(\(col))"
                    values[alias] = (accum.sumHasValue[col] ?? false) ? .double(accum.sums[col]!) : .null
                case .avg(let col):
                    let alias = "AVG(\(col))"
                    let count = accum.avgCounts[col] ?? 0
                    values[alias] = count > 0 ? .double(accum.avgSums[col]! / Double(count)) : .null
                case .min(let col):
                    values["MIN(\(col))"] = accum.mins[col] ?? .null
                case .max(let col):
                    values["MAX(\(col))"] = accum.maxs[col] ?? .null
                }
            }

            let groupRow = Row(values: values)
            if let having = groupBy.having {
                if evaluateCondition(having, row: groupRow) { resultRows.append(groupRow) }
            } else {
                resultRows.append(groupRow)
            }
        }

        // Apply modifiers
        if let mods = modifiers {
            if mods.distinct { resultRows = deduplicateRows(resultRows, columns: nil) }
            if let orderBy = mods.orderBy, !orderBy.isEmpty {
                resultRows.sort { a, b in
                    for clause in orderBy {
                        let aVal = a.values[clause.column] ?? .null
                        let bVal = b.values[clause.column] ?? .null
                        if aVal == bVal { continue }
                        return clause.direction == .ascending ? (aVal < bVal) : !(aVal < bVal)
                    }
                    return false
                }
            }
            if let offset = mods.offset, offset > 0 { resultRows = Array(resultRows.dropFirst(offset)) }
            if let limit = mods.limit, limit >= 0 { resultRows = Array(resultRows.prefix(limit)) }
        }

        return resultRows
    }

    // MARK: - SET Operations (UNION, INTERSECT, EXCEPT)

    public func executeSetOperation(_ operation: SetOperation, left: [Row], right: [Row]) -> [Row] {
        switch operation {
        case .union:
            var seen = Set<Row>()
            var result = [Row]()
            for row in left + right {
                if seen.insert(row).inserted { result.append(row) }
            }
            return result
        case .unionAll:
            return left + right
        case .intersect:
            let rightSet = Set(right)
            var seen = Set<Row>()
            var result = [Row]()
            for row in left where rightSet.contains(row) {
                if seen.insert(row).inserted { result.append(row) }
            }
            return result
        case .except:
            let rightSet = Set(right)
            var seen = Set<Row>()
            var result = [Row]()
            for row in left where !rightSet.contains(row) {
                if seen.insert(row).inserted { result.append(row) }
            }
            return result
        }
    }

    // MARK: - INSERT

    public func executeInsert(into table: String, row: Row, transactionContext: TransactionContext? = nil) async throws {
        // Enforce primary key uniqueness if schema defines one
        if let schema = storageEngine.getTableSchema(table),
           let pkColumn = schema.primaryKeyColumn {
            if let pkValue = row.values[pkColumn.name], pkValue != .null {
                // Check index first if available, otherwise scan
                if let indexed = try await indexManager.attemptIndexLookup(
                    tableName: table,
                    condition: .equals(column: pkColumn.name, value: pkValue)
                ), !indexed.isEmpty {
                    throw PantryError.primaryKeyViolation
                } else if !(indexManager.hasIndex(tableName: table, columnName: pkColumn.name)) {
                    // No index — fall back to scan
                    let existing = try await executeSelect(from: table, columns: [pkColumn.name], where: .equals(column: pkColumn.name, value: pkValue), transactionContext: transactionContext)
                    if !existing.isEmpty {
                        throw PantryError.primaryKeyViolation
                    }
                }
            } else if !pkColumn.isNullable {
                throw PantryError.notNullConstraintViolation(column: pkColumn.name)
            }
        }

        let schema = storageEngine.getTableSchema(table)
        let rowData = schema != nil ? row.toBytesPositional(schema: schema!) : row.toBytes()
        let recordID = generateRecordID()
        let record = Record(id: recordID, data: rowData)
        try await storageEngine.insertRecord(record, tableName: table, row: row, transactionContext: transactionContext)
        invalidateResultCache(forTable: table)
    }

    /// Bulk insert: inserts all rows with deferred index updates for better throughput.
    /// Validates PK uniqueness in batch before inserting, then updates indexes once at the end.
    public func executeBulkInsert(into table: String, rows: [Row], transactionContext: TransactionContext? = nil) async throws {
        guard !rows.isEmpty else { return }

        // Validate primary key uniqueness in batch
        let schema = storageEngine.getTableSchema(table)
        if let pkColumn = schema?.primaryKeyColumn {
            let existingCount = planner.registry.getTableInfo(name: table)?.recordCount ?? 0

            // 1. Collect all PKs and check intra-batch uniqueness
            var pkValues = [DBValue]()
            pkValues.reserveCapacity(rows.count)
            var seenPKs = Set<DBValue>()
            seenPKs.reserveCapacity(rows.count)
            for row in rows {
                if let pkValue = row.values[pkColumn.name], pkValue != .null {
                    guard seenPKs.insert(pkValue).inserted else {
                        throw PantryError.primaryKeyViolation
                    }
                    pkValues.append(pkValue)
                } else if !pkColumn.isNullable {
                    throw PantryError.notNullConstraintViolation(column: pkColumn.name)
                }
            }

            // 2. Batch-check against existing data (single B-tree traversal)
            if existingCount > 0 && !pkValues.isEmpty {
                if let index = indexManager.getIndex(tableName: table, columnName: pkColumn.name) {
                    // Batch-probe: bloom filter pre-check is done inside searchBatch
                    let results: [DBValue: [Row]]
                    if let cached = index.searchBatchCached(keys: pkValues) {
                        results = cached
                    } else {
                        results = try await index.searchBatch(keys: pkValues)
                    }
                    for (_, matchedRows) in results {
                        if !matchedRows.isEmpty {
                            throw PantryError.primaryKeyViolation
                        }
                    }
                } else {
                    // No index: fall back to per-row check
                    for pkValue in pkValues {
                        let existing = try await executeSelect(from: table, columns: [pkColumn.name], where: .equals(column: pkColumn.name, value: pkValue), transactionContext: transactionContext)
                        if !existing.isEmpty {
                            throw PantryError.primaryKeyViolation
                        }
                    }
                }
            }
        }

        // Phase 1: Serialize all records in parallel, then batch-insert with page-level batching
        let bulkSchema = storageEngine.getTableSchema(table)

        // Pre-generate all record IDs sequentially
        var recordIDs = [UInt64]()
        recordIDs.reserveCapacity(rows.count)
        for _ in rows { recordIDs.append(generateRecordID()) }

        // Parallel serialization: serialize rows on multiple cores
        let rowCount = rows.count
        var serializedData = [Data?](repeating: nil, count: rowCount)
        if let schema = bulkSchema {
            serializedData.withUnsafeMutableBufferPointer { buf in
                DispatchQueue.concurrentPerform(iterations: rowCount) { i in
                    buf[i] = rows[i].toBytesPositional(schema: schema)
                }
            }
        } else {
            for i in 0..<rowCount { serializedData[i] = rows[i].toBytes() }
        }

        var serializedRecords = [Record]()
        serializedRecords.reserveCapacity(rowCount)
        var insertedPairs: [(Record, Row)] = []
        insertedPairs.reserveCapacity(rowCount)
        for i in 0..<rowCount {
            let record = Record(id: recordIDs[i], data: serializedData[i]!)
            serializedRecords.append(record)
            insertedPairs.append((record, rows[i]))
        }

        // Batch insert: fills pages to capacity before writing (much fewer I/O ops)
        _ = try await storageEngine.bulkInsertRecordsBatched(serializedRecords, tableName: table, transactionContext: transactionContext)

        // Phase 2: Batch index updates — sorted keys for sequential B-tree traversal
        try await indexManager.updateIndexesBatch(records: insertedPairs, tableName: table)
        invalidateResultCache(forTable: table)
    }

    // MARK: - UPDATE

    public func executeUpdate(table: String, set values: [String: DBValue], where condition: WhereCondition?, transactionContext: TransactionContext? = nil) async throws -> Int {
        // If updating PK column, validate uniqueness of new PK value
        if let schema = storageEngine.getTableSchema(table),
           let pkColumn = schema.primaryKeyColumn,
           let newPKValue = values[pkColumn.name], newPKValue != .null {
            let existing = try await executeSelect(from: table, columns: [pkColumn.name], where: .equals(column: pkColumn.name, value: newPKValue), transactionContext: transactionContext)
            // Allow if the only match is the row being updated (checked below per-row)
            if existing.count > 1 {
                throw PantryError.primaryKeyViolation
            }
        }

        // Use cost-based planner to decide index vs scan
        let updateSchema = storageEngine.getTableSchema(table)
        let pageIDs = transactionContext == nil
            ? try await storageEngine.getPageChainConcurrent(tableName: table)
            : try await storageEngine.getPageChain(tableName: table, transactionContext: transactionContext)
        let plan = planner.chooseAccessPlan(table: table, condition: condition, pageCount: pageIDs.count)

        if case .indexScan = plan, let condition = condition {
            // RID-only path: skip Row allocation for .equals conditions
            var matchingRIDs: Set<UInt64>?
            if let cachedRIDs = indexManager.attemptIndexLookupCachedRIDs(tableName: table, condition: condition) {
                matchingRIDs = cachedRIDs
            } else if let asyncRIDs = try await indexManager.attemptIndexLookupRIDs(tableName: table, condition: condition) {
                matchingRIDs = asyncRIDs
            }
            // Fallback to Row-based path for non-.equals conditions
            if matchingRIDs == nil {
                let indexedRows: [Row]?
                if let cached = indexManager.attemptIndexLookupCached(tableName: table, condition: condition) {
                    indexedRows = cached
                } else {
                    indexedRows = try await indexManager.attemptIndexLookup(tableName: table, condition: condition)
                }
                if let indexedRows {
                    matchingRIDs = Set(indexedRows.compactMap { row -> UInt64? in
                        guard case .integer(let ridSigned) = row.values["__rid"] else { return nil }
                        return UInt64(bitPattern: ridSigned)
                    })
                }
            }
            if let matchingRIDs {

            // Single-pass: group RIDs by page, load each page once, patch + replace in place
            let colMap = updateSchema?.columnOrdinals
            let ridPages = storageEngine.getRIDPageMapping(matchingRIDs, tableName: table)

            // Pre-compute patch templates for reuse across all records
            let patchTemplates: [Row.PatchTemplate]? = updateSchema.flatMap { Row.buildPatchTemplates(updates: values, schema: $0) }

            var updatedCount = 0
            var modifiedPages = Set<Int>()

            // Process pages with cached RID-to-page mapping
            for (pageID, pageRIDs) in ridPages.cached {
                var page = transactionContext == nil
                    ? try await storageEngine.getPageConcurrent(pageID: pageID)
                    : try await storageEngine.getPage(pageID: pageID, transactionContext: transactionContext)
                let ridSet = Set(pageRIDs)
                var pageModified = false
                var allPatchable = true

                for record in page.records where ridSet.contains(record.id) {
                    // Lazy filter: verify condition on raw bytes without full Row decode
                    var lazyConfirmed = false
                    if let colMap = colMap {
                        if let lazyResult = evaluateConditionLazy(condition, data: record.data, columnIndexMap: colMap) {
                            if !lazyResult { continue }
                            lazyConfirmed = true
                        }
                    }

                    // Fast path: patch raw bytes in-place using pre-computed templates
                    if let templates = patchTemplates,
                       let patchedData = Row.patchPositionalDataPrecomputed(record.data, templates: templates) {
                        let newRecord = Record(id: record.id, data: patchedData)
                        if page.replaceRecordAndPatch(id: record.id, with: newRecord) {
                            pageModified = true
                        } else if page.replaceRecord(id: record.id, with: newRecord) {
                            pageModified = true
                            allPatchable = false
                        }
                    } else {
                        // Slow path: full decode + merge + re-encode
                        guard let row = Row.fromBytesAuto(record.data, schema: updateSchema) else { continue }
                        if !lazyConfirmed { guard evaluateCondition(condition, row: row) else { continue } }
                        var updatedValues = row.values
                        for (key, value) in values { updatedValues[key] = value }
                        let updatedRow = Row(values: updatedValues)
                        let rowData = updateSchema != nil ? updatedRow.toBytesPositional(schema: updateSchema!) : updatedRow.toBytes()
                        let newRecord = Record(id: record.id, data: rowData)
                        if page.replaceRecordAndPatch(id: record.id, with: newRecord) {
                            pageModified = true
                        } else if page.replaceRecord(id: record.id, with: newRecord) {
                            pageModified = true
                            allPatchable = false
                        } else {
                            try await storageEngine.deleteRecord(id: record.id, tableName: table, transactionContext: transactionContext, knownPageID: pageID)
                            try await storageEngine.insertRecord(newRecord, tableName: table, row: updatedRow, transactionContext: transactionContext)
                            allPatchable = false
                        }
                    }
                    updatedCount += 1
                }
                if pageModified {
                    if !allPatchable { page.allPatched = false }
                    if transactionContext == nil {
                        try await storageEngine.savePageDeferred(page)
                    } else {
                        try await storageEngine.savePage(page, transactionContext: transactionContext)
                    }
                    modifiedPages.insert(pageID)
                }
            }

            // Fallback: scan for uncached RIDs
            if !ridPages.uncached.isEmpty {
                let rawRecords = try await storageEngine.getRecordDataByIDs(Set(ridPages.uncached), tableName: table, transactionContext: transactionContext)
                for (record, recordPageID) in rawRecords {
                    var lazyConfirmed = false
                    if let colMap = colMap {
                        if let lazyResult = evaluateConditionLazy(condition, data: record.data, columnIndexMap: colMap) {
                            if !lazyResult { continue }
                            lazyConfirmed = true
                        }
                    }
                    if let templates = patchTemplates,
                       let patchedData = Row.patchPositionalDataPrecomputed(record.data, templates: templates) {
                        let newRecord = Record(id: record.id, data: patchedData)
                        try await storageEngine.replaceRecordInPlace(id: record.id, newRecord: newRecord, tableName: table, transactionContext: transactionContext, knownPageID: recordPageID)
                    } else {
                        guard let row = Row.fromBytesAuto(record.data, schema: updateSchema) else { continue }
                        if !lazyConfirmed { guard evaluateCondition(condition, row: row) else { continue } }
                        var updatedValues = row.values
                        for (key, value) in values { updatedValues[key] = value }
                        let updatedRow = Row(values: updatedValues)
                        let rowData = updateSchema != nil ? updatedRow.toBytesPositional(schema: updateSchema!) : updatedRow.toBytes()
                        let newRecord = Record(id: record.id, data: rowData)
                        try await storageEngine.replaceRecordInPlace(id: record.id, newRecord: newRecord, tableName: table, transactionContext: transactionContext, knownPageID: recordPageID)
                    }
                    updatedCount += 1
                    modifiedPages.insert(recordPageID)
                }
            }

            // Dirty pages deferred to background writer / eviction / close
            if updatedCount > 0 { invalidateResultCache(forTable: table, modifiedPages: modifiedPages) }
            return updatedCount
        }
        }

        // Table scan path: page-by-page update to avoid double page reads
        var updatedCount = 0
        let updateColMap = updateSchema?.columnOrdinals
        var modifiedPages = Set<Int>()

        // Pre-compute patch templates for reuse across all records
        let scanPatchTemplates: [Row.PatchTemplate]? = updateSchema.flatMap { Row.buildPatchTemplates(updates: values, schema: $0) }

        for pageID in pageIDs {
            var page = transactionContext == nil
                ? try await storageEngine.getPageConcurrent(pageID: pageID)
                : try await storageEngine.getPage(pageID: pageID, transactionContext: transactionContext)
            var pageModified = false
            var allPatchable = true
            var overflowUpdates: [(Record, Row)]  = [] // records that couldn't be replaced in-place

            for var record in page.records {
                if record.isOverflow {
                    record = try await storageEngine.reassembleOverflowRecord(record)
                }
                let data = record.data

                // Lazy filter: skip full row decode for non-matching records
                var lazyConfirmed = false
                if let condition = condition, let colMap = updateColMap {
                    if let lazyResult = evaluateConditionLazy(condition, data: data, columnIndexMap: colMap) {
                        if !lazyResult { continue }
                        lazyConfirmed = true
                    }
                }
                guard let row = Row.fromBytesAuto(data, schema: updateSchema) else { continue }
                if condition == nil || lazyConfirmed || evaluateCondition(condition!, row: row) {
                    let newRecord: Record
                    let updatedRow: Row
                    // Fast path: patch raw bytes in-place using pre-computed templates
                    if let templates = scanPatchTemplates,
                       let patchedData = Row.patchPositionalDataPrecomputed(data, templates: templates) {
                        newRecord = Record(id: record.id, data: patchedData)
                        updatedRow = row // original row for overflow fallback
                    } else {
                        var updatedValues = row.values
                        for (key, value) in values { updatedValues[key] = value }
                        updatedRow = Row(values: updatedValues)
                        let rowData = updateSchema != nil ? updatedRow.toBytesPositional(schema: updateSchema!) : updatedRow.toBytes()
                        newRecord = Record(id: record.id, data: rowData)
                    }

                    if page.replaceRecordAndPatch(id: record.id, with: newRecord) {
                        pageModified = true
                    } else if page.replaceRecord(id: record.id, with: newRecord) {
                        pageModified = true
                        allPatchable = false
                    } else {
                        overflowUpdates.append((newRecord, updatedRow))
                        allPatchable = false
                    }
                    updatedCount += 1
                }
            }

            if pageModified {
                if !allPatchable { page.allPatched = false }
                if transactionContext == nil {
                    try await storageEngine.savePageDeferred(page)
                } else {
                    try await storageEngine.savePage(page, transactionContext: transactionContext)
                }
                modifiedPages.insert(pageID)
            }

            // Handle records that couldn't be replaced in-place (size changed too much)
            for (newRecord, updatedRow) in overflowUpdates {
                try await storageEngine.deleteRecord(id: newRecord.id, tableName: table, transactionContext: transactionContext, knownPageID: pageID)
                try await storageEngine.insertRecord(newRecord, tableName: table, row: updatedRow, transactionContext: transactionContext)
            }
        }

        // Dirty pages deferred to background writer / eviction / close
        if updatedCount > 0 { invalidateResultCache(forTable: table, modifiedPages: modifiedPages) }
        return updatedCount
    }

    // MARK: - DELETE

    public func executeDelete(from table: String, where condition: WhereCondition?, transactionContext: TransactionContext? = nil) async throws -> Int {
        // Use cost-based planner to decide index vs scan
        let pageIDs = transactionContext == nil
            ? try await storageEngine.getPageChainConcurrent(tableName: table)
            : try await storageEngine.getPageChain(tableName: table, transactionContext: transactionContext)
        let plan = planner.chooseAccessPlan(table: table, condition: condition, pageCount: pageIDs.count)

        if case .indexScan = plan, let condition = condition {
            // RID-only path: skip Row allocation for .equals conditions
            var matchingRIDs: Set<UInt64>?
            if let cachedRIDs = indexManager.attemptIndexLookupCachedRIDs(tableName: table, condition: condition) {
                matchingRIDs = cachedRIDs
            } else if let asyncRIDs = try await indexManager.attemptIndexLookupRIDs(tableName: table, condition: condition) {
                matchingRIDs = asyncRIDs
            }
            // Fallback to Row-based path for non-.equals conditions
            if matchingRIDs == nil {
                let indexedRows: [Row]?
                if let cached = indexManager.attemptIndexLookupCached(tableName: table, condition: condition) {
                    indexedRows = cached
                } else {
                    indexedRows = try await indexManager.attemptIndexLookup(tableName: table, condition: condition)
                }
                if let indexedRows {
                    matchingRIDs = Set(indexedRows.compactMap { row -> UInt64? in
                        guard case .integer(let ridSigned) = row.values["__rid"] else { return nil }
                        return UInt64(bitPattern: ridSigned)
                    })
                }
            }
            if let matchingRIDs {

            let idxSchema = storageEngine.getTableSchema(table)
            if idxSchema != nil {
                // Single-pass: delete from pages AND collect raw data for index removal
                let (deleted, rawData) = try await storageEngine.deleteByRIDs(matchingRIDs, tableName: table, transactionContext: transactionContext)

                // Remove from indexes using collected raw data
                if !rawData.isEmpty {
                    try await indexManager.removeFromIndexesBatchRaw(records: rawData, tableName: table, schema: idxSchema!)
                    invalidateResultCache(forTable: table)
                }
                return deleted
            }

            // Fallback: original Row-based path
            let fullRecords = try await storageEngine.getRecordsByIDsWithPages(matchingRIDs, tableName: table, transactionContext: transactionContext)
            var matchingForDelete: [(id: UInt64, pageID: Int, row: Row?)] = []
            for (record, row, recordPageID) in fullRecords where evaluateCondition(condition, row: row) {
                matchingForDelete.append((id: record.id, pageID: recordPageID, row: row))
            }
            if !matchingForDelete.isEmpty {
                try await storageEngine.deleteRecordsBatch(matchingForDelete, tableName: table, transactionContext: transactionContext)
                let modifiedPages = Set(matchingForDelete.map { $0.pageID })
                invalidateResultCache(forTable: table, modifiedPages: modifiedPages)
            }
            return matchingForDelete.count
        }
        }

        // Table scan path — walk pages directly, collect matching records, batch-delete per page
        let deleteSchema = storageEngine.getTableSchema(table)
        let deleteColMap = deleteSchema?.columnOrdinals
        let useRawPath = deleteSchema != nil  // Use raw data path to avoid full Row deserialization

        var matchingRaw: [(id: UInt64, pageID: Int, data: Data?)] = []
        var matchingRows: [(id: UInt64, pageID: Int, row: Row?)] = []
        for pageID in pageIDs {
            let page = try await storageEngine.getPage(pageID: pageID, transactionContext: transactionContext)
            for var record in page.records {
                if condition == nil {
                    // Unconditional delete
                    if useRawPath {
                        let data = record.isOverflow
                            ? try await storageEngine.reassembleOverflowRecord(record).data
                            : record.data
                        matchingRaw.append((id: record.id, pageID: pageID, data: data))
                    } else {
                        let row: Row? = record.isOverflow
                            ? Row.fromBytesAuto(try await storageEngine.reassembleOverflowRecord(record).data, schema: deleteSchema)
                            : Row.fromBytesAuto(record.data, schema: deleteSchema)
                        matchingRows.append((id: record.id, pageID: pageID, row: row))
                    }
                } else {
                    var data = record.data
                    if record.isOverflow {
                        record = try await storageEngine.reassembleOverflowRecord(record)
                        data = record.data
                    }
                    // Lazy evaluation: extract only the condition column, skip full decode
                    if let colMap = deleteColMap {
                        if let lazyResult = evaluateConditionLazy(condition!, data: data, columnIndexMap: colMap) {
                            if !lazyResult { continue }
                            // Lazy eval was definitive — skip redundant full-Row condition check
                            if useRawPath {
                                matchingRaw.append((id: record.id, pageID: pageID, data: data))
                            } else {
                                guard let row = Row.fromBytesAuto(data, schema: deleteSchema) else { continue }
                                matchingRows.append((id: record.id, pageID: pageID, row: row))
                            }
                            continue
                        }
                    }
                    // Lazy eval couldn't determine result — fall back to full Row decode
                    guard let row = Row.fromBytesAuto(data, schema: deleteSchema), evaluateCondition(condition!, row: row) else { continue }
                    if useRawPath {
                        matchingRaw.append((id: record.id, pageID: pageID, data: data))
                    } else {
                        matchingRows.append((id: record.id, pageID: pageID, row: row))
                    }
                }
            }
        }

        if useRawPath && !matchingRaw.isEmpty {
            try await storageEngine.deleteRecordsBatchRaw(matchingRaw, tableName: table, transactionContext: transactionContext)
            let modifiedPages = Set(matchingRaw.map { $0.pageID })
            invalidateResultCache(forTable: table, modifiedPages: modifiedPages)
            return matchingRaw.count
        } else if !matchingRows.isEmpty {
            try await storageEngine.deleteRecordsBatch(matchingRows, tableName: table, transactionContext: transactionContext)
            let modifiedPages = Set(matchingRows.map { $0.pageID })
            invalidateResultCache(forTable: table, modifiedPages: modifiedPages)
        }
        return matchingRows.count
    }

    // MARK: - AGGREGATE

    public func executeAggregate(from table: String, _ function: AggregateFunction, where condition: WhereCondition? = nil, transactionContext: TransactionContext? = nil) async throws -> DBValue {
        // Aggregate pushdown: avoid full table scan for common patterns

        // COUNT(*) without WHERE → use registry record count
        if case .count(let col) = function, col == nil, condition == nil {
            let count = planner.registry.getTableInfo(name: table)?.recordCount ?? 0
            return .integer(Int64(count))
        }

        // MIN/MAX on indexed column without WHERE → use B-tree endpoints
        if condition == nil {
            switch function {
            case .min(let column):
                if let index = indexManager.getIndex(tableName: table, columnName: column) {
                    let firstRows = try await index.searchRangeWithLimit(from: nil, to: nil, limit: 1, ascending: true)
                    if let first = firstRows.first, let value = first.values[column], value != .null {
                        return value
                    }
                }
            case .max(let column):
                if let index = indexManager.getIndex(tableName: table, columnName: column) {
                    let lastRows = try await index.searchRangeWithLimit(from: nil, to: nil, limit: 1, ascending: false)
                    if let last = lastRows.first, let value = last.values[column], value != .null {
                        return value
                    }
                }
            default:
                break
            }
        }

        // COUNT(*) or COUNT(col) with simple range condition on indexed column → B-tree range count
        if case .count = function, let condition = condition {
            let indexCount = try await indexAcceleratedCount(table: table, condition: condition)
            if let result = indexCount { return result }
        }

        // Streaming aggregate: scan pages directly, extract only the needed column
        let pageIDs = transactionContext == nil
            ? try await storageEngine.getPageChainConcurrent(tableName: table)
            : try await storageEngine.getPageChain(tableName: table, transactionContext: transactionContext)
        let aggSchema = storageEngine.getTableSchema(table)
        let aggColMap = aggSchema?.columnOrdinals

        // Resolve the column index for direct extraction (O(1) per record)
        let aggColumnName: String? = {
            switch function {
            case .count(let c): return c
            case .sum(let c), .avg(let c), .min(let c), .max(let c): return c
            }
        }()
        let aggColumnIndex: Int? = aggColumnName.flatMap { aggColMap?[$0] }

        var count: Int64 = 0
        var intSum: Int64 = 0; var doubleSum: Double = 0; var allIntegers = true; var hasValue = false
        var avgSum: Double = 0; var avgCount: Int64 = 0
        var minVal: DBValue = .null; var maxVal: DBValue = .null

        // Inline closure for processing a single record's data in the aggregate
        func processRecordData(_ recordData: Data) {
            var lazyFilterResult: Bool? = nil
            if let cond = condition, let colMap = aggColMap {
                lazyFilterResult = evaluateConditionLazy(cond, data: recordData, columnIndexMap: colMap)
                if lazyFilterResult == false { return }
            }

            if case .count(nil) = function, lazyFilterResult == true {
                count += 1
                return
            }

            if let colIdx = aggColumnIndex {
                if let rawValue = Row.extractColumnValue(from: recordData, columnIndex: colIdx) {
                    if condition != nil && lazyFilterResult == nil {
                        guard let row = Row.fromBytesAuto(recordData, schema: aggSchema) else { return }
                        if !evaluateCondition(condition!, row: row) { return }
                    }

                    switch function {
                    case .count:
                        if rawValue != .null { count += 1 }
                    case .sum:
                        switch rawValue {
                        case .integer(let v): hasValue = true; let (r, o) = intSum.addingReportingOverflow(v); if o { allIntegers = false }; intSum = r; doubleSum += Double(v)
                        case .double(let v): hasValue = true; allIntegers = false; doubleSum += v
                        default: break
                        }
                    case .avg:
                        if let v = numericValue(rawValue) { avgSum += v; avgCount += 1 }
                    case .min:
                        if rawValue != .null { if minVal == .null || rawValue < minVal { minVal = rawValue } }
                    case .max:
                        if rawValue != .null { if maxVal == .null || rawValue > maxVal { maxVal = rawValue } }
                    }
                    return
                }
            }

            guard let row = Row.fromBytesAuto(recordData, schema: aggSchema) else { return }
            if let cond = condition, !evaluateCondition(cond, row: row) { return }

            switch function {
            case .count(let col):
                if let col = col {
                    if let v = row.values[col], v != .null { count += 1 }
                } else {
                    count += 1
                }
            case .sum(let col):
                if let dbVal = row.values[col] {
                    switch dbVal {
                    case .integer(let v): hasValue = true; let (r, o) = intSum.addingReportingOverflow(v); if o { allIntegers = false }; intSum = r; doubleSum += Double(v)
                    case .double(let v): hasValue = true; allIntegers = false; doubleSum += v
                    default: break
                    }
                }
            case .avg(let col):
                if let v = numericValue(row.values[col]) { avgSum += v; avgCount += 1 }
            case .min(let col):
                if let v = row.values[col], v != .null {
                    if minVal == .null || v < minVal { minVal = v }
                }
            case .max(let col):
                if let v = row.values[col], v != .null {
                    if maxVal == .null || v > maxVal { maxVal = v }
                }
            }
        }

        // Zero-copy inline closure: processes a record via UnsafeRawBufferPointer (no Data allocation)
        func processRecordUnsafe(_ ptr: UnsafeRawBufferPointer) {
            if let colIdx = aggColumnIndex {
                if condition == nil {
                    // Fast path: no condition, just extract + aggregate
                    if let rawValue = Row.extractColumnValueUnsafe(from: ptr, columnIndex: colIdx) {
                        switch function {
                        case .count:
                            if rawValue != .null { count += 1 }
                        case .sum:
                            switch rawValue {
                            case .integer(let v): hasValue = true; let (r, o) = intSum.addingReportingOverflow(v); if o { allIntegers = false }; intSum = r; doubleSum += Double(v)
                            case .double(let v): hasValue = true; allIntegers = false; doubleSum += v
                            default: break
                            }
                        case .avg:
                            if let v = numericValue(rawValue) { avgSum += v; avgCount += 1 }
                        case .min:
                            if rawValue != .null { if minVal == .null || rawValue < minVal { minVal = rawValue } }
                        case .max:
                            if rawValue != .null { if maxVal == .null || rawValue > maxVal { maxVal = rawValue } }
                        }
                        return
                    }
                }
            }
            // Condition present or column extraction failed — fall back to Data-based path
            let recordData = Data(bytes: ptr.baseAddress!, count: ptr.count)
            processRecordData(recordData)
        }

        for pageID in pageIDs {
            // Zero-copy mmap fast path: iterate records via UnsafeRawBufferPointer
            if transactionContext == nil {
                let handled = storageEngine.forEachRecordOnPageMmapUnsafe(pageID: pageID) { _, ptr in
                    processRecordUnsafe(ptr)
                    return true // continue
                }
                if handled { continue }
            }

            // Fallback: full page load (dirty pages, encrypted, overflow)
            let page = transactionContext == nil
                ? try await storageEngine.getPageConcurrent(pageID: pageID)
                : try await storageEngine.getPage(pageID: pageID, transactionContext: transactionContext)
            for var record in page.records {
                if record.isOverflow {
                    record = try await storageEngine.reassembleOverflowRecord(record)
                }
                processRecordData(record.data)
            }
        }

        switch function {
        case .count: return .integer(count)
        case .sum: return hasValue ? (allIntegers ? .integer(intSum) : .double(doubleSum)) : .null
        case .avg: return avgCount > 0 ? .double(avgSum / Double(avgCount)) : .null
        case .min: return minVal
        case .max: return maxVal
        }
    }

    /// Serial aggregate computation
    private func computeAggregate(rows: [Row], function: AggregateFunction) -> DBValue {
        switch function {
        case .count(let column):
            if let column = column {
                let count = rows.filter { $0.values[column] != nil && $0.values[column] != .null }.count
                return .integer(Int64(count))
            }
            return .integer(Int64(rows.count))

        case .sum(let column):
            var intSum: Int64 = 0
            var doubleSum: Double = 0
            var allIntegers = true
            var hasValue = false
            for row in rows {
                guard let dbValue = row.values[column] else { continue }
                switch dbValue {
                case .integer(let v):
                    hasValue = true
                    let (result, overflow) = intSum.addingReportingOverflow(v)
                    if overflow { allIntegers = false }
                    intSum = result
                    doubleSum += Double(v)
                case .double(let v):
                    hasValue = true
                    allIntegers = false
                    doubleSum += v
                default: break
                }
            }
            if !hasValue { return .null }
            return allIntegers ? .integer(intSum) : .double(doubleSum)

        case .avg(let column):
            var sum: Double = 0
            var count = 0
            for row in rows {
                if let value = numericValue(row.values[column]) {
                    sum += value
                    count += 1
                }
            }
            return count > 0 ? .double(sum / Double(count)) : .null

        case .min(let column):
            var result: DBValue = .null
            for row in rows {
                guard let value = row.values[column], value != .null else { continue }
                if result == .null || value < result {
                    result = value
                }
            }
            return result

        case .max(let column):
            var result: DBValue = .null
            for row in rows {
                guard let value = row.values[column], value != .null else { continue }
                if result == .null || value > result {
                    result = value
                }
            }
            return result
        }
    }

    /// Parallel aggregate: partition rows, compute per-partition, merge results
    private func parallelAggregate(rows: [Row], function: AggregateFunction) async throws -> DBValue {
        let partitionCount = min(8, max(2, rows.count / 500))
        let chunkSize = (rows.count + partitionCount - 1) / partitionCount

        let partialResults: [DBValue] = try await withThrowingTaskGroup(of: DBValue.self) { group in
            for i in 0..<partitionCount {
                let start = i * chunkSize
                let end = min(start + chunkSize, rows.count)
                guard start < end else { continue }
                let chunk = Array(rows[start..<end])
                group.addTask {
                    if case .avg(let column) = function {
                        return self.computePartialAVG(rows: chunk, column: column)
                    }
                    return self.computeAggregate(rows: chunk, function: function)
                }
            }
            var results = [DBValue]()
            for try await result in group { results.append(result) }
            return results
        }

        // Merge partial results
        return mergeAggregateResults(partialResults, function: function, totalRowCount: rows.count)
    }

    /// Merge partial aggregate results from parallel partitions
    private func mergeAggregateResults(_ results: [DBValue], function: AggregateFunction, totalRowCount: Int) -> DBValue {
        switch function {
        case .count:
            var total: Int64 = 0
            for r in results { if case .integer(let v) = r { total += v } }
            return .integer(total)

        case .sum:
            var doubleSum: Double = 0; var intSum: Int64 = 0; var allInts = true; var hasValue = false
            for r in results {
                switch r {
                case .integer(let v): intSum += v; doubleSum += Double(v); hasValue = true
                case .double(let v): doubleSum += v; allInts = false; hasValue = true
                default: break
                }
            }
            if !hasValue { return .null }
            return allInts ? .integer(intSum) : .double(doubleSum)

        case .avg:
            // Each partial result is .compound([.double(sum), .integer(count)])
            var totalSum: Double = 0
            var totalCount: Int64 = 0
            for r in results {
                if case .compound(let parts) = r, parts.count == 2,
                   case .double(let s) = parts[0], case .integer(let c) = parts[1] {
                    totalSum += s
                    totalCount += c
                }
            }
            return totalCount > 0 ? .double(totalSum / Double(totalCount)) : .null

        case .min:
            var best: DBValue = .null
            for r in results where r != .null {
                if best == .null || r < best { best = r }
            }
            return best

        case .max:
            var best: DBValue = .null
            for r in results where r != .null {
                if best == .null || r > best { best = r }
            }
            return best
        }
    }

    /// Compute partial AVG as (sum, count) for correct parallel merging.
    /// Returns .compound([.double(sum), .integer(count)]) instead of the divided average.
    private func computePartialAVG(rows: [Row], column: String) -> DBValue {
        var sum: Double = 0
        var count: Int64 = 0
        for row in rows {
            if let value = numericValue(row.values[column]) {
                sum += value
                count += 1
            }
        }
        return .compound([.double(sum), .integer(count)])
    }

    /// Try to answer COUNT using a B-tree range count on an indexed column.
    /// Returns nil if the condition can't be served by an index.
    private func indexAcceleratedCount(table: String, condition: WhereCondition) async throws -> DBValue? {
        switch condition {
        case .greaterThan(let col, let value):
            if let index = indexManager.getIndex(tableName: table, columnName: col) {
                // age > 30 → count from 30 (exclusive). Use the next representable value as lower bound.
                let startKey = incrementKey(value)
                let count = try await index.countRange(from: startKey, to: nil)
                return .integer(count)
            }
        case .greaterThanOrEqual(let col, let value):
            if let index = indexManager.getIndex(tableName: table, columnName: col) {
                let count = try await index.countRange(from: value, to: nil)
                return .integer(count)
            }
        case .lessThan(let col, let value):
            if let index = indexManager.getIndex(tableName: table, columnName: col) {
                // Use decrementKey for exclusive upper bound
                let endKey = decrementKey(value)
                let count = try await index.countRange(from: nil, to: endKey)
                return .integer(count)
            }
        case .lessThanOrEqual(let col, let value):
            if let index = indexManager.getIndex(tableName: table, columnName: col) {
                let count = try await index.countRange(from: nil, to: value)
                return .integer(count)
            }
        case .between(let col, let minVal, let maxVal):
            if let index = indexManager.getIndex(tableName: table, columnName: col) {
                let count = try await index.countRange(from: minVal, to: maxVal)
                return .integer(count)
            }
        case .equals(let col, let value):
            if let index = indexManager.getIndex(tableName: table, columnName: col) {
                if let rows = try await index.search(key: value) {
                    return .integer(Int64(rows.count))
                }
            }
        default:
            break
        }
        return nil
    }

    /// Return the next representable value (for exclusive lower bound in range count).
    private func incrementKey(_ value: DBValue) -> DBValue {
        switch value {
        case .integer(let v): return .integer(v + 1)
        case .double(let v): return .double(v.nextUp)
        case .string(let s): return .string(s + "\0")
        default: return value
        }
    }

    /// Return the previous representable value (for exclusive upper bound in range count).
    private func decrementKey(_ value: DBValue) -> DBValue {
        switch value {
        case .integer(let v): return .integer(v - 1)
        case .double(let v): return .double(v.nextDown)
        default: return value
        }
    }

    private func numericValue(_ value: DBValue?) -> Double? {
        guard let value = value else { return nil }
        switch value {
        case .integer(let v): return Double(v)
        case .double(let v): return v
        default: return nil
        }
    }

    // MARK: - Condition Evaluation

    private func evaluateCondition(_ condition: WhereCondition, row: Row) -> Bool {
        switch condition {
        case let .equals(column, value):
            // SQL: NULL = anything is false; use isNull for NULL checks
            if value == .null { return false }
            guard let rowValue = row.values[column], rowValue != .null else { return false }
            return rowValue == value
        case let .notEquals(column, value):
            // SQL: NULL != anything is false; use isNotNull for NULL checks
            if value == .null { return false }
            guard let rowValue = row.values[column], rowValue != .null else { return false }
            return rowValue != value
        case let .lessThan(column, value):
            guard let rowValue = row.values[column], rowValue != .null, value != .null else { return false }
            return rowValue < value
        case let .greaterThan(column, value):
            guard let rowValue = row.values[column], rowValue != .null, value != .null else { return false }
            return rowValue > value
        case let .lessThanOrEqual(column, value):
            guard let rowValue = row.values[column], rowValue != .null, value != .null else { return false }
            return rowValue <= value
        case let .greaterThanOrEqual(column, value):
            guard let rowValue = row.values[column], rowValue != .null, value != .null else { return false }
            return rowValue >= value
        case let .in(column, values):
            guard let rowValue = row.values[column], rowValue != .null else { return false }
            return values.contains(rowValue)
        case let .between(column, min, max):
            guard let rowValue = row.values[column], rowValue != .null, min != .null, max != .null else { return false }
            return rowValue >= min && rowValue <= max
        case let .like(column, pattern):
            guard let rowValue = row.values[column], case .string(let str) = rowValue else { return false }
            return matchLikePattern(str, pattern: pattern)
        case let .isNull(column):
            return row.values[column] == nil || row.values[column] == .null
        case let .isNotNull(column):
            return row.values[column] != nil && row.values[column] != .null
        case let .and(conditions):
            // Reorder: cheapest/most-selective predicates first for faster short-circuit
            let ordered = conditions.count <= 1 ? conditions : conditions.sorted { conditionCost($0) < conditionCost($1) }
            return ordered.allSatisfy { evaluateCondition($0, row: row) }
        case let .or(conditions):
            // Reorder: cheapest predicates first for faster short-circuit
            let ordered = conditions.count <= 1 ? conditions : conditions.sorted { conditionCost($0) < conditionCost($1) }
            return ordered.contains { evaluateCondition($0, row: row) }
        }
    }

    /// Static cost heuristic for condition types. Lower = cheaper/more selective.
    /// Used to reorder AND/OR children for faster short-circuit evaluation.
    private func conditionCost(_ condition: WhereCondition) -> Int {
        switch condition {
        case .isNull, .isNotNull: return 1      // single nil check
        case .equals: return 2                   // single comparison, high selectivity
        case .notEquals: return 3                // single comparison, low selectivity
        case .in: return 4                       // set membership
        case .lessThan, .greaterThan, .lessThanOrEqual, .greaterThanOrEqual: return 5
        case .between: return 6                  // two comparisons
        case .like: return 10                    // string pattern matching
        case .and(let subs): return 20 + subs.count
        case .or(let subs): return 20 + subs.count
        }
    }

    // MARK: - Lazy Evaluation Helpers

    /// Extract all column names referenced in a WHERE condition.
    private func columnsReferenced(in condition: WhereCondition) -> Set<String> {
        switch condition {
        case .equals(let col, _), .notEquals(let col, _),
             .lessThan(let col, _), .greaterThan(let col, _),
             .lessThanOrEqual(let col, _), .greaterThanOrEqual(let col, _):
            return [col]
        case .in(let col, _), .between(let col, _, _), .like(let col, _):
            return [col]
        case .isNull(let col), .isNotNull(let col):
            return [col]
        case .and(let subs):
            return subs.reduce(into: Set<String>()) { $0.formUnion(columnsReferenced(in: $1)) }
        case .or(let subs):
            return subs.reduce(into: Set<String>()) { $0.formUnion(columnsReferenced(in: $1)) }
        }
    }

    // MARK: - Join Predicate Pushdown

    /// Decompose a WHERE condition into per-table predicates and residual cross-table predicates.
    /// Columns can be qualified ("table.col") or unqualified ("col").
    /// An unqualified column is assigned to a table if it exists in that table's schema and no other.
    /// Only top-level AND conjuncts are pushed down; OR conditions are kept as residual.
    private func decomposePredicates(
        _ condition: WhereCondition,
        tables: [String],
        tableColumns: [String: Set<String>]
    ) -> (perTable: [String: WhereCondition], residual: WhereCondition?) {
        // Flatten top-level AND into conjuncts
        let conjuncts: [WhereCondition]
        if case .and(let subs) = condition {
            conjuncts = subs
        } else {
            conjuncts = [condition]
        }

        var perTable: [String: [WhereCondition]] = [:]
        var residual: [WhereCondition] = []

        for predicate in conjuncts {
            let cols = columnsReferenced(in: predicate)
            // Resolve each column to a table
            var resolvedTable: String? = nil
            var ambiguous = false
            for col in cols {
                let table = resolveColumnTable(col, tables: tables, tableColumns: tableColumns)
                if let table = table {
                    if resolvedTable == nil {
                        resolvedTable = table
                    } else if resolvedTable != table {
                        // Predicate spans multiple tables — can't push down
                        ambiguous = true
                        break
                    }
                } else {
                    ambiguous = true
                    break
                }
            }

            if ambiguous || resolvedTable == nil {
                residual.append(predicate)
            } else {
                // Strip table prefix from columns so the scan can evaluate them
                let stripped = stripTablePrefix(predicate, table: resolvedTable!)
                perTable[resolvedTable!, default: []].append(stripped)
            }
        }

        // Combine per-table predicates into single conditions
        var result: [String: WhereCondition] = [:]
        for (table, preds) in perTable {
            if preds.count == 1 {
                result[table] = preds[0]
            } else {
                result[table] = .and(preds)
            }
        }

        let residualCondition: WhereCondition?
        if residual.isEmpty {
            residualCondition = nil
        } else if residual.count == 1 {
            residualCondition = residual[0]
        } else {
            residualCondition = .and(residual)
        }

        return (result, residualCondition)
    }

    /// Resolve a column name to its table. Handles "table.col" qualified names
    /// and unqualified names (matched if unique across tables).
    private func resolveColumnTable(
        _ column: String,
        tables: [String],
        tableColumns: [String: Set<String>]
    ) -> String? {
        // Check for qualified name "table.col"
        if let dotIdx = column.firstIndex(of: ".") {
            let tableName = String(column[column.startIndex..<dotIdx])
            if tables.contains(tableName) { return tableName }
        }

        // Unqualified: find which table owns this column (must be unique)
        var owner: String? = nil
        for table in tables {
            if tableColumns[table]?.contains(column) == true {
                if owner != nil { return nil } // ambiguous
                owner = table
            }
        }
        return owner
    }

    /// Strip "table." prefix from column references in a predicate so it can be evaluated
    /// against raw (unprefixed) rows during a table scan.
    private func stripTablePrefix(_ condition: WhereCondition, table: String) -> WhereCondition {
        let prefix = "\(table)."
        func strip(_ col: String) -> String {
            col.hasPrefix(prefix) ? String(col.dropFirst(prefix.count)) : col
        }
        switch condition {
        case .equals(let col, let val): return .equals(column: strip(col), value: val)
        case .notEquals(let col, let val): return .notEquals(column: strip(col), value: val)
        case .lessThan(let col, let val): return .lessThan(column: strip(col), value: val)
        case .greaterThan(let col, let val): return .greaterThan(column: strip(col), value: val)
        case .lessThanOrEqual(let col, let val): return .lessThanOrEqual(column: strip(col), value: val)
        case .greaterThanOrEqual(let col, let val): return .greaterThanOrEqual(column: strip(col), value: val)
        case .in(let col, let vals): return .in(column: strip(col), values: vals)
        case .between(let col, let min, let max): return .between(column: strip(col), min: min, max: max)
        case .like(let col, let pattern): return .like(column: strip(col), pattern: pattern)
        case .isNull(let col): return .isNull(column: strip(col))
        case .isNotNull(let col): return .isNotNull(column: strip(col))
        case .and(let subs): return .and(subs.map { stripTablePrefix($0, table: table) })
        case .or(let subs): return .or(subs.map { stripTablePrefix($0, table: table) })
        }
    }

    // MARK: - Query Signature Helpers (for result caching)

    /// Generate a structural signature of a WhereCondition for cache keying.
    /// Same shape but different literal values produce the same signature.
    private func queryConditionSignature(_ condition: WhereCondition) -> String {
        switch condition {
        case .equals(let col, let val): return "eq(\(col),\(dbValueSignature(val)))"
        case .notEquals(let col, let val): return "ne(\(col),\(dbValueSignature(val)))"
        case .lessThan(let col, let val): return "lt(\(col),\(dbValueSignature(val)))"
        case .greaterThan(let col, let val): return "gt(\(col),\(dbValueSignature(val)))"
        case .lessThanOrEqual(let col, let val): return "le(\(col),\(dbValueSignature(val)))"
        case .greaterThanOrEqual(let col, let val): return "ge(\(col),\(dbValueSignature(val)))"
        case .in(let col, let vals): return "in(\(col),\(vals.map { dbValueSignature($0) }.joined(separator: ",")))"
        case .between(let col, let min, let max): return "bw(\(col),\(dbValueSignature(min)),\(dbValueSignature(max)))"
        case .like(let col, let pattern): return "lk(\(col),\(pattern))"
        case .isNull(let col): return "nu(\(col))"
        case .isNotNull(let col): return "nn(\(col))"
        case .and(let subs): return "and[\(subs.map { queryConditionSignature($0) }.joined(separator: ","))]"
        case .or(let subs): return "or[\(subs.map { queryConditionSignature($0) }.joined(separator: ","))]"
        }
    }

    /// Include actual values in the signature since we cache exact results
    private func dbValueSignature(_ val: DBValue) -> String {
        switch val {
        case .null: return "N"
        case .integer(let v): return "i\(v)"
        case .double(let v): return "d\(v)"
        case .string(let v): return "s\(v.hashValue)"
        case .boolean(let v): return "b\(v)"
        case .blob(let v): return "B\(v.hashValue)"
        case .compound(let vals): return "c[\(vals.map { dbValueSignature($0) }.joined(separator: ","))]"
        }
    }

    private func queryModifiersSignature(_ mods: QueryModifiers) -> String {
        var parts: [String] = []
        if let orderBy = mods.orderBy {
            parts.append("o:\(orderBy.map { "\($0.column)\($0.direction == .ascending ? "a" : "d")" }.joined(separator: ","))")
        }
        if let limit = mods.limit { parts.append("l:\(limit)") }
        if let offset = mods.offset { parts.append("f:\(offset)") }
        if mods.distinct { parts.append("D") }
        return parts.joined(separator: ";")
    }

    /// Evaluate a WHERE condition against raw binary data using partial column extraction.
    /// Returns nil if the condition type is too complex for lazy eval.
    /// When `columnIndexMap` is provided (schema-based positional data), uses fast positional lookup.
    private func evaluateConditionLazy(_ condition: WhereCondition, data: Data, columnIndexMap: [String: Int]? = nil) -> Bool? {
        // Build a column lookup closure that dispatches to positional or self-describing extraction
        let lookupColumn: (String) -> DBValue?
        if data.count >= 2, data[data.startIndex] == 0xFF {
            guard let indexMap = columnIndexMap else { return nil }
            // Read colCount from the positional header
            var off = 2
            guard let colCount = data.readUInt16(at: &off) else { return nil }
            let cc = Int(colCount)
            lookupColumn = { col in
                guard let idx = indexMap[col] else { return nil }
                return Row.columnValuePositional(at: idx, colCount: cc, from: data)
            }
        } else {
            lookupColumn = { col in Row.columnValue(named: col, from: data) }
        }

        switch condition {
        case .equals(let col, let value):
            if value == .null { return false }
            guard let rowValue = lookupColumn(col) else { return false }
            if rowValue == .null { return false }
            return rowValue == value
        case .notEquals(let col, let value):
            if value == .null { return false }
            guard let rowValue = lookupColumn(col) else { return false }
            if rowValue == .null { return false }
            return rowValue != value
        case .lessThan(let col, let value):
            guard let rowValue = lookupColumn(col), rowValue != .null, value != .null else { return false }
            return rowValue < value
        case .greaterThan(let col, let value):
            guard let rowValue = lookupColumn(col), rowValue != .null, value != .null else { return false }
            return rowValue > value
        case .lessThanOrEqual(let col, let value):
            guard let rowValue = lookupColumn(col), rowValue != .null, value != .null else { return false }
            return rowValue <= value
        case .greaterThanOrEqual(let col, let value):
            guard let rowValue = lookupColumn(col), rowValue != .null, value != .null else { return false }
            return rowValue >= value
        case .in(let col, let values):
            guard let rowValue = lookupColumn(col), rowValue != .null else { return false }
            return values.contains(rowValue)
        case .between(let col, let min, let max):
            guard let rowValue = lookupColumn(col), rowValue != .null, min != .null, max != .null else { return false }
            return rowValue >= min && rowValue <= max
        case .like(let col, let pattern):
            guard let rowValue = lookupColumn(col), case .string(let str) = rowValue else { return false }
            return matchLikePattern(str, pattern: pattern)
        case .isNull(let col):
            let val = lookupColumn(col)
            return val == nil || val == .null
        case .isNotNull(let col):
            guard let val = lookupColumn(col) else { return false }
            return val != .null
        case .and(let subs):
            let ordered = subs.count <= 1 ? subs : subs.sorted { conditionCost($0) < conditionCost($1) }
            for sub in ordered {
                guard let result = evaluateConditionLazy(sub, data: data, columnIndexMap: columnIndexMap) else { return nil }
                if !result { return false }
            }
            return true
        case .or(let subs):
            let ordered = subs.count <= 1 ? subs : subs.sorted { conditionCost($0) < conditionCost($1) }
            for sub in ordered {
                guard let result = evaluateConditionLazy(sub, data: data, columnIndexMap: columnIndexMap) else { return nil }
                if result { return true }
            }
            return false
        }
    }

    // MARK: - JOIN Helpers

    /// Flush a batch of left join keys: batch-probe right index, batch-fetch right records, combine.
    private func _flushJoinBatch(
        pendingKeys: inout [(key: DBValue, recordData: Data)],
        rightIndex: ColumnIndex,
        join: JoinClause,
        leftPrefix: String,
        rightPrefix: String,
        leftSchema: PantryTableSchema?,
        result: inout [Row],
        totalNeeded: Int,
        transactionContext: TransactionContext?
    ) async throws {
        let batch = pendingKeys
        pendingKeys.removeAll(keepingCapacity: true)

        // Try synchronous cache-only path first, fall back to async
        let uniqueKeys = Array(Set(batch.map { $0.key }))
        let indexResults: [DBValue: [Row]]
        if let cached = rightIndex.searchBatchCached(keys: uniqueKeys) {
            indexResults = cached
        } else {
            indexResults = try await rightIndex.searchBatch(keys: uniqueKeys)
        }

        // Collect all right RIDs and batch-fetch
        var allRIDs = Set<UInt64>()
        var ridsByKey = [DBValue: [UInt64]]()
        for (key, rows) in indexResults {
            let rids = rows.compactMap { row -> UInt64? in
                guard case .integer(let ridSigned) = row.values["__rid"] else { return nil }
                return UInt64(bitPattern: ridSigned)
            }
            ridsByKey[key] = rids
            allRIDs.formUnion(rids)
        }
        guard !allRIDs.isEmpty else { return }
        let rightRecords = try await storageEngine.getRecordsByIDs(allRIDs, tableName: join.table, transactionContext: transactionContext)
        var rightRowsByRID = [UInt64: Row](minimumCapacity: rightRecords.count)
        for (record, row) in rightRecords {
            rightRowsByRID[record.id] = row
        }

        // Build combined rows using pre-computed prefixes, caching left row deserialization
        var leftRowCache = [Data: Row]()
        for (key, recordData) in batch {
            if result.count >= totalNeeded { break }
            guard let rids = ridsByKey[key], !rids.isEmpty else { continue }
            let leftRow: Row
            if let cached = leftRowCache[recordData] {
                leftRow = cached
            } else {
                guard let decoded = Row.fromBytesAuto(recordData, schema: leftSchema) else { continue }
                leftRowCache[recordData] = decoded
                leftRow = decoded
            }
            for rid in rids {
                if result.count >= totalNeeded { break }
                guard let rightRow = rightRowsByRID[rid] else { continue }
                var combined = [String: DBValue](minimumCapacity: leftRow.values.count + rightRow.values.count * 2)
                for (k, v) in leftRow.values { combined[k] = v; combined[leftPrefix + k] = v }
                for (k, v) in rightRow.values { combined[rightPrefix + k] = v; combined[k] = v }
                result.append(Row(values: combined))
            }
        }
    }

    private func scanAllRows(table: String, filter: WhereCondition? = nil, transactionContext: TransactionContext? = nil) async throws -> [Row] {
        let pageIDs = transactionContext == nil
            ? try await storageEngine.getPageChainConcurrent(tableName: table)
            : try await storageEngine.getPageChain(tableName: table, transactionContext: transactionContext)
        let scanSchema = storageEngine.getTableSchema(table)
        return try await withThrowingTaskGroup(of: (Int, [Row]).self) { group in
            for (index, pageID) in pageIDs.enumerated() {
                group.addTask { [storageEngine, filter, scanSchema] in
                    let page = transactionContext == nil
                        ? try await storageEngine.getPageConcurrent(pageID: pageID)
                        : try await storageEngine.getPage(pageID: pageID, transactionContext: transactionContext)
                    let rows = page.records.compactMap { Row.fromBytesAuto($0.data, schema: scanSchema) }
                    if let filter = filter {
                        return (index, rows.filter { self.evaluateCondition(filter, row: $0) })
                    }
                    return (index, rows)
                }
            }
            var indexed: [(Int, [Row])] = []
            for try await result in group { indexed.append(result) }
            return indexed.sorted { $0.0 < $1.0 }.flatMap { $0.1 }
        }
    }

    /// Scan a table returning raw record data + join column value, deferring full Row deserialization.
    /// Falls back to full row decode if positional extraction is not available.
    private func scanJoinRecords(table: String, joinColumn: String, filter: WhereCondition? = nil, transactionContext: TransactionContext? = nil) async throws -> [(key: DBValue, data: Data)] {
        let pageIDs = transactionContext == nil
            ? try await storageEngine.getPageChainConcurrent(tableName: table)
            : try await storageEngine.getPageChain(tableName: table, transactionContext: transactionContext)
        let schema = storageEngine.getTableSchema(table)
        let colIdx = schema?.columnOrdinals[joinColumn]

        // Build column index map for lazy WHERE evaluation
        let columnIndexMap: [String: Int]? = schema?.columnOrdinals

        var results = [(key: DBValue, data: Data)]()
        for pageID in pageIDs {
            let page = transactionContext == nil
                ? try await storageEngine.getPageConcurrent(pageID: pageID)
                : try await storageEngine.getPage(pageID: pageID, transactionContext: transactionContext)
            for record in page.records {
                // Apply pushed WHERE filter lazily if possible
                if let cond = filter {
                    if let lazyResult = evaluateConditionLazy(cond, data: record.data, columnIndexMap: columnIndexMap) {
                        if !lazyResult { continue }
                    } else {
                        // Lazy eval can't handle this condition — fall back to full decode
                        guard let row = Row.fromBytesAuto(record.data, schema: schema) else { continue }
                        if !evaluateCondition(cond, row: row) { continue }
                    }
                }

                // Extract just the join column value positionally when possible
                if let idx = colIdx, let key = Row.extractColumnValue(from: record.data, columnIndex: idx) {
                    if key != .null {
                        results.append((key: key, data: record.data))
                    }
                } else {
                    guard let row = Row.fromBytesAuto(record.data, schema: schema) else { continue }
                    if let key = row.values[joinColumn], key != .null {
                        results.append((key: key, data: record.data))
                    }
                }
            }
        }
        return results
    }

    private func prefixRow(_ row: Row, table: String) -> [String: DBValue] {
        var values = [String: DBValue]()
        for (k, v) in row.values {
            values[table + "." + k] = v
            values[k] = v
        }
        return values
    }

    /// Hash inner join with deferred deserialization: builds hash table on raw (key, Data) tuples,
    /// only deserializes matching rows.
    private func hashInnerJoinLazy(left: [(key: DBValue, data: Data)], right: [(key: DBValue, data: Data)], join: JoinClause, leftTable: String, leftSchema: PantryTableSchema?, rightSchema: PantryTableSchema?, strategy: JoinStrategy, limit: Int? = nil) -> [Row] {
        let buildOnRight: Bool
        if case .hashJoin(let side) = strategy { buildOnRight = (side == .right) } else { buildOnRight = true }

        let rightPrefix = join.table + "."
        let leftPrefix = leftTable + "."

        if buildOnRight {
            // Build hash table on right raw records
            var hashTable = [DBValue: [Data]]()
            for (key, data) in right {
                hashTable[key, default: []].append(data)
            }
            var result = [Row]()
            for (leftKey, leftData) in left {
                guard let rightDataList = hashTable[leftKey] else { continue }
                guard let leftRow = Row.fromBytesAuto(leftData, schema: leftSchema) else { continue }
                for rightData in rightDataList {
                    guard let rightRow = Row.fromBytesAuto(rightData, schema: rightSchema) else { continue }
                    var combined = [String: DBValue](minimumCapacity: leftRow.values.count + rightRow.values.count * 2)
                    for (k, v) in leftRow.values { combined[leftPrefix + k] = v; combined[k] = v }
                    for (k, v) in rightRow.values { combined[rightPrefix + k] = v; combined[k] = v }
                    result.append(Row(values: combined))
                    if let limit, result.count >= limit { return result }
                }
            }
            return result
        } else {
            // Build hash table on left raw records
            var hashTable = [DBValue: [Data]]()
            for (key, data) in left {
                hashTable[key, default: []].append(data)
            }
            var result = [Row]()
            for (rightKey, rightData) in right {
                guard let leftDataList = hashTable[rightKey] else { continue }
                guard let rightRow = Row.fromBytesAuto(rightData, schema: rightSchema) else { continue }
                for leftData in leftDataList {
                    guard let leftRow = Row.fromBytesAuto(leftData, schema: leftSchema) else { continue }
                    var combined = [String: DBValue](minimumCapacity: leftRow.values.count + rightRow.values.count * 2)
                    for (k, v) in leftRow.values { combined[leftPrefix + k] = v; combined[k] = v }
                    for (k, v) in rightRow.values { combined[rightPrefix + k] = v; combined[k] = v }
                    result.append(Row(values: combined))
                    if let limit, result.count >= limit { return result }
                }
            }
            return result
        }
    }

    /// Hash inner join: builds hash table on the smaller side (per planner strategy)
    private func hashInnerJoin(left: [Row], right: [Row], join: JoinClause, strategy: JoinStrategy, limit: Int? = nil) -> [Row] {
        let buildOnRight: Bool
        if case .hashJoin(let side) = strategy { buildOnRight = (side == .right) } else { buildOnRight = true }

        let rightPrefix = join.table + "."

        if buildOnRight {
            var hashTable = [DBValue: [Row]]()
            for row in right {
                let key = row.values[join.rightColumn] ?? .null
                if key != .null { hashTable[key, default: []].append(row) }
            }
            var result = [Row]()
            for leftRow in left {
                let leftKey = leftRow.values[join.leftColumn] ?? .null
                guard leftKey != .null, let matches = hashTable[leftKey] else { continue }
                for rightRow in matches {
                    var combined = leftRow.values
                    for (k, v) in rightRow.values { combined[rightPrefix + k] = v; combined[k] = v }
                    result.append(Row(values: combined))
                    if let limit, result.count >= limit { return result }
                }
            }
            return result
        } else {
            var hashTable = [DBValue: [Row]]()
            for row in left {
                let key = row.values[join.leftColumn] ?? .null
                if key != .null { hashTable[key, default: []].append(row) }
            }
            var result = [Row]()
            for rightRow in right {
                let rightKey = rightRow.values[join.rightColumn] ?? .null
                guard rightKey != .null, let matches = hashTable[rightKey] else { continue }
                for leftRow in matches {
                    var combined = leftRow.values
                    for (k, v) in rightRow.values { combined[rightPrefix + k] = v; combined[k] = v }
                    result.append(Row(values: combined))
                    if let limit, result.count >= limit { return result }
                }
            }
            return result
        }
    }

    /// Hash left join: always probes with left, builds on right
    private func hashLeftJoin(left: [Row], right: [Row], join: JoinClause, strategy: JoinStrategy) -> [Row] {
        let rightPrefix = join.table + "."
        var hashTable = [DBValue: [Row]]()
        var rightColumns = Set<String>()
        for row in right {
            let key = row.values[join.rightColumn] ?? .null
            if key != .null { hashTable[key, default: []].append(row) }
            rightColumns.formUnion(row.values.keys)
        }
        // Pre-compute prefixed null columns for unmatched left rows
        let prefixedNullCols = rightColumns.map { rightPrefix + $0 }

        var result = [Row]()
        for leftRow in left {
            let leftKey = leftRow.values[join.leftColumn] ?? .null
            if leftKey != .null, let matches = hashTable[leftKey] {
                for rightRow in matches {
                    var combined = leftRow.values
                    for (k, v) in rightRow.values { combined[rightPrefix + k] = v; combined[k] = v }
                    result.append(Row(values: combined))
                }
            } else {
                var combined = leftRow.values
                for col in prefixedNullCols { combined[col] = .null }
                result.append(Row(values: combined))
            }
        }
        return result
    }

    /// Hash right join: builds on left, probes with right
    private func hashRightJoin(left: [Row], right: [Row], join: JoinClause, strategy: JoinStrategy) -> [Row] {
        let rightPrefix = join.table + "."
        var hashTable = [DBValue: [Row]]()
        var leftColumns = Set<String>()
        for leftRow in left {
            let key = leftRow.values[join.leftColumn] ?? .null
            if key != .null { hashTable[key, default: []].append(leftRow) }
            leftColumns.formUnion(leftRow.values.keys)
        }

        var result = [Row]()
        for rightRow in right {
            var rightPrefixed = [String: DBValue](minimumCapacity: rightRow.values.count * 2)
            for (k, v) in rightRow.values { rightPrefixed[rightPrefix + k] = v; rightPrefixed[k] = v }

            let rightKey = rightRow.values[join.rightColumn] ?? .null
            if rightKey != .null, let matches = hashTable[rightKey] {
                for leftRow in matches {
                    var combined = leftRow.values
                    combined.merge(rightPrefixed) { _, new in new }
                    result.append(Row(values: combined))
                }
            } else {
                var combined = rightPrefixed
                for col in leftColumns { if combined[col] == nil { combined[col] = .null } }
                result.append(Row(values: combined))
            }
        }
        return result
    }

    private func crossJoin(left: [Row], right: [Row], join: JoinClause) -> [Row] {
        let rightPrefix = join.table + "."
        var result = [Row]()
        result.reserveCapacity(left.count * right.count)
        for leftRow in left {
            for rightRow in right {
                var combined = leftRow.values
                for (k, v) in rightRow.values { combined[rightPrefix + k] = v; combined[k] = v }
                result.append(Row(values: combined))
            }
        }
        return result
    }

    // MARK: - Index Coverage

    /// Build a map of column name → set of columns covered by that index
    private func buildIndexCoverage(table: String) async -> [String: Set<String>] {
        let indexes = indexManager.getIndexes(tableName: table)
        var coverage = [String: Set<String>]()
        for (colName, idx) in indexes {
            let covered = await idx.coveredColumns
            coverage[colName] = covered
        }
        return coverage
    }

    // MARK: - Parallel Table Scan

    /// Decode a row, using projected decode when possible for fewer allocations.
    private func decodeRow(_ data: Data, schema: PantryTableSchema?, neededColumns: Set<String>?) -> Row? {
        if let schema = schema, let needed = neededColumns {
            return Row.fromBytesProjectedV3(data, schema: schema, neededColumns: needed)
        }
        return Row.fromBytesAuto(data, schema: schema)
    }

    private func parallelTableScan(pageIDs: [Int], condition: WhereCondition?, limit: Int? = nil, neededColumns: Set<String>? = nil, projectColumns: Set<String>? = nil, schema: PantryTableSchema? = nil, transactionContext: TransactionContext?) async throws -> [Row] {
        if let condition = condition {
            let conditionColumns = columnsReferenced(in: condition)
            let useLazy = conditionColumns.count <= 3

            // Precompute column name → index map for positional lazy evaluation
            let columnIndexMap: [String: Int]?
            if let schema = schema {
                var map = [String: Int]()
                map.reserveCapacity(schema.columns.count)
                for (i, col) in schema.columns.enumerated() { map[col.name] = i }
                columnIndexMap = map
            } else {
                columnIndexMap = nil
            }

            // Sequential early-exit path when limit is set (avoids over-scanning)
            if let limit = limit {
                var rows: [Row] = []
                rows.reserveCapacity(limit)
                for pageID in pageIDs {
                    let page = try await storageEngine.getPage(pageID: pageID, transactionContext: transactionContext)
                    for var record in page.records {
                        // Lazy overflow: try partial decode from inline data
                        if record.isOverflow {
                            if let needed = neededColumns,
                               let partialRow = Row.fromBytesPartial(record.data, neededColumns: needed) {
                                if evaluateCondition(condition, row: partialRow) {
                                    rows.append(partialRow)
                                    if rows.count >= limit { return rows }
                                }
                                continue
                            }
                            record = try await storageEngine.reassembleOverflowRecord(record)
                        }
                        if useLazy, let result = evaluateConditionLazy(condition, data: record.data, columnIndexMap: columnIndexMap) {
                            if result, let row = decodeRow(record.data, schema: schema, neededColumns: projectColumns) {
                                rows.append(row)
                                if rows.count >= limit { return rows }
                            }
                        } else if let row = decodeRow(record.data, schema: schema, neededColumns: projectColumns), evaluateCondition(condition, row: row) {
                            rows.append(row)
                            if rows.count >= limit { return rows }
                        }
                    }
                }
                return rows
            }

            let cpuCount = max(1, ProcessInfo.processInfo.activeProcessorCount)
            let chunkCount = max(1, min(pageIDs.count, cpuCount))
            let useConcurrent = transactionContext == nil
            return try await withThrowingTaskGroup(of: (Int, [Row]).self) { group in
                for chunkIdx in 0..<chunkCount {
                    let start = chunkIdx * pageIDs.count / chunkCount
                    let end = (chunkIdx + 1) * pageIDs.count / chunkCount
                    guard start < end else { continue }
                    let chunkPageIDs = Array(pageIDs[start..<end])
                    group.addTask { [storageEngine] in
                        var chunkRows: [Row] = []
                        // Batch read all pages in this chunk
                        let chunkPages: [DatabasePage] = useConcurrent
                            ? try await storageEngine.getPagesConcurrent(pageIDs: chunkPageIDs)
                            : try await { var p = [DatabasePage](); for id in chunkPageIDs { p.append(try await storageEngine.getPage(pageID: id, transactionContext: transactionContext)) }; return p }()
                        for page in chunkPages {
                            for var record in page.records {
                                if record.isOverflow {
                                    if let needed = neededColumns,
                                       let partialRow = Row.fromBytesPartial(record.data, neededColumns: needed) {
                                        if self.evaluateCondition(condition, row: partialRow) {
                                            chunkRows.append(partialRow)
                                        }
                                        continue
                                    }
                                    record = try await storageEngine.reassembleOverflowRecord(record)
                                }
                                if useLazy, let result = self.evaluateConditionLazy(condition, data: record.data, columnIndexMap: columnIndexMap) {
                                    if result, let row = self.decodeRow(record.data, schema: schema, neededColumns: projectColumns) {
                                        chunkRows.append(row)
                                    }
                                } else if let row = self.decodeRow(record.data, schema: schema, neededColumns: projectColumns), self.evaluateCondition(condition, row: row) {
                                    chunkRows.append(row)
                                }
                            }
                        }
                        return (chunkIdx, chunkRows)
                    }
                }
                var indexed: [(Int, [Row])] = []
                for try await result in group { indexed.append(result) }
                return indexed.sorted { $0.0 < $1.0 }.flatMap { $0.1 }
            }
        } else {
            // Sequential early-exit path when limit is set
            if let limit = limit {
                var rows: [Row] = []
                rows.reserveCapacity(limit)
                for pageID in pageIDs {
                    let page = try await storageEngine.getPage(pageID: pageID, transactionContext: transactionContext)
                    for var record in page.records {
                        if record.isOverflow {
                            if let needed = neededColumns,
                               let partialRow = Row.fromBytesPartial(record.data, neededColumns: needed) {
                                rows.append(partialRow)
                                if rows.count >= limit { return rows }
                                continue
                            }
                            record = try await storageEngine.reassembleOverflowRecord(record)
                        }
                        if let row = decodeRow(record.data, schema: schema, neededColumns: projectColumns) {
                            rows.append(row)
                            if rows.count >= limit { return rows }
                        }
                    }
                }
                return rows
            }

            let cpuCount = max(1, ProcessInfo.processInfo.activeProcessorCount)
            let chunkCount = max(1, min(pageIDs.count, cpuCount))
            let useConcurrent = transactionContext == nil
            return try await withThrowingTaskGroup(of: (Int, [Row]).self) { group in
                for chunkIdx in 0..<chunkCount {
                    let start = chunkIdx * pageIDs.count / chunkCount
                    let end = (chunkIdx + 1) * pageIDs.count / chunkCount
                    guard start < end else { continue }
                    let chunkPageIDs = Array(pageIDs[start..<end])
                    group.addTask { [storageEngine, schema] in
                        var chunkRows: [Row] = []
                        let chunkPages: [DatabasePage] = useConcurrent
                            ? try await storageEngine.getPagesConcurrent(pageIDs: chunkPageIDs)
                            : try await { var p = [DatabasePage](); for id in chunkPageIDs { p.append(try await storageEngine.getPage(pageID: id, transactionContext: transactionContext)) }; return p }()
                        for page in chunkPages {
                            for var record in page.records {
                                if record.isOverflow {
                                    if let needed = neededColumns,
                                       let partialRow = Row.fromBytesPartial(record.data, neededColumns: needed) {
                                        chunkRows.append(partialRow)
                                        continue
                                    }
                                    record = try await storageEngine.reassembleOverflowRecord(record)
                                }
                                if let row = self.decodeRow(record.data, schema: schema, neededColumns: projectColumns) {
                                    chunkRows.append(row)
                                }
                            }
                        }
                        return (chunkIdx, chunkRows)
                    }
                }
                var indexed: [(Int, [Row])] = []
                for try await result in group { indexed.append(result) }
                return indexed.sorted { $0.0 < $1.0 }.flatMap { $0.1 }
            }
        }
    }

    // MARK: - Top-N Selection

    /// Select the top N elements from an array using a bounded max-heap.
    /// O(n log k) where k = n, much faster than O(n log n) full sort when k << n.
    private func topN(_ elements: [Row], n: Int, by areInIncreasingOrder: (Row, Row) -> Bool) -> [Row] {
        guard n > 0 else { return [] }
        if n >= elements.count {
            return elements.sorted(by: areInIncreasingOrder)
        }

        // Build a max-heap of size n (reverse comparator: worst element at top)
        var heap = Array(elements.prefix(n))
        // Heapify
        for i in stride(from: heap.count / 2 - 1, through: 0, by: -1) {
            siftDown(&heap, i, areInIncreasingOrder)
        }

        // For remaining elements, if smaller than heap top, replace and re-heapify
        for i in n..<elements.count {
            if areInIncreasingOrder(elements[i], heap[0]) {
                heap[0] = elements[i]
                siftDown(&heap, 0, areInIncreasingOrder)
            }
        }

        // Sort the heap to get final ordering
        return heap.sorted(by: areInIncreasingOrder)
    }

    /// Sift down in a max-heap (largest-first, where "largest" = !areInIncreasingOrder)
    private func siftDown(_ heap: inout [Row], _ index: Int, _ areInIncreasingOrder: (Row, Row) -> Bool) {
        var parent = index
        let count = heap.count
        while true {
            var largest = parent
            let left = 2 * parent + 1
            let right = 2 * parent + 2
            // "largest" here means the element that should be evicted first (worst in sort order)
            if left < count && areInIncreasingOrder(heap[largest], heap[left]) {
                largest = left
            }
            if right < count && areInIncreasingOrder(heap[largest], heap[right]) {
                largest = right
            }
            if largest == parent { break }
            heap.swapAt(parent, largest)
            parent = largest
        }
    }

    // MARK: - DISTINCT Helper

    private func deduplicateRows(_ rows: [Row], columns: [String]?) -> [Row] {
        var seen = Set<Row>()
        var unique = [Row]()
        for row in rows {
            let key: Row
            if let columns = columns, !columns.isEmpty {
                let projected = columns.reduce(into: [String: DBValue]()) { r, c in r[c] = row.values[c] ?? .null }
                key = Row(values: projected)
            } else {
                key = row
            }
            if seen.insert(key).inserted {
                unique.append(row)
            }
        }
        return unique
    }

    // MARK: - Helpers

    /// Strip the internal __rid field from index-returned rows before returning to users
    private func stripRID(_ row: Row) -> Row {
        guard row.values["__rid"] != nil else { return row }
        var values = row.values
        values.removeValue(forKey: "__rid")
        return Row(values: values)
    }

    private var nextRecordID: UInt64 = UInt64.random(in: 1...(UInt64.max / 2))

    private func generateRecordID() -> UInt64 {
        let id = nextRecordID
        nextRecordID &+= 1
        return id
    }

    /// SQL LIKE pattern matching: % matches any sequence, _ matches any single character
    private func matchLikePattern(_ string: String, pattern: String) -> Bool {
        let s = Array(string)
        let p = Array(pattern)
        var si = 0, pi = 0
        var starSi = -1, starPi = -1

        while si < s.count {
            if pi < p.count && p[pi] == "%" {
                starPi = pi
                starSi = si
                pi += 1
            } else if pi < p.count && (p[pi] == "_" || p[pi] == s[si]) {
                si += 1
                pi += 1
            } else if starPi >= 0 {
                pi = starPi + 1
                starSi += 1
                si = starSi
            } else {
                return false
            }
        }

        while pi < p.count && p[pi] == "%" {
            pi += 1
        }

        return pi == p.count
    }
}
