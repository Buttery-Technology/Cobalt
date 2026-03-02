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
}

/// Cached query result with generation tracking
private struct CachedQueryResult {
    let rows: [Row]
    let generation: UInt64
    var lastAccess: UInt64
}

/// Executes SELECT, INSERT, UPDATE, DELETE queries against the storage engine.
/// Uses cost-based QueryPlanner for index vs scan decisions and join ordering.
public actor QueryExecutor: Sendable {
    private let storageEngine: StorageEngine
    private let indexManager: IndexManager
    private let planner: QueryPlanner

    /// Query result cache: stores SELECT results keyed by query signature
    private var resultCache: [QueryResultCacheKey: CachedQueryResult] = [:]
    /// Per-table generation counter: incremented on INSERT/UPDATE/DELETE
    private var tableGenerations: [String: UInt64] = [:]
    /// Monotonic access counter for LRU eviction
    private var resultCacheCounter: UInt64 = 0
    /// Maximum cached query results
    private let maxResultCacheSize = 128

    public init(storageEngine: StorageEngine, indexManager: IndexManager, tableRegistry: TableRegistry, costWeights: CostModelWeights = .default) {
        self.storageEngine = storageEngine
        self.indexManager = indexManager
        self.planner = QueryPlanner(storageEngine: storageEngine, indexManager: indexManager, registry: tableRegistry, costWeights: costWeights)
    }

    /// Convenience init — resolves tableRegistry from storageEngine (requires await)
    public init(storageEngine: StorageEngine, indexManager: IndexManager, costWeights: CostModelWeights = .default) async {
        self.storageEngine = storageEngine
        self.indexManager = indexManager
        let reg = await storageEngine.tableRegistry
        self.planner = QueryPlanner(storageEngine: storageEngine, indexManager: indexManager, registry: reg, costWeights: costWeights)
    }

    /// Invalidate the query plan cache (call after index create/drop or schema changes)
    public nonisolated func invalidatePlanCache() {
        planner.invalidateCache()
    }

    /// Invalidate cached plans for a specific table only
    public nonisolated func invalidatePlanCache(forTable table: String) {
        planner.invalidateCache(forTable: table)
    }

    /// Invalidate result cache for a specific table (called after mutations)
    private func invalidateResultCache(forTable table: String) {
        tableGenerations[table, default: 0] += 1
        resultCache = resultCache.filter { $0.key.table != table }
    }

    /// Look up a cached query result, returns nil on miss or stale generation
    private func lookupResultCache(key: QueryResultCacheKey) -> [Row]? {
        guard let cached = resultCache[key],
              cached.generation == tableGenerations[key.table, default: 0] else {
            return nil
        }
        resultCacheCounter += 1
        resultCache[key]?.lastAccess = resultCacheCounter
        return cached.rows
    }

    /// Store a query result in the cache with LRU eviction
    private func storeResultCache(key: QueryResultCacheKey, rows: [Row]) {
        // Don't cache very large result sets (>10K rows)
        guard rows.count <= 10_000 else { return }

        if resultCache.count >= maxResultCacheSize {
            let evictCount = maxResultCacheSize / 4
            let sorted = resultCache.sorted { $0.value.lastAccess < $1.value.lastAccess }
            for entry in sorted.prefix(evictCount) {
                resultCache.removeValue(forKey: entry.key)
            }
        }

        resultCacheCounter += 1
        resultCache[key] = CachedQueryResult(
            rows: rows,
            generation: tableGenerations[key.table, default: 0],
            lastAccess: resultCacheCounter
        )
    }

    // MARK: - SELECT

    public func executeSelect(from table: String, columns: [String]? = nil, where condition: WhereCondition? = nil, modifiers: QueryModifiers? = nil, transactionContext: TransactionContext? = nil) async throws -> [Row] {
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
        if let mods = modifiers,
           let orderBy = mods.orderBy, orderBy.count == 1,
           let limit = mods.limit, limit > 0,
           !mods.distinct,
           condition == nil {
            let orderCol = orderBy[0].column
            let ascending = orderBy[0].direction == .ascending
            if let index = await indexManager.getIndex(tableName: table, columnName: orderCol) {
                let totalNeeded = limit + (mods.offset ?? 0)
                let indexRows = try await index.searchRangeWithLimit(from: nil, to: nil, limit: totalNeeded, ascending: ascending)

                // Fetch full rows by RID
                let rids = Set(indexRows.compactMap { row -> UInt64? in
                    guard case .integer(let ridSigned) = row.values["__rid"] else { return nil }
                    return UInt64(bitPattern: ridSigned)
                })
                let fullRecords = try await storageEngine.getRecordsByIDs(rids, tableName: table, transactionContext: transactionContext)

                // Re-sort by the index order (getRecordsByIDs doesn't preserve order)
                let ridToRow = Dictionary(fullRecords.map { (record, row) in (record.id, row) }, uniquingKeysWith: { a, _ in a })
                rows = indexRows.compactMap { indexRow -> Row? in
                    guard case .integer(let ridSigned) = indexRow.values["__rid"] else { return nil }
                    return ridToRow[UInt64(bitPattern: ridSigned)]
                }

                // Apply OFFSET then LIMIT (already sorted by index)
                if let offset = mods.offset, offset > 0 {
                    rows = Array(rows.dropFirst(offset))
                }
                rows = Array(rows.prefix(limit))

                // Project columns
                if let columns = columns, !columns.isEmpty {
                    return rows.map { row in
                        let projected = columns.reduce(into: [String: DBValue]()) { r, c in r[c] = row.values[c] ?? .null }
                        return Row(values: projected)
                    }
                }
                return rows
            }
        }

        // Get page chain for cost estimation and schema for positional row decoding
        let pageIDs = try await storageEngine.getPageChain(tableName: table, transactionContext: transactionContext)
        let selectSchema = await storageEngine.getTableSchema(table)

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

        switch plan {
        case .indexOnlyScan:
            if let condition = condition,
               let indexed = try await indexManager.attemptIndexLookup(tableName: table, condition: condition) {
                rows = indexed.map { row in
                    var vals = row.values
                    vals.removeValue(forKey: "__rid")
                    return Row(values: vals)
                }
                if let scanLimit = scanLimit { rows = Array(rows.prefix(scanLimit)) }
            } else {
                rows = try await parallelTableScan(pageIDs: pageIDs, condition: condition, limit: scanLimit, neededColumns: overflowNeededColumns, schema: selectSchema, transactionContext: transactionContext)
            }

        case .indexScan:
            if let condition = condition,
               let indexed = try await indexManager.attemptIndexLookup(tableName: table, condition: condition) {
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
                rows = try await parallelTableScan(pageIDs: pageIDs, condition: condition, limit: scanLimit, neededColumns: overflowNeededColumns, schema: selectSchema, transactionContext: transactionContext)
            }

        case .tableScan:
            rows = try await parallelTableScan(pageIDs: pageIDs, condition: condition, limit: scanLimit, neededColumns: overflowNeededColumns, schema: selectSchema, transactionContext: transactionContext)
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
            if let key = cacheKey { storeResultCache(key: key, rows: projected) }
            return projected
        }

        if let key = cacheKey { storeResultCache(key: key, rows: rows) }
        return rows
    }

    // MARK: - JOIN

    public func executeJoin(from table: String, joins: [JoinClause], columns: [String]? = nil, where condition: WhereCondition? = nil, modifiers: QueryModifiers? = nil, transactionContext: TransactionContext? = nil) async throws -> [Row] {
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
                if let schema = await storageEngine.getTableSchema(t) {
                    tableColumns[t] = Set(schema.columns.map { $0.name })
                }
            }

            let (perTable, residual) = decomposePredicates(condition, tables: allTables, tableColumns: tableColumns)
            pushedPredicates = perTable
            residualCondition = residual
        }

        // Parallel scan: load left table and first right table concurrently (with pushed filters)
        let firstRightTable = optimizedJoins.first?.table
        let leftFilter = pushedPredicates[table]
        let firstRightFilter = firstRightTable.flatMap { pushedPredicates[$0] }
        let (leftRows, firstRightRows) = try await withThrowingTaskGroup(of: (String, [Row]).self) { group -> ([Row], [Row]) in
            group.addTask { ("left", try await self.scanAllRows(table: table, filter: leftFilter, transactionContext: transactionContext)) }
            if let rightTable = firstRightTable {
                group.addTask { ("right", try await self.scanAllRows(table: rightTable, filter: firstRightFilter, transactionContext: transactionContext)) }
            }
            var left = [Row](), right = [Row]()
            for try await (tag, rows) in group {
                if tag == "left" { left = rows } else { right = rows }
            }
            return (left, right)
        }

        var result = leftRows.map { row in
            // Prefix left table columns with "tableName."
            var prefixed = [String: DBValue]()
            for (k, v) in row.values { prefixed["\(table).\(k)"] = v; prefixed[k] = v }
            return Row(values: prefixed)
        }

        // Process each join in optimized order, using planner's join strategy
        for (i, join) in optimizedJoins.enumerated() {
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
                result = hashInnerJoin(left: result, right: rightRows, join: join, strategy: strategy)
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
            return result.map { row in
                let projected = columns.reduce(into: [String: DBValue]()) { r, c in r[c] = row.values[c] ?? .null }
                return Row(values: projected)
            }
        }

        return result
    }

    // MARK: - GROUP BY

    public func executeGroupBy(from table: String, select expressions: [SelectExpression], where condition: WhereCondition? = nil, groupBy: GroupByClause, modifiers: QueryModifiers? = nil, transactionContext: TransactionContext? = nil) async throws -> [Row] {
        // Streaming hash aggregate: scan pages directly, accumulate per-group state
        let pageIDs = try await storageEngine.getPageChain(tableName: table, transactionContext: transactionContext)
        let gbSchema = await storageEngine.getTableSchema(table)

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

        for pageID in pageIDs {
            let page = try await storageEngine.getPage(pageID: pageID, transactionContext: transactionContext)
            for var record in page.records {
                if record.isOverflow {
                    record = try await storageEngine.reassembleOverflowRecord(record)
                }
                guard let row = Row.fromBytesAuto(record.data, schema: gbSchema) else { continue }
                if let cond = condition, !evaluateCondition(cond, row: row) { continue }

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
        if let schema = await storageEngine.getTableSchema(table),
           let pkColumn = schema.primaryKeyColumn {
            if let pkValue = row.values[pkColumn.name], pkValue != .null {
                // Check index first if available, otherwise scan
                if let indexed = try await indexManager.attemptIndexLookup(
                    tableName: table,
                    condition: .equals(column: pkColumn.name, value: pkValue)
                ), !indexed.isEmpty {
                    throw PantryError.primaryKeyViolation
                } else if !(await indexManager.hasIndex(tableName: table, columnName: pkColumn.name)) {
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

        let schema = await storageEngine.getTableSchema(table)
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
        let schema = await storageEngine.getTableSchema(table)
        if let pkColumn = schema?.primaryKeyColumn {
            var seenPKs = Set<String>()
            for row in rows {
                if let pkValue = row.values[pkColumn.name], pkValue != .null {
                    let key = "\(pkValue)"
                    guard seenPKs.insert(key).inserted else {
                        throw PantryError.primaryKeyViolation
                    }
                    // Check existing data via index or scan
                    if let indexed = try await indexManager.attemptIndexLookup(
                        tableName: table,
                        condition: .equals(column: pkColumn.name, value: pkValue)
                    ), !indexed.isEmpty {
                        throw PantryError.primaryKeyViolation
                    } else if !(await indexManager.hasIndex(tableName: table, columnName: pkColumn.name)) {
                        let existing = try await executeSelect(from: table, columns: [pkColumn.name], where: .equals(column: pkColumn.name, value: pkValue), transactionContext: transactionContext)
                        if !existing.isEmpty {
                            throw PantryError.primaryKeyViolation
                        }
                    }
                } else if !pkColumn.isNullable {
                    throw PantryError.notNullConstraintViolation(column: pkColumn.name)
                }
            }
        }

        // Phase 1: Insert all records without index updates
        let bulkSchema = await storageEngine.getTableSchema(table)
        var insertedPairs: [(Record, Row)] = []
        insertedPairs.reserveCapacity(rows.count)

        for row in rows {
            let rowData = bulkSchema != nil ? row.toBytesPositional(schema: bulkSchema!) : row.toBytes()
            let recordID = generateRecordID()
            let record = Record(id: recordID, data: rowData)
            _ = try await storageEngine.insertRecordSkipIndex(record, tableName: table, transactionContext: transactionContext)
            insertedPairs.append((record, row))
        }

        // Phase 2: Batch index updates
        for (record, row) in insertedPairs {
            try await indexManager.updateIndexes(record: record, row: row, tableName: table)
        }
        invalidateResultCache(forTable: table)
    }

    // MARK: - UPDATE

    public func executeUpdate(table: String, set values: [String: DBValue], where condition: WhereCondition?, transactionContext: TransactionContext? = nil) async throws -> Int {
        // If updating PK column, validate uniqueness of new PK value
        if let schema = await storageEngine.getTableSchema(table),
           let pkColumn = schema.primaryKeyColumn,
           let newPKValue = values[pkColumn.name], newPKValue != .null {
            let existing = try await executeSelect(from: table, columns: [pkColumn.name], where: .equals(column: pkColumn.name, value: newPKValue), transactionContext: transactionContext)
            // Allow if the only match is the row being updated (checked below per-row)
            if existing.count > 1 {
                throw PantryError.primaryKeyViolation
            }
        }

        // Use cost-based planner to decide index vs scan
        let updateSchema = await storageEngine.getTableSchema(table)
        let pageIDs = try await storageEngine.getPageChain(tableName: table, transactionContext: transactionContext)
        let plan = planner.chooseAccessPlan(table: table, condition: condition, pageCount: pageIDs.count)

        if case .indexScan = plan, let condition = condition,
           let indexedRows = try await indexManager.attemptIndexLookup(tableName: table, condition: condition) {
            let matchingRIDs = Set(indexedRows.compactMap { row -> UInt64? in
                guard case .integer(let ridSigned) = row.values["__rid"] else { return nil }
                return UInt64(bitPattern: ridSigned)
            })
            let fullRecords = try await storageEngine.getRecordsByIDsWithPages(matchingRIDs, tableName: table, transactionContext: transactionContext)

            var updatedCount = 0
            for (record, row, recordPageID) in fullRecords where evaluateCondition(condition, row: row) {
                var updatedValues = row.values
                for (key, value) in values { updatedValues[key] = value }
                let updatedRow = Row(values: updatedValues)

                let rowData = updateSchema != nil ? updatedRow.toBytesPositional(schema: updateSchema!) : updatedRow.toBytes()
                let newRecord = Record(id: record.id, data: rowData)
                let replaced = try await storageEngine.replaceRecordInPlace(id: record.id, newRecord: newRecord, tableName: table, transactionContext: transactionContext, knownPageID: recordPageID)
                if !replaced {
                    try await storageEngine.deleteRecord(id: record.id, tableName: table, transactionContext: transactionContext, knownPageID: recordPageID)
                    try await storageEngine.insertRecord(newRecord, tableName: table, row: updatedRow, transactionContext: transactionContext)
                }
                updatedCount += 1
            }
            if updatedCount > 0 { invalidateResultCache(forTable: table) }
            return updatedCount
        }

        // Table scan path
        let rawRecords = try await storageEngine.scanTableRaw(table, transactionContext: transactionContext)
        var updatedCount = 0

        for (record, data) in rawRecords {
            guard let row = Row.fromBytesAuto(data, schema: updateSchema) else { continue }
            if condition == nil || evaluateCondition(condition!, row: row) {
                var updatedValues = row.values
                for (key, value) in values {
                    updatedValues[key] = value
                }
                let updatedRow = Row(values: updatedValues)

                let rowData = updateSchema != nil ? updatedRow.toBytesPositional(schema: updateSchema!) : updatedRow.toBytes()
                let newRecord = Record(id: record.id, data: rowData)
                let replaced = try await storageEngine.replaceRecordInPlace(id: record.id, newRecord: newRecord, tableName: table, transactionContext: transactionContext)
                if !replaced {
                    try await storageEngine.deleteRecord(id: record.id, tableName: table, transactionContext: transactionContext)
                    try await storageEngine.insertRecord(newRecord, tableName: table, row: updatedRow, transactionContext: transactionContext)
                }
                updatedCount += 1
            }
        }

        if updatedCount > 0 { invalidateResultCache(forTable: table) }
        return updatedCount
    }

    // MARK: - DELETE

    public func executeDelete(from table: String, where condition: WhereCondition?, transactionContext: TransactionContext? = nil) async throws -> Int {
        // Use cost-based planner to decide index vs scan
        let pageIDs = try await storageEngine.getPageChain(tableName: table, transactionContext: transactionContext)
        let plan = planner.chooseAccessPlan(table: table, condition: condition, pageCount: pageIDs.count)

        if case .indexScan = plan, let condition = condition,
           let indexedRows = try await indexManager.attemptIndexLookup(tableName: table, condition: condition) {
            let matchingRIDs = Set(indexedRows.compactMap { row -> UInt64? in
                guard case .integer(let ridSigned) = row.values["__rid"] else { return nil }
                return UInt64(bitPattern: ridSigned)
            })
            let fullRecords = try await storageEngine.getRecordsByIDsWithPages(matchingRIDs, tableName: table, transactionContext: transactionContext)

            var deletedCount = 0
            for (record, row, recordPageID) in fullRecords where evaluateCondition(condition, row: row) {
                try await storageEngine.deleteRecord(id: record.id, tableName: table, transactionContext: transactionContext, knownPageID: recordPageID)
                deletedCount += 1
            }
            if deletedCount > 0 { invalidateResultCache(forTable: table) }
            return deletedCount
        }

        // Table scan path
        let deleteSchema = await storageEngine.getTableSchema(table)
        let rawRecords = try await storageEngine.scanTableRaw(table, transactionContext: transactionContext)
        var deletedCount = 0

        for (record, data) in rawRecords {
            if condition == nil {
                try await storageEngine.deleteRecord(id: record.id, tableName: table, transactionContext: transactionContext)
                deletedCount += 1
            } else if let row = Row.fromBytesAuto(data, schema: deleteSchema), evaluateCondition(condition!, row: row) {
                try await storageEngine.deleteRecord(id: record.id, tableName: table, transactionContext: transactionContext)
                deletedCount += 1
            }
        }

        if deletedCount > 0 { invalidateResultCache(forTable: table) }
        return deletedCount
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
                if let index = await indexManager.getIndex(tableName: table, columnName: column) {
                    let firstRows = try await index.searchRangeWithLimit(from: nil, to: nil, limit: 1, ascending: true)
                    if let first = firstRows.first, let value = first.values[column], value != .null {
                        return value
                    }
                }
            case .max(let column):
                if let index = await indexManager.getIndex(tableName: table, columnName: column) {
                    let lastRows = try await index.searchRangeWithLimit(from: nil, to: nil, limit: 1, ascending: false)
                    if let last = lastRows.first, let value = last.values[column], value != .null {
                        return value
                    }
                }
            default:
                break
            }
        }

        let rows = try await executeSelect(from: table, where: condition, transactionContext: transactionContext)

        // For large datasets, partition and compute in parallel
        if rows.count > 1000 {
            return try await parallelAggregate(rows: rows, function: function)
        }

        return computeAggregate(rows: rows, function: function)
    }

    /// Serial aggregate computation
    private nonisolated func computeAggregate(rows: [Row], function: AggregateFunction) -> DBValue {
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
    private nonisolated func mergeAggregateResults(_ results: [DBValue], function: AggregateFunction, totalRowCount: Int) -> DBValue {
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
    private nonisolated func computePartialAVG(rows: [Row], column: String) -> DBValue {
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

    private nonisolated func numericValue(_ value: DBValue?) -> Double? {
        guard let value = value else { return nil }
        switch value {
        case .integer(let v): return Double(v)
        case .double(let v): return v
        default: return nil
        }
    }

    // MARK: - Condition Evaluation

    private nonisolated func evaluateCondition(_ condition: WhereCondition, row: Row) -> Bool {
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
            return conditions.allSatisfy { evaluateCondition($0, row: row) }
        case let .or(conditions):
            return conditions.contains { evaluateCondition($0, row: row) }
        }
    }

    // MARK: - Lazy Evaluation Helpers

    /// Extract all column names referenced in a WHERE condition.
    private nonisolated func columnsReferenced(in condition: WhereCondition) -> Set<String> {
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
    private nonisolated func decomposePredicates(
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
    private nonisolated func resolveColumnTable(
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
    private nonisolated func stripTablePrefix(_ condition: WhereCondition, table: String) -> WhereCondition {
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
    private nonisolated func queryConditionSignature(_ condition: WhereCondition) -> String {
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
    private nonisolated func dbValueSignature(_ val: DBValue) -> String {
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

    private nonisolated func queryModifiersSignature(_ mods: QueryModifiers) -> String {
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
    private nonisolated func evaluateConditionLazy(_ condition: WhereCondition, data: Data, columnIndexMap: [String: Int]? = nil) -> Bool? {
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
        case .isNull(let col):
            let val = lookupColumn(col)
            return val == nil || val == .null
        case .isNotNull(let col):
            guard let val = lookupColumn(col) else { return false }
            return val != .null
        case .and(let subs):
            for sub in subs {
                guard let result = evaluateConditionLazy(sub, data: data, columnIndexMap: columnIndexMap) else { return nil }
                if !result { return false }
            }
            return true
        case .or(let subs):
            for sub in subs {
                guard let result = evaluateConditionLazy(sub, data: data, columnIndexMap: columnIndexMap) else { return nil }
                if result { return true }
            }
            return false
        default:
            return nil // Fall back to full decode
        }
    }

    // MARK: - JOIN Helpers

    private func scanAllRows(table: String, filter: WhereCondition? = nil, transactionContext: TransactionContext? = nil) async throws -> [Row] {
        let pageIDs = try await storageEngine.getPageChain(tableName: table, transactionContext: transactionContext)
        let scanSchema = await storageEngine.getTableSchema(table)
        return try await withThrowingTaskGroup(of: (Int, [Row]).self) { group in
            for (index, pageID) in pageIDs.enumerated() {
                group.addTask { [storageEngine, filter, scanSchema] in
                    let page = try await storageEngine.getPage(pageID: pageID, transactionContext: transactionContext)
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

    private nonisolated func prefixRow(_ row: Row, table: String) -> [String: DBValue] {
        var values = [String: DBValue]()
        for (k, v) in row.values {
            values["\(table).\(k)"] = v
            values[k] = v
        }
        return values
    }

    /// Hash inner join: builds hash table on the smaller side (per planner strategy)
    private nonisolated func hashInnerJoin(left: [Row], right: [Row], join: JoinClause, strategy: JoinStrategy) -> [Row] {
        let buildOnRight: Bool
        if case .hashJoin(let side) = strategy { buildOnRight = (side == .right) } else { buildOnRight = true }

        if buildOnRight {
            // Build on right, probe with left
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
                    for (k, v) in rightRow.values { combined["\(join.table).\(k)"] = v; combined[k] = v }
                    result.append(Row(values: combined))
                }
            }
            return result
        } else {
            // Build on left, probe with right
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
                    for (k, v) in rightRow.values { combined["\(join.table).\(k)"] = v; combined[k] = v }
                    result.append(Row(values: combined))
                }
            }
            return result
        }
    }

    /// Hash left join: always probes with left, builds on right
    private nonisolated func hashLeftJoin(left: [Row], right: [Row], join: JoinClause, strategy: JoinStrategy) -> [Row] {
        var hashTable = [DBValue: [Row]]()
        var rightColumns = Set<String>()
        for row in right {
            let key = row.values[join.rightColumn] ?? .null
            if key != .null { hashTable[key, default: []].append(row) }
            rightColumns.formUnion(row.values.keys)
        }

        var result = [Row]()
        for leftRow in left {
            let leftKey = leftRow.values[join.leftColumn] ?? .null
            if leftKey != .null, let matches = hashTable[leftKey] {
                for rightRow in matches {
                    var combined = leftRow.values
                    for (k, v) in rightRow.values { combined["\(join.table).\(k)"] = v; combined[k] = v }
                    result.append(Row(values: combined))
                }
            } else {
                var combined = leftRow.values
                for col in rightColumns { combined["\(join.table).\(col)"] = .null }
                result.append(Row(values: combined))
            }
        }
        return result
    }

    /// Hash right join: builds on left, probes with right
    private nonisolated func hashRightJoin(left: [Row], right: [Row], join: JoinClause, strategy: JoinStrategy) -> [Row] {
        var hashTable = [DBValue: [Row]]()
        var leftColumns = Set<String>()
        for leftRow in left {
            let key = leftRow.values[join.leftColumn] ?? .null
            if key != .null { hashTable[key, default: []].append(leftRow) }
            leftColumns.formUnion(leftRow.values.keys)
        }

        var result = [Row]()
        for rightRow in right {
            var rightPrefixed = [String: DBValue]()
            for (k, v) in rightRow.values { rightPrefixed["\(join.table).\(k)"] = v; rightPrefixed[k] = v }

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

    private nonisolated func crossJoin(left: [Row], right: [Row], join: JoinClause) -> [Row] {
        var result = [Row]()
        result.reserveCapacity(left.count * right.count)
        for leftRow in left {
            for rightRow in right {
                var combined = leftRow.values
                for (k, v) in rightRow.values { combined["\(join.table).\(k)"] = v; combined[k] = v }
                result.append(Row(values: combined))
            }
        }
        return result
    }

    // MARK: - Index Coverage

    /// Build a map of column name → set of columns covered by that index
    private func buildIndexCoverage(table: String) async -> [String: Set<String>] {
        let indexes = await indexManager.getIndexes(tableName: table)
        var coverage = [String: Set<String>]()
        for (colName, idx) in indexes {
            let covered = await idx.coveredColumns
            coverage[colName] = covered
        }
        return coverage
    }

    // MARK: - Parallel Table Scan

    private func parallelTableScan(pageIDs: [Int], condition: WhereCondition?, limit: Int? = nil, neededColumns: Set<String>? = nil, schema: PantryTableSchema? = nil, transactionContext: TransactionContext?) async throws -> [Row] {
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
                            if result, let row = Row.fromBytesAuto(record.data, schema: schema) {
                                rows.append(row)
                                if rows.count >= limit { return rows }
                            }
                        } else if let row = Row.fromBytesAuto(record.data, schema: schema), evaluateCondition(condition, row: row) {
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
                        for pageID in chunkPageIDs {
                            let page = useConcurrent
                                ? try await storageEngine.getPageConcurrent(pageID: pageID)
                                : try await storageEngine.getPage(pageID: pageID, transactionContext: transactionContext)
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
                                    if result, let row = Row.fromBytesAuto(record.data, schema: schema) {
                                        chunkRows.append(row)
                                    }
                                } else if let row = Row.fromBytesAuto(record.data, schema: schema), self.evaluateCondition(condition, row: row) {
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
                        if let row = Row.fromBytesAuto(record.data, schema: schema) {
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
                        for pageID in chunkPageIDs {
                            let page = useConcurrent
                                ? try await storageEngine.getPageConcurrent(pageID: pageID)
                                : try await storageEngine.getPage(pageID: pageID, transactionContext: transactionContext)
                            for var record in page.records {
                                if record.isOverflow {
                                    if let needed = neededColumns,
                                       let partialRow = Row.fromBytesPartial(record.data, neededColumns: needed) {
                                        chunkRows.append(partialRow)
                                        continue
                                    }
                                    record = try await storageEngine.reassembleOverflowRecord(record)
                                }
                                if let row = Row.fromBytesAuto(record.data, schema: schema) {
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
    private nonisolated func topN(_ elements: [Row], n: Int, by areInIncreasingOrder: (Row, Row) -> Bool) -> [Row] {
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
    private nonisolated func siftDown(_ heap: inout [Row], _ index: Int, _ areInIncreasingOrder: (Row, Row) -> Bool) {
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

    private nonisolated func deduplicateRows(_ rows: [Row], columns: [String]?) -> [Row] {
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
    private nonisolated func stripRID(_ row: Row) -> Row {
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
    private nonisolated func matchLikePattern(_ string: String, pattern: String) -> Bool {
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
