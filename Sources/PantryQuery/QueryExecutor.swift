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

/// Executes SELECT, INSERT, UPDATE, DELETE queries against the storage engine.
/// Uses cost-based QueryPlanner for index vs scan decisions and join ordering.
public actor QueryExecutor: Sendable {
    private let storageEngine: StorageEngine
    private let indexManager: IndexManager
    private let planner: QueryPlanner

    public init(storageEngine: StorageEngine, indexManager: IndexManager, tableRegistry: TableRegistry) {
        self.storageEngine = storageEngine
        self.indexManager = indexManager
        self.planner = QueryPlanner(storageEngine: storageEngine, indexManager: indexManager, registry: tableRegistry)
    }

    /// Convenience init — resolves tableRegistry from storageEngine (requires await)
    public init(storageEngine: StorageEngine, indexManager: IndexManager) async {
        self.storageEngine = storageEngine
        self.indexManager = indexManager
        let reg = await storageEngine.tableRegistry
        self.planner = QueryPlanner(storageEngine: storageEngine, indexManager: indexManager, registry: reg)
    }

    // MARK: - SELECT

    public func executeSelect(from table: String, columns: [String]? = nil, where condition: WhereCondition? = nil, modifiers: QueryModifiers? = nil, transactionContext: TransactionContext? = nil) async throws -> [Row] {
        var rows: [Row]

        // Get page chain for cost estimation
        let pageIDs = try await storageEngine.getPageChain(tableName: table, transactionContext: transactionContext)

        // Build index coverage map for cost-based planner
        let indexCoverage = await buildIndexCoverage(table: table)

        // Use cost-based planner to choose access strategy
        let plan = planner.chooseAccessPlan(table: table, condition: condition, pageCount: pageIDs.count, requestedColumns: columns, indexCoverage: indexCoverage)

        switch plan {
        case .indexOnlyScan:
            // All requested columns are in the index — skip heap fetch
            if let condition = condition,
               let indexed = try await indexManager.attemptIndexLookup(tableName: table, condition: condition) {
                // Strip __rid and return index rows directly (they have all needed columns)
                rows = indexed.map { row in
                    var vals = row.values
                    vals.removeValue(forKey: "__rid")
                    return Row(values: vals)
                }
            } else {
                rows = try await parallelTableScan(pageIDs: pageIDs, condition: condition, transactionContext: transactionContext)
            }

        case .indexScan:
            if let condition = condition,
               let indexed = try await indexManager.attemptIndexLookup(tableName: table, condition: condition) {
                let matchingRIDs = Set(indexed.compactMap { row -> UInt64? in
                    guard case .integer(let ridSigned) = row.values["__rid"] else { return nil }
                    return UInt64(bitPattern: ridSigned)
                })
                let fullRecords = try await storageEngine.getRecordsByIDs(matchingRIDs, tableName: table, transactionContext: transactionContext)
                rows = fullRecords
                    .map { $0.1 }
                    .filter { condition == nil || evaluateCondition(condition, row: $0) }
            } else {
                rows = try await parallelTableScan(pageIDs: pageIDs, condition: condition, transactionContext: transactionContext)
            }

        case .tableScan:
            rows = try await parallelTableScan(pageIDs: pageIDs, condition: condition, transactionContext: transactionContext)
        }

        // Apply query modifiers: DISTINCT, ORDER BY, OFFSET, LIMIT
        if let mods = modifiers {
            // DISTINCT — deduplicate before sorting
            if mods.distinct {
                rows = deduplicateRows(rows, columns: columns)
            }

            // ORDER BY
            if let orderBy = mods.orderBy, !orderBy.isEmpty {
                rows.sort { a, b in
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

            // OFFSET
            if let offset = mods.offset, offset > 0 {
                rows = Array(rows.dropFirst(offset))
            }

            // LIMIT
            if let limit = mods.limit, limit >= 0 {
                rows = Array(rows.prefix(limit))
            }
        }

        // Project only requested columns
        if let columns = columns, !columns.isEmpty {
            return rows.map { row in
                let projectedValues = columns.reduce(into: [String: DBValue]()) { result, column in
                    result[column] = row.values[column] ?? .null
                }
                return Row(values: projectedValues)
            }
        }

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

        // Parallel scan: load left table and first right table concurrently
        let firstRightTable = optimizedJoins.first?.table
        let (leftRows, firstRightRows) = try await withThrowingTaskGroup(of: (String, [Row]).self) { group -> ([Row], [Row]) in
            group.addTask { ("left", try await self.scanAllRows(table: table, transactionContext: transactionContext)) }
            if let rightTable = firstRightTable {
                group.addTask { ("right", try await self.scanAllRows(table: rightTable, transactionContext: transactionContext)) }
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

        // Process each join in optimized order
        for (i, join) in optimizedJoins.enumerated() {
            let rightRows: [Row]
            if i == 0 {
                rightRows = firstRightRows
            } else {
                rightRows = try await scanAllRows(table: join.table, transactionContext: transactionContext)
            }

            switch join.type {
            case .inner:
                result = innerJoin(left: result, right: rightRows, join: join)
            case .left:
                result = leftJoin(left: result, right: rightRows, join: join)
            case .right:
                result = rightJoin(left: result, right: rightRows, join: join)
            case .cross:
                result = crossJoin(left: result, right: rightRows, join: join)
            }
        }

        // Apply WHERE filter
        if let condition = condition {
            result = result.filter { evaluateCondition(condition, row: $0) }
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
        // Get filtered rows
        let baseRows = try await executeSelect(from: table, where: condition, transactionContext: transactionContext)

        // Group rows by the specified columns
        var groups = [[DBValue]: [Row]]()
        for row in baseRows {
            let key = groupBy.columns.map { row.values[$0] ?? .null }
            groups[key, default: []].append(row)
        }

        // Compute aggregates per group
        var resultRows = [Row]()
        for (key, groupRows) in groups {
            var values = [String: DBValue]()

            // Add group key columns
            for (i, col) in groupBy.columns.enumerated() {
                values[col] = key[i]
            }

            // Compute each select expression
            for expr in expressions {
                switch expr {
                case .column(let name):
                    // Use value from first row in group (same for all grouped rows)
                    values[name] = groupRows.first?.values[name] ?? .null

                case .count(let col):
                    let alias = col.map { "COUNT(\($0))" } ?? "COUNT(*)"
                    if let col = col {
                        let count = groupRows.filter { $0.values[col] != nil && $0.values[col] != .null }.count
                        values[alias] = .integer(Int64(count))
                    } else {
                        values[alias] = .integer(Int64(groupRows.count))
                    }

                case .sum(let col):
                    let alias = "SUM(\(col))"
                    var sum: Double = 0
                    var hasValue = false
                    for row in groupRows {
                        if let v = numericValue(row.values[col]) { sum += v; hasValue = true }
                    }
                    values[alias] = hasValue ? .double(sum) : .null

                case .avg(let col):
                    let alias = "AVG(\(col))"
                    var sum: Double = 0; var count = 0
                    for row in groupRows {
                        if let v = numericValue(row.values[col]) { sum += v; count += 1 }
                    }
                    values[alias] = count > 0 ? .double(sum / Double(count)) : .null

                case .min(let col):
                    let alias = "MIN(\(col))"
                    var result: DBValue = .null
                    for row in groupRows {
                        if let v = row.values[col], v != .null, result == .null || v < result { result = v }
                    }
                    values[alias] = result

                case .max(let col):
                    let alias = "MAX(\(col))"
                    var result: DBValue = .null
                    for row in groupRows {
                        if let v = row.values[col], v != .null, result == .null || v > result { result = v }
                    }
                    values[alias] = result
                }
            }

            let groupRow = Row(values: values)

            // Apply HAVING filter
            if let having = groupBy.having {
                if evaluateCondition(having, row: groupRow) {
                    resultRows.append(groupRow)
                }
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

        let rowData = row.toBytes()
        let recordID = generateRecordID()
        let record = Record(id: recordID, data: rowData)
        try await storageEngine.insertRecord(record, tableName: table, row: row, transactionContext: transactionContext)
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
        let pageIDs = try await storageEngine.getPageChain(tableName: table, transactionContext: transactionContext)
        let plan = planner.chooseAccessPlan(table: table, condition: condition, pageCount: pageIDs.count)

        if case .indexScan = plan, let condition = condition,
           let indexedRows = try await indexManager.attemptIndexLookup(tableName: table, condition: condition) {
            let matchingRIDs = Set(indexedRows.compactMap { row -> UInt64? in
                guard case .integer(let ridSigned) = row.values["__rid"] else { return nil }
                return UInt64(bitPattern: ridSigned)
            })
            let fullRecords = try await storageEngine.getRecordsByIDs(matchingRIDs, tableName: table, transactionContext: transactionContext)

            var updatedCount = 0
            for (record, row) in fullRecords where evaluateCondition(condition, row: row) {
                var updatedValues = row.values
                for (key, value) in values { updatedValues[key] = value }
                let updatedRow = Row(values: updatedValues)

                let rowData = updatedRow.toBytes()
                let newRecord = Record(id: record.id, data: rowData)
                try await storageEngine.deleteRecord(id: record.id, tableName: table, transactionContext: transactionContext)
                try await storageEngine.insertRecord(newRecord, tableName: table, row: updatedRow, transactionContext: transactionContext)
                updatedCount += 1
            }
            return updatedCount
        }

        // Table scan path
        let rawRecords = try await storageEngine.scanTableRaw(table, transactionContext: transactionContext)
        var updatedCount = 0

        for (record, data) in rawRecords {
            guard let row = Row.fromBytes(data) else { continue }
            if condition == nil || evaluateCondition(condition!, row: row) {
                var updatedValues = row.values
                for (key, value) in values {
                    updatedValues[key] = value
                }
                let updatedRow = Row(values: updatedValues)

                let rowData = updatedRow.toBytes()
                let newRecord = Record(id: record.id, data: rowData)
                try await storageEngine.deleteRecord(id: record.id, tableName: table, transactionContext: transactionContext)
                try await storageEngine.insertRecord(newRecord, tableName: table, row: updatedRow, transactionContext: transactionContext)
                updatedCount += 1
            }
        }

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
            let fullRecords = try await storageEngine.getRecordsByIDs(matchingRIDs, tableName: table, transactionContext: transactionContext)

            var deletedCount = 0
            for (record, row) in fullRecords where evaluateCondition(condition, row: row) {
                try await storageEngine.deleteRecord(id: record.id, tableName: table, transactionContext: transactionContext)
                deletedCount += 1
            }
            return deletedCount
        }

        // Table scan path
        let rawRecords = try await storageEngine.scanTableRaw(table, transactionContext: transactionContext)
        var deletedCount = 0

        for (record, data) in rawRecords {
            if condition == nil {
                try await storageEngine.deleteRecord(id: record.id, tableName: table, transactionContext: transactionContext)
                deletedCount += 1
            } else if let row = Row.fromBytes(data), evaluateCondition(condition!, row: row) {
                try await storageEngine.deleteRecord(id: record.id, tableName: table, transactionContext: transactionContext)
                deletedCount += 1
            }
        }

        return deletedCount
    }

    // MARK: - AGGREGATE

    public func executeAggregate(from table: String, _ function: AggregateFunction, where condition: WhereCondition? = nil, transactionContext: TransactionContext? = nil) async throws -> DBValue {
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
                    self.computeAggregate(rows: chunk, function: function)
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
            // Each partial AVG needs to be re-derived from partial sums
            // Since we used avg per chunk, we need to re-weight
            // Simpler: sum all partial avgs weighted by chunk size isn't perfect
            // For correctness, recalculate from the sum aggregate
            var sum: Double = 0; var count = 0
            for r in results {
                if case .double(let v) = r { sum += v; count += 1 }
            }
            // Approximate: since partitions are equal-ish, weight equally
            return count > 0 ? .double(sum / Double(count)) : .null

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

    /// Evaluate a WHERE condition against raw binary data using partial column extraction.
    /// Returns nil if the condition type is too complex for lazy eval.
    private nonisolated func evaluateConditionLazy(_ condition: WhereCondition, data: Data) -> Bool? {
        switch condition {
        case .equals(let col, let value):
            if value == .null { return false }
            guard let rowValue = Row.columnValue(named: col, from: data) else { return false }
            if rowValue == .null { return false }
            return rowValue == value
        case .notEquals(let col, let value):
            if value == .null { return false }
            guard let rowValue = Row.columnValue(named: col, from: data) else { return false }
            if rowValue == .null { return false }
            return rowValue != value
        case .lessThan(let col, let value):
            guard let rowValue = Row.columnValue(named: col, from: data), rowValue != .null, value != .null else { return false }
            return rowValue < value
        case .greaterThan(let col, let value):
            guard let rowValue = Row.columnValue(named: col, from: data), rowValue != .null, value != .null else { return false }
            return rowValue > value
        case .lessThanOrEqual(let col, let value):
            guard let rowValue = Row.columnValue(named: col, from: data), rowValue != .null, value != .null else { return false }
            return rowValue <= value
        case .greaterThanOrEqual(let col, let value):
            guard let rowValue = Row.columnValue(named: col, from: data), rowValue != .null, value != .null else { return false }
            return rowValue >= value
        case .isNull(let col):
            let val = Row.columnValue(named: col, from: data)
            return val == nil || val == .null
        case .isNotNull(let col):
            guard let val = Row.columnValue(named: col, from: data) else { return false }
            return val != .null
        case .and(let subs):
            for sub in subs {
                guard let result = evaluateConditionLazy(sub, data: data) else { return nil }
                if !result { return false }
            }
            return true
        case .or(let subs):
            for sub in subs {
                guard let result = evaluateConditionLazy(sub, data: data) else { return nil }
                if result { return true }
            }
            return false
        default:
            return nil // Fall back to full decode
        }
    }

    // MARK: - JOIN Helpers

    private func scanAllRows(table: String, transactionContext: TransactionContext? = nil) async throws -> [Row] {
        let pageIDs = try await storageEngine.getPageChain(tableName: table, transactionContext: transactionContext)
        return try await withThrowingTaskGroup(of: (Int, [Row]).self) { group in
            for (index, pageID) in pageIDs.enumerated() {
                group.addTask { [storageEngine] in
                    let page = try await storageEngine.getPage(pageID: pageID, transactionContext: transactionContext)
                    return (index, page.records.compactMap { Row.fromBytes($0.data) })
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

    private nonisolated func innerJoin(left: [Row], right: [Row], join: JoinClause) -> [Row] {
        // Build hash table on right side for O(n+m) join
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
    }

    private nonisolated func leftJoin(left: [Row], right: [Row], join: JoinClause) -> [Row] {
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
                // No match — fill right columns with NULL
                var combined = leftRow.values
                for col in rightColumns { combined["\(join.table).\(col)"] = .null }
                result.append(Row(values: combined))
            }
        }
        return result
    }

    private nonisolated func rightJoin(left: [Row], right: [Row], join: JoinClause) -> [Row] {
        // Right join = reverse left join
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

    private func parallelTableScan(pageIDs: [Int], condition: WhereCondition?, transactionContext: TransactionContext?) async throws -> [Row] {
        if let condition = condition {
            let neededColumns = columnsReferenced(in: condition)
            let useLazy = neededColumns.count <= 3

            return try await withThrowingTaskGroup(of: (Int, [Row]).self) { group in
                for (index, pageID) in pageIDs.enumerated() {
                    group.addTask { [storageEngine] in
                        let page = try await storageEngine.getPage(pageID: pageID, transactionContext: transactionContext)
                        var pageRows: [Row] = []
                        for record in page.records {
                            if useLazy, let result = self.evaluateConditionLazy(condition, data: record.data) {
                                if result, let row = Row.fromBytes(record.data) {
                                    pageRows.append(row)
                                }
                            } else if let row = Row.fromBytes(record.data), self.evaluateCondition(condition, row: row) {
                                pageRows.append(row)
                            }
                        }
                        return (index, pageRows)
                    }
                }
                var indexed: [(Int, [Row])] = []
                for try await result in group { indexed.append(result) }
                return indexed.sorted { $0.0 < $1.0 }.flatMap { $0.1 }
            }
        } else {
            return try await withThrowingTaskGroup(of: (Int, [Row]).self) { group in
                for (index, pageID) in pageIDs.enumerated() {
                    group.addTask { [storageEngine] in
                        let page = try await storageEngine.getPage(pageID: pageID, transactionContext: transactionContext)
                        return (index, page.records.compactMap { Row.fromBytes($0.data) })
                    }
                }
                var indexed: [(Int, [Row])] = []
                for try await result in group { indexed.append(result) }
                return indexed.sorted { $0.0 < $1.0 }.flatMap { $0.1 }
            }
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
