import Foundation
import CobaltCore
import CobaltIndex
import CobaltQuery
import CobaltSQL

/// Main entry point for Cobalt — a modular embedded database.
public final class CobaltDatabase: @unchecked Sendable {
    private let storageEngine: StorageEngine
    private let queryExecutor: QueryExecutor
    private let indexManager: IndexManager
    private let configuration: CobaltConfiguration
    /// Mutable state protected by a lock (replaces actor isolation)
    private struct MutableState {
        var currentTransactionContext: TransactionContext?
        var sequenceCounters: [String: Int64] = [:]
        var views: [String: String] = [:]  // view name → SQL definition
    }
    private let mutableState = CobaltLock(MutableState())
    /// Trigger manager for procedural trigger support (already internally thread-safe)
    private let triggerManager = TriggerManager()
    /// Role manager for RBAC support (already internally thread-safe)
    private let roleManager = RoleManager()

    /// Internal metadata table names — hidden from user-facing queries
    private static let internalTableNames: Set<String> = [
        "_cobalt_sequences", "_cobalt_views", "_cobalt_triggers",
        "_cobalt_roles", "_cobalt_grants"
    ]

    /// Whether a table name is an internal metadata table
    private static func isInternalTable(_ name: String) -> Bool {
        internalTableNames.contains(name)
    }

    /// Default session parameters (for RESET)
    private static let defaultSessionParameters: [String: String] = [
        "server_version": "15.0",
        "server_encoding": "UTF8",
        "client_encoding": "UTF8",
        "DateStyle": "ISO, MDY",
        "integer_datetimes": "on",
        "standard_conforming_strings": "on",
        "search_path": "\"$user\", public",
        "TimeZone": "UTC",
        "bytea_output": "hex",
        "application_name": "",
        "is_superuser": "on",
        "session_authorization": "cobalt",
        "max_identifier_length": "63",
    ]

    /// Mutable session parameters, protected by sessionParamsLock
    private let sessionParamsLock = CobaltLock<[String: String]>(CobaltDatabase.defaultSessionParameters)

    /// Create or open a Cobalt database
    public init(configuration: CobaltConfiguration) async throws {
        self.configuration = configuration

        // Set up encryption if key provided
        let encryptionProvider: EncryptionProvider?
        if let key = configuration.encryptionKey {
            encryptionProvider = try AESGCMEncryptionProvider(key: key)
        } else {
            encryptionProvider = nil
        }

        // Initialize storage engine
        let engine = try await StorageEngine(
            databasePath: configuration.path,
            bufferPoolCapacity: configuration.bufferPoolCapacity,
            encryptionProvider: encryptionProvider,
            bufferPoolStripeCount: configuration.bufferPoolStripeCount,
            bgWriterIntervalMs: configuration.bgWriterIntervalMs
        )
        self.storageEngine = engine

        // Configure auto-checkpoint threshold and long-running TX timeout
        engine.setAutoCheckpointThreshold(configuration.autoCheckpointThreshold)
        await engine.transactionManager.setLongRunningTxTimeout(configuration.longRunningTxTimeoutSeconds)

        // Initialize index manager
        let im = IndexManager(
            bufferPool: engine.bufferPoolManager,
            storageManager: engine.storageManager
        )
        self.indexManager = im

        // Wire index hook into storage engine
        engine.setIndexHook(im)

        // Initialize query executor with table registry for cost-based planning
        self.queryExecutor = QueryExecutor(storageEngine: engine, indexManager: im, tableRegistry: engine.tableRegistry, costWeights: configuration.costWeights)

        // Restore persisted indexes
        if let indexData = try await engine.loadIndexRegistry() {
            let entries = try JSONDecoder().decode([IndexRegistryEntry].self, from: indexData)
            try await im.loadIndexRegistry(entries: entries)
            // Mark restored columns as indexed for the query planner
            for entry in entries {
                engine.markColumnIndexed(entry.tableName, column: entry.columnName)
                if let cols = entry.compoundColumns {
                    for col in cols { engine.markColumnIndexed(entry.tableName, column: col) }
                }
            }
        }

    }

    // MARK: - Triggers

    /// Register a trigger definition
    public func createTrigger(_ trigger: TriggerDef) async throws {
        triggerManager.registerTrigger(trigger)
    }

    /// Remove a trigger by name
    public func dropTrigger(name: String) async throws {
        triggerManager.removeTrigger(name: name)
    }

    /// Fire all matching triggers by executing their body SQL statements
    private func fireTriggers(table: String, timing: TriggerTiming, event: TriggerEvent) async throws {
        let triggers = triggerManager.getTriggersForTable(table, timing: timing, event: event)
        for trigger in triggers {
            for sql in trigger.body {
                _ = try await execute(sql: sql)
            }
        }
    }

    // MARK: - Tables

    public func createTable(_ schema: CobaltTableSchema) async throws {
        try await storageEngine.createTable(schema)

        // Auto-create index on primary key column for fast uniqueness checks
        if let pkColumn = schema.primaryKeyColumn {
            try await createIndex(table: schema.name, column: pkColumn.name)
        }
    }

    public func dropTable(_ name: String) async throws {
        try await storageEngine.dropTable(name)
        indexManager.removeIndexes(tableName: name)
    }

    public func tableExists(_ name: String) async -> Bool {
        storageEngine.tableExists(name)
    }

    public func listTables() async -> [String] {
        storageEngine.listTables().filter { !CobaltDatabase.isInternalTable($0) }
    }

    // MARK: - VACUUM

    /// Run VACUUM on a table to reclaim empty pages.
    public func vacuum(table: String) async throws -> VacuumResult {
        try await storageEngine.vacuum(table: table)
    }

    public func getTableSchema(_ name: String) async -> CobaltTableSchema? {
        storageEngine.getTableSchema(name)
    }

    internal func updateTableSchema(_ name: String, schema: CobaltTableSchema) async throws {
        try await storageEngine.updateTableSchema(name, schema: schema)
    }

    /// Create an index on a column, optionally with INCLUDE columns for covering index scans
    public func createIndex(table: String, column: String, include: [String]? = nil) async throws {
        let columnIndex = indexManager.createIndex(tableName: table, columnName: column, includeColumns: include)

        // Populate the index from existing data — extract only needed columns from raw data
        let schema = storageEngine.getTableSchema(table)
        let colIdx = schema?.columnOrdinals[column]
        let includeIdxs: [(String, Int)]? = include.flatMap { cols in
            guard let schema = schema else { return nil }
            return cols.compactMap { col in schema.columnOrdinals[col].map { (col, $0) } }
        }

        let rawRecords = try await storageEngine.scanTableRaw(table)
        var pairs: [(key: DBValue, row: Row)] = []
        pairs.reserveCapacity(rawRecords.count)
        for (record, data) in rawRecords {
            // Fast path: extract column directly from positional data (no full Row allocation)
            let value: DBValue?
            if let idx = colIdx {
                value = Row.extractColumnValue(from: data, columnIndex: idx)
                if value == nil || value == .null { continue }
            } else {
                // Fallback for non-positional data
                guard let row = Row.fromBytesAuto(data, schema: schema) else { continue }
                value = row.values[column]
                guard let value else { continue }
                let capacity = 2 + (include?.count ?? 0)
                var slimValues = Dictionary<String, DBValue>(minimumCapacity: capacity)
                slimValues["__rid"] = .integer(Int64(bitPattern: record.id))
                slimValues[column] = value
                if let include = include {
                    for incCol in include { slimValues[incCol] = row.values[incCol] ?? .null }
                }
                pairs.append((key: value, row: Row(values: slimValues)))
                continue
            }
            let capacity = 2 + (includeIdxs?.count ?? 0)
            var slimValues = Dictionary<String, DBValue>(minimumCapacity: capacity)
            slimValues["__rid"] = .integer(Int64(bitPattern: record.id))
            slimValues[column] = value!
            if let includeIdxs = includeIdxs {
                for (colName, idx) in includeIdxs {
                    slimValues[colName] = Row.extractColumnValue(from: data, columnIndex: idx) ?? .null
                }
            }
            pairs.append((key: value!, row: Row(values: slimValues)))
        }
        if !pairs.isEmpty {
            try await columnIndex.bulkLoad(pairs: pairs)
        }

        // Mark column as indexed for the query planner (lightweight, no full table scan)
        storageEngine.markColumnIndexed(table, column: column)
        queryExecutor.invalidatePlanCache(forTable: table)
    }

    /// Create multiple indexes on the same table with a single table scan.
    /// Much faster than calling createIndex multiple times.
    public func createIndexes(table: String, columns: [String], analyze: Bool = false) async throws {
        let schema = storageEngine.getTableSchema(table)
        // Create all ColumnIndex objects
        var columnIndexes: [(ColumnIndex, String, Int?)] = []
        for column in columns {
            let ci = indexManager.createIndex(tableName: table, columnName: column)
            let colIdx = schema?.columnOrdinals[column]
            columnIndexes.append((ci, column, colIdx))
        }

        // Single table scan — collect pairs for all indexes
        let rawRecords = try await storageEngine.scanTableRaw(table)

        // Build pairs for each index in parallel (independent column extraction)
        let colCount = columns.count
        let recCount = rawRecords.count
        var allPairs: [[(key: DBValue, row: Row)]] = Array(repeating: [], count: colCount)

        if colCount > 1 {
            // Parallel: each index gets its own task
            let results = await withTaskGroup(of: (Int, [(key: DBValue, row: Row)]).self) { group in
                for (i, (_, column, colIdx)) in columnIndexes.enumerated() {
                    let localSchema = schema
                    group.addTask {
                        var pairs: [(key: DBValue, row: Row)] = []
                        pairs.reserveCapacity(recCount)
                        for (record, data) in rawRecords {
                            let rid: DBValue = .integer(Int64(bitPattern: record.id))
                            let value: DBValue?
                            if let idx = colIdx {
                                value = Row.extractColumnValue(from: data, columnIndex: idx)
                                if value == nil || value == .null { continue }
                            } else {
                                guard let row = Row.fromBytesAuto(data, schema: localSchema) else { continue }
                                value = row.values[column]
                                if value == nil { continue }
                            }
                            var d = Dictionary<String, DBValue>(minimumCapacity: 2)
                            d["__rid"] = rid; d[column] = value!
                            pairs.append((key: value!, row: Row(values: d)))
                        }
                        return (i, pairs)
                    }
                }
                var result = [Int: [(key: DBValue, row: Row)]]()
                for await (i, pairs) in group { result[i] = pairs }
                return result
            }
            for i in 0..<colCount { allPairs[i] = results[i] ?? [] }
        } else {
            // Single index: no parallelization overhead
            allPairs[0].reserveCapacity(recCount)
            let (_, column, colIdx) = columnIndexes[0]
            for (record, data) in rawRecords {
                let rid: DBValue = .integer(Int64(bitPattern: record.id))
                let value: DBValue?
                if let idx = colIdx {
                    value = Row.extractColumnValue(from: data, columnIndex: idx)
                    if value == nil || value == .null { continue }
                } else {
                    guard let row = Row.fromBytesAuto(data, schema: schema) else { continue }
                    value = row.values[column]
                    if value == nil { continue }
                }
                var d = Dictionary<String, DBValue>(minimumCapacity: 2)
                d["__rid"] = rid; d[column] = value!
                allPairs[0].append((key: value!, row: Row(values: d)))
            }
        }

        // Bulk load all indexes and analyze concurrently
        try await withThrowingTaskGroup(of: Void.self) { group in
            for (i, (ci, _, _)) in columnIndexes.enumerated() {
                let pairs = allPairs[i]
                if !pairs.isEmpty {
                    group.addTask {
                        try await ci.bulkLoad(pairs: pairs)
                    }
                }
            }
            // Run analyze concurrently with bulkLoad (independent of index building)
            if analyze {
                let engine = storageEngine
                let tableName = table
                let records = rawRecords
                group.addTask {
                    try await engine.analyzeTableFromRaw(tableName, rawRecords: records)
                }
            }
            try await group.waitForAll()
        }

        for column in columns {
            storageEngine.markColumnIndexed(table, column: column)
        }
        queryExecutor.invalidatePlanCache(forTable: table)
    }

    /// Create a partial index on a column — only rows matching `where` condition are indexed.
    /// Partial indexes are smaller and faster when queries always include the same filter.
    public func createPartialIndex(table: String, column: String, where condition: WhereCondition, include: [String]? = nil) async throws {
        let columnIndex = indexManager.createIndex(tableName: table, columnName: column, includeColumns: include, partialCondition: condition)

        // Populate the index from existing data that matches the condition — bulk load
        let rows = try await storageEngine.scanTable(table)
        var pairs: [(key: DBValue, row: Row)] = []
        for (record, row) in rows {
            if !evaluateConditionForPartialIndex(condition, row: row) { continue }
            if let value = row.values[column] {
                var slimValues: [String: DBValue] = [
                    "__rid": .integer(Int64(bitPattern: record.id)),
                    column: value
                ]
                if let include = include {
                    for incCol in include { slimValues[incCol] = row.values[incCol] ?? .null }
                }
                pairs.append((key: value, row: Row(values: slimValues)))
            }
        }
        if !pairs.isEmpty {
            try await columnIndex.bulkLoad(pairs: pairs)
        }

        storageEngine.markColumnIndexed(table, column: column)
        queryExecutor.invalidatePlanCache(forTable: table)
    }

    public func createCompoundIndex(table: String, columns: [String]) async throws {
        let columnIndex = try indexManager.createCompoundIndex(tableName: table, columns: columns)

        // Populate the index from existing data — extract only needed columns from raw data
        let schema = storageEngine.getTableSchema(table)
        let colIdxs: [(String, Int)]? = schema.flatMap { s in
            columns.compactMap { col in s.columnOrdinals[col].map { (col, $0) } }
        }

        let rawRecords = try await storageEngine.scanTableRaw(table)
        var pairs: [(key: DBValue, row: Row)] = []
        pairs.reserveCapacity(rawRecords.count)
        for (record, data) in rawRecords {
            let rid: DBValue = .integer(Int64(bitPattern: record.id))
            var slimValues: [String: DBValue] = ["__rid": rid]
            var keyValues = [DBValue]()
            if let colIdxs = colIdxs {
                for (colName, idx) in colIdxs {
                    let v = Row.extractColumnValue(from: data, columnIndex: idx) ?? .null
                    slimValues[colName] = v
                    keyValues.append(v)
                }
            } else {
                // Fallback for non-positional data
                guard let row = Row.fromBytesAuto(data, schema: schema) else { continue }
                for col in columns {
                    let v = row.values[col] ?? .null
                    slimValues[col] = v
                    keyValues.append(v)
                }
            }
            let compoundKey = DBValue.compound(keyValues)
            pairs.append((key: compoundKey, row: Row(values: slimValues)))
        }
        if !pairs.isEmpty {
            try await columnIndex.bulkLoad(pairs: pairs)
        }
        for col in columns {
            storageEngine.markColumnIndexed(table, column: col)
        }
        queryExecutor.invalidatePlanCache(forTable: table)
    }

    public func listIndexes(on table: String) async -> [(column: String, isCompound: Bool)] {
        indexManager.listIndexes(tableName: table)
    }

    public func dropIndex(table: String, column: String) async {
        indexManager.dropIndex(tableName: table, columnName: column)
        storageEngine.markColumnNotIndexed(table, column: column)
        queryExecutor.invalidatePlanCache(forTable: table)
    }

    /// Collect column statistics for query optimization (like SQL ANALYZE)
    public func analyzeTable(_ table: String) async throws {
        try await storageEngine.analyzeTable(table)
    }

    // MARK: - Synchronous Point Lookup

    /// Non-transactional synchronous point lookup.
    /// Returns nil if the query cannot be served synchronously (caller should fall back to async select).
    /// Only handles equality lookups with LIMIT 1 on indexed columns via mmap.
    public func selectSync(
        from table: String,
        columns: [String]? = nil,
        where condition: WhereCondition,
        limit: Int = 1
    ) -> [Row]? {
        queryExecutor.executeSelectSync(from: table, columns: columns, where: condition, limit: limit)
    }

    // MARK: - Queries

    public func select(
        from table: String,
        columns: [String]? = nil,
        where condition: WhereCondition? = nil,
        orderBy: [OrderBy]? = nil,
        limit: Int? = nil,
        offset: Int? = nil,
        distinct: Bool = false
    ) async throws -> [Row] {
        let mods: QueryModifiers? = (orderBy != nil || limit != nil || offset != nil || distinct)
            ? QueryModifiers(orderBy: orderBy, limit: limit, offset: offset, distinct: distinct)
            : nil
        let txCtx = mutableState.withLock { $0.currentTransactionContext }
        return try await queryExecutor.executeSelect(from: table, columns: columns, where: condition, modifiers: mods, transactionContext: txCtx)
    }

    // MARK: - JOIN Queries

    public func select(
        from table: String,
        join joins: [JoinClause],
        columns: [String]? = nil,
        where condition: WhereCondition? = nil,
        orderBy: [OrderBy]? = nil,
        limit: Int? = nil,
        offset: Int? = nil,
        distinct: Bool = false
    ) async throws -> [Row] {
        let mods: QueryModifiers? = (orderBy != nil || limit != nil || offset != nil || distinct)
            ? QueryModifiers(orderBy: orderBy, limit: limit, offset: offset, distinct: distinct)
            : nil
        let txCtx = mutableState.withLock { $0.currentTransactionContext }
        return try await queryExecutor.executeJoin(from: table, joins: joins, columns: columns, where: condition, modifiers: mods, transactionContext: txCtx)
    }

    // MARK: - GROUP BY Queries

    public func select(
        from table: String,
        select expressions: [SelectExpression],
        where condition: WhereCondition? = nil,
        groupBy columns: [String],
        having: WhereCondition? = nil,
        orderBy: [OrderBy]? = nil,
        limit: Int? = nil,
        offset: Int? = nil
    ) async throws -> [Row] {
        let groupByClause = GroupByClause(columns: columns, having: having)
        let mods: QueryModifiers? = (orderBy != nil || limit != nil || offset != nil)
            ? QueryModifiers(orderBy: orderBy, limit: limit, offset: offset)
            : nil
        let txCtx = mutableState.withLock { $0.currentTransactionContext }
        return try await queryExecutor.executeGroupBy(from: table, select: expressions, where: condition, groupBy: groupByClause, modifiers: mods, transactionContext: txCtx)
    }

    // MARK: - Set Operations

    /// Combine two query results using UNION, UNION ALL, INTERSECT, or EXCEPT
    public func combine(_ operation: SetOperation, left: [Row], right: [Row]) async -> [Row] {
        queryExecutor.executeSetOperation(operation, left: left, right: right)
    }

    // MARK: - DML

    public func insert(into table: String, values: [String: DBValue]) async throws {
        let row = Row(values: values)
        let txCtx = mutableState.withLock { $0.currentTransactionContext }
        try await queryExecutor.executeInsert(into: table, row: row, transactionContext: txCtx)
    }

    public func update(table: String, set values: [String: DBValue], where condition: WhereCondition? = nil) async throws -> Int {
        let txCtx = mutableState.withLock { $0.currentTransactionContext }
        return try await queryExecutor.executeUpdate(table: table, set: values, where: condition, transactionContext: txCtx)
    }

    public func delete(from table: String, where condition: WhereCondition? = nil) async throws -> Int {
        let txCtx = mutableState.withLock { $0.currentTransactionContext }
        return try await queryExecutor.executeDelete(from: table, where: condition, transactionContext: txCtx)
    }

    public func aggregate(from table: String, _ function: AggregateFunction, where condition: WhereCondition? = nil) async throws -> DBValue {
        let txCtx = mutableState.withLock { $0.currentTransactionContext }
        return try await queryExecutor.executeAggregate(from: table, function, where: condition, transactionContext: txCtx)
    }

    public func count(from table: String, where condition: WhereCondition? = nil) async throws -> Int {
        let txCtx = mutableState.withLock { $0.currentTransactionContext }
        let result = try await queryExecutor.executeAggregate(from: table, .count(column: nil), where: condition, transactionContext: txCtx)
        if case .integer(let v) = result { return Int(v) }
        return 0
    }

    public func exists(in table: String, where condition: WhereCondition) async throws -> Bool {
        try await count(from: table, where: condition) > 0
    }

    public func insertAll(into table: String, rows: [[String: DBValue]]) async throws {
        let rowObjects = rows.map { Row(values: $0) }
        try await self.queryExecutor.executeBulkInsert(into: table, rows: rowObjects, transactionContext: nil)
    }

    /// Stream rows from a table, yielding one row at a time for memory-efficient processing.
    public func stream(from table: String) async throws -> AsyncStream<Row> {
        let rawStream = try await storageEngine.scanTableStream(table)
        return AsyncStream { continuation in
            Task {
                for await (_, row) in rawStream {
                    continuation.yield(row)
                }
                continuation.finish()
            }
        }
    }

    // MARK: - Transactions

    public func transaction<T: Sendable>(isolationLevel: IsolationLevel? = nil, body: @Sendable (CobaltDatabase) async throws -> T) async throws -> T {
        // Support nested transaction calls — if already in a transaction, just execute the body
        let alreadyInTx = mutableState.withLock { $0.currentTransactionContext != nil }
        if alreadyInTx {
            return try await body(self)
        }

        let txContext = try await storageEngine.beginTransaction(isolationLevel: isolationLevel ?? configuration.isolationLevel)
        mutableState.withLock { $0.currentTransactionContext = txContext }
        do {
            let result = try await body(self)
            mutableState.withLock { $0.currentTransactionContext = nil }
            try await storageEngine.commitTransaction(txContext)
            return result
        } catch {
            mutableState.withLock { $0.currentTransactionContext = nil }
            do {
                try await storageEngine.rollbackTransaction(txContext)
            } catch {
                // Rollback failure must not mask the original error
            }
            throw error
        }
    }

    // MARK: - SQL Execution

    /// Execute a raw SQL statement and return the result.
    public func execute(sql: String) async throws -> QueryResult {
        let stmt = try Parser.parse(sql)
        let lowering = ASTLowering()

        switch stmt {
        case .select(let selectStmt):
            return .rows(try await executeSelectStatement(selectStmt, lowering: lowering))

        case .insert(let insertStmt):
            // Fire BEFORE INSERT triggers
            try await fireTriggers(table: insertStmt.table, timing: .before, event: .insert)
            let colNames = insertStmt.columns
            // Pre-fetch schema for auto-increment handling
            guard let schema = await getTableSchema(insertStmt.table) else {
                throw CobaltError.tableNotFound(name: insertStmt.table)
            }
            // Build all row dicts up front, then use bulk insert path
            var allRows = [[String: DBValue]]()
            allRows.reserveCapacity(insertStmt.values.count)
            for valueRow in insertStmt.values {
                var dict = [String: DBValue]()
                for (i, expr) in valueRow.enumerated() {
                    let colName: String
                    if let names = colNames, i < names.count {
                        colName = names[i]
                    } else {
                        guard i < schema.columns.count else {
                            throw CobaltError.invalidQuery(description: "Too many values in INSERT")
                        }
                        colName = schema.columns[i].name
                    }
                    dict[colName] = try lowering.lowerToDBValue(expr)
                }
                // Auto-generate values for SERIAL/auto-increment columns not provided
                for col in schema.columns where col.isAutoIncrement {
                    if dict[col.name] == nil {
                        let key = "\(insertStmt.table).\(col.name)"
                        let next = mutableState.withLock { state -> Int64 in
                            let n = (state.sequenceCounters[key] ?? 0) + 1
                            state.sequenceCounters[key] = n
                            return n
                        }
                        dict[col.name] = .integer(next)
                    }
                }
                allRows.append(dict)
            }

            // Handle ON CONFLICT
            if let onConflict = insertStmt.onConflict {
                for rowDict in allRows {
                    do {
                        try await insert(into: insertStmt.table, values: rowDict)
                    } catch CobaltError.primaryKeyViolation {
                        switch onConflict.action {
                        case .doNothing:
                            continue
                        case .doUpdate(let assignments):
                            var setValues = [String: DBValue]()
                            for assignment in assignments {
                                setValues[assignment.column] = try lowering.lowerToDBValue(assignment.value)
                            }
                            guard let pkCol = schema.columns.first(where: { $0.isPrimaryKey }),
                                  let pkVal = rowDict[pkCol.name] else {
                                throw CobaltError.invalidQuery(description: "ON CONFLICT DO UPDATE requires a primary key column")
                            }
                            let cond = WhereCondition.equals(column: pkCol.name, value: pkVal)
                            // Let update errors propagate — do not swallow them
                            _ = try await update(table: insertStmt.table, set: setValues, where: cond)
                        }
                    } catch let error as CobaltError {
                        if case .uniqueConstraintViolation(_) = error {
                            switch onConflict.action {
                            case .doNothing:
                                continue
                            case .doUpdate(let assignments):
                                var setValues = [String: DBValue]()
                                for assignment in assignments {
                                    setValues[assignment.column] = try lowering.lowerToDBValue(assignment.value)
                                }
                                guard let pkCol = schema.columns.first(where: { $0.isPrimaryKey }),
                                      let pkVal = rowDict[pkCol.name] else {
                                    throw CobaltError.invalidQuery(description: "ON CONFLICT DO UPDATE requires a primary key column")
                                }
                                let cond = WhereCondition.equals(column: pkCol.name, value: pkVal)
                                // Let update errors propagate — do not swallow them
                                _ = try await update(table: insertStmt.table, set: setValues, where: cond)
                            }
                        } else {
                            throw error
                        }
                    }
                }
            } else {
                // Use bulk path for batched PK check, page-level batching, and deferred index updates
                try await insertAll(into: insertStmt.table, rows: allRows)
            }
            // Fire AFTER INSERT triggers
            try await fireTriggers(table: insertStmt.table, timing: .after, event: .insert)

            // Handle RETURNING
            if let returningItems = insertStmt.returning {
                let returningRows = allRows.map { Row(values: $0) }
                let filtered = filterReturningColumns(returningItems, rows: returningRows)
                return .rows(filtered)
            }
            return .rowCount(allRows.count)

        case .update(let updateStmt):
            // Fire BEFORE UPDATE triggers
            try await fireTriggers(table: updateStmt.table, timing: .before, event: .update)
            var setValues = [String: DBValue]()
            for (col, expr) in updateStmt.assignments {
                setValues[col] = try lowering.lowerToDBValue(expr)
            }
            let condition: WhereCondition? = try updateStmt.whereClause.map { try lowering.lowerWhereClause($0) }

            // For RETURNING: query rows, apply updates, return
            if let returningItems = updateStmt.returning {
                let preRows = try await select(from: updateStmt.table, where: condition)
                _ = try await update(table: updateStmt.table, set: setValues, where: condition)
                try await fireTriggers(table: updateStmt.table, timing: .after, event: .update)
                let updatedRows = preRows.map { row -> Row in
                    var vals = row.values
                    for (col, val) in setValues { vals[col] = val }
                    return Row(values: vals)
                }
                return .rows(filterReturningColumns(returningItems, rows: updatedRows))
            }

            let affected = try await update(table: updateStmt.table, set: setValues, where: condition)
            // Fire AFTER UPDATE triggers
            try await fireTriggers(table: updateStmt.table, timing: .after, event: .update)
            return .rowCount(affected)

        case .delete(let deleteStmt):
            // Fire BEFORE DELETE triggers
            try await fireTriggers(table: deleteStmt.table, timing: .before, event: .delete)
            let condition: WhereCondition? = try deleteStmt.whereClause.map { try lowering.lowerWhereClause($0) }

            // For RETURNING: select rows before deleting, wrapped in a transaction for atomicity
            if let returningItems = deleteStmt.returning {
                let preRows = try await transaction { db in
                    let rows = try await db.select(from: deleteStmt.table, where: condition)
                    _ = try await db.delete(from: deleteStmt.table, where: condition)
                    return rows
                }
                try await fireTriggers(table: deleteStmt.table, timing: .after, event: .delete)
                return .rows(filterReturningColumns(returningItems, rows: preRows))
            }

            let affected = try await delete(from: deleteStmt.table, where: condition)
            // Fire AFTER DELETE triggers
            try await fireTriggers(table: deleteStmt.table, timing: .after, event: .delete)
            return .rowCount(affected)

        case .compound(let compoundStmt):
            let leftRows = try await executeSelectStatement(compoundStmt.left, lowering: lowering)
            let rightRows = try await executeSelectStatement(compoundStmt.right, lowering: lowering)
            let op: SetOperation
            switch compoundStmt.operation {
            case .union: op = .union
            case .unionAll: op = .unionAll
            case .intersect: op = .intersect
            case .except: op = .except
            }
            var resultRows = await combine(op, left: leftRows, right: rightRows)

            // Apply ORDER BY if present
            if !compoundStmt.orderBy.isEmpty {
                let orderByLowered = try lowering.lowerOrderBy(compoundStmt.orderBy)
                resultRows = applyOrderBy(resultRows, orderBy: orderByLowered)
            }
            // Apply LIMIT/OFFSET if present
            if let limitExpr = compoundStmt.limit, case .integerLiteral(let lv) = limitExpr {
                let offsetVal: Int
                if let offsetExpr = compoundStmt.offset, case .integerLiteral(let ov) = offsetExpr {
                    offsetVal = Int(ov)
                } else {
                    offsetVal = 0
                }
                let start = Swift.min(offsetVal, resultRows.count)
                let end = Swift.min(start + Int(lv), resultRows.count)
                resultRows = Array(resultRows[start..<end])
            } else if let offsetExpr = compoundStmt.offset, case .integerLiteral(let ov) = offsetExpr {
                let start = Swift.min(Int(ov), resultRows.count)
                resultRows = Array(resultRows[start...])
            }
            return .rows(resultRows)

        case .createTable(let createStmt):
            if createStmt.ifNotExists {
                let exists = await tableExists(createStmt.name)
                if exists { return .ok }
            }
            let schema = try lowering.lowerCreateTable(createStmt)
            try await createTable(schema)
            return .ok

        case .dropTable(let dropStmt):
            if dropStmt.ifExists {
                let exists = await tableExists(dropStmt.name)
                if !exists { return .ok }
            }
            try await dropTable(dropStmt.name)
            return .ok

        case .alterTable(let alterStmt):
            switch alterStmt.action {
            case .addColumn(let colDef):
                let col = try lowering.lowerColumnDef(colDef)
                try await migrate(table: alterStmt.table, migrations: [
                    Migration(version: 1, operations: [.addColumn(col)])
                ])
            case .dropColumn(let colName):
                try await migrate(table: alterStmt.table, migrations: [
                    Migration(version: 1, operations: [.dropColumn(colName)])
                ])
            case .renameColumn(let from, let to):
                try await migrate(table: alterStmt.table, migrations: [
                    Migration(version: 1, operations: [.renameColumn(from: from, to: to)])
                ])
            }
            return .ok

        case .createIndex(let indexStmt):
            if indexStmt.columns.count == 1 {
                try await createIndex(table: indexStmt.table, column: indexStmt.columns[0])
            } else {
                try await createCompoundIndex(table: indexStmt.table, columns: indexStmt.columns)
            }
            return .ok

        case .dropIndex:
            // Index name-based drop not directly supported yet — return ok
            return .ok

        case .createTrigger(let triggerStmt):
            let timing: TriggerTiming
            switch triggerStmt.timing {
            case "BEFORE": timing = .before
            case "AFTER": timing = .after
            default: throw CobaltError.invalidQuery(description: "Invalid trigger timing: \(triggerStmt.timing)")
            }
            let event: TriggerEvent
            switch triggerStmt.event {
            case "INSERT": event = .insert
            case "UPDATE": event = .update
            case "DELETE": event = .delete
            default: throw CobaltError.invalidQuery(description: "Invalid trigger event: \(triggerStmt.event)")
            }
            let forEach: TriggerForEach
            switch triggerStmt.forEach {
            case "ROW": forEach = .row
            case "STATEMENT": forEach = .statement
            default: throw CobaltError.invalidQuery(description: "Invalid trigger forEach: \(triggerStmt.forEach)")
            }
            let def = TriggerDef(
                name: triggerStmt.name,
                table: triggerStmt.table,
                timing: timing,
                event: event,
                forEach: forEach,
                body: triggerStmt.body
            )
            try await createTrigger(def)
            return .ok

        case .dropTrigger(let dropStmt):
            try await dropTrigger(name: dropStmt.name)
            return .ok

        case .begin:
            // Atomically check whether we're already in a transaction and set a sentinel
            // to prevent a concurrent BEGIN from also passing the check.
            let shouldBegin = mutableState.withLock { state -> Bool in
                if state.currentTransactionContext != nil { return false }
                // Set a sentinel context to block concurrent BEGIN calls.
                // We use a special marker that will be replaced by the real context below.
                state.currentTransactionContext = TransactionContext.sentinel
                return true
            }
            if shouldBegin {
                do {
                    let txContext = try await storageEngine.beginTransaction(isolationLevel: configuration.isolationLevel)
                    mutableState.withLock { $0.currentTransactionContext = txContext }
                } catch {
                    // Clear the sentinel on failure so the database isn't stuck
                    mutableState.withLock { $0.currentTransactionContext = nil }
                    throw error
                }
            }
            return .ok

        case .commit:
            if let txContext = mutableState.withLock({ $0.currentTransactionContext }) {
                mutableState.withLock { $0.currentTransactionContext = nil }
                try await storageEngine.commitTransaction(txContext)
            }
            return .ok

        case .rollback:
            if let txContext = mutableState.withLock({ $0.currentTransactionContext }) {
                mutableState.withLock { $0.currentTransactionContext = nil }
                do {
                    try await storageEngine.rollbackTransaction(txContext)
                } catch {
                    // Rollback failure is non-fatal
                }
            }
            return .ok

        case .explain(let explainStmt):
            let innerStmt = explainStmt.statement
            switch innerStmt {
            case .select(let sel):
                let tableName = sel.from?.tableName ?? "unknown"
                let condition: WhereCondition? = try sel.whereClause.map { try lowering.lowerWhereClause($0) }
                let hasIndex: Bool
                if let cond = condition {
                    hasIndex = checkHasIndex(table: tableName, condition: cond)
                } else {
                    hasIndex = false
                }
                let joinCount = sel.joins.count
                let planRows = ExplainExecutor.explain(table: tableName, condition: condition, hasIndex: hasIndex, joinCount: joinCount)
                return .rows(planRows)
            default:
                return .rows([Row(values: ["plan": .string("EXECUTE")])])
            }

        case .vacuum(let vacuumStmt):
            guard let tableName = vacuumStmt.table else {
                // VACUUM without a table name — return ok with no-op
                return .rows([Row(values: [
                    "pages_scanned": .integer(0),
                    "pages_reclaimed": .integer(0),
                    "dead_tuples_removed": .integer(0)
                ])])
            }
            let result = try await vacuum(table: tableName)
            return .rows([Row(values: [
                "pages_scanned": .integer(Int64(result.pagesScanned)),
                "pages_reclaimed": .integer(Int64(result.pagesReclaimed)),
                "dead_tuples_removed": .integer(Int64(result.deadTuplesRemoved))
            ])])

        case .set(let setStmt):
            sessionParamsLock.withLock { params in
                params[setStmt.name] = setStmt.value
            }
            return .ok

        case .show(let showStmt):
            if showStmt.name.uppercased() == "ALL" {
                let params = sessionParamsLock.withLock { $0 }
                let rows = params.sorted(by: { $0.key < $1.key }).map { (key, value) in
                    Row(values: [
                        "name": .string(key),
                        "setting": .string(value),
                        "description": .string("")
                    ])
                }
                return .rows(rows)
            } else {
                let value = sessionParamsLock.withLock { params -> String in
                    // Case-insensitive lookup
                    for (key, val) in params {
                        if key.lowercased() == showStmt.name.lowercased() {
                            return val
                        }
                    }
                    return ""
                }
                return .rows([Row(values: [showStmt.name: .string(value)])])
            }

        case .reset(let resetStmt):
            if resetStmt.name.uppercased() == "ALL" {
                sessionParamsLock.withLock { params in
                    params = CobaltDatabase.defaultSessionParameters
                }
            } else {
                sessionParamsLock.withLock { params in
                    for (key, value) in CobaltDatabase.defaultSessionParameters {
                        if key.lowercased() == resetStmt.name.lowercased() {
                            params[key] = value
                            break
                        }
                    }
                }
            }
            return .ok

        case .discard:
            sessionParamsLock.withLock { params in
                params = CobaltDatabase.defaultSessionParameters
            }
            return .ok

        case .createView(let viewStmt):
            // Build the SQL text from the original query by re-serializing
            // For simplicity, store the raw SELECT SQL that was parsed
            let viewSQL = sql.trimmingCharacters(in: .whitespacesAndNewlines)
            // Extract the SELECT portion after AS
            let upperSQL = viewSQL.uppercased()
            guard let asRange = upperSQL.range(of: " AS ") else {
                throw CobaltError.invalidQuery(description: "CREATE VIEW must contain AS SELECT")
            }
            let selectSQL = String(viewSQL[asRange.upperBound...]).trimmingCharacters(in: .whitespacesAndNewlines)

            let existing = mutableState.withLock { $0.views[viewStmt.name.lowercased()] }
            if existing != nil && !viewStmt.orReplace {
                throw CobaltError.tableAlreadyExists(name: viewStmt.name)
            }
            mutableState.withLock { $0.views[viewStmt.name.lowercased()] = selectSQL }
            return .ok

        case .dropView(let dropStmt):
            let exists = mutableState.withLock { $0.views[dropStmt.name.lowercased()] != nil }
            if !exists && !dropStmt.ifExists {
                throw CobaltError.tableNotFound(name: dropStmt.name)
            }
            mutableState.withLock { $0.views.removeValue(forKey: dropStmt.name.lowercased()) }
            return .ok
        }
    }

    // MARK: - System Catalog Virtual Tables

    /// Handle SELECT queries against PostgreSQL system catalogs and information_schema.
    /// Returns nil if this is not a catalog query and should be handled normally.
    private func handleSystemCatalog(tableName: String, stmt: SelectStatement) async throws -> [Row]? {
        let lower = tableName.lowercased()

        // Handle pg_catalog.X notation: the parser sees "pg_catalog" as the table name
        // and ".pg_type" as a qualified reference. But since we use fromRef.tableName,
        // we only get the first part. We need to check if the table alias/name matches patterns.

        switch lower {
        case "pg_type", "pg_catalog.pg_type":
            return pgTypeRows()
        case "pg_database", "pg_catalog.pg_database":
            return [Row(values: [
                "oid": .integer(1),
                "datname": .string("cobalt"),
                "datdba": .integer(10),
                "encoding": .integer(6),
                "datcollate": .string("en_US.UTF-8"),
                "datctype": .string("en_US.UTF-8"),
            ])]
        case "pg_namespace", "pg_catalog.pg_namespace":
            return [
                Row(values: ["oid": .integer(2200), "nspname": .string("public"), "nspowner": .integer(10)]),
                Row(values: ["oid": .integer(11), "nspname": .string("pg_catalog"), "nspowner": .integer(10)]),
            ]
        case "pg_class", "pg_catalog.pg_class":
            let tables = await listTables()  // already filters internal tables
            return tables.enumerated().map { (i, name) in
                Row(values: [
                    "oid": .integer(Int64(16384 + i)),
                    "relname": .string(name),
                    "relnamespace": .integer(2200),
                    "relkind": .string("r"),
                ])
            }
        case "pg_settings", "pg_catalog.pg_settings":
            let params = sessionParamsLock.withLock { $0 }
            return params.sorted(by: { $0.key < $1.key }).map { (key, value) in
                Row(values: [
                    "name": .string(key),
                    "setting": .string(value),
                    "unit": .string(""),
                    "category": .string(""),
                    "short_desc": .string(""),
                    "context": .string("user"),
                    "vartype": .string("string"),
                    "source": .string("default"),
                ])
            }
        case "pg_am", "pg_catalog.pg_am":
            return [
                Row(values: ["oid": .integer(403), "amname": .string("btree"), "amhandler": .string("bthandler"), "amtype": .string("i")]),
                Row(values: ["oid": .integer(405), "amname": .string("hash"), "amhandler": .string("hashhandler"), "amtype": .string("i")]),
            ]
        case "pg_attribute", "pg_catalog.pg_attribute":
            return [] // Minimal stub
        case "pg_attrdef", "pg_catalog.pg_attrdef":
            return []
        case "pg_constraint", "pg_catalog.pg_constraint":
            return []
        case "pg_index", "pg_catalog.pg_index":
            return []
        case "information_schema":
            // This would be hit if someone does SELECT * FROM information_schema
            return []
        case "information_schema.tables":
            let tables = await listTables()
            return tables.map { name in
                Row(values: [
                    "table_catalog": .string("cobalt"),
                    "table_schema": .string("public"),
                    "table_name": .string(name),
                    "table_type": .string("BASE TABLE"),
                ])
            }
        case "information_schema.columns":
            var rows: [Row] = []
            let tables = await listTables()
            for table in tables {
                if let schema = await getTableSchema(table) {
                    for (i, col) in schema.columns.enumerated() {
                        rows.append(Row(values: [
                            "table_catalog": .string("cobalt"),
                            "table_schema": .string("public"),
                            "table_name": .string(table),
                            "column_name": .string(col.name),
                            "ordinal_position": .integer(Int64(i + 1)),
                            "data_type": .string(col.type.rawValue),
                            "is_nullable": .string(col.isNullable ? "YES" : "NO"),
                        ]))
                    }
                }
            }
            return rows
        case "information_schema.schemata":
            return [Row(values: [
                "catalog_name": .string("cobalt"),
                "schema_name": .string("public"),
                "schema_owner": .string("cobalt"),
            ])]
        default:
            // Check for information_schema.tables pattern:
            // Due to how the parser works, "information_schema" is the table name
            // and "tables" would be after a dot. But TableRef only stores the base name.
            // We handle the combined name in case it was somehow combined.
            if lower.hasPrefix("pg_") || lower.hasPrefix("pg_catalog.") {
                return [] // Unknown pg_ table — return empty
            }
            return nil
        }
    }

    /// Synthetic pg_type rows for our supported types
    private func pgTypeRows() -> [Row] {
        let types: [(oid: Int64, name: String, len: Int64, category: String)] = [
            (16, "bool", 1, "B"),
            (20, "int8", 8, "N"),
            (23, "int4", 4, "N"),
            (25, "text", -1, "S"),
            (17, "bytea", -1, "U"),
            (701, "float8", 8, "N"),
            (700, "float4", 4, "N"),
            (1043, "varchar", -1, "S"),
            (1114, "timestamp", 8, "D"),
            (1184, "timestamptz", 8, "D"),
            (2950, "uuid", 16, "U"),
        ]
        return types.map { t in
            Row(values: [
                "oid": .integer(t.oid),
                "typname": .string(t.name),
                "typlen": .integer(t.len),
                "typtype": .string("b"),
                "typcategory": .string(t.category),
                "typnamespace": .integer(11),
            ])
        }
    }

    /// Get a session parameter value (for use by ConnectionHandler)
    public func getSessionParameter(_ name: String) -> String? {
        sessionParamsLock.withLock { params in
            for (key, val) in params {
                if key.lowercased() == name.lowercased() {
                    return val
                }
            }
            return nil
        }
    }

    // MARK: - Expression Evaluation

    /// Evaluate an AST expression against an optional row context.
    private func evaluateExpression(_ expr: CobaltSQL.Expression, row: Row?, lowering: ASTLowering) throws -> DBValue {
        switch expr {
        case .integerLiteral(let v): return .integer(v)
        case .doubleLiteral(let v): return .double(v)
        case .stringLiteral(let v): return .string(v)
        case .booleanLiteral(let v): return .boolean(v)
        case .nullLiteral: return .null
        case .column(_, let name):
            guard let r = row, let val = r.values[name] else { return .null }
            return val
        case .function(let name, let args):
            let evaluatedArgs = try args.map { try evaluateExpression($0, row: row, lowering: lowering) }
            return try BuiltinFunctions.evaluate(name: name, args: evaluatedArgs)
        case .cast(let inner, let dataType):
            let val = try evaluateExpression(inner, row: row, lowering: lowering)
            return try BuiltinFunctions.cast(val, to: dataType)
        case .binaryOp(let left, let op, let right):
            let l = try evaluateExpression(left, row: row, lowering: lowering)
            let r = try evaluateExpression(right, row: row, lowering: lowering)
            return evaluateBinaryOp(l, op, r)
        case .unaryOp(let op, let operand):
            let val = try evaluateExpression(operand, row: row, lowering: lowering)
            return evaluateUnaryOp(op, val)
        default:
            return try lowering.lowerToDBValue(expr)
        }
    }

    private func evaluateBinaryOp(_ left: DBValue, _ op: BinaryOperator, _ right: DBValue) -> DBValue {
        switch op {
        case .add:
            switch (left, right) {
            case (.integer(let a), .integer(let b)): return .integer(a &+ b)
            case (.double(let a), .double(let b)): return .double(a + b)
            case (.integer(let a), .double(let b)): return .double(Double(a) + b)
            case (.double(let a), .integer(let b)): return .double(a + Double(b))
            default: return .null
            }
        case .subtract:
            switch (left, right) {
            case (.integer(let a), .integer(let b)): return .integer(a &- b)
            case (.double(let a), .double(let b)): return .double(a - b)
            case (.integer(let a), .double(let b)): return .double(Double(a) - b)
            case (.double(let a), .integer(let b)): return .double(a - Double(b))
            default: return .null
            }
        case .multiply:
            switch (left, right) {
            case (.integer(let a), .integer(let b)): return .integer(a &* b)
            case (.double(let a), .double(let b)): return .double(a * b)
            case (.integer(let a), .double(let b)): return .double(Double(a) * b)
            case (.double(let a), .integer(let b)): return .double(a * Double(b))
            default: return .null
            }
        case .divide:
            switch (left, right) {
            case (.integer(let a), .integer(let b)):
                guard b != 0 else { return .null }
                return .integer(a / b)
            case (.double(let a), .double(let b)):
                guard b != 0 else { return .null }
                return .double(a / b)
            case (.integer(let a), .double(let b)):
                guard b != 0 else { return .null }
                return .double(Double(a) / b)
            case (.double(let a), .integer(let b)):
                guard b != 0 else { return .null }
                return .double(a / Double(b))
            default: return .null
            }
        case .modulo:
            switch (left, right) {
            case (.integer(let a), .integer(let b)):
                guard b != 0 else { return .null }
                return .integer(a % b)
            default: return .null
            }
        case .concat:
            switch (left, right) {
            case (.string(let a), .string(let b)): return .string(a + b)
            case (.null, _), (_, .null): return .null
            default: return .null
            }
        case .equal: return .boolean(left == right)
        case .notEqual: return .boolean(left != right)
        case .lessThan: return .boolean(left < right)
        case .greaterThan: return .boolean(left > right)
        case .lessOrEqual: return .boolean(left <= right)
        case .greaterOrEqual: return .boolean(left >= right)
        case .and:
            if case .boolean(let a) = left, case .boolean(let b) = right { return .boolean(a && b) }
            return .null
        case .or:
            if case .boolean(let a) = left, case .boolean(let b) = right { return .boolean(a || b) }
            return .null
        }
    }

    private func evaluateUnaryOp(_ op: UnaryOperator, _ val: DBValue) -> DBValue {
        switch op {
        case .negate:
            switch val {
            case .integer(let v): return .integer(-v)
            case .double(let v): return .double(-v)
            default: return .null
            }
        case .not:
            if case .boolean(let v) = val { return .boolean(!v) }
            return .null
        }
    }

    /// Evaluate a table-less SELECT (no FROM clause).
    private func evaluateTablelessSelect(_ stmt: SelectStatement, lowering: ASTLowering) throws -> Row {
        var values = [String: DBValue]()
        for (i, item) in stmt.columns.enumerated() {
            if case .expression(let expr, let alias) = item {
                let noRow: Row? = nil
                let val = try evaluateExpression(expr, row: noRow, lowering: lowering)
                let key = alias ?? columnLabel(for: expr, index: i)
                values[key] = val
            }
        }
        return Row(values: values)
    }

    /// Generate a column label for an expression when no alias is given.
    private func columnLabel(for expr: CobaltSQL.Expression, index: Int) -> String {
        switch expr {
        case .function(let name, _): return name
        case .cast: return "cast"
        case .column(_, let name): return name
        case .integerLiteral(let v): return String(v)
        case .doubleLiteral(let v): return String(v)
        case .stringLiteral(let v): return v
        default: return "col\(index)"
        }
    }

    /// Check if a SELECT has function/cast expressions that need post-processing.
    private func selectNeedsExpressionEval(_ items: [SelectItem]) -> Bool {
        for item in items {
            if case .expression(let expr, _) = item {
                switch expr {
                case .function, .cast, .binaryOp, .unaryOp: return true
                default: continue
                }
            }
        }
        return false
    }

    /// Post-process rows to evaluate function/cast expressions in SELECT list.
    private func postProcessSelectExpressions(_ rows: [Row], stmt: SelectStatement, lowering: ASTLowering) throws -> [Row] {
        try rows.map { row in
            var values = [String: DBValue]()
            for (i, item) in stmt.columns.enumerated() {
                switch item {
                case .allColumns:
                    for (k, v) in row.values { values[k] = v }
                case .tableAllColumns:
                    for (k, v) in row.values { values[k] = v }
                case .expression(let expr, let alias):
                    switch expr {
                    case .function, .cast, .binaryOp, .unaryOp:
                        let val = try evaluateExpression(expr, row: row, lowering: lowering)
                        let key = alias ?? columnLabel(for: expr, index: i)
                        values[key] = val
                    case .column(_, let name):
                        let key = alias ?? name
                        values[key] = row.values[name] ?? .null
                    default:
                        let key = alias ?? columnLabel(for: expr, index: i)
                        values[key] = try evaluateExpression(expr, row: row, lowering: lowering)
                    }
                }
            }
            return Row(values: values)
        }
    }

    // MARK: - Alias & Table Qualifier Resolution

    /// Build a mapping from column alias to the underlying column name from the SELECT list.
    private func buildAliasMap(_ columns: [SelectItem]) -> [String: String] {
        var map = [String: String]()
        for item in columns {
            if case .expression(let expr, let alias) = item, let alias = alias {
                if case .column(_, let name) = expr {
                    map[alias] = name
                }
            }
        }
        return map
    }

    /// Build a set of table alias names from FROM and JOIN clauses.
    private func buildTableAliasMap(_ from: TableRef?, joins: [JoinItem]) -> [String: String] {
        var map = [String: String]()
        if let from = from {
            switch from {
            case .table(let name, let alias):
                if let alias = alias { map[alias] = name }
            case .subquery(_, let alias):
                map[alias] = alias
            }
        }
        for join in joins {
            switch join.table {
            case .table(let name, let alias):
                if let alias = alias { map[alias] = name }
            case .subquery(_, let alias):
                map[alias] = alias
            }
        }
        return map
    }

    /// Rewrite an expression, replacing column alias references and stripping table qualifiers
    /// that match known table aliases.
    private func rewriteExpression(_ expr: CobaltSQL.Expression, aliasMap: [String: String], tableAliases: [String: String]) -> CobaltSQL.Expression {
        switch expr {
        case .column(let table, let name):
            // If table qualifier matches a known table alias, strip it
            if let table = table, tableAliases[table] != nil {
                return .column(table: nil, name: name)
            }
            // If no table qualifier and name matches a column alias, replace
            if table == nil, let realName = aliasMap[name] {
                return .column(table: nil, name: realName)
            }
            return expr
        case .binaryOp(let left, let op, let right):
            return .binaryOp(
                left: rewriteExpression(left, aliasMap: aliasMap, tableAliases: tableAliases),
                op: op,
                right: rewriteExpression(right, aliasMap: aliasMap, tableAliases: tableAliases)
            )
        case .unaryOp(let op, let operand):
            return .unaryOp(op: op, operand: rewriteExpression(operand, aliasMap: aliasMap, tableAliases: tableAliases))
        case .function(let name, let args):
            return .function(name: name, args: args.map { rewriteExpression($0, aliasMap: aliasMap, tableAliases: tableAliases) })
        case .isNull(let inner):
            return .isNull(rewriteExpression(inner, aliasMap: aliasMap, tableAliases: tableAliases))
        case .isNotNull(let inner):
            return .isNotNull(rewriteExpression(inner, aliasMap: aliasMap, tableAliases: tableAliases))
        case .between(let inner, let low, let high):
            return .between(
                rewriteExpression(inner, aliasMap: aliasMap, tableAliases: tableAliases),
                low: rewriteExpression(low, aliasMap: aliasMap, tableAliases: tableAliases),
                high: rewriteExpression(high, aliasMap: aliasMap, tableAliases: tableAliases)
            )
        case .like(let inner, let pattern):
            return .like(
                rewriteExpression(inner, aliasMap: aliasMap, tableAliases: tableAliases),
                pattern: rewriteExpression(pattern, aliasMap: aliasMap, tableAliases: tableAliases)
            )
        case .inList(let inner, let values):
            return .inList(
                rewriteExpression(inner, aliasMap: aliasMap, tableAliases: tableAliases),
                values.map { rewriteExpression($0, aliasMap: aliasMap, tableAliases: tableAliases) }
            )
        case .cast(let inner, let dataType):
            return .cast(rewriteExpression(inner, aliasMap: aliasMap, tableAliases: tableAliases), dataType)
        default:
            return expr
        }
    }

    /// Rewrite a SELECT statement to resolve column aliases in WHERE/ORDER BY
    /// and strip table alias qualifiers.
    private func resolveAliases(_ stmt: SelectStatement) -> SelectStatement {
        let aliasMap = buildAliasMap(stmt.columns)
        let tableAliases = buildTableAliasMap(stmt.from, joins: stmt.joins)

        // Nothing to rewrite if no aliases
        guard !aliasMap.isEmpty || !tableAliases.isEmpty else { return stmt }

        var result = stmt

        // Rewrite WHERE clause
        if let whereClause = stmt.whereClause {
            result.whereClause = rewriteExpression(whereClause, aliasMap: aliasMap, tableAliases: tableAliases)
        }

        // Rewrite ORDER BY
        result.orderBy = stmt.orderBy.map { item in
            OrderByItem(
                expression: rewriteExpression(item.expression, aliasMap: aliasMap, tableAliases: tableAliases),
                ascending: item.ascending
            )
        }

        // Rewrite column references in SELECT list to strip table aliases
        if !tableAliases.isEmpty {
            result.columns = stmt.columns.map { item in
                switch item {
                case .expression(let expr, let alias):
                    return .expression(rewriteExpression(expr, aliasMap: [:], tableAliases: tableAliases), alias: alias)
                case .tableAllColumns(let tbl):
                    if tableAliases[tbl] != nil {
                        return .allColumns
                    }
                    return item
                default:
                    return item
                }
            }

            // Rewrite JOIN conditions
            result.joins = stmt.joins.map { join in
                var j = join
                if let cond = join.condition {
                    j.condition = rewriteExpression(cond, aliasMap: [:], tableAliases: tableAliases)
                }
                return j
            }
        }

        return result
    }

    /// Filter rows to only include columns specified by RETURNING clause
    private func filterReturningColumns(_ items: [SelectItem], rows: [Row]) -> [Row] {
        for item in items {
            if case .allColumns = item { return rows }
        }
        let colNames: [String] = items.compactMap { item -> String? in
            switch item {
            case .expression(let expr, _):
                if case .column(_, let name) = expr { return name }
                return nil
            default:
                return nil
            }
        }
        return rows.map { row in
            var filtered = [String: DBValue]()
            for col in colNames {
                if let val = row.values[col] { filtered[col] = val }
            }
            return Row(values: filtered)
        }
    }

    /// Sort rows by ORDER BY clauses (used for compound SELECT results)
    private func applyOrderBy(_ rows: [Row], orderBy: [OrderBy]) -> [Row] {
        rows.sorted { a, b in
            for ob in orderBy {
                let valA = a.values[ob.column] ?? DBValue.null
                let valB = b.values[ob.column] ?? DBValue.null
                if valA == valB { continue }
                let asc = ob.direction == .ascending
                return asc ? valA < valB : valA > valB
            }
            return false
        }
    }

    /// Internal: execute a parsed SELECT statement
    private func executeSelectStatement(_ originalStmt: SelectStatement, lowering: ASTLowering) async throws -> [Row] {
        // Handle table-less SELECT (e.g., SELECT length('hello'), SELECT 1+2)
        guard originalStmt.from != nil else {
            return try [evaluateTablelessSelect(originalStmt, lowering: lowering)]
        }

        // Resolve column aliases in WHERE/ORDER BY and table alias qualifiers
        let stmt = resolveAliases(originalStmt)

        guard let tableName = stmt.from!.tableName else {
            throw SQLError.unsupported("Subquery in FROM not yet supported")
        }

        // Intercept system catalog / information_schema queries
        if let catalogRows = try await handleSystemCatalog(tableName: tableName, stmt: stmt) {
            return catalogRows
        }

        // Check if this is a view — if so, execute the view's SQL and apply outer clauses
        if let viewSQL = mutableState.withLock({ $0.views[tableName.lowercased()] }) {
            var viewRows = try await execute(sql: viewSQL).rows
            // Apply WHERE clause on top of the view results
            if let whereClause = stmt.whereClause {
                let cond = try lowering.lowerWhereClause(whereClause)
                viewRows = viewRows.filter { row in
                    evaluateConditionForPartialIndex(cond, row: row)
                }
            }
            // Apply ORDER BY
            if !stmt.orderBy.isEmpty {
                if let orderByItems = try? lowering.lowerOrderBy(stmt.orderBy) {
                    viewRows.sort { a, b in
                        for ob in orderByItems {
                            let va = a.values[ob.column] ?? DBValue.null
                            let vb = b.values[ob.column] ?? DBValue.null
                            let asc = ob.direction == .ascending
                            if va < vb { return asc }
                            if va > vb { return !asc }
                        }
                        return false
                    }
                }
            }
            // Apply LIMIT / OFFSET
            if let offsetExpr = stmt.offset, case .integerLiteral(let o) = offsetExpr {
                viewRows = Array(viewRows.dropFirst(Int(o)))
            }
            if let limitExpr = stmt.limit, case .integerLiteral(let l) = limitExpr {
                viewRows = Array(viewRows.prefix(Int(l)))
            }
            // Apply column projection if not SELECT *
            if !stmt.columns.contains(where: { if case .allColumns = $0 { return true } else { return false } }) {
                viewRows = try viewRows.map { row in
                    var projected = [String: DBValue]()
                    for (i, item) in stmt.columns.enumerated() {
                        switch item {
                        case .allColumns:
                            for (k, v) in row.values { projected[k] = v }
                        case .tableAllColumns:
                            for (k, v) in row.values { projected[k] = v }
                        case .expression(let expr, let alias):
                            if case .column(_, let name) = expr {
                                projected[alias ?? name] = row.values[name] ?? .null
                            } else {
                                let val = try evaluateExpression(expr, row: row, lowering: lowering)
                                projected[alias ?? columnLabel(for: expr, index: i)] = val
                            }
                        }
                    }
                    return Row(values: projected)
                }
            }
            return viewRows
        }

        let condition: WhereCondition? = try stmt.whereClause.map { try lowering.lowerWhereClause($0) }
        let orderBy: [OrderBy]? = stmt.orderBy.isEmpty ? nil : try lowering.lowerOrderBy(stmt.orderBy)
        let limit: Int? = try stmt.limit.flatMap { expr -> Int? in
            if case .integerLiteral(let v) = expr { return Int(v) }
            return nil
        }
        let offset: Int? = try stmt.offset.flatMap { expr -> Int? in
            if case .integerLiteral(let v) = expr { return Int(v) }
            return nil
        }

        // Check for GROUP BY
        if !stmt.groupBy.isEmpty {
            let groupCols = stmt.groupBy.compactMap { expr -> String? in
                if case .column(_, let name) = expr { return name }
                return nil
            }
            let selectExprs = try lowering.lowerSelectExpressions(stmt.columns)
            let having: WhereCondition? = try stmt.having.map { try lowering.lowerWhereClause($0) }
            return try await select(
                from: tableName,
                select: selectExprs,
                where: condition,
                groupBy: groupCols,
                having: having,
                orderBy: orderBy,
                limit: limit,
                offset: offset
            )
        }

        // Check for JOINs
        if !stmt.joins.isEmpty {
            let joinClauses = try stmt.joins.map { try lowering.lowerJoin($0) }
            let columns = lowering.extractColumns(stmt.columns)
            return try await select(
                from: tableName,
                join: joinClauses,
                columns: columns,
                where: condition,
                orderBy: orderBy,
                limit: limit,
                offset: offset,
                distinct: stmt.distinct
            )
        }

        // Check if SELECT list has function/cast expressions or aliases needing evaluation
        let needsExprEval = selectNeedsExpressionEval(stmt.columns)
        let hasAliases = stmt.columns.contains { item in
            if case .expression(_, let alias) = item, alias != nil { return true }
            return false
        }
        let needsPostProcess = needsExprEval || hasAliases

        // Simple SELECT — fetch all columns if we need expression evaluation or alias mapping
        let columns = needsPostProcess ? nil : lowering.extractColumns(stmt.columns)
        var rows = try await select(
            from: tableName,
            columns: columns,
            where: condition,
            orderBy: orderBy,
            limit: limit,
            offset: offset,
            distinct: stmt.distinct
        )

        if needsPostProcess {
            rows = try postProcessSelectExpressions(rows, stmt: stmt, lowering: lowering)
        }

        return rows
    }

    // MARK: - Lifecycle

    public func close() async throws {
        // Persist index registry before closing
        let entries = try await indexManager.saveIndexRegistry()
        if !entries.isEmpty {
            let data = try JSONEncoder().encode(entries)
            try await storageEngine.saveIndexRegistry(data)
        }
        try await storageEngine.close()
    }

    public func getBufferPoolStats() async -> BufferPoolStats {
        await storageEngine.getBufferPoolStats()
    }

    // MARK: - EXPLAIN Helpers

    /// Check if any column referenced in the condition has an index
    private func checkHasIndex(table: String, condition: WhereCondition) -> Bool {
        let indexes = indexManager.listIndexes(tableName: table)
        let indexedCols = Set(indexes.map { $0.column })
        return conditionReferencesIndexed(condition, indexedCols: indexedCols)
    }

    private func conditionReferencesIndexed(_ cond: WhereCondition, indexedCols: Set<String>) -> Bool {
        switch cond {
        case .equals(let col, _), .notEquals(let col, _), .lessThan(let col, _),
             .greaterThan(let col, _), .lessThanOrEqual(let col, _), .greaterThanOrEqual(let col, _),
             .in(let col, _), .between(let col, _, _), .like(let col, _),
             .isNull(let col), .isNotNull(let col):
            return indexedCols.contains(col)
        case .and(let subs):
            return subs.contains { conditionReferencesIndexed($0, indexedCols: indexedCols) }
        case .or(let subs):
            return subs.allSatisfy { conditionReferencesIndexed($0, indexedCols: indexedCols) }
        }
    }

    // MARK: - Partial Index Condition Evaluator

    /// Evaluate a WHERE condition against a Row for partial index population.
    private func evaluateConditionForPartialIndex(_ condition: WhereCondition, row: Row) -> Bool {
        switch condition {
        case let .equals(column, value):
            if value == .null { return false }
            guard let rv = row.values[column], rv != .null else { return false }
            return rv == value
        case let .notEquals(column, value):
            if value == .null { return false }
            guard let rv = row.values[column], rv != .null else { return false }
            return rv != value
        case let .greaterThan(column, value):
            guard let rv = row.values[column], rv != .null, value != .null else { return false }
            return rv > value
        case let .lessThan(column, value):
            guard let rv = row.values[column], rv != .null, value != .null else { return false }
            return rv < value
        case let .greaterThanOrEqual(column, value):
            guard let rv = row.values[column], rv != .null, value != .null else { return false }
            return rv >= value
        case let .lessThanOrEqual(column, value):
            guard let rv = row.values[column], rv != .null, value != .null else { return false }
            return rv <= value
        case let .isNull(column):
            return row.values[column] == nil || row.values[column] == .null
        case let .isNotNull(column):
            return row.values[column] != nil && row.values[column] != .null
        case let .and(subs):
            return subs.allSatisfy { evaluateConditionForPartialIndex($0, row: row) }
        case let .or(subs):
            return subs.contains { evaluateConditionForPartialIndex($0, row: row) }
        case let .in(column, values):
            guard let rv = row.values[column], rv != .null else { return false }
            return values.contains(rv)
        case let .between(column, min, max):
            guard let rv = row.values[column], rv != .null else { return false }
            return rv >= min && rv <= max
        case let .like(column, pattern):
            guard let rv = row.values[column], case .string(let str) = rv else { return false }
            // Simple LIKE match
            let s = Array(str), p = Array(pattern)
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
    }

    // MARK: - Role Management (public API)

    /// Create a new role
    public func createRole(_ role: Role) async throws {
        try roleManager.createRole(role)
        try await persistRole(role)
    }

    /// Drop a role by name
    public func dropRole(_ name: String) async throws {
        try roleManager.dropRole(name)
        try await removePersistedRole(name: name)
    }

    /// Grant a permission to a role, optionally on a specific table
    public func grant(_ permission: Permission, to role: String, on table: String? = nil) async throws {
        roleManager.grant(permission, to: role, on: table)
        try await persistGrant(role: role, permission: permission, table: table)
    }

    /// Revoke a permission from a role, optionally on a specific table
    public func revoke(_ permission: Permission, from role: String, on table: String? = nil) async throws {
        roleManager.revoke(permission, from: role, on: table)
        try await removePersistedGrant(role: role, permission: permission, table: table)
    }

    /// Check whether a role has a specific permission
    public func hasPermission(_ role: String, _ permission: Permission, on table: String? = nil) -> Bool {
        roleManager.hasPermission(role, permission, on: table)
    }

    /// List all roles
    public func listRoles() -> [Role] {
        roleManager.listRoles()
    }

    /// List all grants
    public func listGrants() -> [RoleGrant] {
        roleManager.listGrants()
    }

    // MARK: - Metadata Persistence

    /// Ensure a metadata table exists, creating it if needed
    private func ensureMetadataTable(name: String, columns: [CobaltColumn]) async throws {
        if storageEngine.tableExists(name) { return }
        let schema = CobaltTableSchema(name: name, columns: columns)
        do {
            try await storageEngine.createTable(schema)
            // Auto-create index on primary key column
            if let pkColumn = schema.primaryKeyColumn {
                try await createIndex(table: name, column: pkColumn.name)
            }
        } catch CobaltError.tableAlreadyExists {
            // Another concurrent init created it — that's fine
        }
    }

    /// Load all persisted metadata from internal tables on startup
    private func loadPersistedMetadata() async throws {
        // 1. Load sequence counters
        if storageEngine.tableExists("_cobalt_sequences") {
            let rows = try await storageEngine.scanTable("_cobalt_sequences")
            mutableState.withLock { state in
                for (_, row) in rows {
                    if case .string(let key) = row.values["key"],
                       case .integer(let value) = row.values["value"] {
                        state.sequenceCounters[key] = value
                    }
                }
            }
        }

        // Also scan actual tables for MAX values of auto-increment columns
        // to handle data inserted by a previous version without persistence
        let allTables = storageEngine.listTables().filter { !CobaltDatabase.isInternalTable($0) }
        for tableName in allTables {
            guard let schema = storageEngine.getTableSchema(tableName) else { continue }
            let autoIncrementCols = schema.columns.filter { $0.isAutoIncrement }
            if autoIncrementCols.isEmpty { continue }
            for col in autoIncrementCols {
                let key = "\(tableName).\(col.name)"
                let result = try await queryExecutor.executeAggregate(
                    from: tableName, .max(column: col.name),
                    where: nil, transactionContext: nil
                )
                if case .integer(let maxVal) = result {
                    mutableState.withLock { state in
                        let current = state.sequenceCounters[key] ?? 0
                        if maxVal > current {
                            state.sequenceCounters[key] = maxVal
                        }
                    }
                }
            }
        }

        // 2. Load view definitions
        if storageEngine.tableExists("_cobalt_views") {
            let rows = try await storageEngine.scanTable("_cobalt_views")
            mutableState.withLock { state in
                for (_, row) in rows {
                    if case .string(let name) = row.values["name"],
                       case .string(let definition) = row.values["definition"] {
                        state.views[name] = definition
                    }
                }
            }
        }

        // 3. Load trigger definitions
        if storageEngine.tableExists("_cobalt_triggers") {
            let rows = try await storageEngine.scanTable("_cobalt_triggers")
            for (_, row) in rows {
                guard case .string(let name) = row.values["name"],
                      case .string(let tableName) = row.values["table_name"],
                      case .string(let timingStr) = row.values["timing"],
                      case .string(let eventStr) = row.values["event"],
                      case .string(let forEachStr) = row.values["for_each"],
                      case .string(let bodyStr) = row.values["body"] else { continue }
                guard let timing = TriggerTiming(rawValue: timingStr),
                      let event = TriggerEvent(rawValue: eventStr),
                      let forEach = TriggerForEach(rawValue: forEachStr) else { continue }
                let bodyStatements = (try? JSONDecoder().decode([String].self, from: Data(bodyStr.utf8))) ?? [bodyStr]
                let def = TriggerDef(
                    name: name, table: tableName, timing: timing,
                    event: event, forEach: forEach, body: bodyStatements
                )
                triggerManager.registerTrigger(def)
            }
        }

        // 4. Load roles
        if storageEngine.tableExists("_cobalt_roles") {
            let rows = try await storageEngine.scanTable("_cobalt_roles")
            for (_, row) in rows {
                guard case .string(let name) = row.values["name"] else { continue }
                let isSuperuser: Bool
                if case .boolean(let v) = row.values["is_superuser"] { isSuperuser = v }
                else { isSuperuser = false }
                let canLogin: Bool
                if case .boolean(let v) = row.values["can_login"] { canLogin = v }
                else { canLogin = true }
                let passwordHash: String?
                if case .string(let v) = row.values["password_hash"] { passwordHash = v }
                else { passwordHash = nil }
                let role = Role(name: name, isSuperuser: isSuperuser, canLogin: canLogin, passwordHash: passwordHash)
                try? roleManager.createRole(role)
            }
        }

        // 5. Load grants
        if storageEngine.tableExists("_cobalt_grants") {
            let rows = try await storageEngine.scanTable("_cobalt_grants")
            for (_, row) in rows {
                guard case .string(let roleName) = row.values["role"],
                      case .string(let permStr) = row.values["permission"],
                      let perm = Permission(rawValue: permStr) else { continue }
                let table: String?
                if case .string(let t) = row.values["table_name"] { table = t }
                else { table = nil }
                roleManager.grant(perm, to: roleName, on: table)
            }
        }
    }

    // MARK: - Sequence Counter Persistence

    private func persistSequenceCounter(table: String, column: String, value: Int64) async throws {
        try await ensureMetadataTable(name: "_cobalt_sequences", columns: [
            CobaltColumn(name: "key", type: .string, isPrimaryKey: true, isNullable: false),
            CobaltColumn(name: "value", type: .integer, isNullable: false),
        ])
        let key = "\(table).\(column)"
        _ = try await queryExecutor.executeDelete(
            from: "_cobalt_sequences",
            where: .equals(column: "key", value: .string(key)),
            transactionContext: nil
        )
        let row = Row(values: ["key": .string(key), "value": .integer(value)])
        try await queryExecutor.executeInsert(into: "_cobalt_sequences", row: row, transactionContext: nil)
    }

    // MARK: - View Persistence

    private func persistView(name: String, definition: String) async throws {
        try await ensureMetadataTable(name: "_cobalt_views", columns: [
            CobaltColumn(name: "name", type: .string, isPrimaryKey: true, isNullable: false),
            CobaltColumn(name: "definition", type: .string, isNullable: false),
        ])
        _ = try await queryExecutor.executeDelete(
            from: "_cobalt_views",
            where: .equals(column: "name", value: .string(name)),
            transactionContext: nil
        )
        let row = Row(values: ["name": .string(name), "definition": .string(definition)])
        try await queryExecutor.executeInsert(into: "_cobalt_views", row: row, transactionContext: nil)
    }

    private func removePersistedView(name: String) async throws {
        guard storageEngine.tableExists("_cobalt_views") else { return }
        _ = try await queryExecutor.executeDelete(
            from: "_cobalt_views",
            where: .equals(column: "name", value: .string(name)),
            transactionContext: nil
        )
    }

    // MARK: - Trigger Persistence

    private func persistTrigger(_ trigger: TriggerDef) async throws {
        try await ensureMetadataTable(name: "_cobalt_triggers", columns: [
            CobaltColumn(name: "name", type: .string, isPrimaryKey: true, isNullable: false),
            CobaltColumn(name: "table_name", type: .string, isNullable: false),
            CobaltColumn(name: "timing", type: .string, isNullable: false),
            CobaltColumn(name: "event", type: .string, isNullable: false),
            CobaltColumn(name: "for_each", type: .string, isNullable: false),
            CobaltColumn(name: "body", type: .string, isNullable: false),
        ])
        let bodyJSON = (try? String(data: JSONEncoder().encode(trigger.body), encoding: .utf8)) ?? "[]"
        _ = try await queryExecutor.executeDelete(
            from: "_cobalt_triggers",
            where: .equals(column: "name", value: .string(trigger.name)),
            transactionContext: nil
        )
        let row = Row(values: [
            "name": .string(trigger.name),
            "table_name": .string(trigger.table),
            "timing": .string(trigger.timing.rawValue),
            "event": .string(trigger.event.rawValue),
            "for_each": .string(trigger.forEach.rawValue),
            "body": .string(bodyJSON),
        ])
        try await queryExecutor.executeInsert(into: "_cobalt_triggers", row: row, transactionContext: nil)
    }

    private func removePersistedTrigger(name: String) async throws {
        guard storageEngine.tableExists("_cobalt_triggers") else { return }
        _ = try await queryExecutor.executeDelete(
            from: "_cobalt_triggers",
            where: .equals(column: "name", value: .string(name)),
            transactionContext: nil
        )
    }

    // MARK: - Role Persistence

    private func persistRole(_ role: Role) async throws {
        try await ensureMetadataTable(name: "_cobalt_roles", columns: [
            CobaltColumn(name: "name", type: .string, isPrimaryKey: true, isNullable: false),
            CobaltColumn(name: "is_superuser", type: .boolean, isNullable: false),
            CobaltColumn(name: "can_login", type: .boolean, isNullable: false),
            CobaltColumn(name: "password_hash", type: .string, isNullable: true),
        ])
        _ = try await queryExecutor.executeDelete(
            from: "_cobalt_roles",
            where: .equals(column: "name", value: .string(role.name)),
            transactionContext: nil
        )
        var values: [String: DBValue] = [
            "name": .string(role.name),
            "is_superuser": .boolean(role.isSuperuser),
            "can_login": .boolean(role.canLogin),
        ]
        if let hash = role.passwordHash {
            values["password_hash"] = .string(hash)
        } else {
            values["password_hash"] = .null
        }
        let row = Row(values: values)
        try await queryExecutor.executeInsert(into: "_cobalt_roles", row: row, transactionContext: nil)
    }

    private func removePersistedRole(name: String) async throws {
        guard storageEngine.tableExists("_cobalt_roles") else { return }
        _ = try await queryExecutor.executeDelete(
            from: "_cobalt_roles",
            where: .equals(column: "name", value: .string(name)),
            transactionContext: nil
        )
        if storageEngine.tableExists("_cobalt_grants") {
            _ = try await queryExecutor.executeDelete(
                from: "_cobalt_grants",
                where: .equals(column: "role", value: .string(name)),
                transactionContext: nil
            )
        }
    }

    // MARK: - Grant Persistence

    private func persistGrant(role: String, permission: Permission, table: String?) async throws {
        try await ensureMetadataTable(name: "_cobalt_grants", columns: [
            CobaltColumn(name: "role", type: .string, isNullable: false),
            CobaltColumn(name: "permission", type: .string, isNullable: false),
            CobaltColumn(name: "table_name", type: .string, isNullable: true),
        ])
        let row = Row(values: [
            "role": .string(role),
            "permission": .string(permission.rawValue),
            "table_name": table.map { .string($0) } ?? .null,
        ])
        try await queryExecutor.executeInsert(into: "_cobalt_grants", row: row, transactionContext: nil)
    }

    private func removePersistedGrant(role: String, permission: Permission, table: String?) async throws {
        guard storageEngine.tableExists("_cobalt_grants") else { return }
        let tableCondition: WhereCondition
        if let table = table {
            tableCondition = .and([
                .equals(column: "role", value: .string(role)),
                .equals(column: "permission", value: .string(permission.rawValue)),
                .equals(column: "table_name", value: .string(table)),
            ])
        } else {
            tableCondition = .and([
                .equals(column: "role", value: .string(role)),
                .equals(column: "permission", value: .string(permission.rawValue)),
                .isNull(column: "table_name"),
            ])
        }
        _ = try await queryExecutor.executeDelete(
            from: "_cobalt_grants",
            where: tableCondition,
            transactionContext: nil
        )
    }
}

// MARK: - Convenience Initializer

extension CobaltDatabase {
    /// Open a database with sensible defaults.
    /// - Parameters:
    ///   - name: Database name (default: "default"). Stored at `~/Library/Application Support/Cobalt/<name>.cobalt`.
    ///   - encrypted: If true, enables AES-256-GCM encryption with an auto-managed key file at `<name>.cobalt.key`.
    public convenience init(name: String = "default", encrypted: Bool = false) async throws {
        let path = CobaltConfiguration.databasePath(name: name)
        let key: Data? = encrypted ? try CobaltConfiguration.resolveEncryptionKey(for: path) : nil
        let config = CobaltConfiguration(path: path, encryptionKey: key)
        try await self.init(configuration: config)
    }
}

