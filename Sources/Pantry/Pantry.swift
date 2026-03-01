import Foundation
import PantryCore
import PantryIndex
import PantryQuery

/// Main entry point for Pantry — a modular embedded database.
public actor PantryDatabase: Sendable {
    private let storageEngine: StorageEngine
    private let queryExecutor: QueryExecutor
    private let indexManager: IndexManager
    private let configuration: PantryConfiguration
    private var currentTransactionContext: TransactionContext?

    /// Create or open a Pantry database
    public init(configuration: PantryConfiguration) async throws {
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
            encryptionProvider: encryptionProvider
        )
        self.storageEngine = engine

        // Initialize index manager
        let im = IndexManager(
            bufferPool: await engine.bufferPoolManager,
            storageManager: await engine.storageManager
        )
        self.indexManager = im

        // Wire index hook into storage engine
        await engine.setIndexHook(im)

        // Initialize query executor with table registry for cost-based planning
        self.queryExecutor = QueryExecutor(storageEngine: engine, indexManager: im, tableRegistry: await engine.tableRegistry)

        // Restore persisted indexes
        if let indexData = try await engine.loadIndexRegistry() {
            let entries = try JSONDecoder().decode([IndexRegistryEntry].self, from: indexData)
            try await im.loadIndexRegistry(entries: entries)
        }
    }

    // MARK: - Tables

    public func createTable(_ schema: PantryTableSchema) async throws {
        try await storageEngine.createTable(schema)

        // Auto-create index on primary key column for fast uniqueness checks
        if let pkColumn = schema.primaryKeyColumn {
            try await createIndex(table: schema.name, column: pkColumn.name)
        }
    }

    public func dropTable(_ name: String) async throws {
        try await storageEngine.dropTable(name)
        await indexManager.removeIndexes(tableName: name)
    }

    public func tableExists(_ name: String) async -> Bool {
        await storageEngine.tableExists(name)
    }

    public func listTables() async -> [String] {
        await storageEngine.listTables()
    }

    public func getTableSchema(_ name: String) async -> PantryTableSchema? {
        await storageEngine.getTableSchema(name)
    }

    internal func updateTableSchema(_ name: String, schema: PantryTableSchema) async throws {
        try await storageEngine.updateTableSchema(name, schema: schema)
    }

    /// Create an index on a column, optionally with INCLUDE columns for covering index scans
    public func createIndex(table: String, column: String, include: [String]? = nil) async throws {
        let columnIndex = try await indexManager.createIndex(tableName: table, columnName: column, includeColumns: include)

        // Populate the index from existing data with slim rows: __rid + indexed column + INCLUDE columns
        let rows = try await storageEngine.scanTable(table)
        for (record, row) in rows {
            if let value = row.values[column] {
                var slimValues: [String: DBValue] = [
                    "__rid": .integer(Int64(bitPattern: record.id)),
                    column: value
                ]
                // Add INCLUDE columns for covering index
                if let include = include {
                    for incCol in include { slimValues[incCol] = row.values[incCol] ?? .null }
                }
                try await columnIndex.insert(key: value, row: Row(values: slimValues))
            }
        }

        // Mark column as indexed in stats and refresh distinct counts
        try await storageEngine.analyzeTable(table)
    }

    /// Create a partial index on a column — only rows matching `where` condition are indexed.
    /// Partial indexes are smaller and faster when queries always include the same filter.
    public func createPartialIndex(table: String, column: String, where condition: WhereCondition, include: [String]? = nil) async throws {
        let columnIndex = try await indexManager.createIndex(tableName: table, columnName: column, includeColumns: include, partialCondition: condition)

        // Populate the index from existing data that matches the condition
        let rows = try await storageEngine.scanTable(table)
        for (record, row) in rows {
            // Only index rows that satisfy the partial condition
            if !evaluateConditionForPartialIndex(condition, row: row) { continue }
            if let value = row.values[column] {
                var slimValues: [String: DBValue] = [
                    "__rid": .integer(Int64(bitPattern: record.id)),
                    column: value
                ]
                if let include = include {
                    for incCol in include { slimValues[incCol] = row.values[incCol] ?? .null }
                }
                try await columnIndex.insert(key: value, row: Row(values: slimValues))
            }
        }

        try await storageEngine.analyzeTable(table)
    }

    public func createCompoundIndex(table: String, columns: [String]) async throws {
        let columnIndex = try await indexManager.createCompoundIndex(tableName: table, columns: columns)

        // Populate the index from existing data with slim rows: __rid + indexed columns only
        let rows = try await storageEngine.scanTable(table)
        for (record, row) in rows {
            let rid: DBValue = .integer(Int64(bitPattern: record.id))
            var slimValues: [String: DBValue] = ["__rid": rid]
            let keyValues = columns.map { col -> DBValue in
                let v = row.values[col] ?? .null
                slimValues[col] = v
                return v
            }
            let compoundKey = DBValue.compound(keyValues)
            try await columnIndex.insert(key: compoundKey, row: Row(values: slimValues))
        }
    }

    public func listIndexes(on table: String) async -> [(column: String, isCompound: Bool)] {
        await indexManager.listIndexes(tableName: table)
    }

    public func dropIndex(table: String, column: String) async {
        await indexManager.dropIndex(tableName: table, columnName: column)
    }

    /// Collect column statistics for query optimization (like SQL ANALYZE)
    public func analyzeTable(_ table: String) async throws {
        try await storageEngine.analyzeTable(table)
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
        return try await queryExecutor.executeSelect(from: table, columns: columns, where: condition, modifiers: mods, transactionContext: currentTransactionContext)
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
        return try await queryExecutor.executeJoin(from: table, joins: joins, columns: columns, where: condition, modifiers: mods, transactionContext: currentTransactionContext)
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
        return try await queryExecutor.executeGroupBy(from: table, select: expressions, where: condition, groupBy: groupByClause, modifiers: mods, transactionContext: currentTransactionContext)
    }

    // MARK: - Set Operations

    /// Combine two query results using UNION, UNION ALL, INTERSECT, or EXCEPT
    public func combine(_ operation: SetOperation, left: [Row], right: [Row]) async -> [Row] {
        await queryExecutor.executeSetOperation(operation, left: left, right: right)
    }

    // MARK: - DML

    public func insert(into table: String, values: [String: DBValue]) async throws {
        let row = Row(values: values)
        try await queryExecutor.executeInsert(into: table, row: row, transactionContext: currentTransactionContext)
    }

    public func update(table: String, set values: [String: DBValue], where condition: WhereCondition? = nil) async throws -> Int {
        try await queryExecutor.executeUpdate(table: table, set: values, where: condition, transactionContext: currentTransactionContext)
    }

    public func delete(from table: String, where condition: WhereCondition? = nil) async throws -> Int {
        try await queryExecutor.executeDelete(from: table, where: condition, transactionContext: currentTransactionContext)
    }

    public func aggregate(from table: String, _ function: AggregateFunction, where condition: WhereCondition? = nil) async throws -> DBValue {
        try await queryExecutor.executeAggregate(from: table, function, where: condition, transactionContext: currentTransactionContext)
    }

    public func count(from table: String, where condition: WhereCondition? = nil) async throws -> Int {
        let result = try await queryExecutor.executeAggregate(from: table, .count(column: nil), where: condition, transactionContext: currentTransactionContext)
        if case .integer(let v) = result { return Int(v) }
        return 0
    }

    public func exists(in table: String, where condition: WhereCondition) async throws -> Bool {
        try await count(from: table, where: condition) > 0
    }

    public func insertAll(into table: String, rows: [[String: DBValue]]) async throws {
        try await transaction { db in
            for row in rows {
                try await db.insert(into: table, values: row)
            }
        }
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

    public func transaction<T: Sendable>(isolationLevel: IsolationLevel? = nil, body: @Sendable (PantryDatabase) async throws -> T) async throws -> T {
        // Support nested transaction calls — if already in a transaction, just execute the body
        if currentTransactionContext != nil {
            return try await body(self)
        }

        let txContext = try await storageEngine.beginTransaction(isolationLevel: isolationLevel ?? configuration.isolationLevel)
        currentTransactionContext = txContext
        do {
            let result = try await body(self)
            currentTransactionContext = nil
            try await storageEngine.commitTransaction(txContext)
            return result
        } catch {
            currentTransactionContext = nil
            do {
                try await storageEngine.rollbackTransaction(txContext)
            } catch {
                // Rollback failure must not mask the original error
            }
            throw error
        }
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
}

// MARK: - Convenience Initializer

extension PantryDatabase {
    /// Open a database with sensible defaults.
    /// - Parameters:
    ///   - name: Database name (default: "default"). Stored at `~/Library/Application Support/Pantry/<name>.pantry`.
    ///   - encrypted: If true, enables AES-256-GCM encryption with an auto-managed key file at `<name>.pantry.key`.
    public init(name: String = "default", encrypted: Bool = false) async throws {
        let path = PantryConfiguration.databasePath(name: name)
        let key: Data? = encrypted ? try PantryConfiguration.resolveEncryptionKey(for: path) : nil
        let config = PantryConfiguration(path: path, encryptionKey: key)
        try await self.init(configuration: config)
    }
}

// Extension to wire index hook
extension StorageEngine {
    public func setIndexHook(_ hook: any IndexHook) {
        self.indexHook = hook
    }
}
