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

        // Initialize query executor
        self.queryExecutor = QueryExecutor(storageEngine: engine, indexManager: im)

        // Restore persisted indexes
        if let indexData = try await engine.loadIndexRegistry() {
            let entries = try JSONDecoder().decode([IndexRegistryEntry].self, from: indexData)
            try await im.loadIndexRegistry(entries: entries)
        }
    }

    // MARK: - Tables

    public func createTable(_ schema: PantryTableSchema) async throws {
        try await storageEngine.createTable(schema)
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

    public func createIndex(table: String, column: String) async throws {
        let columnIndex = try await indexManager.createIndex(tableName: table, columnName: column)

        // Populate the index from existing data
        let rows = try await storageEngine.scanTable(table)
        for (_, row) in rows {
            if let value = row.values[column] {
                try await columnIndex.insert(key: value, row: row)
            }
        }
    }

    public func createCompoundIndex(table: String, columns: [String]) async throws {
        let columnIndex = try await indexManager.createCompoundIndex(tableName: table, columns: columns)

        // Populate the index from existing data
        let rows = try await storageEngine.scanTable(table)
        for (_, row) in rows {
            let keyValues = columns.map { row.values[$0] ?? .null }
            let compoundKey = DBValue.compound(keyValues)
            try await columnIndex.insert(key: compoundKey, row: row)
        }
    }

    // MARK: - Queries

    public func select(from table: String, columns: [String]? = nil, where condition: WhereCondition? = nil) async throws -> [Row] {
        try await queryExecutor.executeSelect(from: table, columns: columns, where: condition, transactionContext: currentTransactionContext)
    }

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
