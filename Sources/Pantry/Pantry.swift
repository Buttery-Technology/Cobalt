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

    // MARK: - Queries

    public func select(from table: String, columns: [String]? = nil, where condition: WhereCondition? = nil) async throws -> [Row] {
        try await queryExecutor.executeSelect(from: table, columns: columns, where: condition)
    }

    public func insert(into table: String, values: [String: DBValue]) async throws {
        let row = Row(values: values)
        try await queryExecutor.executeInsert(into: table, row: row)
    }

    public func update(table: String, set values: [String: DBValue], where condition: WhereCondition? = nil) async throws -> Int {
        try await queryExecutor.executeUpdate(table: table, set: values, where: condition)
    }

    public func delete(from table: String, where condition: WhereCondition? = nil) async throws -> Int {
        try await queryExecutor.executeDelete(from: table, where: condition)
    }

    // MARK: - Transactions

    public func transaction<T: Sendable>(isolationLevel: IsolationLevel? = nil, body: @Sendable (PantryDatabase) async throws -> T) async throws -> T {
        let txContext = try await storageEngine.beginTransaction(isolationLevel: isolationLevel ?? configuration.isolationLevel)
        do {
            let result = try await body(self)
            try await storageEngine.commitTransaction(txContext)
            return result
        } catch {
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
        try await storageEngine.close()
    }

    public func getBufferPoolStats() async -> BufferPoolStats {
        await storageEngine.getBufferPoolStats()
    }
}

// Extension to wire index hook
extension StorageEngine {
    public func setIndexHook(_ hook: any IndexHook) {
        self.indexHook = hook
    }
}
