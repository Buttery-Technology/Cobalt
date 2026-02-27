import Foundation
import PantryCore
import PantryQuery

/// Key-value convenience methods for PantryDatabase
extension PantryDatabase {
    private static let kvTableName = "_pantry_kv"
    private static let kvEncoder = JSONEncoder()
    private static let kvDecoder = JSONDecoder()

    /// Set a key-value pair
    public func set(_ key: String, value: DBValue) async throws {
        try await ensureKVTable()

        // Delete existing key if present
        _ = try await delete(from: Self.kvTableName, where: .equals(column: "_key", value: .string(key)))

        try await insert(into: Self.kvTableName, values: [
            "_key": .string(key),
            "_value": value,
            "_timestamp": .double(Date().timeIntervalSince1970)
        ])
    }

    /// Get a value by key
    public func get(_ key: String) async throws -> DBValue? {
        try await ensureKVTable()

        let rows = try await select(
            from: Self.kvTableName,
            columns: ["_value"],
            where: .equals(column: "_key", value: .string(key))
        )

        return rows.first?.values["_value"]
    }

    /// Set a Codable value for a key
    public func set<T: Codable & Sendable>(_ key: String, codableValue: T) async throws {
        let data = try Self.kvEncoder.encode(codableValue)
        guard let jsonString = String(data: data, encoding: .utf8) else {
            throw PantryError.schemaSerializationError
        }
        try await set(key, value: .string(jsonString))
    }

    /// Get a Codable value for a key
    public func get<T: Codable & Sendable>(_ key: String, as type: T.Type) async throws -> T? {
        guard let value = try await get(key),
              case .string(let jsonString) = value,
              let data = jsonString.data(using: .utf8) else {
            return nil
        }
        return try Self.kvDecoder.decode(T.self, from: data)
    }

    /// Delete a key
    public func delete(key: String) async throws {
        try await ensureKVTable()
        _ = try await delete(from: Self.kvTableName, where: .equals(column: "_key", value: .string(key)))
    }

    /// Ensure the KV table exists
    private func ensureKVTable() async throws {
        if await tableExists(Self.kvTableName) { return }
        do {
            let schema = PantryTableSchema(name: Self.kvTableName, columns: [
                PantryColumn(name: "_key", type: .string, isPrimaryKey: true, isNullable: false),
                PantryColumn(name: "_value", type: .string, isNullable: true),
                PantryColumn(name: "_timestamp", type: .double, isNullable: false),
            ])
            try await createTable(schema)
        } catch PantryError.tableAlreadyExists {
            // Another concurrent call created the table between our check and create
        }
    }
}
