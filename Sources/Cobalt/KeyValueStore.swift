import Foundation
import CobaltCore
import CobaltQuery

/// Key-value convenience methods for CobaltDatabase
extension CobaltDatabase {
    private static let kvTableName = "_cobalt_kv"
    private static let kvEncoder: JSONEncoder = {
        let enc = JSONEncoder()
        enc.nonConformingFloatEncodingStrategy = .convertToString(positiveInfinity: "+inf", negativeInfinity: "-inf", nan: "nan")
        return enc
    }()
    private static let kvDecoder: JSONDecoder = {
        let dec = JSONDecoder()
        dec.nonConformingFloatDecodingStrategy = .convertFromString(positiveInfinity: "+inf", negativeInfinity: "-inf", nan: "nan")
        return dec
    }()

    /// Set a key-value pair
    public func set(_ key: String, value: DBValue) async throws {
        try await ensureKVTable()

        // Atomic upsert: delete + insert in a transaction
        try await transaction { tx in
            _ = try await tx.delete(from: Self.kvTableName, where: .equals(column: "_key", value: .string(key)))
            try await tx.insert(into: Self.kvTableName, values: [
                "_key": .string(key),
                "_value": value,
                "_timestamp": .double(Date().timeIntervalSince1970)
            ])
        }
    }

    /// Get a value by key
    public func get(_ key: String) async throws -> DBValue? {
        guard await tableExists(Self.kvTableName) else { return nil }

        let rows: [Row]
        do {
            rows = try await select(
                from: Self.kvTableName,
                columns: ["_value"],
                where: .equals(column: "_key", value: .string(key))
            )
        } catch CobaltError.tableNotFound {
            return nil // Table dropped between exists check and select
        }

        return rows.first?.values["_value"]
    }

    /// Set a Codable value for a key
    public func set<T: Codable & Sendable>(_ key: String, codableValue: T) async throws {
        let data = try Self.kvEncoder.encode(codableValue)
        guard let jsonString = String(data: data, encoding: .utf8) else {
            throw CobaltError.schemaSerializationError
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
        guard await tableExists(Self.kvTableName) else { return }
        do {
            _ = try await delete(from: Self.kvTableName, where: .equals(column: "_key", value: .string(key)))
        } catch CobaltError.tableNotFound {
            return // Table dropped between exists check and delete
        }
    }

    /// Ensure the KV table exists
    private func ensureKVTable() async throws {
        if await tableExists(Self.kvTableName) { return }
        do {
            let schema = CobaltTableSchema(name: Self.kvTableName, columns: [
                CobaltColumn(name: "_key", type: .string, isPrimaryKey: true, isNullable: false),
                CobaltColumn(name: "_value", type: .string, isNullable: true),
                CobaltColumn(name: "_timestamp", type: .double, isNullable: false),
            ])
            try await createTable(schema)
        } catch CobaltError.tableAlreadyExists {
            // Another concurrent call created the table between our check and create
        }
    }
}
