import Foundation
import PantryCore
import PantryQuery

/// Codable convenience methods for PantryDatabase
extension PantryDatabase {
    /// Store a Codable value in a collection with an optional ID
    public func store<T: Codable & Sendable>(_ value: T, id: String? = nil, in collection: String) async throws -> String {
        let actualID = id ?? UUID().uuidString

        // Ensure the collection table exists
        try await ensureCollectionTable(collection)

        let encoder = JSONEncoder()
        let jsonData = try encoder.encode(value)
        let jsonString = String(data: jsonData, encoding: .utf8) ?? ""

        try await insert(into: collection, values: [
            "_id": .string(actualID),
            "_data": .string(jsonString),
            "_type": .string(String(describing: T.self)),
            "_timestamp": .double(Date().timeIntervalSince1970)
        ])

        return actualID
    }

    /// Retrieve a Codable value by ID from a collection
    public func retrieve<T: Codable & Sendable>(_ type: T.Type, id: String, from collection: String) async throws -> T? {
        let rows = try await select(
            from: collection,
            where: .equals(column: "_id", value: .string(id))
        )

        guard let row = rows.first,
              case .string(let jsonString) = row.values["_data"],
              let jsonData = jsonString.data(using: .utf8) else {
            return nil
        }

        return try JSONDecoder().decode(T.self, from: jsonData)
    }

    /// Retrieve all values of a type from a collection
    public func retrieveAll<T: Codable & Sendable>(_ type: T.Type, from collection: String) async throws -> [T] {
        let rows = try await select(from: collection)
        var results: [T] = []

        for row in rows {
            if case .string(let jsonString) = row.values["_data"],
               let jsonData = jsonString.data(using: .utf8),
               let value = try? JSONDecoder().decode(T.self, from: jsonData) {
                results.append(value)
            }
        }

        return results
    }

    /// Remove a value by ID from a collection
    public func remove(id: String, from collection: String) async throws {
        _ = try await delete(from: collection, where: .equals(column: "_id", value: .string(id)))
    }

    /// Ensure a collection table exists with the standard schema
    private func ensureCollectionTable(_ name: String) async throws {
        if await tableExists(name) { return }
        let schema = PantryTableSchema(name: name, columns: [
            PantryColumn(name: "_id", type: .string, isPrimaryKey: true, isNullable: false),
            PantryColumn(name: "_data", type: .string, isNullable: false),
            PantryColumn(name: "_type", type: .string, isNullable: false),
            PantryColumn(name: "_timestamp", type: .double, isNullable: false),
        ])
        try await createTable(schema)
    }
}
