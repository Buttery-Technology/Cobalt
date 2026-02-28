import Foundation
import PantryCore
import PantryQuery

/// Codable convenience methods for PantryDatabase
extension PantryDatabase {
    private static let codableEncoder = JSONEncoder()
    private static let codableDecoder = JSONDecoder()

    /// Store a Codable value in a collection with an optional ID
    public func store<T: Codable & Sendable>(_ value: T, id: String? = nil, in collection: String) async throws -> String {
        let actualID = id ?? UUID().uuidString

        // Ensure the collection table exists
        try await ensureCollectionTable(collection)

        let jsonData = try Self.codableEncoder.encode(value)
        guard let jsonString = String(data: jsonData, encoding: .utf8) else {
            throw PantryError.schemaSerializationError
        }

        // Remove existing entry with same ID to prevent duplicates
        _ = try await delete(from: collection, where: .equals(column: "_id", value: .string(actualID)))

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
        guard await tableExists(collection) else { return nil }
        let rows = try await select(
            from: collection,
            where: .equals(column: "_id", value: .string(id))
        )

        guard let row = rows.first,
              case .string(let jsonString) = row.values["_data"],
              let jsonData = jsonString.data(using: .utf8) else {
            return nil
        }

        return try Self.codableDecoder.decode(T.self, from: jsonData)
    }

    /// Retrieve all values of a type from a collection
    public func retrieveAll<T: Codable & Sendable>(_ type: T.Type, from collection: String) async throws -> [T] {
        guard await tableExists(collection) else { return [] }
        let rows = try await select(from: collection)
        let decoder = Self.codableDecoder
        var results: [T] = []

        for row in rows {
            if case .string(let jsonString) = row.values["_data"],
               let jsonData = jsonString.data(using: .utf8),
               let value = try? decoder.decode(T.self, from: jsonData) {
                results.append(value)
            }
        }

        return results
    }

    /// Remove a value by ID from a collection
    public func remove(id: String, from collection: String) async throws {
        guard await tableExists(collection) else { return }
        _ = try await delete(from: collection, where: .equals(column: "_id", value: .string(id)))
    }

    /// Ensure a collection table exists with the standard schema
    private func ensureCollectionTable(_ name: String) async throws {
        if await tableExists(name) { return }
        do {
            let schema = PantryTableSchema(name: name, columns: [
                PantryColumn(name: "_id", type: .string, isPrimaryKey: true, isNullable: false),
                PantryColumn(name: "_data", type: .string, isNullable: false),
                PantryColumn(name: "_type", type: .string, isNullable: false),
                PantryColumn(name: "_timestamp", type: .double, isNullable: false),
            ])
            try await createTable(schema)
        } catch PantryError.tableAlreadyExists {
            // Another concurrent call created the table between our check and create
        }
    }
}
