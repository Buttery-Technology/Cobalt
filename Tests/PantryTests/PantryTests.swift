import Testing
import Foundation
@testable import PantryCore
@testable import Pantry

@Test func testPantryDatabaseCreateAndQuery() async throws {
    let path = NSTemporaryDirectory() + "pantry_integration_\(UUID().uuidString).pantry"
    defer { try? FileManager.default.removeItem(atPath: path) }

    let db = try await PantryDatabase(configuration: PantryConfiguration(path: path))

    // Create a table
    try await db.createTable(PantryTableSchema(name: "users", columns: [
        PantryColumn(name: "id", type: .integer, isPrimaryKey: true, isNullable: false),
        PantryColumn(name: "name", type: .string, isNullable: false),
        PantryColumn(name: "age", type: .integer),
    ]))

    // Insert rows
    try await db.insert(into: "users", values: ["id": .integer(1), "name": .string("Alice"), "age": .integer(30)])
    try await db.insert(into: "users", values: ["id": .integer(2), "name": .string("Bob"), "age": .integer(25)])
    try await db.insert(into: "users", values: ["id": .integer(3), "name": .string("Charlie"), "age": .integer(35)])

    // Query
    let all = try await db.select(from: "users")
    #expect(all.count == 3)

    let young = try await db.select(from: "users", where: .lessThan(column: "age", value: .integer(30)))
    #expect(young.count == 1)
    #expect(young[0].values["name"] == .string("Bob"))

    // Update
    let updated = try await db.update(table: "users", set: ["age": .integer(31)], where: .equals(column: "name", value: .string("Alice")))
    #expect(updated == 1)

    // Delete
    let deleted = try await db.delete(from: "users", where: .equals(column: "name", value: .string("Charlie")))
    #expect(deleted == 1)

    let remaining = try await db.select(from: "users")
    #expect(remaining.count == 2)

    try await db.close()
}

@Test func testKeyValueStore() async throws {
    let path = NSTemporaryDirectory() + "pantry_kv_\(UUID().uuidString).pantry"
    defer { try? FileManager.default.removeItem(atPath: path) }

    let db = try await PantryDatabase(configuration: PantryConfiguration(path: path))

    // Simple KV
    try await db.set("theme", value: .string("dark"))
    let theme = try await db.get("theme")
    #expect(theme == .string("dark"))

    // Overwrite
    try await db.set("theme", value: .string("light"))
    let updated = try await db.get("theme")
    #expect(updated == .string("light"))

    // Delete
    try await db.delete(key: "theme")
    let gone = try await db.get("theme")
    #expect(gone == nil)

    // Codable KV
    struct Settings: Codable, Sendable, Equatable {
        var fontSize: Int
        var darkMode: Bool
    }

    try await db.set("settings", codableValue: Settings(fontSize: 14, darkMode: true))
    let settings: Settings? = try await db.get("settings", as: Settings.self)
    #expect(settings == Settings(fontSize: 14, darkMode: true))

    try await db.close()
}

@Test func testCodableStore() async throws {
    let path = NSTemporaryDirectory() + "pantry_codable_\(UUID().uuidString).pantry"
    defer { try? FileManager.default.removeItem(atPath: path) }

    let db = try await PantryDatabase(configuration: PantryConfiguration(path: path))

    struct ModelConfig: Codable, Sendable, Equatable {
        var name: String
        var temperature: Double
        var maxTokens: Int
    }

    let config = ModelConfig(name: "gpt4", temperature: 0.7, maxTokens: 4096)
    let storedID = try await db.store(config, id: "gpt4-default", in: "model_configs")
    #expect(storedID == "gpt4-default")

    let retrieved: ModelConfig? = try await db.retrieve(ModelConfig.self, id: "gpt4-default", from: "model_configs")
    #expect(retrieved == config)

    // Store another
    let config2 = ModelConfig(name: "claude", temperature: 0.5, maxTokens: 8192)
    _ = try await db.store(config2, id: "claude-default", in: "model_configs")

    let allConfigs: [ModelConfig] = try await db.retrieveAll(ModelConfig.self, from: "model_configs")
    #expect(allConfigs.count == 2)

    // Remove
    try await db.remove(id: "gpt4-default", from: "model_configs")
    let afterRemove: ModelConfig? = try await db.retrieve(ModelConfig.self, id: "gpt4-default", from: "model_configs")
    #expect(afterRemove == nil)

    try await db.close()
}

@Test func testEncryptedDatabase() async throws {
    let path = NSTemporaryDirectory() + "pantry_encrypted_\(UUID().uuidString).pantry"
    defer { try? FileManager.default.removeItem(atPath: path) }

    let key = Data(repeating: 0x42, count: 32)
    let db = try await PantryDatabase(configuration: PantryConfiguration(
        path: path,
        encryptionKey: key,
        bufferPoolCapacity: 50
    ))

    try await db.createTable(PantryTableSchema(name: "secrets", columns: [
        PantryColumn(name: "id", type: .integer, isPrimaryKey: true, isNullable: false),
        PantryColumn(name: "value", type: .string),
    ]))

    try await db.insert(into: "secrets", values: ["id": .integer(1), "value": .string("classified")])

    let rows = try await db.select(from: "secrets")
    #expect(rows.count == 1)
    #expect(rows[0].values["value"] == .string("classified"))

    try await db.close()
}

@Test func testBufferPoolStats() async throws {
    let path = NSTemporaryDirectory() + "pantry_stats_\(UUID().uuidString).pantry"
    defer { try? FileManager.default.removeItem(atPath: path) }

    let db = try await PantryDatabase(configuration: PantryConfiguration(path: path))
    let stats = await db.getBufferPoolStats()
    #expect(stats.hitRate >= 0)

    try await db.close()
}

// MARK: - Stress Tests

@Test func testBulkInsertAndQuery() async throws {
    let path = NSTemporaryDirectory() + "pantry_bulk_\(UUID().uuidString).pantry"
    defer { try? FileManager.default.removeItem(atPath: path) }

    let db = try await PantryDatabase(configuration: PantryConfiguration(
        path: path,
        bufferPoolCapacity: 500
    ))

    try await db.createTable(PantryTableSchema(name: "items", columns: [
        PantryColumn(name: "id", type: .integer, isPrimaryKey: true, isNullable: false),
        PantryColumn(name: "name", type: .string, isNullable: false),
        PantryColumn(name: "score", type: .double),
    ]))

    // Insert 1000 rows
    let insertCount = 1000
    for i in 0..<insertCount {
        try await db.insert(into: "items", values: [
            "id": .integer(Int64(i)),
            "name": .string("item_\(i)"),
            "score": .double(Double(i) * 0.1)
        ])
    }

    // Full scan should return all rows
    let all = try await db.select(from: "items")
    #expect(all.count == insertCount)

    // Filtered query
    let highScore = try await db.select(from: "items", where: .greaterThan(column: "score", value: .double(99.0)))
    #expect(highScore.count == 9) // items 991-999 (990 has score exactly 99.0)

    // Update subset (items 0-9 have scores 0.0 to 0.9)
    let updated = try await db.update(table: "items", set: ["score": .double(-1.0)], where: .lessThan(column: "score", value: .double(1.0)))
    #expect(updated == 10) // items 0-9

    // Delete subset (items 990-999 have scores >= 99.0)
    let deleted = try await db.delete(from: "items", where: .greaterThanOrEqual(column: "score", value: .double(99.0)))
    #expect(deleted == 10)

    let remaining = try await db.select(from: "items")
    #expect(remaining.count == insertCount - 10)

    try await db.close()
}

@Test func testIndexedQuery() async throws {
    let path = NSTemporaryDirectory() + "pantry_indexed_\(UUID().uuidString).pantry"
    defer { try? FileManager.default.removeItem(atPath: path) }

    let db = try await PantryDatabase(configuration: PantryConfiguration(path: path))

    try await db.createTable(PantryTableSchema(name: "products", columns: [
        PantryColumn(name: "id", type: .integer, isPrimaryKey: true, isNullable: false),
        PantryColumn(name: "category", type: .string),
        PantryColumn(name: "price", type: .double),
    ]))

    // Insert data
    let categories = ["electronics", "books", "clothing", "food"]
    for i in 0..<200 {
        try await db.insert(into: "products", values: [
            "id": .integer(Int64(i)),
            "category": .string(categories[i % categories.count]),
            "price": .double(Double(i) * 1.5)
        ])
    }

    // Create index on category
    try await db.createIndex(table: "products", column: "category")

    // Query using index
    let electronics = try await db.select(from: "products", where: .equals(column: "category", value: .string("electronics")))
    #expect(electronics.count == 50)

    // Range query on price
    try await db.createIndex(table: "products", column: "price")
    let cheap = try await db.select(from: "products", where: .lessThan(column: "price", value: .double(15.0)))
    #expect(cheap.count == 10) // items 0-9

    try await db.close()
}

@Test func testTableExistsAndListTables() async throws {
    let path = NSTemporaryDirectory() + "pantry_tables_\(UUID().uuidString).pantry"
    defer { try? FileManager.default.removeItem(atPath: path) }

    let db = try await PantryDatabase(configuration: PantryConfiguration(path: path))

    let existsBefore = await db.tableExists("users")
    #expect(!existsBefore)

    try await db.createTable(PantryTableSchema(name: "users", columns: [
        PantryColumn(name: "id", type: .integer, isPrimaryKey: true, isNullable: false),
    ]))
    try await db.createTable(PantryTableSchema(name: "posts", columns: [
        PantryColumn(name: "id", type: .integer, isPrimaryKey: true, isNullable: false),
    ]))

    let existsAfter = await db.tableExists("users")
    #expect(existsAfter)

    let tables = await db.listTables()
    #expect(tables.contains("users"))
    #expect(tables.contains("posts"))
    #expect(tables.count == 2)

    try await db.dropTable("posts")
    let tablesAfterDrop = await db.listTables()
    #expect(tablesAfterDrop.count == 1)
    #expect(!tablesAfterDrop.contains("posts"))

    try await db.close()
}

@Test func testDBValueLiterals() {
    let intVal: DBValue = 42
    #expect(intVal == .integer(42))

    let doubleVal: DBValue = 3.14
    #expect(doubleVal == .double(3.14))

    let stringVal: DBValue = "hello"
    #expect(stringVal == .string("hello"))

    let boolVal: DBValue = true
    #expect(boolVal == .boolean(true))

    let nullVal: DBValue = nil
    #expect(nullVal == .null)
}

@Test func testMultiPageTable() async throws {
    let path = NSTemporaryDirectory() + "pantry_multipage_\(UUID().uuidString).pantry"
    defer { try? FileManager.default.removeItem(atPath: path) }

    let db = try await PantryDatabase(configuration: PantryConfiguration(path: path))

    try await db.createTable(PantryTableSchema(name: "large", columns: [
        PantryColumn(name: "id", type: .integer, isPrimaryKey: true, isNullable: false),
        PantryColumn(name: "payload", type: .string),
    ]))

    // Insert records with large payloads to force multi-page allocation
    let payload = String(repeating: "x", count: 500)
    for i in 0..<50 {
        try await db.insert(into: "large", values: [
            "id": .integer(Int64(i)),
            "payload": .string(payload)
        ])
    }

    let all = try await db.select(from: "large")
    #expect(all.count == 50)

    // Verify data integrity
    for row in all {
        #expect(row.values["payload"] == .string(payload))
    }

    try await db.close()
}
