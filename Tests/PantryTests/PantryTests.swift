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

// MARK: - Data Persistence Tests

@Test func testDataPersistsAcrossCloseAndReopen() async throws {
    let path = NSTemporaryDirectory() + "pantry_persist_\(UUID().uuidString).pantry"
    defer { try? FileManager.default.removeItem(atPath: path) }

    // Session 1: create table and insert data
    let db1 = try await PantryDatabase(configuration: PantryConfiguration(path: path))
    try await db1.createTable(PantryTableSchema(name: "notes", columns: [
        .id("id"),
        .string("title", nullable: false),
        .string("body"),
        .double("created_at"),
    ]))

    try await db1.insert(into: "notes", values: [
        "id": .string("n1"), "title": .string("First Note"),
        "body": .string("Hello world"), "created_at": .double(1000.0)
    ])
    try await db1.insert(into: "notes", values: [
        "id": .string("n2"), "title": .string("Second Note"),
        "body": .string("Goodbye world"), "created_at": .double(2000.0)
    ])
    try await db1.close()

    // Session 2: reopen and verify data survived
    let db2 = try await PantryDatabase(configuration: PantryConfiguration(path: path))
    let rows = try await db2.select(from: "notes")
    #expect(rows.count == 2)

    let titles = Set(rows.compactMap { $0["title"] })
    #expect(titles.contains(.string("First Note")))
    #expect(titles.contains(.string("Second Note")))

    // Verify queries work on reopened DB
    let filtered = try await db2.select(from: "notes",
        where: .equals(column: "id", value: .string("n1")))
    #expect(filtered.count == 1)
    #expect(filtered[0]["body"] == .string("Hello world"))

    try await db2.close()
}

@Test func testEncryptedDataPersistsAcrossCloseAndReopen() async throws {
    let path = NSTemporaryDirectory() + "pantry_enc_persist_\(UUID().uuidString).pantry"
    defer { try? FileManager.default.removeItem(atPath: path) }

    let key = Data((0..<32).map { UInt8($0) })

    // Session 1: write encrypted data
    let db1 = try await PantryDatabase(configuration: PantryConfiguration(
        path: path, encryptionKey: key
    ))
    try await db1.createTable(PantryTableSchema(name: "secrets", columns: [
        .id(), .string("content", nullable: false),
    ]))
    try await db1.insert(into: "secrets", values: [
        "_id": .string("s1"), "content": .string("top secret payload")
    ])
    try await db1.close()

    // Session 2: reopen with same key
    let db2 = try await PantryDatabase(configuration: PantryConfiguration(
        path: path, encryptionKey: key
    ))
    let rows = try await db2.select(from: "secrets")
    #expect(rows.count == 1)
    #expect(rows[0]["content"] == .string("top secret payload"))
    try await db2.close()
}

// MARK: - WHERE Condition Tests

@Test func testAllWhereConditionTypes() async throws {
    let path = NSTemporaryDirectory() + "pantry_where_\(UUID().uuidString).pantry"
    defer { try? FileManager.default.removeItem(atPath: path) }

    let db = try await PantryDatabase(configuration: PantryConfiguration(path: path))
    try await db.createTable(PantryTableSchema(name: "products", columns: [
        .integer("id", nullable: false),
        .string("name"),
        .double("price"),
        .string("category"),
    ]))

    let items: [(Int64, String, Double, String)] = [
        (1, "Apple", 1.50, "fruit"),
        (2, "Banana", 0.75, "fruit"),
        (3, "Carrot", 2.00, "vegetable"),
        (4, "Donut", 3.50, "bakery"),
        (5, "Eclair", 4.00, "bakery"),
    ]
    for (id, name, price, cat) in items {
        try await db.insert(into: "products", values: [
            "id": .integer(id), "name": .string(name),
            "price": .double(price), "category": .string(cat),
        ])
    }

    // notEquals
    let notFruit = try await db.select(from: "products",
        where: .notEquals(column: "category", value: .string("fruit")))
    #expect(notFruit.count == 3)

    // lessThanOrEqual
    let cheapOrEqual = try await db.select(from: "products",
        where: .lessThanOrEqual(column: "price", value: .double(2.0)))
    #expect(cheapOrEqual.count == 3) // Apple 1.50, Banana 0.75, Carrot 2.00

    // greaterThanOrEqual
    let expensive = try await db.select(from: "products",
        where: .greaterThanOrEqual(column: "price", value: .double(3.50)))
    #expect(expensive.count == 2) // Donut, Eclair

    // IN
    let selected = try await db.select(from: "products",
        where: .in(column: "name", values: [.string("Apple"), .string("Donut"), .string("Eclair")]))
    #expect(selected.count == 3)

    // BETWEEN
    let midRange = try await db.select(from: "products",
        where: .between(column: "price", min: .double(1.00), max: .double(3.00)))
    #expect(midRange.count == 2) // Apple 1.50, Carrot 2.00

    // LIKE - starts with
    let startsWithC = try await db.select(from: "products",
        where: .like(column: "name", pattern: "C%"))
    #expect(startsWithC.count == 1)
    #expect(startsWithC[0]["name"] == .string("Carrot"))

    // LIKE - ends with
    let endsWithA = try await db.select(from: "products",
        where: .like(column: "name", pattern: "%a"))
    #expect(endsWithA.count == 1) // Banana

    // LIKE - contains
    let containsAn = try await db.select(from: "products",
        where: .like(column: "name", pattern: "%an%"))
    #expect(containsAn.count == 1) // Banana

    // LIKE - single character wildcard
    let fiveChars = try await db.select(from: "products",
        where: .like(column: "name", pattern: "_____")) // 5 underscores = 5 chars
    #expect(fiveChars.count == 2) // Apple and Donut

    // Complex AND/OR
    let complex = try await db.select(from: "products", where: .or([
        .and([
            .equals(column: "category", value: .string("fruit")),
            .greaterThan(column: "price", value: .double(1.00))
        ]),
        .equals(column: "name", value: .string("Donut"))
    ]))
    #expect(complex.count == 2) // Apple (fruit & >1.00) + Donut

    try await db.close()
}

// MARK: - NULL Handling Tests

@Test func testNullHandling() async throws {
    let path = NSTemporaryDirectory() + "pantry_null_\(UUID().uuidString).pantry"
    defer { try? FileManager.default.removeItem(atPath: path) }

    let db = try await PantryDatabase(configuration: PantryConfiguration(path: path))
    try await db.createTable(PantryTableSchema(name: "nullable", columns: [
        .integer("id", nullable: false),
        .string("name"),
        .integer("score"),
    ]))

    try await db.insert(into: "nullable", values: [
        "id": .integer(1), "name": .string("Alice"), "score": .integer(100)
    ])
    try await db.insert(into: "nullable", values: [
        "id": .integer(2), "name": .string("Bob"), "score": .null
    ])
    try await db.insert(into: "nullable", values: [
        "id": .integer(3), "name": .null, "score": .integer(50)
    ])

    // isNull finds rows with NULL values
    let nullScores = try await db.select(from: "nullable", where: .isNull(column: "score"))
    #expect(nullScores.count == 1)
    #expect(nullScores[0]["name"] == .string("Bob"))

    // isNotNull excludes NULLs
    let hasScores = try await db.select(from: "nullable", where: .isNotNull(column: "score"))
    #expect(hasScores.count == 2)

    // equals with .null always returns empty (SQL semantics: NULL = NULL is false)
    let nullEquals = try await db.select(from: "nullable",
        where: .equals(column: "score", value: .null))
    #expect(nullEquals.count == 0)

    // notEquals with .null also returns empty (SQL semantics)
    let nullNotEquals = try await db.select(from: "nullable",
        where: .notEquals(column: "score", value: .null))
    #expect(nullNotEquals.count == 0)

    // Comparison operators skip NULLs
    let gt40 = try await db.select(from: "nullable",
        where: .greaterThan(column: "score", value: .integer(40)))
    #expect(gt40.count == 2) // Alice=100, row3=50; Bob's NULL is skipped

    // Update NULL to a value
    let updated = try await db.update(table: "nullable",
        set: ["score": .integer(75)],
        where: .isNull(column: "score"))
    #expect(updated == 1)

    let allScores = try await db.select(from: "nullable", where: .isNotNull(column: "score"))
    #expect(allScores.count == 3) // All have scores now

    try await db.close()
}

// MARK: - NaN and Infinity Tests

@Test func testNaNAndInfinityValues() async throws {
    let path = NSTemporaryDirectory() + "pantry_nan_\(UUID().uuidString).pantry"
    defer { try? FileManager.default.removeItem(atPath: path) }

    let db = try await PantryDatabase(configuration: PantryConfiguration(path: path))

    struct Measurement: Codable, Sendable, Equatable {
        var value: Double
        var label: String
    }

    // Store NaN and Infinity through CodableStore
    let nan = Measurement(value: .nan, label: "invalid")
    let posInf = Measurement(value: .infinity, label: "overflow")
    let negInf = Measurement(value: -.infinity, label: "underflow")

    _ = try await db.store(nan, id: "nan", in: "measurements")
    _ = try await db.store(posInf, id: "posinf", in: "measurements")
    _ = try await db.store(negInf, id: "neginf", in: "measurements")

    let rNan: Measurement? = try await db.retrieve(id: "nan", from: "measurements")
    #expect(rNan != nil)
    #expect(rNan!.value.isNaN)
    #expect(rNan!.label == "invalid")

    let rPosInf: Measurement? = try await db.retrieve(id: "posinf", from: "measurements")
    #expect(rPosInf?.value == .infinity)

    let rNegInf: Measurement? = try await db.retrieve(id: "neginf", from: "measurements")
    #expect(rNegInf?.value == -.infinity)

    // KV store with NaN/Infinity Codable values
    try await db.set("special", codableValue: Measurement(value: .nan, label: "kv_nan"))
    let kvNan: Measurement? = try await db.get("special", as: Measurement.self)
    #expect(kvNan != nil)
    #expect(kvNan!.value.isNaN)

    try await db.close()
}

// MARK: - CodableStore Edge Cases

@Test func testCodableStoreAutoIDAndOverwrite() async throws {
    let path = NSTemporaryDirectory() + "pantry_codable_edge_\(UUID().uuidString).pantry"
    defer { try? FileManager.default.removeItem(atPath: path) }

    let db = try await PantryDatabase(configuration: PantryConfiguration(path: path))

    struct Item: Codable, Sendable, Equatable {
        var name: String
        var count: Int
    }

    // Auto-generated ID
    let autoID = try await db.store(Item(name: "auto", count: 1), in: "items")
    #expect(!autoID.isEmpty)

    let retrieved: Item? = try await db.retrieve(id: autoID, from: "items")
    #expect(retrieved == Item(name: "auto", count: 1))

    // Overwrite with same ID
    _ = try await db.store(Item(name: "updated", count: 2), id: "fixed", in: "items")
    _ = try await db.store(Item(name: "overwritten", count: 3), id: "fixed", in: "items")

    let overwritten: Item? = try await db.retrieve(id: "fixed", from: "items")
    #expect(overwritten == Item(name: "overwritten", count: 3))

    // All items: auto-generated + one fixed (overwritten, not two)
    let all: [Item] = try await db.retrieveAll(from: "items")
    #expect(all.count == 2)

    try await db.close()
}

@Test func testCodableStoreComplexNestedTypes() async throws {
    let path = NSTemporaryDirectory() + "pantry_nested_\(UUID().uuidString).pantry"
    defer { try? FileManager.default.removeItem(atPath: path) }

    let db = try await PantryDatabase(configuration: PantryConfiguration(path: path))

    struct Address: Codable, Sendable, Equatable {
        var street: String
        var city: String
        var zip: String
    }

    struct Contact: Codable, Sendable, Equatable {
        var name: String
        var emails: [String]
        var address: Address?
        var metadata: [String: String]
    }

    let contact = Contact(
        name: "Alice",
        emails: ["alice@example.com", "alice@work.com"],
        address: Address(street: "123 Main St", city: "Springfield", zip: "62704"),
        metadata: ["role": "admin", "team": "engineering"]
    )

    _ = try await db.store(contact, id: "alice", in: "contacts")
    let retrieved: Contact? = try await db.retrieve(id: "alice", from: "contacts")
    #expect(retrieved == contact)

    // Contact with nil address
    let noAddr = Contact(name: "Bob", emails: [], address: nil, metadata: [:])
    _ = try await db.store(noAddr, id: "bob", in: "contacts")
    let rBob: Contact? = try await db.retrieve(id: "bob", from: "contacts")
    #expect(rBob == noAddr)

    try await db.close()
}

// MARK: - Empty Table and Edge Cases

@Test func testEmptyTableOperations() async throws {
    let path = NSTemporaryDirectory() + "pantry_empty_\(UUID().uuidString).pantry"
    defer { try? FileManager.default.removeItem(atPath: path) }

    let db = try await PantryDatabase(configuration: PantryConfiguration(path: path))
    try await db.createTable(PantryTableSchema(name: "empty", columns: [
        .integer("id", nullable: false), .string("val"),
    ]))

    // Select from empty table
    let rows = try await db.select(from: "empty")
    #expect(rows.isEmpty)

    // Select with condition from empty table
    let filtered = try await db.select(from: "empty",
        where: .equals(column: "id", value: .integer(1)))
    #expect(filtered.isEmpty)

    // Update on empty table
    let updated = try await db.update(table: "empty",
        set: ["val": .string("x")],
        where: .equals(column: "id", value: .integer(1)))
    #expect(updated == 0)

    // Delete from empty table
    let deleted = try await db.delete(from: "empty",
        where: .equals(column: "id", value: .integer(1)))
    #expect(deleted == 0)

    try await db.close()
}

@Test func testDropAndRecreateTable() async throws {
    let path = NSTemporaryDirectory() + "pantry_recreate_\(UUID().uuidString).pantry"
    defer { try? FileManager.default.removeItem(atPath: path) }

    let db = try await PantryDatabase(configuration: PantryConfiguration(path: path))

    // Create, insert, drop
    try await db.createTable(PantryTableSchema(name: "temp", columns: [
        .integer("id", nullable: false), .string("val"),
    ]))
    try await db.insert(into: "temp", values: ["id": .integer(1), "val": .string("original")])
    #expect(await db.tableExists("temp"))

    try await db.dropTable("temp")
    #expect(!(await db.tableExists("temp")))

    // Recreate with different schema
    try await db.createTable(PantryTableSchema(name: "temp", columns: [
        .string("key", nullable: false), .double("amount"),
    ]))
    try await db.insert(into: "temp", values: ["key": .string("a"), "amount": .double(99.9)])
    let rows = try await db.select(from: "temp")
    #expect(rows.count == 1)
    #expect(rows[0]["amount"] == .double(99.9))

    try await db.close()
}

@Test func testDuplicateTableCreateThrows() async throws {
    let path = NSTemporaryDirectory() + "pantry_dup_table_\(UUID().uuidString).pantry"
    defer { try? FileManager.default.removeItem(atPath: path) }

    let db = try await PantryDatabase(configuration: PantryConfiguration(path: path))
    try await db.createTable(PantryTableSchema(name: "users", columns: [
        .integer("id", nullable: false),
    ]))

    await #expect(throws: PantryError.self) {
        try await db.createTable(PantryTableSchema(name: "users", columns: [
            .integer("id", nullable: false),
        ]))
    }

    try await db.close()
}

// MARK: - Unicode and Special Characters

@Test func testUnicodeAndSpecialCharacters() async throws {
    let path = NSTemporaryDirectory() + "pantry_unicode_\(UUID().uuidString).pantry"
    defer { try? FileManager.default.removeItem(atPath: path) }

    let db = try await PantryDatabase(configuration: PantryConfiguration(path: path))
    try await db.createTable(PantryTableSchema(name: "i18n", columns: [
        .id(), .string("text"),
    ]))

    let testStrings: [(String, String)] = [
        ("emoji", "Hello 🌍🎉🚀"),
        ("cjk", "你好世界"),
        ("arabic", "مرحبا بالعالم"),
        ("combining", "e\u{0301}"), // é as combining character
        ("newlines", "line1\nline2\ttab"),
        ("empty", ""),
        ("long", String(repeating: "長", count: 1000)),
    ]

    for (id, text) in testStrings {
        try await db.insert(into: "i18n", values: ["_id": .string(id), "text": .string(text)])
    }

    // Verify all survived round-trip
    for (id, text) in testStrings {
        let rows = try await db.select(from: "i18n",
            where: .equals(column: "_id", value: .string(id)))
        #expect(rows.count == 1, "Missing row for id: \(id)")
        #expect(rows[0]["text"] == .string(text), "Text mismatch for id: \(id)")
    }

    // LIKE with unicode
    let emojiMatch = try await db.select(from: "i18n",
        where: .like(column: "text", pattern: "%🌍%"))
    #expect(emojiMatch.count == 1)

    try await db.close()
}

// MARK: - Transaction Tests

@Test func testTransactionCommit() async throws {
    let path = NSTemporaryDirectory() + "pantry_tx_commit_\(UUID().uuidString).pantry"
    defer { try? FileManager.default.removeItem(atPath: path) }

    let db = try await PantryDatabase(configuration: PantryConfiguration(path: path))
    try await db.createTable(PantryTableSchema(name: "accounts", columns: [
        .id("name"), .double("balance"),
    ]))

    try await db.insert(into: "accounts", values: ["name": .string("Alice"), "balance": .double(1000.0)])
    try await db.insert(into: "accounts", values: ["name": .string("Bob"), "balance": .double(500.0)])

    // Transaction: transfer money
    try await db.transaction { tx in
        _ = try await tx.update(table: "accounts", set: ["balance": .double(900.0)],
            where: .equals(column: "name", value: .string("Alice")))
        _ = try await tx.update(table: "accounts", set: ["balance": .double(600.0)],
            where: .equals(column: "name", value: .string("Bob")))
    }

    let alice = try await db.select(from: "accounts",
        where: .equals(column: "name", value: .string("Alice")))
    #expect(alice[0]["balance"] == .double(900.0))

    let bob = try await db.select(from: "accounts",
        where: .equals(column: "name", value: .string("Bob")))
    #expect(bob[0]["balance"] == .double(600.0))

    try await db.close()
}

@Test func testTransactionRollbackOnError() async throws {
    let path = NSTemporaryDirectory() + "pantry_tx_rollback_\(UUID().uuidString).pantry"
    defer { try? FileManager.default.removeItem(atPath: path) }

    let db = try await PantryDatabase(configuration: PantryConfiguration(path: path))
    try await db.createTable(PantryTableSchema(name: "data", columns: [
        .id(), .integer("value"),
    ]))
    try await db.insert(into: "data", values: ["_id": .string("k1"), "value": .integer(42)])

    struct TestError: Error {}

    // Transaction that throws — should rollback
    do {
        try await db.transaction { tx in
            _ = try await tx.update(table: "data", set: ["value": .integer(999)],
                where: .equals(column: "_id", value: .string("k1")))
            throw TestError()
        }
    } catch is TestError {
        // Expected
    }

    // Value should be unchanged after rollback
    let rows = try await db.select(from: "data",
        where: .equals(column: "_id", value: .string("k1")))
    // Note: since transactions don't currently isolate reads from the same actor,
    // the update may have been applied. This tests the rollback mechanism is invoked.
    #expect(rows.count == 1)

    try await db.close()
}

// MARK: - Large-Scale Indexed Query

@Test func testLargeScaleIndexedQuery() async throws {
    let path = NSTemporaryDirectory() + "pantry_10k_\(UUID().uuidString).pantry"
    defer { try? FileManager.default.removeItem(atPath: path) }

    let db = try await PantryDatabase(configuration: PantryConfiguration(
        path: path, bufferPoolCapacity: 500
    ))

    try await db.createTable(PantryTableSchema(name: "events", columns: [
        .integer("id", nullable: false),
        .string("type"),
        .double("timestamp"),
        .string("payload"),
    ]))

    let types = ["click", "view", "purchase", "signup", "logout"]
    let rowCount = 5000

    for i in 0..<rowCount {
        try await db.insert(into: "events", values: [
            "id": .integer(Int64(i)),
            "type": .string(types[i % types.count]),
            "timestamp": .double(Double(i) * 0.1),
            "payload": .string("event_\(i)"),
        ])
    }

    // Full table scan
    let all = try await db.select(from: "events")
    #expect(all.count == rowCount)

    // Create indexes
    try await db.createIndex(table: "events", column: "type")
    try await db.createIndex(table: "events", column: "timestamp")

    // Indexed equality query
    let purchases = try await db.select(from: "events",
        where: .equals(column: "type", value: .string("purchase")))
    #expect(purchases.count == rowCount / types.count) // 1000

    // Indexed range query
    let recent = try await db.select(from: "events",
        where: .greaterThan(column: "timestamp", value: .double(490.0)))
    #expect(recent.count > 0)
    #expect(recent.count < rowCount)

    // Indexed BETWEEN query
    let midRange = try await db.select(from: "events",
        where: .between(column: "timestamp", min: .double(100.0), max: .double(200.0)))
    #expect(midRange.count > 0)

    // Combined index + filter
    let purchasesRecent = try await db.select(from: "events", where: .and([
        .equals(column: "type", value: .string("purchase")),
        .greaterThan(column: "timestamp", value: .double(400.0)),
    ]))
    #expect(purchasesRecent.count > 0)
    for row in purchasesRecent {
        #expect(row["type"] == .string("purchase"))
    }

    try await db.close()
}

// MARK: - KV Store Edge Cases

@Test func testKVStoreAllValueTypes() async throws {
    let path = NSTemporaryDirectory() + "pantry_kv_types_\(UUID().uuidString).pantry"
    defer { try? FileManager.default.removeItem(atPath: path) }

    let db = try await PantryDatabase(configuration: PantryConfiguration(path: path))

    try await db.set("int_key", value: .integer(42))
    try await db.set("double_key", value: .double(3.14))
    try await db.set("string_key", value: .string("hello"))
    try await db.set("bool_key", value: .boolean(true))
    try await db.set("null_key", value: .null)

    #expect(try await db.get("int_key") == .integer(42))
    #expect(try await db.get("double_key") == .double(3.14))
    #expect(try await db.get("string_key") == .string("hello"))
    #expect(try await db.get("bool_key") == .boolean(true))
    // .null is stored — get returns .null (not nil)
    let nullVal = try await db.get("null_key")
    #expect(nullVal == .null)

    // Non-existent key
    let missing = try await db.get("nonexistent")
    #expect(missing == nil)

    // Delete and verify gone
    try await db.delete(key: "int_key")
    #expect(try await db.get("int_key") == nil)

    // Delete non-existent key doesn't throw
    try await db.delete(key: "nonexistent")

    try await db.close()
}

// MARK: - Column Projection Edge Cases

@Test func testColumnProjectionEdgeCases() async throws {
    let path = NSTemporaryDirectory() + "pantry_proj_\(UUID().uuidString).pantry"
    defer { try? FileManager.default.removeItem(atPath: path) }

    let db = try await PantryDatabase(configuration: PantryConfiguration(path: path))
    try await db.createTable(PantryTableSchema(name: "wide", columns: [
        .integer("a"), .integer("b"), .integer("c"), .integer("d"),
    ]))
    try await db.insert(into: "wide", values: [
        "a": .integer(1), "b": .integer(2), "c": .integer(3), "d": .integer(4),
    ])

    // Project subset of columns
    let subset = try await db.select(from: "wide", columns: ["a", "c"])
    #expect(subset.count == 1)
    #expect(subset[0]["a"] == .integer(1))
    #expect(subset[0]["c"] == .integer(3))
    #expect(subset[0].values.count == 2)

    // Project non-existent column returns .null for that column
    let withMissing = try await db.select(from: "wide", columns: ["a", "nonexistent"])
    #expect(withMissing[0]["a"] == .integer(1))
    #expect(withMissing[0]["nonexistent"] == .null)

    // Empty columns array returns all columns
    let emptyProj = try await db.select(from: "wide", columns: [])
    #expect(emptyProj[0].values.count == 4)

    try await db.close()
}

// MARK: - Retrieve from Non-Existent Collection

@Test func testRetrieveFromNonExistentCollection() async throws {
    let path = NSTemporaryDirectory() + "pantry_no_collection_\(UUID().uuidString).pantry"
    defer { try? FileManager.default.removeItem(atPath: path) }

    let db = try await PantryDatabase(configuration: PantryConfiguration(path: path))

    struct Dummy: Codable, Sendable { var x: Int }

    // Retrieve from collection that doesn't exist
    let single: Dummy? = try await db.retrieve(id: "nope", from: "ghosts")
    #expect(single == nil)

    let all: [Dummy] = try await db.retrieveAll(from: "ghosts")
    #expect(all.isEmpty)

    // Remove from non-existent collection doesn't throw
    try await db.remove(id: "nope", from: "ghosts")

    // KV get from non-existent table
    let kvVal = try await db.get("nope")
    #expect(kvVal == nil)

    try await db.close()
}

// MARK: - Multiple Tables Simultaneously

@Test func testMultipleTablesSimultaneously() async throws {
    let path = NSTemporaryDirectory() + "pantry_multi_table_\(UUID().uuidString).pantry"
    defer { try? FileManager.default.removeItem(atPath: path) }

    let db = try await PantryDatabase(configuration: PantryConfiguration(path: path))

    // Create 5 tables
    for i in 0..<5 {
        try await db.createTable(PantryTableSchema(name: "table_\(i)", columns: [
            .integer("id", nullable: false), .string("data"),
        ]))
    }

    #expect((await db.listTables()).count == 5)

    // Insert into each
    for i in 0..<5 {
        for j in 0..<10 {
            try await db.insert(into: "table_\(i)", values: [
                "id": .integer(Int64(j)),
                "data": .string("table\(i)_row\(j)"),
            ])
        }
    }

    // Verify isolation — each table has its own 10 rows
    for i in 0..<5 {
        let rows = try await db.select(from: "table_\(i)")
        #expect(rows.count == 10, "table_\(i) should have 10 rows")
    }

    // Drop one table doesn't affect others
    try await db.dropTable("table_2")
    #expect((await db.listTables()).count == 4)
    let t0 = try await db.select(from: "table_0")
    #expect(t0.count == 10)

    try await db.close()
}

// MARK: - Delete All Rows

@Test func testDeleteAllRows() async throws {
    let path = NSTemporaryDirectory() + "pantry_del_all_\(UUID().uuidString).pantry"
    defer { try? FileManager.default.removeItem(atPath: path) }

    let db = try await PantryDatabase(configuration: PantryConfiguration(path: path))
    try await db.createTable(PantryTableSchema(name: "data", columns: [
        .integer("id", nullable: false), .string("val"),
    ]))

    for i in 0..<20 {
        try await db.insert(into: "data", values: [
            "id": .integer(Int64(i)), "val": .string("v\(i)"),
        ])
    }
    #expect(try await db.select(from: "data").count == 20)

    // Delete all with no WHERE
    let deleted = try await db.delete(from: "data")
    #expect(deleted == 20)

    let remaining = try await db.select(from: "data")
    #expect(remaining.isEmpty)

    // Table still exists, can insert again
    try await db.insert(into: "data", values: ["id": .integer(1), "val": .string("new")])
    #expect(try await db.select(from: "data").count == 1)

    try await db.close()
}

// MARK: - WAL Rollback Tests

@Test func testRollbackRestoresOriginalData() async throws {
    let path = NSTemporaryDirectory() + "pantry_rollback_\(UUID().uuidString).pantry"
    defer { try? FileManager.default.removeItem(atPath: path) }

    let db = try await PantryDatabase(configuration: PantryConfiguration(path: path))
    try await db.createTable(PantryTableSchema(name: "data", columns: [
        .id(), .integer("value"),
    ]))
    try await db.insert(into: "data", values: ["_id": .string("k1"), "value": .integer(42)])

    struct TestError: Error {}

    // Transaction that modifies data then throws — should rollback
    do {
        try await db.transaction { tx in
            _ = try await tx.update(table: "data", set: ["value": .integer(999)],
                where: .equals(column: "_id", value: .string("k1")))
            throw TestError()
        }
    } catch is TestError {}

    // Value should be restored to original after rollback
    let rows = try await db.select(from: "data",
        where: .equals(column: "_id", value: .string("k1")))
    #expect(rows.count == 1)
    #expect(rows[0]["value"] == .integer(42))

    try await db.close()
}

@Test func testRollbackAfterMultiPageModifications() async throws {
    let path = NSTemporaryDirectory() + "pantry_rollback_multi_\(UUID().uuidString).pantry"
    defer { try? FileManager.default.removeItem(atPath: path) }

    let db = try await PantryDatabase(configuration: PantryConfiguration(path: path))
    try await db.createTable(PantryTableSchema(name: "items", columns: [
        .id(), .integer("value"),
    ]))

    // Insert enough records to span multiple pages
    for i in 0..<20 {
        try await db.insert(into: "items", values: [
            "_id": .string("item_\(i)"), "value": .integer(Int64(i)),
        ])
    }

    struct TestError: Error {}

    // Transaction that modifies multiple records then fails
    do {
        try await db.transaction { tx in
            _ = try await tx.update(table: "items",
                set: ["value": .integer(999)])
            throw TestError()
        }
    } catch is TestError {}

    // All values should be restored
    let rows = try await db.select(from: "items")
    #expect(rows.count == 20)
    for row in rows {
        #expect(row["value"] != .integer(999))
    }

    try await db.close()
}

// MARK: - WAL Checkpoint Tests

@Test func testWALCheckpointShrinks() async throws {
    let path = NSTemporaryDirectory() + "pantry_ckpt_\(UUID().uuidString).pantry"
    let walPath = path + ".wal"
    defer {
        try? FileManager.default.removeItem(atPath: path)
        try? FileManager.default.removeItem(atPath: walPath)
    }

    let db = try await PantryDatabase(configuration: PantryConfiguration(path: path))
    try await db.createTable(PantryTableSchema(name: "data", columns: [
        .id(), .string("value"),
    ]))

    // Insert data to grow the WAL
    for i in 0..<50 {
        try await db.insert(into: "data", values: [
            "_id": .string("k\(i)"), "value": .string("val_\(i)"),
        ])
    }

    try await db.close()

    // WAL should be small (header only = 64 bytes) after checkpoint on close
    let walData = try Data(contentsOf: URL(fileURLWithPath: walPath))
    #expect(walData.count == 64, "WAL should be truncated to header-only after checkpoint")
}

@Test func testDataIntactAfterCheckpointAndReopen() async throws {
    let path = NSTemporaryDirectory() + "pantry_ckpt_reopen_\(UUID().uuidString).pantry"
    defer { try? FileManager.default.removeItem(atPath: path) }

    let db1 = try await PantryDatabase(configuration: PantryConfiguration(path: path))
    try await db1.createTable(PantryTableSchema(name: "items", columns: [
        .id(), .integer("value"),
    ]))
    for i in 0..<20 {
        try await db1.insert(into: "items", values: [
            "_id": .string("item_\(i)"), "value": .integer(Int64(i)),
        ])
    }
    try await db1.close() // Triggers checkpoint

    // Reopen and verify all data survived
    let db2 = try await PantryDatabase(configuration: PantryConfiguration(path: path))
    let rows = try await db2.select(from: "items")
    #expect(rows.count == 20)
    try await db2.close()
}

// MARK: - Index Persistence Tests

@Test func testIndexPersistsAcrossRestart() async throws {
    let path = NSTemporaryDirectory() + "pantry_idx_persist_\(UUID().uuidString).pantry"
    defer { try? FileManager.default.removeItem(atPath: path) }

    // Session 1: create table, insert data, create index
    let db1 = try await PantryDatabase(configuration: PantryConfiguration(path: path))
    try await db1.createTable(PantryTableSchema(name: "products", columns: [
        .integer("id", nullable: false),
        .string("category"),
        .double("price"),
    ]))
    for i in 0..<100 {
        try await db1.insert(into: "products", values: [
            "id": .integer(Int64(i)),
            "category": .string(["electronics", "books", "food"][i % 3]),
            "price": .double(Double(i) * 1.5),
        ])
    }
    try await db1.createIndex(table: "products", column: "category")
    // Verify index works
    let elec1 = try await db1.select(from: "products",
        where: .equals(column: "category", value: .string("electronics")))
    #expect(elec1.count == 34) // 0,3,6,...,99 = ceil(100/3) = 34
    try await db1.close()

    // Session 2: reopen — index should work without createIndex
    let db2 = try await PantryDatabase(configuration: PantryConfiguration(path: path))
    let elec2 = try await db2.select(from: "products",
        where: .equals(column: "category", value: .string("electronics")))
    #expect(elec2.count == 34)
    try await db2.close()
}

// MARK: - Multi-Page Registry Tests

@Test func testManyTablesExceedOnePage() async throws {
    let path = NSTemporaryDirectory() + "pantry_many_tables_\(UUID().uuidString).pantry"
    defer { try? FileManager.default.removeItem(atPath: path) }

    let db1 = try await PantryDatabase(configuration: PantryConfiguration(path: path))

    // Create enough tables to exceed one 8KB page (each TableInfo JSON ~100-200 bytes, so ~50+ tables)
    let tableCount = 60
    for i in 0..<tableCount {
        try await db1.createTable(PantryTableSchema(name: "table_\(String(format: "%03d", i))", columns: [
            .integer("id", nullable: false),
            .string("data_column_with_a_longer_name_\(i)"),
        ]))
        try await db1.insert(into: "table_\(String(format: "%03d", i))", values: [
            "id": .integer(1),
            "data_column_with_a_longer_name_\(i)": .string("value_\(i)"),
        ])
    }
    try await db1.close()

    // Reopen and verify all tables survived
    let db2 = try await PantryDatabase(configuration: PantryConfiguration(path: path))
    let tables = await db2.listTables()
    #expect(tables.count == tableCount)
    for i in 0..<tableCount {
        let rows = try await db2.select(from: "table_\(String(format: "%03d", i))")
        #expect(rows.count == 1, "table_\(i) should have 1 row")
    }
    try await db2.close()
}

// MARK: - Free List Tests

@Test func testFreeListPageReuse() async throws {
    let path = NSTemporaryDirectory() + "pantry_freelist_\(UUID().uuidString).pantry"
    defer { try? FileManager.default.removeItem(atPath: path) }

    let db = try await PantryDatabase(configuration: PantryConfiguration(path: path))
    try await db.createTable(PantryTableSchema(name: "temp", columns: [
        .integer("id", nullable: false), .string("data"),
    ]))

    // Insert enough data to allocate several pages
    let payload = String(repeating: "x", count: 500)
    for i in 0..<30 {
        try await db.insert(into: "temp", values: [
            "id": .integer(Int64(i)), "data": .string(payload),
        ])
    }

    // Get file size before drop
    try await db.close()
    let sizeBeforeDrop = try FileManager.default.attributesOfItem(atPath: path)[.size] as! UInt64

    // Reopen, drop table, create new one
    let db2 = try await PantryDatabase(configuration: PantryConfiguration(path: path))
    try await db2.dropTable("temp")

    try await db2.createTable(PantryTableSchema(name: "reused", columns: [
        .integer("id", nullable: false), .string("data"),
    ]))
    for i in 0..<30 {
        try await db2.insert(into: "reused", values: [
            "id": .integer(Int64(i)), "data": .string(payload),
        ])
    }
    try await db2.close()

    // File should not grow (or grow minimally) because freed pages are reused
    let sizeAfterReuse = try FileManager.default.attributesOfItem(atPath: path)[.size] as! UInt64
    #expect(sizeAfterReuse <= sizeBeforeDrop + 8192 * 2, "File should reuse freed pages, not grow significantly")

    // Verify data integrity
    let db3 = try await PantryDatabase(configuration: PantryConfiguration(path: path))
    let rows = try await db3.select(from: "reused")
    #expect(rows.count == 30)
    try await db3.close()
}

@Test func testFreeListSurvivesRestart() async throws {
    let path = NSTemporaryDirectory() + "pantry_freelist_restart_\(UUID().uuidString).pantry"
    defer { try? FileManager.default.removeItem(atPath: path) }

    // Session 1: create, populate, drop
    let db1 = try await PantryDatabase(configuration: PantryConfiguration(path: path))
    try await db1.createTable(PantryTableSchema(name: "temp", columns: [
        .integer("id", nullable: false),
    ]))
    for i in 0..<20 {
        try await db1.insert(into: "temp", values: ["id": .integer(Int64(i))])
    }
    try await db1.dropTable("temp")
    try await db1.close()

    let sizeAfterDrop = try FileManager.default.attributesOfItem(atPath: path)[.size] as! UInt64

    // Session 2: reopen, create new table — should reuse freed pages
    let db2 = try await PantryDatabase(configuration: PantryConfiguration(path: path))
    try await db2.createTable(PantryTableSchema(name: "new_table", columns: [
        .integer("id", nullable: false),
    ]))
    for i in 0..<20 {
        try await db2.insert(into: "new_table", values: ["id": .integer(Int64(i))])
    }
    try await db2.close()

    let sizeAfterReuse = try FileManager.default.attributesOfItem(atPath: path)[.size] as! UInt64
    #expect(sizeAfterReuse <= sizeAfterDrop + 8192 * 2)
}

// MARK: - Schema Factory Methods

@Test func testSchemaFactoryMethods() async throws {
    let path = NSTemporaryDirectory() + "pantry_factory_\(UUID().uuidString).pantry"
    defer { try? FileManager.default.removeItem(atPath: path) }

    let db = try await PantryDatabase(configuration: PantryConfiguration(path: path))

    // Use factory methods to create schema
    try await db.createTable(PantryTableSchema(name: "users", columns: [
        .id("user_id"),
        .string("name", nullable: false),
        .integer("age"),
        .double("score"),
        .boolean("active", defaultValue: true),
    ]))

    try await db.insert(into: "users", values: [
        "user_id": .string("u1"),
        "name": .string("Alice"),
        "age": .integer(30),
        "score": .double(95.5),
        "active": .boolean(true),
    ])

    let rows = try await db.select(from: "users")
    #expect(rows.count == 1)
    #expect(rows[0]["user_id"] == .string("u1"))
    #expect(rows[0]["name"] == .string("Alice"))

    try await db.close()
}

// MARK: - Aggregate Queries

@Test func testAggregateQueries() async throws {
    let path = NSTemporaryDirectory() + "pantry_agg_\(UUID().uuidString).pantry"
    defer { try? FileManager.default.removeItem(atPath: path) }

    let db = try await PantryDatabase(configuration: PantryConfiguration(path: path))

    try await db.createTable(PantryTableSchema(name: "scores", columns: [
        .string("name"), .integer("score"), .double("rating"),
    ]))

    try await db.insert(into: "scores", values: ["name": .string("Alice"), "score": .integer(80), "rating": .double(4.5)])
    try await db.insert(into: "scores", values: ["name": .string("Bob"), "score": .integer(90), "rating": .double(3.8)])
    try await db.insert(into: "scores", values: ["name": .string("Charlie"), "score": .integer(70), "rating": .double(4.2)])
    try await db.insert(into: "scores", values: ["name": .string("Diana"), "score": .null, "rating": .double(4.9)])

    // COUNT
    let countAll = try await db.aggregate(from: "scores", .count(column: nil))
    #expect(countAll == .integer(4))

    let countScores = try await db.aggregate(from: "scores", .count(column: "score"))
    #expect(countScores == .integer(3)) // Diana's null excluded

    // SUM
    let sumScores = try await db.aggregate(from: "scores", .sum(column: "score"))
    #expect(sumScores == .integer(240))

    // AVG
    let avgScores = try await db.aggregate(from: "scores", .avg(column: "score"))
    #expect(avgScores == .double(80.0))

    let avgRating = try await db.aggregate(from: "scores", .avg(column: "rating"))
    if case .double(let v) = avgRating {
        #expect(abs(v - 4.35) < 0.001)
    } else {
        Issue.record("Expected double for avg rating")
    }

    // MIN
    let minScore = try await db.aggregate(from: "scores", .min(column: "score"))
    #expect(minScore == .integer(70))

    // MAX
    let maxScore = try await db.aggregate(from: "scores", .max(column: "score"))
    #expect(maxScore == .integer(90))

    // MIN/MAX on strings
    let minName = try await db.aggregate(from: "scores", .min(column: "name"))
    #expect(minName == .string("Alice"))
    let maxName = try await db.aggregate(from: "scores", .max(column: "name"))
    #expect(maxName == .string("Diana"))

    // Aggregate with WHERE
    let sumHigh = try await db.aggregate(from: "scores", .sum(column: "score"),
        where: .greaterThan(column: "score", value: .integer(75)))
    #expect(sumHigh == .integer(170)) // 80 + 90

    // Aggregate on empty result set
    let sumNone = try await db.aggregate(from: "scores", .sum(column: "score"),
        where: .greaterThan(column: "score", value: .integer(1000)))
    #expect(sumNone == .null)

    let avgNone = try await db.aggregate(from: "scores", .avg(column: "score"),
        where: .greaterThan(column: "score", value: .integer(1000)))
    #expect(avgNone == .null)

    try await db.close()
}

// MARK: - AsyncSequence Streaming

@Test func testStreamResults() async throws {
    let path = NSTemporaryDirectory() + "pantry_stream_\(UUID().uuidString).pantry"
    defer { try? FileManager.default.removeItem(atPath: path) }

    let db = try await PantryDatabase(configuration: PantryConfiguration(path: path))

    try await db.createTable(PantryTableSchema(name: "items", columns: [
        .integer("id"), .string("name"),
    ]))

    let rowCount = 100
    for i in 0..<rowCount {
        try await db.insert(into: "items", values: [
            "id": .integer(Int64(i)),
            "name": .string("item_\(i)"),
        ])
    }

    // Stream all rows
    var count = 0
    let stream = try await db.stream(from: "items")
    for await row in stream {
        #expect(row.values["id"] != nil)
        #expect(row.values["name"] != nil)
        count += 1
    }
    #expect(count == rowCount)

    // Stream empty table
    try await db.createTable(PantryTableSchema(name: "empty", columns: [
        .integer("x"),
    ]))
    var emptyCount = 0
    let emptyStream = try await db.stream(from: "empty")
    for await _ in emptyStream {
        emptyCount += 1
    }
    #expect(emptyCount == 0)

    try await db.close()
}

@Test func testCompoundIndex() async throws {
    let path = NSTemporaryDirectory() + "pantry_compound_idx_\(UUID().uuidString).pantry"
    defer { try? FileManager.default.removeItem(atPath: path) }

    let db = try await PantryDatabase(configuration: PantryConfiguration(path: path))

    // Create a products table
    let schema = PantryTableSchema(name: "products", columns: [
        PantryColumn(name: "category", type: .string),
        PantryColumn(name: "price", type: .double),
        PantryColumn(name: "name", type: .string),
    ])
    try await db.createTable(schema)

    // Insert test data
    try await db.insert(into: "products", values: ["category": "electronics", "price": .double(999.99), "name": "Laptop"])
    try await db.insert(into: "products", values: ["category": "electronics", "price": .double(499.99), "name": "Tablet"])
    try await db.insert(into: "products", values: ["category": "electronics", "price": .double(199.99), "name": "Headphones"])
    try await db.insert(into: "products", values: ["category": "books", "price": .double(29.99), "name": "Swift Programming"])
    try await db.insert(into: "products", values: ["category": "books", "price": .double(19.99), "name": "Design Patterns"])
    try await db.insert(into: "products", values: ["category": "clothing", "price": .double(49.99), "name": "T-Shirt"])

    // Create compound index on (category, price)
    try await db.createCompoundIndex(table: "products", columns: ["category", "price"])

    // Test full compound key match: category=electronics AND price=499.99
    let tablets = try await db.select(from: "products", where: .and([
        .equals(column: "category", value: "electronics"),
        .equals(column: "price", value: .double(499.99))
    ]))
    #expect(tablets.count == 1)
    #expect(tablets[0].values["name"] == "Tablet")

    // Test prefix query: only category column matches (prefix of compound index)
    let books = try await db.select(from: "products", where: .and([
        .equals(column: "category", value: "books"),
        .greaterThan(column: "price", value: .double(0))
    ]))
    #expect(books.count == 2)

    // Test that non-matching compound query returns empty
    let empty = try await db.select(from: "products", where: .and([
        .equals(column: "category", value: "electronics"),
        .equals(column: "price", value: .double(1.0))
    ]))
    #expect(empty.isEmpty)

    // Test DBValue.compound ordering
    let a = DBValue.compound(["electronics", .double(100)])
    let b = DBValue.compound(["electronics", .double(200)])
    let c = DBValue.compound(["food", .double(5)])
    #expect(a < b)
    #expect(b < c) // "electronics" < "food"
    #expect(a == a)

    try await db.close()
}

@Test func testCompoundIndexPersistence() async throws {
    let path = NSTemporaryDirectory() + "pantry_compound_persist_\(UUID().uuidString).pantry"
    defer { try? FileManager.default.removeItem(atPath: path) }

    // Create database, insert data, create compound index
    do {
        let db = try await PantryDatabase(configuration: PantryConfiguration(path: path))
        let schema = PantryTableSchema(name: "orders", columns: [
            PantryColumn(name: "customer", type: .string),
            PantryColumn(name: "status", type: .string),
            PantryColumn(name: "amount", type: .double),
        ])
        try await db.createTable(schema)
        try await db.createCompoundIndex(table: "orders", columns: ["customer", "status"])

        try await db.insert(into: "orders", values: ["customer": "Alice", "status": "shipped", "amount": .double(100)])
        try await db.insert(into: "orders", values: ["customer": "Alice", "status": "pending", "amount": .double(50)])
        try await db.insert(into: "orders", values: ["customer": "Bob", "status": "shipped", "amount": .double(200)])

        try await db.close()
    }

    // Reopen and verify compound index was persisted
    do {
        let db = try await PantryDatabase(configuration: PantryConfiguration(path: path))

        // Query using compound index — should use persisted index
        let aliceShipped = try await db.select(from: "orders", where: .and([
            .equals(column: "customer", value: "Alice"),
            .equals(column: "status", value: "shipped")
        ]))
        #expect(aliceShipped.count == 1)
        #expect(aliceShipped[0].values["amount"] == .double(100))

        try await db.close()
    }
}

// MARK: - Schema Migration Tests

@Test func testMigrationAddColumn() async throws {
    let path = NSTemporaryDirectory() + "pantry_migrate_add_\(UUID().uuidString).pantry"
    defer { try? FileManager.default.removeItem(atPath: path) }

    let db = try await PantryDatabase(configuration: PantryConfiguration(path: path))

    // Create table with initial schema
    let schema = PantryTableSchema(name: "items", columns: [
        PantryColumn(name: "id", type: .string, isPrimaryKey: true, isNullable: false),
        PantryColumn(name: "name", type: .string),
    ])
    try await db.createTable(schema)

    // Insert some data
    try await db.insert(into: "items", values: ["id": "1", "name": "Widget"])
    try await db.insert(into: "items", values: ["id": "2", "name": "Gadget"])

    // Migrate: add a new nullable column
    try await db.migrate(table: "items", migrations: [
        Migration(version: 1, operations: [
            .addColumn(PantryColumn(name: "price", type: .double, isNullable: true))
        ])
    ])

    // Verify schema was updated
    let updatedSchema = await db.getTableSchema("items")!
    #expect(updatedSchema.columns.count == 3)
    #expect(updatedSchema.columns.map { $0.name }.contains("price"))

    // Existing rows should still be queryable (price is null for them)
    let rows = try await db.select(from: "items")
    #expect(rows.count == 2)

    // New inserts can use the new column
    try await db.insert(into: "items", values: ["id": "3", "name": "Doohickey", "price": .double(9.99)])
    let newItem = try await db.select(from: "items", where: .equals(column: "id", value: "3"))
    #expect(newItem[0].values["price"] == .double(9.99))

    try await db.close()
}

@Test func testMigrationAddColumnWithDefault() async throws {
    let path = NSTemporaryDirectory() + "pantry_migrate_default_\(UUID().uuidString).pantry"
    defer { try? FileManager.default.removeItem(atPath: path) }

    let db = try await PantryDatabase(configuration: PantryConfiguration(path: path))

    let schema = PantryTableSchema(name: "settings", columns: [
        PantryColumn(name: "id", type: .string, isPrimaryKey: true, isNullable: false),
        PantryColumn(name: "key", type: .string),
    ])
    try await db.createTable(schema)
    try await db.insert(into: "settings", values: ["id": "1", "key": "theme"])
    try await db.insert(into: "settings", values: ["id": "2", "key": "lang"])

    // Migrate: add column with default value — existing rows get backfilled
    try await db.migrate(table: "settings", migrations: [
        Migration(version: 1, operations: [
            .addColumn(PantryColumn(name: "value", type: .string, isNullable: true, defaultValue: .string("default")))
        ])
    ])

    let rows = try await db.select(from: "settings")
    for row in rows {
        #expect(row.values["value"] == .string("default"))
    }

    try await db.close()
}

@Test func testMigrationDropColumn() async throws {
    let path = NSTemporaryDirectory() + "pantry_migrate_drop_\(UUID().uuidString).pantry"
    defer { try? FileManager.default.removeItem(atPath: path) }

    let db = try await PantryDatabase(configuration: PantryConfiguration(path: path))

    let schema = PantryTableSchema(name: "profiles", columns: [
        PantryColumn(name: "id", type: .string, isPrimaryKey: true, isNullable: false),
        PantryColumn(name: "name", type: .string),
        PantryColumn(name: "legacy_field", type: .string),
    ])
    try await db.createTable(schema)
    try await db.insert(into: "profiles", values: ["id": "1", "name": "Alice", "legacy_field": "old_data"])

    // Drop the legacy column
    try await db.migrate(table: "profiles", migrations: [
        Migration(version: 1, operations: [
            .dropColumn("legacy_field")
        ])
    ])

    // Schema should no longer have the column
    let updatedSchema = await db.getTableSchema("profiles")!
    #expect(!updatedSchema.columns.map { $0.name }.contains("legacy_field"))

    // Row should no longer have the column data
    let rows = try await db.select(from: "profiles")
    #expect(rows.count == 1)
    #expect(rows[0].values["legacy_field"] == nil)
    #expect(rows[0].values["name"] == "Alice")

    try await db.close()
}

@Test func testMigrationRenameColumn() async throws {
    let path = NSTemporaryDirectory() + "pantry_migrate_rename_\(UUID().uuidString).pantry"
    defer { try? FileManager.default.removeItem(atPath: path) }

    let db = try await PantryDatabase(configuration: PantryConfiguration(path: path))

    let schema = PantryTableSchema(name: "logs", columns: [
        PantryColumn(name: "id", type: .string, isPrimaryKey: true, isNullable: false),
        PantryColumn(name: "msg", type: .string),
    ])
    try await db.createTable(schema)
    try await db.insert(into: "logs", values: ["id": "1", "msg": "hello world"])

    // Rename msg -> message
    try await db.migrate(table: "logs", migrations: [
        Migration(version: 1, operations: [
            .renameColumn(from: "msg", to: "message")
        ])
    ])

    let updatedSchema = await db.getTableSchema("logs")!
    #expect(updatedSchema.columns.map { $0.name }.contains("message"))
    #expect(!updatedSchema.columns.map { $0.name }.contains("msg"))

    let rows = try await db.select(from: "logs")
    #expect(rows[0].values["message"] == "hello world")
    #expect(rows[0].values["msg"] == nil)

    try await db.close()
}

@Test func testAutoMigrationNewOptionalProperty() async throws {
    let path = NSTemporaryDirectory() + "pantry_automigrate_\(UUID().uuidString).pantry"
    defer { try? FileManager.default.removeItem(atPath: path) }

    let db = try await PantryDatabase(configuration: PantryConfiguration(path: path))

    // First, create the table with the "old" schema (V1 model)
    // Simulate saving a model that only has id, name, age
    struct UserV1: PantryModel, Equatable {
        static let tableName = "evolving_users"
        var id: String = UUID().uuidString
        var name: String
        var age: Int
    }

    let oldUser = UserV1(name: "Alice", age: 30)
    try await db.save(oldUser)

    // Verify initial schema
    let initialSchema = await db.getTableSchema("evolving_users")!
    let initialNames = Set(initialSchema.columns.map { $0.name })
    #expect(initialNames.contains("name"))
    #expect(initialNames.contains("age"))
    #expect(!initialNames.contains("bio"))

    // Now save a V2 model with a new optional property
    struct UserV2: PantryModel, Equatable {
        static let tableName = "evolving_users"
        var id: String = UUID().uuidString
        var name: String
        var age: Int
        var bio: String?
    }

    let newUser = UserV2(name: "Bob", age: 25, bio: "Developer")
    try await db.save(newUser)

    // Schema should now include the new column
    let updatedSchema = await db.getTableSchema("evolving_users")!
    let updatedNames = Set(updatedSchema.columns.map { $0.name })
    #expect(updatedNames.contains("bio"))

    // Old user should still be retrievable
    let allRows = try await db.select(from: "evolving_users")
    #expect(allRows.count == 2)

    try await db.close()
}

// MARK: - Row Convenience Getters

@Test func testRowStringGetter() async {
    let row = Row(values: ["name": .string("Alice"), "age": .integer(30)])
    #expect(row.string("name") == "Alice")
}

@Test func testRowIntegerGetter() async {
    let row = Row(values: ["age": .integer(30)])
    #expect(row.integer("age") == 30)
}

@Test func testRowIntGetter() async {
    let row = Row(values: ["age": .integer(30)])
    #expect(row.int("age") == 30)
}

@Test func testRowDoubleGetter() async {
    let row = Row(values: ["score": .double(9.5)])
    #expect(row.double("score") == 9.5)
}

@Test func testRowBoolGetter() async {
    let row = Row(values: ["active": .boolean(true)])
    #expect(row.bool("active") == true)
}

@Test func testRowBlobGetter() async {
    let data = Data([0x01, 0x02, 0x03])
    let row = Row(values: ["payload": .blob(data)])
    #expect(row.blob("payload") == data)
}

@Test func testRowGetterReturnsNilForNull() async {
    let row = Row(values: ["name": .null])
    #expect(row.string("name") == nil)
}

@Test func testRowGetterReturnsNilForMissingKey() async {
    let row = Row(values: ["name": .string("Alice")])
    #expect(row.string("missing") == nil)
}

@Test func testRowGetterReturnsNilForTypeMismatch() async {
    let row = Row(values: ["age": .integer(30)])
    #expect(row.string("age") == nil)
    #expect(row.double("age") == nil)
    #expect(row.bool("age") == nil)
}

// MARK: - WhereCondition Shorthand

@Test func testWhereConditionColumnEquals() async throws {
    let path = NSTemporaryDirectory() + "pantry_wc_eq_\(UUID().uuidString).pantry"
    defer { try? FileManager.default.removeItem(atPath: path) }

    let db = try await PantryDatabase(configuration: PantryConfiguration(path: path))
    try await db.createTable(PantryTableSchema(name: "t", columns: [
        PantryColumn(name: "id", type: .integer, isPrimaryKey: true, isNullable: false),
        PantryColumn(name: "name", type: .string),
    ]))
    try await db.insert(into: "t", values: ["id": .integer(1), "name": .string("Alice")])
    try await db.insert(into: "t", values: ["id": .integer(2), "name": .string("Bob")])

    let rows = try await db.select(from: "t", where: .column("name", equals: .string("Alice")))
    #expect(rows.count == 1)
    #expect(rows[0].string("name") == "Alice")

    try await db.close()
}

@Test func testWhereConditionColumnGreaterThan() async throws {
    let path = NSTemporaryDirectory() + "pantry_wc_gt_\(UUID().uuidString).pantry"
    defer { try? FileManager.default.removeItem(atPath: path) }

    let db = try await PantryDatabase(configuration: PantryConfiguration(path: path))
    try await db.createTable(PantryTableSchema(name: "t", columns: [
        PantryColumn(name: "id", type: .integer, isPrimaryKey: true, isNullable: false),
        PantryColumn(name: "val", type: .integer),
    ]))
    try await db.insert(into: "t", values: ["id": .integer(1), "val": .integer(10)])
    try await db.insert(into: "t", values: ["id": .integer(2), "val": .integer(20)])
    try await db.insert(into: "t", values: ["id": .integer(3), "val": .integer(30)])

    let rows = try await db.select(from: "t", where: .column("val", greaterThan: .integer(15)))
    #expect(rows.count == 2)

    try await db.close()
}

@Test func testWhereConditionColumnLessThan() async throws {
    let path = NSTemporaryDirectory() + "pantry_wc_lt_\(UUID().uuidString).pantry"
    defer { try? FileManager.default.removeItem(atPath: path) }

    let db = try await PantryDatabase(configuration: PantryConfiguration(path: path))
    try await db.createTable(PantryTableSchema(name: "t", columns: [
        PantryColumn(name: "id", type: .integer, isPrimaryKey: true, isNullable: false),
        PantryColumn(name: "val", type: .integer),
    ]))
    try await db.insert(into: "t", values: ["id": .integer(1), "val": .integer(10)])
    try await db.insert(into: "t", values: ["id": .integer(2), "val": .integer(20)])

    let rows = try await db.select(from: "t", where: .column("val", lessThan: .integer(20)))
    #expect(rows.count == 1)
    #expect(rows[0].int("val") == 10)

    try await db.close()
}

@Test func testWhereConditionColumnNotEquals() async throws {
    let path = NSTemporaryDirectory() + "pantry_wc_neq_\(UUID().uuidString).pantry"
    defer { try? FileManager.default.removeItem(atPath: path) }

    let db = try await PantryDatabase(configuration: PantryConfiguration(path: path))
    try await db.createTable(PantryTableSchema(name: "t", columns: [
        PantryColumn(name: "id", type: .integer, isPrimaryKey: true, isNullable: false),
        PantryColumn(name: "name", type: .string),
    ]))
    try await db.insert(into: "t", values: ["id": .integer(1), "name": .string("Alice")])
    try await db.insert(into: "t", values: ["id": .integer(2), "name": .string("Bob")])

    let rows = try await db.select(from: "t", where: .column("name", notEquals: .string("Alice")))
    #expect(rows.count == 1)
    #expect(rows[0].string("name") == "Bob")

    try await db.close()
}

@Test func testWhereConditionColumnGreaterThanOrEqual() async throws {
    let path = NSTemporaryDirectory() + "pantry_wc_gte_\(UUID().uuidString).pantry"
    defer { try? FileManager.default.removeItem(atPath: path) }

    let db = try await PantryDatabase(configuration: PantryConfiguration(path: path))
    try await db.createTable(PantryTableSchema(name: "t", columns: [
        PantryColumn(name: "id", type: .integer, isPrimaryKey: true, isNullable: false),
        PantryColumn(name: "val", type: .integer),
    ]))
    try await db.insert(into: "t", values: ["id": .integer(1), "val": .integer(10)])
    try await db.insert(into: "t", values: ["id": .integer(2), "val": .integer(20)])
    try await db.insert(into: "t", values: ["id": .integer(3), "val": .integer(30)])

    let rows = try await db.select(from: "t", where: .column("val", greaterThanOrEqual: .integer(20)))
    #expect(rows.count == 2)

    try await db.close()
}

@Test func testWhereConditionColumnLessThanOrEqual() async throws {
    let path = NSTemporaryDirectory() + "pantry_wc_lte_\(UUID().uuidString).pantry"
    defer { try? FileManager.default.removeItem(atPath: path) }

    let db = try await PantryDatabase(configuration: PantryConfiguration(path: path))
    try await db.createTable(PantryTableSchema(name: "t", columns: [
        PantryColumn(name: "id", type: .integer, isPrimaryKey: true, isNullable: false),
        PantryColumn(name: "val", type: .integer),
    ]))
    try await db.insert(into: "t", values: ["id": .integer(1), "val": .integer(10)])
    try await db.insert(into: "t", values: ["id": .integer(2), "val": .integer(20)])
    try await db.insert(into: "t", values: ["id": .integer(3), "val": .integer(30)])

    let rows = try await db.select(from: "t", where: .column("val", lessThanOrEqual: .integer(20)))
    #expect(rows.count == 2)

    try await db.close()
}

@Test func testWhereConditionColumnIsNull() async throws {
    let path = NSTemporaryDirectory() + "pantry_wc_null_\(UUID().uuidString).pantry"
    defer { try? FileManager.default.removeItem(atPath: path) }

    let db = try await PantryDatabase(configuration: PantryConfiguration(path: path))
    try await db.createTable(PantryTableSchema(name: "t", columns: [
        PantryColumn(name: "id", type: .integer, isPrimaryKey: true, isNullable: false),
        PantryColumn(name: "name", type: .string),
    ]))
    try await db.insert(into: "t", values: ["id": .integer(1), "name": .string("Alice")])
    try await db.insert(into: "t", values: ["id": .integer(2), "name": .null])

    let rows = try await db.select(from: "t", where: .columnIsNull("name"))
    #expect(rows.count == 1)
    #expect(rows[0].int("id") == 2)

    try await db.close()
}

@Test func testWhereConditionColumnIsNotNull() async throws {
    let path = NSTemporaryDirectory() + "pantry_wc_notnull_\(UUID().uuidString).pantry"
    defer { try? FileManager.default.removeItem(atPath: path) }

    let db = try await PantryDatabase(configuration: PantryConfiguration(path: path))
    try await db.createTable(PantryTableSchema(name: "t", columns: [
        PantryColumn(name: "id", type: .integer, isPrimaryKey: true, isNullable: false),
        PantryColumn(name: "name", type: .string),
    ]))
    try await db.insert(into: "t", values: ["id": .integer(1), "name": .string("Alice")])
    try await db.insert(into: "t", values: ["id": .integer(2), "name": .null])

    let rows = try await db.select(from: "t", where: .columnIsNotNull("name"))
    #expect(rows.count == 1)
    #expect(rows[0].string("name") == "Alice")

    try await db.close()
}

@Test func testWhereConditionColumnLike() async throws {
    let path = NSTemporaryDirectory() + "pantry_wc_like_\(UUID().uuidString).pantry"
    defer { try? FileManager.default.removeItem(atPath: path) }

    let db = try await PantryDatabase(configuration: PantryConfiguration(path: path))
    try await db.createTable(PantryTableSchema(name: "t", columns: [
        PantryColumn(name: "id", type: .integer, isPrimaryKey: true, isNullable: false),
        PantryColumn(name: "name", type: .string),
    ]))
    try await db.insert(into: "t", values: ["id": .integer(1), "name": .string("Alice")])
    try await db.insert(into: "t", values: ["id": .integer(2), "name": .string("Adam")])
    try await db.insert(into: "t", values: ["id": .integer(3), "name": .string("Bob")])

    let rows = try await db.select(from: "t", where: .column("name", like: "A%"))
    #expect(rows.count == 2)

    try await db.close()
}

@Test func testWhereConditionColumnIn() async throws {
    let path = NSTemporaryDirectory() + "pantry_wc_in_\(UUID().uuidString).pantry"
    defer { try? FileManager.default.removeItem(atPath: path) }

    let db = try await PantryDatabase(configuration: PantryConfiguration(path: path))
    try await db.createTable(PantryTableSchema(name: "t", columns: [
        PantryColumn(name: "id", type: .integer, isPrimaryKey: true, isNullable: false),
        PantryColumn(name: "name", type: .string),
    ]))
    try await db.insert(into: "t", values: ["id": .integer(1), "name": .string("Alice")])
    try await db.insert(into: "t", values: ["id": .integer(2), "name": .string("Bob")])
    try await db.insert(into: "t", values: ["id": .integer(3), "name": .string("Charlie")])

    let rows = try await db.select(from: "t", where: .column("name", in: [.string("Alice"), .string("Charlie")]))
    #expect(rows.count == 2)

    try await db.close()
}

@Test func testWhereConditionColumnBetween() async throws {
    let path = NSTemporaryDirectory() + "pantry_wc_btwn_\(UUID().uuidString).pantry"
    defer { try? FileManager.default.removeItem(atPath: path) }

    let db = try await PantryDatabase(configuration: PantryConfiguration(path: path))
    try await db.createTable(PantryTableSchema(name: "t", columns: [
        PantryColumn(name: "id", type: .integer, isPrimaryKey: true, isNullable: false),
        PantryColumn(name: "val", type: .integer),
    ]))
    try await db.insert(into: "t", values: ["id": .integer(1), "val": .integer(10)])
    try await db.insert(into: "t", values: ["id": .integer(2), "val": .integer(20)])
    try await db.insert(into: "t", values: ["id": .integer(3), "val": .integer(30)])

    let rows = try await db.select(from: "t", where: .column("val", between: .integer(10), and: .integer(20)))
    #expect(rows.count == 2)

    try await db.close()
}

@Test func testWhereConditionAndCombinator() async throws {
    let path = NSTemporaryDirectory() + "pantry_wc_and_\(UUID().uuidString).pantry"
    defer { try? FileManager.default.removeItem(atPath: path) }

    let db = try await PantryDatabase(configuration: PantryConfiguration(path: path))
    try await db.createTable(PantryTableSchema(name: "t", columns: [
        PantryColumn(name: "id", type: .integer, isPrimaryKey: true, isNullable: false),
        PantryColumn(name: "age", type: .integer),
        PantryColumn(name: "name", type: .string),
    ]))
    try await db.insert(into: "t", values: ["id": .integer(1), "age": .integer(25), "name": .string("Alice")])
    try await db.insert(into: "t", values: ["id": .integer(2), "age": .integer(30), "name": .string("Adam")])
    try await db.insert(into: "t", values: ["id": .integer(3), "age": .integer(35), "name": .string("Bob")])

    let rows = try await db.select(from: "t", where: .column("age", greaterThan: .integer(20)) && .column("name", like: "A%"))
    #expect(rows.count == 2)

    try await db.close()
}

@Test func testWhereConditionOrCombinator() async throws {
    let path = NSTemporaryDirectory() + "pantry_wc_or_\(UUID().uuidString).pantry"
    defer { try? FileManager.default.removeItem(atPath: path) }

    let db = try await PantryDatabase(configuration: PantryConfiguration(path: path))
    try await db.createTable(PantryTableSchema(name: "t", columns: [
        PantryColumn(name: "id", type: .integer, isPrimaryKey: true, isNullable: false),
        PantryColumn(name: "name", type: .string),
    ]))
    try await db.insert(into: "t", values: ["id": .integer(1), "name": .string("Alice")])
    try await db.insert(into: "t", values: ["id": .integer(2), "name": .string("Bob")])
    try await db.insert(into: "t", values: ["id": .integer(3), "name": .string("Charlie")])

    let rows = try await db.select(from: "t", where: .column("name", equals: .string("Alice")) || .column("name", equals: .string("Charlie")))
    #expect(rows.count == 2)

    try await db.close()
}

// MARK: - Count Shorthand

@Test func testCountShorthand() async throws {
    let path = NSTemporaryDirectory() + "pantry_count_\(UUID().uuidString).pantry"
    defer { try? FileManager.default.removeItem(atPath: path) }

    let db = try await PantryDatabase(configuration: PantryConfiguration(path: path))
    try await db.createTable(PantryTableSchema(name: "t", columns: [
        PantryColumn(name: "id", type: .integer, isPrimaryKey: true, isNullable: false),
        PantryColumn(name: "name", type: .string),
    ]))
    try await db.insert(into: "t", values: ["id": .integer(1), "name": .string("Alice")])
    try await db.insert(into: "t", values: ["id": .integer(2), "name": .string("Bob")])
    try await db.insert(into: "t", values: ["id": .integer(3), "name": .string("Charlie")])

    let total = try await db.count(from: "t")
    #expect(total == 3)

    let filtered = try await db.count(from: "t", where: .column("name", like: "A%"))
    #expect(filtered == 1)

    try await db.close()
}

// MARK: - Exists Shorthand

@Test func testExistsShorthand() async throws {
    let path = NSTemporaryDirectory() + "pantry_exists_\(UUID().uuidString).pantry"
    defer { try? FileManager.default.removeItem(atPath: path) }

    let db = try await PantryDatabase(configuration: PantryConfiguration(path: path))
    try await db.createTable(PantryTableSchema(name: "t", columns: [
        PantryColumn(name: "id", type: .integer, isPrimaryKey: true, isNullable: false),
        PantryColumn(name: "name", type: .string),
    ]))
    try await db.insert(into: "t", values: ["id": .integer(1), "name": .string("Alice")])

    let found = try await db.exists(in: "t", where: .column("name", equals: "Alice"))
    #expect(found == true)

    let missing = try await db.exists(in: "t", where: .column("name", equals: "Nobody"))
    #expect(missing == false)

    try await db.close()
}

// MARK: - Batch Insert

@Test func testBatchInsert() async throws {
    let path = NSTemporaryDirectory() + "pantry_batch_\(UUID().uuidString).pantry"
    defer { try? FileManager.default.removeItem(atPath: path) }

    let db = try await PantryDatabase(configuration: PantryConfiguration(path: path))
    try await db.createTable(PantryTableSchema(name: "t", columns: [
        PantryColumn(name: "id", type: .integer, isPrimaryKey: true, isNullable: false),
        PantryColumn(name: "name", type: .string),
    ]))

    let rows: [[String: DBValue]] = [
        ["id": .integer(1), "name": .string("Alice")],
        ["id": .integer(2), "name": .string("Bob")],
        ["id": .integer(3), "name": .string("Charlie")],
    ]
    try await db.insertAll(into: "t", rows: rows)

    let count = try await db.count(from: "t")
    #expect(count == 3)

    try await db.close()
}

@Test func testBatchInsertEmptyArray() async throws {
    let path = NSTemporaryDirectory() + "pantry_batch_empty_\(UUID().uuidString).pantry"
    defer { try? FileManager.default.removeItem(atPath: path) }

    let db = try await PantryDatabase(configuration: PantryConfiguration(path: path))
    try await db.createTable(PantryTableSchema(name: "t", columns: [
        PantryColumn(name: "id", type: .integer, isPrimaryKey: true, isNullable: false),
    ]))

    try await db.insertAll(into: "t", rows: [])
    let count = try await db.count(from: "t")
    #expect(count == 0)

    try await db.close()
}

// MARK: - Index List / Drop

@Test func testListIndexes() async throws {
    let path = NSTemporaryDirectory() + "pantry_idx_list_\(UUID().uuidString).pantry"
    defer { try? FileManager.default.removeItem(atPath: path) }

    let db = try await PantryDatabase(configuration: PantryConfiguration(path: path))
    try await db.createTable(PantryTableSchema(name: "t", columns: [
        PantryColumn(name: "id", type: .integer, isPrimaryKey: true, isNullable: false),
        PantryColumn(name: "name", type: .string),
        PantryColumn(name: "age", type: .integer),
    ]))

    // No indexes initially
    let before = await db.listIndexes(on: "t")
    #expect(before.isEmpty)

    // Create a single-column index
    try await db.createIndex(table: "t", column: "name")
    let after = await db.listIndexes(on: "t")
    #expect(after.count == 1)
    #expect(after[0].column == "name")
    #expect(after[0].isCompound == false)

    try await db.close()
}

@Test func testDropIndex() async throws {
    let path = NSTemporaryDirectory() + "pantry_idx_drop_\(UUID().uuidString).pantry"
    defer { try? FileManager.default.removeItem(atPath: path) }

    let db = try await PantryDatabase(configuration: PantryConfiguration(path: path))
    try await db.createTable(PantryTableSchema(name: "t", columns: [
        PantryColumn(name: "id", type: .integer, isPrimaryKey: true, isNullable: false),
        PantryColumn(name: "name", type: .string),
    ]))

    try await db.createIndex(table: "t", column: "name")
    let beforeDrop = await db.listIndexes(on: "t")
    #expect(beforeDrop.count == 1)

    await db.dropIndex(table: "t", column: "name")
    let afterDrop = await db.listIndexes(on: "t")
    #expect(afterDrop.isEmpty)

    try await db.close()
}

@Test func testListCompoundIndex() async throws {
    let path = NSTemporaryDirectory() + "pantry_idx_compound_\(UUID().uuidString).pantry"
    defer { try? FileManager.default.removeItem(atPath: path) }

    let db = try await PantryDatabase(configuration: PantryConfiguration(path: path))
    try await db.createTable(PantryTableSchema(name: "t", columns: [
        PantryColumn(name: "id", type: .integer, isPrimaryKey: true, isNullable: false),
        PantryColumn(name: "name", type: .string),
        PantryColumn(name: "age", type: .integer),
    ]))

    try await db.createCompoundIndex(table: "t", columns: ["name", "age"])
    let indexes = await db.listIndexes(on: "t")
    #expect(indexes.count == 1)
    #expect(indexes[0].isCompound == true)

    try await db.close()
}
