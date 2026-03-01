import Testing
import Foundation
@testable import PantryCore
@testable import PantryIndex
@testable import PantryQuery

@Test func testQueryExecutorSelect() async throws {
    let path = NSTemporaryDirectory() + "pantry_qe_\(UUID().uuidString).db"
    defer { try? FileManager.default.removeItem(atPath: path) }

    let engine = try await StorageEngine(databasePath: path, bufferPoolCapacity: 100)
    let im = IndexManager(bufferPool: await engine.bufferPoolManager, storageManager: await engine.storageManager)
    let qe = QueryExecutor(storageEngine: engine, indexManager: im)

    // Create table
    let schema = PantryTableSchema(name: "items", columns: [
        PantryColumn(name: "id", type: .integer, isPrimaryKey: true, isNullable: false),
        PantryColumn(name: "name", type: .string),
        PantryColumn(name: "price", type: .double),
    ])
    try await engine.createTable(schema)

    // Insert rows
    try await qe.executeInsert(into: "items", row: Row(values: [
        "id": .integer(1), "name": .string("Widget"), "price": .double(9.99)
    ]))
    try await qe.executeInsert(into: "items", row: Row(values: [
        "id": .integer(2), "name": .string("Gadget"), "price": .double(19.99)
    ]))
    try await qe.executeInsert(into: "items", row: Row(values: [
        "id": .integer(3), "name": .string("Doohickey"), "price": .double(4.99)
    ]))

    // Select all
    let all = try await qe.executeSelect(from: "items")
    #expect(all.count == 3)

    // Select with WHERE
    let expensive = try await qe.executeSelect(
        from: "items",
        where: .greaterThan(column: "price", value: .double(10.0))
    )
    #expect(expensive.count == 1)
    #expect(expensive[0].values["name"] == .string("Gadget"))

    // Column projection
    let names = try await qe.executeSelect(from: "items", columns: ["name"])
    #expect(names[0].values.count == 1)
    #expect(names[0].values["name"] != nil)

    try await engine.close()
}

@Test func testQueryExecutorUpdateDelete() async throws {
    let path = NSTemporaryDirectory() + "pantry_qe_ud_\(UUID().uuidString).db"
    defer { try? FileManager.default.removeItem(atPath: path) }

    let engine = try await StorageEngine(databasePath: path, bufferPoolCapacity: 100)
    let im = IndexManager(bufferPool: await engine.bufferPoolManager, storageManager: await engine.storageManager)
    let qe = QueryExecutor(storageEngine: engine, indexManager: im)

    let schema = PantryTableSchema(name: "data", columns: [
        PantryColumn(name: "key", type: .string, isPrimaryKey: true, isNullable: false),
        PantryColumn(name: "val", type: .integer),
    ])
    try await engine.createTable(schema)

    try await qe.executeInsert(into: "data", row: Row(values: ["key": .string("a"), "val": .integer(1)]))
    try await qe.executeInsert(into: "data", row: Row(values: ["key": .string("b"), "val": .integer(2)]))

    // Update
    let updated = try await qe.executeUpdate(
        table: "data",
        set: ["val": .integer(99)],
        where: .equals(column: "key", value: .string("a"))
    )
    #expect(updated == 1)

    let rows = try await qe.executeSelect(from: "data", where: .equals(column: "key", value: .string("a")))
    #expect(rows.count == 1)
    #expect(rows[0].values["val"] == .integer(99))

    // Delete
    let deleted = try await qe.executeDelete(from: "data", where: .equals(column: "key", value: .string("b")))
    #expect(deleted == 1)

    let remaining = try await qe.executeSelect(from: "data")
    #expect(remaining.count == 1)

    try await engine.close()
}

@Test func testConditionEvaluation() async throws {
    let path = NSTemporaryDirectory() + "pantry_cond_\(UUID().uuidString).db"
    defer { try? FileManager.default.removeItem(atPath: path) }

    let engine = try await StorageEngine(databasePath: path, bufferPoolCapacity: 100)
    let im = IndexManager(bufferPool: await engine.bufferPoolManager, storageManager: await engine.storageManager)
    let qe = QueryExecutor(storageEngine: engine, indexManager: im)

    let schema = PantryTableSchema(name: "test", columns: [
        PantryColumn(name: "x", type: .integer),
        PantryColumn(name: "y", type: .string),
    ])
    try await engine.createTable(schema)

    try await qe.executeInsert(into: "test", row: Row(values: ["x": .integer(10), "y": .string("hello")]))
    try await qe.executeInsert(into: "test", row: Row(values: ["x": .integer(20), "y": .null]))

    // AND condition
    let andResult = try await qe.executeSelect(from: "test", where: .and([
        .greaterThan(column: "x", value: .integer(5)),
        .isNotNull(column: "y")
    ]))
    #expect(andResult.count == 1)

    // OR condition
    let orResult = try await qe.executeSelect(from: "test", where: .or([
        .equals(column: "x", value: .integer(10)),
        .equals(column: "x", value: .integer(20))
    ]))
    #expect(orResult.count == 2)

    // isNull
    let nullResult = try await qe.executeSelect(from: "test", where: .isNull(column: "y"))
    #expect(nullResult.count == 1)

    try await engine.close()
}

// MARK: - LIKE Pattern Matching Edge Cases

@Test func testLikePatternMatching() async throws {
    let path = NSTemporaryDirectory() + "pantry_like_\(UUID().uuidString).db"
    defer { try? FileManager.default.removeItem(atPath: path) }

    let engine = try await StorageEngine(databasePath: path, bufferPoolCapacity: 100)
    let im = IndexManager(bufferPool: await engine.bufferPoolManager, storageManager: await engine.storageManager)
    let qe = QueryExecutor(storageEngine: engine, indexManager: im)

    let schema = PantryTableSchema(name: "strings", columns: [
        PantryColumn(name: "val", type: .string),
    ])
    try await engine.createTable(schema)

    let testData = ["hello", "world", "help", "held", "h", "", "HELLO", "he%lo", "he_lo"]
    for s in testData {
        try await qe.executeInsert(into: "strings", row: Row(values: ["val": .string(s)]))
    }

    // % matches any sequence
    let startsH = try await qe.executeSelect(from: "strings", where: .like(column: "val", pattern: "h%"))
    let startsHVals = Set(startsH.compactMap { row -> String? in
        if case .string(let v) = row["val"] { return v } else { return nil }
    })
    #expect(startsHVals == ["hello", "help", "held", "h", "he%lo", "he_lo"])

    // _ matches single character
    let hXld = try await qe.executeSelect(from: "strings", where: .like(column: "val", pattern: "h_ld"))
    #expect(hXld.count == 1)
    #expect(hXld[0]["val"] == .string("held"))

    // % at both ends = contains
    let containsL = try await qe.executeSelect(from: "strings", where: .like(column: "val", pattern: "%el%"))
    let containsLVals = Set(containsL.compactMap { row -> String? in
        if case .string(let v) = row["val"] { return v } else { return nil }
    })
    #expect(containsLVals.contains("hello"))
    #expect(containsLVals.contains("help"))
    #expect(containsLVals.contains("held"))

    // Exact match (no wildcards)
    let exact = try await qe.executeSelect(from: "strings", where: .like(column: "val", pattern: "hello"))
    #expect(exact.count == 1)
    #expect(exact[0]["val"] == .string("hello"))

    // Just % matches everything
    let all = try await qe.executeSelect(from: "strings", where: .like(column: "val", pattern: "%"))
    #expect(all.count == testData.count)

    // Empty pattern matches only empty string
    let emptyPattern = try await qe.executeSelect(from: "strings", where: .like(column: "val", pattern: ""))
    #expect(emptyPattern.count == 1)
    #expect(emptyPattern[0]["val"] == .string(""))

    try await engine.close()
}

// MARK: - IN Condition

@Test func testInCondition() async throws {
    let path = NSTemporaryDirectory() + "pantry_in_\(UUID().uuidString).db"
    defer { try? FileManager.default.removeItem(atPath: path) }

    let engine = try await StorageEngine(databasePath: path, bufferPoolCapacity: 100)
    let im = IndexManager(bufferPool: await engine.bufferPoolManager, storageManager: await engine.storageManager)
    let qe = QueryExecutor(storageEngine: engine, indexManager: im)

    let schema = PantryTableSchema(name: "items", columns: [
        PantryColumn(name: "id", type: .integer), PantryColumn(name: "name", type: .string),
    ])
    try await engine.createTable(schema)

    for i in 1...10 {
        try await qe.executeInsert(into: "items", row: Row(values: [
            "id": .integer(Int64(i)), "name": .string("item_\(i)")
        ]))
    }

    // IN with subset
    let subset = try await qe.executeSelect(from: "items",
        where: .in(column: "id", values: [.integer(2), .integer(5), .integer(8)]))
    #expect(subset.count == 3)

    // IN with no matches
    let none = try await qe.executeSelect(from: "items",
        where: .in(column: "id", values: [.integer(99), .integer(100)]))
    #expect(none.isEmpty)

    // IN with single value (same as equals)
    let single = try await qe.executeSelect(from: "items",
        where: .in(column: "name", values: [.string("item_3")]))
    #expect(single.count == 1)

    // IN with strings
    let strIn = try await qe.executeSelect(from: "items",
        where: .in(column: "name", values: [.string("item_1"), .string("item_10")]))
    #expect(strIn.count == 2)

    try await engine.close()
}

// MARK: - BETWEEN Condition

@Test func testBetweenCondition() async throws {
    let path = NSTemporaryDirectory() + "pantry_between_\(UUID().uuidString).db"
    defer { try? FileManager.default.removeItem(atPath: path) }

    let engine = try await StorageEngine(databasePath: path, bufferPoolCapacity: 100)
    let im = IndexManager(bufferPool: await engine.bufferPoolManager, storageManager: await engine.storageManager)
    let qe = QueryExecutor(storageEngine: engine, indexManager: im)

    let schema = PantryTableSchema(name: "nums", columns: [
        PantryColumn(name: "val", type: .integer),
    ])
    try await engine.createTable(schema)

    for i in 1...20 {
        try await qe.executeInsert(into: "nums", row: Row(values: ["val": .integer(Int64(i))]))
    }

    // Inclusive range
    let range = try await qe.executeSelect(from: "nums",
        where: .between(column: "val", min: .integer(5), max: .integer(15)))
    #expect(range.count == 11) // 5, 6, ..., 15

    // Single value range (min == max)
    let single = try await qe.executeSelect(from: "nums",
        where: .between(column: "val", min: .integer(10), max: .integer(10)))
    #expect(single.count == 1)

    // No matches
    let none = try await qe.executeSelect(from: "nums",
        where: .between(column: "val", min: .integer(100), max: .integer(200)))
    #expect(none.isEmpty)

    try await engine.close()
}

// MARK: - Complex Nested Conditions

@Test func testComplexNestedConditions() async throws {
    let path = NSTemporaryDirectory() + "pantry_nested_cond_\(UUID().uuidString).db"
    defer { try? FileManager.default.removeItem(atPath: path) }

    let engine = try await StorageEngine(databasePath: path, bufferPoolCapacity: 100)
    let im = IndexManager(bufferPool: await engine.bufferPoolManager, storageManager: await engine.storageManager)
    let qe = QueryExecutor(storageEngine: engine, indexManager: im)

    let schema = PantryTableSchema(name: "people", columns: [
        PantryColumn(name: "name", type: .string),
        PantryColumn(name: "age", type: .integer),
        PantryColumn(name: "city", type: .string),
    ])
    try await engine.createTable(schema)

    let data: [(String, Int64, String)] = [
        ("Alice", 30, "NYC"),
        ("Bob", 25, "LA"),
        ("Charlie", 35, "NYC"),
        ("Diana", 28, "Chicago"),
        ("Eve", 30, "LA"),
    ]
    for (name, age, city) in data {
        try await qe.executeInsert(into: "people", row: Row(values: [
            "name": .string(name), "age": .integer(age), "city": .string(city),
        ]))
    }

    // (city = NYC AND age >= 30) OR (city = LA AND age < 30)
    let result = try await qe.executeSelect(from: "people", where: .or([
        .and([
            .equals(column: "city", value: .string("NYC")),
            .greaterThanOrEqual(column: "age", value: .integer(30)),
        ]),
        .and([
            .equals(column: "city", value: .string("LA")),
            .lessThan(column: "age", value: .integer(30)),
        ]),
    ]))

    let names = Set(result.compactMap { row -> String? in
        if case .string(let v) = row["name"] { return v } else { return nil }
    })
    #expect(names == ["Alice", "Charlie", "Bob"])

    // Nested AND within AND
    let doubleAnd = try await qe.executeSelect(from: "people", where: .and([
        .and([
            .greaterThanOrEqual(column: "age", value: .integer(28)),
            .lessThanOrEqual(column: "age", value: .integer(32)),
        ]),
        .notEquals(column: "city", value: .string("Chicago")),
    ]))
    let doubleAndNames = Set(doubleAnd.compactMap { row -> String? in
        if case .string(let v) = row["name"] { return v } else { return nil }
    })
    #expect(doubleAndNames == ["Alice", "Eve"])

    try await engine.close()
}

// MARK: - NULL Semantics in All Operators

@Test func testNullSemanticsComprehensive() async throws {
    let path = NSTemporaryDirectory() + "pantry_null_sem_\(UUID().uuidString).db"
    defer { try? FileManager.default.removeItem(atPath: path) }

    let engine = try await StorageEngine(databasePath: path, bufferPoolCapacity: 100)
    let im = IndexManager(bufferPool: await engine.bufferPoolManager, storageManager: await engine.storageManager)
    let qe = QueryExecutor(storageEngine: engine, indexManager: im)

    let schema = PantryTableSchema(name: "nulltest", columns: [
        PantryColumn(name: "a", type: .integer),
    ])
    try await engine.createTable(schema)

    try await qe.executeInsert(into: "nulltest", row: Row(values: ["a": .integer(10)]))
    try await qe.executeInsert(into: "nulltest", row: Row(values: ["a": .null]))
    try await qe.executeInsert(into: "nulltest", row: Row(values: ["a": .integer(20)]))

    // equals with NULL → empty (SQL: NULL = NULL is false)
    #expect(try await qe.executeSelect(from: "nulltest",
        where: .equals(column: "a", value: .null)).isEmpty)

    // notEquals with NULL → empty
    #expect(try await qe.executeSelect(from: "nulltest",
        where: .notEquals(column: "a", value: .null)).isEmpty)

    // lessThan with NULL value → empty
    #expect(try await qe.executeSelect(from: "nulltest",
        where: .lessThan(column: "a", value: .null)).isEmpty)

    // greaterThan with NULL value → empty
    #expect(try await qe.executeSelect(from: "nulltest",
        where: .greaterThan(column: "a", value: .null)).isEmpty)

    // Comparison operators skip rows with NULL column values
    let gt5 = try await qe.executeSelect(from: "nulltest",
        where: .greaterThan(column: "a", value: .integer(5)))
    #expect(gt5.count == 2) // 10, 20 — NULL is skipped

    // IN with NULL row value → not matched
    let inResult = try await qe.executeSelect(from: "nulltest",
        where: .in(column: "a", values: [.integer(10), .integer(20)]))
    #expect(inResult.count == 2) // NULL row excluded

    // BETWEEN with NULL → skipped
    let betweenResult = try await qe.executeSelect(from: "nulltest",
        where: .between(column: "a", min: .integer(0), max: .integer(100)))
    #expect(betweenResult.count == 2) // NULL row excluded

    // isNull / isNotNull are the only way to find NULLs
    #expect(try await qe.executeSelect(from: "nulltest",
        where: .isNull(column: "a")).count == 1)
    #expect(try await qe.executeSelect(from: "nulltest",
        where: .isNotNull(column: "a")).count == 2)

    try await engine.close()
}

// MARK: - Update and Delete Edge Cases

@Test func testUpdateNoMatchingRows() async throws {
    let path = NSTemporaryDirectory() + "pantry_upd_none_\(UUID().uuidString).db"
    defer { try? FileManager.default.removeItem(atPath: path) }

    let engine = try await StorageEngine(databasePath: path, bufferPoolCapacity: 100)
    let im = IndexManager(bufferPool: await engine.bufferPoolManager, storageManager: await engine.storageManager)
    let qe = QueryExecutor(storageEngine: engine, indexManager: im)

    let schema = PantryTableSchema(name: "data", columns: [
        PantryColumn(name: "id", type: .integer), PantryColumn(name: "v", type: .string),
    ])
    try await engine.createTable(schema)
    try await qe.executeInsert(into: "data", row: Row(values: ["id": .integer(1), "v": .string("a")]))

    // Update with non-matching condition
    let updated = try await qe.executeUpdate(
        table: "data", set: ["v": .string("z")],
        where: .equals(column: "id", value: .integer(999)))
    #expect(updated == 0)

    // Original unchanged
    let rows = try await qe.executeSelect(from: "data")
    #expect(rows[0]["v"] == .string("a"))

    try await engine.close()
}

@Test func testDeleteNoMatchingRows() async throws {
    let path = NSTemporaryDirectory() + "pantry_del_none_\(UUID().uuidString).db"
    defer { try? FileManager.default.removeItem(atPath: path) }

    let engine = try await StorageEngine(databasePath: path, bufferPoolCapacity: 100)
    let im = IndexManager(bufferPool: await engine.bufferPoolManager, storageManager: await engine.storageManager)
    let qe = QueryExecutor(storageEngine: engine, indexManager: im)

    let schema = PantryTableSchema(name: "data", columns: [
        PantryColumn(name: "id", type: .integer),
    ])
    try await engine.createTable(schema)
    try await qe.executeInsert(into: "data", row: Row(values: ["id": .integer(1)]))

    let deleted = try await qe.executeDelete(
        from: "data", where: .equals(column: "id", value: .integer(999)))
    #expect(deleted == 0)
    #expect(try await qe.executeSelect(from: "data").count == 1)

    try await engine.close()
}

@Test func testSelectFromEmptyTable() async throws {
    let path = NSTemporaryDirectory() + "pantry_empty_sel_\(UUID().uuidString).db"
    defer { try? FileManager.default.removeItem(atPath: path) }

    let engine = try await StorageEngine(databasePath: path, bufferPoolCapacity: 100)
    let im = IndexManager(bufferPool: await engine.bufferPoolManager, storageManager: await engine.storageManager)
    let qe = QueryExecutor(storageEngine: engine, indexManager: im)

    let schema = PantryTableSchema(name: "empty", columns: [
        PantryColumn(name: "x", type: .integer),
    ])
    try await engine.createTable(schema)

    // All query types on empty table
    #expect(try await qe.executeSelect(from: "empty").isEmpty)
    #expect(try await qe.executeSelect(from: "empty", where: .equals(column: "x", value: .integer(1))).isEmpty)
    #expect(try await qe.executeSelect(from: "empty", columns: ["x"]).isEmpty)
    #expect(try await qe.executeUpdate(table: "empty", set: ["x": .integer(1)], where: nil) == 0)
    #expect(try await qe.executeDelete(from: "empty", where: nil) == 0)

    try await engine.close()
}

// MARK: - Update All Rows (no WHERE)

@Test func testUpdateAllRows() async throws {
    let path = NSTemporaryDirectory() + "pantry_upd_all_\(UUID().uuidString).db"
    defer { try? FileManager.default.removeItem(atPath: path) }

    let engine = try await StorageEngine(databasePath: path, bufferPoolCapacity: 100)
    let im = IndexManager(bufferPool: await engine.bufferPoolManager, storageManager: await engine.storageManager)
    let qe = QueryExecutor(storageEngine: engine, indexManager: im)

    let schema = PantryTableSchema(name: "data", columns: [
        PantryColumn(name: "id", type: .integer), PantryColumn(name: "status", type: .string),
    ])
    try await engine.createTable(schema)

    for i in 1...5 {
        try await qe.executeInsert(into: "data", row: Row(values: [
            "id": .integer(Int64(i)), "status": .string("pending"),
        ]))
    }

    // Update all with no WHERE
    let updated = try await qe.executeUpdate(table: "data", set: ["status": .string("done")], where: nil)
    #expect(updated == 5)

    let rows = try await qe.executeSelect(from: "data")
    for row in rows {
        #expect(row["status"] == .string("done"))
    }

    try await engine.close()
}
