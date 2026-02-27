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
