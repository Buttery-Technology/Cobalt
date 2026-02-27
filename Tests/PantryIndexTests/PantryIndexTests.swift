import Testing
import Foundation
@testable import PantryCore
@testable import PantryIndex

@Test func testBloomFilter() {
    var filter = BloomFilter(expectedElements: 100)
    filter.add("apple")
    filter.add("banana")

    #expect(filter.contains("apple"))
    #expect(filter.contains("banana"))
    #expect(!filter.contains("cherry")) // May false-positive, but unlikely with 100-element sizing and 2 elements
}

@Test func testBloomFilterNoFalseNegatives() {
    var filter = BloomFilter(expectedElements: 1000)

    let items = (0..<100).map { "item_\($0)" }
    for item in items {
        filter.add(item)
    }

    // No false negatives: all added items must return true
    for item in items {
        #expect(filter.contains(item))
    }
}

@Test func testBTreeInsertAndSearch() async throws {
    let path = NSTemporaryDirectory() + "pantry_btree_test_\(UUID().uuidString).db"
    defer { try? FileManager.default.removeItem(atPath: path) }

    let sm = try StorageManager(databasePath: path)
    let bp = BufferPoolManager(capacity: 100, storageManager: sm)
    let nodeStore = PageBackedNodeStore(bufferPool: bp, storageManager: sm)
    let btree = BTree(order: 4, nodeStore: nodeStore)

    // Insert some entries
    try await btree.insert(key: .integer(10), row: Row(values: ["val": .string("ten")]))
    try await btree.insert(key: .integer(20), row: Row(values: ["val": .string("twenty")]))
    try await btree.insert(key: .integer(5), row: Row(values: ["val": .string("five")]))

    // Point query
    let result = try await btree.search(key: .integer(10))
    #expect(result.count == 1)
    #expect(result[0].values["val"] == .string("ten"))

    // Miss
    let miss = try await btree.search(key: .integer(99))
    #expect(miss.isEmpty)

    try await sm.close()
}

@Test func testBTreeRangeQuery() async throws {
    let path = NSTemporaryDirectory() + "pantry_btree_range_\(UUID().uuidString).db"
    defer { try? FileManager.default.removeItem(atPath: path) }

    let sm = try StorageManager(databasePath: path)
    let bp = BufferPoolManager(capacity: 100, storageManager: sm)
    let nodeStore = PageBackedNodeStore(bufferPool: bp, storageManager: sm)
    let btree = BTree(order: 4, nodeStore: nodeStore)

    for i in stride(from: 0, to: 50, by: 5) {
        try await btree.insert(key: .integer(Int64(i)), row: Row(values: ["n": .integer(Int64(i))]))
    }

    let range = try await btree.searchRange(from: .integer(10), to: .integer(30))
    #expect(range.count >= 4) // 10, 15, 20, 25, 30
    #expect(range.count <= 6)

    try await sm.close()
}

@Test func testBTreeDelete() async throws {
    let path = NSTemporaryDirectory() + "pantry_btree_del_\(UUID().uuidString).db"
    defer { try? FileManager.default.removeItem(atPath: path) }

    let sm = try StorageManager(databasePath: path)
    let bp = BufferPoolManager(capacity: 100, storageManager: sm)
    let nodeStore = PageBackedNodeStore(bufferPool: bp, storageManager: sm)
    let btree = BTree(order: 4, nodeStore: nodeStore)

    try await btree.insert(key: .integer(1), row: Row(values: ["v": .string("one")]))
    try await btree.insert(key: .integer(2), row: Row(values: ["v": .string("two")]))
    try await btree.insert(key: .integer(3), row: Row(values: ["v": .string("three")]))

    try await btree.delete(key: .integer(2))
    let result = try await btree.search(key: .integer(2))
    #expect(result.isEmpty)

    // Other keys still present
    #expect(try await btree.search(key: .integer(1)).count == 1)
    #expect(try await btree.search(key: .integer(3)).count == 1)

    try await sm.close()
}

@Test func testColumnIndex() async throws {
    let path = NSTemporaryDirectory() + "pantry_colidx_\(UUID().uuidString).db"
    defer { try? FileManager.default.removeItem(atPath: path) }

    let sm = try StorageManager(databasePath: path)
    let bp = BufferPoolManager(capacity: 100, storageManager: sm)
    let nodeStore = PageBackedNodeStore(bufferPool: bp, storageManager: sm)
    let btree = BTree(order: 64, nodeStore: nodeStore)
    let idx = ColumnIndex(tableName: "users", columnName: "age", btree: btree)

    try await idx.insert(key: .integer(25), row: Row(values: ["name": .string("Alice"), "age": .integer(25)]))
    try await idx.insert(key: .integer(30), row: Row(values: ["name": .string("Bob"), "age": .integer(30)]))

    let result = try await idx.search(key: .integer(25))
    #expect(result != nil)
    #expect(result!.count == 1)

    // Bloom filter negative
    let miss = try await idx.search(key: .integer(99))
    #expect(miss != nil)
    #expect(miss!.isEmpty)

    try await sm.close()
}
