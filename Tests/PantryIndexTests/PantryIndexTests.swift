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
    let idx = ColumnIndex(tableName: "users", columnName: "age", btree: btree, nodeStore: nodeStore)

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

// MARK: - B-Tree Edge Cases

@Test func testBTreeDuplicateKeys() async throws {
    let path = NSTemporaryDirectory() + "pantry_btree_dup_\(UUID().uuidString).db"
    defer { try? FileManager.default.removeItem(atPath: path) }

    let sm = try StorageManager(databasePath: path)
    let bp = BufferPoolManager(capacity: 100, storageManager: sm)
    let nodeStore = PageBackedNodeStore(bufferPool: bp, storageManager: sm)
    let btree = BTree(order: 4, nodeStore: nodeStore)

    // Insert multiple rows with the same key
    try await btree.insert(key: .integer(10), row: Row(values: ["v": .string("a")]))
    try await btree.insert(key: .integer(10), row: Row(values: ["v": .string("b")]))
    try await btree.insert(key: .integer(10), row: Row(values: ["v": .string("c")]))

    let results = try await btree.search(key: .integer(10))
    #expect(results.count == 3)

    let vals = Set(results.compactMap { $0["v"] })
    #expect(vals.contains(.string("a")))
    #expect(vals.contains(.string("b")))
    #expect(vals.contains(.string("c")))

    try await sm.close()
}

@Test func testBTreeDeleteAllThenReinsert() async throws {
    let path = NSTemporaryDirectory() + "pantry_btree_empty_\(UUID().uuidString).db"
    defer { try? FileManager.default.removeItem(atPath: path) }

    let sm = try StorageManager(databasePath: path)
    let bp = BufferPoolManager(capacity: 100, storageManager: sm)
    let nodeStore = PageBackedNodeStore(bufferPool: bp, storageManager: sm)
    let btree = BTree(order: 4, nodeStore: nodeStore)

    // Insert and delete all
    for i in 1...10 {
        try await btree.insert(key: .integer(Int64(i)), row: Row(values: ["n": .integer(Int64(i))]))
    }
    for i in 1...10 {
        try await btree.delete(key: .integer(Int64(i)))
    }

    // Tree should be empty
    for i in 1...10 {
        let r = try await btree.search(key: .integer(Int64(i)))
        #expect(r.isEmpty, "Key \(i) should be deleted")
    }

    // Reinsert should work fine
    try await btree.insert(key: .integer(42), row: Row(values: ["n": .integer(42)]))
    let result = try await btree.search(key: .integer(42))
    #expect(result.count == 1)
    #expect(result[0]["n"] == .integer(42))

    try await sm.close()
}

@Test func testBTreeLargeDatasetWithSplits() async throws {
    let path = NSTemporaryDirectory() + "pantry_btree_large_\(UUID().uuidString).db"
    defer { try? FileManager.default.removeItem(atPath: path) }

    let sm = try StorageManager(databasePath: path)
    let bp = BufferPoolManager(capacity: 200, storageManager: sm)
    let nodeStore = PageBackedNodeStore(bufferPool: bp, storageManager: sm)
    let btree = BTree(order: 4, nodeStore: nodeStore) // Small order = many splits

    let count = 500

    // Insert in random-ish order to exercise splits
    let keys = (0..<count).map { Int64($0 * 7 % count) } // pseudo-shuffle
    for key in keys {
        try await btree.insert(key: .integer(key), row: Row(values: ["k": .integer(key)]))
    }

    // Verify all entries are findable
    for i in 0..<count {
        let result = try await btree.search(key: .integer(Int64(i)))
        #expect(result.count == 1, "Missing key \(i)")
    }

    // Range query should return sorted results
    let range = try await btree.searchRange(from: .integer(100), to: .integer(200))
    #expect(range.count == 101) // 100...200 inclusive
    let rangeKeys = range.compactMap { row -> Int64? in
        if case .integer(let v) = row["k"] { return v }
        return nil
    }
    #expect(rangeKeys == rangeKeys.sorted())

    try await sm.close()
}

@Test func testBTreeStringKeys() async throws {
    let path = NSTemporaryDirectory() + "pantry_btree_str_\(UUID().uuidString).db"
    defer { try? FileManager.default.removeItem(atPath: path) }

    let sm = try StorageManager(databasePath: path)
    let bp = BufferPoolManager(capacity: 100, storageManager: sm)
    let nodeStore = PageBackedNodeStore(bufferPool: bp, storageManager: sm)
    let btree = BTree(order: 4, nodeStore: nodeStore)

    let words = ["banana", "apple", "cherry", "date", "elderberry", "fig", "grape"]
    for word in words {
        try await btree.insert(key: .string(word), row: Row(values: ["w": .string(word)]))
    }

    // Point lookup
    let result = try await btree.search(key: .string("cherry"))
    #expect(result.count == 1)
    #expect(result[0]["w"] == .string("cherry"))

    // Range query on strings
    let range = try await btree.searchRange(from: .string("b"), to: .string("d"))
    let found = range.compactMap { row -> String? in
        if case .string(let v) = row["w"] { return v }
        return nil
    }
    #expect(found.contains("banana"))
    #expect(found.contains("cherry"))

    // Miss
    let miss = try await btree.search(key: .string("zebra"))
    #expect(miss.isEmpty)

    try await sm.close()
}

@Test func testBTreeDeleteFromMiddle() async throws {
    let path = NSTemporaryDirectory() + "pantry_btree_delmid_\(UUID().uuidString).db"
    defer { try? FileManager.default.removeItem(atPath: path) }

    let sm = try StorageManager(databasePath: path)
    let bp = BufferPoolManager(capacity: 100, storageManager: sm)
    let nodeStore = PageBackedNodeStore(bufferPool: bp, storageManager: sm)
    let btree = BTree(order: 4, nodeStore: nodeStore)

    // Insert sequential keys
    for i in 1...20 {
        try await btree.insert(key: .integer(Int64(i)), row: Row(values: ["n": .integer(Int64(i))]))
    }

    // Delete from the middle (forces borrow/merge operations)
    for i in [10, 5, 15, 8, 12, 18, 3] {
        try await btree.delete(key: .integer(Int64(i)))
    }

    // Verify deleted keys are gone
    for i in [10, 5, 15, 8, 12, 18, 3] {
        let r = try await btree.search(key: .integer(Int64(i)))
        #expect(r.isEmpty, "Key \(i) should be deleted")
    }

    // Verify remaining keys are intact
    let expected = Set<Int64>([1, 2, 4, 6, 7, 9, 11, 13, 14, 16, 17, 19, 20])
    for key in expected {
        let r = try await btree.search(key: .integer(key))
        #expect(r.count == 1, "Key \(key) should still exist")
    }

    try await sm.close()
}

// MARK: - Bloom Filter Edge Cases

@Test func testBloomFilterEmpty() {
    let filter = BloomFilter(expectedElements: 100)
    #expect(!filter.contains("anything"))
    #expect(!filter.contains(""))
}

@Test func testBloomFilterUnicode() {
    var filter = BloomFilter(expectedElements: 100)
    filter.add("🌍")
    filter.add("你好")
    filter.add("")

    #expect(filter.contains("🌍"))
    #expect(filter.contains("你好"))
    #expect(filter.contains(""))
}

@Test func testBloomFilterHighLoad() {
    var filter = BloomFilter(expectedElements: 1000, falsePositiveRate: 0.01)

    // Add 1000 elements
    for i in 0..<1000 {
        filter.add("element_\(i)")
    }

    // Zero false negatives
    for i in 0..<1000 {
        #expect(filter.contains("element_\(i)"), "False negative at \(i)")
    }

    // Count false positives on 1000 non-existent elements
    var falsePositives = 0
    for i in 1000..<2000 {
        if filter.contains("element_\(i)") {
            falsePositives += 1
        }
    }

    // Should be roughly ≤1% false positive rate (allow some slack)
    #expect(falsePositives < 50, "Too many false positives: \(falsePositives)")
}

// MARK: - IndexManager Edge Cases

@Test func testIndexManagerMultipleColumns() async throws {
    let path = NSTemporaryDirectory() + "pantry_im_multi_\(UUID().uuidString).db"
    defer { try? FileManager.default.removeItem(atPath: path) }

    let sm = try StorageManager(databasePath: path)
    let bp = BufferPoolManager(capacity: 100, storageManager: sm)
    let im = IndexManager(bufferPool: bp, storageManager: sm)

    // Create indexes on two columns of the same table
    let nameIdx = try await im.createIndex(tableName: "users", columnName: "name")
    let ageIdx = try await im.createIndex(tableName: "users", columnName: "age")

    let row1 = Row(values: ["name": .string("Alice"), "age": .integer(30)])
    let row2 = Row(values: ["name": .string("Bob"), "age": .integer(25)])

    try await nameIdx.insert(key: .string("Alice"), row: row1)
    try await nameIdx.insert(key: .string("Bob"), row: row2)
    try await ageIdx.insert(key: .integer(30), row: row1)
    try await ageIdx.insert(key: .integer(25), row: row2)

    // Query by name
    let byName = try await im.attemptIndexLookup(
        tableName: "users",
        condition: .equals(column: "name", value: .string("Alice")))
    #expect(byName != nil)
    #expect(byName!.count == 1)

    // Query by age
    let byAge = try await im.attemptIndexLookup(
        tableName: "users",
        condition: .greaterThanOrEqual(column: "age", value: .integer(25)))
    #expect(byAge != nil)
    #expect(byAge!.count == 2)

    // Query non-indexed column returns nil (fallback to scan)
    let byEmail = try await im.attemptIndexLookup(
        tableName: "users",
        condition: .equals(column: "email", value: .string("test@test.com")))
    #expect(byEmail == nil)

    // getIndexes returns all for table
    let indexes = await im.getIndexes(tableName: "users")
    #expect(indexes.count == 2)
    #expect(indexes["name"] != nil)
    #expect(indexes["age"] != nil)

    try await sm.close()
}

@Test func testIndexManagerNullLookup() async throws {
    let path = NSTemporaryDirectory() + "pantry_im_null_\(UUID().uuidString).db"
    defer { try? FileManager.default.removeItem(atPath: path) }

    let sm = try StorageManager(databasePath: path)
    let bp = BufferPoolManager(capacity: 100, storageManager: sm)
    let im = IndexManager(bufferPool: bp, storageManager: sm)

    _ = try await im.createIndex(tableName: "t", columnName: "col")

    // .equals with .null should return empty (SQL NULL semantics)
    let result = try await im.attemptIndexLookup(
        tableName: "t",
        condition: .equals(column: "col", value: .null))
    #expect(result != nil)
    #expect(result!.isEmpty)

    try await sm.close()
}

@Test func testIndexManagerRemoveIndexes() async throws {
    let path = NSTemporaryDirectory() + "pantry_im_remove_\(UUID().uuidString).db"
    defer { try? FileManager.default.removeItem(atPath: path) }

    let sm = try StorageManager(databasePath: path)
    let bp = BufferPoolManager(capacity: 100, storageManager: sm)
    let im = IndexManager(bufferPool: bp, storageManager: sm)

    _ = try await im.createIndex(tableName: "t", columnName: "a")
    _ = try await im.createIndex(tableName: "t", columnName: "b")
    #expect(await im.getIndexes(tableName: "t").count == 2)

    await im.removeIndexes(tableName: "t")
    #expect(await im.getIndexes(tableName: "t").isEmpty)

    // Lookup on removed index returns nil
    let result = try await im.attemptIndexLookup(
        tableName: "t",
        condition: .equals(column: "a", value: .integer(1)))
    #expect(result == nil)

    try await sm.close()
}

@Test func testColumnIndexRangeQuery() async throws {
    let path = NSTemporaryDirectory() + "pantry_colidx_range_\(UUID().uuidString).db"
    defer { try? FileManager.default.removeItem(atPath: path) }

    let sm = try StorageManager(databasePath: path)
    let bp = BufferPoolManager(capacity: 100, storageManager: sm)
    let nodeStore = PageBackedNodeStore(bufferPool: bp, storageManager: sm)
    let btree = BTree(order: 10, nodeStore: nodeStore)
    let idx = ColumnIndex(tableName: "sales", columnName: "amount", btree: btree, nodeStore: nodeStore)

    for i in 1...100 {
        try await idx.insert(
            key: .double(Double(i) * 10.0),
            row: Row(values: ["id": .integer(Int64(i)), "amount": .double(Double(i) * 10.0)])
        )
    }

    // Range: 200.0 to 500.0
    let range = try await idx.searchRange(from: .double(200.0), to: .double(500.0))
    #expect(range.count == 31) // 20, 21, ..., 50 → amounts 200, 210, ..., 500

    // Open-ended range (from X to end)
    let tail = try await idx.searchRange(from: .double(900.0), to: nil)
    #expect(tail.count == 11) // 90, 91, ..., 100 → amounts 900, 910, ..., 1000

    try await sm.close()
}
