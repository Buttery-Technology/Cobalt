import Testing
import Foundation
@testable import PantryCore

@Test func testCRC32() {
    let data = Data("Hello, World!".utf8)
    let checksum = CRC32.checksum(data)
    #expect(checksum != 0)
    #expect(CRC32.checksum(data) == checksum)
    let other = Data("Different".utf8)
    #expect(CRC32.checksum(other) != checksum)
}

@Test func testPageCreateAndSerialize() throws {
    var page = DatabasePage(pageID: 42)
    #expect(page.pageID == 42)
    #expect(page.isEmpty)
    #expect(page.recordCount == 0)

    let record = Record(id: 1, data: Data("test data".utf8))
    let added = page.addRecord(record)
    #expect(added)
    #expect(page.recordCount == 1)

    try page.saveRecords()
    page.loadRecords()
    #expect(page.records.count == 1)
    #expect(page.records[0].id == 1)
    #expect(String(data: page.records[0].data, encoding: .utf8) == "test data")
}

@Test func testPageOverflow() throws {
    var page = DatabasePage(pageID: 0)
    let hugeData = Data(repeating: 0xFF, count: PantryConstants.PAGE_SIZE)
    let record = Record(id: 1, data: hugeData)
    let added = page.addRecord(record)
    #expect(!added)
}

@Test func testPageDeleteRecord() throws {
    var page = DatabasePage(pageID: 0)
    let r1 = Record(id: 1, data: Data("one".utf8))
    let r2 = Record(id: 2, data: Data("two".utf8))
    _ = page.addRecord(r1)
    _ = page.addRecord(r2)
    #expect(page.recordCount == 2)

    let deleted = page.deleteRecord(id: 1)
    #expect(deleted)
    #expect(page.recordCount == 1)
    let deletedMiss = page.deleteRecord(id: 99)
    #expect(!deletedMiss)
}

@Test func testRecordSerializeDeserialize() {
    let original = Record(id: 12345, data: Data("payload".utf8))
    let serialized = original.serialize()
    let deserialized = Record.deserialize(from: serialized)
    #expect(deserialized != nil)
    #expect(deserialized!.id == 12345)
    #expect(deserialized!.data == Data("payload".utf8))
}

@Test func testPageFlags() {
    var page = DatabasePage(pageID: 0)
    page.pageFlags = [.system, .tableRegistry]
    #expect(page.isSystemPage)
    #expect(page.pageFlags.contains(.tableRegistry))
    #expect(!page.pageFlags.contains(.indexNode))
}

@Test func testEncryption() throws {
    let key = Data(repeating: 0xAB, count: 32)
    let provider = try AESGCMEncryptionProvider(key: key)

    let plaintext = Data("secret page data".utf8)
    let ciphertext = try provider.encrypt(plaintext)

    #expect(ciphertext != plaintext)
    #expect(ciphertext.count > plaintext.count)

    let decrypted = try provider.decrypt(ciphertext)
    #expect(decrypted == plaintext)
}

@Test func testEncryptionInvalidKey() {
    #expect(throws: PantryError.self) {
        _ = try AESGCMEncryptionProvider(key: Data(repeating: 0, count: 16))
    }
}

@Test func testStorageManagerRoundTrip() async throws {
    let path = NSTemporaryDirectory() + "pantry_test_\(UUID().uuidString).db"
    defer { try? FileManager.default.removeItem(atPath: path) }

    let sm = try StorageManager(databasePath: path)

    var page = try await sm.createNewPage()
    let record = Record(id: 42, data: Data("hello storage".utf8))
    _ = page.addRecord(record)
    try await sm.writePage(&page)

    let loaded = try await sm.readPage(pageID: page.pageID)
    #expect(loaded.records.count == 1)
    #expect(loaded.records[0].id == 42)

    try await sm.close()
}

@Test func testEncryptedStorageManager() async throws {
    let path = NSTemporaryDirectory() + "pantry_enc_test_\(UUID().uuidString).db"
    defer { try? FileManager.default.removeItem(atPath: path) }

    let key = Data(repeating: 0xCD, count: 32)
    let provider = try AESGCMEncryptionProvider(key: key)
    let sm = try StorageManager(databasePath: path, encryptionProvider: provider)

    var page = try await sm.createNewPage()
    let record = Record(id: 7, data: Data("encrypted payload".utf8))
    _ = page.addRecord(record)
    try await sm.writePage(&page)

    let loaded = try await sm.readPage(pageID: page.pageID)
    #expect(loaded.records.count == 1)
    #expect(loaded.records[0].id == 7)

    try await sm.close()
}

@Test func testBufferPoolCacheHit() async throws {
    let path = NSTemporaryDirectory() + "pantry_bp_test_\(UUID().uuidString).db"
    defer { try? FileManager.default.removeItem(atPath: path) }

    let sm = try StorageManager(databasePath: path)
    let bp = BufferPoolManager(capacity: 10, storageManager: sm)

    var page = try await sm.createNewPage()
    let record = Record(id: 1, data: Data("bp test".utf8))
    _ = page.addRecord(record)
    try await sm.writePage(&page)

    // First access = miss
    _ = try await bp.getPage(pageID: page.pageID)
    let stats1 = await bp.getStats()
    #expect(stats1.missCount == 1)

    // Second access = hit
    _ = try await bp.getPage(pageID: page.pageID)
    let stats2 = await bp.getStats()
    #expect(stats2.hitCount >= 1)

    try await sm.close()
}

@Test func testDBValue() {
    #expect(DBValue.null < DBValue.integer(0))
    #expect(DBValue.integer(1) < DBValue.integer(2))
    #expect(DBValue.string("a") < DBValue.string("b"))
    #expect(DBValue.boolean(false) < DBValue.boolean(true))
    #expect(DBValue.integer(1) == DBValue.integer(1))
    #expect(DBValue.string("x") != DBValue.string("y"))
}

@Test func testRow() {
    let row = Row(values: ["name": .string("Alice"), "age": .integer(30)])
    #expect(row["name"] == .string("Alice"))
    #expect(row["age"] == .integer(30))
    #expect(row["missing"] == nil)
}

@Test func testTableSchema() {
    let schema = PantryTableSchema(name: "users", columns: [
        PantryColumn(name: "id", type: .integer, isPrimaryKey: true, isNullable: false),
        PantryColumn(name: "name", type: .string),
        PantryColumn(name: "email", type: .string, isNullable: false),
    ])

    #expect(schema.name == "users")
    #expect(schema.columns.count == 3)
    #expect(schema.primaryKeyColumn?.name == "id")
}
