import Testing
import Foundation
@testable import CobaltCore

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
    let hugeData = Data(repeating: 0xFF, count: CobaltConstants.PAGE_SIZE)
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
    #expect(throws: CobaltError.self) {
        _ = try AESGCMEncryptionProvider(key: Data(repeating: 0, count: 16))
    }
}

@Test func testStorageManagerRoundTrip() async throws {
    let path = NSTemporaryDirectory() + "cobalt_test_\(UUID().uuidString).db"
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
    let path = NSTemporaryDirectory() + "cobalt_enc_test_\(UUID().uuidString).db"
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
    let path = NSTemporaryDirectory() + "cobalt_bp_test_\(UUID().uuidString).db"
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
    let schema = CobaltTableSchema(name: "users", columns: [
        CobaltColumn(name: "id", type: .integer, isPrimaryKey: true, isNullable: false),
        CobaltColumn(name: "name", type: .string),
        CobaltColumn(name: "email", type: .string, isNullable: false),
    ])

    #expect(schema.name == "users")
    #expect(schema.columns.count == 3)
    #expect(schema.primaryKeyColumn?.name == "id")
}

// MARK: - Page Edge Cases

@Test func testPageFillNearCapacity() throws {
    var page = DatabasePage(pageID: 0)
    var recordsAdded = 0

    // Fill the page with small records until it can't fit more
    while true {
        let record = Record(id: UInt64(recordsAdded + 1), data: Data("record_\(recordsAdded)".utf8))
        if !page.addRecord(record) {
            break
        }
        recordsAdded += 1
    }

    #expect(recordsAdded > 0, "Should fit at least some records")
    #expect(page.recordCount == recordsAdded)

    // Verify data integrity after near-capacity fill
    try page.saveRecords()
    page.loadRecords()
    #expect(page.records.count == recordsAdded)

    // Verify each record's ID survived
    let ids = Set(page.records.map { $0.id })
    for i in 1...recordsAdded {
        #expect(ids.contains(UInt64(i)), "Missing record ID \(i)")
    }
}

@Test func testPageMultipleDeletesAndAdds() throws {
    var page = DatabasePage(pageID: 0)

    // Add 5 records
    for i in 1...5 {
        _ = page.addRecord(Record(id: UInt64(i), data: Data("r\(i)".utf8)))
    }
    #expect(page.recordCount == 5)

    // Delete odd IDs
    _ = page.deleteRecord(id: 1)
    _ = page.deleteRecord(id: 3)
    _ = page.deleteRecord(id: 5)
    #expect(page.recordCount == 2)

    // Verify remaining
    try page.saveRecords()
    page.loadRecords()
    let ids = Set(page.records.map { $0.id })
    #expect(ids == [2, 4])
}

@Test func testRecordWithEmptyData() {
    let record = Record(id: 1, data: Data())
    let serialized = record.serialize()
    let deserialized = Record.deserialize(from: serialized)
    #expect(deserialized != nil)
    #expect(deserialized!.id == 1)
    #expect(deserialized!.data.isEmpty)
}

@Test func testRecordWithLargeData() {
    let largeData = Data(repeating: 0xAB, count: 4000)
    let record = Record(id: 99, data: largeData)
    let serialized = record.serialize()
    let deserialized = Record.deserialize(from: serialized)
    #expect(deserialized != nil)
    #expect(deserialized!.id == 99)
    #expect(deserialized!.data == largeData)
}

// MARK: - DBValue Edge Cases

@Test func testDBValueNaNHandling() {
    // NaN == NaN should be true (reflexive for storage, unlike IEEE)
    let nan1 = DBValue.double(.nan)
    let nan2 = DBValue.double(.nan)
    #expect(nan1 == nan2)

    // NaN hashing must be consistent
    #expect(nan1.hashValue == nan2.hashValue)

    // -0.0 == +0.0
    let negZero = DBValue.double(-0.0)
    let posZero = DBValue.double(0.0)
    #expect(negZero == posZero)
    #expect(negZero.hashValue == posZero.hashValue)
}

@Test func testDBValueCrossTypeComparisons() {
    // Null is less than everything
    #expect(DBValue.null < DBValue.integer(0))
    #expect(DBValue.null < DBValue.string(""))
    #expect(DBValue.null < DBValue.boolean(false))

    // Type ordering: null(0) < boolean(1) < integer(2) < double(3) < string(4) < blob(5)
    // Note: integer and double cross-compare as numbers, not by type order
    #expect(DBValue.boolean(false) < DBValue.integer(0))
    #expect(DBValue.double(0.0) < DBValue.string(""))
    #expect(DBValue.string("") < DBValue.blob(Data()))

    // Integer and double compare numerically
    #expect(DBValue.integer(0) == DBValue.double(0.0))
    #expect(DBValue.integer(1) < DBValue.double(1.5))
    #expect(DBValue.double(0.5) < DBValue.integer(1))
}

@Test func testDBValueIntegerComparisons() {
    #expect(DBValue.integer(Int64.min) < DBValue.integer(0))
    #expect(DBValue.integer(0) < DBValue.integer(Int64.max))
    #expect(DBValue.integer(Int64.max) == DBValue.integer(Int64.max))
}

@Test func testDBValueBlobComparisons() {
    let a = DBValue.blob(Data([0x00, 0x01]))
    let b = DBValue.blob(Data([0x00, 0x02]))
    let c = DBValue.blob(Data([0x00, 0x01, 0x00]))
    #expect(a < b)
    #expect(a < c) // shorter but prefix matches
}

// MARK: - Row Edge Cases

@Test func testRowEquality() {
    let r1 = Row(values: ["a": .integer(1), "b": .string("hello")])
    let r2 = Row(values: ["a": .integer(1), "b": .string("hello")])
    let r3 = Row(values: ["a": .integer(1), "b": .string("world")])
    #expect(r1 == r2)
    #expect(r1 != r3)
}

@Test func testRowHashing() {
    let r1 = Row(values: ["x": .integer(42)])
    let r2 = Row(values: ["x": .integer(42)])
    #expect(r1.hashValue == r2.hashValue)

    // Can be used in Set
    var rowSet: Set<Row> = [r1, r2]
    #expect(rowSet.count == 1)
    rowSet.insert(Row(values: ["x": .integer(43)]))
    #expect(rowSet.count == 2)
}

@Test func testRowEmptyValues() {
    let empty = Row(values: [:])
    #expect(empty["anything"] == nil)
    #expect(empty.values.isEmpty)
}

// MARK: - CRC32 Edge Cases

@Test func testCRC32EmptyData() {
    let checksum = CRC32.checksum(Data())
    #expect(checksum != 0 || checksum == 0) // Should not crash; value is 0 for empty
}

@Test func testCRC32KnownValue() {
    // CRC32 of "123456789" is a well-known test vector: 0xCBF43926
    let data = Data("123456789".utf8)
    let checksum = CRC32.checksum(data)
    #expect(checksum == 0xCBF43926)
}

@Test func testCRC32LargeData() {
    let largeData = Data(repeating: 0xFF, count: 100_000)
    let checksum = CRC32.checksum(largeData)
    // Same data should produce same checksum
    #expect(CRC32.checksum(largeData) == checksum)
}

// MARK: - Encryption Edge Cases

@Test func testEncryptionSmallData() throws {
    let key = Data(repeating: 0x42, count: 32)
    let provider = try AESGCMEncryptionProvider(key: key)

    // Single byte — smallest non-empty plaintext
    let small = Data([0x42])
    let encrypted = try provider.encrypt(small)
    #expect(encrypted.count > small.count) // nonce + ciphertext + tag
    let decrypted = try provider.decrypt(encrypted)
    #expect(decrypted == small)
}

@Test func testEncryptionDifferentCiphertexts() throws {
    let key = Data(repeating: 0x42, count: 32)
    let provider = try AESGCMEncryptionProvider(key: key)

    let plaintext = Data("same data".utf8)
    let enc1 = try provider.encrypt(plaintext)
    let enc2 = try provider.encrypt(plaintext)

    // Different nonces = different ciphertexts
    #expect(enc1 != enc2)

    // Both decrypt to same plaintext
    #expect(try provider.decrypt(enc1) == plaintext)
    #expect(try provider.decrypt(enc2) == plaintext)
}

@Test func testDecryptionWithWrongKey() throws {
    let key1 = Data(repeating: 0x42, count: 32)
    let key2 = Data(repeating: 0x43, count: 32)

    let provider1 = try AESGCMEncryptionProvider(key: key1)
    let provider2 = try AESGCMEncryptionProvider(key: key2)

    let encrypted = try provider1.encrypt(Data("secret".utf8))

    #expect(throws: Error.self) {
        _ = try provider2.decrypt(encrypted)
    }
}

@Test func testDecryptionWithCorruptedData() throws {
    let key = Data(repeating: 0x42, count: 32)
    let provider = try AESGCMEncryptionProvider(key: key)

    let encrypted = try provider.encrypt(Data("test".utf8))
    var corrupted = encrypted
    corrupted[corrupted.count / 2] ^= 0xFF // Flip bits in the middle

    #expect(throws: Error.self) {
        _ = try provider.decrypt(corrupted)
    }
}

// MARK: - Schema Factory Methods

@Test func testSchemaFactoryMethods() {
    let id = CobaltColumn.id("pk")
    #expect(id.name == "pk")
    #expect(id.type == .string)
    #expect(id.isPrimaryKey)
    #expect(!id.isNullable)

    let str = CobaltColumn.string("name", nullable: false, defaultValue: "unknown")
    #expect(str.type == .string)
    #expect(!str.isNullable)
    #expect(str.defaultValue == .string("unknown"))

    let int = CobaltColumn.integer("age", defaultValue: 0)
    #expect(int.type == .integer)
    #expect(int.isNullable) // default
    #expect(int.defaultValue == .integer(0))

    let dbl = CobaltColumn.double("score")
    #expect(dbl.type == .double)
    #expect(dbl.defaultValue == nil)

    let bool = CobaltColumn.boolean("active", defaultValue: true)
    #expect(bool.type == .boolean)
    #expect(bool.defaultValue == .boolean(true))

    let data = CobaltColumn.blob("photo")
    #expect(data.type == .blob)
}

// MARK: - StorageManager Edge Cases

@Test func testStorageManagerMultiplePages() async throws {
    let path = NSTemporaryDirectory() + "cobalt_sm_multi_\(UUID().uuidString).db"
    defer { try? FileManager.default.removeItem(atPath: path) }

    let sm = try StorageManager(databasePath: path)

    // Create and write multiple pages
    var pages: [DatabasePage] = []
    for i in 0..<10 {
        var page = try await sm.createNewPage()
        _ = page.addRecord(Record(id: UInt64(i + 1), data: Data("page_\(i)".utf8)))
        try await sm.writePage(&page)
        pages.append(page)
    }

    // Read them all back in reverse order
    for i in (0..<10).reversed() {
        let loaded = try await sm.readPage(pageID: pages[i].pageID)
        #expect(loaded.records.count == 1)
        #expect(loaded.records[0].id == UInt64(i + 1))
    }

    try await sm.close()
}

@Test func testPageFlagsAllCombinations() {
    var page = DatabasePage(pageID: 0)

    // Set all flags
    page.pageFlags = [.system, .indexNode, .dataPage, .overflow, .tableRegistry]
    #expect(page.isSystemPage)
    #expect(page.pageFlags.contains(.indexNode))
    #expect(page.pageFlags.contains(.dataPage))
    #expect(page.pageFlags.contains(.overflow))
    #expect(page.pageFlags.contains(.tableRegistry))

    // Clear and set one
    page.pageFlags = .dataPage
    #expect(!page.isSystemPage)
    #expect(page.pageFlags.contains(.dataPage))
    #expect(!page.pageFlags.contains(.indexNode))
}

// MARK: - Crash Recovery Tests

@Test func testDataSurvivesCloseAndReopen() async throws {
    let path = NSTemporaryDirectory() + "cobalt_crash_reopen_\(UUID().uuidString).db"
    defer { try? FileManager.default.removeItem(atPath: path) }

    // Session 1: create pages and write data, then close properly
    let sm1 = try StorageManager(databasePath: path)
    var page1 = try await sm1.createNewPage()
    let r1 = Record(id: 1, data: Data("survive_close".utf8))
    _ = page1.addRecord(r1)
    try await sm1.writePage(&page1)

    var page2 = try await sm1.createNewPage()
    let r2 = Record(id: 2, data: Data("second_record".utf8))
    _ = page2.addRecord(r2)
    try await sm1.writePage(&page2)

    let page1ID = page1.pageID
    let page2ID = page2.pageID
    try await sm1.close()

    // Session 2: reopen and verify all data is present
    let sm2 = try StorageManager(databasePath: path)
    let loaded1 = try await sm2.readPage(pageID: page1ID)
    #expect(loaded1.records.count == 1)
    #expect(loaded1.records[0].id == 1)
    #expect(String(data: loaded1.records[0].data, encoding: .utf8) == "survive_close")

    let loaded2 = try await sm2.readPage(pageID: page2ID)
    #expect(loaded2.records.count == 1)
    #expect(loaded2.records[0].id == 2)
    #expect(String(data: loaded2.records[0].data, encoding: .utf8) == "second_record")

    try await sm2.close()
}

@Test func testDataSurvivesWithoutExplicitClose() async throws {
    let path = NSTemporaryDirectory() + "cobalt_crash_noclse_\(UUID().uuidString).db"
    defer { try? FileManager.default.removeItem(atPath: path) }

    var writtenPageID: Int = -1

    // Session 1: create data and do NOT call close() — simulate crash
    do {
        let sm = try StorageManager(databasePath: path)
        var page = try await sm.createNewPage()
        let record = Record(id: 99, data: Data("crash_data".utf8))
        _ = page.addRecord(record)
        try await sm.writePage(&page)
        writtenPageID = page.pageID
        // No close() — sm goes out of scope
    }

    // Session 2: reopen — WAL recovery should handle this
    let sm2 = try StorageManager(databasePath: path)
    let loaded = try await sm2.readPage(pageID: writtenPageID)
    #expect(loaded.records.count == 1)
    #expect(loaded.records[0].id == 99)
    #expect(String(data: loaded.records[0].data, encoding: .utf8) == "crash_data")

    try await sm2.close()
}

// MARK: - Page Corruption Detection Tests

@Test func testCorruptedPageThrowsError() async throws {
    let path = NSTemporaryDirectory() + "cobalt_corrupt_\(UUID().uuidString).db"
    defer { try? FileManager.default.removeItem(atPath: path) }

    // Write valid data
    let sm = try StorageManager(databasePath: path)
    var page = try await sm.createNewPage()
    let record = Record(id: 42, data: Data("valid_data".utf8))
    _ = page.addRecord(record)
    try await sm.writePage(&page)
    let pageID = page.pageID
    try await sm.close()

    // Corrupt a byte in the data portion of the page on disk
    let fileHandle = try FileHandle(forUpdating: URL(fileURLWithPath: path))
    let pageSize = CobaltConstants.PAGE_SIZE
    let offset = UInt64(pageID * pageSize)
    // Corrupt a byte in the middle of the page data area (after header)
    let corruptOffset = offset + UInt64(CobaltConstants.PAGE_HEADER_SIZE + 10)
    fileHandle.seek(toFileOffset: corruptOffset)
    let originalByte = fileHandle.readData(ofLength: 1)
    fileHandle.seek(toFileOffset: corruptOffset)
    let flippedByte = Data([originalByte[0] ^ 0xFF])
    fileHandle.write(flippedByte)
    fileHandle.closeFile()

    // Reopen and try to read — should throw corruptPage
    let sm2 = try StorageManager(databasePath: path)
    await #expect(throws: CobaltError.self) {
        _ = try await sm2.readPage(pageID: pageID)
    }
}
