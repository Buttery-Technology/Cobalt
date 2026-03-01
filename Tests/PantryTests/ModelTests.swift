import Testing
import Foundation
@testable import PantryCore
@testable import Pantry

// MARK: - Test Models

struct User: PantryModel, Equatable {
    static let tableName = "users"
    var id: String = UUID().uuidString
    var name: String
    var age: Int
    var email: String?

    static let _name = Column<User, String>(key: "name")
    static let _age = Column<User, Int>(key: "age")
    static let _email = Column<User, String?>(key: "email")
}

struct Task_: PantryModel, Equatable {
    static let tableName = "tasks"
    var id: String = UUID().uuidString
    var title: String
    var completed: Bool
    var priority: Int

    static let _title = Column<Task_, String>(key: "title")
    static let _completed = Column<Task_, Bool>(key: "completed")
    static let _priority = Column<Task_, Int>(key: "priority")
}

struct Event: PantryModel, Equatable {
    static let tableName = "events"
    var id: String = UUID().uuidString
    var name: String
    var date: Date
    var uuid: UUID
    var score: Double

    static let _name = Column<Event, String>(key: "name")
    static let _date = Column<Event, Date>(key: "date")
    static let _score = Column<Event, Double>(key: "score")
}

struct Document: PantryModel, Equatable {
    static let tableName = "documents"
    var id: String = UUID().uuidString
    var title: String
    var tags: [String]
    var metadata: [String: String]

    static let _title = Column<Document, String>(key: "title")
}

// MARK: - Helper

private func makeDB() async throws -> PantryDatabase {
    let path = NSTemporaryDirectory() + "pantry_model_\(UUID().uuidString).pantry"
    return try await PantryDatabase(configuration: PantryConfiguration(path: path))
}

// MARK: - Basic CRUD Tests

@Test func testSaveAndFind() async throws {
    let db = try await makeDB()

    let user = User(name: "Alice", age: 30, email: "alice@test.com")
    try await db.save(user)

    let found = try await db.find(User.self, id: user.id)
    #expect(found != nil)
    #expect(found?.name == "Alice")
    #expect(found?.age == 30)
    #expect(found?.email == "alice@test.com")

    try await db.close()
}

@Test func testSaveAndFindAll() async throws {
    let db = try await makeDB()

    try await db.save(User(name: "Alice", age: 30))
    try await db.save(User(name: "Bob", age: 25))
    try await db.save(User(name: "Charlie", age: 35))

    let all = try await db.findAll(User.self)
    #expect(all.count == 3)

    let names = Set(all.map { $0.name })
    #expect(names == Set(["Alice", "Bob", "Charlie"]))

    try await db.close()
}

@Test func testDeleteModel() async throws {
    let db = try await makeDB()

    let user = User(name: "Alice", age: 30)
    try await db.save(user)

    let deleted = try await db.delete(user)
    #expect(deleted == 1)

    let found = try await db.find(User.self, id: user.id)
    #expect(found == nil)

    try await db.close()
}

@Test func testUpsertBehavior() async throws {
    let db = try await makeDB()

    var user = User(id: "u1", name: "Alice", age: 30)
    try await db.save(user)

    // Update same ID
    user.name = "Alice Updated"
    user.age = 31
    try await db.save(user)

    let found = try await db.find(User.self, id: "u1")
    #expect(found?.name == "Alice Updated")
    #expect(found?.age == 31)

    // Should still be only 1 row
    let all = try await db.findAll(User.self)
    #expect(all.count == 1)

    try await db.close()
}

// MARK: - Query Builder Tests

@Test func testQueryFilterEquals() async throws {
    let db = try await makeDB()

    try await db.save(User(name: "Alice", age: 30))
    try await db.save(User(name: "Bob", age: 25))
    try await db.save(User(name: "Charlie", age: 30))

    let thirtyYearOlds = try await db.query(User.self)
        .filter(User._age == 30)
        .all()
    #expect(thirtyYearOlds.count == 2)

    let names = Set(thirtyYearOlds.map { $0.name })
    #expect(names == Set(["Alice", "Charlie"]))

    try await db.close()
}

@Test func testQueryFilterComparison() async throws {
    let db = try await makeDB()

    try await db.save(User(name: "Alice", age: 30))
    try await db.save(User(name: "Bob", age: 25))
    try await db.save(User(name: "Charlie", age: 35))

    let olderThan28 = try await db.query(User.self)
        .filter(User._age > 28)
        .all()
    #expect(olderThan28.count == 2)

    let youngerOrEqual30 = try await db.query(User.self)
        .filter(User._age <= 30)
        .all()
    #expect(youngerOrEqual30.count == 2)

    try await db.close()
}

@Test func testQueryFilterCombined() async throws {
    let db = try await makeDB()

    try await db.save(User(name: "Alice", age: 30, email: "alice@test.com"))
    try await db.save(User(name: "Bob", age: 25))
    try await db.save(User(name: "Charlie", age: 35, email: "charlie@test.com"))

    // AND: age > 28 AND name == "Alice"
    let result = try await db.query(User.self)
        .filter(User._age > 28)
        .filter(User._name == "Alice")
        .all()
    #expect(result.count == 1)
    #expect(result[0].name == "Alice")

    try await db.close()
}

@Test func testQuerySort() async throws {
    let db = try await makeDB()

    try await db.save(User(name: "Charlie", age: 35))
    try await db.save(User(name: "Alice", age: 30))
    try await db.save(User(name: "Bob", age: 25))

    let sorted = try await db.query(User.self)
        .sort(User._age, .ascending)
        .all()
    #expect(sorted.map { $0.name } == ["Bob", "Alice", "Charlie"])

    let sortedDesc = try await db.query(User.self)
        .sort(User._age, .descending)
        .all()
    #expect(sortedDesc.map { $0.name } == ["Charlie", "Alice", "Bob"])

    try await db.close()
}

@Test func testQueryLimit() async throws {
    let db = try await makeDB()

    for i in 0..<10 {
        try await db.save(User(name: "User\(i)", age: 20 + i))
    }

    let limited = try await db.query(User.self)
        .sort(User._age, .ascending)
        .limit(3)
        .all()
    #expect(limited.count == 3)
    #expect(limited[0].age == 20)
    #expect(limited[2].age == 22)

    try await db.close()
}

@Test func testQueryFirst() async throws {
    let db = try await makeDB()

    try await db.save(User(name: "Alice", age: 30))
    try await db.save(User(name: "Bob", age: 25))

    let youngest = try await db.query(User.self)
        .sort(User._age, .ascending)
        .first()
    #expect(youngest?.name == "Bob")

    // First on empty result
    let noMatch = try await db.query(User.self)
        .filter(User._age == 999)
        .first()
    #expect(noMatch == nil)

    try await db.close()
}

@Test func testQueryCount() async throws {
    let db = try await makeDB()

    try await db.save(User(name: "Alice", age: 30))
    try await db.save(User(name: "Bob", age: 25))
    try await db.save(User(name: "Charlie", age: 30))

    let total = try await db.query(User.self).count()
    #expect(total == 3)

    let thirtyCount = try await db.query(User.self)
        .filter(User._age == 30)
        .count()
    #expect(thirtyCount == 2)

    try await db.close()
}

@Test func testQueryDelete() async throws {
    let db = try await makeDB()

    try await db.save(User(name: "Alice", age: 30))
    try await db.save(User(name: "Bob", age: 25))
    try await db.save(User(name: "Charlie", age: 35))

    let deleted = try await db.query(User.self)
        .filter(User._age > 28)
        .delete()
    #expect(deleted == 2)

    let remaining = try await db.findAll(User.self)
    #expect(remaining.count == 1)
    #expect(remaining[0].name == "Bob")

    try await db.close()
}

// MARK: - Optional Properties

@Test func testOptionalNilRoundTrip() async throws {
    let db = try await makeDB()

    let user = User(name: "Alice", age: 30, email: nil)
    try await db.save(user)

    let found = try await db.find(User.self, id: user.id)
    #expect(found != nil)
    #expect(found?.email == nil)

    try await db.close()
}

@Test func testOptionalNonNilRoundTrip() async throws {
    let db = try await makeDB()

    let user = User(name: "Alice", age: 30, email: "alice@test.com")
    try await db.save(user)

    let found = try await db.find(User.self, id: user.id)
    #expect(found?.email == "alice@test.com")

    try await db.close()
}

@Test func testOptionalColumnFilter() async throws {
    let db = try await makeDB()

    try await db.save(User(name: "Alice", age: 30, email: "alice@test.com"))
    try await db.save(User(name: "Bob", age: 25, email: nil))
    try await db.save(User(name: "Charlie", age: 35, email: "charlie@test.com"))

    // Filter by optional column with unwrapped value
    let withAliceEmail = try await db.query(User.self)
        .filter(User._email == "alice@test.com")
        .all()
    #expect(withAliceEmail.count == 1)
    #expect(withAliceEmail[0].name == "Alice")

    // isNull / isNotNull
    let nullEmails = try await db.query(User.self)
        .filter(User._email.isNull())
        .all()
    #expect(nullEmails.count == 1)
    #expect(nullEmails[0].name == "Bob")

    let hasEmails = try await db.query(User.self)
        .filter(User._email.isNotNull())
        .all()
    #expect(hasEmails.count == 2)

    try await db.close()
}

// MARK: - Date and UUID Properties

@Test func testDateAndUUIDRoundTrip() async throws {
    let db = try await makeDB()

    let now = Date(timeIntervalSince1970: 1700000000.0)
    let testUUID = UUID()
    let event = Event(name: "Launch", date: now, uuid: testUUID, score: 9.5)
    try await db.save(event)

    let found = try await db.find(Event.self, id: event.id)
    #expect(found != nil)
    #expect(found?.name == "Launch")
    #expect(found?.date.timeIntervalSince1970 == now.timeIntervalSince1970)
    #expect(found?.uuid == testUUID)
    #expect(found?.score == 9.5)

    try await db.close()
}

// MARK: - Nested Codable Types

@Test func testNestedCodableRoundTrip() async throws {
    let db = try await makeDB()

    let doc = Document(
        title: "My Doc",
        tags: ["swift", "database", "pantry"],
        metadata: ["author": "Alice", "version": "1.0"]
    )
    try await db.save(doc)

    let found = try await db.find(Document.self, id: doc.id)
    #expect(found != nil)
    #expect(found?.title == "My Doc")
    #expect(found?.tags == ["swift", "database", "pantry"])
    #expect(found?.metadata == ["author": "Alice", "version": "1.0"])

    try await db.close()
}

// MARK: - Auto Table Creation

@Test func testAutoTableCreation() async throws {
    let db = try await makeDB()

    // Table shouldn't exist yet
    let existsBefore = await db.tableExists("users")
    #expect(!existsBefore)

    // Save should auto-create the table
    try await db.save(User(name: "Alice", age: 30))

    let existsAfter = await db.tableExists("users")
    #expect(existsAfter)

    try await db.close()
}

// MARK: - Multiple Model Types

@Test func testMultipleModelTypes() async throws {
    let db = try await makeDB()

    try await db.save(User(name: "Alice", age: 30))
    try await db.save(Task_(title: "Write tests", completed: false, priority: 1))
    try await db.save(Task_(title: "Ship feature", completed: true, priority: 2))

    let users = try await db.findAll(User.self)
    #expect(users.count == 1)

    let tasks = try await db.findAll(Task_.self)
    #expect(tasks.count == 2)

    // Query on each type independently
    let completedTasks = try await db.query(Task_.self)
        .filter(Task_._completed == true)
        .all()
    #expect(completedTasks.count == 1)
    #expect(completedTasks[0].title == "Ship feature")

    try await db.close()
}

// MARK: - Find on Non-Existent Table

@Test func testFindOnNonExistentTable() async throws {
    let db = try await makeDB()

    let found = try await db.find(User.self, id: "nonexistent")
    #expect(found == nil)

    let all = try await db.findAll(User.self)
    #expect(all.isEmpty)

    try await db.close()
}

// MARK: - Filter NotEquals

@Test func testFilterNotEquals() async throws {
    let db = try await makeDB()

    try await db.save(User(name: "Alice", age: 30))
    try await db.save(User(name: "Bob", age: 25))

    let notAlice = try await db.query(User.self)
        .filter(User._name != "Alice")
        .all()
    #expect(notAlice.count == 1)
    #expect(notAlice[0].name == "Bob")

    try await db.close()
}

// MARK: - Combined AND/OR Filters

@Test func testCombinedAndOrFilters() async throws {
    let db = try await makeDB()

    try await db.save(User(name: "Alice", age: 30, email: "alice@test.com"))
    try await db.save(User(name: "Bob", age: 25))
    try await db.save(User(name: "Charlie", age: 35, email: "charlie@test.com"))

    // (age > 28 AND name == "Alice") OR (age == 25)
    let result = try await db.query(User.self)
        .filter((User._age > 28 && User._name == "Alice") || User._age == 25)
        .all()
    #expect(result.count == 2)
    let names = Set(result.map { $0.name })
    #expect(names == Set(["Alice", "Bob"]))

    try await db.close()
}

// MARK: - Sort with Filter and Limit

@Test func testSortFilterLimit() async throws {
    let db = try await makeDB()

    for i in 0..<20 {
        try await db.save(User(name: "User\(i)", age: 18 + i))
    }

    // Top 3 oldest users over 25
    let top3 = try await db.query(User.self)
        .filter(User._age > 25)
        .sort(User._age, .descending)
        .limit(3)
        .all()
    #expect(top3.count == 3)
    #expect(top3[0].age == 37)
    #expect(top3[1].age == 36)
    #expect(top3[2].age == 35)

    try await db.close()
}

// MARK: - Bool Property

@Test func testBoolProperty() async throws {
    let db = try await makeDB()

    try await db.save(Task_(title: "Done", completed: true, priority: 1))
    try await db.save(Task_(title: "Pending", completed: false, priority: 2))

    let done = try await db.query(Task_.self)
        .filter(Task_._completed == true)
        .all()
    #expect(done.count == 1)
    #expect(done[0].title == "Done")

    let pending = try await db.query(Task_.self)
        .filter(Task_._completed == false)
        .all()
    #expect(pending.count == 1)
    #expect(pending[0].title == "Pending")

    try await db.close()
}

// MARK: - Delete on Non-Existent Returns Zero

@Test func testDeleteNonExistent() async throws {
    let db = try await makeDB()

    let user = User(name: "Ghost", age: 0)
    let deleted = try await db.delete(user)
    #expect(deleted == 0)

    try await db.close()
}

// MARK: - Empty Query

@Test func testEmptyQueryResults() async throws {
    let db = try await makeDB()

    // Save one user so table exists
    try await db.save(User(name: "Alice", age: 30))

    let noMatch = try await db.query(User.self)
        .filter(User._age == 999)
        .all()
    #expect(noMatch.isEmpty)

    let count = try await db.query(User.self)
        .filter(User._age == 999)
        .count()
    #expect(count == 0)

    try await db.close()
}

// MARK: - Default Table Name

struct Widget: PantryModel, Equatable {
    var id: String = UUID().uuidString
    var label: String
    // No explicit tableName — should default to "widgets"
}

@Test func testDefaultTableName() async throws {
    #expect(Widget.tableName == "widgets")

    let db = try await makeDB()
    try await db.save(Widget(label: "Gear"))

    let found = try await db.findAll(Widget.self)
    #expect(found.count == 1)
    #expect(found[0].label == "Gear")

    // Verify the actual table name used
    let exists = await db.tableExists("widgets")
    #expect(exists)

    try await db.close()
}

// MARK: - Batch Save

@Test func testSaveAll() async throws {
    let db = try await makeDB()

    let users = [
        User(name: "Alice", age: 30),
        User(name: "Bob", age: 25),
        User(name: "Charlie", age: 35),
    ]
    try await db.saveAll(users)

    let all = try await db.findAll(User.self)
    #expect(all.count == 3)

    let names = Set(all.map { $0.name })
    #expect(names == Set(["Alice", "Bob", "Charlie"]))

    try await db.close()
}

// MARK: - Exists

@Test func testQueryExists() async throws {
    let db = try await makeDB()

    try await db.save(User(name: "Alice", age: 30))
    try await db.save(User(name: "Bob", age: 25))

    let hasAlice = try await db.query(User.self)
        .filter(User._name == "Alice")
        .exists()
    #expect(hasAlice)

    let hasNoOne = try await db.query(User.self)
        .filter(User._age == 999)
        .exists()
    #expect(!hasNoOne)

    try await db.close()
}

// MARK: - Nil Optional First Save Regression

@Test func testNilOptionalFirstSaveThenNonNil() async throws {
    let db = try await makeDB()

    // First save has nil email — schema must still include the email column
    try await db.save(User(name: "Alice", age: 30, email: nil))

    // Second save has non-nil email — must not fail
    try await db.save(User(name: "Bob", age: 25, email: "bob@test.com"))

    let alice = try await db.query(User.self)
        .filter(User._name == "Alice")
        .first()
    #expect(alice?.email == nil)

    let bob = try await db.query(User.self)
        .filter(User._name == "Bob")
        .first()
    #expect(bob?.email == "bob@test.com")

    // Filter on the optional column should work
    let withEmail = try await db.query(User.self)
        .filter(User._email.isNotNull())
        .all()
    #expect(withEmail.count == 1)
    #expect(withEmail[0].name == "Bob")

    try await db.close()
}

// MARK: - Convenience Init Tests

@Test func testConvenienceInit() async throws {
    let dbPath = PantryConfiguration.databasePath(name: "default")
    try? FileManager.default.removeItem(atPath: dbPath)

    let db = try await PantryDatabase()

    try await db.save(User(name: "Alice", age: 30))
    let found = try await db.findAll(User.self)
    #expect(found.count == 1)
    #expect(found[0].name == "Alice")

    try await db.close()
    try? FileManager.default.removeItem(atPath: dbPath)
}

@Test func testNamedInit() async throws {
    let name = "test_named_\(UUID().uuidString)"
    let db = try await PantryDatabase(name: name)

    try await db.save(User(name: "Bob", age: 25))
    let found = try await db.findAll(User.self)
    #expect(found.count == 1)
    #expect(found[0].name == "Bob")

    try await db.close()

    // Clean up
    let path = PantryConfiguration.databasePath(name: name)
    try? FileManager.default.removeItem(atPath: path)
}

@Test func testEncryptedConvenienceInit() async throws {
    let name = "test_encrypted_\(UUID().uuidString)"
    let db = try await PantryDatabase(name: name, encrypted: true)

    try await db.save(User(name: "Secret", age: 42))
    let found = try await db.findAll(User.self)
    #expect(found.count == 1)
    #expect(found[0].name == "Secret")

    // Verify .key file was created
    let keyPath = PantryConfiguration.databasePath(name: name) + ".key"
    #expect(FileManager.default.fileExists(atPath: keyPath))
    let keyData = FileManager.default.contents(atPath: keyPath)
    #expect(keyData?.count == 32)

    try await db.close()

    // Clean up
    let dbPath = PantryConfiguration.databasePath(name: name)
    try? FileManager.default.removeItem(atPath: dbPath)
    try? FileManager.default.removeItem(atPath: keyPath)
}

@Test func testEncryptedReopenWithKeyFile() async throws {
    let name = "test_reopen_\(UUID().uuidString)"

    // Open, write, close
    let db1 = try await PantryDatabase(name: name, encrypted: true)
    let user = User(id: "u1", name: "Persist", age: 99)
    try await db1.save(user)
    try await db1.close()

    // Reopen same name — should reuse key file
    let db2 = try await PantryDatabase(name: name, encrypted: true)
    let found = try await db2.find(User.self, id: "u1")
    #expect(found != nil)
    #expect(found?.name == "Persist")
    #expect(found?.age == 99)
    try await db2.close()

    // Clean up
    let dbPath = PantryConfiguration.databasePath(name: name)
    try? FileManager.default.removeItem(atPath: dbPath)
    try? FileManager.default.removeItem(atPath: dbPath + ".key")
}

@Test func testGenerateKey() async throws {
    let key1 = PantryConfiguration.generateKey()
    let key2 = PantryConfiguration.generateKey()

    #expect(key1.count == 32)
    #expect(key2.count == 32)
    #expect(key1 != key2)
}

@Test func testDefaultDirectory() async throws {
    let dir = PantryConfiguration.defaultDirectory()
    #expect(!dir.isEmpty)
    #expect(FileManager.default.fileExists(atPath: dir))
}
