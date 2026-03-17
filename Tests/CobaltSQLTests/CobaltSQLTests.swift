import Testing
import Foundation
@testable import CobaltSQL
@testable import CobaltCore
@testable import CobaltQuery
@testable import Cobalt

// MARK: - Lexer Tests

@Test func testLexerBasicTokens() throws {
    var lexer = Lexer("SELECT * FROM users WHERE id = 1")
    let tokens = try lexer.tokenize()
    let types = tokens.map { $0.0 }

    #expect(types[0] == .keyword(.select))
    #expect(types[1] == .star)
    #expect(types[2] == .keyword(.from))
    #expect(types[3] == .identifier("users"))
    #expect(types[4] == .keyword(.where))
    #expect(types[5] == .identifier("id"))
    #expect(types[6] == .equals)
    #expect(types[7] == .integerLiteral(1))
    #expect(types[8] == .eof)
}

@Test func testLexerStringLiteral() throws {
    var lexer = Lexer("'hello world'")
    let tokens = try lexer.tokenize()
    #expect(tokens[0].0 == .stringLiteral("hello world"))
}

@Test func testLexerEscapedString() throws {
    var lexer = Lexer("'it''s'")
    let tokens = try lexer.tokenize()
    #expect(tokens[0].0 == .stringLiteral("it's"))
}

@Test func testLexerDoubleLiteral() throws {
    var lexer = Lexer("3.14")
    let tokens = try lexer.tokenize()
    #expect(tokens[0].0 == .doubleLiteral(3.14))
}

@Test func testLexerOperators() throws {
    var lexer = Lexer("<= >= != <>")
    let tokens = try lexer.tokenize()
    #expect(tokens[0].0 == .lessOrEqual)
    #expect(tokens[1].0 == .greaterOrEqual)
    #expect(tokens[2].0 == .notEquals)
    #expect(tokens[3].0 == .notEquals)
}

@Test func testLexerComments() throws {
    var lexer = Lexer("SELECT -- this is a comment\n42")
    let tokens = try lexer.tokenize()
    #expect(tokens[0].0 == .keyword(.select))
    #expect(tokens[1].0 == .integerLiteral(42))
}

@Test func testLexerBlockComment() throws {
    var lexer = Lexer("SELECT /* block */ 42")
    let tokens = try lexer.tokenize()
    #expect(tokens[0].0 == .keyword(.select))
    #expect(tokens[1].0 == .integerLiteral(42))
}

@Test func testLexerParameter() throws {
    var lexer = Lexer("$1 $2")
    let tokens = try lexer.tokenize()
    #expect(tokens[0].0 == .parameter(1))
    #expect(tokens[1].0 == .parameter(2))
}

@Test func testLexerQuotedIdentifier() throws {
    var lexer = Lexer("\"my table\"")
    let tokens = try lexer.tokenize()
    #expect(tokens[0].0 == .identifier("my table"))
}

// MARK: - Parser Tests

@Test func testParseSimpleSelect() throws {
    let stmt = try Parser.parse("SELECT * FROM users")
    guard case .select(let sel) = stmt else {
        Issue.record("Expected SELECT")
        return
    }
    #expect(sel.columns.count == 1)
    if case .allColumns = sel.columns[0] {} else {
        Issue.record("Expected *")
    }
    if case .table(let name, _) = sel.from {
        #expect(name == "users")
    } else {
        Issue.record("Expected table ref")
    }
}

@Test func testParseSelectWithWhere() throws {
    let stmt = try Parser.parse("SELECT name, age FROM users WHERE age > 18")
    guard case .select(let sel) = stmt else {
        Issue.record("Expected SELECT"); return
    }
    #expect(sel.columns.count == 2)
    #expect(sel.whereClause != nil)
}

@Test func testParseSelectDistinct() throws {
    let stmt = try Parser.parse("SELECT DISTINCT name FROM users")
    guard case .select(let sel) = stmt else {
        Issue.record("Expected SELECT"); return
    }
    #expect(sel.distinct)
}

@Test func testParseSelectOrderByLimitOffset() throws {
    let stmt = try Parser.parse("SELECT * FROM users ORDER BY age DESC LIMIT 10 OFFSET 5")
    guard case .select(let sel) = stmt else {
        Issue.record("Expected SELECT"); return
    }
    #expect(sel.orderBy.count == 1)
    #expect(sel.orderBy[0].ascending == false)
    if case .integerLiteral(10) = sel.limit {} else { Issue.record("Expected LIMIT 10") }
    if case .integerLiteral(5) = sel.offset {} else { Issue.record("Expected OFFSET 5") }
}

@Test func testParseSelectJoin() throws {
    let stmt = try Parser.parse("SELECT * FROM users INNER JOIN orders ON users.id = orders.user_id")
    guard case .select(let sel) = stmt else {
        Issue.record("Expected SELECT"); return
    }
    #expect(sel.joins.count == 1)
    #expect(sel.joins[0].joinType == .inner)
}

@Test func testParseSelectGroupBy() throws {
    let stmt = try Parser.parse("SELECT city, COUNT(*) FROM users GROUP BY city HAVING COUNT(*) > 5")
    guard case .select(let sel) = stmt else {
        Issue.record("Expected SELECT"); return
    }
    #expect(sel.groupBy.count == 1)
    #expect(sel.having != nil)
}

@Test func testParseInsert() throws {
    let stmt = try Parser.parse("INSERT INTO users (name, age) VALUES ('Alice', 30)")
    guard case .insert(let ins) = stmt else {
        Issue.record("Expected INSERT"); return
    }
    #expect(ins.table == "users")
    #expect(ins.columns == ["name", "age"])
    #expect(ins.values.count == 1)
    #expect(ins.values[0].count == 2)
}

@Test func testParseInsertMultiRow() throws {
    let stmt = try Parser.parse("INSERT INTO users (name) VALUES ('Alice'), ('Bob')")
    guard case .insert(let ins) = stmt else {
        Issue.record("Expected INSERT"); return
    }
    #expect(ins.values.count == 2)
}

@Test func testParseUpdate() throws {
    let stmt = try Parser.parse("UPDATE users SET name = 'Bob', age = 25 WHERE id = 1")
    guard case .update(let upd) = stmt else {
        Issue.record("Expected UPDATE"); return
    }
    #expect(upd.table == "users")
    #expect(upd.assignments.count == 2)
    #expect(upd.whereClause != nil)
}

@Test func testParseDelete() throws {
    let stmt = try Parser.parse("DELETE FROM users WHERE age < 18")
    guard case .delete(let del) = stmt else {
        Issue.record("Expected DELETE"); return
    }
    #expect(del.table == "users")
    #expect(del.whereClause != nil)
}

@Test func testParseCreateTable() throws {
    let stmt = try Parser.parse("""
        CREATE TABLE users (
            id INTEGER PRIMARY KEY NOT NULL,
            name TEXT NOT NULL,
            email VARCHAR(255),
            active BOOLEAN DEFAULT TRUE
        )
    """)
    guard case .createTable(let ct) = stmt else {
        Issue.record("Expected CREATE TABLE"); return
    }
    #expect(ct.name == "users")
    #expect(ct.columns.count == 4)
    #expect(ct.columns[0].isPrimaryKey)
    #expect(!ct.columns[0].isNullable)
    #expect(ct.columns[2].isNullable)
}

@Test func testParseCreateTableIfNotExists() throws {
    let stmt = try Parser.parse("CREATE TABLE IF NOT EXISTS users (id INTEGER)")
    guard case .createTable(let ct) = stmt else {
        Issue.record("Expected CREATE TABLE"); return
    }
    #expect(ct.ifNotExists)
}

@Test func testParseDropTable() throws {
    let stmt = try Parser.parse("DROP TABLE users")
    guard case .dropTable(let dt) = stmt else {
        Issue.record("Expected DROP TABLE"); return
    }
    #expect(dt.name == "users")
    #expect(!dt.ifExists)
}

@Test func testParseDropTableIfExists() throws {
    let stmt = try Parser.parse("DROP TABLE IF EXISTS users")
    guard case .dropTable(let dt) = stmt else {
        Issue.record("Expected DROP TABLE"); return
    }
    #expect(dt.ifExists)
}

@Test func testParseAlterTableAddColumn() throws {
    let stmt = try Parser.parse("ALTER TABLE users ADD COLUMN email TEXT")
    guard case .alterTable(let at) = stmt else {
        Issue.record("Expected ALTER TABLE"); return
    }
    #expect(at.table == "users")
    if case .addColumn(let col) = at.action {
        #expect(col.name == "email")
    } else {
        Issue.record("Expected ADD COLUMN")
    }
}

@Test func testParseAlterTableDropColumn() throws {
    let stmt = try Parser.parse("ALTER TABLE users DROP COLUMN email")
    guard case .alterTable(let at) = stmt else {
        Issue.record("Expected ALTER TABLE"); return
    }
    if case .dropColumn(let name) = at.action {
        #expect(name == "email")
    } else {
        Issue.record("Expected DROP COLUMN")
    }
}

@Test func testParseAlterTableRenameColumn() throws {
    let stmt = try Parser.parse("ALTER TABLE users RENAME COLUMN name TO full_name")
    guard case .alterTable(let at) = stmt else {
        Issue.record("Expected ALTER TABLE"); return
    }
    if case .renameColumn(let from, let to) = at.action {
        #expect(from == "name")
        #expect(to == "full_name")
    } else {
        Issue.record("Expected RENAME COLUMN")
    }
}

@Test func testParseCreateIndex() throws {
    let stmt = try Parser.parse("CREATE INDEX idx_age ON users (age)")
    guard case .createIndex(let ci) = stmt else {
        Issue.record("Expected CREATE INDEX"); return
    }
    #expect(ci.name == "idx_age")
    #expect(ci.table == "users")
    #expect(ci.columns == ["age"])
    #expect(!ci.unique)
}

@Test func testParseCreateUniqueIndex() throws {
    let stmt = try Parser.parse("CREATE UNIQUE INDEX idx_email ON users (email)")
    guard case .createIndex(let ci) = stmt else {
        Issue.record("Expected CREATE INDEX"); return
    }
    #expect(ci.unique)
}

@Test func testParseBeginCommitRollback() throws {
    let begin = try Parser.parse("BEGIN")
    guard case .begin = begin else { Issue.record("Expected BEGIN"); return }

    let commit = try Parser.parse("COMMIT")
    guard case .commit = commit else { Issue.record("Expected COMMIT"); return }

    let rollback = try Parser.parse("ROLLBACK")
    guard case .rollback = rollback else { Issue.record("Expected ROLLBACK"); return }
}

// MARK: - Expression Parsing

@Test func testParseExpressionBetween() throws {
    let stmt = try Parser.parse("SELECT * FROM t WHERE x BETWEEN 1 AND 10")
    guard case .select(let sel) = stmt, let w = sel.whereClause else {
        Issue.record("Expected WHERE"); return
    }
    if case .between = w {} else {
        Issue.record("Expected BETWEEN expression")
    }
}

@Test func testParseExpressionIn() throws {
    let stmt = try Parser.parse("SELECT * FROM t WHERE x IN (1, 2, 3)")
    guard case .select(let sel) = stmt, let w = sel.whereClause else {
        Issue.record("Expected WHERE"); return
    }
    if case .inList(_, let vals) = w {
        #expect(vals.count == 3)
    } else {
        Issue.record("Expected IN list")
    }
}

@Test func testParseExpressionLike() throws {
    let stmt = try Parser.parse("SELECT * FROM t WHERE name LIKE '%alice%'")
    guard case .select(let sel) = stmt, let w = sel.whereClause else {
        Issue.record("Expected WHERE"); return
    }
    if case .like = w {} else {
        Issue.record("Expected LIKE expression")
    }
}

@Test func testParseExpressionIsNull() throws {
    let stmt = try Parser.parse("SELECT * FROM t WHERE x IS NULL")
    guard case .select(let sel) = stmt, let w = sel.whereClause else {
        Issue.record("Expected WHERE"); return
    }
    if case .isNull = w {} else {
        Issue.record("Expected IS NULL")
    }
}

@Test func testParseExpressionIsNotNull() throws {
    let stmt = try Parser.parse("SELECT * FROM t WHERE x IS NOT NULL")
    guard case .select(let sel) = stmt, let w = sel.whereClause else {
        Issue.record("Expected WHERE"); return
    }
    if case .isNotNull = w {} else {
        Issue.record("Expected IS NOT NULL")
    }
}

@Test func testParseExpressionAnd() throws {
    let stmt = try Parser.parse("SELECT * FROM t WHERE a = 1 AND b = 2")
    guard case .select(let sel) = stmt, let w = sel.whereClause else {
        Issue.record("Expected WHERE"); return
    }
    if case .binaryOp(_, .and, _) = w {} else {
        Issue.record("Expected AND")
    }
}

@Test func testParseExpressionOr() throws {
    let stmt = try Parser.parse("SELECT * FROM t WHERE a = 1 OR b = 2")
    guard case .select(let sel) = stmt, let w = sel.whereClause else {
        Issue.record("Expected WHERE"); return
    }
    if case .binaryOp(_, .or, _) = w {} else {
        Issue.record("Expected OR")
    }
}

@Test func testParseExpressionNot() throws {
    let stmt = try Parser.parse("SELECT * FROM t WHERE NOT a = 1")
    guard case .select(let sel) = stmt, let w = sel.whereClause else {
        Issue.record("Expected WHERE"); return
    }
    if case .unaryOp(.not, _) = w {} else {
        Issue.record("Expected NOT")
    }
}

@Test func testParseExpressionNegative() throws {
    let stmt = try Parser.parse("SELECT * FROM t WHERE x = -42")
    guard case .select(let sel) = stmt, let w = sel.whereClause else {
        Issue.record("Expected WHERE"); return
    }
    if case .binaryOp(_, .equal, .integerLiteral(-42)) = w {} else {
        Issue.record("Expected -42")
    }
}

@Test func testParseAggregate() throws {
    let stmt = try Parser.parse("SELECT COUNT(*), SUM(amount), AVG(price) FROM orders")
    guard case .select(let sel) = stmt else {
        Issue.record("Expected SELECT"); return
    }
    #expect(sel.columns.count == 3)
}

@Test func testParseCast() throws {
    let stmt = try Parser.parse("SELECT CAST(x AS INTEGER) FROM t")
    guard case .select(let sel) = stmt else {
        Issue.record("Expected SELECT"); return
    }
    if case .expression(let expr, _) = sel.columns[0] {
        if case .cast(_, .integer) = expr {} else {
            Issue.record("Expected CAST to INTEGER")
        }
    }
}

// MARK: - AST Lowering Tests

@Test func testLowerSimpleEquals() throws {
    let lowering = ASTLowering()
    let expr = Expression.binaryOp(
        left: .column(table: nil, name: "age"),
        op: .equal,
        right: .integerLiteral(30)
    )
    let cond = try lowering.lowerWhereClause(expr)
    if case .equals(let col, let val) = cond {
        #expect(col == "age")
        #expect(val == .integer(30))
    } else {
        Issue.record("Expected equals condition")
    }
}

@Test func testLowerBetween() throws {
    let lowering = ASTLowering()
    let expr = Expression.between(
        .column(table: nil, name: "price"),
        low: .doubleLiteral(10.0),
        high: .doubleLiteral(100.0)
    )
    let cond = try lowering.lowerWhereClause(expr)
    if case .between(let col, let min, let max) = cond {
        #expect(col == "price")
        #expect(min == .double(10.0))
        #expect(max == .double(100.0))
    } else {
        Issue.record("Expected between condition")
    }
}

@Test func testLowerInList() throws {
    let lowering = ASTLowering()
    let expr = Expression.inList(
        .column(table: nil, name: "status"),
        [.stringLiteral("active"), .stringLiteral("pending")]
    )
    let cond = try lowering.lowerWhereClause(expr)
    if case .in(let col, let values) = cond {
        #expect(col == "status")
        #expect(values.count == 2)
    } else {
        Issue.record("Expected IN condition")
    }
}

@Test func testLowerAndOr() throws {
    let lowering = ASTLowering()
    let expr = Expression.binaryOp(
        left: .binaryOp(left: .column(table: nil, name: "a"), op: .equal, right: .integerLiteral(1)),
        op: .and,
        right: .binaryOp(left: .column(table: nil, name: "b"), op: .greaterThan, right: .integerLiteral(2))
    )
    let cond = try lowering.lowerWhereClause(expr)
    if case .and(let subs) = cond {
        #expect(subs.count == 2)
    } else {
        Issue.record("Expected AND condition")
    }
}

@Test func testLowerCreateTable() throws {
    let lowering = ASTLowering()
    let create = CreateTableStatement(name: "test", columns: [
        ColumnDef(name: "id", dataType: .integer, isPrimaryKey: true, isNullable: false),
        ColumnDef(name: "name", dataType: .text),
        ColumnDef(name: "score", dataType: .real),
    ])
    let schema = try lowering.lowerCreateTable(create)
    #expect(schema.name == "test")
    #expect(schema.columns.count == 3)
    #expect(schema.columns[0].type == .integer)
    #expect(schema.columns[0].isPrimaryKey)
    #expect(schema.columns[1].type == .string)
    #expect(schema.columns[2].type == .double)
}

@Test func testLowerOrderBy() throws {
    let lowering = ASTLowering()
    let items = [
        OrderByItem(expression: .column(table: nil, name: "age"), ascending: false),
        OrderByItem(expression: .column(table: nil, name: "name"), ascending: true),
    ]
    let result = try lowering.lowerOrderBy(items)
    #expect(result.count == 2)
    #expect(result[0].column == "age")
    #expect(result[0].direction == .descending)
    #expect(result[1].column == "name")
    #expect(result[1].direction == .ascending)
}

// MARK: - Integration Tests (SQL → db.execute)

private func makeDB() async throws -> CobaltDatabase {
    let path = NSTemporaryDirectory() + "cobalt_sql_\(UUID().uuidString).cobalt"
    return try await CobaltDatabase(configuration: CobaltConfiguration(path: path))
}

@Test func testExecuteCreateTableAndInsert() async throws {
    let db = try await makeDB()

    _ = try await db.execute(sql: """
        CREATE TABLE products (
            id INTEGER PRIMARY KEY NOT NULL,
            name TEXT NOT NULL,
            price REAL
        )
    """)

    #expect(await db.tableExists("products"))

    _ = try await db.execute(sql: "INSERT INTO products (id, name, price) VALUES (1, 'Widget', 9.99)")
    _ = try await db.execute(sql: "INSERT INTO products (id, name, price) VALUES (2, 'Gadget', 19.99)")

    let result = try await db.execute(sql: "SELECT * FROM products")
    guard case .rows(let rows) = result else {
        Issue.record("Expected rows"); return
    }
    #expect(rows.count == 2)

    try await db.close()
}

@Test func testExecuteSelectWithWhere() async throws {
    let db = try await makeDB()

    _ = try await db.execute(sql: "CREATE TABLE items (id INTEGER PRIMARY KEY NOT NULL, name TEXT, price REAL)")
    _ = try await db.execute(sql: "INSERT INTO items (id, name, price) VALUES (1, 'A', 5.0)")
    _ = try await db.execute(sql: "INSERT INTO items (id, name, price) VALUES (2, 'B', 15.0)")
    _ = try await db.execute(sql: "INSERT INTO items (id, name, price) VALUES (3, 'C', 25.0)")

    let result = try await db.execute(sql: "SELECT * FROM items WHERE price > 10.0")
    guard case .rows(let rows) = result else {
        Issue.record("Expected rows"); return
    }
    #expect(rows.count == 2)

    try await db.close()
}

@Test func testExecuteUpdate() async throws {
    let db = try await makeDB()

    _ = try await db.execute(sql: "CREATE TABLE data (key TEXT PRIMARY KEY NOT NULL, val INTEGER)")
    _ = try await db.execute(sql: "INSERT INTO data (key, val) VALUES ('a', 1)")
    _ = try await db.execute(sql: "INSERT INTO data (key, val) VALUES ('b', 2)")

    let result = try await db.execute(sql: "UPDATE data SET val = 99 WHERE key = 'a'")
    guard case .rowCount(let n) = result else {
        Issue.record("Expected rowCount"); return
    }
    #expect(n == 1)

    let rows = try await db.execute(sql: "SELECT * FROM data WHERE key = 'a'")
    guard case .rows(let r) = rows else { Issue.record("Expected rows"); return }
    #expect(r[0]["val"] == .integer(99))

    try await db.close()
}

@Test func testExecuteDelete() async throws {
    let db = try await makeDB()

    _ = try await db.execute(sql: "CREATE TABLE data (id INTEGER PRIMARY KEY NOT NULL, v TEXT)")
    _ = try await db.execute(sql: "INSERT INTO data (id, v) VALUES (1, 'x')")
    _ = try await db.execute(sql: "INSERT INTO data (id, v) VALUES (2, 'y')")

    let result = try await db.execute(sql: "DELETE FROM data WHERE id = 1")
    guard case .rowCount(let n) = result else {
        Issue.record("Expected rowCount"); return
    }
    #expect(n == 1)

    let remaining = try await db.execute(sql: "SELECT * FROM data")
    guard case .rows(let rows) = remaining else { Issue.record("Expected rows"); return }
    #expect(rows.count == 1)

    try await db.close()
}

@Test func testExecuteDropTable() async throws {
    let db = try await makeDB()

    _ = try await db.execute(sql: "CREATE TABLE temp (id INTEGER)")
    #expect(await db.tableExists("temp"))

    _ = try await db.execute(sql: "DROP TABLE temp")
    #expect(!(await db.tableExists("temp")))

    try await db.close()
}

@Test func testExecuteSelectOrderByLimit() async throws {
    let db = try await makeDB()

    _ = try await db.execute(sql: "CREATE TABLE nums (id INTEGER PRIMARY KEY NOT NULL, val INTEGER)")
    for i in 1...10 {
        _ = try await db.execute(sql: "INSERT INTO nums (id, val) VALUES (\(i), \(i * 10))")
    }

    let result = try await db.execute(sql: "SELECT * FROM nums ORDER BY val DESC LIMIT 3")
    guard case .rows(let rows) = result else { Issue.record("Expected rows"); return }
    #expect(rows.count == 3)
    #expect(rows[0]["val"] == .integer(100))
    #expect(rows[1]["val"] == .integer(90))
    #expect(rows[2]["val"] == .integer(80))

    try await db.close()
}

@Test func testExecuteSelectBetween() async throws {
    let db = try await makeDB()

    _ = try await db.execute(sql: "CREATE TABLE nums (id INTEGER PRIMARY KEY NOT NULL, val INTEGER)")
    for i in 1...20 {
        _ = try await db.execute(sql: "INSERT INTO nums (id, val) VALUES (\(i), \(i))")
    }

    let result = try await db.execute(sql: "SELECT * FROM nums WHERE val BETWEEN 5 AND 15")
    guard case .rows(let rows) = result else { Issue.record("Expected rows"); return }
    #expect(rows.count == 11)

    try await db.close()
}

@Test func testExecuteSelectIn() async throws {
    let db = try await makeDB()

    _ = try await db.execute(sql: "CREATE TABLE items (id INTEGER PRIMARY KEY NOT NULL, name TEXT)")
    _ = try await db.execute(sql: "INSERT INTO items (id, name) VALUES (1, 'A')")
    _ = try await db.execute(sql: "INSERT INTO items (id, name) VALUES (2, 'B')")
    _ = try await db.execute(sql: "INSERT INTO items (id, name) VALUES (3, 'C')")

    let result = try await db.execute(sql: "SELECT * FROM items WHERE name IN ('A', 'C')")
    guard case .rows(let rows) = result else { Issue.record("Expected rows"); return }
    #expect(rows.count == 2)

    try await db.close()
}

@Test func testExecuteSelectLike() async throws {
    let db = try await makeDB()

    _ = try await db.execute(sql: "CREATE TABLE items (id INTEGER PRIMARY KEY NOT NULL, name TEXT)")
    _ = try await db.execute(sql: "INSERT INTO items (id, name) VALUES (1, 'hello')")
    _ = try await db.execute(sql: "INSERT INTO items (id, name) VALUES (2, 'world')")
    _ = try await db.execute(sql: "INSERT INTO items (id, name) VALUES (3, 'help')")

    let result = try await db.execute(sql: "SELECT * FROM items WHERE name LIKE 'hel%'")
    guard case .rows(let rows) = result else { Issue.record("Expected rows"); return }
    #expect(rows.count == 2)

    try await db.close()
}

@Test func testExecuteSelectIsNull() async throws {
    let db = try await makeDB()

    _ = try await db.execute(sql: "CREATE TABLE data (id INTEGER PRIMARY KEY NOT NULL, val TEXT)")
    _ = try await db.execute(sql: "INSERT INTO data (id, val) VALUES (1, 'x')")
    _ = try await db.execute(sql: "INSERT INTO data (id, val) VALUES (2, NULL)")

    let result = try await db.execute(sql: "SELECT * FROM data WHERE val IS NULL")
    guard case .rows(let rows) = result else { Issue.record("Expected rows"); return }
    #expect(rows.count == 1)
    #expect(rows[0]["id"] == .integer(2))

    try await db.close()
}

@Test func testExecuteCreateTableIfNotExists() async throws {
    let db = try await makeDB()

    _ = try await db.execute(sql: "CREATE TABLE test (id INTEGER)")
    // Should not throw
    _ = try await db.execute(sql: "CREATE TABLE IF NOT EXISTS test (id INTEGER)")
    #expect(await db.tableExists("test"))

    try await db.close()
}

@Test func testExecuteMultiRowInsert() async throws {
    let db = try await makeDB()

    _ = try await db.execute(sql: "CREATE TABLE items (id INTEGER PRIMARY KEY NOT NULL, name TEXT)")
    _ = try await db.execute(sql: "INSERT INTO items (id, name) VALUES (1, 'A'), (2, 'B'), (3, 'C')")

    let result = try await db.execute(sql: "SELECT * FROM items")
    guard case .rows(let rows) = result else { Issue.record("Expected rows"); return }
    #expect(rows.count == 3)

    try await db.close()
}

@Test func testSQLMatchesNativeAPI() async throws {
    let db = try await makeDB()

    // Create table via SQL
    _ = try await db.execute(sql: """
        CREATE TABLE users (
            id TEXT PRIMARY KEY NOT NULL,
            name TEXT NOT NULL,
            age INTEGER
        )
    """)

    // Insert via native API
    try await db.insert(into: "users", values: ["id": .string("1"), "name": .string("Alice"), "age": .integer(30)])
    try await db.insert(into: "users", values: ["id": .string("2"), "name": .string("Bob"), "age": .integer(25)])

    // Query via SQL
    let sqlResult = try await db.execute(sql: "SELECT * FROM users WHERE age > 20 ORDER BY age DESC")
    guard case .rows(let sqlRows) = sqlResult else { Issue.record("Expected rows"); return }

    // Query via native API
    let nativeRows = try await db.select(from: "users", where: .greaterThan(column: "age", value: .integer(20)),
                                          orderBy: [.desc("age")])

    // Compare
    #expect(sqlRows.count == nativeRows.count)
    #expect(sqlRows[0]["name"] == nativeRows[0]["name"])
    #expect(sqlRows[1]["name"] == nativeRows[1]["name"])

    try await db.close()
}

// MARK: - Phase 2: Schema Enhancements Tests

@Test func testSerialAutoIncrement() async throws {
    let db = try await makeDB()

    _ = try await db.execute(sql: """
        CREATE TABLE events (
            id SERIAL,
            name TEXT NOT NULL
        )
    """)

    // Insert without providing id — should auto-generate
    _ = try await db.execute(sql: "INSERT INTO events (name) VALUES ('first')")
    _ = try await db.execute(sql: "INSERT INTO events (name) VALUES ('second')")
    _ = try await db.execute(sql: "INSERT INTO events (name) VALUES ('third')")

    let result = try await db.execute(sql: "SELECT * FROM events ORDER BY id")
    guard case .rows(let rows) = result else {
        Issue.record("Expected rows"); return
    }
    #expect(rows.count == 3)
    #expect(rows[0]["id"] == .integer(1))
    #expect(rows[1]["id"] == .integer(2))
    #expect(rows[2]["id"] == .integer(3))
    #expect(rows[0]["name"] == .string("first"))

    try await db.close()
}

@Test func testAlterTableAddColumnViaSQL() async throws {
    let db = try await makeDB()

    _ = try await db.execute(sql: """
        CREATE TABLE people (
            id INTEGER PRIMARY KEY NOT NULL,
            name TEXT NOT NULL
        )
    """)
    _ = try await db.execute(sql: "INSERT INTO people (id, name) VALUES (1, 'Alice')")

    // Add a column
    _ = try await db.execute(sql: "ALTER TABLE people ADD COLUMN age INTEGER")

    // Verify schema has the new column
    let schema = await db.getTableSchema("people")
    #expect(schema != nil)
    #expect(schema!.columns.count == 3)
    #expect(schema!.columns[2].name == "age")

    // Insert with new column
    _ = try await db.execute(sql: "INSERT INTO people (id, name, age) VALUES (2, 'Bob', 30)")
    let result = try await db.execute(sql: "SELECT * FROM people WHERE id = 2")
    guard case .rows(let rows) = result else {
        Issue.record("Expected rows"); return
    }
    #expect(rows[0]["age"] == .integer(30))

    try await db.close()
}

@Test func testAlterTableDropColumnViaSQL() async throws {
    let db = try await makeDB()

    _ = try await db.execute(sql: """
        CREATE TABLE records (
            id INTEGER PRIMARY KEY NOT NULL,
            name TEXT,
            extra TEXT
        )
    """)
    _ = try await db.execute(sql: "INSERT INTO records (id, name, extra) VALUES (1, 'A', 'remove_me')")

    // Drop a column
    _ = try await db.execute(sql: "ALTER TABLE records DROP COLUMN extra")

    // Verify schema no longer has the column
    let schema = await db.getTableSchema("records")
    #expect(schema != nil)
    #expect(schema!.columns.count == 2)
    #expect(!schema!.columns.contains(where: { $0.name == "extra" }))

    // Existing data should not contain the dropped column
    let result = try await db.execute(sql: "SELECT * FROM records WHERE id = 1")
    guard case .rows(let rows) = result else {
        Issue.record("Expected rows"); return
    }
    #expect(rows[0]["name"] == .string("A"))

    try await db.close()
}

@Test func testAlterTableRenameColumnViaSQL() async throws {
    let db = try await makeDB()

    _ = try await db.execute(sql: """
        CREATE TABLE items2 (
            id INTEGER PRIMARY KEY NOT NULL,
            title TEXT
        )
    """)
    _ = try await db.execute(sql: "INSERT INTO items2 (id, title) VALUES (1, 'Hello')")

    // Rename a column
    _ = try await db.execute(sql: "ALTER TABLE items2 RENAME COLUMN title TO label")

    // Verify schema has the renamed column
    let schema = await db.getTableSchema("items2")
    #expect(schema != nil)
    #expect(schema!.columns.contains(where: { $0.name == "label" }))
    #expect(!schema!.columns.contains(where: { $0.name == "title" }))

    // Existing data should be accessible via the new column name
    let result = try await db.execute(sql: "SELECT * FROM items2 WHERE id = 1")
    guard case .rows(let rows) = result else {
        Issue.record("Expected rows"); return
    }
    #expect(rows[0]["label"] == .string("Hello"))

    try await db.close()
}

// MARK: - Phase 5: Built-in Functions & Data Types Tests

@Test func testBuiltinLength() async throws {
    let db = try await makeDB()
    let result = try await db.execute(sql: "SELECT length('hello') AS len")
    guard case .rows(let rows) = result else { Issue.record("Expected rows"); return }
    #expect(rows.count == 1)
    #expect(rows[0]["len"] == .integer(5))
    try await db.close()
}

@Test func testBuiltinUpper() async throws {
    let db = try await makeDB()
    let result = try await db.execute(sql: "SELECT upper('hello') AS val")
    guard case .rows(let rows) = result else { Issue.record("Expected rows"); return }
    #expect(rows[0]["val"] == .string("HELLO"))
    try await db.close()
}

@Test func testBuiltinLower() async throws {
    let db = try await makeDB()
    let result = try await db.execute(sql: "SELECT lower('HELLO') AS val")
    guard case .rows(let rows) = result else { Issue.record("Expected rows"); return }
    #expect(rows[0]["val"] == .string("hello"))
    try await db.close()
}

@Test func testBuiltinTrim() async throws {
    let db = try await makeDB()
    let result = try await db.execute(sql: "SELECT trim('  hello  ') AS val")
    guard case .rows(let rows) = result else { Issue.record("Expected rows"); return }
    #expect(rows[0]["val"] == .string("hello"))
    try await db.close()
}

@Test func testBuiltinConcat() async throws {
    let db = try await makeDB()
    let result = try await db.execute(sql: "SELECT concat('hello', ' ', 'world') AS val")
    guard case .rows(let rows) = result else { Issue.record("Expected rows"); return }
    #expect(rows[0]["val"] == .string("hello world"))
    try await db.close()
}

@Test func testBuiltinReplace() async throws {
    let db = try await makeDB()
    let result = try await db.execute(sql: "SELECT replace('hello world', 'world', 'there') AS val")
    guard case .rows(let rows) = result else { Issue.record("Expected rows"); return }
    #expect(rows[0]["val"] == .string("hello there"))
    try await db.close()
}

@Test func testBuiltinSubstring() async throws {
    let db = try await makeDB()
    let result = try await db.execute(sql: "SELECT substring('hello world', 1, 5) AS val")
    guard case .rows(let rows) = result else { Issue.record("Expected rows"); return }
    #expect(rows[0]["val"] == .string("hello"))
    try await db.close()
}

@Test func testBuiltinAbs() async throws {
    let db = try await makeDB()
    let result = try await db.execute(sql: "SELECT abs(-42) AS val")
    guard case .rows(let rows) = result else { Issue.record("Expected rows"); return }
    #expect(rows[0]["val"] == .integer(42))
    try await db.close()
}

@Test func testBuiltinAbsDouble() async throws {
    let db = try await makeDB()
    let result = try await db.execute(sql: "SELECT abs(-3.14) AS val")
    guard case .rows(let rows) = result else { Issue.record("Expected rows"); return }
    #expect(rows[0]["val"] == .double(3.14))
    try await db.close()
}

@Test func testBuiltinCeil() async throws {
    let db = try await makeDB()
    let result = try await db.execute(sql: "SELECT ceil(3.2) AS val")
    guard case .rows(let rows) = result else { Issue.record("Expected rows"); return }
    #expect(rows[0]["val"] == .double(4.0))
    try await db.close()
}

@Test func testBuiltinFloor() async throws {
    let db = try await makeDB()
    let result = try await db.execute(sql: "SELECT floor(3.8) AS val")
    guard case .rows(let rows) = result else { Issue.record("Expected rows"); return }
    #expect(rows[0]["val"] == .double(3.0))
    try await db.close()
}

@Test func testBuiltinRound() async throws {
    let db = try await makeDB()
    let result = try await db.execute(sql: "SELECT round(3.14159, 2) AS val")
    guard case .rows(let rows) = result else { Issue.record("Expected rows"); return }
    #expect(rows[0]["val"] == .double(3.14))
    try await db.close()
}

@Test func testBuiltinRoundNoDecimals() async throws {
    let db = try await makeDB()
    let result = try await db.execute(sql: "SELECT round(3.7) AS val")
    guard case .rows(let rows) = result else { Issue.record("Expected rows"); return }
    #expect(rows[0]["val"] == .double(4.0))
    try await db.close()
}

@Test func testBuiltinPower() async throws {
    let db = try await makeDB()
    let result = try await db.execute(sql: "SELECT power(2, 10) AS val")
    guard case .rows(let rows) = result else { Issue.record("Expected rows"); return }
    #expect(rows[0]["val"] == .double(1024.0))
    try await db.close()
}

@Test func testBuiltinSqrt() async throws {
    let db = try await makeDB()
    let result = try await db.execute(sql: "SELECT sqrt(144.0) AS val")
    guard case .rows(let rows) = result else { Issue.record("Expected rows"); return }
    #expect(rows[0]["val"] == .double(12.0))
    try await db.close()
}

@Test func testBuiltinMod() async throws {
    let db = try await makeDB()
    let result = try await db.execute(sql: "SELECT mod(17, 5) AS val")
    guard case .rows(let rows) = result else { Issue.record("Expected rows"); return }
    #expect(rows[0]["val"] == .integer(2))
    try await db.close()
}

@Test func testBuiltinNow() async throws {
    let db = try await makeDB()
    let result = try await db.execute(sql: "SELECT now() AS ts")
    guard case .rows(let rows) = result else { Issue.record("Expected rows"); return }
    guard case .double(let ts) = rows[0]["ts"] else { Issue.record("Expected double"); return }
    // Should be a reasonable epoch timestamp (after year 2020)
    #expect(ts > 1_577_836_800)
    try await db.close()
}

@Test func testCastStringToInteger() async throws {
    let db = try await makeDB()
    let result = try await db.execute(sql: "SELECT CAST('42' AS INTEGER) AS val")
    guard case .rows(let rows) = result else { Issue.record("Expected rows"); return }
    #expect(rows[0]["val"] == .integer(42))
    try await db.close()
}

@Test func testCastIntegerToText() async throws {
    let db = try await makeDB()
    let result = try await db.execute(sql: "SELECT CAST(123 AS TEXT) AS val")
    guard case .rows(let rows) = result else { Issue.record("Expected rows"); return }
    #expect(rows[0]["val"] == .string("123"))
    try await db.close()
}

@Test func testCastDoubleToInteger() async throws {
    let db = try await makeDB()
    let result = try await db.execute(sql: "SELECT CAST(3.99 AS INTEGER) AS val")
    guard case .rows(let rows) = result else { Issue.record("Expected rows"); return }
    #expect(rows[0]["val"] == .integer(3))
    try await db.close()
}

@Test func testCastBooleanToInteger() async throws {
    let db = try await makeDB()
    let result = try await db.execute(sql: "SELECT CAST(TRUE AS INTEGER) AS val")
    guard case .rows(let rows) = result else { Issue.record("Expected rows"); return }
    #expect(rows[0]["val"] == .integer(1))
    try await db.close()
}

@Test func testFunctionsWithTableData() async throws {
    let db = try await makeDB()

    _ = try await db.execute(sql: "CREATE TABLE words (id INTEGER PRIMARY KEY NOT NULL, word TEXT)")
    _ = try await db.execute(sql: "INSERT INTO words (id, word) VALUES (1, 'Hello')")
    _ = try await db.execute(sql: "INSERT INTO words (id, word) VALUES (2, 'World')")

    let result = try await db.execute(sql: "SELECT upper(word) AS up, length(word) AS len FROM words ORDER BY id")
    guard case .rows(let rows) = result else { Issue.record("Expected rows"); return }
    #expect(rows.count == 2)
    #expect(rows[0]["up"] == .string("HELLO"))
    #expect(rows[0]["len"] == .integer(5))
    #expect(rows[1]["up"] == .string("WORLD"))
    #expect(rows[1]["len"] == .integer(5))

    try await db.close()
}

@Test func testMultipleFunctionsTableless() async throws {
    let db = try await makeDB()

    let result = try await db.execute(sql: "SELECT length('hello') AS len, upper('hello') AS up, abs(-42) AS a")
    guard case .rows(let rows) = result else { Issue.record("Expected rows"); return }
    #expect(rows.count == 1)
    #expect(rows[0]["len"] == .integer(5))
    #expect(rows[0]["up"] == .string("HELLO"))
    #expect(rows[0]["a"] == .integer(42))

    try await db.close()
}

@Test func testNullPropagation() async throws {
    let db = try await makeDB()
    let result = try await db.execute(sql: "SELECT length(NULL) AS val")
    guard case .rows(let rows) = result else { Issue.record("Expected rows"); return }
    #expect(rows[0]["val"] == .null)
    try await db.close()
}

@Test func testBuiltinFunctionsUnit() throws {
    // Direct unit tests for BuiltinFunctions without a database
    #expect(try BuiltinFunctions.evaluate(name: "length", args: [.string("test")]) == .integer(4))
    #expect(try BuiltinFunctions.evaluate(name: "upper", args: [.string("abc")]) == .string("ABC"))
    #expect(try BuiltinFunctions.evaluate(name: "lower", args: [.string("XYZ")]) == .string("xyz"))
    #expect(try BuiltinFunctions.evaluate(name: "trim", args: [.string("  hi  ")]) == .string("hi"))
    #expect(try BuiltinFunctions.evaluate(name: "abs", args: [.integer(-7)]) == .integer(7))
    #expect(try BuiltinFunctions.evaluate(name: "floor", args: [.double(2.9)]) == .double(2.0))
    #expect(try BuiltinFunctions.evaluate(name: "ceil", args: [.double(2.1)]) == .double(3.0))
    #expect(try BuiltinFunctions.evaluate(name: "sqrt", args: [.double(25.0)]) == .double(5.0))
    #expect(try BuiltinFunctions.evaluate(name: "mod", args: [.integer(10), .integer(3)]) == .integer(1))
    #expect(try BuiltinFunctions.evaluate(name: "power", args: [.double(3.0), .double(2.0)]) == .double(9.0))
    #expect(try BuiltinFunctions.evaluate(name: "replace", args: [.string("abc"), .string("b"), .string("x")]) == .string("axc"))
    #expect(try BuiltinFunctions.evaluate(name: "substring", args: [.string("hello"), .integer(2), .integer(3)]) == .string("ell"))
    #expect(try BuiltinFunctions.evaluate(name: "concat", args: [.string("a"), .string("b")]) == .string("ab"))
}

@Test func testCastUnit() throws {
    #expect(try BuiltinFunctions.cast(.string("42"), to: .integer) == .integer(42))
    #expect(try BuiltinFunctions.cast(.integer(42), to: .real) == .double(42.0))
    #expect(try BuiltinFunctions.cast(.integer(42), to: .text) == .string("42"))
    #expect(try BuiltinFunctions.cast(.double(3.5), to: .integer) == .integer(3))
    #expect(try BuiltinFunctions.cast(.boolean(true), to: .integer) == .integer(1))
    #expect(try BuiltinFunctions.cast(.boolean(false), to: .integer) == .integer(0))
    #expect(try BuiltinFunctions.cast(.null, to: .integer) == .null)
}

// MARK: - Phase 4: Triggers Tests

@Test func testParseCreateTrigger() throws {
    let stmt = try Parser.parse("""
        CREATE TRIGGER audit_insert AFTER INSERT ON orders FOR EACH ROW BEGIN INSERT INTO audit_log (action) VALUES ('inserted'); END
    """)
    guard case .createTrigger(let ct) = stmt else {
        Issue.record("Expected CREATE TRIGGER"); return
    }
    #expect(ct.name == "audit_insert")
    #expect(ct.timing == "AFTER")
    #expect(ct.event == "INSERT")
    #expect(ct.table == "orders")
    #expect(ct.forEach == "ROW")
    #expect(ct.body.count == 1)
}

@Test func testParseDropTrigger() throws {
    let stmt = try Parser.parse("DROP TRIGGER audit_insert")
    guard case .dropTrigger(let dt) = stmt else {
        Issue.record("Expected DROP TRIGGER"); return
    }
    #expect(dt.name == "audit_insert")
}

@Test func testTriggerAfterInsertFiresAndCreatesAuditEntry() async throws {
    let db = try await makeDB()

    // Create orders and audit_log tables
    _ = try await db.execute(sql: """
        CREATE TABLE orders (id INTEGER PRIMARY KEY NOT NULL, product TEXT)
    """)
    _ = try await db.execute(sql: """
        CREATE TABLE audit_log (id SERIAL, action TEXT)
    """)

    // Create trigger: after inserting into orders, insert into audit_log
    _ = try await db.execute(sql: """
        CREATE TRIGGER trg_audit AFTER INSERT ON orders FOR EACH ROW BEGIN INSERT INTO audit_log (action) VALUES ('order_inserted'); END
    """)

    // Insert into orders — should fire trigger
    _ = try await db.execute(sql: "INSERT INTO orders (id, product) VALUES (1, 'Widget')")

    // Verify audit_log has the entry
    let result = try await db.execute(sql: "SELECT * FROM audit_log")
    guard case .rows(let rows) = result else {
        Issue.record("Expected rows"); return
    }
    #expect(rows.count == 1)
    #expect(rows[0]["action"] == .string("order_inserted"))

    try await db.close()
}

@Test func testDropTriggerStopsExecution() async throws {
    let db = try await makeDB()

    // Create tables
    _ = try await db.execute(sql: """
        CREATE TABLE orders2 (id INTEGER PRIMARY KEY NOT NULL, product TEXT)
    """)
    _ = try await db.execute(sql: """
        CREATE TABLE audit_log2 (id SERIAL, action TEXT)
    """)

    // Create and then drop the trigger
    _ = try await db.execute(sql: """
        CREATE TRIGGER trg_audit2 AFTER INSERT ON orders2 FOR EACH ROW BEGIN INSERT INTO audit_log2 (action) VALUES ('order_inserted'); END
    """)
    _ = try await db.execute(sql: "DROP TRIGGER trg_audit2")

    // Insert into orders2 — trigger should NOT fire
    _ = try await db.execute(sql: "INSERT INTO orders2 (id, product) VALUES (1, 'Gadget')")

    // Verify audit_log2 is empty
    let result = try await db.execute(sql: "SELECT * FROM audit_log2")
    guard case .rows(let rows) = result else {
        Issue.record("Expected rows"); return
    }
    #expect(rows.count == 0)

    try await db.close()
}

@Test func testBeforeTriggerFires() async throws {
    let db = try await makeDB()

    // Create tables
    _ = try await db.execute(sql: """
        CREATE TABLE events (id INTEGER PRIMARY KEY NOT NULL, name TEXT)
    """)
    _ = try await db.execute(sql: """
        CREATE TABLE event_log (id SERIAL, action TEXT)
    """)

    // Create BEFORE INSERT trigger
    _ = try await db.execute(sql: """
        CREATE TRIGGER trg_before BEFORE INSERT ON events FOR EACH ROW BEGIN INSERT INTO event_log (action) VALUES ('before_insert'); END
    """)

    _ = try await db.execute(sql: "INSERT INTO events (id, name) VALUES (1, 'test')")

    let result = try await db.execute(sql: "SELECT * FROM event_log")
    guard case .rows(let rows) = result else {
        Issue.record("Expected rows"); return
    }
    #expect(rows.count == 1)
    #expect(rows[0]["action"] == .string("before_insert"))

    try await db.close()
}

@Test func testMultipleTriggersOnSameTable() async throws {
    let db = try await makeDB()

    _ = try await db.execute(sql: "CREATE TABLE items3 (id INTEGER PRIMARY KEY NOT NULL, name TEXT)")
    _ = try await db.execute(sql: "CREATE TABLE log3 (id SERIAL, msg TEXT)")

    // Register two AFTER INSERT triggers
    _ = try await db.execute(sql: """
        CREATE TRIGGER trg_a AFTER INSERT ON items3 FOR EACH ROW BEGIN INSERT INTO log3 (msg) VALUES ('trigger_a'); END
    """)
    _ = try await db.execute(sql: """
        CREATE TRIGGER trg_b AFTER INSERT ON items3 FOR EACH ROW BEGIN INSERT INTO log3 (msg) VALUES ('trigger_b'); END
    """)

    _ = try await db.execute(sql: "INSERT INTO items3 (id, name) VALUES (1, 'x')")

    let result = try await db.execute(sql: "SELECT * FROM log3 ORDER BY id")
    guard case .rows(let rows) = result else {
        Issue.record("Expected rows"); return
    }
    #expect(rows.count == 2)
    #expect(rows[0]["msg"] == .string("trigger_a"))
    #expect(rows[1]["msg"] == .string("trigger_b"))

    try await db.close()
}

// MARK: - Phase 7: VACUUM Tests

@Test func testVacuumBasic() async throws {
    let db = try await makeDB()

    _ = try await db.execute(sql: "CREATE TABLE vac_test (id INTEGER PRIMARY KEY NOT NULL, val TEXT)")
    // Insert many rows
    for i in 1...50 {
        _ = try await db.execute(sql: "INSERT INTO vac_test (id, val) VALUES (\(i), 'row\(i)')")
    }
    // Delete most rows
    _ = try await db.execute(sql: "DELETE FROM vac_test WHERE id > 5")

    // Run VACUUM via SQL
    let result = try await db.execute(sql: "VACUUM vac_test")
    guard case .rows(let rows) = result else {
        Issue.record("Expected rows from VACUUM"); return
    }
    #expect(rows.count == 1)
    // pages_scanned should be > 0
    guard case .integer(let scanned) = rows[0]["pages_scanned"] else {
        Issue.record("Expected integer pages_scanned"); return
    }
    #expect(scanned > 0)

    try await db.close()
}

@Test func testVacuumViaPublicAPI() async throws {
    let db = try await makeDB()

    _ = try await db.execute(sql: "CREATE TABLE vac2 (id INTEGER PRIMARY KEY NOT NULL, name TEXT)")
    for i in 1...20 {
        _ = try await db.execute(sql: "INSERT INTO vac2 (id, name) VALUES (\(i), 'item\(i)')")
    }
    _ = try await db.execute(sql: "DELETE FROM vac2 WHERE id > 2")

    let result = try await db.vacuum(table: "vac2")
    #expect(result.pagesScanned > 0)

    try await db.close()
}

@Test func testVacuumParseNoTable() throws {
    let stmt = try Parser.parse("VACUUM")
    guard case .vacuum(let v) = stmt else {
        Issue.record("Expected vacuum statement"); return
    }
    #expect(v.table == nil)
}

@Test func testVacuumParseWithTable() throws {
    let stmt = try Parser.parse("VACUUM my_table")
    guard case .vacuum(let v) = stmt else {
        Issue.record("Expected vacuum statement"); return
    }
    #expect(v.table == "my_table")
}

// MARK: - Phase 7: Roles & Permissions Tests

@Test func testRoleManagerBasic() throws {
    let rm = RoleManager()

    // Default superuser role exists
    let roles = rm.listRoles()
    #expect(roles.contains { $0.name == "cobalt" && $0.isSuperuser })

    // Superuser has all permissions
    #expect(rm.hasPermission("cobalt", .select, on: "any_table"))
    #expect(rm.hasPermission("cobalt", .delete, on: nil))
}

@Test func testRoleCreateAndDrop() throws {
    let rm = RoleManager()

    let appRole = Role(name: "app_user", isSuperuser: false, canLogin: true, passwordHash: "hashed_pw")
    try rm.createRole(appRole)

    let roles = rm.listRoles()
    #expect(roles.contains { $0.name == "app_user" })

    // Duplicate creation should throw
    #expect(throws: RoleError.self) {
        try rm.createRole(appRole)
    }

    try rm.dropRole("app_user")
    let rolesAfter = rm.listRoles()
    #expect(!rolesAfter.contains { $0.name == "app_user" })

    // Dropping non-existent role should throw
    #expect(throws: RoleError.self) {
        try rm.dropRole("nonexistent")
    }
}

@Test func testRoleGrantAndRevoke() throws {
    let rm = RoleManager()

    let reader = Role(name: "reader", isSuperuser: false, canLogin: true)
    try rm.createRole(reader)

    // No permissions initially
    #expect(!rm.hasPermission("reader", .select, on: "users"))

    // Grant SELECT on users
    rm.grant(.select, to: "reader", on: "users")
    #expect(rm.hasPermission("reader", .select, on: "users"))
    #expect(!rm.hasPermission("reader", .select, on: "orders"))  // different table
    #expect(!rm.hasPermission("reader", .insert, on: "users"))  // different permission

    // Grant ALL globally
    rm.grant(.all, to: "reader", on: nil)
    #expect(rm.hasPermission("reader", .insert, on: "orders"))
    #expect(rm.hasPermission("reader", .delete, on: "anything"))

    // Revoke ALL globally
    rm.revoke(.all, from: "reader", on: nil)
    #expect(!rm.hasPermission("reader", .insert, on: "orders"))
    // SELECT on users should still work
    #expect(rm.hasPermission("reader", .select, on: "users"))

    // Revoke SELECT on users
    rm.revoke(.select, from: "reader", on: "users")
    #expect(!rm.hasPermission("reader", .select, on: "users"))
}

@Test func testRoleGlobalGrant() throws {
    let rm = RoleManager()

    let writer = Role(name: "writer", isSuperuser: false, canLogin: true)
    try rm.createRole(writer)

    // Global SELECT grant covers any table
    rm.grant(.select, to: "writer", on: nil)
    #expect(rm.hasPermission("writer", .select, on: "any_table"))
    #expect(rm.hasPermission("writer", .select, on: "another_table"))
    #expect(!rm.hasPermission("writer", .insert, on: "any_table"))
}

// MARK: - Phase 7: COPY (CSV) Tests

@Test func testCopyExportCSV() throws {
    let rows = [
        Row(values: ["id": .integer(1), "name": .string("Alice"), "score": .double(95.5)]),
        Row(values: ["id": .integer(2), "name": .string("Bob"), "score": .double(87.3)]),
    ]
    let columns = ["id", "name", "score"]
    let csv = CopyExecutor.exportCSV(rows: rows, columns: columns)

    let lines = csv.components(separatedBy: "\n")
    #expect(lines[0] == "id,name,score")
    #expect(lines[1] == "1,Alice,95.5")
    #expect(lines[2] == "2,Bob,87.3")
}

@Test func testCopyParseCSV() throws {
    let csv = "id,name,score\n1,Alice,95.5\n2,Bob,87.3"
    let parsed = CopyExecutor.parseCSV(csv, columns: ["id", "name", "score"])

    #expect(parsed.count == 2)
    #expect(parsed[0]["id"] == .integer(1))
    #expect(parsed[0]["name"] == .string("Alice"))
    #expect(parsed[0]["score"] == .double(95.5))
    #expect(parsed[1]["id"] == .integer(2))
    #expect(parsed[1]["name"] == .string("Bob"))
}

@Test func testCopyRoundTrip() throws {
    let originalRows = [
        Row(values: ["id": .integer(1), "name": .string("Alice")]),
        Row(values: ["id": .integer(2), "name": .string("Bob")]),
        Row(values: ["id": .integer(3), "name": .string("Charlie")]),
    ]
    let columns = ["id", "name"]

    // Export
    let csv = CopyExecutor.exportCSV(rows: originalRows, columns: columns)

    // Parse back
    let parsed = CopyExecutor.parseCSV(csv, columns: columns)

    #expect(parsed.count == 3)
    #expect(parsed[0]["id"] == .integer(1))
    #expect(parsed[0]["name"] == .string("Alice"))
    #expect(parsed[2]["id"] == .integer(3))
    #expect(parsed[2]["name"] == .string("Charlie"))
}

@Test func testCopyCSVWithQuotedFields() throws {
    let rows = [
        Row(values: ["id": .integer(1), "desc": .string("hello, world")]),
        Row(values: ["id": .integer(2), "desc": .string("say \"hi\"")]),
    ]
    let csv = CopyExecutor.exportCSV(rows: rows, columns: ["id", "desc"])

    // Parse it back
    let parsed = CopyExecutor.parseCSV(csv, columns: ["id", "desc"])
    #expect(parsed.count == 2)
    #expect(parsed[0]["desc"] == .string("hello, world"))
    #expect(parsed[1]["desc"] == .string("say \"hi\""))
}

@Test func testCopyParseCSVAutoHeaders() throws {
    let csv = "x,y\n10,20\n30,40"
    let parsed = CopyExecutor.parseCSV(csv)
    #expect(parsed.count == 2)
    #expect(parsed[0]["x"] == .integer(10))
    #expect(parsed[0]["y"] == .integer(20))
    #expect(parsed[1]["x"] == .integer(30))
    #expect(parsed[1]["y"] == .integer(40))
}

// MARK: - SET / SHOW / RESET / DISCARD Parser Tests

@Test func testParseSetEquals() throws {
    let stmt = try Parser.parse("SET client_encoding = 'UTF8'")
    if case .set(let s) = stmt {
        #expect(s.name == "client_encoding")
        #expect(s.value == "UTF8")
    } else {
        #expect(Bool(false), "Expected .set statement")
    }
}

@Test func testParseSetTo() throws {
    let stmt = try Parser.parse("SET search_path TO public")
    if case .set(let s) = stmt {
        #expect(s.name == "search_path")
        #expect(s.value == "public")
    } else {
        #expect(Bool(false), "Expected .set statement")
    }
}

@Test func testParseShow() throws {
    let stmt = try Parser.parse("SHOW server_version")
    if case .show(let s) = stmt {
        #expect(s.name == "server_version")
    } else {
        #expect(Bool(false), "Expected .show statement")
    }
}

@Test func testParseShowAll() throws {
    let stmt = try Parser.parse("SHOW ALL")
    if case .show(let s) = stmt {
        #expect(s.name == "ALL")
    } else {
        #expect(Bool(false), "Expected .show statement")
    }
}

@Test func testParseReset() throws {
    let stmt = try Parser.parse("RESET client_encoding")
    if case .reset(let s) = stmt {
        #expect(s.name == "client_encoding")
    } else {
        #expect(Bool(false), "Expected .reset statement")
    }
}

@Test func testParseResetAll() throws {
    let stmt = try Parser.parse("RESET ALL")
    if case .reset(let s) = stmt {
        #expect(s.name == "ALL")
    } else {
        #expect(Bool(false), "Expected .reset statement")
    }
}

@Test func testParseDiscardAll() throws {
    let stmt = try Parser.parse("DISCARD ALL")
    if case .discard(let s) = stmt {
        #expect(s.target == "ALL")
    } else {
        #expect(Bool(false), "Expected .discard statement")
    }
}

@Test func testParseSchemaQualifiedTable() throws {
    let stmt = try Parser.parse("SELECT * FROM pg_catalog.pg_type")
    if case .select(let s) = stmt {
        if case .table(let name, _) = s.from {
            #expect(name == "pg_catalog.pg_type")
        } else {
            #expect(Bool(false), "Expected table ref")
        }
    } else {
        #expect(Bool(false), "Expected .select statement")
    }
}

@Test func testParseInformationSchemaTables() throws {
    let stmt = try Parser.parse("SELECT * FROM information_schema.tables")
    if case .select(let s) = stmt {
        if case .table(let name, _) = s.from {
            #expect(name == "information_schema.tables")
        } else {
            #expect(Bool(false), "Expected table ref")
        }
    } else {
        #expect(Bool(false), "Expected .select statement")
    }
}

// MARK: - SET / SHOW / RESET Execution Tests

@Test func testExecuteSetAndShow() async throws {
    let db = try await makeDB()

    // SET a parameter
    let setResult = try await db.execute(sql: "SET application_name = 'test_app'")
    guard case .ok = setResult else {
        #expect(Bool(false), "Expected .ok for SET")
        return
    }

    // SHOW that parameter
    let showResult = try await db.execute(sql: "SHOW application_name")
    guard case .rows(let rows) = showResult else {
        #expect(Bool(false), "Expected .rows for SHOW")
        return
    }
    #expect(rows.count == 1)
    #expect(rows[0].values["application_name"] == .string("test_app"))

    // RESET it
    let resetResult = try await db.execute(sql: "RESET application_name")
    guard case .ok = resetResult else {
        #expect(Bool(false), "Expected .ok for RESET")
        return
    }

    // Verify it's back to default
    let showResult2 = try await db.execute(sql: "SHOW application_name")
    guard case .rows(let rows2) = showResult2 else {
        #expect(Bool(false), "Expected .rows for SHOW")
        return
    }
    #expect(rows2[0].values["application_name"] == .string(""))
}

@Test func testExecuteShowAll() async throws {
    let db = try await makeDB()

    let result = try await db.execute(sql: "SHOW ALL")
    guard case .rows(let rows) = result else {
        #expect(Bool(false), "Expected .rows for SHOW ALL")
        return
    }
    #expect(rows.count > 0)
    // Check at least server_version is present
    let names = rows.compactMap { row -> String? in if case .string(let s) = row.values["name"] { return s } else { return nil } }
    #expect(names.contains("server_version"))
}

@Test func testExecuteInformationSchemaTables() async throws {
    let db = try await makeDB()

    // Create a table first
    _ = try await db.execute(sql: "CREATE TABLE test_users (id INTEGER PRIMARY KEY, name TEXT)")

    let result = try await db.execute(sql: "SELECT * FROM information_schema.tables")
    guard case .rows(let rows) = result else {
        #expect(Bool(false), "Expected .rows for information_schema.tables")
        return
    }
    let tableNames = rows.compactMap { row -> String? in if case .string(let s) = row.values["table_name"] { return s } else { return nil } }
    #expect(tableNames.contains("test_users"))
}

@Test func testExecutePgType() async throws {
    let db = try await makeDB()

    let result = try await db.execute(sql: "SELECT * FROM pg_type")
    guard case .rows(let rows) = result else {
        #expect(Bool(false), "Expected .rows for pg_type")
        return
    }
    #expect(rows.count > 0)
    let typeNames = rows.compactMap { row -> String? in if case .string(let s) = row.values["typname"] { return s } else { return nil } }
    #expect(typeNames.contains("bool"))
    #expect(typeNames.contains("int8"))
    #expect(typeNames.contains("text"))
    #expect(typeNames.contains("float8"))
    #expect(typeNames.contains("bytea"))
}

@Test func testExecuteDiscardAll() async throws {
    let db = try await makeDB()

    // Change a setting
    _ = try await db.execute(sql: "SET TimeZone = 'America/New_York'")

    // Discard all
    let result = try await db.execute(sql: "DISCARD ALL")
    guard case .ok = result else {
        #expect(Bool(false), "Expected .ok for DISCARD ALL")
        return
    }

    // Verify reset to default
    let showResult = try await db.execute(sql: "SHOW TimeZone")
    guard case .rows(let rows) = showResult else {
        #expect(Bool(false), "Expected .rows for SHOW")
        return
    }
    #expect(rows[0].values["TimeZone"] == .string("UTC"))
}

// MARK: - RETURNING Tests

@Test func testInsertReturningAll() async throws {
    let db = try await makeDB()
    _ = try await db.execute(sql: """
        CREATE TABLE items (
            id INTEGER PRIMARY KEY NOT NULL,
            name TEXT NOT NULL,
            price REAL
        )
    """)

    let result = try await db.execute(sql: "INSERT INTO items (id, name, price) VALUES (1, 'Widget', 9.99) RETURNING *")
    guard case .rows(let rows) = result else {
        #expect(Bool(false), "Expected .rows for INSERT RETURNING")
        return
    }
    #expect(rows.count == 1)
    #expect(rows[0].values["id"] == .integer(1))
    #expect(rows[0].values["name"] == .string("Widget"))
}

@Test func testUpdateReturningColumns() async throws {
    let db = try await makeDB()
    _ = try await db.execute(sql: """
        CREATE TABLE items (
            id INTEGER PRIMARY KEY NOT NULL,
            name TEXT NOT NULL,
            price REAL
        )
    """)
    _ = try await db.execute(sql: "INSERT INTO items (id, name, price) VALUES (1, 'Widget', 9.99)")

    let result = try await db.execute(sql: "UPDATE items SET price = 19.99 WHERE id = 1 RETURNING price")
    guard case .rows(let rows) = result else {
        #expect(Bool(false), "Expected .rows for UPDATE RETURNING")
        return
    }
    #expect(rows.count == 1)
    #expect(rows[0].values["price"] == .double(19.99))
}

@Test func testDeleteReturningAll() async throws {
    let db = try await makeDB()
    _ = try await db.execute(sql: """
        CREATE TABLE items (
            id INTEGER PRIMARY KEY NOT NULL,
            name TEXT NOT NULL
        )
    """)
    _ = try await db.execute(sql: "INSERT INTO items (id, name) VALUES (1, 'Widget')")
    _ = try await db.execute(sql: "INSERT INTO items (id, name) VALUES (2, 'Gadget')")

    let result = try await db.execute(sql: "DELETE FROM items WHERE id = 1 RETURNING *")
    guard case .rows(let rows) = result else {
        #expect(Bool(false), "Expected .rows for DELETE RETURNING")
        return
    }
    #expect(rows.count == 1)
    #expect(rows[0].values["id"] == .integer(1))
    #expect(rows[0].values["name"] == .string("Widget"))

    // Verify the row was actually deleted
    let remaining = try await db.execute(sql: "SELECT * FROM items")
    guard case .rows(let remRows) = remaining else { return }
    #expect(remRows.count == 1)
}

// MARK: - ON CONFLICT Tests

@Test func testInsertOnConflictDoNothing() async throws {
    let db = try await makeDB()
    _ = try await db.execute(sql: """
        CREATE TABLE items (
            id INTEGER PRIMARY KEY NOT NULL,
            name TEXT NOT NULL
        )
    """)
    _ = try await db.execute(sql: "INSERT INTO items (id, name) VALUES (1, 'Widget')")

    // This should not throw
    let result = try await db.execute(sql: "INSERT INTO items (id, name) VALUES (1, 'Duplicate') ON CONFLICT (id) DO NOTHING")
    guard case .rowCount(let count) = result else {
        #expect(Bool(false), "Expected .rowCount for ON CONFLICT DO NOTHING")
        return
    }
    #expect(count == 1)

    // Verify original row is unchanged
    let rows = try await db.execute(sql: "SELECT * FROM items WHERE id = 1")
    guard case .rows(let r) = rows else { return }
    #expect(r[0].values["name"] == .string("Widget"))
}

@Test func testInsertOnConflictDoUpdate() async throws {
    let db = try await makeDB()
    _ = try await db.execute(sql: """
        CREATE TABLE items (
            id INTEGER PRIMARY KEY NOT NULL,
            name TEXT NOT NULL,
            price REAL
        )
    """)
    _ = try await db.execute(sql: "INSERT INTO items (id, name, price) VALUES (1, 'Widget', 9.99)")

    // Upsert: update name and price on conflict
    _ = try await db.execute(sql: "INSERT INTO items (id, name, price) VALUES (1, 'Super Widget', 29.99) ON CONFLICT (id) DO UPDATE SET name = 'Super Widget', price = 29.99")

    let result = try await db.execute(sql: "SELECT * FROM items WHERE id = 1")
    guard case .rows(let rows) = result else {
        #expect(Bool(false), "Expected rows")
        return
    }
    #expect(rows.count == 1)
    #expect(rows[0].values["name"] == .string("Super Widget"))
    #expect(rows[0].values["price"] == .double(29.99))
}

@Test func testInsertOnConflictDoUpdatePropagatesError() async throws {
    let db = try await makeDB()
    // ON CONFLICT DO UPDATE should succeed when PK exists and update is valid
    _ = try await db.execute(sql: """
        CREATE TABLE conflict_items (
            id INTEGER PRIMARY KEY NOT NULL,
            name TEXT NOT NULL,
            count INTEGER
        )
    """)
    _ = try await db.execute(sql: "INSERT INTO conflict_items (id, name, count) VALUES (1, 'Widget', 10)")

    // Upsert: conflicting insert should perform the update and not swallow errors
    _ = try await db.execute(sql: "INSERT INTO conflict_items (id, name, count) VALUES (1, 'Ignored', 5) ON CONFLICT (id) DO UPDATE SET name = 'Updated', count = 20")

    let result = try await db.execute(sql: "SELECT * FROM conflict_items WHERE id = 1")
    guard case .rows(let rows) = result else {
        #expect(Bool(false), "Expected rows")
        return
    }
    #expect(rows.count == 1)
    #expect(rows[0].values["name"] == .string("Updated"))
    #expect(rows[0].values["count"] == .integer(20))
}

// MARK: - UNION / INTERSECT / EXCEPT Tests

@Test func testUnion() async throws {
    let db = try await makeDB()
    _ = try await db.execute(sql: """
        CREATE TABLE t1 (id INTEGER PRIMARY KEY NOT NULL, val TEXT NOT NULL)
    """)
    _ = try await db.execute(sql: """
        CREATE TABLE t2 (id INTEGER PRIMARY KEY NOT NULL, val TEXT NOT NULL)
    """)
    _ = try await db.execute(sql: "INSERT INTO t1 (id, val) VALUES (1, 'a')")
    _ = try await db.execute(sql: "INSERT INTO t1 (id, val) VALUES (2, 'b')")
    _ = try await db.execute(sql: "INSERT INTO t2 (id, val) VALUES (2, 'b')")
    _ = try await db.execute(sql: "INSERT INTO t2 (id, val) VALUES (3, 'c')")

    let result = try await db.execute(sql: "SELECT val FROM t1 UNION SELECT val FROM t2")
    guard case .rows(let rows) = result else {
        #expect(Bool(false), "Expected rows for UNION")
        return
    }
    // UNION deduplicates: should have 3 distinct values
    #expect(rows.count == 3)
}

@Test func testUnionAll() async throws {
    let db = try await makeDB()
    _ = try await db.execute(sql: """
        CREATE TABLE t1 (id INTEGER PRIMARY KEY NOT NULL, val TEXT NOT NULL)
    """)
    _ = try await db.execute(sql: """
        CREATE TABLE t2 (id INTEGER PRIMARY KEY NOT NULL, val TEXT NOT NULL)
    """)
    _ = try await db.execute(sql: "INSERT INTO t1 (id, val) VALUES (1, 'a')")
    _ = try await db.execute(sql: "INSERT INTO t1 (id, val) VALUES (2, 'b')")
    _ = try await db.execute(sql: "INSERT INTO t2 (id, val) VALUES (2, 'b')")
    _ = try await db.execute(sql: "INSERT INTO t2 (id, val) VALUES (3, 'c')")

    let result = try await db.execute(sql: "SELECT val FROM t1 UNION ALL SELECT val FROM t2")
    guard case .rows(let rows) = result else {
        #expect(Bool(false), "Expected rows for UNION ALL")
        return
    }
    // UNION ALL preserves duplicates: 4 rows total
    #expect(rows.count == 4)
}

@Test func testIntersect() async throws {
    let db = try await makeDB()
    _ = try await db.execute(sql: """
        CREATE TABLE t1 (id INTEGER PRIMARY KEY NOT NULL, val TEXT NOT NULL)
    """)
    _ = try await db.execute(sql: """
        CREATE TABLE t2 (id INTEGER PRIMARY KEY NOT NULL, val TEXT NOT NULL)
    """)
    _ = try await db.execute(sql: "INSERT INTO t1 (id, val) VALUES (1, 'a')")
    _ = try await db.execute(sql: "INSERT INTO t1 (id, val) VALUES (2, 'b')")
    _ = try await db.execute(sql: "INSERT INTO t2 (id, val) VALUES (2, 'b')")
    _ = try await db.execute(sql: "INSERT INTO t2 (id, val) VALUES (3, 'c')")

    let result = try await db.execute(sql: "SELECT val FROM t1 INTERSECT SELECT val FROM t2")
    guard case .rows(let rows) = result else {
        #expect(Bool(false), "Expected rows for INTERSECT")
        return
    }
    // INTERSECT: only 'b' is common
    #expect(rows.count == 1)
    #expect(rows[0].values["val"] == .string("b"))
}

@Test func testExcept() async throws {
    let db = try await makeDB()
    _ = try await db.execute(sql: """
        CREATE TABLE t1 (id INTEGER PRIMARY KEY NOT NULL, val TEXT NOT NULL)
    """)
    _ = try await db.execute(sql: """
        CREATE TABLE t2 (id INTEGER PRIMARY KEY NOT NULL, val TEXT NOT NULL)
    """)
    _ = try await db.execute(sql: "INSERT INTO t1 (id, val) VALUES (1, 'a')")
    _ = try await db.execute(sql: "INSERT INTO t1 (id, val) VALUES (2, 'b')")
    _ = try await db.execute(sql: "INSERT INTO t2 (id, val) VALUES (2, 'b')")
    _ = try await db.execute(sql: "INSERT INTO t2 (id, val) VALUES (3, 'c')")

    let result = try await db.execute(sql: "SELECT val FROM t1 EXCEPT SELECT val FROM t2")
    guard case .rows(let rows) = result else {
        #expect(Bool(false), "Expected rows for EXCEPT")
        return
    }
    // EXCEPT: 'a' is in t1 but not t2
    #expect(rows.count == 1)
    #expect(rows[0].values["val"] == .string("a"))
}

// MARK: - CREATE VIEW / DROP VIEW Tests

@Test func testCreateViewAndSelectFromView() async throws {
    let db = try await makeDB()
    _ = try await db.execute(sql: "CREATE TABLE employees (id INTEGER PRIMARY KEY, name TEXT, dept TEXT)")
    _ = try await db.execute(sql: "INSERT INTO employees (id, name, dept) VALUES (1, 'Alice', 'eng')")
    _ = try await db.execute(sql: "INSERT INTO employees (id, name, dept) VALUES (2, 'Bob', 'eng')")
    _ = try await db.execute(sql: "INSERT INTO employees (id, name, dept) VALUES (3, 'Carol', 'sales')")

    // Create a view
    _ = try await db.execute(sql: "CREATE VIEW eng_employees AS SELECT id, name FROM employees WHERE dept = 'eng'")

    // Select from the view
    let result = try await db.execute(sql: "SELECT * FROM eng_employees")
    guard case .rows(let rows) = result else {
        Issue.record("Expected rows from view")
        return
    }
    #expect(rows.count == 2)
    let names = Set(rows.compactMap { row -> String? in
        if case .string(let s) = row.values["name"] { return s }
        return nil
    })
    #expect(names == ["Alice", "Bob"])

    try await db.close()
}

@Test func testDropView() async throws {
    let db = try await makeDB()
    _ = try await db.execute(sql: "CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)")
    _ = try await db.execute(sql: "INSERT INTO items (id, name) VALUES (1, 'Widget')")

    _ = try await db.execute(sql: "CREATE VIEW all_items AS SELECT * FROM items")

    // View should work
    let r1 = try await db.execute(sql: "SELECT * FROM all_items")
    guard case .rows(let rows1) = r1 else {
        Issue.record("Expected rows")
        return
    }
    #expect(rows1.count == 1)

    // Drop the view
    _ = try await db.execute(sql: "DROP VIEW all_items")

    // Selecting from dropped view should fail (table not found)
    do {
        _ = try await db.execute(sql: "SELECT * FROM all_items")
        Issue.record("Should have thrown")
    } catch {
        // Expected
    }

    // DROP VIEW IF EXISTS on non-existent view should not throw
    _ = try await db.execute(sql: "DROP VIEW IF EXISTS all_items")

    try await db.close()
}

@Test func testCreateOrReplaceView() async throws {
    let db = try await makeDB()
    _ = try await db.execute(sql: "CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT, price INTEGER)")
    _ = try await db.execute(sql: "INSERT INTO products (id, name, price) VALUES (1, 'A', 10)")
    _ = try await db.execute(sql: "INSERT INTO products (id, name, price) VALUES (2, 'B', 20)")
    _ = try await db.execute(sql: "INSERT INTO products (id, name, price) VALUES (3, 'C', 30)")

    // Create a view
    _ = try await db.execute(sql: "CREATE VIEW cheap AS SELECT * FROM products WHERE price < 25")

    let r1 = try await db.execute(sql: "SELECT * FROM cheap")
    guard case .rows(let rows1) = r1 else {
        Issue.record("Expected rows")
        return
    }
    #expect(rows1.count == 2)

    // CREATE without OR REPLACE should fail on existing view
    do {
        _ = try await db.execute(sql: "CREATE VIEW cheap AS SELECT * FROM products WHERE price < 15")
        Issue.record("Should have thrown for duplicate view")
    } catch {
        // Expected
    }

    // CREATE OR REPLACE should succeed
    _ = try await db.execute(sql: "CREATE OR REPLACE VIEW cheap AS SELECT * FROM products WHERE price < 15")

    let r2 = try await db.execute(sql: "SELECT * FROM cheap")
    guard case .rows(let rows2) = r2 else {
        Issue.record("Expected rows")
        return
    }
    #expect(rows2.count == 1)
    #expect(rows2[0].values["name"] == .string("A"))

    try await db.close()
}

@Test func testViewWithWhereOnTop() async throws {
    let db = try await makeDB()
    _ = try await db.execute(sql: "CREATE TABLE data (id INTEGER PRIMARY KEY, val INTEGER)")
    _ = try await db.execute(sql: "INSERT INTO data (id, val) VALUES (1, 10)")
    _ = try await db.execute(sql: "INSERT INTO data (id, val) VALUES (2, 20)")
    _ = try await db.execute(sql: "INSERT INTO data (id, val) VALUES (3, 30)")
    _ = try await db.execute(sql: "INSERT INTO data (id, val) VALUES (4, 40)")

    _ = try await db.execute(sql: "CREATE VIEW big_vals AS SELECT * FROM data WHERE val > 15")

    // Query view with additional WHERE
    let result = try await db.execute(sql: "SELECT * FROM big_vals WHERE val < 35")
    guard case .rows(let rows) = result else {
        Issue.record("Expected rows")
        return
    }
    #expect(rows.count == 2)
    let vals = Set(rows.compactMap { $0.values["val"] })
    #expect(vals == [.integer(20), .integer(30)])

    try await db.close()
}

// MARK: - Parser Tests for VIEW syntax

@Test func testParseCreateView() throws {
    let stmt = try Parser.parse("CREATE VIEW myview AS SELECT id, name FROM users")
    if case .createView(let v) = stmt {
        #expect(v.name == "myview")
        #expect(v.orReplace == false)
        #expect(v.columns == nil)
    } else {
        Issue.record("Expected createView statement")
    }
}

@Test func testParseCreateOrReplaceView() throws {
    let stmt = try Parser.parse("CREATE OR REPLACE VIEW myview AS SELECT * FROM users")
    if case .createView(let v) = stmt {
        #expect(v.name == "myview")
        #expect(v.orReplace == true)
    } else {
        Issue.record("Expected createView statement")
    }
}

@Test func testParseCreateViewWithColumns() throws {
    let stmt = try Parser.parse("CREATE VIEW myview (a, b) AS SELECT id, name FROM users")
    if case .createView(let v) = stmt {
        #expect(v.name == "myview")
        #expect(v.columns == ["a", "b"])
    } else {
        Issue.record("Expected createView statement")
    }
}

@Test func testParseDropView() throws {
    let stmt = try Parser.parse("DROP VIEW myview")
    if case .dropView(let v) = stmt {
        #expect(v.name == "myview")
        #expect(v.ifExists == false)
    } else {
        Issue.record("Expected dropView statement")
    }
}

@Test func testParseDropViewIfExists() throws {
    let stmt = try Parser.parse("DROP VIEW IF EXISTS myview")
    if case .dropView(let v) = stmt {
        #expect(v.name == "myview")
        #expect(v.ifExists == true)
    } else {
        Issue.record("Expected dropView statement")
    }
}

// MARK: - COALESCE / NULLIF / Conditional Functions

@Test func testCoalesceReturnsFirstNonNull() async throws {
    let db = try await makeDB()
    let r1 = try await db.execute(sql: "SELECT coalesce(NULL, NULL, 'third') AS val")
    guard case .rows(let rows1) = r1 else { Issue.record("Expected rows"); return }
    #expect(rows1[0]["val"] == .string("third"))

    let r2 = try await db.execute(sql: "SELECT coalesce('first', 'second') AS val")
    guard case .rows(let rows2) = r2 else { Issue.record("Expected rows"); return }
    #expect(rows2[0]["val"] == .string("first"))

    let r3 = try await db.execute(sql: "SELECT coalesce(NULL, NULL) AS val")
    guard case .rows(let rows3) = r3 else { Issue.record("Expected rows"); return }
    #expect(rows3[0]["val"] == .null)

    try await db.close()
}

@Test func testNullIfReturnsNullWhenEqual() async throws {
    let db = try await makeDB()
    let r1 = try await db.execute(sql: "SELECT nullif(1, 1) AS val")
    guard case .rows(let rows1) = r1 else { Issue.record("Expected rows"); return }
    #expect(rows1[0]["val"] == .null)

    let r2 = try await db.execute(sql: "SELECT nullif(1, 2) AS val")
    guard case .rows(let rows2) = r2 else { Issue.record("Expected rows"); return }
    #expect(rows2[0]["val"] == .integer(1))

    try await db.close()
}

@Test func testGreatestAndLeast() async throws {
    let db = try await makeDB()
    let r1 = try await db.execute(sql: "SELECT greatest(3, 1, 4, 1, 5) AS val")
    guard case .rows(let rows1) = r1 else { Issue.record("Expected rows"); return }
    #expect(rows1[0]["val"] == .integer(5))

    let r2 = try await db.execute(sql: "SELECT least(3, 1, 4, 1, 5) AS val")
    guard case .rows(let rows2) = r2 else { Issue.record("Expected rows"); return }
    #expect(rows2[0]["val"] == .integer(1))

    try await db.close()
}

// MARK: - gen_random_uuid()

@Test func testGenRandomUuid() async throws {
    let db = try await makeDB()
    let result = try await db.execute(sql: "SELECT gen_random_uuid() AS val")
    guard case .rows(let rows) = result else { Issue.record("Expected rows"); return }
    guard case .string(let uuid) = rows[0]["val"] else { Issue.record("Expected string UUID"); return }
    // UUID format: 8-4-4-4-12 hex chars
    let parts = uuid.split(separator: "-")
    #expect(parts.count == 5)
    #expect(parts[0].count == 8)
    #expect(parts[1].count == 4)
    #expect(parts[2].count == 4)
    #expect(parts[3].count == 4)
    #expect(parts[4].count == 12)
    try await db.close()
}

// MARK: - :: Type Casting Syntax

@Test func testDoubleColonTypeCast() async throws {
    let db = try await makeDB()
    let r1 = try await db.execute(sql: "SELECT '42'::integer AS val")
    guard case .rows(let rows1) = r1 else { Issue.record("Expected rows"); return }
    #expect(rows1[0]["val"] == .integer(42))

    let r2 = try await db.execute(sql: "SELECT 123::text AS val")
    guard case .rows(let rows2) = r2 else { Issue.record("Expected rows"); return }
    #expect(rows2[0]["val"] == .string("123"))

    try await db.close()
}

@Test func testDoubleColonCastParses() throws {
    let stmt = try Parser.parse("SELECT '42'::integer AS val")
    if case .select(let sel) = stmt,
       case .expression(let expr, let alias) = sel.columns[0] {
        #expect(alias == "val")
        if case .cast(let inner, let dt) = expr {
            if case .stringLiteral(let s) = inner { #expect(s == "42") }
            else { Issue.record("Expected string literal '42'") }
            if case .integer = dt {} else { Issue.record("Expected INTEGER data type") }
        } else {
            Issue.record("Expected cast expression")
        }
    } else {
        Issue.record("Expected select statement")
    }
}

// MARK: - Column Alias Resolution in ORDER BY

@Test func testColumnAliasInOrderBy() async throws {
    let db = try await makeDB()
    _ = try await db.execute(sql: "CREATE TABLE alias_test (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)")
    _ = try await db.execute(sql: "INSERT INTO alias_test (id, name, age) VALUES (1, 'Charlie', 30)")
    _ = try await db.execute(sql: "INSERT INTO alias_test (id, name, age) VALUES (2, 'Alice', 25)")
    _ = try await db.execute(sql: "INSERT INTO alias_test (id, name, age) VALUES (3, 'Bob', 35)")

    let result = try await db.execute(sql: "SELECT name AS n, age AS a FROM alias_test ORDER BY n")
    guard case .rows(let rows) = result else { Issue.record("Expected rows"); return }
    #expect(rows.count == 3)
    #expect(rows[0]["n"] == .string("Alice"))
    #expect(rows[1]["n"] == .string("Bob"))
    #expect(rows[2]["n"] == .string("Charlie"))
    try await db.close()
}

// MARK: - Table Alias Resolution

@Test func testTableAliasInSelect() async throws {
    let db = try await makeDB()
    _ = try await db.execute(sql: "CREATE TABLE talias_users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)")
    _ = try await db.execute(sql: "INSERT INTO talias_users (id, name, age) VALUES (1, 'Alice', 30)")
    _ = try await db.execute(sql: "INSERT INTO talias_users (id, name, age) VALUES (2, 'Bob', 25)")

    let result = try await db.execute(sql: "SELECT u.name FROM talias_users u WHERE u.age > 25")
    guard case .rows(let rows) = result else { Issue.record("Expected rows"); return }
    #expect(rows.count == 1)
    #expect(rows[0]["name"] == .string("Alice"))
    try await db.close()
}

// MARK: - Boolean Shorthand in WHERE

@Test func testBooleanShorthandInWhere() async throws {
    let db = try await makeDB()
    _ = try await db.execute(sql: "CREATE TABLE bool_test (id INTEGER PRIMARY KEY, name TEXT, active BOOLEAN)")
    _ = try await db.execute(sql: "INSERT INTO bool_test (id, name, active) VALUES (1, 'Alice', TRUE)")
    _ = try await db.execute(sql: "INSERT INTO bool_test (id, name, active) VALUES (2, 'Bob', FALSE)")

    let r1 = try await db.execute(sql: "SELECT name FROM bool_test WHERE active")
    guard case .rows(let rows1) = r1 else { Issue.record("Expected rows"); return }
    #expect(rows1.count == 1)
    #expect(rows1[0]["name"] == .string("Alice"))

    let r2 = try await db.execute(sql: "SELECT name FROM bool_test WHERE NOT active")
    guard case .rows(let rows2) = r2 else { Issue.record("Expected rows"); return }
    #expect(rows2.count == 1)
    #expect(rows2[0]["name"] == .string("Bob"))

    try await db.close()
}

// MARK: - Additional String Functions

@Test func testLeftRightFunctions() async throws {
    let db = try await makeDB()
    let r1 = try await db.execute(sql: "SELECT left('hello', 3) AS val")
    guard case .rows(let rows1) = r1 else { Issue.record("Expected rows"); return }
    #expect(rows1[0]["val"] == .string("hel"))

    let r2 = try await db.execute(sql: "SELECT right('hello', 3) AS val")
    guard case .rows(let rows2) = r2 else { Issue.record("Expected rows"); return }
    #expect(rows2[0]["val"] == .string("llo"))
    try await db.close()
}

@Test func testLpadRpadFunctions() async throws {
    let db = try await makeDB()
    let r1 = try await db.execute(sql: "SELECT lpad('hi', 5, 'xy') AS val")
    guard case .rows(let rows1) = r1 else { Issue.record("Expected rows"); return }
    #expect(rows1[0]["val"] == .string("xyxhi"))

    let r2 = try await db.execute(sql: "SELECT rpad('hi', 5, 'xy') AS val")
    guard case .rows(let rows2) = r2 else { Issue.record("Expected rows"); return }
    #expect(rows2[0]["val"] == .string("hixyx"))
    try await db.close()
}

@Test func testRepeatReverseFunctions() async throws {
    let db = try await makeDB()
    let r1 = try await db.execute(sql: "SELECT repeat('ab', 3) AS val")
    guard case .rows(let rows1) = r1 else { Issue.record("Expected rows"); return }
    #expect(rows1[0]["val"] == .string("ababab"))

    let r2 = try await db.execute(sql: "SELECT reverse('hello') AS val")
    guard case .rows(let rows2) = r2 else { Issue.record("Expected rows"); return }
    #expect(rows2[0]["val"] == .string("olleh"))
    try await db.close()
}

@Test func testMd5Function() async throws {
    let db = try await makeDB()
    let result = try await db.execute(sql: "SELECT md5('hello') AS val")
    guard case .rows(let rows) = result else { Issue.record("Expected rows"); return }
    #expect(rows[0]["val"] == .string("5d41402abc4b2a76b9719d911017c592"))
    try await db.close()
}

@Test func testOctetLengthFunction() async throws {
    let db = try await makeDB()
    let result = try await db.execute(sql: "SELECT octet_length('hello') AS val")
    guard case .rows(let rows) = result else { Issue.record("Expected rows"); return }
    #expect(rows[0]["val"] == .integer(5))
    try await db.close()
}

@Test func testPositionFunction() async throws {
    let db = try await makeDB()
    let result = try await db.execute(sql: "SELECT position('lo', 'hello') AS val")
    guard case .rows(let rows) = result else { Issue.record("Expected rows"); return }
    #expect(rows[0]["val"] == .integer(4))
    try await db.close()
}

// MARK: - View Tests

@Test func testViewCreationAndQuery() async throws {
    let db = try await makeDB()

    _ = try await db.execute(sql: """
        CREATE TABLE employees (id INTEGER PRIMARY KEY NOT NULL, name TEXT, dept TEXT)
    """)
    _ = try await db.execute(sql: "INSERT INTO employees (id, name, dept) VALUES (1, 'Alice', 'eng')")
    _ = try await db.execute(sql: "INSERT INTO employees (id, name, dept) VALUES (2, 'Bob', 'sales')")
    _ = try await db.execute(sql: "INSERT INTO employees (id, name, dept) VALUES (3, 'Carol', 'eng')")
    _ = try await db.execute(sql: "CREATE VIEW eng_team AS SELECT * FROM employees WHERE dept = 'eng'")

    // Query the view
    let result = try await db.execute(sql: "SELECT * FROM eng_team")
    guard case .rows(let rows) = result else { Issue.record("Expected rows"); return }
    #expect(rows.count == 2)
    let names = Set(rows.compactMap { row -> String? in
        if case .string(let n) = row["name"] { return n }
        return nil
    })
    #expect(names.contains("Alice"))
    #expect(names.contains("Carol"))

    // Insert more data, view should reflect it
    _ = try await db.execute(sql: "INSERT INTO employees (id, name, dept) VALUES (4, 'Dave', 'eng')")
    let result2 = try await db.execute(sql: "SELECT * FROM eng_team")
    guard case .rows(let rows2) = result2 else { Issue.record("Expected rows"); return }
    #expect(rows2.count == 3)

    // Drop view
    _ = try await db.execute(sql: "DROP VIEW eng_team")
    await #expect(throws: Error.self) {
        _ = try await db.execute(sql: "SELECT * FROM eng_team")
    }

    try await db.close()
}

// MARK: - Trigger Tests

@Test func testTriggerFiresMultipleTimes() async throws {
    let db = try await makeDB()

    _ = try await db.execute(sql: "CREATE TABLE orders (id INTEGER PRIMARY KEY NOT NULL, product TEXT)")
    _ = try await db.execute(sql: "CREATE TABLE audit (id SERIAL, action TEXT)")
    _ = try await db.execute(sql: """
        CREATE TRIGGER trg_multi AFTER INSERT ON orders FOR EACH ROW BEGIN INSERT INTO audit (action) VALUES ('order_added'); END
    """)

    // Insert multiple orders — trigger should fire each time
    _ = try await db.execute(sql: "INSERT INTO orders (id, product) VALUES (1, 'Widget')")
    _ = try await db.execute(sql: "INSERT INTO orders (id, product) VALUES (2, 'Gadget')")
    _ = try await db.execute(sql: "INSERT INTO orders (id, product) VALUES (3, 'Donut')")

    let result = try await db.execute(sql: "SELECT * FROM audit")
    guard case .rows(let rows) = result else { Issue.record("Expected rows"); return }
    #expect(rows.count == 3)
    for row in rows {
        #expect(row["action"] == .string("order_added"))
    }

    try await db.close()
}

// MARK: - SERIAL Counter Tests

@Test func testSerialAutoIncrementInSameSession() async throws {
    let db = try await makeDB()

    _ = try await db.execute(sql: "CREATE TABLE events (id SERIAL, name TEXT NOT NULL)")
    _ = try await db.execute(sql: "INSERT INTO events (name) VALUES ('first')")
    _ = try await db.execute(sql: "INSERT INTO events (name) VALUES ('second')")
    _ = try await db.execute(sql: "INSERT INTO events (name) VALUES ('third')")
    _ = try await db.execute(sql: "INSERT INTO events (name) VALUES ('fourth')")
    _ = try await db.execute(sql: "INSERT INTO events (name) VALUES ('fifth')")

    let result = try await db.execute(sql: "SELECT * FROM events ORDER BY id")
    guard case .rows(let rows) = result else { Issue.record("Expected rows"); return }
    #expect(rows.count == 5)
    // Verify sequential IDs
    for i in 0..<5 {
        #expect(rows[i]["id"] == .integer(Int64(i + 1)))
    }
    #expect(rows[0]["name"] == .string("first"))
    #expect(rows[4]["name"] == .string("fifth"))

    try await db.close()
}

@Test func testSerialDataPersistsAcrossReopen() async throws {
    let path = NSTemporaryDirectory() + "cobalt_serial_persist_\(UUID().uuidString).cobalt"
    defer { try? FileManager.default.removeItem(atPath: path) }

    // Session 1: create SERIAL table, insert 3 rows, close
    let db1 = try await CobaltDatabase(configuration: CobaltConfiguration(path: path))
    _ = try await db1.execute(sql: "CREATE TABLE events (id SERIAL, name TEXT NOT NULL)")
    _ = try await db1.execute(sql: "INSERT INTO events (name) VALUES ('first')")
    _ = try await db1.execute(sql: "INSERT INTO events (name) VALUES ('second')")
    _ = try await db1.execute(sql: "INSERT INTO events (name) VALUES ('third')")
    try await db1.close()

    // Session 2: reopen, verify existing SERIAL data survived
    let db2 = try await CobaltDatabase(configuration: CobaltConfiguration(path: path))
    let result = try await db2.execute(sql: "SELECT * FROM events ORDER BY id")
    guard case .rows(let rows) = result else { Issue.record("Expected rows"); return }
    #expect(rows.count == 3)
    #expect(rows[0]["id"] == .integer(1))
    #expect(rows[1]["id"] == .integer(2))
    #expect(rows[2]["id"] == .integer(3))
    #expect(rows[0]["name"] == .string("first"))
    #expect(rows[2]["name"] == .string("third"))

    try await db2.close()
}

// MARK: - SQL Feature Execution Tests

@Test func testUnionExecution() async throws {
    let db = try await makeDB()

    _ = try await db.execute(sql: "CREATE TABLE fruits (id INTEGER PRIMARY KEY NOT NULL, name TEXT)")
    _ = try await db.execute(sql: "CREATE TABLE veggies (id INTEGER PRIMARY KEY NOT NULL, name TEXT)")
    _ = try await db.execute(sql: "INSERT INTO fruits (id, name) VALUES (1, 'Apple')")
    _ = try await db.execute(sql: "INSERT INTO fruits (id, name) VALUES (2, 'Banana')")
    _ = try await db.execute(sql: "INSERT INTO veggies (id, name) VALUES (1, 'Carrot')")
    _ = try await db.execute(sql: "INSERT INTO veggies (id, name) VALUES (2, 'Daikon')")

    let result = try await db.execute(sql: "SELECT name FROM fruits UNION SELECT name FROM veggies")
    guard case .rows(let rows) = result else { Issue.record("Expected rows"); return }
    #expect(rows.count == 4)
    let names = Set(rows.compactMap { row -> String? in
        if case .string(let n) = row["name"] { return n }
        return nil
    })
    #expect(names == ["Apple", "Banana", "Carrot", "Daikon"])

    try await db.close()
}

@Test func testUnionAllWithDuplicates() async throws {
    let db = try await makeDB()

    _ = try await db.execute(sql: "CREATE TABLE t1 (id INTEGER PRIMARY KEY NOT NULL, name TEXT)")
    _ = try await db.execute(sql: "CREATE TABLE t2 (id INTEGER PRIMARY KEY NOT NULL, name TEXT)")
    _ = try await db.execute(sql: "INSERT INTO t1 (id, name) VALUES (1, 'Shared')")
    _ = try await db.execute(sql: "INSERT INTO t2 (id, name) VALUES (1, 'Shared')")

    // UNION removes duplicates
    let unionResult = try await db.execute(sql: "SELECT name FROM t1 UNION SELECT name FROM t2")
    guard case .rows(let unionRows) = unionResult else { Issue.record("Expected rows"); return }
    #expect(unionRows.count == 1)

    // UNION ALL keeps duplicates
    let unionAllResult = try await db.execute(sql: "SELECT name FROM t1 UNION ALL SELECT name FROM t2")
    guard case .rows(let allRows) = unionAllResult else { Issue.record("Expected rows"); return }
    #expect(allRows.count == 2)

    try await db.close()
}

@Test func testGroupByExecution() async throws {
    let db = try await makeDB()

    _ = try await db.execute(sql: "CREATE TABLE sales (id INTEGER PRIMARY KEY NOT NULL, product TEXT, amount REAL)")
    _ = try await db.execute(sql: "INSERT INTO sales (id, product, amount) VALUES (1, 'A', 10.0)")
    _ = try await db.execute(sql: "INSERT INTO sales (id, product, amount) VALUES (2, 'A', 20.0)")
    _ = try await db.execute(sql: "INSERT INTO sales (id, product, amount) VALUES (3, 'A', 30.0)")
    _ = try await db.execute(sql: "INSERT INTO sales (id, product, amount) VALUES (4, 'B', 5.0)")
    _ = try await db.execute(sql: "INSERT INTO sales (id, product, amount) VALUES (5, 'B', 15.0)")
    _ = try await db.execute(sql: "INSERT INTO sales (id, product, amount) VALUES (6, 'C', 100.0)")

    // GROUP BY without HAVING
    let result = try await db.execute(sql: "SELECT product, COUNT(*) AS cnt FROM sales GROUP BY product")
    guard case .rows(let rows) = result else { Issue.record("Expected rows"); return }
    #expect(rows.count == 3) // A, B, C

    let countMap = Dictionary(uniqueKeysWithValues: rows.compactMap { row -> (String, Int64)? in
        guard case .string(let p) = row["product"],
              case .integer(let c) = row["COUNT(*)"] else { return nil }
        return (p, c)
    })
    #expect(countMap["A"] == 3)
    #expect(countMap["B"] == 2)
    #expect(countMap["C"] == 1)

    try await db.close()
}

@Test func testGroupByWithHavingNativeAPI() async throws {
    let path = NSTemporaryDirectory() + "cobalt_groupby_having_\(UUID().uuidString).cobalt"
    defer { try? FileManager.default.removeItem(atPath: path) }

    let db = try await CobaltDatabase(configuration: CobaltConfiguration(path: path))
    try await db.createTable(CobaltTableSchema(name: "sales", columns: [
        CobaltColumn(name: "id", type: .integer, isPrimaryKey: true, isNullable: false),
        CobaltColumn(name: "product", type: .string),
        CobaltColumn(name: "amount", type: .double),
    ]))

    let data: [(Int64, String, Double)] = [
        (1, "A", 10.0), (2, "A", 20.0), (3, "A", 30.0),
        (4, "B", 5.0), (5, "B", 15.0),
        (6, "C", 100.0),
    ]
    for (id, product, amount) in data {
        try await db.insert(into: "sales", values: [
            "id": .integer(id), "product": .string(product), "amount": .double(amount),
        ])
    }

    // GROUP BY with HAVING via native API — filter where count > 1
    let rows = try await db.select(
        from: "sales",
        select: [.column("product"), .count(column: nil)],
        groupBy: ["product"],
        having: .greaterThan(column: "COUNT(*)", value: .integer(1))
    )
    // A has 3 rows, B has 2 rows — both pass HAVING; C has 1 row — filtered out
    #expect(rows.count == 2)
    let products = Set(rows.compactMap { row -> String? in
        if case .string(let p) = row["product"] { return p }
        return nil
    })
    #expect(products == ["A", "B"])

    try await db.close()
}

@Test func testLeftJoinExecution() async throws {
    let db = try await makeDB()

    _ = try await db.execute(sql: "CREATE TABLE customers (id INTEGER PRIMARY KEY NOT NULL, name TEXT)")
    _ = try await db.execute(sql: "CREATE TABLE orders (id INTEGER PRIMARY KEY NOT NULL, customer_id INTEGER, product TEXT)")
    _ = try await db.execute(sql: "INSERT INTO customers (id, name) VALUES (1, 'Alice')")
    _ = try await db.execute(sql: "INSERT INTO customers (id, name) VALUES (2, 'Bob')")
    _ = try await db.execute(sql: "INSERT INTO customers (id, name) VALUES (3, 'Carol')")
    _ = try await db.execute(sql: "INSERT INTO orders (id, customer_id, product) VALUES (1, 1, 'Widget')")
    _ = try await db.execute(sql: "INSERT INTO orders (id, customer_id, product) VALUES (2, 1, 'Gadget')")
    _ = try await db.execute(sql: "INSERT INTO orders (id, customer_id, product) VALUES (3, 3, 'Donut')")
    // Bob (id=2) has no orders

    let result = try await db.execute(sql: "SELECT customers.name, orders.product FROM customers LEFT JOIN orders ON customers.id = orders.customer_id ORDER BY customers.name")
    guard case .rows(let rows) = result else { Issue.record("Expected rows"); return }
    // Alice has 2 orders, Bob has 0 (1 NULL row), Carol has 1 = 4 total
    #expect(rows.count == 4)

    // Verify Bob's row has NULL product
    let bobRows = rows.filter { $0["name"] == .string("Bob") }
    #expect(bobRows.count == 1)
    #expect(bobRows[0]["product"] == .null || bobRows[0]["product"] == nil)

    try await db.close()
}
