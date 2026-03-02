import Foundation
import Pantry
import PantryCore
import PantryQuery
import CSQLite

// MARK: - SQLite Helpers

final class SQLiteDB: @unchecked Sendable {
    let db: OpaquePointer

    init(path: String) {
        var handle: OpaquePointer?
        let rc = sqlite3_open(path, &handle)
        precondition(rc == SQLITE_OK, "Failed to open SQLite: \(String(cString: sqlite3_errmsg(handle!)))")
        self.db = handle!
        exec("PRAGMA journal_mode=WAL")
        exec("PRAGMA synchronous=NORMAL")
        exec("PRAGMA cache_size=-64000")
        exec("PRAGMA mmap_size=268435456")
        exec("PRAGMA temp_store=MEMORY")
    }

    func exec(_ sql: String) {
        var err: UnsafeMutablePointer<CChar>?
        let rc = sqlite3_exec(db, sql, nil, nil, &err)
        if rc != SQLITE_OK {
            let msg = err.map { String(cString: $0) } ?? "unknown"
            sqlite3_free(err)
            fatalError("SQLite exec failed [\(rc)]: \(msg)\nSQL: \(sql)")
        }
    }

    func prepare(_ sql: String) -> OpaquePointer {
        var stmt: OpaquePointer?
        let rc = sqlite3_prepare_v2(db, sql, -1, &stmt, nil)
        precondition(rc == SQLITE_OK, "SQLite prepare failed: \(String(cString: sqlite3_errmsg(db)))\nSQL: \(sql)")
        return stmt!
    }

    deinit {
        sqlite3_close(db)
    }
}

// MARK: - DBValue Helpers

extension DBValue {
    var stringValue: String? {
        if case .string(let s) = self { return s }
        return nil
    }
    var integerValue: Int64? {
        if case .integer(let i) = self { return i }
        return nil
    }
    var doubleValue: Double? {
        if case .double(let d) = self { return d }
        return nil
    }
}

// MARK: - Benchmark Result

struct BenchmarkResult: Sendable {
    let name: String
    let rowCount: Int
    let pantryMs: Double
    let sqliteMs: Double

    var ratio: Double { pantryMs / sqliteMs }
}

// MARK: - Timing

let SQLITE_TRANSIENT = unsafeBitCast(-1, to: sqlite3_destructor_type.self)

func elapsedMs(_ start: ContinuousClock.Instant) -> Double {
    let d = ContinuousClock.now - start
    return Double(d.components.seconds) * 1000.0
        + Double(d.components.attoseconds) / 1_000_000_000_000_000.0
}

// MARK: - Main

@MainActor
func runBenchmarks() async throws {
    let rowCounts = [1_000, 10_000, 25_000]
    let tmpDir = NSTemporaryDirectory()
    var results = [BenchmarkResult]()

    func header(_ title: String) {
        print("\n" + String(repeating: "=", count: 70))
        print("  \(title)")
        print(String(repeating: "=", count: 70))
    }

    func record(_ name: String, _ rowCount: Int, _ pantry: Double, _ sqlite: Double) {
        let ratio = pantry / sqlite
        let tag = ratio <= 1.0 ? "PANTRY WINS" : (ratio < 2.0 ? "~comparable" : "SQLite faster")
        results.append(BenchmarkResult(name: name, rowCount: rowCount, pantryMs: pantry, sqliteMs: sqlite))
        let padded = name.padding(toLength: 32, withPad: " ", startingAt: 0)
        print("  \(padded) \(String(format: "%5d", rowCount / 1000))k rows  Pantry: \(String(format: "%8.1f", pantry)) ms  SQLite: \(String(format: "%8.1f", sqlite)) ms  ratio: \(String(format: "%.2f", ratio))x  [\(tag)]")
    }

    func measurePantry(_ iterations: Int = 1, _ body: @Sendable () async throws -> Void) async throws -> Double {
        // warmup
        try await body()
        let start = ContinuousClock.now
        for _ in 0..<iterations {
            try await body()
        }
        return elapsedMs(start) / Double(iterations)
    }

    func measureSync(_ iterations: Int = 1, _ body: () throws -> Void) rethrows -> Double {
        // warmup
        try body()
        let start = ContinuousClock.now
        for _ in 0..<iterations {
            try body()
        }
        return elapsedMs(start) / Double(iterations)
    }

    for count in rowCounts {
        header("Benchmarks — \(count / 1000)k rows")

        // ── Setup Pantry ──
        let pantryPath = tmpDir + "bench_pantry_\(count).pantry"
        try? FileManager.default.removeItem(atPath: pantryPath)
        let config = PantryConfiguration(path: pantryPath, bufferPoolCapacity: 4000, costWeights: .ssd)
        let pantry = try await PantryDatabase(configuration: config)

        let schema = PantryTableSchema(name: "users", columns: [
            .id("id"),
            .string("name", nullable: false),
            .string("email", nullable: false),
            .integer("age", nullable: false),
            .string("city", nullable: false),
            .double("score", nullable: false),
        ])
        try await pantry.createTable(schema)

        // ── Setup SQLite ──
        let sqlitePath = tmpDir + "bench_sqlite_\(count).db"
        try? FileManager.default.removeItem(atPath: sqlitePath)
        let sqlite = SQLiteDB(path: sqlitePath)
        sqlite.exec("""
            CREATE TABLE users (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                email TEXT NOT NULL,
                age INTEGER NOT NULL,
                city TEXT NOT NULL,
                score REAL NOT NULL
            )
        """)

        // ── Generate test data ──
        let cities = ["New York", "London", "Tokyo", "Paris", "Berlin", "Sydney", "Toronto", "Mumbai"]
        var testRows = [[String: DBValue]]()
        testRows.reserveCapacity(count)
        for i in 0..<count {
            testRows.append([
                "id": .string("user_\(i)"),
                "name": .string("User \(i)"),
                "email": .string("user\(i)@example.com"),
                "age": .integer(Int64(18 + (i % 60))),
                "city": .string(cities[i % cities.count]),
                "score": .double(Double(i % 1000) / 10.0),
            ])
        }

        // ═══════════════════════════════════════════════════════════
        // 1. BULK INSERT
        // ═══════════════════════════════════════════════════════════
        let pantryInsertStart = ContinuousClock.now
        try await pantry.insertAll(into: "users", rows: testRows)
        let pantryInsertMs = elapsedMs(pantryInsertStart)

        let sqliteInsertStart = ContinuousClock.now
        sqlite.exec("BEGIN TRANSACTION")
        let insertStmt = sqlite.prepare("INSERT INTO users (id, name, email, age, city, score) VALUES (?, ?, ?, ?, ?, ?)")
        for r in testRows {
            sqlite3_reset(insertStmt)
            sqlite3_bind_text(insertStmt, 1, r["id"]!.stringValue!, -1, SQLITE_TRANSIENT)
            sqlite3_bind_text(insertStmt, 2, r["name"]!.stringValue!, -1, SQLITE_TRANSIENT)
            sqlite3_bind_text(insertStmt, 3, r["email"]!.stringValue!, -1, SQLITE_TRANSIENT)
            sqlite3_bind_int64(insertStmt, 4, r["age"]!.integerValue!)
            sqlite3_bind_text(insertStmt, 5, r["city"]!.stringValue!, -1, SQLITE_TRANSIENT)
            sqlite3_bind_double(insertStmt, 6, r["score"]!.doubleValue!)
            let rc = sqlite3_step(insertStmt)
            precondition(rc == SQLITE_DONE, "SQLite insert step failed: \(rc)")
        }
        sqlite3_finalize(insertStmt)
        sqlite.exec("COMMIT")
        let sqliteInsertMs = elapsedMs(sqliteInsertStart)

        record("Bulk INSERT", count, pantryInsertMs, sqliteInsertMs)

        // ── Create indexes ──
        let pantryIdxStart = ContinuousClock.now
        try await pantry.createIndex(table: "users", column: "age")
        try await pantry.createIndex(table: "users", column: "city")
        try await pantry.analyzeTable("users")
        let pantryIdxMs = elapsedMs(pantryIdxStart)

        let sqliteIdxStart = ContinuousClock.now
        sqlite.exec("CREATE INDEX idx_age ON users(age)")
        sqlite.exec("CREATE INDEX idx_city ON users(city)")
        sqlite.exec("ANALYZE")
        let sqliteIdxMs = elapsedMs(sqliteIdxStart)

        record("Create indexes + ANALYZE", count, pantryIdxMs, sqliteIdxMs)

        // ═══════════════════════════════════════════════════════════
        // 2. PK POINT LOOKUP
        // ═══════════════════════════════════════════════════════════
        let lookupID = "user_\(count / 2)"
        let pantryLookup = try await measurePantry(100) {
            let r = try await pantry.select(from: "users", where: .equals(column: "id", value: .string(lookupID)), limit: 1)
            precondition(r.count == 1)
        }
        let sqliteLookupStmt = sqlite.prepare("SELECT * FROM users WHERE id = ? LIMIT 1")
        let sqliteLookup = measureSync(100) {
            sqlite3_reset(sqliteLookupStmt)
            sqlite3_bind_text(sqliteLookupStmt, 1, lookupID, -1, SQLITE_TRANSIENT)
            precondition(sqlite3_step(sqliteLookupStmt) == SQLITE_ROW)
        }
        sqlite3_finalize(sqliteLookupStmt)
        record("PK point lookup", count, pantryLookup, sqliteLookup)

        // ═══════════════════════════════════════════════════════════
        // 3. INDEX RANGE SCAN
        // ═══════════════════════════════════════════════════════════
        let pantryRange = try await measurePantry(20) {
            let r = try await pantry.select(from: "users", where: .between(column: "age", min: .integer(25), max: .integer(35)))
            precondition(r.count > 0)
        }
        let sqliteRangeStmt = sqlite.prepare("SELECT * FROM users WHERE age BETWEEN 25 AND 35")
        let sqliteRange = measureSync(20) {
            sqlite3_reset(sqliteRangeStmt)
            var cnt = 0
            while sqlite3_step(sqliteRangeStmt) == SQLITE_ROW { cnt += 1 }
            precondition(cnt > 0)
        }
        sqlite3_finalize(sqliteRangeStmt)
        record("Index range (age 25-35)", count, pantryRange, sqliteRange)

        // ═══════════════════════════════════════════════════════════
        // 4. EQUALITY FILTER
        // ═══════════════════════════════════════════════════════════
        let pantryEq = try await measurePantry(20) {
            let r = try await pantry.select(from: "users", where: .equals(column: "city", value: .string("Tokyo")))
            precondition(r.count > 0)
        }
        let sqliteEqStmt = sqlite.prepare("SELECT * FROM users WHERE city = 'Tokyo'")
        let sqliteEq = measureSync(20) {
            sqlite3_reset(sqliteEqStmt)
            var cnt = 0
            while sqlite3_step(sqliteEqStmt) == SQLITE_ROW { cnt += 1 }
            precondition(cnt > 0)
        }
        sqlite3_finalize(sqliteEqStmt)
        record("Equality (city=Tokyo)", count, pantryEq, sqliteEq)

        // ═══════════════════════════════════════════════════════════
        // 5. ORDER BY + LIMIT
        // ═══════════════════════════════════════════════════════════
        let pantryTopN = try await measurePantry(20) {
            let r = try await pantry.select(from: "users", orderBy: [.desc("age")], limit: 10)
            precondition(r.count == 10)
        }
        let sqliteTopNStmt = sqlite.prepare("SELECT * FROM users ORDER BY age DESC LIMIT 10")
        let sqliteTopN = measureSync(20) {
            sqlite3_reset(sqliteTopNStmt)
            var cnt = 0
            while sqlite3_step(sqliteTopNStmt) == SQLITE_ROW { cnt += 1 }
            precondition(cnt == 10)
        }
        sqlite3_finalize(sqliteTopNStmt)
        record("ORDER BY DESC LIMIT 10", count, pantryTopN, sqliteTopN)

        // ═══════════════════════════════════════════════════════════
        // 6. COUNT AGGREGATE
        // ═══════════════════════════════════════════════════════════
        let pantryCount = try await measurePantry(20) {
            let c = try await pantry.count(from: "users", where: .greaterThan(column: "age", value: .integer(30)))
            precondition(c > 0)
        }
        let sqliteCountStmt = sqlite.prepare("SELECT COUNT(*) FROM users WHERE age > 30")
        let sqliteCount = measureSync(20) {
            sqlite3_reset(sqliteCountStmt)
            precondition(sqlite3_step(sqliteCountStmt) == SQLITE_ROW)
            _ = sqlite3_column_int64(sqliteCountStmt, 0)
        }
        sqlite3_finalize(sqliteCountStmt)
        record("COUNT WHERE age > 30", count, pantryCount, sqliteCount)

        // ═══════════════════════════════════════════════════════════
        // 7. SUM AGGREGATE
        // ═══════════════════════════════════════════════════════════
        let pantrySum = try await measurePantry(20) {
            let s = try await pantry.aggregate(from: "users", .sum(column: "score"))
            precondition(s != .null)
        }
        let sqliteSumStmt = sqlite.prepare("SELECT SUM(score) FROM users")
        let sqliteSum = measureSync(20) {
            sqlite3_reset(sqliteSumStmt)
            precondition(sqlite3_step(sqliteSumStmt) == SQLITE_ROW)
            _ = sqlite3_column_double(sqliteSumStmt, 0)
        }
        sqlite3_finalize(sqliteSumStmt)
        record("SUM(score) full table", count, pantrySum, sqliteSum)

        // ═══════════════════════════════════════════════════════════
        // 8. AVG AGGREGATE
        // ═══════════════════════════════════════════════════════════
        let pantryAvg = try await measurePantry(20) {
            let a = try await pantry.aggregate(from: "users", .avg(column: "age"))
            precondition(a != .null)
        }
        let sqliteAvgStmt = sqlite.prepare("SELECT AVG(age) FROM users")
        let sqliteAvg = measureSync(20) {
            sqlite3_reset(sqliteAvgStmt)
            precondition(sqlite3_step(sqliteAvgStmt) == SQLITE_ROW)
            _ = sqlite3_column_double(sqliteAvgStmt, 0)
        }
        sqlite3_finalize(sqliteAvgStmt)
        record("AVG(age) full table", count, pantryAvg, sqliteAvg)

        // ═══════════════════════════════════════════════════════════
        // 9. UPDATE
        // ═══════════════════════════════════════════════════════════
        let pantryUpdate = try await measurePantry(5) {
            let n = try await pantry.update(table: "users", set: ["score": .double(99.9)], where: .equals(column: "city", value: .string("Berlin")))
            precondition(n > 0)
        }
        let sqliteUpdateStmt = sqlite.prepare("UPDATE users SET score = 99.9 WHERE city = 'Berlin'")
        let sqliteUpdate = measureSync(5) {
            sqlite3_reset(sqliteUpdateStmt)
            precondition(sqlite3_step(sqliteUpdateStmt) == SQLITE_DONE)
        }
        sqlite3_finalize(sqliteUpdateStmt)
        record("UPDATE WHERE city=Berlin", count, pantryUpdate, sqliteUpdate)

        // ═══════════════════════════════════════════════════════════
        // 10. DELETE
        // ═══════════════════════════════════════════════════════════
        let delCount = count / 10
        let deleteRows = (0..<delCount).map { i -> [String: DBValue] in
            ["id": .string("del_\(i)"), "name": .string("Del \(i)"), "email": .string("d\(i)@t.com"),
             "age": .integer(99), "city": .string("DeleteCity"), "score": .double(0.0)]
        }
        try await pantry.insertAll(into: "users", rows: deleteRows)

        sqlite.exec("BEGIN")
        let delStmt = sqlite.prepare("INSERT INTO users (id, name, email, age, city, score) VALUES (?, ?, ?, ?, ?, ?)")
        for r in deleteRows {
            sqlite3_reset(delStmt)
            sqlite3_bind_text(delStmt, 1, r["id"]!.stringValue!, -1, SQLITE_TRANSIENT)
            sqlite3_bind_text(delStmt, 2, r["name"]!.stringValue!, -1, SQLITE_TRANSIENT)
            sqlite3_bind_text(delStmt, 3, r["email"]!.stringValue!, -1, SQLITE_TRANSIENT)
            sqlite3_bind_int64(delStmt, 4, r["age"]!.integerValue!)
            sqlite3_bind_text(delStmt, 5, r["city"]!.stringValue!, -1, SQLITE_TRANSIENT)
            sqlite3_bind_double(delStmt, 6, r["score"]!.doubleValue!)
            precondition(sqlite3_step(delStmt) == SQLITE_DONE)
        }
        sqlite3_finalize(delStmt)
        sqlite.exec("COMMIT")

        let pantryDelStart = ContinuousClock.now
        let n = try await pantry.delete(from: "users", where: .equals(column: "city", value: .string("DeleteCity")))
        precondition(n >= 0)
        let pantryDelMs = elapsedMs(pantryDelStart)

        let sqliteDelStart = ContinuousClock.now
        sqlite.exec("DELETE FROM users WHERE city = 'DeleteCity'")
        let sqliteDelMs = elapsedMs(sqliteDelStart)

        record("DELETE (city=DeleteCity)", count, pantryDelMs, sqliteDelMs)

        // ═══════════════════════════════════════════════════════════
        // 11. JOIN (skip at large row counts to avoid schema page overflow)
        // ═══════════════════════════════════════════════════════════
        do {
            let ordersSchema = PantryTableSchema(name: "orders", columns: [
                .id("order_id"),
                .string("user_id", nullable: false),
                .double("amount", nullable: false),
                .string("status", nullable: false),
            ])
            try await pantry.createTable(ordersSchema)
            sqlite.exec("""
                CREATE TABLE orders (
                    order_id TEXT PRIMARY KEY,
                    user_id TEXT NOT NULL,
                    amount REAL NOT NULL,
                    status TEXT NOT NULL
                )
            """)

            let orderCount = count / 2
            var orderRows = [[String: DBValue]]()
            orderRows.reserveCapacity(orderCount)
            for i in 0..<orderCount {
                orderRows.append([
                    "order_id": .string("order_\(i)"),
                    "user_id": .string("user_\(i % count)"),
                    "amount": .double(Double(i % 500) + 10.0),
                    "status": .string(i % 3 == 0 ? "shipped" : (i % 3 == 1 ? "pending" : "delivered")),
                ])
            }
            try await pantry.insertAll(into: "orders", rows: orderRows)
            try await pantry.createIndex(table: "orders", column: "user_id")

            sqlite.exec("BEGIN")
            let orderStmt = sqlite.prepare("INSERT INTO orders (order_id, user_id, amount, status) VALUES (?, ?, ?, ?)")
            for r in orderRows {
                sqlite3_reset(orderStmt)
                sqlite3_bind_text(orderStmt, 1, r["order_id"]!.stringValue!, -1, SQLITE_TRANSIENT)
                sqlite3_bind_text(orderStmt, 2, r["user_id"]!.stringValue!, -1, SQLITE_TRANSIENT)
                sqlite3_bind_double(orderStmt, 3, r["amount"]!.doubleValue!)
                sqlite3_bind_text(orderStmt, 4, r["status"]!.stringValue!, -1, SQLITE_TRANSIENT)
                precondition(sqlite3_step(orderStmt) == SQLITE_DONE)
            }
            sqlite3_finalize(orderStmt)
            sqlite.exec("COMMIT")
            sqlite.exec("CREATE INDEX idx_orders_user ON orders(user_id)")

            let pantryJoin = try await measurePantry(5) {
                let r = try await pantry.select(
                    from: "users",
                    join: [JoinClause(table: "orders", type: .inner, on: "id", equals: "user_id")],
                    limit: 100
                )
                precondition(r.count > 0)
            }
            let sqliteJoinStmt = sqlite.prepare("SELECT * FROM users INNER JOIN orders ON users.id = orders.user_id LIMIT 100")
            let sqliteJoin = measureSync(5) {
                sqlite3_reset(sqliteJoinStmt)
                var cnt = 0
                while sqlite3_step(sqliteJoinStmt) == SQLITE_ROW { cnt += 1 }
                precondition(cnt > 0)
            }
            sqlite3_finalize(sqliteJoinStmt)
            record("INNER JOIN LIMIT 100", count, pantryJoin, sqliteJoin)
        } catch {
            print("  [JOIN skipped at \(count) rows: \(error)]")
        }

        // Cleanup
        try? await pantry.close()
        try? FileManager.default.removeItem(atPath: pantryPath)
        try? FileManager.default.removeItem(atPath: sqlitePath)
    }

    // ═══════════════════════════════════════════════════════════════
    // SUMMARY
    // ═══════════════════════════════════════════════════════════════
    header("SUMMARY")
    print("  \("Benchmark".padding(toLength: 32, withPad: " ", startingAt: 0))    Rows      Pantry      SQLite   Ratio  Winner")
    print("  " + String(repeating: "-", count: 86))
    for r in results {
        let tag = r.ratio <= 1.0 ? "Pantry" : "SQLite"
        let padded = r.name.padding(toLength: 32, withPad: " ", startingAt: 0)
        print("  \(padded)  \(String(format: "%5d", r.rowCount / 1000))k  \(String(format: "%8.1f", r.pantryMs)) ms  \(String(format: "%8.1f", r.sqliteMs)) ms  \(String(format: "%6.2f", r.ratio))x  \(tag)")
    }

    let pantryWins = results.filter { $0.ratio <= 1.0 }.count
    let sqliteWins = results.count - pantryWins
    print("\n  Pantry wins: \(pantryWins)/\(results.count)  |  SQLite wins: \(sqliteWins)/\(results.count)")
    let avgRatio = results.map(\.ratio).reduce(0, +) / Double(results.count)
    print(String(format: "  Average ratio (Pantry/SQLite): %.2fx", avgRatio))
    if avgRatio <= 1.0 {
        print("  Overall: Pantry is faster on average!")
    } else {
        print(String(format: "  Overall: SQLite is %.1fx faster on average", avgRatio))
    }
}

try await runBenchmarks()
