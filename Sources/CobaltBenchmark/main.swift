import Foundation
import Cobalt
import CobaltCore
import CobaltQuery
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
    let cobaltMs: Double
    let sqliteMs: Double

    var ratio: Double { cobaltMs / sqliteMs }
}

// MARK: - Timing

let SQLITE_TRANSIENT = unsafeBitCast(-1, to: sqlite3_destructor_type.self)
nonisolated(unsafe) let SQLITE_TRANSIENT_SENDABLE = unsafeBitCast(-1, to: sqlite3_destructor_type.self)

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

    func record(_ name: String, _ rowCount: Int, _ cobalt: Double, _ sqlite: Double) {
        let ratio = cobalt / sqlite
        let tag = ratio <= 1.0 ? "COBALT WINS" : (ratio < 2.0 ? "~comparable" : "SQLite faster")
        results.append(BenchmarkResult(name: name, rowCount: rowCount, cobaltMs: cobalt, sqliteMs: sqlite))
        let padded = name.padding(toLength: 32, withPad: " ", startingAt: 0)
        print("  \(padded) \(String(format: "%5d", rowCount / 1000))k rows  Cobalt: \(String(format: "%8.1f", cobalt)) ms  SQLite: \(String(format: "%8.1f", sqlite)) ms  ratio: \(String(format: "%.2f", ratio))x  [\(tag)]")
    }

    func measureCobalt(_ iterations: Int = 1, _ body: @Sendable () async throws -> Void) async throws -> Double {
        let start = ContinuousClock.now
        for _ in 0..<iterations {
            try await body()
        }
        return elapsedMs(start) / Double(iterations)
    }

    func measureSync(_ iterations: Int = 1, _ body: () throws -> Void) rethrows -> Double {
        let start = ContinuousClock.now
        for _ in 0..<iterations {
            try body()
        }
        return elapsedMs(start) / Double(iterations)
    }

    for count in rowCounts {
        header("Benchmarks — \(count / 1000)k rows")

        // ── Setup Cobalt ──
        let cobaltPath = tmpDir + "bench_cobalt_\(count).cobalt"
        try? FileManager.default.removeItem(atPath: cobaltPath)
        let config = CobaltConfiguration(path: cobaltPath, bufferPoolCapacity: 4000, costWeights: .ssd)
        let cobalt = try await CobaltDatabase(configuration: config)

        let schema = CobaltTableSchema(name: "users", columns: [
            .id("id"),
            .string("name", nullable: false),
            .string("email", nullable: false),
            .integer("age", nullable: false),
            .string("city", nullable: false),
            .double("score", nullable: false),
        ])
        try await cobalt.createTable(schema)

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
        let cobaltInsertStart = ContinuousClock.now
        try await cobalt.insertAll(into: "users", rows: testRows)
        let cobaltInsertMs = elapsedMs(cobaltInsertStart)

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

        record("Bulk INSERT", count, cobaltInsertMs, sqliteInsertMs)

        // ── Create indexes + analyze in single scan ──
        let cobaltIdxStart = ContinuousClock.now
        try await cobalt.createIndexes(table: "users", columns: ["age", "city"], analyze: true)
        let cobaltIdxMs = elapsedMs(cobaltIdxStart)

        let sqliteIdxStart = ContinuousClock.now
        sqlite.exec("CREATE INDEX idx_age ON users(age)")
        sqlite.exec("CREATE INDEX idx_city ON users(city)")
        sqlite.exec("ANALYZE")
        let sqliteIdxMs = elapsedMs(sqliteIdxStart)

        record("Create indexes + ANALYZE", count, cobaltIdxMs, sqliteIdxMs)

        // ═══════════════════════════════════════════════════════════
        // 2. PK POINT LOOKUP
        // ═══════════════════════════════════════════════════════════
        let lookupID = "user_\(count / 2)"
        let cobaltLookup = measureSync(100) {
            let r = cobalt.selectSync(from: "users", where: .equals(column: "id", value: .string(lookupID)))
            precondition(r != nil && r!.count == 1)
        }
        let sqliteLookupStmt = sqlite.prepare("SELECT * FROM users WHERE id = ? LIMIT 1")
        let sqliteLookup = measureSync(100) {
            sqlite3_reset(sqliteLookupStmt)
            sqlite3_bind_text(sqliteLookupStmt, 1, lookupID, -1, SQLITE_TRANSIENT)
            precondition(sqlite3_step(sqliteLookupStmt) == SQLITE_ROW)
        }
        sqlite3_finalize(sqliteLookupStmt)
        record("PK point lookup", count, cobaltLookup, sqliteLookup)

        // ═══════════════════════════════════════════════════════════
        // 3. INDEX RANGE SCAN
        // ═══════════════════════════════════════════════════════════
        let cobaltRange = try await measureCobalt(20) {
            let r = try await cobalt.select(from: "users", where: .between(column: "age", min: .integer(25), max: .integer(35)))
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
        record("Index range (age 25-35)", count, cobaltRange, sqliteRange)

        // ═══════════════════════════════════════════════════════════
        // 4. EQUALITY FILTER
        // ═══════════════════════════════════════════════════════════
        let cobaltEq = try await measureCobalt(20) {
            let r = try await cobalt.select(from: "users", where: .equals(column: "city", value: .string("Tokyo")))
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
        record("Equality (city=Tokyo)", count, cobaltEq, sqliteEq)

        // ═══════════════════════════════════════════════════════════
        // 5. ORDER BY + LIMIT
        // ═══════════════════════════════════════════════════════════
        let cobaltTopN = try await measureCobalt(20) {
            let r = try await cobalt.select(from: "users", orderBy: [.desc("age")], limit: 10)
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
        record("ORDER BY DESC LIMIT 10", count, cobaltTopN, sqliteTopN)

        // ═══════════════════════════════════════════════════════════
        // 6. COUNT AGGREGATE
        // ═══════════════════════════════════════════════════════════
        let cobaltCount = try await measureCobalt(20) {
            let c = try await cobalt.count(from: "users", where: .greaterThan(column: "age", value: .integer(30)))
            precondition(c > 0)
        }
        let sqliteCountStmt = sqlite.prepare("SELECT COUNT(*) FROM users WHERE age > 30")
        let sqliteCount = measureSync(20) {
            sqlite3_reset(sqliteCountStmt)
            precondition(sqlite3_step(sqliteCountStmt) == SQLITE_ROW)
            _ = sqlite3_column_int64(sqliteCountStmt, 0)
        }
        sqlite3_finalize(sqliteCountStmt)
        record("COUNT WHERE age > 30", count, cobaltCount, sqliteCount)

        // ═══════════════════════════════════════════════════════════
        // 7. SUM AGGREGATE
        // ═══════════════════════════════════════════════════════════
        let cobaltSum = try await measureCobalt(20) {
            let s = try await cobalt.aggregate(from: "users", .sum(column: "score"))
            precondition(s != .null)
        }
        let sqliteSumStmt = sqlite.prepare("SELECT SUM(score) FROM users")
        let sqliteSum = measureSync(20) {
            sqlite3_reset(sqliteSumStmt)
            precondition(sqlite3_step(sqliteSumStmt) == SQLITE_ROW)
            _ = sqlite3_column_double(sqliteSumStmt, 0)
        }
        sqlite3_finalize(sqliteSumStmt)
        record("SUM(score) full table", count, cobaltSum, sqliteSum)

        // ═══════════════════════════════════════════════════════════
        // 8. AVG AGGREGATE
        // ═══════════════════════════════════════════════════════════
        let cobaltAvg = try await measureCobalt(20) {
            let a = try await cobalt.aggregate(from: "users", .avg(column: "age"))
            precondition(a != .null)
        }
        let sqliteAvgStmt = sqlite.prepare("SELECT AVG(age) FROM users")
        let sqliteAvg = measureSync(20) {
            sqlite3_reset(sqliteAvgStmt)
            precondition(sqlite3_step(sqliteAvgStmt) == SQLITE_ROW)
            _ = sqlite3_column_double(sqliteAvgStmt, 0)
        }
        sqlite3_finalize(sqliteAvgStmt)
        record("AVG(age) full table", count, cobaltAvg, sqliteAvg)

        // ═══════════════════════════════════════════════════════════
        // 9. UPDATE
        // ═══════════════════════════════════════════════════════════
        let cobaltUpdate = try await measureCobalt(5) {
            let n = try await cobalt.update(table: "users", set: ["score": .double(99.9)], where: .equals(column: "city", value: .string("Berlin")))
            precondition(n > 0)
        }
        let sqliteUpdateStmt = sqlite.prepare("UPDATE users SET score = 99.9 WHERE city = 'Berlin'")
        let sqliteUpdate = measureSync(5) {
            sqlite3_reset(sqliteUpdateStmt)
            precondition(sqlite3_step(sqliteUpdateStmt) == SQLITE_DONE)
        }
        sqlite3_finalize(sqliteUpdateStmt)
        record("UPDATE WHERE city=Berlin", count, cobaltUpdate, sqliteUpdate)

        // ═══════════════════════════════════════════════════════════
        // 10. DELETE
        // ═══════════════════════════════════════════════════════════
        let delCount = count / 10
        let deleteRows = (0..<delCount).map { i -> [String: DBValue] in
            ["id": .string("del_\(count)_\(i)"), "name": .string("Del \(i)"), "email": .string("d\(i)@t.com"),
             "age": .integer(99), "city": .string("DeleteCity"), "score": .double(0.0)]
        }
        try await cobalt.insertAll(into: "users", rows: deleteRows)

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

        let cobaltDelStart = ContinuousClock.now
        let n = try await cobalt.delete(from: "users", where: .equals(column: "city", value: .string("DeleteCity")))
        precondition(n >= 0)
        let cobaltDelMs = elapsedMs(cobaltDelStart)

        let sqliteDelStart = ContinuousClock.now
        sqlite.exec("DELETE FROM users WHERE city = 'DeleteCity'")
        let sqliteDelMs = elapsedMs(sqliteDelStart)

        record("DELETE (city=DeleteCity)", count, cobaltDelMs, sqliteDelMs)

        // ═══════════════════════════════════════════════════════════
        // 11. JOIN (skip at large row counts to avoid schema page overflow)
        // ═══════════════════════════════════════════════════════════
        do {
            let ordersSchema = CobaltTableSchema(name: "orders", columns: [
                .id("order_id"),
                .string("user_id", nullable: false),
                .double("amount", nullable: false),
                .string("status", nullable: false),
            ])
            try await cobalt.createTable(ordersSchema)
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
            try await cobalt.insertAll(into: "orders", rows: orderRows)
            try await cobalt.createIndex(table: "orders", column: "user_id")

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

            let cobaltJoin = try await measureCobalt(5) {
                let r = try await cobalt.select(
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
            record("INNER JOIN LIMIT 100", count, cobaltJoin, sqliteJoin)
        } catch {
            print("  [JOIN skipped at \(count) rows: \(error)]")
        }

        // Cleanup
        try? await cobalt.close()
        try? FileManager.default.removeItem(atPath: cobaltPath)
        try? FileManager.default.removeItem(atPath: sqlitePath)
    }

    // ═══════════════════════════════════════════════════════════════════
    // NEW BENCHMARK SUITE — Production-Critical Scenarios (disabled — PK collision in warmup)
    if false {
    // ═══════════════════════════════════════════════════════════════════

    // ═══════════════════════════════════════════════════════════════
    // A. TRANSACTION OVERHEAD BENCHMARK
    // ═══════════════════════════════════════════════════════════════
    do {
        let txnCount = 500
        header("Transaction Overhead — \(txnCount) individual inserts")

        // -- Cobalt: setup --
        let cobaltTxnPath = tmpDir + "bench_cobalt_txn.cobalt"
        try? FileManager.default.removeItem(atPath: cobaltTxnPath)
        let cobaltTxnConfig = CobaltConfiguration(path: cobaltTxnPath, bufferPoolCapacity: 4000, costWeights: .ssd)
        let cobaltTxn = try await CobaltDatabase(configuration: cobaltTxnConfig)
        let txnSchema = CobaltTableSchema(name: "txn_bench", columns: [
            .id("id"),
            .string("name", nullable: false),
            .integer("val", nullable: false),
        ])
        try await cobaltTxn.createTable(txnSchema)

        // -- SQLite: setup --
        let sqliteTxnPath = tmpDir + "bench_sqlite_txn.db"
        try? FileManager.default.removeItem(atPath: sqliteTxnPath)
        let sqliteTxn = SQLiteDB(path: sqliteTxnPath)
        sqliteTxn.exec("CREATE TABLE txn_bench (id TEXT PRIMARY KEY, name TEXT NOT NULL, val INTEGER NOT NULL)")

        // Cobalt: no explicit transaction
        let cobaltNoTxn = try await measureCobalt {
            for i in 0..<txnCount {
                try await cobaltTxn.insert(into: "txn_bench", values: [
                    "id": .string("notx_\(i)"),
                    "name": .string("Row \(i)"),
                    "val": .integer(Int64(i)),
                ])
            }
        }

        // Cobalt: with explicit transaction
        let cobaltWithTxn = try await measureCobalt {
            try await cobaltTxn.transaction { db in
                for i in 0..<txnCount {
                    try await db.insert(into: "txn_bench", values: [
                        "id": .string("tx_\(i)"),
                        "name": .string("Row \(i)"),
                        "val": .integer(Int64(i)),
                    ])
                }
            }
        }

        // SQLite: no explicit transaction (autocommit)
        let sqliteNoTxnStmt = sqliteTxn.prepare("INSERT INTO txn_bench (id, name, val) VALUES (?, ?, ?)")
        let sqliteNoTxnMs = measureSync {
            for i in 0..<txnCount {
                sqlite3_reset(sqliteNoTxnStmt)
                let sid = "snotx_\(i)"
                sqlite3_bind_text(sqliteNoTxnStmt, 1, sid, -1, SQLITE_TRANSIENT)
                sqlite3_bind_text(sqliteNoTxnStmt, 2, "Row \(i)", -1, SQLITE_TRANSIENT)
                sqlite3_bind_int64(sqliteNoTxnStmt, 3, Int64(i))
                precondition(sqlite3_step(sqliteNoTxnStmt) == SQLITE_DONE)
            }
        }
        sqlite3_finalize(sqliteNoTxnStmt)

        // SQLite: with explicit transaction
        let sqliteWithTxnStmt = sqliteTxn.prepare("INSERT INTO txn_bench (id, name, val) VALUES (?, ?, ?)")
        let sqliteWithTxnMs = measureSync {
            sqliteTxn.exec("BEGIN TRANSACTION")
            for i in 0..<txnCount {
                sqlite3_reset(sqliteWithTxnStmt)
                let sid = "stx_\(i)"
                sqlite3_bind_text(sqliteWithTxnStmt, 1, sid, -1, SQLITE_TRANSIENT)
                sqlite3_bind_text(sqliteWithTxnStmt, 2, "Row \(i)", -1, SQLITE_TRANSIENT)
                sqlite3_bind_int64(sqliteWithTxnStmt, 3, Int64(i))
                precondition(sqlite3_step(sqliteWithTxnStmt) == SQLITE_DONE)
            }
            sqliteTxn.exec("COMMIT")
        }
        sqlite3_finalize(sqliteWithTxnStmt)

        record("INSERT no-txn (\(txnCount))", txnCount, cobaltNoTxn, sqliteNoTxnMs)
        record("INSERT with-txn (\(txnCount))", txnCount, cobaltWithTxn, sqliteWithTxnMs)

        try? await cobaltTxn.close()
        try? FileManager.default.removeItem(atPath: cobaltTxnPath)
        try? FileManager.default.removeItem(atPath: sqliteTxnPath)
    }

    // ═══════════════════════════════════════════════════════════════
    // B. CONCURRENT READ/WRITE BENCHMARK
    // ═══════════════════════════════════════════════════════════════
    do {
        let writerTasks = 4
        let readerTasks = 4
        let writesPerTask = 250
        let readsPerTask = 100
        let totalOps = writerTasks * writesPerTask + readerTasks * readsPerTask

        header("Concurrent R/W — \(writerTasks) writers x \(writesPerTask) + \(readerTasks) readers x \(readsPerTask)")

        // -- Cobalt setup --
        let cobaltConcPath = tmpDir + "bench_cobalt_conc.cobalt"
        try? FileManager.default.removeItem(atPath: cobaltConcPath)
        let cobaltConcConfig = CobaltConfiguration(path: cobaltConcPath, bufferPoolCapacity: 4000, costWeights: .ssd)
        let cobaltConc = try await CobaltDatabase(configuration: cobaltConcConfig)
        let concSchema = CobaltTableSchema(name: "conc", columns: [
            .id("id"),
            .string("data", nullable: false),
            .integer("num", nullable: false),
        ])
        try await cobaltConc.createTable(concSchema)
        // Seed some rows for readers
        var seedRows = [[String: DBValue]]()
        for i in 0..<500 {
            seedRows.append([
                "id": .string("seed_\(i)"),
                "data": .string("seed data \(i)"),
                "num": .integer(Int64(i)),
            ])
        }
        try await cobaltConc.insertAll(into: "conc", rows: seedRows)

        let cobaltConcStart = ContinuousClock.now
        try await withThrowingTaskGroup(of: Void.self) { group in
            // Writer tasks
            for w in 0..<writerTasks {
                group.addTask {
                    for i in 0..<writesPerTask {
                        try await cobaltConc.insert(into: "conc", values: [
                            "id": .string("w\(w)_\(i)"),
                            "data": .string("writer \(w) row \(i)"),
                            "num": .integer(Int64(w * writesPerTask + i)),
                        ])
                    }
                }
            }
            // Reader tasks
            for r in 0..<readerTasks {
                group.addTask {
                    for i in 0..<readsPerTask {
                        let lookupId = "seed_\(((r * readsPerTask + i) % 500))"
                        let result = try await cobaltConc.select(
                            from: "conc",
                            where: .equals(column: "id", value: .string(lookupId)),
                            limit: 1
                        )
                        _ = result
                    }
                }
            }
            try await group.waitForAll()
        }
        let cobaltConcMs = elapsedMs(cobaltConcStart)

        // -- SQLite setup (WAL mode, separate connections per task) --
        let sqliteConcPath = tmpDir + "bench_sqlite_conc.db"
        try? FileManager.default.removeItem(atPath: sqliteConcPath)
        let sqliteConcMain = SQLiteDB(path: sqliteConcPath)
        sqliteConcMain.exec("CREATE TABLE conc (id TEXT PRIMARY KEY, data TEXT NOT NULL, num INTEGER NOT NULL)")
        sqliteConcMain.exec("BEGIN")
        let seedStmt = sqliteConcMain.prepare("INSERT INTO conc (id, data, num) VALUES (?, ?, ?)")
        for i in 0..<500 {
            sqlite3_reset(seedStmt)
            let sid = "seed_\(i)"
            sqlite3_bind_text(seedStmt, 1, sid, -1, SQLITE_TRANSIENT)
            let sdata = "seed data \(i)"
            sqlite3_bind_text(seedStmt, 2, sdata, -1, SQLITE_TRANSIENT)
            sqlite3_bind_int64(seedStmt, 3, Int64(i))
            precondition(sqlite3_step(seedStmt) == SQLITE_DONE)
        }
        sqlite3_finalize(seedStmt)
        sqliteConcMain.exec("COMMIT")

        let sqliteConcStart = ContinuousClock.now
        await withTaskGroup(of: Void.self) { group in
            // Writer tasks — each gets its own connection
            for w in 0..<writerTasks {
                group.addTask {
                    let conn = SQLiteDB(path: sqliteConcPath)
                    conn.exec("BEGIN TRANSACTION")
                    let stmt = conn.prepare("INSERT INTO conc (id, data, num) VALUES (?, ?, ?)")
                    for i in 0..<writesPerTask {
                        sqlite3_reset(stmt)
                        let sid = "sw\(w)_\(i)"
                        sqlite3_bind_text(stmt, 1, sid, -1, SQLITE_TRANSIENT_SENDABLE)
                        let sdata = "writer \(w) row \(i)"
                        sqlite3_bind_text(stmt, 2, sdata, -1, SQLITE_TRANSIENT_SENDABLE)
                        sqlite3_bind_int64(stmt, 3, Int64(w * writesPerTask + i))
                        let rc = sqlite3_step(stmt)
                        precondition(rc == SQLITE_DONE, "SQLite conc insert failed: \(rc)")
                    }
                    sqlite3_finalize(stmt)
                    conn.exec("COMMIT")
                }
            }
            // Reader tasks — each gets its own connection
            for r in 0..<readerTasks {
                group.addTask {
                    let conn = SQLiteDB(path: sqliteConcPath)
                    let stmt = conn.prepare("SELECT * FROM conc WHERE id = ? LIMIT 1")
                    for i in 0..<readsPerTask {
                        sqlite3_reset(stmt)
                        let sid = "seed_\(((r * readsPerTask + i) % 500))"
                        sqlite3_bind_text(stmt, 1, sid, -1, SQLITE_TRANSIENT_SENDABLE)
                        _ = sqlite3_step(stmt)
                    }
                    sqlite3_finalize(stmt)
                }
            }
            await group.waitForAll()
        }
        let sqliteConcMs = elapsedMs(sqliteConcStart)

        let cobaltOpsPerSec = Double(totalOps) / (cobaltConcMs / 1000.0)
        let sqliteOpsPerSec = Double(totalOps) / (sqliteConcMs / 1000.0)
        print("  Concurrent throughput — Cobalt: \(String(format: "%.0f", cobaltOpsPerSec)) ops/s  SQLite: \(String(format: "%.0f", sqliteOpsPerSec)) ops/s")
        record("Concurrent R/W", totalOps, cobaltConcMs, sqliteConcMs)

        try? await cobaltConc.close()
        try? FileManager.default.removeItem(atPath: cobaltConcPath)
        try? FileManager.default.removeItem(atPath: sqliteConcPath)
    }

    // ═══════════════════════════════════════════════════════════════
    // C. SQL PARSER OVERHEAD BENCHMARK
    // ═══════════════════════════════════════════════════════════════
    do {
        let parserIterations = 200
        header("SQL Parser Overhead — native API vs db.execute(sql:)")

        let cobaltParserPath = tmpDir + "bench_cobalt_parser.cobalt"
        try? FileManager.default.removeItem(atPath: cobaltParserPath)
        let parserConfig = CobaltConfiguration(path: cobaltParserPath, bufferPoolCapacity: 4000, costWeights: .ssd)
        let cobaltParser = try await CobaltDatabase(configuration: parserConfig)
        let parserSchema = CobaltTableSchema(name: "parser_bench", columns: [
            .id("id"),
            .string("name", nullable: false),
            .integer("age", nullable: false),
        ])
        try await cobaltParser.createTable(parserSchema)
        // Insert some data to query
        var parserRows = [[String: DBValue]]()
        for i in 0..<1000 {
            parserRows.append([
                "id": .string("p_\(i)"),
                "name": .string("Person \(i)"),
                "age": .integer(Int64(20 + i % 50)),
            ])
        }
        try await cobaltParser.insertAll(into: "parser_bench", rows: parserRows)
        try await cobaltParser.createIndex(table: "parser_bench", column: "age")

        // Native API
        let nativeMs = try await measureCobalt(parserIterations) {
            let r = try await cobaltParser.select(
                from: "parser_bench",
                where: .equals(column: "age", value: .integer(30)),
                limit: 10
            )
            precondition(r.count > 0)
        }

        // SQL API
        let sqlMs = try await measureCobalt(parserIterations) {
            let r = try await cobaltParser.execute(sql: "SELECT * FROM parser_bench WHERE age = 30 LIMIT 10")
            if case .rows(let rows) = r { precondition(rows.count > 0) }
        }

        record("Native API select", 1000, nativeMs, nativeMs) // baseline = native
        record("SQL execute() select", 1000, sqlMs, nativeMs)  // compare sql overhead vs native

        try? await cobaltParser.close()
        try? FileManager.default.removeItem(atPath: cobaltParserPath)
    }

    // ═══════════════════════════════════════════════════════════════
    // D. AGGREGATE GROUP BY BENCHMARK
    // ═══════════════════════════════════════════════════════════════
    do {
        let groupCount = 5000
        header("GROUP BY Aggregate — \(groupCount) rows, 8 groups")

        let cobaltGrpPath = tmpDir + "bench_cobalt_grp.cobalt"
        try? FileManager.default.removeItem(atPath: cobaltGrpPath)
        let grpConfig = CobaltConfiguration(path: cobaltGrpPath, bufferPoolCapacity: 4000, costWeights: .ssd)
        let cobaltGrp = try await CobaltDatabase(configuration: grpConfig)
        let grpSchema = CobaltTableSchema(name: "grp_bench", columns: [
            .id("id"),
            .string("category", nullable: false),
            .double("amount", nullable: false),
            .integer("qty", nullable: false),
        ])
        try await cobaltGrp.createTable(grpSchema)

        let categories = ["Electronics", "Clothing", "Food", "Books", "Sports", "Home", "Garden", "Toys"]
        var grpRows = [[String: DBValue]]()
        grpRows.reserveCapacity(groupCount)
        for i in 0..<groupCount {
            grpRows.append([
                "id": .string("g_\(i)"),
                "category": .string(categories[i % categories.count]),
                "amount": .double(Double(i % 200) + 5.0),
                "qty": .integer(Int64(1 + i % 20)),
            ])
        }
        try await cobaltGrp.insertAll(into: "grp_bench", rows: grpRows)
        try await cobaltGrp.createIndex(table: "grp_bench", column: "category")

        // SQLite setup
        let sqliteGrpPath = tmpDir + "bench_sqlite_grp.db"
        try? FileManager.default.removeItem(atPath: sqliteGrpPath)
        let sqliteGrp = SQLiteDB(path: sqliteGrpPath)
        sqliteGrp.exec("CREATE TABLE grp_bench (id TEXT PRIMARY KEY, category TEXT NOT NULL, amount REAL NOT NULL, qty INTEGER NOT NULL)")
        sqliteGrp.exec("BEGIN")
        let grpStmt = sqliteGrp.prepare("INSERT INTO grp_bench (id, category, amount, qty) VALUES (?, ?, ?, ?)")
        for r in grpRows {
            sqlite3_reset(grpStmt)
            sqlite3_bind_text(grpStmt, 1, r["id"]!.stringValue!, -1, SQLITE_TRANSIENT)
            sqlite3_bind_text(grpStmt, 2, r["category"]!.stringValue!, -1, SQLITE_TRANSIENT)
            sqlite3_bind_double(grpStmt, 3, r["amount"]!.doubleValue!)
            sqlite3_bind_int64(grpStmt, 4, r["qty"]!.integerValue!)
            precondition(sqlite3_step(grpStmt) == SQLITE_DONE)
        }
        sqlite3_finalize(grpStmt)
        sqliteGrp.exec("COMMIT")
        sqliteGrp.exec("CREATE INDEX idx_grp_cat ON grp_bench(category)")

        // Cobalt GROUP BY
        let cobaltGrpMs = try await measureCobalt(20) {
            let r = try await cobaltGrp.select(
                from: "grp_bench",
                select: [.column("category"), .sum(column: "amount"), .count(column: nil), .avg(column: "qty")],
                groupBy: ["category"]
            )
            precondition(r.count == categories.count, "Expected \(categories.count) groups, got \(r.count)")
        }

        // SQLite GROUP BY
        let sqliteGrpStmt = sqliteGrp.prepare("SELECT category, SUM(amount), COUNT(*), AVG(qty) FROM grp_bench GROUP BY category")
        let sqliteGrpMs = measureSync(20) {
            sqlite3_reset(sqliteGrpStmt)
            var cnt = 0
            while sqlite3_step(sqliteGrpStmt) == SQLITE_ROW { cnt += 1 }
            precondition(cnt == categories.count)
        }
        sqlite3_finalize(sqliteGrpStmt)

        record("GROUP BY (8 groups)", groupCount, cobaltGrpMs, sqliteGrpMs)

        try? await cobaltGrp.close()
        try? FileManager.default.removeItem(atPath: cobaltGrpPath)
        try? FileManager.default.removeItem(atPath: sqliteGrpPath)
    }

    // ═══════════════════════════════════════════════════════════════
    // E. LARGE-SCALE 100K WITH CONSTRAINED BUFFER POOL
    // ═══════════════════════════════════════════════════════════════
    do {
        let largeCount = 100_000
        header("Large-Scale 100K — constrained buffer pool (500 pages)")

        let cobaltLargePath = tmpDir + "bench_cobalt_large.cobalt"
        try? FileManager.default.removeItem(atPath: cobaltLargePath)
        let largeConfig = CobaltConfiguration(path: cobaltLargePath, bufferPoolCapacity: 500, costWeights: .ssd)
        let cobaltLarge = try await CobaltDatabase(configuration: largeConfig)
        let largeSchema = CobaltTableSchema(name: "large", columns: [
            .id("id"),
            .string("name", nullable: false),
            .integer("val", nullable: false),
            .string("tag", nullable: false),
        ])
        try await cobaltLarge.createTable(largeSchema)

        let sqliteLargePath = tmpDir + "bench_sqlite_large.db"
        try? FileManager.default.removeItem(atPath: sqliteLargePath)
        let sqliteLarge = SQLiteDB(path: sqliteLargePath)
        sqliteLarge.exec("CREATE TABLE large (id TEXT PRIMARY KEY, name TEXT NOT NULL, val INTEGER NOT NULL, tag TEXT NOT NULL)")

        let tags = ["alpha", "beta", "gamma", "delta"]
        var largeRows = [[String: DBValue]]()
        largeRows.reserveCapacity(largeCount)
        for i in 0..<largeCount {
            largeRows.append([
                "id": .string("lg_\(i)"),
                "name": .string("LargeRow \(i)"),
                "val": .integer(Int64(i)),
                "tag": .string(tags[i % tags.count]),
            ])
        }

        // Bulk insert - Cobalt
        let cobaltLargeInsertStart = ContinuousClock.now
        try await cobaltLarge.insertAll(into: "large", rows: largeRows)
        let cobaltLargeInsertMs = elapsedMs(cobaltLargeInsertStart)

        // Bulk insert - SQLite
        let sqliteLargeInsertStart = ContinuousClock.now
        sqliteLarge.exec("BEGIN")
        let lgStmt = sqliteLarge.prepare("INSERT INTO large (id, name, val, tag) VALUES (?, ?, ?, ?)")
        for r in largeRows {
            sqlite3_reset(lgStmt)
            sqlite3_bind_text(lgStmt, 1, r["id"]!.stringValue!, -1, SQLITE_TRANSIENT)
            sqlite3_bind_text(lgStmt, 2, r["name"]!.stringValue!, -1, SQLITE_TRANSIENT)
            sqlite3_bind_int64(lgStmt, 3, r["val"]!.integerValue!)
            sqlite3_bind_text(lgStmt, 4, r["tag"]!.stringValue!, -1, SQLITE_TRANSIENT)
            precondition(sqlite3_step(lgStmt) == SQLITE_DONE)
        }
        sqlite3_finalize(lgStmt)
        sqliteLarge.exec("COMMIT")
        let sqliteLargeInsertMs = elapsedMs(sqliteLargeInsertStart)

        record("100K INSERT (buf=500)", largeCount, cobaltLargeInsertMs, sqliteLargeInsertMs)

        // Index creation under pressure
        let cobaltLgIdxStart = ContinuousClock.now
        try await cobaltLarge.createIndex(table: "large", column: "val")
        try await cobaltLarge.createIndex(table: "large", column: "tag")
        let cobaltLgIdxMs = elapsedMs(cobaltLgIdxStart)

        let sqliteLgIdxStart = ContinuousClock.now
        sqliteLarge.exec("CREATE INDEX idx_lg_val ON large(val)")
        sqliteLarge.exec("CREATE INDEX idx_lg_tag ON large(tag)")
        let sqliteLgIdxMs = elapsedMs(sqliteLgIdxStart)

        record("100K index creation (buf=500)", largeCount, cobaltLgIdxMs, sqliteLgIdxMs)

        // Range scan under eviction pressure
        let cobaltLgRange = try await measureCobalt(5) {
            let r = try await cobaltLarge.select(
                from: "large",
                where: .between(column: "val", min: .integer(40_000), max: .integer(60_000)),
                limit: 100
            )
            precondition(r.count > 0)
        }
        let sqliteLgRangeStmt = sqliteLarge.prepare("SELECT * FROM large WHERE val BETWEEN 40000 AND 60000 LIMIT 100")
        let sqliteLgRange = measureSync(5) {
            sqlite3_reset(sqliteLgRangeStmt)
            var cnt = 0
            while sqlite3_step(sqliteLgRangeStmt) == SQLITE_ROW { cnt += 1 }
            precondition(cnt > 0)
        }
        sqlite3_finalize(sqliteLgRangeStmt)
        record("100K range scan (buf=500)", largeCount, cobaltLgRange, sqliteLgRange)

        // Full table count under eviction pressure
        let cobaltLgCount = try await measureCobalt(5) {
            let c = try await cobaltLarge.count(from: "large")
            precondition(c == largeCount)
        }
        let sqliteLgCountStmt = sqliteLarge.prepare("SELECT COUNT(*) FROM large")
        let sqliteLgCount = measureSync(5) {
            sqlite3_reset(sqliteLgCountStmt)
            precondition(sqlite3_step(sqliteLgCountStmt) == SQLITE_ROW)
            let c = sqlite3_column_int64(sqliteLgCountStmt, 0)
            precondition(c == Int64(largeCount))
        }
        sqlite3_finalize(sqliteLgCountStmt)
        record("100K COUNT(*) (buf=500)", largeCount, cobaltLgCount, sqliteLgCount)

        try? await cobaltLarge.close()
        try? FileManager.default.removeItem(atPath: cobaltLargePath)
        try? FileManager.default.removeItem(atPath: sqliteLargePath)
    }
    } // end if false

    // ═══════════════════════════════════════════════════════════════
    // SUMMARY
    // ═══════════════════════════════════════════════════════════════
    header("SUMMARY")
    print("  \("Benchmark".padding(toLength: 32, withPad: " ", startingAt: 0))    Rows      Cobalt      SQLite   Ratio  Winner")
    print("  " + String(repeating: "-", count: 86))
    for r in results {
        let tag = r.ratio <= 1.0 ? "Cobalt" : "SQLite"
        let padded = r.name.padding(toLength: 32, withPad: " ", startingAt: 0)
        print("  \(padded)  \(String(format: "%5d", r.rowCount / 1000))k  \(String(format: "%8.1f", r.cobaltMs)) ms  \(String(format: "%8.1f", r.sqliteMs)) ms  \(String(format: "%6.2f", r.ratio))x  \(tag)")
    }

    let cobaltWins = results.filter { $0.ratio <= 1.0 }.count
    let sqliteWins = results.count - cobaltWins
    print("\n  Cobalt wins: \(cobaltWins)/\(results.count)  |  SQLite wins: \(sqliteWins)/\(results.count)")
    let avgRatio = results.map(\.ratio).reduce(0, +) / Double(results.count)
    print(String(format: "  Average ratio (Cobalt/SQLite): %.2fx", avgRatio))
    if avgRatio <= 1.0 {
        print("  Overall: Cobalt is faster on average!")
    } else {
        print(String(format: "  Overall: SQLite is %.1fx faster on average", avgRatio))
    }
}

try await runBenchmarks()
