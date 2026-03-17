import Testing
import Foundation
@testable import CobaltCore
@testable import CobaltQuery

// MARK: - Window Function Tests

@Test func testWindowRowNumber() {
    let rows: [Row] = [
        Row(values: ["dept": .string("A"), "name": .string("Alice"), "salary": .integer(50)]),
        Row(values: ["dept": .string("A"), "name": .string("Bob"), "salary": .integer(60)]),
        Row(values: ["dept": .string("B"), "name": .string("Carol"), "salary": .integer(70)]),
        Row(values: ["dept": .string("B"), "name": .string("Dave"), "salary": .integer(40)]),
    ]

    let spec = WindowSpec(partitionBy: ["dept"], orderBy: [OrderBy("salary", .ascending)])
    let result = WindowExecutor.executeWindow(rows: rows, function: .rowNumber, spec: spec, outputColumn: "rn")

    #expect(result.count == 4)

    // Partition A sorted by salary: Alice(50)=1, Bob(60)=2
    let alice = result.first { $0.values["name"] == .string("Alice") }!
    let bob = result.first { $0.values["name"] == .string("Bob") }!
    #expect(alice.values["rn"] == .integer(1))
    #expect(bob.values["rn"] == .integer(2))

    // Partition B sorted by salary: Dave(40)=1, Carol(70)=2
    let dave = result.first { $0.values["name"] == .string("Dave") }!
    let carol = result.first { $0.values["name"] == .string("Carol") }!
    #expect(dave.values["rn"] == .integer(1))
    #expect(carol.values["rn"] == .integer(2))
}

@Test func testWindowRank() {
    let rows: [Row] = [
        Row(values: ["name": .string("A"), "score": .integer(100)]),
        Row(values: ["name": .string("B"), "score": .integer(100)]),
        Row(values: ["name": .string("C"), "score": .integer(90)]),
    ]

    let spec = WindowSpec(partitionBy: [], orderBy: [OrderBy("score", .descending)])
    let result = WindowExecutor.executeWindow(rows: rows, function: .rank, spec: spec, outputColumn: "rnk")

    // A and B tied at rank 1, C at rank 3 (not 2, because standard rank skips)
    let a = result.first { $0.values["name"] == .string("A") }!
    let b = result.first { $0.values["name"] == .string("B") }!
    let c = result.first { $0.values["name"] == .string("C") }!
    #expect(a.values["rnk"] == .integer(1))
    #expect(b.values["rnk"] == .integer(1))
    #expect(c.values["rnk"] == .integer(3))
}

@Test func testWindowDenseRank() {
    let rows: [Row] = [
        Row(values: ["name": .string("A"), "score": .integer(100)]),
        Row(values: ["name": .string("B"), "score": .integer(100)]),
        Row(values: ["name": .string("C"), "score": .integer(90)]),
    ]

    let spec = WindowSpec(partitionBy: [], orderBy: [OrderBy("score", .descending)])
    let result = WindowExecutor.executeWindow(rows: rows, function: .denseRank, spec: spec, outputColumn: "drnk")

    let a = result.first { $0.values["name"] == .string("A") }!
    let b = result.first { $0.values["name"] == .string("B") }!
    let c = result.first { $0.values["name"] == .string("C") }!
    #expect(a.values["drnk"] == .integer(1))
    #expect(b.values["drnk"] == .integer(1))
    // Dense rank: C is rank 2 (no gap)
    #expect(c.values["drnk"] == .integer(2))
}

@Test func testWindowLag() {
    let rows: [Row] = [
        Row(values: ["id": .integer(1), "val": .string("a")]),
        Row(values: ["id": .integer(2), "val": .string("b")]),
        Row(values: ["id": .integer(3), "val": .string("c")]),
    ]

    let spec = WindowSpec(partitionBy: [], orderBy: [OrderBy("id", .ascending)])
    let result = WindowExecutor.executeWindow(
        rows: rows, function: .lag(column: "val", offset: 1, defaultValue: .string("none")),
        spec: spec, outputColumn: "prev_val"
    )

    let r0 = result.first { $0.values["id"] == .integer(1) }!
    let r1 = result.first { $0.values["id"] == .integer(2) }!
    let r2 = result.first { $0.values["id"] == .integer(3) }!
    #expect(r0.values["prev_val"] == .string("none"))
    #expect(r1.values["prev_val"] == .string("a"))
    #expect(r2.values["prev_val"] == .string("b"))
}

@Test func testWindowSumOver() {
    let rows: [Row] = [
        Row(values: ["dept": .string("A"), "salary": .integer(50)]),
        Row(values: ["dept": .string("A"), "salary": .integer(60)]),
        Row(values: ["dept": .string("B"), "salary": .integer(70)]),
    ]

    let spec = WindowSpec(partitionBy: ["dept"], orderBy: [])
    let result = WindowExecutor.executeWindow(rows: rows, function: .sumOver(column: "salary"), spec: spec, outputColumn: "dept_total")

    let deptA = result.filter { $0.values["dept"] == .string("A") }
    let deptB = result.filter { $0.values["dept"] == .string("B") }
    // Dept A total: 50 + 60 = 110
    for r in deptA { #expect(r.values["dept_total"] == .double(110.0)) }
    // Dept B total: 70
    for r in deptB { #expect(r.values["dept_total"] == .double(70.0)) }
}

// MARK: - EXPLAIN Tests

@Test func testExplainFullScan() {
    let rows = ExplainExecutor.explain(table: "users", condition: nil, hasIndex: false, joinCount: 0)
    #expect(rows.count >= 2)
    let planTexts = rows.compactMap { $0.values["plan"]?.stringValue }
    #expect(planTexts[0].contains("FULL TABLE SCAN"))
    #expect(planTexts[0].contains("users"))
    #expect(planTexts.last == "RESULT")
}

@Test func testExplainIndexScan() {
    let cond = WhereCondition.equals(column: "id", value: .integer(42))
    let rows = ExplainExecutor.explain(table: "users", condition: cond, hasIndex: true, joinCount: 0)
    let planTexts = rows.compactMap { $0.values["plan"]?.stringValue }
    #expect(planTexts[0].contains("INDEX SCAN"))
    #expect(planTexts[0].contains("id"))
}

@Test func testExplainWithJoins() {
    let rows = ExplainExecutor.explain(table: "orders", condition: nil, hasIndex: false, joinCount: 2)
    let planTexts = rows.compactMap { $0.values["plan"]?.stringValue }
    #expect(planTexts.contains { $0.contains("NESTED LOOP JOIN") && $0.contains("2 joins") })
}

@Test func testExplainWithFilter() {
    let cond = WhereCondition.greaterThan(column: "age", value: .integer(21))
    let rows = ExplainExecutor.explain(table: "people", condition: cond, hasIndex: false, joinCount: 0)
    let planTexts = rows.compactMap { $0.values["plan"]?.stringValue }
    #expect(planTexts.contains { $0.contains("FILTER") && $0.contains("age") })
}

// MARK: - CTE Definition Tests

@Test func testCTEDefinitionInit() {
    let def = CTEDefinition(name: "top_users", columns: ["id", "name"], query: "SELECT id, name FROM users", isRecursive: false)
    #expect(def.name == "top_users")
    #expect(def.columns == ["id", "name"])
    #expect(def.query == "SELECT id, name FROM users")
    #expect(def.isRecursive == false)
}

// MARK: - DBValue string helper (for test assertions)

extension DBValue {
    var stringValue: String? {
        if case .string(let s) = self { return s }
        return nil
    }
}
