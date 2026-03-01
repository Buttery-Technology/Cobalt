import Foundation
import PantryCore
import PantryIndex

/// Aggregate functions supported by the query executor.
public enum AggregateFunction: Sendable {
    case count(column: String?)
    case sum(column: String)
    case avg(column: String)
    case min(column: String)
    case max(column: String)
}

/// Executes SELECT, INSERT, UPDATE, DELETE queries against the storage engine.
/// Uses IndexManager for index-accelerated lookups before falling back to table scan.
public actor QueryExecutor: Sendable {
    private let storageEngine: StorageEngine
    private let indexManager: IndexManager

    public init(storageEngine: StorageEngine, indexManager: IndexManager) {
        self.storageEngine = storageEngine
        self.indexManager = indexManager
    }

    // MARK: - SELECT

    public func executeSelect(from table: String, columns: [String]? = nil, where condition: WhereCondition? = nil, transactionContext: TransactionContext? = nil) async throws -> [Row] {
        let rows: [Row]

        // Attempt index lookup before falling back to table scan
        if let condition = condition,
           let indexed = try await indexManager.attemptIndexLookup(tableName: table, condition: condition) {
            // Index returned results; apply remaining in-memory filtering and strip __rid
            rows = indexed.filter { evaluateCondition(condition, row: $0) }.map { stripRID($0) }
        } else if let condition = condition {
            // Full table scan with parallel page decoding + lazy deserialization
            let pageIDs = try await storageEngine.getPageChain(tableName: table, transactionContext: transactionContext)
            let neededColumns = columnsReferenced(in: condition)
            let useLazy = neededColumns.count <= 3

            rows = try await withThrowingTaskGroup(of: (Int, [Row]).self) { group in
                for (index, pageID) in pageIDs.enumerated() {
                    group.addTask { [storageEngine] in
                        let page = try await storageEngine.getPage(pageID: pageID, transactionContext: transactionContext)
                        var pageRows: [Row] = []
                        for record in page.records {
                            if useLazy, let result = self.evaluateConditionLazy(condition, data: record.data) {
                                if result, let row = Row.fromBytes(record.data) {
                                    pageRows.append(row)
                                }
                            } else if let row = Row.fromBytes(record.data), self.evaluateCondition(condition, row: row) {
                                pageRows.append(row)
                            }
                        }
                        return (index, pageRows)
                    }
                }
                // Collect results maintaining page order
                var indexed: [(Int, [Row])] = []
                for try await result in group {
                    indexed.append(result)
                }
                return indexed.sorted { $0.0 < $1.0 }.flatMap { $0.1 }
            }
        } else {
            // No condition — parallel decode all rows
            let pageIDs = try await storageEngine.getPageChain(tableName: table, transactionContext: transactionContext)

            rows = try await withThrowingTaskGroup(of: (Int, [Row]).self) { group in
                for (index, pageID) in pageIDs.enumerated() {
                    group.addTask { [storageEngine] in
                        let page = try await storageEngine.getPage(pageID: pageID, transactionContext: transactionContext)
                        let pageRows = page.records.compactMap { Row.fromBytes($0.data) }
                        return (index, pageRows)
                    }
                }
                var indexed: [(Int, [Row])] = []
                for try await result in group {
                    indexed.append(result)
                }
                return indexed.sorted { $0.0 < $1.0 }.flatMap { $0.1 }
            }
        }

        // Project only requested columns
        if let columns = columns, !columns.isEmpty {
            return rows.map { row in
                let projectedValues = columns.reduce(into: [String: DBValue]()) { result, column in
                    result[column] = row.values[column] ?? .null
                }
                return Row(values: projectedValues)
            }
        }

        return rows
    }

    // MARK: - INSERT

    public func executeInsert(into table: String, row: Row, transactionContext: TransactionContext? = nil) async throws {
        let rowData = row.toBytes()
        let recordID = generateRecordID()
        let record = Record(id: recordID, data: rowData)
        try await storageEngine.insertRecord(record, tableName: table, row: row, transactionContext: transactionContext)
    }

    // MARK: - UPDATE

    public func executeUpdate(table: String, set values: [String: DBValue], where condition: WhereCondition?, transactionContext: TransactionContext? = nil) async throws -> Int {
        // Try index-accelerated path when a condition with __rid-enabled index results is available
        if let condition = condition,
           let indexedRows = try await indexManager.attemptIndexLookup(tableName: table, condition: condition) {
            var updatedCount = 0
            for indexRow in indexedRows where evaluateCondition(condition, row: indexRow) {
                guard case .integer(let ridSigned) = indexRow.values["__rid"] else { continue }
                let recordId = UInt64(bitPattern: ridSigned)

                var updatedValues = indexRow.values
                updatedValues.removeValue(forKey: "__rid")
                for (key, value) in values { updatedValues[key] = value }
                let updatedRow = Row(values: updatedValues)

                let rowData = updatedRow.toBytes()
                let newRecord = Record(id: recordId, data: rowData)
                try await storageEngine.deleteRecord(id: recordId, tableName: table, transactionContext: transactionContext)
                try await storageEngine.insertRecord(newRecord, tableName: table, row: updatedRow, transactionContext: transactionContext)
                updatedCount += 1
            }
            return updatedCount
        }

        // Fallback: full table scan with raw records
        let rawRecords = try await storageEngine.scanTableRaw(table, transactionContext: transactionContext)
        var updatedCount = 0

        for (record, data) in rawRecords {
            guard let row = Row.fromBytes(data) else { continue }
            if condition == nil || evaluateCondition(condition!, row: row) {
                var updatedValues = row.values
                for (key, value) in values {
                    updatedValues[key] = value
                }
                let updatedRow = Row(values: updatedValues)

                let rowData = updatedRow.toBytes()
                let newRecord = Record(id: record.id, data: rowData)
                try await storageEngine.deleteRecord(id: record.id, tableName: table, transactionContext: transactionContext)
                try await storageEngine.insertRecord(newRecord, tableName: table, row: updatedRow, transactionContext: transactionContext)
                updatedCount += 1
            }
        }

        return updatedCount
    }

    // MARK: - DELETE

    public func executeDelete(from table: String, where condition: WhereCondition?, transactionContext: TransactionContext? = nil) async throws -> Int {
        // Try index-accelerated path
        if let condition = condition,
           let indexedRows = try await indexManager.attemptIndexLookup(tableName: table, condition: condition) {
            var deletedCount = 0
            for indexRow in indexedRows where evaluateCondition(condition, row: indexRow) {
                guard case .integer(let ridSigned) = indexRow.values["__rid"] else { continue }
                let recordId = UInt64(bitPattern: ridSigned)
                try await storageEngine.deleteRecord(id: recordId, tableName: table, transactionContext: transactionContext)
                deletedCount += 1
            }
            return deletedCount
        }

        // Fallback: full table scan with raw records
        let rawRecords = try await storageEngine.scanTableRaw(table, transactionContext: transactionContext)
        var deletedCount = 0

        for (record, data) in rawRecords {
            if condition == nil {
                try await storageEngine.deleteRecord(id: record.id, tableName: table, transactionContext: transactionContext)
                deletedCount += 1
            } else if let row = Row.fromBytes(data), evaluateCondition(condition!, row: row) {
                try await storageEngine.deleteRecord(id: record.id, tableName: table, transactionContext: transactionContext)
                deletedCount += 1
            }
        }

        return deletedCount
    }

    // MARK: - AGGREGATE

    public func executeAggregate(from table: String, _ function: AggregateFunction, where condition: WhereCondition? = nil, transactionContext: TransactionContext? = nil) async throws -> DBValue {
        let rows = try await executeSelect(from: table, where: condition, transactionContext: transactionContext)

        switch function {
        case .count(let column):
            if let column = column {
                // Count non-null values in the column
                let count = rows.filter { $0.values[column] != nil && $0.values[column] != .null }.count
                return .integer(Int64(count))
            }
            return .integer(Int64(rows.count))

        case .sum(let column):
            var intSum: Int64 = 0
            var doubleSum: Double = 0
            var allIntegers = true
            var hasValue = false
            for row in rows {
                guard let dbValue = row.values[column] else { continue }
                switch dbValue {
                case .integer(let v):
                    hasValue = true
                    let (result, overflow) = intSum.addingReportingOverflow(v)
                    if overflow { allIntegers = false }
                    intSum = result
                    doubleSum += Double(v)
                case .double(let v):
                    hasValue = true
                    allIntegers = false
                    doubleSum += v
                default: break
                }
            }
            if !hasValue { return .null }
            return allIntegers ? .integer(intSum) : .double(doubleSum)

        case .avg(let column):
            var sum: Double = 0
            var count = 0
            for row in rows {
                if let value = numericValue(row.values[column]) {
                    sum += value
                    count += 1
                }
            }
            return count > 0 ? .double(sum / Double(count)) : .null

        case .min(let column):
            var result: DBValue = .null
            for row in rows {
                guard let value = row.values[column], value != .null else { continue }
                if result == .null || value < result {
                    result = value
                }
            }
            return result

        case .max(let column):
            var result: DBValue = .null
            for row in rows {
                guard let value = row.values[column], value != .null else { continue }
                if result == .null || value > result {
                    result = value
                }
            }
            return result
        }
    }

    private func numericValue(_ value: DBValue?) -> Double? {
        guard let value = value else { return nil }
        switch value {
        case .integer(let v): return Double(v)
        case .double(let v): return v
        default: return nil
        }
    }

    // MARK: - Condition Evaluation

    private nonisolated func evaluateCondition(_ condition: WhereCondition, row: Row) -> Bool {
        switch condition {
        case let .equals(column, value):
            // SQL: NULL = anything is false; use isNull for NULL checks
            if value == .null { return false }
            guard let rowValue = row.values[column], rowValue != .null else { return false }
            return rowValue == value
        case let .notEquals(column, value):
            // SQL: NULL != anything is false; use isNotNull for NULL checks
            if value == .null { return false }
            guard let rowValue = row.values[column], rowValue != .null else { return false }
            return rowValue != value
        case let .lessThan(column, value):
            guard let rowValue = row.values[column], rowValue != .null, value != .null else { return false }
            return rowValue < value
        case let .greaterThan(column, value):
            guard let rowValue = row.values[column], rowValue != .null, value != .null else { return false }
            return rowValue > value
        case let .lessThanOrEqual(column, value):
            guard let rowValue = row.values[column], rowValue != .null, value != .null else { return false }
            return rowValue <= value
        case let .greaterThanOrEqual(column, value):
            guard let rowValue = row.values[column], rowValue != .null, value != .null else { return false }
            return rowValue >= value
        case let .in(column, values):
            guard let rowValue = row.values[column], rowValue != .null else { return false }
            return values.contains(rowValue)
        case let .between(column, min, max):
            guard let rowValue = row.values[column], rowValue != .null, min != .null, max != .null else { return false }
            return rowValue >= min && rowValue <= max
        case let .like(column, pattern):
            guard let rowValue = row.values[column], case .string(let str) = rowValue else { return false }
            return matchLikePattern(str, pattern: pattern)
        case let .isNull(column):
            return row.values[column] == nil || row.values[column] == .null
        case let .isNotNull(column):
            return row.values[column] != nil && row.values[column] != .null
        case let .and(conditions):
            return conditions.allSatisfy { evaluateCondition($0, row: row) }
        case let .or(conditions):
            return conditions.contains { evaluateCondition($0, row: row) }
        }
    }

    // MARK: - Lazy Evaluation Helpers

    /// Extract all column names referenced in a WHERE condition.
    private nonisolated func columnsReferenced(in condition: WhereCondition) -> Set<String> {
        switch condition {
        case .equals(let col, _), .notEquals(let col, _),
             .lessThan(let col, _), .greaterThan(let col, _),
             .lessThanOrEqual(let col, _), .greaterThanOrEqual(let col, _):
            return [col]
        case .in(let col, _), .between(let col, _, _), .like(let col, _):
            return [col]
        case .isNull(let col), .isNotNull(let col):
            return [col]
        case .and(let subs):
            return subs.reduce(into: Set<String>()) { $0.formUnion(columnsReferenced(in: $1)) }
        case .or(let subs):
            return subs.reduce(into: Set<String>()) { $0.formUnion(columnsReferenced(in: $1)) }
        }
    }

    /// Evaluate a WHERE condition against raw binary data using partial column extraction.
    /// Returns nil if the condition type is too complex for lazy eval.
    private nonisolated func evaluateConditionLazy(_ condition: WhereCondition, data: Data) -> Bool? {
        switch condition {
        case .equals(let col, let value):
            if value == .null { return false }
            guard let rowValue = Row.columnValue(named: col, from: data) else { return false }
            if rowValue == .null { return false }
            return rowValue == value
        case .notEquals(let col, let value):
            if value == .null { return false }
            guard let rowValue = Row.columnValue(named: col, from: data) else { return false }
            if rowValue == .null { return false }
            return rowValue != value
        case .lessThan(let col, let value):
            guard let rowValue = Row.columnValue(named: col, from: data), rowValue != .null, value != .null else { return false }
            return rowValue < value
        case .greaterThan(let col, let value):
            guard let rowValue = Row.columnValue(named: col, from: data), rowValue != .null, value != .null else { return false }
            return rowValue > value
        case .lessThanOrEqual(let col, let value):
            guard let rowValue = Row.columnValue(named: col, from: data), rowValue != .null, value != .null else { return false }
            return rowValue <= value
        case .greaterThanOrEqual(let col, let value):
            guard let rowValue = Row.columnValue(named: col, from: data), rowValue != .null, value != .null else { return false }
            return rowValue >= value
        case .isNull(let col):
            let val = Row.columnValue(named: col, from: data)
            return val == nil || val == .null
        case .isNotNull(let col):
            guard let val = Row.columnValue(named: col, from: data) else { return false }
            return val != .null
        case .and(let subs):
            for sub in subs {
                guard let result = evaluateConditionLazy(sub, data: data) else { return nil }
                if !result { return false }
            }
            return true
        case .or(let subs):
            for sub in subs {
                guard let result = evaluateConditionLazy(sub, data: data) else { return nil }
                if result { return true }
            }
            return false
        default:
            return nil // Fall back to full decode
        }
    }

    // MARK: - Helpers

    /// Strip the internal __rid field from index-returned rows before returning to users
    private nonisolated func stripRID(_ row: Row) -> Row {
        guard row.values["__rid"] != nil else { return row }
        var values = row.values
        values.removeValue(forKey: "__rid")
        return Row(values: values)
    }

    private var nextRecordID: UInt64 = UInt64.random(in: 1...(UInt64.max / 2))

    private func generateRecordID() -> UInt64 {
        let id = nextRecordID
        nextRecordID &+= 1
        return id
    }

    /// SQL LIKE pattern matching: % matches any sequence, _ matches any single character
    private nonisolated func matchLikePattern(_ string: String, pattern: String) -> Bool {
        let s = Array(string)
        let p = Array(pattern)
        var si = 0, pi = 0
        var starSi = -1, starPi = -1

        while si < s.count {
            if pi < p.count && p[pi] == "%" {
                starPi = pi
                starSi = si
                pi += 1
            } else if pi < p.count && (p[pi] == "_" || p[pi] == s[si]) {
                si += 1
                pi += 1
            } else if starPi >= 0 {
                pi = starPi + 1
                starSi += 1
                si = starSi
            } else {
                return false
            }
        }

        while pi < p.count && p[pi] == "%" {
            pi += 1
        }

        return pi == p.count
    }
}
