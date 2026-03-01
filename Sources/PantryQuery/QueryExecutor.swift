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
    private let encoder: JSONEncoder
    private let decoder: JSONDecoder

    public init(storageEngine: StorageEngine, indexManager: IndexManager) {
        self.storageEngine = storageEngine
        self.indexManager = indexManager
        let enc = JSONEncoder()
        enc.nonConformingFloatEncodingStrategy = .convertToString(positiveInfinity: "+inf", negativeInfinity: "-inf", nan: "nan")
        self.encoder = enc
        let dec = JSONDecoder()
        dec.nonConformingFloatDecodingStrategy = .convertFromString(positiveInfinity: "+inf", negativeInfinity: "-inf", nan: "nan")
        self.decoder = dec
    }

    // MARK: - SELECT

    public func executeSelect(from table: String, columns: [String]? = nil, where condition: WhereCondition? = nil, transactionContext: TransactionContext? = nil) async throws -> [Row] {
        let rows: [Row]

        // Attempt index lookup before falling back to table scan
        if let condition = condition,
           let indexed = try await indexManager.attemptIndexLookup(tableName: table, condition: condition) {
            // Index returned results; apply remaining in-memory filtering and strip __rid
            rows = indexed.filter { evaluateCondition(condition, row: $0) }.map { stripRID($0) }
        } else {
            // Full table scan
            let scanned = try await storageEngine.scanTable(table, transactionContext: transactionContext)
            let allRows = scanned.map { $0.1 }
            rows = condition != nil ? allRows.filter { evaluateCondition(condition!, row: $0) } : allRows
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
        let rowData = try encoder.encode(row)
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

                let rowData = try encoder.encode(updatedRow)
                let newRecord = Record(id: recordId, data: rowData)
                try await storageEngine.deleteRecord(id: recordId, tableName: table, transactionContext: transactionContext)
                try await storageEngine.insertRecord(newRecord, tableName: table, row: updatedRow, transactionContext: transactionContext)
                updatedCount += 1
            }
            return updatedCount
        }

        // Fallback: full table scan
        let scanned = try await storageEngine.scanTable(table, transactionContext: transactionContext)
        var updatedCount = 0

        for (record, row) in scanned {
            if condition == nil || evaluateCondition(condition!, row: row) {
                var updatedValues = row.values
                for (key, value) in values {
                    updatedValues[key] = value
                }
                let updatedRow = Row(values: updatedValues)

                let rowData = try encoder.encode(updatedRow)
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

        // Fallback: full table scan
        let scanned = try await storageEngine.scanTable(table, transactionContext: transactionContext)
        var deletedCount = 0

        for (record, row) in scanned {
            if condition == nil || evaluateCondition(condition!, row: row) {
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
            var sum: Double = 0
            var hasValue = false
            for row in rows {
                if let value = numericValue(row.values[column]) {
                    sum += value
                    hasValue = true
                }
            }
            return hasValue ? .double(sum) : .null

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

    private func evaluateCondition(_ condition: WhereCondition, row: Row) -> Bool {
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

    // MARK: - Helpers

    /// Strip the internal __rid field from index-returned rows before returning to users
    private func stripRID(_ row: Row) -> Row {
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
    private func matchLikePattern(_ string: String, pattern: String) -> Bool {
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
