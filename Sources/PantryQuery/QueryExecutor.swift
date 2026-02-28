import Foundation
import PantryCore
import PantryIndex

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

    public func executeSelect(from table: String, columns: [String]? = nil, where condition: WhereCondition? = nil) async throws -> [Row] {
        let rows: [Row]

        // Attempt index lookup before falling back to table scan
        if let condition = condition,
           let indexed = try await indexManager.attemptIndexLookup(tableName: table, condition: condition) {
            // Index returned results; apply remaining in-memory filtering
            rows = indexed.filter { evaluateCondition(condition, row: $0) }
        } else {
            // Full table scan
            let scanned = try await storageEngine.scanTable(table)
            let allRows = scanned.map { $0.1 }
            rows = condition != nil ? allRows.filter { evaluateCondition(condition!, row: $0) } : allRows
        }

        // Project only requested columns
        if let columns = columns {
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

    public func executeInsert(into table: String, row: Row) async throws {
        let rowData = try encoder.encode(row)
        let recordID = generateRecordID()
        let record = Record(id: recordID, data: rowData)
        try await storageEngine.insertRecord(record, tableName: table, row: row)
    }

    // MARK: - UPDATE

    public func executeUpdate(table: String, set values: [String: DBValue], where condition: WhereCondition?) async throws -> Int {
        let scanned = try await storageEngine.scanTable(table)
        var updatedCount = 0

        for (record, row) in scanned {
            if condition == nil || evaluateCondition(condition!, row: row) {
                var updatedValues = row.values
                for (key, value) in values {
                    updatedValues[key] = value
                }
                let updatedRow = Row(values: updatedValues)

                // Encode first so a failure doesn't lose the old record
                let rowData = try encoder.encode(updatedRow)
                let newRecord = Record(id: record.id, data: rowData)
                try await storageEngine.deleteRecord(id: record.id, tableName: table)
                try await storageEngine.insertRecord(newRecord, tableName: table, row: updatedRow)
                updatedCount += 1
            }
        }

        return updatedCount
    }

    // MARK: - DELETE

    public func executeDelete(from table: String, where condition: WhereCondition?) async throws -> Int {
        let scanned = try await storageEngine.scanTable(table)
        var deletedCount = 0

        for (record, row) in scanned {
            if condition == nil || evaluateCondition(condition!, row: row) {
                try await storageEngine.deleteRecord(id: record.id, tableName: table)
                deletedCount += 1
            }
        }

        return deletedCount
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

    private var nextRecordID: UInt64 = UInt64.random(in: 1...(UInt64.max / 2))

    private func generateRecordID() -> UInt64 {
        let id = nextRecordID
        nextRecordID &+= 1
        return id
    }
}
