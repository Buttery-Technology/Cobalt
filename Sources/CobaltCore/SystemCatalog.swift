import Foundation

/// In-memory system catalog that tracks metadata about tables, columns, constraints, indexes, and sequences.
/// This is a metadata layer persisted alongside the table registry.
public struct SystemCatalog: Sendable, Codable {

    // MARK: - Catalog Entry Types

    /// Metadata for a registered table.
    public struct TableEntry: Sendable, Codable {
        public let name: String
        public let createdAt: Date

        public init(name: String, createdAt: Date = Date()) {
            self.name = name
            self.createdAt = createdAt
        }
    }

    /// Metadata for a column within a table.
    public struct ColumnEntry: Sendable, Codable {
        public let tableName: String
        public let columnName: String
        public let ordinalPosition: Int
        public let dataType: String
        public let isNullable: Bool
        public let isPrimaryKey: Bool
        public let isAutoIncrement: Bool

        public init(tableName: String, columnName: String, ordinalPosition: Int,
                    dataType: String, isNullable: Bool, isPrimaryKey: Bool, isAutoIncrement: Bool = false) {
            self.tableName = tableName
            self.columnName = columnName
            self.ordinalPosition = ordinalPosition
            self.dataType = dataType
            self.isNullable = isNullable
            self.isPrimaryKey = isPrimaryKey
            self.isAutoIncrement = isAutoIncrement
        }
    }

    /// Metadata for a constraint.
    public struct ConstraintEntry: Sendable, Codable {
        public let tableName: String
        public let constraintName: String?
        public let constraint: ConstraintDef

        public init(tableName: String, constraintName: String?, constraint: ConstraintDef) {
            self.tableName = tableName
            self.constraintName = constraintName
            self.constraint = constraint
        }
    }

    /// Metadata for an index.
    public struct IndexEntry: Sendable, Codable {
        public let tableName: String
        public let indexName: String
        public let columns: [String]
        public let isUnique: Bool

        public init(tableName: String, indexName: String, columns: [String], isUnique: Bool = false) {
            self.tableName = tableName
            self.indexName = indexName
            self.columns = columns
            self.isUnique = isUnique
        }
    }

    /// Metadata for a sequence (auto-increment counter).
    public struct SequenceEntry: Sendable, Codable {
        public let tableName: String
        public let columnName: String
        public var currentValue: Int64

        public init(tableName: String, columnName: String, currentValue: Int64 = 0) {
            self.tableName = tableName
            self.columnName = columnName
            self.currentValue = currentValue
        }
    }

    // MARK: - Storage

    public var tables: [String: TableEntry]
    public var columns: [String: [ColumnEntry]]       // keyed by table name
    public var constraints: [String: [ConstraintEntry]] // keyed by table name
    public var indexes: [String: [IndexEntry]]          // keyed by table name
    public var sequences: [String: SequenceEntry]       // keyed by "table.column"

    public init() {
        self.tables = [:]
        self.columns = [:]
        self.constraints = [:]
        self.indexes = [:]
        self.sequences = [:]
    }

    // MARK: - Sequence Helpers

    /// Get the next value for a sequence identified by table + column, incrementing the counter.
    public mutating func nextSequenceValue(table: String, column: String) -> Int64 {
        let key = "\(table).\(column)"
        var entry = sequences[key] ?? SequenceEntry(tableName: table, columnName: column, currentValue: 0)
        entry.currentValue += 1
        sequences[key] = entry
        return entry.currentValue
    }

    /// Register a table in the catalog.
    public mutating func registerTable(_ name: String) {
        if tables[name] == nil {
            tables[name] = TableEntry(name: name)
        }
    }

    /// Remove a table and all associated metadata from the catalog.
    public mutating func removeTable(_ name: String) {
        tables.removeValue(forKey: name)
        columns.removeValue(forKey: name)
        constraints.removeValue(forKey: name)
        indexes.removeValue(forKey: name)
        // Remove sequences for this table
        sequences = sequences.filter { !$0.key.hasPrefix("\(name).") }
    }
}
