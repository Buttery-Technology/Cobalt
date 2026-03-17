/// Constraint types for Cobalt table columns.

/// Referential action for foreign key ON DELETE / ON UPDATE clauses.
public enum ReferentialAction: String, Sendable, Codable {
    case cascade = "CASCADE"
    case restrict = "RESTRICT"
    case setNull = "SET NULL"
    case noAction = "NO ACTION"
}

/// Definition of a foreign key reference.
public struct ForeignKeyDef: Sendable, Codable {
    public let referencedTable: String
    public let referencedColumns: [String]
    public let onDelete: ReferentialAction
    public let onUpdate: ReferentialAction

    public init(
        referencedTable: String,
        referencedColumns: [String],
        onDelete: ReferentialAction = .noAction,
        onUpdate: ReferentialAction = .noAction
    ) {
        self.referencedTable = referencedTable
        self.referencedColumns = referencedColumns
        self.onDelete = onDelete
        self.onUpdate = onUpdate
    }
}

/// The kind of constraint applied to one or more columns.
public enum ConstraintType: Sendable, Codable {
    case primaryKey
    case unique
    case foreignKey(ForeignKeyDef)
    case check(String) // SQL expression stored as string
    case notNull
}

/// A named (or anonymous) constraint on a set of columns.
public struct ConstraintDef: Sendable, Codable {
    public let name: String?
    public let type: ConstraintType
    public let columns: [String]

    public init(name: String? = nil, type: ConstraintType, columns: [String]) {
        self.name = name
        self.type = type
        self.columns = columns
    }
}
