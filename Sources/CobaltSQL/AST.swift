import Foundation

/// Top-level SQL statement
public indirect enum Statement: Sendable {
    case select(SelectStatement)
    case insert(InsertStatement)
    case update(UpdateStatement)
    case delete(DeleteStatement)
    case createTable(CreateTableStatement)
    case dropTable(DropTableStatement)
    case alterTable(AlterTableStatement)
    case createIndex(CreateIndexStatement)
    case dropIndex(DropIndexStatement)
    case explain(ExplainStatement)
    case begin(BeginStatement)
    case commit
    case rollback
    case createTrigger(CreateTriggerStatement)
    case dropTrigger(DropTriggerStatement)
    case vacuum(VacuumStatement)
    case set(SetStatement)
    case show(ShowStatement)
    case reset(ResetStatement)
    case discard(DiscardStatement)
    case compound(CompoundSelectStatement)
    case createView(CreateViewStatement)
    case dropView(DropViewStatement)
}

// MARK: - CREATE VIEW

public struct CreateViewStatement: Sendable {
    public var name: String
    public var orReplace: Bool
    public var columns: [String]?
    public var query: SelectStatement

    public init(name: String, orReplace: Bool = false, columns: [String]? = nil, query: SelectStatement) {
        self.name = name
        self.orReplace = orReplace
        self.columns = columns
        self.query = query
    }
}

// MARK: - DROP VIEW

public struct DropViewStatement: Sendable {
    public var name: String
    public var ifExists: Bool

    public init(name: String, ifExists: Bool = false) {
        self.name = name
        self.ifExists = ifExists
    }
}

// MARK: - CTE (Common Table Expression)

public struct CTEDef: Sendable {
    public var name: String
    public var columns: [String]?
    public var query: SelectStatement
    public var isRecursive: Bool

    public init(name: String, columns: [String]? = nil, query: SelectStatement, isRecursive: Bool = false) {
        self.name = name
        self.columns = columns
        self.query = query
        self.isRecursive = isRecursive
    }
}

// MARK: - SELECT

public struct SelectStatement: Sendable {
    public var ctes: [CTEDef]
    public var distinct: Bool
    public var columns: [SelectItem]
    public var from: TableRef?
    public var joins: [JoinItem]
    public var whereClause: Expression?
    public var groupBy: [Expression]
    public var having: Expression?
    public var orderBy: [OrderByItem]
    public var limit: Expression?
    public var offset: Expression?

    public init(
        ctes: [CTEDef] = [],
        distinct: Bool = false,
        columns: [SelectItem] = [],
        from: TableRef? = nil,
        joins: [JoinItem] = [],
        whereClause: Expression? = nil,
        groupBy: [Expression] = [],
        having: Expression? = nil,
        orderBy: [OrderByItem] = [],
        limit: Expression? = nil,
        offset: Expression? = nil
    ) {
        self.ctes = ctes
        self.distinct = distinct
        self.columns = columns
        self.from = from
        self.joins = joins
        self.whereClause = whereClause
        self.groupBy = groupBy
        self.having = having
        self.orderBy = orderBy
        self.limit = limit
        self.offset = offset
    }
}

/// A single item in the SELECT list
public enum SelectItem: Sendable {
    /// All columns: *
    case allColumns
    /// All columns from a table: table.*
    case tableAllColumns(String)
    /// An expression with optional alias: expr AS alias
    case expression(Expression, alias: String?)
}

/// A table reference (FROM clause)
public indirect enum TableRef: Sendable {
    case table(name: String, alias: String?)
    case subquery(SelectStatement, alias: String)
}

/// A JOIN clause
public struct JoinItem: Sendable {
    public var joinType: ASTJoinType
    public var table: TableRef
    public var condition: Expression?

    public init(joinType: ASTJoinType, table: TableRef, condition: Expression?) {
        self.joinType = joinType
        self.table = table
        self.condition = condition
    }
}

public enum ASTJoinType: Sendable {
    case inner
    case left
    case right
    case cross
}

/// ORDER BY item
public struct OrderByItem: Sendable {
    public var expression: Expression
    public var ascending: Bool

    public init(expression: Expression, ascending: Bool = true) {
        self.expression = expression
        self.ascending = ascending
    }
}

// MARK: - INSERT

public struct InsertStatement: Sendable {
    public var table: String
    public var columns: [String]?
    public var values: [[Expression]]
    public var onConflict: OnConflictClause?
    public var returning: [SelectItem]?

    public init(table: String, columns: [String]?, values: [[Expression]], onConflict: OnConflictClause? = nil, returning: [SelectItem]? = nil) {
        self.table = table
        self.columns = columns
        self.values = values
        self.onConflict = onConflict
        self.returning = returning
    }
}

// MARK: - ON CONFLICT

public struct OnConflictClause: Sendable {
    public var columns: [String]?
    public var action: OnConflictAction

    public init(columns: [String]? = nil, action: OnConflictAction) {
        self.columns = columns
        self.action = action
    }
}

public enum OnConflictAction: Sendable {
    case doNothing
    case doUpdate(assignments: [OnConflictAssignment])
}

public struct OnConflictAssignment: Sendable {
    public var column: String
    public var value: Expression

    public init(column: String, value: Expression) {
        self.column = column
        self.value = value
    }
}

// MARK: - UPDATE

public struct UpdateStatement: Sendable {
    public var table: String
    public var assignments: [(column: String, value: Expression)]
    public var whereClause: Expression?
    public var returning: [SelectItem]?

    public init(table: String, assignments: [(column: String, value: Expression)], whereClause: Expression?, returning: [SelectItem]? = nil) {
        self.table = table
        self.assignments = assignments
        self.whereClause = whereClause
        self.returning = returning
    }
}

// MARK: - DELETE

public struct DeleteStatement: Sendable {
    public var table: String
    public var whereClause: Expression?
    public var returning: [SelectItem]?

    public init(table: String, whereClause: Expression?, returning: [SelectItem]? = nil) {
        self.table = table
        self.whereClause = whereClause
        self.returning = returning
    }
}

// MARK: - CREATE TABLE

public struct CreateTableStatement: Sendable {
    public var name: String
    public var ifNotExists: Bool
    public var columns: [ColumnDef]

    public init(name: String, ifNotExists: Bool = false, columns: [ColumnDef]) {
        self.name = name
        self.ifNotExists = ifNotExists
        self.columns = columns
    }
}

public struct ColumnDef: Sendable {
    public var name: String
    public var dataType: SQLDataType
    public var isPrimaryKey: Bool
    public var isNullable: Bool
    public var isUnique: Bool
    public var defaultValue: Expression?

    public init(name: String, dataType: SQLDataType, isPrimaryKey: Bool = false,
                isNullable: Bool = true, isUnique: Bool = false, defaultValue: Expression? = nil) {
        self.name = name
        self.dataType = dataType
        self.isPrimaryKey = isPrimaryKey
        self.isNullable = isNullable
        self.isUnique = isUnique
        self.defaultValue = defaultValue
    }
}

public enum SQLDataType: Sendable {
    case integer
    case text
    case real
    case blob
    case boolean
    case varchar(Int?)
    case serial
}

// MARK: - DROP TABLE

public struct DropTableStatement: Sendable {
    public var name: String
    public var ifExists: Bool

    public init(name: String, ifExists: Bool = false) {
        self.name = name
        self.ifExists = ifExists
    }
}

// MARK: - ALTER TABLE

public struct AlterTableStatement: Sendable {
    public var table: String
    public var action: AlterAction

    public init(table: String, action: AlterAction) {
        self.table = table
        self.action = action
    }
}

public enum AlterAction: Sendable {
    case addColumn(ColumnDef)
    case dropColumn(String)
    case renameColumn(from: String, to: String)
}

// MARK: - CREATE INDEX

public struct CreateIndexStatement: Sendable {
    public var name: String?
    public var table: String
    public var columns: [String]
    public var unique: Bool

    public init(name: String?, table: String, columns: [String], unique: Bool = false) {
        self.name = name
        self.table = table
        self.columns = columns
        self.unique = unique
    }
}

// MARK: - DROP INDEX

public struct DropIndexStatement: Sendable {
    public var name: String

    public init(name: String) {
        self.name = name
    }
}

// MARK: - EXPLAIN

public final class ExplainStatement: @unchecked Sendable {
    public let analyze: Bool
    public let statement: Statement

    public init(analyze: Bool, statement: Statement) {
        self.analyze = analyze
        self.statement = statement
    }

}

// MARK: - BEGIN

public struct BeginStatement: Sendable {
    public init() {}
}

// MARK: - CREATE TRIGGER

public struct CreateTriggerStatement: Sendable {
    public var name: String
    public var timing: String   // "BEFORE" or "AFTER"
    public var event: String    // "INSERT", "UPDATE", or "DELETE"
    public var table: String
    public var forEach: String  // "ROW" or "STATEMENT"
    public var body: [String]   // SQL statements inside BEGIN...END

    public init(name: String, timing: String, event: String, table: String, forEach: String, body: [String]) {
        self.name = name
        self.timing = timing
        self.event = event
        self.table = table
        self.forEach = forEach
        self.body = body
    }
}

// MARK: - DROP TRIGGER

public struct DropTriggerStatement: Sendable {
    public var name: String

    public init(name: String) {
        self.name = name
    }
}

// MARK: - VACUUM

public struct VacuumStatement: Sendable {
    public var table: String?

    public init(table: String? = nil) {
        self.table = table
    }
}

// MARK: - Expression

public indirect enum Expression: Sendable {
    /// Column reference, optionally qualified: table.column
    case column(table: String?, name: String)
    /// Wildcard: *
    case wildcard
    /// Integer literal
    case integerLiteral(Int64)
    /// Floating-point literal
    case doubleLiteral(Double)
    /// String literal
    case stringLiteral(String)
    /// Boolean literal
    case booleanLiteral(Bool)
    /// NULL literal
    case nullLiteral
    /// Binary operation: left op right
    case binaryOp(left: Expression, op: BinaryOperator, right: Expression)
    /// Unary operation: op expr
    case unaryOp(op: UnaryOperator, operand: Expression)
    /// Function call: name(args...)
    case function(name: String, args: [Expression])
    /// Aggregate function: COUNT, SUM, etc.
    case aggregate(AggregateType, Expression?)
    /// BETWEEN: expr BETWEEN low AND high
    case between(Expression, low: Expression, high: Expression)
    /// IN list: expr IN (val1, val2, ...)
    case inList(Expression, [Expression])
    /// IN subquery: expr IN (SELECT ...)
    case inSubquery(Expression, SelectStatement)
    /// EXISTS (SELECT ...)
    case exists(SelectStatement)
    /// IS NULL / IS NOT NULL
    case isNull(Expression)
    case isNotNull(Expression)
    /// LIKE: expr LIKE pattern
    case like(Expression, pattern: Expression)
    /// NOT LIKE
    case notLike(Expression, pattern: Expression)
    /// CAST(expr AS type)
    case cast(Expression, SQLDataType)
    /// Scalar subquery: (SELECT ...)
    case subquery(SelectStatement)
    /// Positional parameter: $1, $2, etc.
    case parameter(Int)
    /// CASE WHEN ... THEN ... ELSE ... END
    case caseExpr(operand: Expression?, whens: [(condition: Expression, result: Expression)], elseResult: Expression?)
}

public enum BinaryOperator: Sendable {
    case add, subtract, multiply, divide, modulo
    case equal, notEqual, lessThan, greaterThan, lessOrEqual, greaterOrEqual
    case and, or
    case concat
}

public enum UnaryOperator: Sendable {
    case not
    case negate
}

public enum AggregateType: String, Sendable {
    case count = "COUNT"
    case sum = "SUM"
    case avg = "AVG"
    case min = "MIN"
    case max = "MAX"
}

// MARK: - SET

public struct SetStatement: Sendable {
    public var name: String
    public var value: String

    public init(name: String, value: String) {
        self.name = name
        self.value = value
    }
}

// MARK: - SHOW

public struct ShowStatement: Sendable {
    public var name: String  // "ALL" for SHOW ALL

    public init(name: String) {
        self.name = name
    }
}

// MARK: - RESET

public struct ResetStatement: Sendable {
    public var name: String  // "ALL" for RESET ALL

    public init(name: String) {
        self.name = name
    }
}

// MARK: - DISCARD

public struct DiscardStatement: Sendable {
    public var target: String  // "ALL", "PLANS", "SEQUENCES", "TEMP"

    public init(target: String) {
        self.target = target
    }
}

// MARK: - Compound SELECT (UNION / INTERSECT / EXCEPT)

public struct CompoundSelectStatement: Sendable {
    public var left: SelectStatement
    public var operation: SetOperationType
    public var right: SelectStatement
    public var orderBy: [OrderByItem]
    public var limit: Expression?
    public var offset: Expression?

    public init(left: SelectStatement, operation: SetOperationType, right: SelectStatement, orderBy: [OrderByItem] = [], limit: Expression? = nil, offset: Expression? = nil) {
        self.left = left
        self.operation = operation
        self.right = right
        self.orderBy = orderBy
        self.limit = limit
        self.offset = offset
    }
}

public enum SetOperationType: Sendable {
    case union
    case unionAll
    case intersect
    case except
}
