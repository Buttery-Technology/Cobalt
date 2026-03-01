/// Represents a WHERE clause condition
public enum WhereCondition: Sendable {
    case equals(column: String, value: DBValue)
    case notEquals(column: String, value: DBValue)
    case lessThan(column: String, value: DBValue)
    case greaterThan(column: String, value: DBValue)
    case lessThanOrEqual(column: String, value: DBValue)
    case greaterThanOrEqual(column: String, value: DBValue)
    case `in`(column: String, values: [DBValue])
    case between(column: String, min: DBValue, max: DBValue)
    case like(column: String, pattern: String)
    case isNull(column: String)
    case isNotNull(column: String)
    case and([WhereCondition])
    case or([WhereCondition])
}

// MARK: - Concise Static Helpers

extension WhereCondition {
    public static func column(_ name: String, equals value: DBValue) -> WhereCondition {
        .equals(column: name, value: value)
    }

    public static func column(_ name: String, greaterThan value: DBValue) -> WhereCondition {
        .greaterThan(column: name, value: value)
    }

    public static func column(_ name: String, lessThan value: DBValue) -> WhereCondition {
        .lessThan(column: name, value: value)
    }

    public static func column(_ name: String, notEquals value: DBValue) -> WhereCondition {
        .notEquals(column: name, value: value)
    }

    public static func column(_ name: String, greaterThanOrEqual value: DBValue) -> WhereCondition {
        .greaterThanOrEqual(column: name, value: value)
    }

    public static func column(_ name: String, lessThanOrEqual value: DBValue) -> WhereCondition {
        .lessThanOrEqual(column: name, value: value)
    }

    public static func column(_ name: String, like pattern: String) -> WhereCondition {
        .like(column: name, pattern: pattern)
    }

    public static func column(_ name: String, in values: [DBValue]) -> WhereCondition {
        .in(column: name, values: values)
    }

    public static func column(_ name: String, between min: DBValue, and max: DBValue) -> WhereCondition {
        .between(column: name, min: min, max: max)
    }

    public static func columnIsNull(_ name: String) -> WhereCondition {
        .isNull(column: name)
    }

    public static func columnIsNotNull(_ name: String) -> WhereCondition {
        .isNotNull(column: name)
    }
}

// MARK: - Combinators

public func && (lhs: WhereCondition, rhs: WhereCondition) -> WhereCondition {
    .and([lhs, rhs])
}

public func || (lhs: WhereCondition, rhs: WhereCondition) -> WhereCondition {
    .or([lhs, rhs])
}

// MARK: - Query Modifiers

/// Sort direction for ORDER BY
public enum SortDirection: Sendable {
    case ascending
    case descending
}

/// A single ORDER BY clause entry
public struct OrderBy: Sendable {
    public let column: String
    public let direction: SortDirection

    public init(_ column: String, _ direction: SortDirection = .ascending) {
        self.column = column
        self.direction = direction
    }

    public static func asc(_ column: String) -> OrderBy { OrderBy(column, .ascending) }
    public static func desc(_ column: String) -> OrderBy { OrderBy(column, .descending) }
}

/// Encapsulates query modifiers: ORDER BY, LIMIT, OFFSET, DISTINCT
public struct QueryModifiers: Sendable {
    public var orderBy: [OrderBy]?
    public var limit: Int?
    public var offset: Int?
    public var distinct: Bool

    public init(orderBy: [OrderBy]? = nil, limit: Int? = nil, offset: Int? = nil, distinct: Bool = false) {
        self.orderBy = orderBy
        self.limit = limit
        self.offset = offset
        self.distinct = distinct
    }
}

// MARK: - JOIN Types

/// Type of join operation
public enum JoinType: Sendable {
    case inner
    case left
    case right
    case cross
}

/// A single JOIN clause
public struct JoinClause: Sendable {
    public let table: String
    public let type: JoinType
    public let leftColumn: String
    public let rightColumn: String

    public init(table: String, type: JoinType = .inner, on leftColumn: String, equals rightColumn: String) {
        self.table = table
        self.type = type
        self.leftColumn = leftColumn
        self.rightColumn = rightColumn
    }
}

// MARK: - GROUP BY Types

/// A GROUP BY query with optional HAVING clause
public struct GroupByClause: Sendable {
    public let columns: [String]
    public let having: WhereCondition?

    public init(columns: [String], having: WhereCondition? = nil) {
        self.columns = columns
        self.having = having
    }
}

/// Aggregate expressions for SELECT with GROUP BY
public enum SelectExpression: Sendable {
    case column(String)
    case count(column: String?)
    case sum(column: String)
    case avg(column: String)
    case min(column: String)
    case max(column: String)
}
