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
