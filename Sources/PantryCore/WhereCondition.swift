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
