/// Represents a WHERE clause condition
public enum WhereCondition: Sendable {
    case equals(column: String, value: DBValue)
    case notEquals(column: String, value: DBValue)
    case lessThan(column: String, value: DBValue)
    case greaterThan(column: String, value: DBValue)
    case lessThanOrEqual(column: String, value: DBValue)
    case greaterThanOrEqual(column: String, value: DBValue)
    case isNull(column: String)
    case isNotNull(column: String)
    case and([WhereCondition])
    case or([WhereCondition])
}
