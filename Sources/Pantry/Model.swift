import Foundation
import PantryCore

// MARK: - PantryModel Protocol

/// A type-safe model that can be stored in a Pantry database.
/// Models are value types (structs) with a string ID and Codable properties.
public protocol PantryModel: Codable, Sendable {
    /// The database table name for this model type.
    /// Defaults to the lowercased type name + "s" (e.g., `User` → `"users"`).
    static var tableName: String { get }
    /// The unique identifier for this model instance.
    var id: String { get set }
}

extension PantryModel {
    /// Default table name: lowercased type name + "s" (e.g., `User` → `"users"`).
    public static var tableName: String {
        String(describing: Self.self).lowercased() + "s"
    }
}

// MARK: - Column (Type-Safe Column Reference)

/// A type-safe reference to a column on a model. The generic parameters ensure
/// that filters can only compare columns to values of the correct type.
public struct Column<M: PantryModel, V: DBValueConvertible>: Sendable {
    /// The column name in the database.
    public let key: String

    public init(key: String) {
        self.key = key
    }
}

// MARK: - ModelFilter

/// A type-safe filter scoped to a specific model type.
/// Prevents accidentally mixing columns from different models.
public struct ModelFilter<M: PantryModel>: Sendable {
    /// The underlying WhereCondition used by the query engine.
    public let condition: WhereCondition
}

// MARK: - Equality Operators (==, !=)

/// Column == value
public func == <M, V: DBValueConvertible & Equatable>(lhs: Column<M, V>, rhs: V) -> ModelFilter<M> {
    ModelFilter(condition: .equals(column: lhs.key, value: rhs.toDBValue()))
}

/// Column != value
public func != <M, V: DBValueConvertible & Equatable>(lhs: Column<M, V>, rhs: V) -> ModelFilter<M> {
    ModelFilter(condition: .notEquals(column: lhs.key, value: rhs.toDBValue()))
}

// MARK: - Comparison Operators (<, >, <=, >=)

/// Column < value
public func < <M, V: DBValueConvertible & Comparable>(lhs: Column<M, V>, rhs: V) -> ModelFilter<M> {
    ModelFilter(condition: .lessThan(column: lhs.key, value: rhs.toDBValue()))
}

/// Column > value
public func > <M, V: DBValueConvertible & Comparable>(lhs: Column<M, V>, rhs: V) -> ModelFilter<M> {
    ModelFilter(condition: .greaterThan(column: lhs.key, value: rhs.toDBValue()))
}

/// Column <= value
public func <= <M, V: DBValueConvertible & Comparable>(lhs: Column<M, V>, rhs: V) -> ModelFilter<M> {
    ModelFilter(condition: .lessThanOrEqual(column: lhs.key, value: rhs.toDBValue()))
}

/// Column >= value
public func >= <M, V: DBValueConvertible & Comparable>(lhs: Column<M, V>, rhs: V) -> ModelFilter<M> {
    ModelFilter(condition: .greaterThanOrEqual(column: lhs.key, value: rhs.toDBValue()))
}

// MARK: - Optional Column Operators (accept unwrapped values)

/// Optional Column == unwrapped value
public func == <M, V: DBValueConvertible & Equatable>(lhs: Column<M, V?>, rhs: V) -> ModelFilter<M> {
    ModelFilter(condition: .equals(column: lhs.key, value: rhs.toDBValue()))
}

/// Optional Column != unwrapped value
public func != <M, V: DBValueConvertible & Equatable>(lhs: Column<M, V?>, rhs: V) -> ModelFilter<M> {
    ModelFilter(condition: .notEquals(column: lhs.key, value: rhs.toDBValue()))
}

/// Optional Column < unwrapped value
public func < <M, V: DBValueConvertible & Comparable>(lhs: Column<M, V?>, rhs: V) -> ModelFilter<M> {
    ModelFilter(condition: .lessThan(column: lhs.key, value: rhs.toDBValue()))
}

/// Optional Column > unwrapped value
public func > <M, V: DBValueConvertible & Comparable>(lhs: Column<M, V?>, rhs: V) -> ModelFilter<M> {
    ModelFilter(condition: .greaterThan(column: lhs.key, value: rhs.toDBValue()))
}

/// Optional Column <= unwrapped value
public func <= <M, V: DBValueConvertible & Comparable>(lhs: Column<M, V?>, rhs: V) -> ModelFilter<M> {
    ModelFilter(condition: .lessThanOrEqual(column: lhs.key, value: rhs.toDBValue()))
}

/// Optional Column >= unwrapped value
public func >= <M, V: DBValueConvertible & Comparable>(lhs: Column<M, V?>, rhs: V) -> ModelFilter<M> {
    ModelFilter(condition: .greaterThanOrEqual(column: lhs.key, value: rhs.toDBValue()))
}

// MARK: - Null Checks for Optional Columns

extension Column where V: _OptionalProtocol {
    /// Filter for rows where this optional column IS NULL.
    public func isNull() -> ModelFilter<M> {
        ModelFilter(condition: .isNull(column: key))
    }

    /// Filter for rows where this optional column IS NOT NULL.
    public func isNotNull() -> ModelFilter<M> {
        ModelFilter(condition: .isNotNull(column: key))
    }
}

/// Internal protocol to detect Optional types in Column generics.
public protocol _OptionalProtocol {}
extension Optional: _OptionalProtocol {}

// MARK: - Combining Filters (&&, ||)

/// Combine two filters with AND.
public func && <M>(lhs: ModelFilter<M>, rhs: ModelFilter<M>) -> ModelFilter<M> {
    ModelFilter(condition: .and([lhs.condition, rhs.condition]))
}

/// Combine two filters with OR.
public func || <M>(lhs: ModelFilter<M>, rhs: ModelFilter<M>) -> ModelFilter<M> {
    ModelFilter(condition: .or([lhs.condition, rhs.condition]))
}
