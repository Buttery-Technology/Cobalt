import Foundation
import CobaltCore

/// Produces a simple textual query plan description.
public struct ExplainExecutor: Sendable {

    /// Generate a query plan description as rows with a "plan" column.
    /// - Parameters:
    ///   - table: The primary table being queried
    ///   - condition: The WHERE condition, if any
    ///   - hasIndex: Whether the query can use an index
    ///   - joinCount: Number of JOINs in the query
    /// - Returns: Rows describing the plan steps
    public static func explain(
        table: String,
        condition: WhereCondition?,
        hasIndex: Bool,
        joinCount: Int
    ) -> [Row] {
        var steps: [String] = []

        // Scan strategy
        if hasIndex, let cond = condition {
            let col = indexableColumn(cond)
            if let col = col {
                steps.append("INDEX SCAN on \(table) using index on \(col)")
            } else {
                steps.append("FULL TABLE SCAN on \(table)")
            }
        } else if condition != nil {
            steps.append("FULL TABLE SCAN on \(table) with filter")
        } else {
            steps.append("FULL TABLE SCAN on \(table)")
        }

        // Filter
        if let cond = condition {
            steps.append("FILTER: \(describeCondition(cond))")
        }

        // Joins
        if joinCount > 0 {
            steps.append("NESTED LOOP JOIN (\(joinCount) join\(joinCount > 1 ? "s" : ""))")
        }

        // Result
        steps.append("RESULT")

        return steps.map { Row(values: ["plan": .string($0)]) }
    }

    // MARK: - Private helpers

    /// Extract the indexable column from a simple condition
    private static func indexableColumn(_ cond: WhereCondition) -> String? {
        switch cond {
        case .equals(let col, _), .lessThan(let col, _), .greaterThan(let col, _),
             .lessThanOrEqual(let col, _), .greaterThanOrEqual(let col, _):
            return col
        case .and(let subs):
            for sub in subs {
                if let col = indexableColumn(sub) { return col }
            }
            return nil
        default:
            return nil
        }
    }

    /// Produce a human-readable description of a WHERE condition
    private static func describeCondition(_ cond: WhereCondition) -> String {
        switch cond {
        case .equals(let col, let val): return "\(col) = \(val)"
        case .notEquals(let col, let val): return "\(col) != \(val)"
        case .lessThan(let col, let val): return "\(col) < \(val)"
        case .greaterThan(let col, let val): return "\(col) > \(val)"
        case .lessThanOrEqual(let col, let val): return "\(col) <= \(val)"
        case .greaterThanOrEqual(let col, let val): return "\(col) >= \(val)"
        case .in(let col, let vals): return "\(col) IN (\(vals.map { "\($0)" }.joined(separator: ", ")))"
        case .between(let col, let min, let max): return "\(col) BETWEEN \(min) AND \(max)"
        case .like(let col, let pat): return "\(col) LIKE '\(pat)'"
        case .isNull(let col): return "\(col) IS NULL"
        case .isNotNull(let col): return "\(col) IS NOT NULL"
        case .and(let subs): return subs.map { describeCondition($0) }.joined(separator: " AND ")
        case .or(let subs): return subs.map { describeCondition($0) }.joined(separator: " OR ")
        }
    }
}
