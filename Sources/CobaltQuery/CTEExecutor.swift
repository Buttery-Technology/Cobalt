import Foundation
import CobaltCore

/// Definition of a Common Table Expression for execution
public struct CTEDefinition: Sendable {
    public let name: String
    public let columns: [String]?
    public let query: String  // SQL text to execute
    public let isRecursive: Bool

    public init(name: String, columns: [String]? = nil, query: String, isRecursive: Bool = false) {
        self.name = name
        self.columns = columns
        self.query = query
        self.isRecursive = isRecursive
    }
}

/// Executes Common Table Expressions by materializing them into in-memory row arrays.
public struct CTEExecutor: Sendable {

    /// Materialized CTE results keyed by CTE name
    public typealias CTEContext = [String: [Row]]

    /// Materialize a non-recursive CTE: execute the query and store results.
    /// The `executeQuery` closure runs a SQL SELECT and returns rows.
    public static func materialize(
        definition: CTEDefinition,
        executeQuery: @Sendable (String) async throws -> [Row]
    ) async throws -> [Row] {
        let rows = try await executeQuery(definition.query)
        return applyColumnAliases(rows: rows, columns: definition.columns)
    }

    /// Materialize a recursive CTE using iterative fixed-point evaluation.
    /// - Parameters:
    ///   - definition: The CTE definition (must have isRecursive = true)
    ///   - baseQuery: SQL for the base (non-recursive) part
    ///   - recursiveQuery: SQL for the recursive part, which references the CTE name
    ///   - executeQuery: Closure that executes SQL and returns rows
    ///   - maxIterations: Safety limit to prevent infinite recursion
    public static func materializeRecursive(
        definition: CTEDefinition,
        baseQuery: String,
        recursiveQuery: String,
        executeQuery: @Sendable (String) async throws -> [Row],
        maxIterations: Int = 1000
    ) async throws -> [Row] {
        // Execute base case
        var allRows = try await executeQuery(baseQuery)
        var workingSet = allRows
        var iteration = 0

        // Iterate until no new rows or max iterations reached
        while !workingSet.isEmpty && iteration < maxIterations {
            let newRows = try await executeQuery(recursiveQuery)
            if newRows.isEmpty { break }

            // Deduplicate: only keep rows not already in allRows
            let existingSet = Set(allRows)
            let uniqueNew = newRows.filter { !existingSet.contains($0) }
            if uniqueNew.isEmpty { break }

            allRows.append(contentsOf: uniqueNew)
            workingSet = uniqueNew
            iteration += 1
        }

        return applyColumnAliases(rows: allRows, columns: definition.columns)
    }

    /// If the CTE defines column aliases, rename row keys accordingly.
    private static func applyColumnAliases(rows: [Row], columns: [String]?) -> [Row] {
        guard let columns = columns, !columns.isEmpty else { return rows }
        guard let firstRow = rows.first else { return rows }

        let existingKeys = firstRow.values.keys.sorted()
        guard existingKeys.count >= columns.count else { return rows }

        return rows.map { row in
            var newValues = [String: DBValue]()
            let sortedKeys = row.values.keys.sorted()
            for (i, alias) in columns.enumerated() {
                if i < sortedKeys.count {
                    newValues[alias] = row.values[sortedKeys[i]]
                }
            }
            // Keep any remaining columns beyond the alias list
            for i in columns.count..<sortedKeys.count {
                newValues[sortedKeys[i]] = row.values[sortedKeys[i]]
            }
            return Row(values: newValues)
        }
    }
}
