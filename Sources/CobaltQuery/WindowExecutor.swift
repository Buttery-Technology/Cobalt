import Foundation
import CobaltCore

/// Supported window functions
public enum WindowFunction: Sendable {
    case rowNumber
    case rank
    case denseRank
    case lag(column: String, offset: Int, defaultValue: DBValue?)
    case lead(column: String, offset: Int, defaultValue: DBValue?)
    case sumOver(column: String)
    case avgOver(column: String)
    case countOver
}

/// Window specification: PARTITION BY + ORDER BY
public struct WindowSpec: Sendable {
    public let partitionBy: [String]
    public let orderBy: [OrderBy]

    public init(partitionBy: [String] = [], orderBy: [OrderBy] = []) {
        self.partitionBy = partitionBy
        self.orderBy = orderBy
    }
}

/// Executes window functions over a set of rows.
public struct WindowExecutor: Sendable {

    /// Apply a window function to the given rows and return a new column name → value for each row.
    /// The result array is parallel to the input rows array.
    /// - Parameters:
    ///   - rows: Input rows
    ///   - function: The window function to apply
    ///   - spec: Partition and ordering specification
    ///   - outputColumn: Name of the output column to add
    /// - Returns: New rows with the window function result column added
    public static func executeWindow(
        rows: [Row],
        function: WindowFunction,
        spec: WindowSpec,
        outputColumn: String = "window_value"
    ) -> [Row] {
        guard !rows.isEmpty else { return [] }

        // Partition rows by partitionBy columns, preserving original indices
        let partitions = partitionRows(rows, by: spec.partitionBy)

        // Prepare output: start with original values
        var results = rows.map { $0.values }

        for partition in partitions {
            // Sort indices within partition by orderBy
            let sortedIndices = sortPartition(partition, rows: rows, orderBy: spec.orderBy)

            // Apply the window function
            switch function {
            case .rowNumber:
                for (rank, idx) in sortedIndices.enumerated() {
                    results[idx][outputColumn] = .integer(Int64(rank + 1))
                }

            case .rank:
                applyRank(sortedIndices: sortedIndices, rows: rows, orderBy: spec.orderBy,
                          results: &results, outputColumn: outputColumn, dense: false)

            case .denseRank:
                applyRank(sortedIndices: sortedIndices, rows: rows, orderBy: spec.orderBy,
                          results: &results, outputColumn: outputColumn, dense: true)

            case .lag(let column, let offset, let defaultValue):
                for (pos, idx) in sortedIndices.enumerated() {
                    let sourcePos = pos - offset
                    if sourcePos >= 0 && sourcePos < sortedIndices.count {
                        let sourceIdx = sortedIndices[sourcePos]
                        results[idx][outputColumn] = rows[sourceIdx].values[column] ?? defaultValue ?? .null
                    } else {
                        results[idx][outputColumn] = defaultValue ?? .null
                    }
                }

            case .lead(let column, let offset, let defaultValue):
                for (pos, idx) in sortedIndices.enumerated() {
                    let sourcePos = pos + offset
                    if sourcePos >= 0 && sourcePos < sortedIndices.count {
                        let sourceIdx = sortedIndices[sourcePos]
                        results[idx][outputColumn] = rows[sourceIdx].values[column] ?? defaultValue ?? .null
                    } else {
                        results[idx][outputColumn] = defaultValue ?? .null
                    }
                }

            case .sumOver(let column):
                var sum: Double = 0
                for idx in sortedIndices {
                    sum += doubleValue(rows[idx].values[column])
                }
                let dbSum = DBValue.double(sum)
                for idx in sortedIndices {
                    results[idx][outputColumn] = dbSum
                }

            case .avgOver(let column):
                var sum: Double = 0
                let count = sortedIndices.count
                for idx in sortedIndices {
                    sum += doubleValue(rows[idx].values[column])
                }
                let dbAvg = count > 0 ? DBValue.double(sum / Double(count)) : .null
                for idx in sortedIndices {
                    results[idx][outputColumn] = dbAvg
                }

            case .countOver:
                let dbCount = DBValue.integer(Int64(sortedIndices.count))
                for idx in sortedIndices {
                    results[idx][outputColumn] = dbCount
                }
            }
        }

        return results.map { Row(values: $0) }
    }

    // MARK: - Private helpers

    /// Partition row indices by the given columns. Returns groups of original indices.
    private static func partitionRows(_ rows: [Row], by columns: [String]) -> [[Int]] {
        if columns.isEmpty {
            // No partitioning — all rows in one partition
            return [Array(0..<rows.count)]
        }

        var groups = [String: [Int]]()
        for (i, row) in rows.enumerated() {
            let key = columns.map { col in
                row.values[col].map { "\($0)" } ?? "NULL"
            }.joined(separator: "|")
            groups[key, default: []].append(i)
        }
        return Array(groups.values)
    }

    /// Sort indices within a partition by the ORDER BY spec
    private static func sortPartition(_ indices: [Int], rows: [Row], orderBy: [OrderBy]) -> [Int] {
        if orderBy.isEmpty { return indices }

        return indices.sorted { a, b in
            for ob in orderBy {
                let va = rows[a].values[ob.column] ?? .null
                let vb = rows[b].values[ob.column] ?? .null
                if va == vb { continue }
                let less = va < vb
                return ob.direction == .ascending ? less : !less
            }
            return false
        }
    }

    /// Apply RANK or DENSE_RANK
    private static func applyRank(sortedIndices: [Int], rows: [Row], orderBy: [OrderBy],
                                  results: inout [[String: DBValue]], outputColumn: String, dense: Bool) {
        guard !sortedIndices.isEmpty else { return }

        var currentRank: Int64 = 1
        var displayRank: Int64 = 1
        results[sortedIndices[0]][outputColumn] = .integer(1)

        for i in 1..<sortedIndices.count {
            let prev = sortedIndices[i - 1]
            let curr = sortedIndices[i]

            let tied = orderBy.allSatisfy { ob in
                (rows[prev].values[ob.column] ?? .null) == (rows[curr].values[ob.column] ?? .null)
            }

            currentRank += 1
            if !tied {
                displayRank = dense ? (displayRank + 1) : currentRank
            }
            results[curr][outputColumn] = .integer(displayRank)
        }
    }

    /// Extract a Double from a DBValue
    private static func doubleValue(_ val: DBValue?) -> Double {
        switch val {
        case .integer(let v): return Double(v)
        case .double(let v): return v
        default: return 0
        }
    }
}
