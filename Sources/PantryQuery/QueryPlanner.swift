import Foundation
import PantryCore
import PantryIndex

/// Represents a physical access strategy for a single-table query
enum AccessPlan: Sendable {
    /// Full table scan: read all pages, filter in memory
    case tableScan(pageCount: Int)
    /// Index scan: look up via B-tree, then batch-fetch matching records
    case indexScan(column: String, estimatedRows: Int)
    /// Index-only scan: all needed columns are in the index, no heap fetch needed
    case indexOnlyScan(column: String, estimatedRows: Int)
}

/// Represents a join strategy
enum JoinStrategy: Sendable {
    /// Build hash table on smaller side, probe with larger side
    case hashJoin(buildSide: JoinSide)
    /// Nested loop: for each left row, scan right side
    case nestedLoop
}

enum JoinSide: Sendable {
    case left, right
}

/// Cost estimate in abstract I/O units (1 unit ≈ 1 page read)
struct QueryCost: Comparable, Sendable {
    let ioPages: Double  // estimated page reads
    let cpuRows: Double  // estimated rows processed in memory

    /// Combined cost: I/O dominates (10x weight vs CPU)
    var total: Double { ioPages * 10.0 + cpuRows * 0.01 }

    static func < (lhs: QueryCost, rhs: QueryCost) -> Bool {
        lhs.total < rhs.total
    }
}

/// Estimates query costs and selects optimal execution plans
struct QueryPlanner: Sendable {
    private let storageEngine: StorageEngine
    private let indexManager: IndexManager
    let registry: TableRegistry

    init(storageEngine: StorageEngine, indexManager: IndexManager, registry: TableRegistry) {
        self.storageEngine = storageEngine
        self.indexManager = indexManager
        self.registry = registry
    }

    // MARK: - Single-Table Plan Selection

    /// Choose the best access plan for a single-table SELECT with an optional WHERE condition.
    /// Pass `requestedColumns` to enable index-only scans when all columns are covered.
    /// Pass `coveredColumnsForIndex` from IndexManager to check covering index eligibility.
    nonisolated func chooseAccessPlan(table: String, condition: WhereCondition?, pageCount: Int, requestedColumns: [String]? = nil, indexCoverage: [String: Set<String>]? = nil) -> AccessPlan {
        guard let condition = condition else {
            return .tableScan(pageCount: pageCount)
        }

        let tableInfo = registry.getTableInfo(name: table)
        let totalRows = tableInfo?.recordCount ?? (pageCount * 50) // ~50 rows per 8KB page estimate

        // Extract indexable predicates
        let indexable = extractIndexableColumns(condition)

        // Evaluate each candidate index
        var bestPlan: AccessPlan = .tableScan(pageCount: pageCount)
        var bestCost = costOfTableScan(pageCount: pageCount, totalRows: totalRows)

        for (column, predType) in indexable {
            guard let stats = storageEngine.getColumnStats(table, column: column),
                  stats.isIndexed else { continue }

            let estimatedRows = estimateMatchingRows(
                totalRows: totalRows,
                stats: stats,
                predicateType: predType
            )

            // Check if this is a covering index (all requested columns are in the index)
            let isCovering: Bool
            if let requested = requestedColumns, let coverage = indexCoverage?[column] {
                isCovering = Set(requested).isSubset(of: coverage)
            } else {
                isCovering = false
            }

            let indexCost: QueryCost
            if isCovering {
                // Index-only scan: no heap pages needed
                let treeDepth = max(1.0, log(Double(max(1, totalRows))) / log(64.0))
                indexCost = QueryCost(ioPages: treeDepth, cpuRows: Double(estimatedRows))
            } else {
                indexCost = costOfIndexScan(
                    estimatedRows: estimatedRows,
                    totalRows: totalRows,
                    pageCount: pageCount
                )
            }

            if indexCost < bestCost {
                bestCost = indexCost
                if isCovering {
                    bestPlan = .indexOnlyScan(column: column, estimatedRows: estimatedRows)
                } else {
                    bestPlan = .indexScan(column: column, estimatedRows: estimatedRows)
                }
            }
        }

        return bestPlan
    }

    // MARK: - Join Order Optimization

    /// Given multiple joins, determine the optimal join order by estimating result sizes.
    /// Uses a greedy approach: always join the smallest intermediate result next.
    nonisolated func optimizeJoinOrder(primaryTable: String, joins: [JoinClause], primaryRowCount: Int) -> [JoinClause] {
        guard joins.count > 1 else { return joins }

        var remaining = joins
        var ordered = [JoinClause]()
        var currentRows = primaryRowCount

        while !remaining.isEmpty {
            var bestIdx = 0
            var bestCost = Double.infinity

            for (i, join) in remaining.enumerated() {
                let rightInfo = registry.getTableInfo(name: join.table)
                let rightRows = rightInfo?.recordCount ?? 1000

                let resultRows: Double
                switch join.type {
                case .inner:
                    let selectivity = estimateJoinSelectivity(join: join, leftRows: currentRows, rightRows: rightRows)
                    resultRows = Double(currentRows) * Double(rightRows) * selectivity
                case .left:
                    resultRows = Double(currentRows)
                case .right:
                    resultRows = Double(rightRows)
                case .cross:
                    resultRows = Double(currentRows) * Double(rightRows)
                }

                let cost = resultRows + Double(rightRows)
                if cost < bestCost {
                    bestCost = cost
                    bestIdx = i
                }
            }

            let chosen = remaining.remove(at: bestIdx)
            let rightInfo = registry.getTableInfo(name: chosen.table)
            let rightRows = rightInfo?.recordCount ?? 1000
            let selectivity = estimateJoinSelectivity(join: chosen, leftRows: currentRows, rightRows: rightRows)

            switch chosen.type {
            case .inner:
                currentRows = max(1, Int(Double(currentRows) * Double(rightRows) * selectivity))
            case .left:
                break
            case .right:
                currentRows = rightRows
            case .cross:
                currentRows = currentRows * rightRows
            }

            ordered.append(chosen)
        }

        return ordered
    }

    /// Choose join strategy: hash join (preferred) or nested loop
    nonisolated func chooseJoinStrategy(leftRows: Int, rightRows: Int, join: JoinClause) -> JoinStrategy {
        if join.type == .cross {
            return .nestedLoop
        }
        // Hash join: build on smaller side
        return .hashJoin(buildSide: rightRows <= leftRows ? .right : .left)
    }

    // MARK: - Cost Estimation

    private nonisolated func costOfTableScan(pageCount: Int, totalRows: Int) -> QueryCost {
        QueryCost(ioPages: Double(pageCount), cpuRows: Double(totalRows))
    }

    private nonisolated func costOfIndexScan(estimatedRows: Int, totalRows: Int, pageCount: Int) -> QueryCost {
        // B-tree traversal: ~log64(totalRows) page reads for the tree
        let treeDepth = max(1.0, log(Double(max(1, totalRows))) / log(64.0))
        // Plus one page read per matching record (random I/O)
        let heapPages = Double(estimatedRows) * Double(pageCount) / Double(max(1, totalRows))
        return QueryCost(ioPages: treeDepth + heapPages, cpuRows: Double(estimatedRows))
    }

    private nonisolated func estimateMatchingRows(totalRows: Int, stats: ColumnStats, predicateType: PredicateType) -> Int {
        switch predicateType {
        case .equality:
            return max(1, Int(Double(totalRows) * stats.equalitySelectivity))
        case .range:
            // Assume ~30% selectivity for range queries
            return max(1, Int(Double(totalRows) * 0.3))
        case .inList(let count):
            return max(1, min(totalRows, Int(Double(totalRows) * stats.equalitySelectivity * Double(count))))
        case .like:
            // Assume ~10% for LIKE with leading wildcard, ~1% for prefix LIKE
            return max(1, Int(Double(totalRows) * 0.1))
        case .isNull:
            // Assume ~5% null rate
            return max(1, Int(Double(totalRows) * 0.05))
        }
    }

    private nonisolated func estimateJoinSelectivity(join: JoinClause, leftRows: Int, rightRows: Int) -> Double {
        // Use column stats if available; otherwise assume 1/max(left, right)
        if let stats = storageEngine.getColumnStats(join.table, column: join.rightColumn) {
            return stats.equalitySelectivity
        }
        return 1.0 / Double(max(leftRows, rightRows, 1))
    }

    // MARK: - Predicate Analysis

    private nonisolated func extractIndexableColumns(_ condition: WhereCondition) -> [(String, PredicateType)] {
        switch condition {
        case .equals(let col, _):
            return [(col, .equality)]
        case .lessThan(let col, _), .greaterThan(let col, _),
             .lessThanOrEqual(let col, _), .greaterThanOrEqual(let col, _):
            return [(col, .range)]
        case .between(let col, _, _):
            return [(col, .range)]
        case .in(let col, let values):
            return [(col, .inList(count: values.count))]
        case .like(let col, _):
            return [(col, .like)]
        case .isNull(let col):
            return [(col, .isNull)]
        case .and(let subs):
            return subs.flatMap { extractIndexableColumns($0) }
        default:
            return []
        }
    }
}

enum PredicateType: Sendable {
    case equality
    case range
    case inList(count: Int)
    case like
    case isNull
}
