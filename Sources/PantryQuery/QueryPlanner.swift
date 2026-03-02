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

/// Cost model weights — configurable for different storage media (SSD vs HDD)
public struct CostModelWeights: Sendable, Equatable {
    /// I/O page read weight (higher = prefer index scans). Default 10 for HDD, use 2–3 for SSD.
    public let ioWeight: Double
    /// CPU row processing weight
    public let cpuWeight: Double

    public init(ioWeight: Double, cpuWeight: Double) {
        self.ioWeight = ioWeight
        self.cpuWeight = cpuWeight
    }

    public static let `default` = CostModelWeights(ioWeight: 10.0, cpuWeight: 0.01)
    public static let ssd = CostModelWeights(ioWeight: 2.0, cpuWeight: 0.01)
}

/// Cost estimate in abstract I/O units (1 unit ≈ 1 page read)
struct QueryCost: Comparable, Sendable {
    let ioPages: Double  // estimated page reads
    let cpuRows: Double  // estimated rows processed in memory
    let weights: CostModelWeights

    init(ioPages: Double, cpuRows: Double, weights: CostModelWeights = .default) {
        self.ioPages = ioPages
        self.cpuRows = cpuRows
        self.weights = weights
    }

    var total: Double { ioPages * weights.ioWeight + cpuRows * weights.cpuWeight }

    static func < (lhs: QueryCost, rhs: QueryCost) -> Bool {
        lhs.total < rhs.total
    }
}

/// Key for the query plan cache
private struct PlanCacheKey: Hashable, Sendable {
    let table: String
    let conditionSignature: String
    let pageCount: Int
    let requestedColumns: [String]?
}

/// Cached plan with a generation counter for invalidation and LRU tracking
private struct CachedPlan: Sendable {
    let plan: AccessPlan
    let generation: UInt64
    var lastAccess: UInt64  // monotonic counter for LRU eviction
}

/// Estimates query costs and selects optimal execution plans
struct QueryPlanner: Sendable {
    private let storageEngine: StorageEngine
    private let indexManager: IndexManager
    let registry: TableRegistry

    /// Plan cache: stores computed access plans for repeated queries
    private let planCache: PantryLock<[PlanCacheKey: CachedPlan]>
    /// Generation counter: incremented on index/schema changes to invalidate cache
    private let cacheGeneration: PantryLock<UInt64>
    /// Monotonic access counter for LRU eviction
    private let accessCounter: PantryLock<UInt64>
    /// Maximum cache entries
    private let maxCacheSize = 256
    /// Cost model weights (configurable for SSD vs HDD)
    let costWeights: CostModelWeights

    init(storageEngine: StorageEngine, indexManager: IndexManager, registry: TableRegistry, costWeights: CostModelWeights = .default) {
        self.storageEngine = storageEngine
        self.indexManager = indexManager
        self.registry = registry
        self.planCache = PantryLock([:])
        self.cacheGeneration = PantryLock(0)
        self.accessCounter = PantryLock(0)
        self.costWeights = costWeights
    }

    /// Invalidate the plan cache (call after index create/drop or schema change)
    func invalidateCache() {
        cacheGeneration.withLock { $0 += 1 }
        planCache.withLock { $0.removeAll() }
    }

    /// Invalidate cached plans for a specific table only
    func invalidateCache(forTable table: String) {
        cacheGeneration.withLock { $0 += 1 }
        planCache.withLock { cache in
            cache = cache.filter { $0.key.table != table }
        }
    }

    // MARK: - Single-Table Plan Selection

    /// Choose the best access plan for a single-table SELECT with an optional WHERE condition.
    /// Pass `requestedColumns` to enable index-only scans when all columns are covered.
    /// Pass `coveredColumnsForIndex` from IndexManager to check covering index eligibility.
    nonisolated func chooseAccessPlan(table: String, condition: WhereCondition?, pageCount: Int, requestedColumns: [String]? = nil, indexCoverage: [String: Set<String>]? = nil) -> AccessPlan {
        guard let condition = condition else {
            return .tableScan(pageCount: pageCount)
        }

        // Check plan cache
        let gen = cacheGeneration.withLock { $0 }
        let cacheKey = PlanCacheKey(table: table, conditionSignature: conditionSignature(condition), pageCount: pageCount, requestedColumns: requestedColumns)
        if let cached = planCache.withLock({ cache -> CachedPlan? in
            guard var entry = cache[cacheKey], entry.generation == gen else { return nil }
            entry.lastAccess = self.accessCounter.withLock { $0 += 1; return $0 }
            cache[cacheKey] = entry
            return entry
        }) {
            return cached.plan
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
                indexCost = QueryCost(ioPages: treeDepth, cpuRows: Double(estimatedRows), weights: costWeights)
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

        // Store in cache with LRU eviction
        let ac = accessCounter.withLock { $0 += 1; return $0 }
        planCache.withLock { cache in
            if cache.count >= maxCacheSize {
                // O(n) LRU eviction: single-pass min-k selection
                let evictCount = maxCacheSize / 4
                var evictKeys = [PlanCacheKey]()
                evictKeys.reserveCapacity(evictCount)
                var evictAccesses = [UInt64]()
                evictAccesses.reserveCapacity(evictCount)
                var maxInSet: UInt64 = 0
                for (key, value) in cache {
                    if evictKeys.count < evictCount {
                        evictKeys.append(key)
                        evictAccesses.append(value.lastAccess)
                        if value.lastAccess > maxInSet { maxInSet = value.lastAccess }
                    } else if value.lastAccess < maxInSet {
                        if let maxIdx = evictAccesses.firstIndex(of: maxInSet) {
                            evictKeys[maxIdx] = key
                            evictAccesses[maxIdx] = value.lastAccess
                            maxInSet = evictAccesses.max() ?? 0
                        }
                    }
                }
                for key in evictKeys { cache.removeValue(forKey: key) }
            }
            cache[cacheKey] = CachedPlan(plan: bestPlan, generation: gen, lastAccess: ac)
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
        QueryCost(ioPages: Double(pageCount), cpuRows: Double(totalRows), weights: costWeights)
    }

    private nonisolated func costOfIndexScan(estimatedRows: Int, totalRows: Int, pageCount: Int) -> QueryCost {
        // B-tree traversal: ~log64(totalRows) page reads for the tree
        let treeDepth = max(1.0, log(Double(max(1, totalRows))) / log(64.0))
        // Plus one page read per matching record (random I/O)
        let heapPages = Double(estimatedRows) * Double(pageCount) / Double(max(1, totalRows))
        return QueryCost(ioPages: treeDepth + heapPages, cpuRows: Double(estimatedRows), weights: costWeights)
    }

    private nonisolated func estimateMatchingRows(totalRows: Int, stats: ColumnStats, predicateType: PredicateType) -> Int {
        switch predicateType {
        case .equality:
            return max(1, Int(Double(totalRows) * stats.equalitySelectivity))
        case .range:
            // Use histogram-based range selectivity if available
            let selectivity = stats.histogramBoundaries.isEmpty ? 0.3 : stats.rangeSelectivity(low: stats.minValue, high: stats.maxValue) * 0.5
            return max(1, Int(Double(totalRows) * selectivity))
        case .inList(let count):
            return max(1, min(totalRows, Int(Double(totalRows) * stats.equalitySelectivity * Double(count))))
        case .like:
            return max(1, Int(Double(totalRows) * 0.1))
        case .isNull:
            // Use actual null rate from statistics
            return max(1, Int(Double(totalRows) * stats.nullSelectivity))
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

// MARK: - Condition Signature for Plan Caching

/// Generates a structural signature of a WhereCondition for cache keying.
/// Two conditions with the same shape but different literal values produce the same signature.
private func conditionSignature(_ condition: WhereCondition) -> String {
    switch condition {
    case .equals(let col, _): return "eq(\(col))"
    case .notEquals(let col, _): return "ne(\(col))"
    case .lessThan(let col, _): return "lt(\(col))"
    case .greaterThan(let col, _): return "gt(\(col))"
    case .lessThanOrEqual(let col, _): return "le(\(col))"
    case .greaterThanOrEqual(let col, _): return "ge(\(col))"
    case .in(let col, let vals): return "in(\(col),\(vals.count))"
    case .between(let col, _, _): return "bw(\(col))"
    case .like(let col, _): return "lk(\(col))"
    case .isNull(let col): return "nu(\(col))"
    case .isNotNull(let col): return "nn(\(col))"
    case .and(let subs): return "and[\(subs.map { conditionSignature($0) }.joined(separator: ","))]"
    case .or(let subs): return "or[\(subs.map { conditionSignature($0) }.joined(separator: ","))]"
    }
}
