import Foundation
/// Global monotonic access counter shared across all stripes for fair eviction scoring.
private let globalAccessCounter = PantryLock<UInt64>(0)

/// Per-stripe mutable state with frequency+recency tracking for ARC-style eviction
private struct StripeState {
    var pageCache: [Int: DatabasePage] = [:]
    var dirtyPages = Set<Int>()
    var accessOrder: [Int: UInt64] = [:]    // recency: global access counter value at last access
    var accessFrequency: [Int: UInt32] = [:]  // frequency: total hit count per page
    var hitCount: Int = 0
    var missCount: Int = 0
    var evictionCount: Int = 0
    var flushCount: Int = 0

    /// Compute eviction score: lower = more evictable. Combines recency (70%) and frequency (30%).
    /// Uses global access counter for fair cross-stripe comparison.
    func evictionScore(pageID: Int, globalMax: UInt64) -> Double {
        let recency = Double(accessOrder[pageID] ?? 0)
        let frequency = Double(accessFrequency[pageID] ?? 1)
        let normalizedRecency = globalMax > 0 ? recency / Double(globalMax) : 0
        let logFreq = log2(frequency + 1) / log2(11.0)
        return normalizedRecency * 0.7 + min(1.0, logFreq) * 0.3
    }
}

/// Configuration for the background page writer
public struct BackgroundWriterConfig: Sendable {
    /// Interval between flush cycles in milliseconds
    public let intervalMilliseconds: Int
    /// Maximum number of dirty pages to flush per cycle
    public let maxPagesPerCycle: Int
    /// Dirty page ratio threshold to trigger flush (0.0–1.0)
    public let dirtyThreshold: Double

    public init(intervalMilliseconds: Int = 100, maxPagesPerCycle: Int = 16, dirtyThreshold: Double = 0.25) {
        self.intervalMilliseconds = intervalMilliseconds
        self.maxPagesPerCycle = maxPagesPerCycle
        self.dirtyThreshold = dirtyThreshold
    }
}

/// Manages an in-memory cache of database pages to minimize disk I/O.
/// Uses stripe-based partitioning (pageID % stripeCount) to reduce mutex contention.
/// Includes a background writer that asynchronously flushes dirty pages.
public final class BufferPoolManager: Sendable {
    private let capacity: Int
    private let storageManager: StorageManager
    private let stripes: [PantryLock<StripeState>]
    private let stripeCount: Int
    private let bgWriterConfig: BackgroundWriterConfig
    private let bgWriterTask: PantryLock<Task<Void, Never>?>

    public init(capacity: Int, storageManager: StorageManager, stripeCount: Int = 8, bgWriterConfig: BackgroundWriterConfig = BackgroundWriterConfig()) {
        self.capacity = capacity
        self.storageManager = storageManager
        self.stripeCount = max(1, stripeCount)
        self.stripes = (0..<max(1, stripeCount)).map { _ in PantryLock(StripeState()) }
        self.bgWriterConfig = bgWriterConfig
        self.bgWriterTask = PantryLock(nil)
    }

    /// Start the background dirty page writer
    public func startBackgroundWriter() {
        let task = Task { [weak self] in
            while !Task.isCancelled {
                try? await Task.sleep(for: .milliseconds(self?.bgWriterConfig.intervalMilliseconds ?? 100))
                guard !Task.isCancelled, let self = self else { break }
                await self.backgroundFlushCycle()
            }
        }
        bgWriterTask.withLock { $0 = task }
    }

    /// Stop the background writer
    public func stopBackgroundWriter() {
        bgWriterTask.withLock { task in
            task?.cancel()
            task = nil
        }
    }

    /// Single background flush cycle: flush oldest dirty pages from stripes that exceed threshold
    /// Pages are sorted by pageID for sequential disk I/O.
    private func backgroundFlushCycle() async {
        let maxPerCycle = bgWriterConfig.maxPagesPerCycle

        // Collect candidate pages from all stripes
        var candidates: [(Int, DatabasePage, PantryLock<StripeState>)] = []
        for s in stripes {
            let pages: [(Int, DatabasePage)] = s.withLock { st in
                let dirtyRatio = st.pageCache.isEmpty ? 0.0 : Double(st.dirtyPages.count) / Double(st.pageCache.count)
                guard dirtyRatio >= bgWriterConfig.dirtyThreshold else { return [] }

                let sorted = st.dirtyPages
                    .compactMap { pageID -> (Int, UInt64, DatabasePage)? in
                        guard let page = st.pageCache[pageID] else { return nil }
                        return (pageID, st.accessOrder[pageID] ?? 0, page)
                    }
                    .sorted { $0.1 < $1.1 }

                return sorted.map { ($0.0, $0.2) }
            }
            for (pageID, page) in pages {
                candidates.append((pageID, page, s))
            }
        }

        // Sort by pageID for sequential I/O, then take up to maxPerCycle
        candidates.sort { $0.0 < $1.0 }

        var totalFlushed = 0
        for (pageID, var page, stripe) in candidates {
            guard totalFlushed < maxPerCycle else { break }
            do {
                try await storageManager.writePage(&page, alreadySerialized: true)
                stripe.withLock { st in
                    st.dirtyPages.remove(pageID)
                    st.flushCount += 1
                }
                totalFlushed += 1
            } catch {
                // Background flush errors are non-critical
            }
        }
    }

    private func stripe(for pageID: Int) -> PantryLock<StripeState> {
        stripes[pageID % stripeCount]
    }

    private var capacityPerStripe: Int {
        max(1, capacity / stripeCount)
    }

    // MARK: - Core Page Management

    public func getPage(pageID: Int) async throws -> DatabasePage {
        let s = stripe(for: pageID)
        let cached: DatabasePage? = s.withLock { st in
            if let page = st.pageCache[pageID] {
                let counter = globalAccessCounter.withLock { c -> UInt64 in c += 1; return c }
                st.accessOrder[pageID] = counter
                st.accessFrequency[pageID, default: 0] += 1
                st.hitCount += 1
                return page
            }
            st.missCount += 1
            return nil
        }
        if let cached { return cached }

        let page = try await storageManager.readPage(pageID: pageID)
        try await addToCache(page)
        return page
    }

    public func isPageCached(pageID: Int) -> Bool {
        stripe(for: pageID).withLock { $0.pageCache[pageID] != nil }
    }

    public func getCachedPage(pageID: Int) -> DatabasePage? {
        stripe(for: pageID).withLock { st in
            if let page = st.pageCache[pageID] {
                let counter = globalAccessCounter.withLock { c -> UInt64 in c += 1; return c }
                st.accessOrder[pageID] = counter
                st.accessFrequency[pageID, default: 0] += 1
                st.hitCount += 1
                return page
            }
            return nil
        }
    }

    public func updatePage(_ page: DatabasePage) {
        stripe(for: page.pageID).withLock { st in
            st.pageCache[page.pageID] = page
            let counter = globalAccessCounter.withLock { c -> UInt64 in c += 1; return c }
            st.accessOrder[page.pageID] = counter
        }
    }

    public func markDirty(pageID: Int) {
        _ = stripe(for: pageID).withLock { $0.dirtyPages.insert(pageID) }
    }

    public func isDirty(pageID: Int) -> Bool {
        stripe(for: pageID).withLock { $0.dirtyPages.contains(pageID) }
    }

    public func clearDirtyFlag(pageID: Int) {
        _ = stripe(for: pageID).withLock { $0.dirtyPages.remove(pageID) }
    }

    // MARK: - Cache Management

    private func addToCache(_ page: DatabasePage) async throws {
        let idx = page.pageID % stripeCount
        let needsEviction = stripes[idx].withLock { $0.pageCache.count >= capacityPerStripe }
        if needsEviction {
            try await evictFromStripe(index: idx)
        }
        stripes[idx].withLock { st in
            st.pageCache[page.pageID] = page
            let counter = globalAccessCounter.withLock { c -> UInt64 in c += 1; return c }
            st.accessOrder[page.pageID] = counter
            st.accessFrequency[page.pageID] = 1
        }
    }

    /// Evict the lowest-scoring page from a specific stripe using ARC (frequency+recency) scoring.
    /// Prefers evicting clean pages; falls back to dirty pages if needed.
    private func evictFromStripe(index idx: Int) async throws {
        let victim: (Int, DatabasePage?)? = stripes[idx].withLock { st in
            guard !st.pageCache.isEmpty else { return nil }

            // Prefer evicting clean pages first (avoid I/O)
            let cleanPages = st.accessOrder.keys.filter { !st.dirtyPages.contains($0) }
            let candidates = cleanPages.isEmpty ? Array(st.accessOrder.keys) : cleanPages

            let gmax = globalAccessCounter.withLock { $0 }
            guard let bestVictim = candidates.min(by: { st.evictionScore(pageID: $0, globalMax: gmax) < st.evictionScore(pageID: $1, globalMax: gmax) }) else { return nil }

            let dirtyPage: DatabasePage? = st.dirtyPages.contains(bestVictim) ? st.pageCache[bestVictim] : nil
            return (bestVictim, dirtyPage)
        }
        guard let (pageID, dirtyPage) = victim else { return }

        if var page = dirtyPage {
            try await storageManager.writePage(&page, alreadySerialized: true)
        }
        stripes[idx].withLock { st in
            st.dirtyPages.remove(pageID)
            st.pageCache.removeValue(forKey: pageID)
            st.accessOrder.removeValue(forKey: pageID)
            st.accessFrequency.removeValue(forKey: pageID)
            st.evictionCount += 1
        }
    }

    public func evictPages(count: Int = 1) async throws {
        // Evict from the fullest stripes first
        for _ in 0..<count {
            var fullestIdx = 0
            var maxCount = 0
            for (i, s) in stripes.enumerated() {
                let c = s.withLock { $0.pageCache.count }
                if c > maxCount {
                    maxCount = c
                    fullestIdx = i
                }
            }
            if maxCount > 0 {
                try await evictFromStripe(index: fullestIdx)
            }
        }
    }

    /// Evict a specific page from cache (used after rollback to discard stale data)
    public func evictPage(pageID: Int) {
        stripe(for: pageID).withLock { st in
            st.pageCache.removeValue(forKey: pageID)
            st.accessOrder.removeValue(forKey: pageID)
            st.accessFrequency.removeValue(forKey: pageID)
            st.dirtyPages.remove(pageID)
        }
    }

    // MARK: - Flushing

    public func flushAllDirtyPages() async throws {
        // Collect all dirty pages across stripes, sorted by pageID for sequential I/O
        var allDirtyPages: [(Int, DatabasePage, PantryLock<StripeState>)] = []
        for s in stripes {
            let pages: [(Int, DatabasePage)] = s.withLock { st in
                st.dirtyPages.compactMap { pageID in
                    guard let page = st.pageCache[pageID] else {
                        st.dirtyPages.remove(pageID)
                        return nil
                    }
                    return (pageID, page)
                }
            }
            for (pageID, page) in pages {
                allDirtyPages.append((pageID, page, s))
            }
        }

        // Sort by pageID for sequential disk writes
        allDirtyPages.sort { $0.0 < $1.0 }

        for (pageID, var page, stripe) in allDirtyPages {
            try await storageManager.writePage(&page, alreadySerialized: true)
            stripe.withLock { st in
                st.flushCount += 1
                st.dirtyPages.remove(pageID)
            }
        }
        try await storageManager.sync()
    }

    public func flushPage(pageID: Int) async throws {
        let s = stripe(for: pageID)
        let pageToFlush: DatabasePage? = s.withLock { st in
            if st.dirtyPages.contains(pageID), let page = st.pageCache[pageID] {
                return page
            }
            return nil
        }
        if var page = pageToFlush {
            try await storageManager.writePage(&page, alreadySerialized: true)
            s.withLock { st in
                st.dirtyPages.remove(pageID)
                st.flushCount += 1
            }
        }
    }

    // MARK: - Maintenance

    public func performMaintenance() async throws {
        for s in stripes {
            let pagesToFlush: [Int] = s.withLock { st in
                let dirtyPageCount = st.dirtyPages.count
                guard dirtyPageCount > st.pageCache.count / 4 || dirtyPageCount > capacityPerStripe / 4 else { return [] }
                let sorted = st.dirtyPages.sorted { pageID1, pageID2 in
                    let order1 = st.accessOrder[pageID1] ?? 0
                    let order2 = st.accessOrder[pageID2] ?? 0
                    return order1 < order2
                }
                return Array(sorted.prefix(max(1, dirtyPageCount / 4)))
            }

            for pageID in pagesToFlush {
                try await flushPage(pageID: pageID)
            }

            // Evict clean pages if stripe is >90% full
            s.withLock { st in
                let cap = capacityPerStripe
                if st.pageCache.count > Int(Double(cap) * 0.9) {
                    let targetCount = Int(Double(cap) * 0.8)
                    let evictionCount = st.pageCache.count - targetCount
                    if evictionCount > 0 {
                        let gmax = globalAccessCounter.withLock { $0 }
                        let cleanPages = st.accessOrder.keys
                            .filter { !st.dirtyPages.contains($0) }
                            .sorted { st.evictionScore(pageID: $0, globalMax: gmax) < st.evictionScore(pageID: $1, globalMax: gmax) }
                            .prefix(evictionCount)
                        for pageID in cleanPages {
                            st.pageCache.removeValue(forKey: pageID)
                            st.accessOrder.removeValue(forKey: pageID)
                            st.accessFrequency.removeValue(forKey: pageID)
                            st.evictionCount += 1
                        }
                    }
                }
            }
        }
    }

    public func cachePage(_ page: DatabasePage) async {
        let s = stripe(for: page.pageID)
        let needsEviction = s.withLock { st in
            st.pageCache[page.pageID] = page
            let counter = globalAccessCounter.withLock { c -> UInt64 in c += 1; return c }
            st.accessOrder[page.pageID] = counter
            return st.pageCache.count > capacityPerStripe
        }
        if needsEviction {
            try? await evictFromStripe(index: page.pageID % stripeCount)
        }
    }

    // MARK: - Prefetching

    /// Prefetch pages into the buffer pool asynchronously.
    /// Non-critical — failures are silently ignored.
    public func prefetchPages(_ pageIDs: [Int]) async {
        for pageID in pageIDs {
            let alreadyCached = stripe(for: pageID).withLock { $0.pageCache[pageID] != nil }
            if alreadyCached { continue }
            do {
                let page = try await storageManager.readPage(pageID: pageID)
                try await addToCache(page)
                stripe(for: pageID).withLock { st in
                    st.hitCount += 0 // Don't count as miss or hit — it's a prefetch
                }
            } catch {
                // Prefetch failures are non-critical
            }
        }
    }

    // MARK: - Statistics

    public func getStats() -> BufferPoolStats {
        var stats = BufferPoolStats()
        for s in stripes {
            s.withLock { st in
                stats.hitCount += st.hitCount
                stats.missCount += st.missCount
                stats.evictionCount += st.evictionCount
                stats.flushCount += st.flushCount
            }
        }
        return stats
    }

    public func resetStats() {
        for s in stripes {
            s.withLock { st in
                st.hitCount = 0
                st.missCount = 0
                st.evictionCount = 0
                st.flushCount = 0
            }
        }
    }

    public func getCacheOccupancy() -> Double {
        var total = 0
        for s in stripes {
            total += s.withLock { $0.pageCache.count }
        }
        return Double(total) / Double(capacity)
    }
}

/// Statistics for buffer pool performance monitoring
public struct BufferPoolStats: Sendable {
    public var hitCount: Int = 0
    public var missCount: Int = 0
    public var evictionCount: Int = 0
    public var flushCount: Int = 0
    public var prefetchCount: Int = 0
    public var prefetchErrorCount: Int = 0

    public var hitRate: Double {
        let total = hitCount + missCount
        return total > 0 ? (Double(hitCount) / Double(total)) * 100.0 : 0.0
    }

    public init() {}
}
