import Foundation
import Synchronization

/// Lock-free monotonic access counter shared across all stripes.
private let globalAccessCounter = Atomic<UInt64>(0)

/// Per-stripe mutable state with clock sweep eviction.
/// Each page has a "referenced" bit. On eviction, the clock hand sweeps:
/// if referenced, clear the bit and advance; if unreferenced, evict.
private struct StripeState {
    var pageCache: [Int: DatabasePage] = [:]
    var dirtyPages = Set<Int>()
    var referenced = Set<Int>()       // clock sweep: recently-accessed pages
    var pageOrder: [Int] = []         // insertion-order list for clock sweep
    var clockHand: Int = 0            // current position in pageOrder
    var hitCount: Int = 0
    var missCount: Int = 0
    var evictionCount: Int = 0
    var flushCount: Int = 0
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

        // Collect dirty page IDs only (no page data copies)
        var dirtyPageIDs: [(Int, PantryLock<StripeState>)] = []
        for s in stripes {
            let ids: [Int] = s.withLock { st in
                let dirtyRatio = st.pageCache.isEmpty ? 0.0 : Double(st.dirtyPages.count) / Double(st.pageCache.count)
                guard dirtyRatio >= bgWriterConfig.dirtyThreshold else { return [] }
                return Array(st.dirtyPages)
            }
            for id in ids {
                dirtyPageIDs.append((id, s))
            }
        }

        // Sort by pageID for sequential I/O
        dirtyPageIDs.sort { $0.0 < $1.0 }

        var totalFlushed = 0
        for (pageID, stripe) in dirtyPageIDs {
            guard totalFlushed < maxPerCycle else { break }
            // Look up page at write time
            let pageToWrite: DatabasePage? = stripe.withLock { st in
                st.dirtyPages.contains(pageID) ? st.pageCache[pageID] : nil
            }
            guard var page = pageToWrite else { continue }
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
                st.referenced.insert(pageID)
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
                st.referenced.insert(pageID)
                st.hitCount += 1
                return page
            }
            return nil
        }
    }

    public func updatePage(_ page: DatabasePage) {
        stripe(for: page.pageID).withLock { st in
            st.pageCache[page.pageID] = page
            st.referenced.insert(page.pageID)
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
            if st.pageCache[page.pageID] == nil {
                st.pageOrder.append(page.pageID)
            }
            st.pageCache[page.pageID] = page
            st.referenced.insert(page.pageID)
        }
    }

    /// Clock sweep eviction: O(1) amortized. Sweeps from clockHand position,
    /// clearing referenced bits until an unreferenced page is found.
    /// Prefers clean pages to avoid I/O.
    private func evictFromStripe(index idx: Int) async throws {
        let victim: (Int, DatabasePage?)? = stripes[idx].withLock { st in
            guard !st.pageOrder.isEmpty else { return nil }
            let n = st.pageOrder.count
            // Up to 2 full sweeps: first pass skips dirty, second accepts dirty
            for _ in 0..<(2 * n) {
                if st.clockHand >= n { st.clockHand = 0 }
                let pid = st.pageOrder[st.clockHand]
                if st.referenced.contains(pid) {
                    st.referenced.remove(pid)
                    st.clockHand += 1
                    continue
                }
                // Prefer clean pages on first sweep
                if st.dirtyPages.contains(pid) {
                    // Give dirty pages a second chance on first pass
                    st.clockHand += 1
                    continue
                }
                // Found a clean, unreferenced victim
                let dirtyPage: DatabasePage? = nil
                return (pid, dirtyPage)
            }
            // All pages referenced or dirty — evict first unreferenced (even if dirty)
            for _ in 0..<n {
                if st.clockHand >= n { st.clockHand = 0 }
                let pid = st.pageOrder[st.clockHand]
                if st.referenced.contains(pid) {
                    st.referenced.remove(pid)
                    st.clockHand += 1
                    continue
                }
                let dirtyPage: DatabasePage? = st.dirtyPages.contains(pid) ? st.pageCache[pid] : nil
                return (pid, dirtyPage)
            }
            // Everything referenced — force evict at clock hand
            let pid = st.pageOrder[st.clockHand % n]
            let dirtyPage: DatabasePage? = st.dirtyPages.contains(pid) ? st.pageCache[pid] : nil
            return (pid, dirtyPage)
        }
        guard let (pageID, dirtyPage) = victim else { return }

        if var page = dirtyPage {
            try await storageManager.writePage(&page, alreadySerialized: true)
        }
        stripes[idx].withLock { st in
            st.dirtyPages.remove(pageID)
            st.pageCache.removeValue(forKey: pageID)
            st.referenced.remove(pageID)
            if let orderIdx = st.pageOrder.firstIndex(of: pageID) {
                st.pageOrder.remove(at: orderIdx)
                if st.clockHand > orderIdx { st.clockHand -= 1 }
            }
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
            st.referenced.remove(pageID)
            st.dirtyPages.remove(pageID)
            if let orderIdx = st.pageOrder.firstIndex(of: pageID) {
                st.pageOrder.remove(at: orderIdx)
                if st.clockHand > orderIdx { st.clockHand -= 1 }
            }
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
                // Flush unreferenced dirty pages first (coldest)
                let unreferenced = st.dirtyPages.filter { !st.referenced.contains($0) }
                let candidates = unreferenced.isEmpty ? Array(st.dirtyPages) : Array(unreferenced)
                return Array(candidates.prefix(max(1, dirtyPageCount / 4)))
            }

            for pageID in pagesToFlush {
                try await flushPage(pageID: pageID)
            }

            // Evict clean pages if stripe is >90% full using clock sweep
            let evictCount: Int = s.withLock { st in
                let cap = capacityPerStripe
                if st.pageCache.count > Int(Double(cap) * 0.9) {
                    return st.pageCache.count - Int(Double(cap) * 0.8)
                }
                return 0
            }
            if evictCount > 0 {
                for _ in 0..<evictCount {
                    try? await evictFromStripe(index: stripes.firstIndex(where: { $0 === s }) ?? 0)
                }
            }
        }
    }

    public func cachePage(_ page: DatabasePage) async {
        let s = stripe(for: page.pageID)
        let needsEviction = s.withLock { st in
            if st.pageCache[page.pageID] == nil {
                st.pageOrder.append(page.pageID)
            }
            st.pageCache[page.pageID] = page
            st.referenced.insert(page.pageID)
            return st.pageCache.count > capacityPerStripe
        }
        if needsEviction {
            try? await evictFromStripe(index: page.pageID % stripeCount)
        }
    }

    // MARK: - Batch Page Read

    /// Read multiple pages, returning cached pages and batch-reading cache misses.
    /// Returns pages in the same order as input pageIDs.
    public func getPages(pageIDs: [Int]) async throws -> [DatabasePage] {
        // Partition into hits and misses
        var result = [DatabasePage?](repeating: nil, count: pageIDs.count)
        var missIndices: [Int] = []  // indices into pageIDs array
        var missPageIDs: [Int] = []  // actual page IDs to read

        for (i, pageID) in pageIDs.enumerated() {
            let s = stripe(for: pageID)
            let cached: DatabasePage? = s.withLock { st in
                if let page = st.pageCache[pageID] {
                    st.referenced.insert(pageID)
                    st.hitCount += 1
                    return page
                }
                st.missCount += 1
                return nil
            }
            if let cached {
                result[i] = cached
            } else {
                missIndices.append(i)
                missPageIDs.append(pageID)
            }
        }

        // Batch read all misses
        if !missPageIDs.isEmpty {
            let readPages = try storageManager.readPages(pageIDs: missPageIDs)
            // readPages returns in sorted order; map back to miss indices
            var pageByID = [Int: DatabasePage]()
            for page in readPages {
                pageByID[page.pageID] = page
                try await addToCache(page)
            }
            for i in missIndices {
                result[i] = pageByID[pageIDs[i]]
            }
        }

        return result.compactMap { $0 }
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
