import Foundation

/// Manages an in-memory cache of database pages to minimize disk I/O
public actor BufferPoolManager: Sendable {
    private let capacity: Int
    private let storageManager: StorageManager
    private var pageCache: [Int: DatabasePage] = [:]
    private var dirtyPages = Set<Int>()
    private var accessOrder: [Int: UInt64] = [:]
    private var nextAccessCounter: UInt64 = 0
    private(set) var stats = BufferPoolStats()

    public init(capacity: Int, storageManager: StorageManager) {
        self.capacity = capacity
        self.storageManager = storageManager
    }

    // MARK: - Core Page Management

    public func getPage(pageID: Int) async throws -> DatabasePage {
        if let cachedPage = pageCache[pageID] {
            touchAccess(pageID: pageID)
            stats.hitCount += 1
            return cachedPage
        }

        stats.missCount += 1
        let page = try await storageManager.readPage(pageID: pageID)
        try await addToCache(page)
        return page
    }

    public func isPageCached(pageID: Int) -> Bool {
        pageCache[pageID] != nil
    }

    public func getCachedPage(pageID: Int) -> DatabasePage? {
        if let page = pageCache[pageID] {
            touchAccess(pageID: pageID)
            stats.hitCount += 1
            return page
        }
        return nil
    }

    public func updatePage(_ page: DatabasePage) {
        pageCache[page.pageID] = page
        touchAccess(pageID: page.pageID)
    }

    public func markDirty(pageID: Int) {
        dirtyPages.insert(pageID)
    }

    public func isDirty(pageID: Int) -> Bool {
        dirtyPages.contains(pageID)
    }

    public func clearDirtyFlag(pageID: Int) {
        dirtyPages.remove(pageID)
    }

    // MARK: - Cache Management

    private func addToCache(_ page: DatabasePage) async throws {
        if pageCache.count >= capacity {
            try await evictPages(count: 1)
        }
        pageCache[page.pageID] = page
        touchAccess(pageID: page.pageID)
    }

    private func touchAccess(pageID: Int) {
        accessOrder[pageID] = nextAccessCounter
        nextAccessCounter += 1
    }

    private func selectPagesForEviction(count: Int = 1) -> [Int] {
        if count == 1 {
            // O(n) minimum scan instead of O(n log n) sort
            guard let oldest = accessOrder.min(by: { $0.value < $1.value }) else { return [] }
            return [oldest.key]
        }
        return Array(accessOrder.sorted { $0.value < $1.value }.prefix(count).map { $0.key })
    }

    public func evictPages(count: Int = 1) async throws {
        let pagesToEvict = selectPagesForEviction(count: count)

        for pageID in pagesToEvict {
            if dirtyPages.contains(pageID) {
                if var page = pageCache[pageID] {
                    try await storageManager.writePage(&page)
                }
                dirtyPages.remove(pageID)
            }
            pageCache.removeValue(forKey: pageID)
            accessOrder.removeValue(forKey: pageID)
            stats.evictionCount += 1
        }
    }

    /// Evict a specific page from cache (used after rollback to discard stale data)
    public func evictPage(pageID: Int) {
        pageCache.removeValue(forKey: pageID)
        accessOrder.removeValue(forKey: pageID)
        dirtyPages.remove(pageID)
    }

    // MARK: - Flushing

    public func flushAllDirtyPages() async throws {
        let dirtyPagesCopy = dirtyPages
        for pageID in dirtyPagesCopy {
            if var page = pageCache[pageID] {
                try await storageManager.writePage(&page)
                stats.flushCount += 1
            }
            // Always clear dirty flag — if page is not in cache, it's an orphaned entry
            dirtyPages.remove(pageID)
        }
        // Single fsync after all pages are written
        try await storageManager.sync()
    }

    public func flushPage(pageID: Int) async throws {
        if dirtyPages.contains(pageID), var page = pageCache[pageID] {
            try await storageManager.writePage(&page)
            dirtyPages.remove(pageID)
            stats.flushCount += 1
        }
    }

    // MARK: - Maintenance

    public func performMaintenance() async throws {
        let dirtyPageCount = dirtyPages.count
        if dirtyPageCount > capacity / 4 {
            let sortedDirtyPages = dirtyPages.sorted { pageID1, pageID2 in
                let order1 = accessOrder[pageID1] ?? 0
                let order2 = accessOrder[pageID2] ?? 0
                return order1 < order2
            }
            let pagesToFlush = sortedDirtyPages.prefix(max(1, dirtyPageCount / 4))
            for pageID in pagesToFlush {
                try await flushPage(pageID: pageID)
            }
        }

        if pageCache.count > Int(Double(capacity) * 0.9) {
            let targetCount = Int(Double(capacity) * 0.8)
            let evictionCount = pageCache.count - targetCount
            if evictionCount > 0 {
                let cleanPages = accessOrder
                    .filter { !dirtyPages.contains($0.key) }
                    .sorted { $0.value < $1.value }
                    .prefix(evictionCount)
                for (pageID, _) in cleanPages {
                    pageCache.removeValue(forKey: pageID)
                    accessOrder.removeValue(forKey: pageID)
                    stats.evictionCount += 1
                }
            }
        }
    }

    public func cachePage(_ page: DatabasePage) async {
        pageCache[page.pageID] = page
        touchAccess(pageID: page.pageID)
        if pageCache.count > capacity {
            try? await evictPages(count: 1)
        }
    }

    // MARK: - Statistics

    public func getStats() -> BufferPoolStats {
        stats
    }

    public func resetStats() {
        stats = BufferPoolStats()
    }

    public func getCacheOccupancy() -> Double {
        Double(pageCache.count) / Double(capacity)
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
