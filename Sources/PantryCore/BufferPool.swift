import Foundation
import Synchronization

/// Mutable state protected by Mutex
private struct BufferPoolState: ~Copyable {
    var pageCache: [Int: DatabasePage] = [:]
    var dirtyPages = Set<Int>()
    var accessOrder: [Int: UInt64] = [:]
    var nextAccessCounter: UInt64 = 0
    var stats = BufferPoolStats()
}

/// Manages an in-memory cache of database pages to minimize disk I/O
public final class BufferPoolManager: Sendable {
    private let capacity: Int
    private let storageManager: StorageManager
    private let state = Mutex(BufferPoolState())

    public init(capacity: Int, storageManager: StorageManager) {
        self.capacity = capacity
        self.storageManager = storageManager
    }

    // MARK: - Core Page Management

    public func getPage(pageID: Int) async throws -> DatabasePage {
        let cached: DatabasePage? = state.withLock { s in
            if let page = s.pageCache[pageID] {
                s.accessOrder[pageID] = s.nextAccessCounter
                s.nextAccessCounter += 1
                s.stats.hitCount += 1
                return page
            }
            s.stats.missCount += 1
            return nil
        }
        if let cached { return cached }

        let page = try await storageManager.readPage(pageID: pageID)
        try await addToCache(page)
        return page
    }

    public func isPageCached(pageID: Int) -> Bool {
        state.withLock { $0.pageCache[pageID] != nil }
    }

    public func getCachedPage(pageID: Int) -> DatabasePage? {
        state.withLock { s in
            if let page = s.pageCache[pageID] {
                s.accessOrder[pageID] = s.nextAccessCounter
                s.nextAccessCounter += 1
                s.stats.hitCount += 1
                return page
            }
            return nil
        }
    }

    public func updatePage(_ page: DatabasePage) {
        state.withLock { s in
            s.pageCache[page.pageID] = page
            s.accessOrder[page.pageID] = s.nextAccessCounter
            s.nextAccessCounter += 1
        }
    }

    public func markDirty(pageID: Int) {
        _ = state.withLock { $0.dirtyPages.insert(pageID) }
    }

    public func isDirty(pageID: Int) -> Bool {
        state.withLock { $0.dirtyPages.contains(pageID) }
    }

    public func clearDirtyFlag(pageID: Int) {
        _ = state.withLock { $0.dirtyPages.remove(pageID) }
    }

    // MARK: - Cache Management

    private func addToCache(_ page: DatabasePage) async throws {
        let needsEviction = state.withLock { $0.pageCache.count >= capacity }
        if needsEviction {
            try await evictPages(count: 1)
        }
        state.withLock { s in
            s.pageCache[page.pageID] = page
            s.accessOrder[page.pageID] = s.nextAccessCounter
            s.nextAccessCounter += 1
        }
    }

    private func selectPagesForEviction(count: Int = 1) -> [Int] {
        state.withLock { s in
            if count == 1 {
                guard let oldest = s.accessOrder.min(by: { $0.value < $1.value }) else { return [] }
                return [oldest.key]
            }
            return Array(s.accessOrder.sorted { $0.value < $1.value }.prefix(count).map { $0.key })
        }
    }

    public func evictPages(count: Int = 1) async throws {
        let pagesToEvict = selectPagesForEviction(count: count)

        for pageID in pagesToEvict {
            // Snapshot the dirty page under lock, then do I/O outside
            let dirtyPage: DatabasePage? = state.withLock { s in
                if s.dirtyPages.contains(pageID), let page = s.pageCache[pageID] {
                    return page
                }
                return nil
            }
            if var page = dirtyPage {
                try await storageManager.writePage(&page)
            }
            state.withLock { s in
                s.dirtyPages.remove(pageID)
                s.pageCache.removeValue(forKey: pageID)
                s.accessOrder.removeValue(forKey: pageID)
                s.stats.evictionCount += 1
            }
        }
    }

    /// Evict a specific page from cache (used after rollback to discard stale data)
    public func evictPage(pageID: Int) {
        state.withLock { s in
            s.pageCache.removeValue(forKey: pageID)
            s.accessOrder.removeValue(forKey: pageID)
            s.dirtyPages.remove(pageID)
        }
    }

    // MARK: - Flushing

    public func flushAllDirtyPages() async throws {
        // Snapshot dirty pages and their data under lock
        let pagesToFlush: [(Int, DatabasePage)] = state.withLock { s in
            s.dirtyPages.compactMap { pageID in
                guard let page = s.pageCache[pageID] else {
                    s.dirtyPages.remove(pageID)
                    return nil
                }
                return (pageID, page)
            }
        }

        for (pageID, var page) in pagesToFlush {
            try await storageManager.writePage(&page)
            state.withLock { s in
                s.stats.flushCount += 1
                s.dirtyPages.remove(pageID)
            }
        }
        try await storageManager.sync()
    }

    public func flushPage(pageID: Int) async throws {
        let pageToFlush: DatabasePage? = state.withLock { s in
            if s.dirtyPages.contains(pageID), let page = s.pageCache[pageID] {
                return page
            }
            return nil
        }
        if var page = pageToFlush {
            try await storageManager.writePage(&page)
            state.withLock { s in
                s.dirtyPages.remove(pageID)
                s.stats.flushCount += 1
            }
        }
    }

    // MARK: - Maintenance

    public func performMaintenance() async throws {
        let pagesToFlush: [Int] = state.withLock { s in
            let dirtyPageCount = s.dirtyPages.count
            guard dirtyPageCount > s.pageCache.count / 4 || dirtyPageCount > capacity / 4 else { return [] }
            let sorted = s.dirtyPages.sorted { pageID1, pageID2 in
                let order1 = s.accessOrder[pageID1] ?? 0
                let order2 = s.accessOrder[pageID2] ?? 0
                return order1 < order2
            }
            return Array(sorted.prefix(max(1, dirtyPageCount / 4)))
        }

        for pageID in pagesToFlush {
            try await flushPage(pageID: pageID)
        }

        // Evict clean pages if cache is >90% full
        state.withLock { s in
            if s.pageCache.count > Int(Double(capacity) * 0.9) {
                let targetCount = Int(Double(capacity) * 0.8)
                let evictionCount = s.pageCache.count - targetCount
                if evictionCount > 0 {
                    let cleanPages = s.accessOrder
                        .filter { !s.dirtyPages.contains($0.key) }
                        .sorted { $0.value < $1.value }
                        .prefix(evictionCount)
                    for (pageID, _) in cleanPages {
                        s.pageCache.removeValue(forKey: pageID)
                        s.accessOrder.removeValue(forKey: pageID)
                        s.stats.evictionCount += 1
                    }
                }
            }
        }
    }

    public func cachePage(_ page: DatabasePage) async {
        let needsEviction = state.withLock { s in
            s.pageCache[page.pageID] = page
            s.accessOrder[page.pageID] = s.nextAccessCounter
            s.nextAccessCounter += 1
            return s.pageCache.count > capacity
        }
        if needsEviction {
            try? await evictPages(count: 1)
        }
    }

    // MARK: - Statistics

    public func getStats() -> BufferPoolStats {
        state.withLock { $0.stats }
    }

    public func resetStats() {
        state.withLock { $0.stats = BufferPoolStats() }
    }

    public func getCacheOccupancy() -> Double {
        state.withLock { Double($0.pageCache.count) / Double(capacity) }
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
