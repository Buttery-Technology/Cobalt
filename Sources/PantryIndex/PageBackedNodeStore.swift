import Foundation
import PantryCore

/// Replaces SwiftDB's file-per-node PageManager with page-backed storage via BufferPool.
/// Each B-tree node is serialized and stored in a PantryCore page.
/// Converted from actor to class with internal locking to eliminate actor hops from BTree.
public final class PageBackedNodeStore: @unchecked Sendable {
    private struct State {
        var nodePageMap: [UUID: Int] = [:]
        var nodeCache: [UUID: BTreeNode] = [:]
        var nodeCacheOrder: [UUID: UInt64] = [:]
        var nodeCacheCounter: UInt64 = 0
        var dirtyNodes: Set<UUID> = []
    }
    private let state: PantryLock<State>
    private static let maxNodeCacheSize = 10_000
    private let bufferPool: BufferPoolManager
    private let storageManager: StorageManager

    public init(bufferPool: BufferPoolManager, storageManager: StorageManager) {
        self.bufferPool = bufferPool
        self.storageManager = storageManager
        self.state = PantryLock(State())
    }

    /// Save a B-tree node — caches in memory and marks dirty.
    /// Serialization is deferred until flushDirtyNodes() for batch I/O.
    public func saveNode(_ node: BTreeNode) async throws {
        let needsPage = state.withLock { s in
            s.nodePageMap[node.nodeId] == nil
        }
        if needsPage {
            var page = try await storageManager.createNewPage()
            page.pageFlags = [.indexNode]
            state.withLock { s in
                s.nodePageMap[node.nodeId] = page.pageID
            }
            await bufferPool.cachePage(page)
        }
        state.withLock { s in
            s.nodeCache[node.nodeId] = node
            s.dirtyNodes.insert(node.nodeId)
            s.nodeCacheCounter += 1
            s.nodeCacheOrder[node.nodeId] = s.nodeCacheCounter
            Self.evictIfNeeded(&s)
        }
    }

    /// Serialize and persist all dirty nodes to their pages.
    /// Called at transaction boundaries or during flush.
    public func flushDirtyNodes() async throws {
        let nodesToFlush: [(UUID, BTreeNode, Int)] = state.withLock { s in
            var result = [(UUID, BTreeNode, Int)]()
            for nodeId in s.dirtyNodes {
                guard let node = s.nodeCache[nodeId],
                      let pageID = s.nodePageMap[nodeId] else { continue }
                result.append((nodeId, node, pageID))
            }
            s.dirtyNodes.removeAll()
            return result
        }
        for (_, node, pageID) in nodesToFlush {
            let data = try node.serialize()
            let record = Record(id: Self.nodeId(node.nodeId), data: data)
            let recordSize = record.serialize().count
            let maxRecordSize = PantryConstants.PAGE_SIZE - PantryConstants.PAGE_HEADER_SIZE - PantryConstants.SLOT_SIZE
            guard recordSize <= maxRecordSize else {
                throw PantryError.pageOverflow
            }
            var page = try await bufferPool.getPage(pageID: pageID)
            page.records = [record]
            page.recordCount = 1
            page.pageFlags = [.indexNode]
            try page.saveRecords()
            bufferPool.updatePage(page)
            bufferPool.markDirty(pageID: pageID)
        }
    }

    /// Load a B-tree node — synchronous fast path for cache hits, async for misses.
    public func loadNode(nodeId: UUID) async throws -> BTreeNode? {
        // Fast path: cache hit (no async, no actor hop)
        let cached: BTreeNode? = state.withLock { s in
            if let node = s.nodeCache[nodeId] {
                s.nodeCacheCounter += 1
                s.nodeCacheOrder[nodeId] = s.nodeCacheCounter
                return node
            }
            return nil
        }
        if let cached { return cached }

        // Slow path: load from buffer pool
        let pageID: Int? = state.withLock { s in s.nodePageMap[nodeId] }
        guard let pageID else { return nil }

        let page = try await bufferPool.getPage(pageID: pageID)
        guard let record = page.records.first else { return nil }

        let node = try BTreeNode.deserialize(from: record.data)
        state.withLock { s in
            s.nodeCache[nodeId] = node
            s.nodeCacheCounter += 1
            s.nodeCacheOrder[nodeId] = s.nodeCacheCounter
            Self.evictIfNeeded(&s)
        }
        return node
    }

    /// Remove a node from the page map and cache (e.g. after merge absorbs it)
    public func removeNode(nodeId: UUID) {
        state.withLock { s in
            s.nodePageMap.removeValue(forKey: nodeId)
            s.nodeCache.removeValue(forKey: nodeId)
            s.nodeCacheOrder.removeValue(forKey: nodeId)
        }
    }

    /// LRU eviction: remove least recently used quarter when cache exceeds max size
    private static func evictIfNeeded(_ s: inout State) {
        guard s.nodeCache.count > maxNodeCacheSize else { return }
        let evictCount = maxNodeCacheSize / 4
        let sorted = s.nodeCacheOrder.sorted { $0.value < $1.value }
        for entry in sorted.prefix(evictCount) {
            s.nodeCache.removeValue(forKey: entry.key)
            s.nodeCacheOrder.removeValue(forKey: entry.key)
        }
    }

    /// Flush all dirty nodes then flush dirty index pages to disk
    public func flush() async throws {
        try await flushDirtyNodes()
        try await bufferPool.flushAllDirtyPages()
    }

    /// Get the current node→page mapping (for persistence)
    public func getNodePageMap() -> [UUID: Int] {
        state.withLock { s in s.nodePageMap }
    }

    /// Restore the node→page mapping (from persisted index registry)
    public func restoreNodePageMap(_ map: [UUID: Int]) {
        state.withLock { s in s.nodePageMap = map }
    }

    // Convert UUID to a stable UInt64 ID using all 16 bytes
    private static func nodeId(_ uuid: UUID) -> UInt64 {
        let u = uuid.uuid
        let high = UInt64(u.0) | UInt64(u.1) << 8 | UInt64(u.2) << 16 | UInt64(u.3) << 24 |
                   UInt64(u.4) << 32 | UInt64(u.5) << 40 | UInt64(u.6) << 48 | UInt64(u.7) << 56
        let low = UInt64(u.8) | UInt64(u.9) << 8 | UInt64(u.10) << 16 | UInt64(u.11) << 24 |
                  UInt64(u.12) << 32 | UInt64(u.13) << 40 | UInt64(u.14) << 48 | UInt64(u.15) << 56
        return high ^ low
    }
}

/// Metadata for tracking the root of a B-tree index
public struct IndexMetadata: Codable, Sendable {
    public var rootNodeId: UUID?
    public var tableName: String
    public var columnName: String
    public var nodeCount: Int

    public init(rootNodeId: UUID? = nil, tableName: String, columnName: String, nodeCount: Int = 0) {
        self.rootNodeId = rootNodeId
        self.tableName = tableName
        self.columnName = columnName
        self.nodeCount = nodeCount
    }
}

/// Persisted index registry entry — stores everything needed to restore an index
public struct IndexRegistryEntry: Codable, Sendable {
    public var tableName: String
    public var columnName: String
    public var compoundColumns: [String]?
    public var rootNodeId: UUID?
    public var nodePageMap: [String: Int] // UUID string → page ID
    public var bloomFilterSnapshot: BloomFilterSnapshot?

    public init(tableName: String, columnName: String, compoundColumns: [String]? = nil, rootNodeId: UUID?, nodePageMap: [UUID: Int], bloomFilterSnapshot: BloomFilterSnapshot? = nil) {
        self.tableName = tableName
        self.columnName = columnName
        self.compoundColumns = compoundColumns
        self.rootNodeId = rootNodeId
        self.nodePageMap = Dictionary(uniqueKeysWithValues: nodePageMap.map { ($0.key.uuidString, $0.value) })
        self.bloomFilterSnapshot = bloomFilterSnapshot
    }

    public var decodedNodePageMap: [UUID: Int] {
        Dictionary(uniqueKeysWithValues: nodePageMap.compactMap { key, value in
            UUID(uuidString: key).map { ($0, value) }
        })
    }
}
