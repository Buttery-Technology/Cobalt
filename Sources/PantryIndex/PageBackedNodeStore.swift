import Foundation
import PantryCore

/// Replaces SwiftDB's file-per-node PageManager with page-backed storage via BufferPool.
/// Each B-tree node is serialized and stored in a PantryCore page.
public actor PageBackedNodeStore: Sendable {
    private var nodePageMap: [UUID: Int] = [:]
    /// Node object cache with LRU eviction — returns references directly without deep copying.
    /// Safe because BTree and PageBackedNodeStore are both actors (serialized access).
    private var nodeCache: [UUID: BTreeNode] = [:]
    private var nodeCacheOrder: [UUID: UInt64] = [:]  // LRU: access counter per node
    private var nodeCacheCounter: UInt64 = 0
    private static let maxNodeCacheSize = 10_000
    private let bufferPool: BufferPoolManager
    private let storageManager: StorageManager

    public init(bufferPool: BufferPoolManager, storageManager: StorageManager) {
        self.bufferPool = bufferPool
        self.storageManager = storageManager
    }

    /// Save a B-tree node to a page
    public func saveNode(_ node: BTreeNode) async throws {
        let data = try node.serialize()
        let record = Record(id: nodeId(node.nodeId), data: data)

        if let existingPageID = nodePageMap[node.nodeId] {
            // Update existing page — validate serialized node fits
            let recordSize = record.serialize().count
            let maxRecordSize = PantryConstants.PAGE_SIZE - PantryConstants.PAGE_HEADER_SIZE - PantryConstants.SLOT_SIZE
            guard recordSize <= maxRecordSize else {
                throw PantryError.pageOverflow
            }
            var page = try await bufferPool.getPage(pageID: existingPageID)
            page.records = [record]
            page.recordCount = 1
            page.pageFlags = [.indexNode]
            try page.saveRecords()
            bufferPool.updatePage(page)
            bufferPool.markDirty(pageID: existingPageID)
        } else {
            // Allocate a new page
            var page = try await storageManager.createNewPage()
            page.pageFlags = [.indexNode]
            guard page.addRecord(record) else {
                throw PantryError.pageOverflow
            }
            try page.saveRecords()
            await bufferPool.cachePage(page)
            bufferPool.markDirty(pageID: page.pageID)
            nodePageMap[node.nodeId] = page.pageID
        }
        // Cache the node directly — no copy needed (actor serializes access)
        nodeCache[node.nodeId] = node
        nodeCacheCounter += 1
        nodeCacheOrder[node.nodeId] = nodeCacheCounter
        evictNodeCacheIfNeeded()
    }

    /// Load a B-tree node from its page.
    /// Returns cached reference directly (no deep copy) — safe under actor isolation.
    public func loadNode(nodeId: UUID) async throws -> BTreeNode? {
        if let cached = nodeCache[nodeId] {
            nodeCacheCounter += 1
            nodeCacheOrder[nodeId] = nodeCacheCounter
            return cached
        }

        guard let pageID = nodePageMap[nodeId] else {
            return nil
        }

        let page = try await bufferPool.getPage(pageID: pageID)
        guard let record = page.records.first else {
            return nil
        }

        let node = try BTreeNode.deserialize(from: record.data)
        nodeCache[nodeId] = node
        nodeCacheCounter += 1
        nodeCacheOrder[nodeId] = nodeCacheCounter
        evictNodeCacheIfNeeded()
        return node
    }

    /// Remove a node from the page map and cache (e.g. after merge absorbs it)
    public func removeNode(nodeId: UUID) {
        nodePageMap.removeValue(forKey: nodeId)
        nodeCache.removeValue(forKey: nodeId)
        nodeCacheOrder.removeValue(forKey: nodeId)
    }

    /// LRU eviction: remove least recently used quarter when cache exceeds max size
    private func evictNodeCacheIfNeeded() {
        guard nodeCache.count > Self.maxNodeCacheSize else { return }
        let evictCount = Self.maxNodeCacheSize / 4
        let sorted = nodeCacheOrder.sorted { $0.value < $1.value }
        for entry in sorted.prefix(evictCount) {
            nodeCache.removeValue(forKey: entry.key)
            nodeCacheOrder.removeValue(forKey: entry.key)
        }
    }

    /// Flush all dirty index pages to disk
    public func flush() async throws {
        try await bufferPool.flushAllDirtyPages()
    }

    /// Get the current node→page mapping (for persistence)
    public func getNodePageMap() -> [UUID: Int] {
        nodePageMap
    }

    /// Restore the node→page mapping (from persisted index registry)
    public func restoreNodePageMap(_ map: [UUID: Int]) {
        nodePageMap = map
    }

    // Convert UUID to a stable UInt64 ID using all 16 bytes
    private func nodeId(_ uuid: UUID) -> UInt64 {
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
