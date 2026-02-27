import Foundation
import PantryCore

/// Replaces SwiftDB's file-per-node PageManager with page-backed storage via BufferPool.
/// Each B-tree node is serialized and stored in a PantryCore page.
public actor PageBackedNodeStore: Sendable {
    private var nodePageMap: [UUID: Int] = [:]
    private var nodeCache: [UUID: BTreeNode] = [:]
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
            // Update existing page
            var page = try await bufferPool.getPage(pageID: existingPageID)
            page.records = [record]
            page.recordCount = 1
            page.pageFlags = [.indexNode]
            try page.saveRecords()
            await bufferPool.updatePage(page)
            await bufferPool.markDirty(pageID: existingPageID)
        } else {
            // Allocate a new page
            var page = try await storageManager.createNewPage()
            page.pageFlags = [.indexNode]
            _ = page.addRecord(record)
            try page.saveRecords()
            await bufferPool.cachePage(page)
            await bufferPool.markDirty(pageID: page.pageID)
            nodePageMap[node.nodeId] = page.pageID
        }
        nodeCache[node.nodeId] = node
    }

    /// Load a B-tree node from its page
    public func loadNode(nodeId: UUID) async throws -> BTreeNode? {
        // Check in-memory cache first to avoid deserialization
        if let cached = nodeCache[nodeId] {
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
        return node
    }

    /// Flush all dirty index pages to disk
    public func flush() async throws {
        try await bufferPool.flushAllDirtyPages()
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
