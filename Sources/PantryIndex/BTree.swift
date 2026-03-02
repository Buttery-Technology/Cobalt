import Foundation
import PantryCore

/// B-tree backed by PantryCore pages via PageBackedNodeStore.
/// Algorithm logic preserved from SwiftDB; persistence layer swapped.
/// Converted from actor to class — access is serialized by the owning ColumnIndex actor.
public final class BTree: @unchecked Sendable {
    private let order: Int
    private var rootId: UUID?
    public let nodeStore: PageBackedNodeStore

    public init(order: Int = 64, nodeStore: PageBackedNodeStore) {
        self.order = order
        self.nodeStore = nodeStore
    }

    /// Set the root ID (used when restoring from metadata)
    public func setRootId(_ id: UUID?) {
        self.rootId = id
    }

    public func getRootId() -> UUID? {
        rootId
    }

    /// Whether this B-tree is empty (no root node)
    public var isEmpty: Bool { rootId == nil }

    // MARK: - Insert

    public func insert(key: DBValue, row: Row) async throws {
        if rootId == nil {
            let root = BTreeNode(isLeaf: true)
            root.keys.append(key)
            root.values.append(row)
            try await nodeStore.saveNode(root)
            rootId = root.nodeId
            return
        }

        guard let rootNode = try await nodeStore.loadNode(nodeId: rootId!) else {
            throw PantryError.indexCorrupted(description: "Root node not found")
        }

        if rootNode.keys.count == 2 * order - 1 {
            let newRoot = BTreeNode(isLeaf: false)
            newRoot.children!.append(rootNode.nodeId)
            try await splitChild(parent: newRoot, childIndex: 0)
            rootId = newRoot.nodeId
            try await insertNonFull(node: newRoot, key: key, row: row)
        } else {
            try await insertNonFull(node: rootNode, key: key, row: row)
        }
    }

    // MARK: - Search

    public func search(key: DBValue) async throws -> [Row] {
        guard let rootId = rootId,
              let root = try await nodeStore.loadNode(nodeId: rootId) else {
            return []
        }
        return try await searchNode(node: root, key: key)
    }

    /// Synchronous search using only cached B-tree nodes. Returns nil on any cache miss.
    /// Eliminates all async overhead for warm-cache lookups.
    /// Uses single-lock path traversal for minimal lock overhead.
    public func searchCached(key: DBValue) -> [Row]? {
        guard let rootId = rootId else { return nil }

        // Fast path: single-lock root→leaf traversal
        if let leaf = nodeStore.loadPathCached(rootId: rootId, searchKey: key) {
            return searchLeafCached(leaf: leaf, key: key)
        }
        // Fallback: per-node traversal (shouldn't normally happen if path is warm)
        guard let root = nodeStore.loadNodeCached(nodeId: rootId) else { return nil }
        return searchNodeCached(node: root, key: key)
    }

    /// Search within a leaf node and follow sibling pointers for duplicates.
    private func searchLeafCached(leaf: BTreeNode, key: DBValue) -> [Row]? {
        var i = lowerBound(leaf.keys, key)
        var results: [Row] = []
        var currentLeaf: BTreeNode? = leaf
        var j = i
        while let leaf = currentLeaf {
            while j < leaf.keys.count && leaf.keys[j] == key {
                results.append(leaf.values[j])
                j += 1
            }
            if j >= leaf.keys.count, let nextId = leaf.nextLeafId {
                guard let next = nodeStore.loadNodeCached(nodeId: nextId) else { return nil }
                currentLeaf = next
                j = 0
            } else {
                break
            }
        }
        return results
    }

    private func searchNodeCached(node: BTreeNode, key: DBValue) -> [Row]? {
        var i = lowerBound(node.keys, key)

        if node.isLeaf {
            var results: [Row] = []
            var currentLeaf: BTreeNode? = node
            var j = i
            while let leaf = currentLeaf {
                while j < leaf.keys.count && leaf.keys[j] == key {
                    results.append(leaf.values[j])
                    j += 1
                }
                if j >= leaf.keys.count, let nextId = leaf.nextLeafId {
                    guard let next = nodeStore.loadNodeCached(nodeId: nextId) else { return nil }
                    currentLeaf = next
                    j = 0
                } else {
                    break
                }
            }
            return results
        }

        if let childId = node.children?[i],
           let child = nodeStore.loadNodeCached(nodeId: childId) {
            return searchNodeCached(node: child, key: key)
        }
        return nil
    }

    /// Synchronous cache-only range scan with limit. Returns nil on any cache miss.
    public func searchRangeWithLimitCached(from startKey: DBValue?, to endKey: DBValue?, limit: Int, ascending: Bool = true) -> [Row]? {
        guard let rootId = rootId,
              let root = nodeStore.loadNodeCached(nodeId: rootId) else {
            return nil
        }
        var results: [Row] = []
        results.reserveCapacity(limit)
        if ascending {
            if !collectRangeLimitedCached(node: root, startKey: startKey, endKey: endKey, results: &results, limit: limit) { return nil }
        } else {
            if !collectRangeReverseLimitedCached(node: root, startKey: startKey, endKey: endKey, results: &results, limit: limit) { return nil }
        }
        return results
    }

    /// Sync ascending range scan. Returns false on cache miss.
    private func collectRangeLimitedCached(node: BTreeNode, startKey: DBValue?, endKey: DBValue?, results: inout [Row], limit: Int) -> Bool {
        guard let (leaf, startIndex) = findLeafAndIndexCached(from: node, key: startKey) else { return false }
        var currentLeaf: BTreeNode? = leaf
        var i = startIndex
        while let leaf = currentLeaf, results.count < limit {
            while i < leaf.keys.count && results.count < limit {
                if let end = endKey, leaf.keys[i] > end { return true }
                results.append(leaf.values[i])
                i += 1
            }
            if let nextId = leaf.nextLeafId {
                guard let next = nodeStore.loadNodeCached(nodeId: nextId) else { return false }
                currentLeaf = next
                i = 0
            } else {
                currentLeaf = nil
            }
        }
        return true
    }

    /// Sync descending range scan. Returns false on cache miss.
    private func collectRangeReverseLimitedCached(node: BTreeNode, startKey: DBValue?, endKey: DBValue?, results: inout [Row], limit: Int) -> Bool {
        guard let (leaf, endIndex) = findLeafAndIndexReverseCached(from: node, key: endKey) else { return false }
        var currentLeaf: BTreeNode? = leaf
        var i = endIndex
        while let leaf = currentLeaf, results.count < limit {
            while i >= 0 && results.count < limit {
                if let start = startKey, leaf.keys[i] < start { return true }
                results.append(leaf.values[i])
                i -= 1
            }
            if let prevId = leaf.prevLeafId {
                guard let prev = nodeStore.loadNodeCached(nodeId: prevId) else { return false }
                currentLeaf = prev
                i = (prev.keys.count) - 1
            } else {
                currentLeaf = nil
            }
        }
        return true
    }

    /// Sync find leaf for ascending scan. Returns nil on cache miss.
    private func findLeafAndIndexCached(from node: BTreeNode, key: DBValue?) -> (BTreeNode, Int)? {
        var current = node
        while !current.isLeaf {
            let childIdx = key.map { lowerBound(current.keys, $0) } ?? 0
            guard let childId = current.children?[childIdx],
                  let child = nodeStore.loadNodeCached(nodeId: childId) else { return nil }
            current = child
        }
        let startIdx = key.map { lowerBound(current.keys, $0) } ?? 0
        return (current, startIdx)
    }

    /// Sync find leaf for descending scan. Returns nil on cache miss.
    private func findLeafAndIndexReverseCached(from node: BTreeNode, key: DBValue?) -> (BTreeNode, Int)? {
        var current = node
        while !current.isLeaf {
            let childIdx: Int
            if let k = key {
                childIdx = min(upperBound(current.keys, k), (current.children?.count ?? 1) - 1)
            } else {
                childIdx = (current.children?.count ?? 1) - 1
            }
            guard let childId = current.children?[childIdx],
                  let child = nodeStore.loadNodeCached(nodeId: childId) else { return nil }
            current = child
        }
        let endIdx = key.map { upperBound(current.keys, $0) - 1 } ?? (current.keys.count - 1)
        return (current, endIdx)
    }

    /// Range scan from a lower bound, collecting rows while predicate returns true.
    /// Exploits B-tree sort order: once predicate fails, no further matches exist.
    public func searchRangeWhile(from startKey: DBValue?, predicate: @Sendable (DBValue) -> Bool) async throws -> [Row] {
        guard let rootId = rootId,
              let root = try await nodeStore.loadNode(nodeId: rootId) else {
            return []
        }
        var results: [Row] = []
        try await collectRangeWhile(node: root, startKey: startKey, predicate: predicate, results: &results)
        return results
    }

    /// In-order traversal from lowerBound(startKey), collecting while predicate holds on keys.
    private func collectRangeWhile(node: BTreeNode, startKey: DBValue?, predicate: @Sendable (DBValue) -> Bool, results: inout [Row]) async throws {
        let (leaf, startIndex) = try await findLeafAndIndex(from: node, key: startKey)
        var currentLeaf: BTreeNode? = leaf
        var i = startIndex
        while let leaf = currentLeaf {
            while i < leaf.keys.count {
                guard predicate(leaf.keys[i]) else { return }
                results.append(leaf.values[i])
                i += 1
            }
            if let nextId = leaf.nextLeafId {
                currentLeaf = try await nodeStore.loadNode(nodeId: nextId)
                i = 0
            } else {
                currentLeaf = nil
            }
        }
    }

    public func searchRange(from startKey: DBValue?, to endKey: DBValue?) async throws -> [Row] {
        guard let rootId = rootId,
              let root = try await nodeStore.loadNode(nodeId: rootId) else {
            return []
        }
        var results: [Row] = []
        try await collectRange(node: root, startKey: startKey, endKey: endKey, results: &results)
        return results
    }

    /// Range scan returning (key, value) pairs for TID-only index reconstruction
    public func searchRangeKeyed(from startKey: DBValue?, to endKey: DBValue?) async throws -> [(DBValue, Row)] {
        guard let rootId = rootId,
              let root = try await nodeStore.loadNode(nodeId: rootId) else {
            return []
        }
        var results: [(DBValue, Row)] = []
        try await collectRangeKeyed(node: root, startKey: startKey, endKey: endKey, results: &results)
        return results
    }

    private func collectRangeKeyed(node: BTreeNode, startKey: DBValue?, endKey: DBValue?, results: inout [(DBValue, Row)]) async throws {
        let (leaf, startIndex) = try await findLeafAndIndex(from: node, key: startKey)
        var currentLeaf: BTreeNode? = leaf
        var i = startIndex
        while let leaf = currentLeaf {
            while i < leaf.keys.count {
                if let end = endKey, leaf.keys[i] > end { return }
                results.append((leaf.keys[i], leaf.values[i]))
                i += 1
            }
            if let nextId = leaf.nextLeafId {
                currentLeaf = try await nodeStore.loadNode(nodeId: nextId)
                i = 0
            } else {
                currentLeaf = nil
            }
        }
    }

    /// Range scan with early termination after collecting `limit` rows.
    /// When `ascending` is false, results are collected in descending order.
    public func searchRangeWithLimit(from startKey: DBValue?, to endKey: DBValue?, limit: Int, ascending: Bool = true) async throws -> [Row] {
        guard let rootId = rootId,
              let root = try await nodeStore.loadNode(nodeId: rootId) else {
            return []
        }
        var results: [Row] = []
        results.reserveCapacity(limit)
        if ascending {
            try await collectRangeLimited(node: root, startKey: startKey, endKey: endKey, results: &results, limit: limit)
        } else {
            try await collectRangeReverseLimited(node: root, startKey: startKey, endKey: endKey, results: &results, limit: limit)
        }
        return results
    }

    /// Collect rows in ascending order with early exit once limit is reached
    private func collectRangeLimited(node: BTreeNode, startKey: DBValue?, endKey: DBValue?, results: inout [Row], limit: Int) async throws {
        let (leaf, startIndex) = try await findLeafAndIndex(from: node, key: startKey)
        var currentLeaf: BTreeNode? = leaf
        var i = startIndex
        while let leaf = currentLeaf, results.count < limit {
            while i < leaf.keys.count && results.count < limit {
                if let end = endKey, leaf.keys[i] > end { return }
                results.append(leaf.values[i])
                i += 1
            }
            if let nextId = leaf.nextLeafId {
                currentLeaf = try await nodeStore.loadNode(nodeId: nextId)
                i = 0
            } else {
                currentLeaf = nil
            }
        }
    }

    /// Collect rows in descending order with early exit once limit is reached.
    /// Uses prevLeafId sibling pointers for efficient right-to-left leaf traversal.
    private func collectRangeReverseLimited(node: BTreeNode, startKey: DBValue?, endKey: DBValue?, results: inout [Row], limit: Int) async throws {
        // Find the rightmost leaf containing endKey (or the last leaf if no endKey)
        let (leaf, endIndex) = try await findLeafAndIndexReverse(from: node, key: endKey)

        var currentLeaf: BTreeNode? = leaf
        var i = endIndex
        let lo = startKey
        while let leaf = currentLeaf, results.count < limit {
            while i >= 0 && results.count < limit {
                if let start = lo, leaf.keys[i] < start { return }
                results.append(leaf.values[i])
                i -= 1
            }
            if let prevId = leaf.prevLeafId {
                currentLeaf = try await nodeStore.loadNode(nodeId: prevId)
                i = (currentLeaf?.keys.count ?? 0) - 1
            } else {
                currentLeaf = nil
            }
        }
    }

    /// Navigate from a node down to the leaf containing (or just before) the given key for reverse scan.
    private func findLeafAndIndexReverse(from node: BTreeNode, key: DBValue?) async throws -> (BTreeNode, Int) {
        var current = node
        while !current.isLeaf {
            let childIdx: Int
            if let k = key {
                childIdx = min(upperBound(current.keys, k), (current.children?.count ?? 1) - 1)
            } else {
                childIdx = (current.children?.count ?? 1) - 1
            }
            guard let childId = current.children?[childIdx],
                  let child = try await nodeStore.loadNode(nodeId: childId) else {
                throw PantryError.indexCorrupted(description: "Child node not found during reverse leaf search")
            }
            current = child
        }
        let endIdx: Int
        if let k = key {
            endIdx = upperBound(current.keys, k) - 1
        } else {
            endIdx = current.keys.count - 1
        }
        return (current, endIdx)
    }

    /// Count entries in a range without materializing Row objects.
    /// Much faster than searchRange + count for large result sets.
    public func countRange(from startKey: DBValue?, to endKey: DBValue?) async throws -> Int64 {
        guard let rootId = rootId,
              let root = try await nodeStore.loadNode(nodeId: rootId) else {
            return 0
        }
        let (leaf, startIndex) = try await findLeafAndIndex(from: root, key: startKey)
        var count: Int64 = 0
        var currentLeaf: BTreeNode? = leaf
        var i = startIndex
        while let leaf = currentLeaf {
            while i < leaf.keys.count {
                if let end = endKey, leaf.keys[i] > end { return count }
                count += 1
                i += 1
            }
            if let nextId = leaf.nextLeafId {
                currentLeaf = try await nodeStore.loadNode(nodeId: nextId)
                i = 0
            } else {
                currentLeaf = nil
            }
        }
        return count
    }

    // MARK: - Batch Sorted Search

    /// Sorted batch search: given keys in ascending order, traverse the leaf chain once.
    /// O(log N + K + L) where K=keys, L=leaves visited, vs O(K * log N) for individual searches.
    public func searchBatchSorted(sortedKeys: [DBValue]) async throws -> [DBValue: [Row]] {
        guard !sortedKeys.isEmpty, let rootId = rootId,
              let root = try await nodeStore.loadNode(nodeId: rootId) else { return [:] }

        let (leaf, startIndex) = try await findLeafAndIndex(from: root, key: sortedKeys[0])
        var currentLeaf: BTreeNode? = leaf
        var leafIdx = startIndex
        var keyIdx = 0
        var results = [DBValue: [Row]]()

        while let leaf = currentLeaf, keyIdx < sortedKeys.count {
            while leafIdx < leaf.keys.count && keyIdx < sortedKeys.count {
                let leafKey = leaf.keys[leafIdx]
                let searchKey = sortedKeys[keyIdx]
                if leafKey < searchKey {
                    leafIdx += 1
                } else if leafKey == searchKey {
                    results[searchKey, default: []].append(leaf.values[leafIdx])
                    leafIdx += 1
                } else {
                    keyIdx += 1
                }
            }
            if let nextId = leaf.nextLeafId {
                currentLeaf = try await nodeStore.loadNode(nodeId: nextId)
                leafIdx = 0
            } else {
                break
            }
        }
        return results
    }

    /// Synchronous cache-only sorted batch search. Returns nil on any cache miss.
    public func searchBatchSortedCached(sortedKeys: [DBValue]) -> [DBValue: [Row]]? {
        guard !sortedKeys.isEmpty, let rootId = rootId,
              let root = nodeStore.loadNodeCached(nodeId: rootId) else {
            return sortedKeys.isEmpty ? [:] : nil
        }

        guard let (leaf, startIndex) = findLeafAndIndexCached(from: root, key: sortedKeys[0]) else { return nil }
        var currentLeaf: BTreeNode? = leaf
        var leafIdx = startIndex
        var keyIdx = 0
        var results = [DBValue: [Row]]()

        while let leaf = currentLeaf, keyIdx < sortedKeys.count {
            while leafIdx < leaf.keys.count && keyIdx < sortedKeys.count {
                let leafKey = leaf.keys[leafIdx]
                let searchKey = sortedKeys[keyIdx]
                if leafKey < searchKey {
                    leafIdx += 1
                } else if leafKey == searchKey {
                    results[searchKey, default: []].append(leaf.values[leafIdx])
                    leafIdx += 1
                } else {
                    keyIdx += 1
                }
            }
            if let nextId = leaf.nextLeafId {
                guard let next = nodeStore.loadNodeCached(nodeId: nextId) else { return nil }
                currentLeaf = next
                leafIdx = 0
            } else {
                break
            }
        }
        return results
    }

    // MARK: - RID-Only Range Scan

    /// Range scan returning only RIDs in index order (no Row allocation).
    public func searchRangeWithLimitRIDs(from startKey: DBValue?, to endKey: DBValue?, limit: Int, ascending: Bool = true) async throws -> [UInt64] {
        guard let rootId = rootId,
              let root = try await nodeStore.loadNode(nodeId: rootId) else { return [] }
        var results = [UInt64]()
        results.reserveCapacity(limit)
        if ascending {
            let (leaf, startIndex) = try await findLeafAndIndex(from: root, key: startKey)
            var currentLeaf: BTreeNode? = leaf
            var i = startIndex
            while let leaf = currentLeaf, results.count < limit {
                while i < leaf.keys.count && results.count < limit {
                    if let end = endKey, leaf.keys[i] > end { return results }
                    if case .integer(let ridSigned) = leaf.values[i].values["__rid"] {
                        results.append(UInt64(bitPattern: ridSigned))
                    }
                    i += 1
                }
                if let nextId = leaf.nextLeafId {
                    currentLeaf = try await nodeStore.loadNode(nodeId: nextId)
                    i = 0
                } else {
                    currentLeaf = nil
                }
            }
        } else {
            let (leaf, endIndex) = try await findLeafAndIndexReverse(from: root, key: endKey)
            var currentLeaf: BTreeNode? = leaf
            var i = endIndex
            while let leaf = currentLeaf, results.count < limit {
                while i >= 0 && results.count < limit {
                    if let start = startKey, leaf.keys[i] < start { return results }
                    if case .integer(let ridSigned) = leaf.values[i].values["__rid"] {
                        results.append(UInt64(bitPattern: ridSigned))
                    }
                    i -= 1
                }
                if let prevId = leaf.prevLeafId {
                    currentLeaf = try await nodeStore.loadNode(nodeId: prevId)
                    i = (currentLeaf?.keys.count ?? 0) - 1
                } else {
                    currentLeaf = nil
                }
            }
        }
        return results
    }

    /// Synchronous cache-only RID range scan. Returns nil on any cache miss.
    public func searchRangeWithLimitRIDsCached(from startKey: DBValue?, to endKey: DBValue?, limit: Int, ascending: Bool = true) -> [UInt64]? {
        guard let rootId = rootId,
              let root = nodeStore.loadNodeCached(nodeId: rootId) else { return nil }
        var results = [UInt64]()
        results.reserveCapacity(limit)
        if ascending {
            guard let (leaf, startIndex) = findLeafAndIndexCached(from: root, key: startKey) else { return nil }
            var currentLeaf: BTreeNode? = leaf
            var i = startIndex
            while let leaf = currentLeaf, results.count < limit {
                while i < leaf.keys.count && results.count < limit {
                    if let end = endKey, leaf.keys[i] > end { return results }
                    if case .integer(let ridSigned) = leaf.values[i].values["__rid"] {
                        results.append(UInt64(bitPattern: ridSigned))
                    }
                    i += 1
                }
                if let nextId = leaf.nextLeafId {
                    guard let next = nodeStore.loadNodeCached(nodeId: nextId) else { return nil }
                    currentLeaf = next
                    i = 0
                } else {
                    currentLeaf = nil
                }
            }
        } else {
            guard let (leaf, endIndex) = findLeafAndIndexReverseCached(from: root, key: endKey) else { return nil }
            var currentLeaf: BTreeNode? = leaf
            var i = endIndex
            while let leaf = currentLeaf, results.count < limit {
                while i >= 0 && results.count < limit {
                    if let start = startKey, leaf.keys[i] < start { return results }
                    if case .integer(let ridSigned) = leaf.values[i].values["__rid"] {
                        results.append(UInt64(bitPattern: ridSigned))
                    }
                    i -= 1
                }
                if let prevId = leaf.prevLeafId {
                    guard let prev = nodeStore.loadNodeCached(nodeId: prevId) else { return nil }
                    currentLeaf = prev
                    i = (prev.keys.count) - 1
                } else {
                    currentLeaf = nil
                }
            }
        }
        return results
    }

    // MARK: - Delete

    public func delete(key: DBValue, rid: DBValue? = nil) async throws {
        guard let rootId = rootId,
              let root = try await nodeStore.loadNode(nodeId: rootId) else {
            return
        }

        try await deleteFromNode(node: root, key: key, rid: rid)

        if root.keys.isEmpty {
            if root.isLeaf {
                // Tree is now empty — reset rootId
                self.rootId = nil
                nodeStore.removeNode(nodeId: rootId)
            } else if let newRootId = root.children?.first {
                self.rootId = newRootId
                nodeStore.removeNode(nodeId: rootId)
            }
        } else {
            try await nodeStore.saveNode(root)
        }
    }

    /// Batch delete: sorts keys for cache-friendly B-tree traversal.
    public func deleteBatch(pairs: [(key: DBValue, rid: DBValue)]) async throws {
        guard !pairs.isEmpty else { return }
        let sorted = pairs.sorted { $0.key < $1.key }

        for (key, rid) in sorted {
            guard let currentRootId = rootId,
                  let root = try await nodeStore.loadNode(nodeId: currentRootId) else { return }
            try await deleteFromNode(node: root, key: key, rid: rid)
            if root.keys.isEmpty {
                if root.isLeaf {
                    self.rootId = nil
                    nodeStore.removeNode(nodeId: currentRootId)
                    return
                } else if let newRootId = root.children?.first {
                    self.rootId = newRootId
                    nodeStore.removeNode(nodeId: currentRootId)
                }
            }
        }
        // Save root once at end
        if let currentRootId = rootId,
           let root = try await nodeStore.loadNode(nodeId: currentRootId) {
            try await nodeStore.saveNode(root)
        }
    }

    // MARK: - Binary Search Helpers

    /// Binary search for the first index where keys[index] >= key (lower bound)
    private func lowerBound(_ keys: [DBValue], _ key: DBValue) -> Int {
        var lo = 0, hi = keys.count
        while lo < hi {
            let mid = lo + (hi - lo) / 2
            if keys[mid] < key { lo = mid + 1 } else { hi = mid }
        }
        return lo
    }

    /// Binary search for the first index where keys[index] > key (upper bound)
    private func upperBound(_ keys: [DBValue], _ key: DBValue) -> Int {
        var lo = 0, hi = keys.count
        while lo < hi {
            let mid = lo + (hi - lo) / 2
            if keys[mid] <= key { lo = mid + 1 } else { hi = mid }
        }
        return lo
    }

    // MARK: - Search Helpers

    private func searchNode(node: BTreeNode, key: DBValue) async throws -> [Row] {
        var i = lowerBound(node.keys, key)

        if node.isLeaf {
            // B+ tree: all data lives in leaves. Collect matching keys, follow sibling if needed.
            var results: [Row] = []
            var currentLeaf: BTreeNode? = node
            var j = i
            while let leaf = currentLeaf {
                while j < leaf.keys.count && leaf.keys[j] == key {
                    results.append(leaf.values[j])
                    j += 1
                }
                // Duplicates may span to next leaf
                if j >= leaf.keys.count, let nextId = leaf.nextLeafId {
                    currentLeaf = try await nodeStore.loadNode(nodeId: nextId)
                    j = 0
                } else {
                    break
                }
            }
            return results
        }

        // Internal node: keys are routing copies. Descend to find leaf-level data.
        // Search the leftmost child that could contain the key
        if let childId = node.children?[i],
           let child = try await nodeStore.loadNode(nodeId: childId) {
            return try await searchNode(node: child, key: key)
        }

        return []
    }

    private func collectRange(node: BTreeNode, startKey: DBValue?, endKey: DBValue?, results: inout [Row]) async throws {
        // Find the starting leaf node
        let (leaf, startIndex) = try await findLeafAndIndex(from: node, key: startKey)

        // Scan from the starting position, following sibling pointers
        var currentLeaf: BTreeNode? = leaf
        var i = startIndex
        while let leaf = currentLeaf {
            while i < leaf.keys.count {
                if let end = endKey, leaf.keys[i] > end {
                    return
                }
                results.append(leaf.values[i])
                i += 1
            }
            // Follow sibling pointer to next leaf
            if let nextId = leaf.nextLeafId {
                currentLeaf = try await nodeStore.loadNode(nodeId: nextId)
                i = 0
            } else {
                currentLeaf = nil
            }
        }
    }

    /// Navigate from a node down to the leaf containing (or just after) the given key.
    /// Returns the leaf node and the starting index within it.
    private func findLeafAndIndex(from node: BTreeNode, key: DBValue?) async throws -> (BTreeNode, Int) {
        var current = node
        while !current.isLeaf {
            let childIdx: Int
            if let k = key {
                childIdx = lowerBound(current.keys, k)
            } else {
                childIdx = 0
            }
            guard let childId = current.children?[childIdx],
                  let child = try await nodeStore.loadNode(nodeId: childId) else {
                throw PantryError.indexCorrupted(description: "Child node not found during leaf search")
            }
            current = child
        }
        let startIdx: Int
        if let k = key {
            startIdx = lowerBound(current.keys, k)
        } else {
            startIdx = 0
        }
        return (current, startIdx)
    }

    // MARK: - Insert Helpers

    private func insertNonFull(node: BTreeNode, key: DBValue, row: Row) async throws {
        if node.isLeaf {
            // Binary search for insertion point
            let insertAt = lowerBound(node.keys, key)
            node.keys.insert(key, at: insertAt)
            node.values.insert(row, at: insertAt)
            try await nodeStore.saveNode(node)
        } else {
            // upperBound: first index where keys[i] > key — gives correct child index
            var i = upperBound(node.keys, key)

            guard let childId = node.children?[i],
                  let child = try await nodeStore.loadNode(nodeId: childId) else {
                throw PantryError.indexCorrupted(description: "Child node not found")
            }

            if child.keys.count == 2 * order - 1 {
                try await splitChild(parent: node, childIndex: i)
                if node.keys[i] < key {
                    i += 1
                }

                guard let newChildId = node.children?[i],
                      let newChild = try await nodeStore.loadNode(nodeId: newChildId) else {
                    throw PantryError.indexCorrupted(description: "Child node not found after split")
                }
                try await insertNonFull(node: newChild, key: key, row: row)
            } else {
                try await insertNonFull(node: child, key: key, row: row)
            }
        }
    }

    private func splitChild(parent: BTreeNode, childIndex: Int) async throws {
        guard let childId = parent.children?[childIndex],
              let child = try await nodeStore.loadNode(nodeId: childId) else {
            throw PantryError.indexCorrupted(description: "Child node not found for splitting")
        }

        let newNode = BTreeNode(isLeaf: child.isLeaf)
        let midIndex = order - 1

        let midKey: DBValue
        let midValue: Row

        if child.isLeaf {
            // B+ tree leaf split: copy-up. All data stays in leaves.
            // Right child gets keys[midIndex..end] (includes midKey)
            newNode.keys = Array(child.keys[midIndex...])
            newNode.values = Array(child.values[midIndex...])
            midKey = child.keys[midIndex]
            midValue = child.values[midIndex]
            child.keys = Array(child.keys[..<midIndex])
            child.values = Array(child.values[..<midIndex])
        } else {
            // Traditional B-tree internal split: push-up
            newNode.keys = Array(child.keys[(midIndex + 1)...])
            newNode.values = Array(child.values[(midIndex + 1)...])
            newNode.children = Array(child.children![(midIndex + 1)...])
            midKey = child.keys[midIndex]
            midValue = child.values[midIndex]
            child.keys = Array(child.keys[..<midIndex])
            child.values = Array(child.values[..<midIndex])
            child.children = Array(child.children![...(midIndex)])
        }

        parent.keys.insert(midKey, at: childIndex)
        parent.values.insert(midValue, at: childIndex)
        parent.children!.insert(newNode.nodeId, at: childIndex + 1)

        // Maintain leaf sibling pointers
        if child.isLeaf {
            newNode.nextLeafId = child.nextLeafId
            newNode.prevLeafId = child.nodeId
            if let oldNextId = child.nextLeafId,
               let oldNext = try await nodeStore.loadNode(nodeId: oldNextId) {
                oldNext.prevLeafId = newNode.nodeId
                try await nodeStore.saveNode(oldNext)
            }
            child.nextLeafId = newNode.nodeId
        }

        try await nodeStore.saveNode(child)
        try await nodeStore.saveNode(newNode)
        try await nodeStore.saveNode(parent)
    }

    // MARK: - Bulk Load

    /// Build the B-tree bottom-up from pre-sorted pairs. O(n) vs O(n log n) for individual inserts.
    /// The tree must be empty (rootId == nil) before calling this method.
    public func bulkLoad(sortedPairs: [(key: DBValue, row: Row)]) async throws {
        guard !sortedPairs.isEmpty else { return }
        precondition(rootId == nil, "bulkLoad requires an empty B-tree")

        // Fill to `order` keys per node (not 2*order-1) to stay within page size limits
        // and leave room for future inserts without immediate splits.
        let keysPerNode = order

        // Step 1: Create leaf nodes from sorted data
        var leafNodes = [BTreeNode]()
        leafNodes.reserveCapacity((sortedPairs.count + keysPerNode - 1) / keysPerNode)
        var i = 0
        while i < sortedPairs.count {
            let node = BTreeNode(isLeaf: true)
            let end = min(i + keysPerNode, sortedPairs.count)
            node.keys.reserveCapacity(end - i)
            node.values.reserveCapacity(end - i)
            for j in i..<end {
                node.keys.append(sortedPairs[j].key)
                node.values.append(sortedPairs[j].row)
            }
            leafNodes.append(node)
            i = end
        }

        // Step 2: Link leaf siblings
        for j in 0..<leafNodes.count {
            if j > 0 { leafNodes[j].prevLeafId = leafNodes[j - 1].nodeId }
            if j < leafNodes.count - 1 { leafNodes[j].nextLeafId = leafNodes[j + 1].nodeId }
        }

        // Step 3: Save all leaf nodes in batch (single file extend)
        try await nodeStore.saveNodesBatch(leafNodes)

        // Step 4: Build internal levels bottom-up
        var currentLevel: [BTreeNode] = leafNodes
        while currentLevel.count > 1 {
            var nextLevel = [BTreeNode]()
            var idx = 0
            while idx < currentLevel.count {
                let parent = BTreeNode(isLeaf: false)
                // First child — no separator key needed
                parent.children!.append(currentLevel[idx].nodeId)
                idx += 1
                // Add subsequent children with separator keys
                while idx < currentLevel.count && parent.keys.count < keysPerNode {
                    let child = currentLevel[idx]
                    parent.keys.append(child.keys[0])
                    parent.values.append(child.values[0])
                    parent.children!.append(child.nodeId)
                    idx += 1
                }
                // If this parent has only 1 child, merge into previous parent
                if parent.keys.isEmpty && !nextLevel.isEmpty {
                    let prev = nextLevel[nextLevel.count - 1]
                    let orphanChild = currentLevel[idx - 1]
                    prev.keys.append(orphanChild.keys[0])
                    prev.values.append(orphanChild.values[0])
                    prev.children!.append(orphanChild.nodeId)
                } else {
                    nextLevel.append(parent)
                }
            }
            // Save internal level in batch
            try await nodeStore.saveNodesBatch(nextLevel)
            currentLevel = nextLevel
        }

        // Step 5: Set root
        rootId = currentLevel.first?.nodeId

        // Step 6: Flush all dirty nodes
        try await nodeStore.flushDirtyNodes()
    }

    // MARK: - Delete Helpers

    private func deleteFromNode(node: BTreeNode, key: DBValue, rid: DBValue? = nil) async throws {
        if node.isLeaf {
            // B+ tree: all data lives in leaves. Find and remove the matching entry.
            var keyIndex = lowerBound(node.keys, key)
            if let rid = rid {
                while keyIndex < node.keys.count && node.keys[keyIndex] == key && node.values[keyIndex].values["__rid"] != rid {
                    keyIndex += 1
                }
            }
            if keyIndex < node.keys.count && node.keys[keyIndex] == key {
                removeFromLeaf(node: node, index: keyIndex)
                try await nodeStore.saveNode(node)
            }
            return
        }

        // Internal node: routing copies only. Descend to the correct child.
        let keyIndex = lowerBound(node.keys, key)
        // If key matches separator, data is in right subtree (child[keyIndex+1])
        let childIdx = (keyIndex < node.keys.count && node.keys[keyIndex] == key) ? keyIndex + 1 : keyIndex

        guard childIdx < (node.children?.count ?? 0),
              let childId = node.children?[childIdx],
              let child = try await nodeStore.loadNode(nodeId: childId) else {
            throw PantryError.indexCorrupted(description: "Child not found during B+ tree delete descent")
        }

        if child.keys.count == order - 1 {
            try await fillChild(parent: node, childIndex: childIdx)
            // After fill/merge, structure may have changed — re-search from this node
            try await deleteFromNode(node: node, key: key, rid: rid)
        } else {
            try await deleteFromNode(node: child, key: key, rid: rid)
        }
    }

    private func fillChild(parent: BTreeNode, childIndex: Int) async throws {
        if childIndex != parent.keys.count {
            guard let leftChildId = parent.children?[childIndex],
                  let leftChild = try await nodeStore.loadNode(nodeId: leftChildId),
                  let rightChildId = parent.children?[childIndex + 1],
                  let rightChild = try await nodeStore.loadNode(nodeId: rightChildId) else {
                throw PantryError.indexCorrupted(description: "Children not found during fill")
            }

            if rightChild.keys.count >= order {
                borrowFromRightSibling(parent: parent, leftChild: leftChild, rightChild: rightChild, parentKeyIndex: childIndex)
                try await nodeStore.saveNode(parent)
                try await nodeStore.saveNode(leftChild)
                try await nodeStore.saveNode(rightChild)
                return
            }
        }

        if childIndex != 0 {
            guard let leftChildId = parent.children?[childIndex - 1],
                  let leftChild = try await nodeStore.loadNode(nodeId: leftChildId),
                  let rightChildId = parent.children?[childIndex],
                  let rightChild = try await nodeStore.loadNode(nodeId: rightChildId) else {
                throw PantryError.indexCorrupted(description: "Children not found during fill")
            }

            if leftChild.keys.count >= order {
                borrowFromLeftSibling(parent: parent, leftChild: leftChild, rightChild: rightChild, parentKeyIndex: childIndex - 1)
                try await nodeStore.saveNode(parent)
                try await nodeStore.saveNode(leftChild)
                try await nodeStore.saveNode(rightChild)
                return
            }
        }

        if childIndex != parent.keys.count {
            try await mergeWithRightSibling(parent: parent, leftIndex: childIndex)
        } else {
            try await mergeWithRightSibling(parent: parent, leftIndex: childIndex - 1)
        }
    }

    private func borrowFromRightSibling(parent: BTreeNode, leftChild: BTreeNode, rightChild: BTreeNode, parentKeyIndex: Int) {
        if leftChild.isLeaf {
            // B+ tree leaf: move right[0] to left, update separator to right's new first key
            leftChild.keys.append(rightChild.keys[0])
            leftChild.values.append(rightChild.values[0])
            rightChild.keys.removeFirst()
            rightChild.values.removeFirst()
            parent.keys[parentKeyIndex] = rightChild.keys[0]
            parent.values[parentKeyIndex] = rightChild.values[0]
        } else {
            // Internal node: standard B-tree borrow
            leftChild.keys.append(parent.keys[parentKeyIndex])
            leftChild.values.append(parent.values[parentKeyIndex])
            parent.keys[parentKeyIndex] = rightChild.keys[0]
            parent.values[parentKeyIndex] = rightChild.values[0]
            leftChild.children!.append(rightChild.children![0])
            rightChild.children!.removeFirst()
            rightChild.keys.removeFirst()
            rightChild.values.removeFirst()
        }
    }

    private func borrowFromLeftSibling(parent: BTreeNode, leftChild: BTreeNode, rightChild: BTreeNode, parentKeyIndex: Int) {
        if rightChild.isLeaf {
            // B+ tree leaf: move left.last to right[0], update separator to new right[0]
            rightChild.keys.insert(leftChild.keys.last!, at: 0)
            rightChild.values.insert(leftChild.values.last!, at: 0)
            leftChild.keys.removeLast()
            leftChild.values.removeLast()
            parent.keys[parentKeyIndex] = rightChild.keys[0]
            parent.values[parentKeyIndex] = rightChild.values[0]
        } else {
            // Internal node: standard B-tree borrow
            rightChild.keys.insert(parent.keys[parentKeyIndex], at: 0)
            rightChild.values.insert(parent.values[parentKeyIndex], at: 0)
            parent.keys[parentKeyIndex] = leftChild.keys.last!
            parent.values[parentKeyIndex] = leftChild.values.last!
            rightChild.children!.insert(leftChild.children!.last!, at: 0)
            leftChild.children!.removeLast()
            leftChild.keys.removeLast()
            leftChild.values.removeLast()
        }
    }

    private func mergeWithRightSibling(parent: BTreeNode, leftIndex: Int) async throws {
        guard let leftChildId = parent.children?[leftIndex],
              let leftChild = try await nodeStore.loadNode(nodeId: leftChildId),
              let rightChildId = parent.children?[leftIndex + 1],
              let rightChild = try await nodeStore.loadNode(nodeId: rightChildId) else {
            throw PantryError.indexCorrupted(description: "Children not found during merge")
        }

        if leftChild.isLeaf {
            // B+ tree leaf merge: separator is a copy, skip it
            leftChild.keys.append(contentsOf: rightChild.keys)
            leftChild.values.append(contentsOf: rightChild.values)
        } else {
            // Internal node merge: push separator down
            leftChild.keys.append(parent.keys[leftIndex])
            leftChild.values.append(parent.values[leftIndex])
            leftChild.keys.append(contentsOf: rightChild.keys)
            leftChild.values.append(contentsOf: rightChild.values)
            leftChild.children!.append(contentsOf: rightChild.children!)
        }

        parent.keys.remove(at: leftIndex)
        parent.values.remove(at: leftIndex)
        parent.children!.remove(at: leftIndex + 1)

        // Maintain leaf sibling pointers after merge
        if leftChild.isLeaf {
            leftChild.nextLeafId = rightChild.nextLeafId
            if let nextId = rightChild.nextLeafId,
               let nextNode = try await nodeStore.loadNode(nodeId: nextId) {
                nextNode.prevLeafId = leftChild.nodeId
                try await nodeStore.saveNode(nextNode)
            }
        }

        try await nodeStore.saveNode(leftChild)
        try await nodeStore.saveNode(parent)

        // Clean up the absorbed right child from cache/page map
        nodeStore.removeNode(nodeId: rightChildId)
    }

    // MARK: - Helpers

    private func removeFromLeaf(node: BTreeNode, index: Int) {
        node.keys.remove(at: index)
        node.values.remove(at: index)
    }

    private func findKeyIndex(node: BTreeNode, key: DBValue) -> Int {
        lowerBound(node.keys, key)
    }
}
