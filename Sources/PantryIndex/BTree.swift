import Foundation
import PantryCore

/// B-tree backed by PantryCore pages via PageBackedNodeStore.
/// Algorithm logic preserved from SwiftDB; persistence layer swapped.
public actor BTree: Sendable {
    private let order: Int
    private var rootId: UUID?
    private let nodeStore: PageBackedNodeStore

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

    public func searchRange(from startKey: DBValue?, to endKey: DBValue?) async throws -> [Row] {
        guard let rootId = rootId,
              let root = try await nodeStore.loadNode(nodeId: rootId) else {
            return []
        }
        var results: [Row] = []
        try await collectRange(node: root, startKey: startKey, endKey: endKey, results: &results)
        return results
    }

    // MARK: - Delete

    public func delete(key: DBValue, row: Row? = nil) async throws {
        guard let rootId = rootId,
              let root = try await nodeStore.loadNode(nodeId: rootId) else {
            return
        }

        try await deleteFromNode(node: root, key: key, row: row)

        if root.keys.isEmpty && !root.isLeaf {
            if let newRootId = root.children?.first {
                self.rootId = newRootId
            }
        } else {
            try await nodeStore.saveNode(root)
        }
    }

    // MARK: - Search Helpers

    private func searchNode(node: BTreeNode, key: DBValue) async throws -> [Row] {
        var i = 0
        while i < node.keys.count && key > node.keys[i] {
            i += 1
        }

        if node.isLeaf {
            // Collect all matching keys at this leaf
            var results: [Row] = []
            var j = i
            while j < node.keys.count && node.keys[j] == key {
                results.append(node.values[j])
                j += 1
            }
            return results
        }

        // Internal node: collect from left subtree, matching keys here, and right subtrees
        var results: [Row] = []

        // Search left child subtree
        if let childId = node.children?[i],
           let child = try await nodeStore.loadNode(nodeId: childId) {
            results.append(contentsOf: try await searchNode(node: child, key: key))
        }

        // Collect all matching keys at this internal node, plus right subtrees
        while i < node.keys.count && node.keys[i] == key {
            results.append(node.values[i])
            i += 1
            if let childId = node.children?[i],
               let child = try await nodeStore.loadNode(nodeId: childId) {
                results.append(contentsOf: try await searchNode(node: child, key: key))
            }
        }

        return results
    }

    private func collectRange(node: BTreeNode, startKey: DBValue?, endKey: DBValue?, results: inout [Row]) async throws {
        var i = 0

        if let start = startKey {
            while i < node.keys.count && node.keys[i] < start {
                i += 1
            }
        }

        if node.isLeaf {
            while i < node.keys.count {
                if let end = endKey, node.keys[i] > end {
                    break
                }
                results.append(node.values[i])
                i += 1
            }
        } else {
            while i <= node.keys.count {
                // Traverse child subtree first (contains values < keys[i])
                if let childId = node.children?[i],
                   let child = try await nodeStore.loadNode(nodeId: childId) {
                    try await collectRange(node: child, startKey: startKey, endKey: endKey, results: &results)
                }

                // Then check if current key is beyond range
                if i < node.keys.count {
                    if let end = endKey, node.keys[i] > end {
                        break
                    }
                    results.append(node.values[i])
                }

                i += 1
            }
        }
    }

    // MARK: - Insert Helpers

    private func insertNonFull(node: BTreeNode, key: DBValue, row: Row) async throws {
        var i = node.keys.count - 1

        if node.isLeaf {
            node.keys.append(key)
            node.values.append(row)

            while i >= 0 && node.keys[i] > key {
                node.keys[i + 1] = node.keys[i]
                node.values[i + 1] = node.values[i]
                i -= 1
            }
            node.keys[i + 1] = key
            node.values[i + 1] = row

            try await nodeStore.saveNode(node)
        } else {
            while i >= 0 && node.keys[i] > key {
                i -= 1
            }
            i += 1

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

        newNode.keys = Array(child.keys[(midIndex + 1)...])
        newNode.values = Array(child.values[(midIndex + 1)...])

        if !child.isLeaf {
            newNode.children = Array(child.children![(midIndex + 1)...])
        }

        let midKey = child.keys[midIndex]
        let midValue = child.values[midIndex]
        child.keys = Array(child.keys[..<midIndex])
        child.values = Array(child.values[..<midIndex])

        if !child.isLeaf {
            child.children = Array(child.children![...(midIndex)])
        }

        parent.keys.insert(midKey, at: childIndex)
        parent.values.insert(midValue, at: childIndex)
        parent.children!.insert(newNode.nodeId, at: childIndex + 1)

        try await nodeStore.saveNode(child)
        try await nodeStore.saveNode(newNode)
        try await nodeStore.saveNode(parent)
    }

    // MARK: - Delete Helpers

    private func deleteFromNode(node: BTreeNode, key: DBValue, row: Row? = nil) async throws {
        var keyIndex = findKeyIndex(node: node, key: key)

        // Row-matching only at leaf level — at internal nodes, advancing past matching keys
        // with a different row value would cause descent into the wrong child subtree
        if node.isLeaf, let row = row {
            while keyIndex < node.keys.count && node.keys[keyIndex] == key && node.values[keyIndex] != row {
                keyIndex += 1
            }
        }

        if keyIndex < node.keys.count && node.keys[keyIndex] == key {
            if node.isLeaf {
                removeFromLeaf(node: node, index: keyIndex)
                try await nodeStore.saveNode(node)
            } else if row == nil || node.values[keyIndex] == row {
                // Key and row both match (or no row filter) — standard internal node delete
                try await deleteFromInternalNode(node: node, keyIndex: keyIndex, row: row)
            } else {
                // Key matches but row doesn't — check consecutive duplicate keys at this node
                var scanIdx = keyIndex + 1
                while scanIdx < node.keys.count && node.keys[scanIdx] == key {
                    if node.values[scanIdx] == row {
                        try await deleteFromInternalNode(node: node, keyIndex: scanIdx, row: row)
                        return
                    }
                    scanIdx += 1
                }
                // Not found at this node — descend into subtree via re-search
                guard let childId = node.children?[keyIndex],
                      let child = try await nodeStore.loadNode(nodeId: childId) else {
                    throw PantryError.indexCorrupted(description: "Child node not found during row-filtered delete")
                }
                if child.keys.count == order - 1 {
                    try await fillChild(parent: node, childIndex: keyIndex)
                    try await deleteFromNode(node: node, key: key, row: row)
                } else {
                    try await deleteFromNode(node: child, key: key, row: row)
                }
                return
            }
        } else if !node.isLeaf {
            guard let childId = node.children?[keyIndex],
                  let child = try await nodeStore.loadNode(nodeId: childId) else {
                throw PantryError.indexCorrupted(description: "Child node not found during deletion")
            }

            if child.keys.count == order - 1 {
                try await fillChild(parent: node, childIndex: keyIndex)
                // After fill/merge, structure may have changed — re-search from this node
                try await deleteFromNode(node: node, key: key, row: row)
            } else {
                try await deleteFromNode(node: child, key: key, row: row)
            }
        }
    }

    private func deleteFromInternalNode(node: BTreeNode, keyIndex: Int, row: Row? = nil) async throws {
        let key = node.keys[keyIndex]

        guard let leftChildId = node.children?[keyIndex],
              let leftChild = try await nodeStore.loadNode(nodeId: leftChildId) else {
            throw PantryError.indexCorrupted(description: "Left child not found")
        }

        if leftChild.keys.count >= order {
            let (predKey, predValue) = try await getPredecessor(node: leftChild)
            node.keys[keyIndex] = predKey
            node.values[keyIndex] = predValue
            try await nodeStore.saveNode(node)
            // Delete the specific predecessor entry
            try await deleteFromNode(node: leftChild, key: predKey, row: predValue)
        } else {
            guard let rightChildId = node.children?[keyIndex + 1],
                  let rightChild = try await nodeStore.loadNode(nodeId: rightChildId) else {
                throw PantryError.indexCorrupted(description: "Right child not found")
            }

            if rightChild.keys.count >= order {
                let (succKey, succValue) = try await getSuccessor(node: rightChild)
                node.keys[keyIndex] = succKey
                node.values[keyIndex] = succValue
                try await nodeStore.saveNode(node)
                // Delete the specific successor entry
                try await deleteFromNode(node: rightChild, key: succKey, row: succValue)
            } else {
                try await mergeWithRightSibling(parent: node, leftIndex: keyIndex)
                // Reload left child after merge — the original reference is stale
                guard let mergedChild = try await nodeStore.loadNode(nodeId: leftChildId) else {
                    throw PantryError.indexCorrupted(description: "Merged child not found")
                }
                try await deleteFromNode(node: mergedChild, key: key, row: row)
            }
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
        leftChild.keys.append(parent.keys[parentKeyIndex])
        leftChild.values.append(parent.values[parentKeyIndex])

        parent.keys[parentKeyIndex] = rightChild.keys[0]
        parent.values[parentKeyIndex] = rightChild.values[0]

        if !leftChild.isLeaf {
            leftChild.children!.append(rightChild.children![0])
            rightChild.children!.removeFirst()
        }

        rightChild.keys.removeFirst()
        rightChild.values.removeFirst()
    }

    private func borrowFromLeftSibling(parent: BTreeNode, leftChild: BTreeNode, rightChild: BTreeNode, parentKeyIndex: Int) {
        rightChild.keys.insert(parent.keys[parentKeyIndex], at: 0)
        rightChild.values.insert(parent.values[parentKeyIndex], at: 0)

        parent.keys[parentKeyIndex] = leftChild.keys.last!
        parent.values[parentKeyIndex] = leftChild.values.last!

        if !rightChild.isLeaf {
            rightChild.children!.insert(leftChild.children!.last!, at: 0)
            leftChild.children!.removeLast()
        }

        leftChild.keys.removeLast()
        leftChild.values.removeLast()
    }

    private func mergeWithRightSibling(parent: BTreeNode, leftIndex: Int) async throws {
        guard let leftChildId = parent.children?[leftIndex],
              let leftChild = try await nodeStore.loadNode(nodeId: leftChildId),
              let rightChildId = parent.children?[leftIndex + 1],
              let rightChild = try await nodeStore.loadNode(nodeId: rightChildId) else {
            throw PantryError.indexCorrupted(description: "Children not found during merge")
        }

        leftChild.keys.append(parent.keys[leftIndex])
        leftChild.values.append(parent.values[leftIndex])
        leftChild.keys.append(contentsOf: rightChild.keys)
        leftChild.values.append(contentsOf: rightChild.values)

        if !leftChild.isLeaf {
            leftChild.children!.append(contentsOf: rightChild.children!)
        }

        parent.keys.remove(at: leftIndex)
        parent.values.remove(at: leftIndex)
        parent.children!.remove(at: leftIndex + 1)

        try await nodeStore.saveNode(leftChild)
        try await nodeStore.saveNode(parent)

        // Clean up the absorbed right child from cache/page map
        await nodeStore.removeNode(nodeId: rightChildId)
    }

    // MARK: - Helpers

    private func getPredecessor(node: BTreeNode) async throws -> (DBValue, Row) {
        var current = node
        while !current.isLeaf {
            guard let lastChildId = current.children?.last,
                  let lastChild = try await nodeStore.loadNode(nodeId: lastChildId) else {
                throw PantryError.indexCorrupted(description: "Child not found while finding predecessor")
            }
            current = lastChild
        }
        return (current.keys.last!, current.values.last!)
    }

    private func getSuccessor(node: BTreeNode) async throws -> (DBValue, Row) {
        var current = node
        while !current.isLeaf {
            guard let firstChildId = current.children?.first,
                  let firstChild = try await nodeStore.loadNode(nodeId: firstChildId) else {
                throw PantryError.indexCorrupted(description: "Child not found while finding successor")
            }
            current = firstChild
        }
        return (current.keys.first!, current.values.first!)
    }

    private func removeFromLeaf(node: BTreeNode, index: Int) {
        node.keys.remove(at: index)
        node.values.remove(at: index)
    }

    private func findKeyIndex(node: BTreeNode, key: DBValue) -> Int {
        var index = 0
        while index < node.keys.count && node.keys[index] < key {
            index += 1
        }
        return index
    }
}
