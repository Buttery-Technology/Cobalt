import Foundation
import PantryCore

/// B-tree node for page-backed storage
public final class BTreeNode: Codable, @unchecked Sendable {
    public var keys: [DBValue]
    public var values: [Row]
    public var children: [UUID]?
    public let nodeId: UUID
    public var isLeaf: Bool

    public init(isLeaf: Bool = true) {
        self.keys = []
        self.values = []
        self.children = isLeaf ? nil : []
        self.nodeId = UUID()
        self.isLeaf = isLeaf
    }

    // Private init that preserves nodeId (for copy via Codable round-trip is expensive)
    private init(nodeId: UUID, isLeaf: Bool, keys: [DBValue], values: [Row], children: [UUID]?) {
        self.nodeId = nodeId
        self.isLeaf = isLeaf
        self.keys = keys
        self.values = values
        self.children = children
    }

    /// Create a deep copy preserving the node ID
    public func deepCopy() -> BTreeNode {
        BTreeNode(nodeId: nodeId, isLeaf: isLeaf, keys: keys, values: values, children: children)
    }

    /// Serialize to binary data for page storage
    public func serialize() throws -> Data {
        let encoder = JSONEncoder()
        encoder.nonConformingFloatEncodingStrategy = .convertToString(positiveInfinity: "+inf", negativeInfinity: "-inf", nan: "nan")
        return try encoder.encode(self)
    }

    /// Deserialize from binary data
    public static func deserialize(from data: Data) throws -> BTreeNode {
        let decoder = JSONDecoder()
        decoder.nonConformingFloatDecodingStrategy = .convertFromString(positiveInfinity: "+inf", negativeInfinity: "-inf", nan: "nan")
        return try decoder.decode(BTreeNode.self, from: data)
    }
}
