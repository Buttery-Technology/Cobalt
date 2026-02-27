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

    /// Serialize to binary data for page storage
    public func serialize() throws -> Data {
        try JSONEncoder().encode(self)
    }

    /// Deserialize from binary data
    public static func deserialize(from data: Data) throws -> BTreeNode {
        try JSONDecoder().decode(BTreeNode.self, from: data)
    }
}
