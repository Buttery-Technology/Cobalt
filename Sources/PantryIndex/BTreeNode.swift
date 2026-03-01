import Foundation
import PantryCore

/// Magic bytes for binary B-tree node format: "BTN\x01"
private let btnMagic: [UInt8] = [0x42, 0x54, 0x4E, 0x01]

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

    /// Serialize to binary data for page storage
    ///
    /// Binary format:
    /// ```
    /// [4B magic "BTN\x01"]
    /// [1B flags: bit 0 = isLeaf]
    /// [16B nodeId UUID (big-endian)]
    /// [2B key count (UInt16)]
    /// per key:
    ///   [DBValue binary encoding]
    /// per key (values array — same count):
    ///   [Row binary encoding via row.toBytes()]
    /// if !isLeaf:
    ///   [2B child count (UInt16)]
    ///   per child:
    ///     [16B UUID (big-endian)]
    /// ```
    public func serialize() throws -> Data {
        var buf = Data()

        // Magic
        buf.append(contentsOf: btnMagic)

        // Flags
        var flags: UInt8 = 0
        if isLeaf { flags |= 1 }
        buf.append(flags)

        // Node ID (UUID as 16 big-endian bytes)
        let uuid = nodeId.uuid
        withUnsafeBytes(of: uuid) { buf.append(contentsOf: $0) }

        // Key count
        buf.appendUInt16(UInt16(keys.count))

        // Keys
        for key in keys {
            Row.encodeDBValue(key, into: &buf)
        }

        // Values (same count as keys)
        for value in values {
            let rowBytes = value.toBytes()
            buf.appendUInt32(UInt32(rowBytes.count))
            buf.append(rowBytes)
        }

        // Children (only for non-leaf nodes)
        if !isLeaf, let children = children {
            buf.appendUInt16(UInt16(children.count))
            for child in children {
                let cuuid = child.uuid
                withUnsafeBytes(of: cuuid) { buf.append(contentsOf: $0) }
            }
        }

        return buf
    }

    /// Deserialize from binary data. Falls back to JSON for backward compatibility.
    public static func deserialize(from data: Data) throws -> BTreeNode {
        // Check for binary magic
        if data.count >= 4,
           data[data.startIndex] == btnMagic[0],
           data[data.startIndex + 1] == btnMagic[1],
           data[data.startIndex + 2] == btnMagic[2],
           data[data.startIndex + 3] == btnMagic[3] {
            return try deserializeBinary(from: data)
        }

        // Fallback: JSON decode for backward compatibility
        let decoder = JSONDecoder()
        decoder.nonConformingFloatDecodingStrategy = .convertFromString(positiveInfinity: "+inf", negativeInfinity: "-inf", nan: "nan")
        return try decoder.decode(BTreeNode.self, from: data)
    }

    private static func deserializeBinary(from data: Data) throws -> BTreeNode {
        var offset = 4 // skip magic

        // Flags
        guard offset < data.count else { throw BTreeNodeError.malformedBinary }
        let flags = data[offset]
        offset += 1
        let isLeaf = (flags & 1) != 0

        // Node ID
        guard offset + 16 <= data.count else { throw BTreeNodeError.malformedBinary }
        let uuid = data.withUnsafeBytes { buf -> uuid_t in
            buf.loadUnaligned(fromByteOffset: offset, as: uuid_t.self)
        }
        let nodeId = UUID(uuid: uuid)
        offset += 16

        // Key count
        guard let keyCount = data.readUInt16(at: &offset) else { throw BTreeNodeError.malformedBinary }

        // Keys
        var keys = [DBValue]()
        keys.reserveCapacity(Int(keyCount))
        for _ in 0..<keyCount {
            guard let key = Row.decodeDBValue(from: data, at: &offset) else {
                throw BTreeNodeError.malformedBinary
            }
            keys.append(key)
        }

        // Values
        var values = [Row]()
        values.reserveCapacity(Int(keyCount))
        for _ in 0..<keyCount {
            guard let rowLen = data.readUInt32(at: &offset) else { throw BTreeNodeError.malformedBinary }
            let rowEnd = offset + Int(rowLen)
            guard rowEnd <= data.count else { throw BTreeNodeError.malformedBinary }
            let rowData = data.subdata(in: offset..<rowEnd)
            guard let row = Row.fromBytes(rowData) else { throw BTreeNodeError.malformedBinary }
            values.append(row)
            offset = rowEnd
        }

        // Children
        var children: [UUID]? = nil
        if !isLeaf {
            guard let childCount = data.readUInt16(at: &offset) else { throw BTreeNodeError.malformedBinary }
            var childList = [UUID]()
            childList.reserveCapacity(Int(childCount))
            for _ in 0..<childCount {
                guard offset + 16 <= data.count else { throw BTreeNodeError.malformedBinary }
                let cuuid = data.withUnsafeBytes { buf -> uuid_t in
                    buf.loadUnaligned(fromByteOffset: offset, as: uuid_t.self)
                }
                childList.append(UUID(uuid: cuuid))
                offset += 16
            }
            children = childList
        }

        return BTreeNode(nodeId: nodeId, isLeaf: isLeaf, keys: keys, values: values, children: children)
    }
}

enum BTreeNodeError: Error {
    case malformedBinary
}
