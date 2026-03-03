import Foundation

/// A database record storing an ID and raw binary data.
/// Supports overflow for records larger than a single page.
///
/// Normal record data format: `[0x00 flag][raw data bytes]`
/// Overflow record data format: `[0x01 flag][4B total length][4B first overflow pageID][inline data bytes]`
public struct Record: Sendable {
    public var id: UInt64
    public var data: Data

    /// If set, the record's full data spans overflow pages starting at this page ID
    public var overflowPageID: Int?

    public init(id: UInt64, data: Data, overflowPageID: Int? = nil) {
        self.id = id
        self.data = data
        self.overflowPageID = overflowPageID
    }

    /// Pre-computed serialized size: avoids calling serialize() just to measure.
    /// Non-overflow: 8B id + 4B length + data.count
    /// Overflow: 8B id + 4B length + 1B flag + 4B totalLen + 4B overflowPageID + data.count
    public var serializedSize: Int {
        if overflowPageID != nil {
            return 12 + 9 + data.count
        }
        return 12 + data.count
    }

    /// Serialize the record into binary format: [id: 8 bytes][length: 4 bytes][data]
    /// For overflow records, data is prefixed with overflow header.
    public func serialize() -> Data {
        let payload: Data
        if let overflowPage = overflowPageID {
            // Overflow record: [0x01][4B total length][4B overflow pageID][inline data]
            var buf = Data(capacity: 9 + data.count)
            buf.append(0x01)
            var totalLen = UInt32(data.count)
            withUnsafeBytes(of: &totalLen) { buf.append(contentsOf: $0) }
            var opid = Int32(overflowPage)
            withUnsafeBytes(of: &opid) { buf.append(contentsOf: $0) }
            buf.append(data)
            payload = buf
        } else {
            payload = data
        }

        var recordData = Data(capacity: 12 + payload.count)
        var idCopy = id
        withUnsafeBytes(of: &idCopy) { recordData.append(contentsOf: $0) }
        var dataLength = UInt32(payload.count)
        withUnsafeBytes(of: &dataLength) { recordData.append(contentsOf: $0) }
        recordData.append(payload)
        return recordData
    }

    /// Deserialize a record from binary data
    public static func deserialize(from rawData: Data) -> Record? {
        var position = 0

        guard rawData.count >= 12 else { return nil } // 8 (id) + 4 (length)

        let id = rawData.withUnsafeBytes { $0.loadUnaligned(fromByteOffset: position, as: UInt64.self) }
        position += 8

        let dataLength = rawData.withUnsafeBytes { Int($0.loadUnaligned(fromByteOffset: position, as: UInt32.self)) }
        position += 4

        guard rawData.count >= position + dataLength else { return nil }
        let payload = rawData.subdata(in: position..<(position + dataLength))

        // Check for overflow flag
        if !payload.isEmpty && payload[payload.startIndex] == 0x01 && payload.count >= 9 {
            // Overflow record
            var off = 1
            let totalLen = payload.withUnsafeBytes { $0.loadUnaligned(fromByteOffset: off, as: UInt32.self) }
            off += 4
            let overflowPageID = Int(payload.withUnsafeBytes { $0.loadUnaligned(fromByteOffset: off, as: Int32.self) })
            off += 4
            let inlineData = payload.subdata(in: (payload.startIndex + off)..<payload.endIndex)
            _ = totalLen  // totalLen includes inline + overflow data; used during reassembly
            return Record(id: id, data: inlineData, overflowPageID: overflowPageID)
        }

        return Record(id: id, data: payload)
    }

    /// Serialize directly into a mutable buffer at the given offset, avoiding intermediate Data allocation.
    public func serializeInto(_ buf: UnsafeMutableRawBufferPointer, at offset: Int) {
        buf.storeBytes(of: id, toByteOffset: offset, as: UInt64.self)
        if let overflowPage = overflowPageID {
            let payloadLen = UInt32(9 + data.count)
            buf.storeBytes(of: payloadLen, toByteOffset: offset + 8, as: UInt32.self)
            var pos = offset + 12
            buf[pos] = 0x01; pos += 1
            buf.storeBytes(of: UInt32(data.count), toByteOffset: pos, as: UInt32.self); pos += 4
            buf.storeBytes(of: Int32(overflowPage), toByteOffset: pos, as: Int32.self); pos += 4
            data.copyBytes(to: buf.baseAddress!.advanced(by: pos).assumingMemoryBound(to: UInt8.self),
                           count: data.count)
        } else {
            buf.storeBytes(of: UInt32(data.count), toByteOffset: offset + 8, as: UInt32.self)
            data.copyBytes(to: buf.baseAddress!.advanced(by: offset + 12).assumingMemoryBound(to: UInt8.self),
                           count: data.count)
        }
    }

    /// Whether this record has overflow pages that need to be followed
    public var isOverflow: Bool {
        overflowPageID != nil
    }
}
