import Foundation

/// A database record storing an ID and raw binary data.
/// Callers are responsible for encoding/decoding the data payload.
public struct Record: Sendable {
    public var id: UInt64
    public var data: Data

    public init(id: UInt64, data: Data) {
        self.id = id
        self.data = data
    }

    /// Serialize the record into binary format: [id: 8 bytes][length: 4 bytes][data]
    public func serialize() -> Data {
        var recordData = Data(capacity: 12 + data.count)
        var idCopy = id
        withUnsafeBytes(of: &idCopy) { recordData.append(contentsOf: $0) }
        var dataLength = UInt32(data.count)
        withUnsafeBytes(of: &dataLength) { recordData.append(contentsOf: $0) }
        recordData.append(data)
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

        return Record(id: id, data: payload)
    }
}
