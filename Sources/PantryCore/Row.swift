import Foundation

/// Represents a row in the database
public struct Row: Codable, Sendable, Hashable, Equatable {
    public let values: [String: DBValue]

    public init(values: [String: DBValue]) {
        self.values = values
    }

    public subscript(column: String) -> DBValue? {
        values[column]
    }
}

// MARK: - Typed Convenience Getters

extension Row {
    public func string(_ key: String) -> String? {
        guard case .string(let v) = values[key] else { return nil }
        return v
    }

    public func integer(_ key: String) -> Int64? {
        guard case .integer(let v) = values[key] else { return nil }
        return v
    }

    public func int(_ key: String) -> Int? {
        guard case .integer(let v) = values[key] else { return nil }
        return Int(v)
    }

    public func double(_ key: String) -> Double? {
        guard case .double(let v) = values[key] else { return nil }
        return v
    }

    public func bool(_ key: String) -> Bool? {
        guard case .boolean(let v) = values[key] else { return nil }
        return v
    }

    public func blob(_ key: String) -> Data? {
        guard case .blob(let v) = values[key] else { return nil }
        return v
    }
}

// MARK: - Binary Encoding

extension Row {
    /// Encode this row into a compact binary format.
    ///
    /// Format:
    /// ```
    /// [2B column count (UInt16)]
    /// per column:
    ///   [2B name length (UInt16)][N bytes name UTF-8]
    ///   [1B type tag]
    ///   value payload (tag-dependent)
    /// ```
    public func toBytes() -> Data {
        var buf = Data()
        let cols = values
        buf.appendUInt16(UInt16(cols.count))
        for (name, value) in cols {
            let nameBytes = Array(name.utf8)
            buf.appendUInt16(UInt16(nameBytes.count))
            buf.append(contentsOf: nameBytes)
            Row.encodeDBValue(value, into: &buf)
        }
        return buf
    }

    /// Decode a row from compact binary format. Returns nil on malformed data.
    public static func fromBytes(_ data: Data) -> Row? {
        var offset = 0
        guard let colCount = data.readUInt16(at: &offset) else { return nil }
        var values = [String: DBValue]()
        values.reserveCapacity(Int(colCount))
        for _ in 0..<colCount {
            guard let nameLen = data.readUInt16(at: &offset) else { return nil }
            let nameEnd = offset + Int(nameLen)
            guard nameEnd <= data.count else { return nil }
            guard let name = String(bytes: data[offset..<nameEnd], encoding: .utf8) else { return nil }
            offset = nameEnd
            guard let value = decodeDBValue(from: data, at: &offset) else { return nil }
            values[name] = value
        }
        return Row(values: values)
    }

    /// Extract a single column value from binary-encoded row data without allocating a full Row.
    /// Scans the binary format sequentially, skipping columns that don't match.
    public static func columnValue(named column: String, from data: Data) -> DBValue? {
        var offset = 0
        guard let colCount = data.readUInt16(at: &offset) else { return nil }
        let columnUTF8 = Array(column.utf8)
        for _ in 0..<colCount {
            guard let nameLen = data.readUInt16(at: &offset) else { return nil }
            let nameEnd = offset + Int(nameLen)
            guard nameEnd <= data.count else { return nil }
            // Compare UTF-8 bytes directly to avoid String allocation
            let matches = nameLen == columnUTF8.count && data[offset..<nameEnd].elementsEqual(columnUTF8)
            offset = nameEnd
            if matches {
                return decodeDBValue(from: data, at: &offset)
            } else {
                // Skip the value without decoding it fully
                guard skipDBValue(in: data, at: &offset) else { return nil }
            }
        }
        return nil
    }

    /// Skip past a DBValue in binary data without decoding it.
    private static func skipDBValue(in data: Data, at offset: inout Int) -> Bool {
        guard offset < data.count else { return false }
        let tag = data[offset]
        offset += 1
        switch tag {
        case 0: // null
            return true
        case 1: // integer
            guard offset + 8 <= data.count else { return false }
            offset += 8
            return true
        case 2: // double
            guard offset + 8 <= data.count else { return false }
            offset += 8
            return true
        case 3: // string
            guard let len = data.readUInt32(at: &offset) else { return false }
            let end = offset + Int(len)
            guard end <= data.count else { return false }
            offset = end
            return true
        case 4: // blob
            guard let len = data.readUInt32(at: &offset) else { return false }
            let end = offset + Int(len)
            guard end <= data.count else { return false }
            offset = end
            return true
        case 5: // boolean
            guard offset < data.count else { return false }
            offset += 1
            return true
        case 6: // compound
            guard let count = data.readUInt16(at: &offset) else { return false }
            for _ in 0..<count {
                guard skipDBValue(in: data, at: &offset) else { return false }
            }
            return true
        default:
            return false
        }
    }

    // MARK: - DBValue binary helpers

    public static func encodeDBValue(_ value: DBValue, into buf: inout Data) {
        switch value {
        case .null:
            buf.append(0) // tag 0
        case .integer(let v):
            buf.append(1) // tag 1
            buf.appendInt64(v)
        case .double(let v):
            buf.append(2) // tag 2
            buf.appendFloat64(v)
        case .string(let v):
            buf.append(3) // tag 3
            let bytes = Array(v.utf8)
            buf.appendUInt32(UInt32(bytes.count))
            buf.append(contentsOf: bytes)
        case .blob(let v):
            buf.append(4) // tag 4
            buf.appendUInt32(UInt32(v.count))
            buf.append(v)
        case .boolean(let v):
            buf.append(5) // tag 5
            buf.append(v ? 1 : 0)
        case .compound(let items):
            buf.append(6) // tag 6
            buf.appendUInt16(UInt16(items.count))
            for item in items {
                encodeDBValue(item, into: &buf)
            }
        }
    }

    public static func decodeDBValue(from data: Data, at offset: inout Int) -> DBValue? {
        guard offset < data.count else { return nil }
        let tag = data[offset]
        offset += 1
        switch tag {
        case 0: // null
            return .null
        case 1: // integer
            guard let v = data.readInt64(at: &offset) else { return nil }
            return .integer(v)
        case 2: // double
            guard let v = data.readFloat64(at: &offset) else { return nil }
            return .double(v)
        case 3: // string
            guard let len = data.readUInt32(at: &offset) else { return nil }
            let end = offset + Int(len)
            guard end <= data.count else { return nil }
            guard let s = String(bytes: data[offset..<end], encoding: .utf8) else { return nil }
            offset = end
            return .string(s)
        case 4: // blob
            guard let len = data.readUInt32(at: &offset) else { return nil }
            let end = offset + Int(len)
            guard end <= data.count else { return nil }
            let blob = Data(data[offset..<end])
            offset = end
            return .blob(blob)
        case 5: // boolean
            guard offset < data.count else { return nil }
            let v = data[offset]
            offset += 1
            return .boolean(v != 0)
        case 6: // compound
            guard let count = data.readUInt16(at: &offset) else { return nil }
            var items = [DBValue]()
            items.reserveCapacity(Int(count))
            for _ in 0..<count {
                guard let item = decodeDBValue(from: data, at: &offset) else { return nil }
                items.append(item)
            }
            return .compound(items)
        default:
            return nil
        }
    }
}

// MARK: - Data binary read/write helpers

extension Data {
    public mutating func appendUInt16(_ value: UInt16) {
        var v = value.littleEndian
        Swift.withUnsafeBytes(of: &v) { self.append(contentsOf: $0) }
    }

    public mutating func appendUInt32(_ value: UInt32) {
        var v = value.littleEndian
        Swift.withUnsafeBytes(of: &v) { self.append(contentsOf: $0) }
    }

    public mutating func appendInt64(_ value: Int64) {
        var v = value.littleEndian
        Swift.withUnsafeBytes(of: &v) { self.append(contentsOf: $0) }
    }

    public mutating func appendFloat64(_ value: Double) {
        var v = value.bitPattern.littleEndian
        Swift.withUnsafeBytes(of: &v) { self.append(contentsOf: $0) }
    }

    public func readUInt16(at offset: inout Int) -> UInt16? {
        guard offset + 2 <= count else { return nil }
        let value = self.withUnsafeBytes { $0.loadUnaligned(fromByteOffset: offset, as: UInt16.self) }
        offset += 2
        return UInt16(littleEndian: value)
    }

    public func readUInt32(at offset: inout Int) -> UInt32? {
        guard offset + 4 <= count else { return nil }
        let value = self.withUnsafeBytes { $0.loadUnaligned(fromByteOffset: offset, as: UInt32.self) }
        offset += 4
        return UInt32(littleEndian: value)
    }

    public func readInt64(at offset: inout Int) -> Int64? {
        guard offset + 8 <= count else { return nil }
        let value = self.withUnsafeBytes { $0.loadUnaligned(fromByteOffset: offset, as: Int64.self) }
        offset += 8
        return Int64(littleEndian: value)
    }

    public func readFloat64(at offset: inout Int) -> Double? {
        guard offset + 8 <= count else { return nil }
        let bits = self.withUnsafeBytes { $0.loadUnaligned(fromByteOffset: offset, as: UInt64.self) }
        offset += 8
        return Double(bitPattern: UInt64(littleEndian: bits))
    }
}
