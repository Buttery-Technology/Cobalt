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
    /// Self-describing format (used when no schema is available):
    /// ```
    /// [2B column count (UInt16)]
    /// per column:
    ///   [2B name length (UInt16)][N bytes name UTF-8]
    ///   [1B type tag]
    ///   value payload (tag-dependent)
    /// ```
    public func toBytes() -> Data {
        // Pre-allocate: 2B col count + ~32B per column (2B nameLen + ~8B name + ~20B value)
        var buf = Data(capacity: 2 + values.count * 32)
        let cols = values
        buf.appendUInt16(UInt16(cols.count))
        for (name, value) in cols {
            let nameUTF8 = name.utf8
            buf.appendUInt16(UInt16(nameUTF8.count))
            buf.append(contentsOf: nameUTF8)
            Row.encodeDBValue(value, into: &buf)
        }
        return buf
    }

    /// Schema-based positional encoding with NULL bitmap and column offset table.
    /// Format v3:
    /// ```
    /// [1B magic 0xFF][1B version 0x03]
    /// [2B column count (UInt16)]
    /// [ceil(colCount/8) bytes NULL bitmap — bit=1 means NULL]
    /// [2B * colCount offset table — byte offset from values start, 0xFFFF for NULL]
    /// per non-NULL column (in schema order):
    ///   [1B type tag][value payload]
    /// ```
    /// The offset table enables O(1) random column access.
    public func toBytesPositional(schema: PantryTableSchema) -> Data {
        let colCount = schema.columns.count
        let bitmapBytes = (colCount + 7) / 8
        let offsetTableSize = colCount * 2
        var buf = Data(capacity: 4 + bitmapBytes + offsetTableSize + colCount * 10)
        buf.append(0xFF) // magic
        buf.append(0x03) // version 3: NULL bitmap + offset table
        buf.appendUInt16(UInt16(colCount))

        // Build NULL bitmap
        var bitmap = [UInt8](repeating: 0, count: bitmapBytes)
        for (i, col) in schema.columns.enumerated() {
            let value = values[col.name] ?? .null
            if value == .null {
                bitmap[i / 8] |= (1 << (i % 8))
            }
        }
        buf.append(contentsOf: bitmap)

        // Reserve space for offset table (fill with 0xFFFF initially)
        let offsetTableStart = buf.count
        for _ in 0..<colCount {
            buf.appendUInt16(0xFFFF)
        }
        let valuesStart = buf.count

        // Encode non-NULL values and record offsets
        for (i, col) in schema.columns.enumerated() {
            let isNull = (bitmap[i / 8] & (1 << (i % 8))) != 0
            if !isNull {
                let relOffset = UInt16(buf.count - valuesStart)
                // Write offset into the reserved table slot
                var leOffset = relOffset.littleEndian
                withUnsafeBytes(of: &leOffset) { ptr in
                    let pos = offsetTableStart + i * 2
                    buf.replaceSubrange(pos..<(pos + 2), with: ptr)
                }
                let value = values[col.name] ?? .null
                Row.encodeDBValue(value, into: &buf)
            }
        }
        return buf
    }

    /// Decode from positional format v3 (NULL bitmap + offset table) using schema for column names.
    public static func fromBytesPositionalV3(_ data: Data, schema: PantryTableSchema) -> Row? {
        guard data.count >= 4,
              data[data.startIndex] == 0xFF,
              data[data.startIndex + 1] == 0x03 else { return nil }
        var offset = 2
        guard let colCount = data.readUInt16(at: &offset) else { return nil }

        let encodedCount = Int(colCount)
        let bitmapBytes = (encodedCount + 7) / 8
        guard offset + bitmapBytes <= data.count else { return nil }
        let bitmap = Array(data[offset..<(offset + bitmapBytes)])
        offset += bitmapBytes

        // Skip offset table (2B per column)
        let offsetTableSize = encodedCount * 2
        guard offset + offsetTableSize <= data.count else { return nil }
        offset += offsetTableSize

        // Decode values sequentially (same as v2 from here)
        var values = [String: DBValue]()
        values.reserveCapacity(max(encodedCount, schema.columns.count))
        let decodableCount = min(encodedCount, schema.columns.count)
        for i in 0..<decodableCount {
            let col = schema.columns[i]
            let isNull = (bitmap[i / 8] & (1 << (i % 8))) != 0
            if isNull {
                values[col.name] = .null
            } else {
                guard let value = decodeDBValue(from: data, at: &offset) else { return nil }
                values[col.name] = value
            }
        }
        if encodedCount > schema.columns.count {
            for i in schema.columns.count..<encodedCount {
                let isNull = (bitmap[i / 8] & (1 << (i % 8))) != 0
                if !isNull {
                    guard skipDBValue(in: data, at: &offset) else { return nil }
                }
            }
        }
        if encodedCount < schema.columns.count {
            for i in encodedCount..<schema.columns.count {
                values[schema.columns[i].name] = .null
            }
        }
        return Row(values: values)
    }

    /// Decode from positional format v2 (NULL bitmap) using schema for column names.
    public static func fromBytesPositionalV2(_ data: Data, schema: PantryTableSchema) -> Row? {
        guard data.count >= 4,
              data[data.startIndex] == 0xFF,
              data[data.startIndex + 1] == 0x02 else { return nil }
        var offset = 2
        guard let colCount = data.readUInt16(at: &offset) else { return nil }

        let encodedCount = Int(colCount)
        let bitmapBytes = (encodedCount + 7) / 8
        guard offset + bitmapBytes <= data.count else { return nil }
        let bitmap = Array(data[offset..<(offset + bitmapBytes)])
        offset += bitmapBytes

        var values = [String: DBValue]()
        values.reserveCapacity(max(encodedCount, schema.columns.count))
        // Decode columns that exist in the encoded data
        let decodableCount = min(encodedCount, schema.columns.count)
        for i in 0..<decodableCount {
            let col = schema.columns[i]
            let isNull = (bitmap[i / 8] & (1 << (i % 8))) != 0
            if isNull {
                values[col.name] = .null
            } else {
                guard let value = decodeDBValue(from: data, at: &offset) else { return nil }
                values[col.name] = value
            }
        }
        // Skip extra encoded columns not in current schema (schema shrank)
        if encodedCount > schema.columns.count {
            for i in schema.columns.count..<encodedCount {
                let isNull = (bitmap[i / 8] & (1 << (i % 8))) != 0
                if !isNull {
                    guard skipDBValue(in: data, at: &offset) else { return nil }
                }
            }
        }
        // Fill missing columns with .null (schema grew via migration)
        if encodedCount < schema.columns.count {
            for i in encodedCount..<schema.columns.count {
                values[schema.columns[i].name] = .null
            }
        }
        return Row(values: values)
    }

    /// Decode from positional format v1 (no NULL bitmap) using schema for column names.
    public static func fromBytesPositionalV1(_ data: Data, schema: PantryTableSchema) -> Row? {
        guard data.count >= 4,
              data[data.startIndex] == 0xFF,
              data[data.startIndex + 1] == 0x01 else { return nil }
        var offset = 2
        guard let colCount = data.readUInt16(at: &offset) else { return nil }
        let encodedCount = Int(colCount)
        var values = [String: DBValue]()
        values.reserveCapacity(max(encodedCount, schema.columns.count))
        let decodableCount = min(encodedCount, schema.columns.count)
        for i in 0..<decodableCount {
            guard let value = decodeDBValue(from: data, at: &offset) else { return nil }
            values[schema.columns[i].name] = value
        }
        // Skip extra encoded columns not in current schema
        if encodedCount > schema.columns.count {
            for _ in schema.columns.count..<encodedCount {
                guard skipDBValue(in: data, at: &offset) else { return nil }
            }
        }
        // Fill missing columns with .null (schema grew)
        if encodedCount < schema.columns.count {
            for i in encodedCount..<schema.columns.count {
                values[schema.columns[i].name] = .null
            }
        }
        return Row(values: values)
    }

    /// Auto-detecting decode: checks magic byte, dispatches to appropriate decoder.
    public static func fromBytesAuto(_ data: Data, schema: PantryTableSchema?) -> Row? {
        if data.count >= 2, data[data.startIndex] == 0xFF {
            if let schema = schema {
                let version = data[data.startIndex + 1]
                if version == 0x03 {
                    return fromBytesPositionalV3(data, schema: schema)
                } else if version == 0x02 {
                    return fromBytesPositionalV2(data, schema: schema)
                } else if version == 0x01 {
                    return fromBytesPositionalV1(data, schema: schema)
                }
            }
        }
        return fromBytes(data)
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

    /// Decode as many complete columns as possible from potentially truncated binary data.
    /// Returns a Row with whatever columns were fully decodable.
    /// Used for overflow record lazy loading: decode columns from inline data without reading overflow pages.
    /// Returns nil only if the data is so short that not even the column count can be read.
    public static func fromBytesPartial(_ data: Data, neededColumns: Set<String>? = nil) -> Row? {
        var offset = 0
        guard let colCount = data.readUInt16(at: &offset) else { return nil }
        var values = [String: DBValue]()
        for _ in 0..<colCount {
            let savedOffset = offset
            guard let nameLen = data.readUInt16(at: &offset) else { break }
            let nameEnd = offset + Int(nameLen)
            guard nameEnd <= data.count else { offset = savedOffset; break }
            guard let name = String(bytes: data[offset..<nameEnd], encoding: .utf8) else { break }
            offset = nameEnd
            let valueOffset = offset
            guard let value = decodeDBValue(from: data, at: &offset) else { offset = valueOffset; break }
            values[name] = value
        }
        guard !values.isEmpty else { return nil }
        // Check if all needed columns were found in the inline portion
        if let needed = neededColumns, !needed.isSubset(of: Set(values.keys)) {
            return nil  // signal that overflow reassembly is required
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

    /// Extract a single column value from positional-encoded (v2/v3) binary data by column index.
    /// v3: Uses offset table for O(1) jump. v2: Skips preceding non-NULL values sequentially.
    public static func columnValuePositional(at index: Int, colCount: Int, from data: Data) -> DBValue? {
        guard data.count >= 4, data[data.startIndex] == 0xFF else { return nil }
        let version = data[data.startIndex + 1]
        guard index < colCount else { return nil }

        let bitmapBytes = (colCount + 7) / 8
        let bitmapStart = 4  // after magic(1) + version(1) + colCount(2)
        guard bitmapStart + bitmapBytes <= data.count else { return nil }

        // Check NULL bitmap for target column
        let targetByte = data[data.startIndex + bitmapStart + index / 8]
        if (targetByte & (1 << (index % 8))) != 0 {
            return .null
        }

        if version == 0x03 {
            // v3: use offset table for O(1) access
            let offsetTableStart = bitmapStart + bitmapBytes
            let offsetPos = offsetTableStart + index * 2
            guard offsetPos + 2 <= data.count else { return nil }
            let relOffset = data.withUnsafeBytes {
                UInt16(littleEndian: $0.loadUnaligned(fromByteOffset: offsetPos, as: UInt16.self))
            }
            guard relOffset != 0xFFFF else { return .null }
            let valuesStart = offsetTableStart + colCount * 2
            var offset = valuesStart + Int(relOffset)
            return decodeDBValue(from: data, at: &offset)
        }

        // v2: sequential skip
        guard version == 0x02 else { return nil }
        var offset = bitmapStart + bitmapBytes
        for i in 0..<index {
            let byteIdx = data[data.startIndex + bitmapStart + i / 8]
            let isNull = (byteIdx & (1 << (i % 8))) != 0
            if !isNull {
                guard skipDBValue(in: data, at: &offset) else { return nil }
            }
        }

        return decodeDBValue(from: data, at: &offset)
    }

    /// Skip past a DBValue in binary data without decoding it.
    public static func skipDBValue(in data: Data, at offset: inout Int) -> Bool {
        guard offset < data.count else { return false }
        let tag = data[offset]
        offset += 1
        switch tag {
        case 0: // null
            return true
        case 1: // integer (legacy 8-byte)
            guard offset + 8 <= data.count else { return false }
            offset += 8
            return true
        case 7: // integer (varint zigzag)
            while offset < data.count {
                let b = data[offset]
                offset += 1
                if b & 0x80 == 0 { return true }
            }
            return false
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
            // Varint encoding with zigzag: tag 7
            buf.append(7)
            let zigzag = UInt64(bitPattern: (v << 1) ^ (v >> 63))
            var z = zigzag
            while z >= 0x80 {
                buf.append(UInt8(z & 0x7F) | 0x80)
                z >>= 7
            }
            buf.append(UInt8(z))
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
        case 1: // integer (legacy 8-byte)
            guard let v = data.readInt64(at: &offset) else { return nil }
            return .integer(v)
        case 7: // integer (varint zigzag)
            var zigzag: UInt64 = 0
            var shift: UInt64 = 0
            while offset < data.count {
                let b = UInt64(data[offset])
                offset += 1
                zigzag |= (b & 0x7F) << shift
                if b & 0x80 == 0 { break }
                shift += 7
                if shift >= 70 { return nil } // overflow protection
            }
            let v = Int64(bitPattern: (zigzag >> 1) ^ (0 &- (zigzag & 1)))
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
