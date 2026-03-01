import Foundation
import PantryCore

// MARK: - DBValueEncoder

/// Encodes a Codable struct into [String: DBValue] without going through JSON.
/// Preserves exact Int vs Double distinction.
internal struct DBValueEncoder {
    /// Encode a model instance to a dictionary of column name → DBValue.
    static func encode<T: Encodable>(_ value: T) throws -> [String: DBValue] {
        let encoder = _Encoder()
        try value.encode(to: encoder)
        return encoder.container.values
    }

    /// Derive a PantryTableSchema from a model instance.
    /// Uses the encoded values to determine column types, and Mirror to detect Optional properties.
    /// Nil optionals are included via Mirror since encodeIfPresent skips them.
    static func deriveSchema<M: PantryModel>(from instance: M) throws -> PantryTableSchema {
        let values = try encode(instance)
        let optionalInfo = Self.optionalPropertyInfo(of: instance)

        var columns: [PantryColumn] = [
            PantryColumn(name: "id", type: .string, isPrimaryKey: true, isNullable: false)
        ]

        // Merge encoded keys with optional keys (nil optionals won't appear in encoded output)
        var allKeys = Set(values.keys)
        allKeys.formUnion(optionalInfo.names)
        allKeys.remove("id")

        for key in allKeys.sorted() {
            let colType: PantryColumnType
            if let dbValue = values[key] {
                colType = Self.columnType(for: dbValue)
            } else {
                colType = optionalInfo.types[key] ?? .string
            }
            let isNullable = optionalInfo.names.contains(key)
            columns.append(PantryColumn(name: key, type: colType, isNullable: isNullable))
        }

        return PantryTableSchema(name: M.tableName, columns: columns)
    }

    private static func columnType(for value: DBValue) -> PantryColumnType {
        switch value {
        case .integer: return .integer
        case .double: return .double
        case .string: return .string
        case .blob: return .blob
        case .boolean: return .boolean
        case .null: return .string // Default nullable columns to string
        }
    }

    /// Use Mirror to find Optional property names and infer their column types.
    /// This is needed because encodeIfPresent skips nil values entirely.
    private static func optionalPropertyInfo(of value: Any) -> (names: Set<String>, types: [String: PantryColumnType]) {
        let mirror = Mirror(reflecting: value)
        var names = Set<String>()
        var types = [String: PantryColumnType]()
        for child in mirror.children {
            guard let label = child.label else { continue }
            if isOptionalValue(child.value) {
                names.insert(label)
                if let convertibleType = type(of: child.value) as? any DBValueConvertible.Type {
                    types[label] = convertibleType.columnType
                }
            }
        }
        return (names, types)
    }

    private static func isOptionalValue(_ value: Any) -> Bool {
        let mirror = Mirror(reflecting: value)
        return mirror.displayStyle == .optional
    }
}

// MARK: - Internal Encoder Types

private final class _EncoderContainer {
    var values: [String: DBValue] = [:]
}

private struct _Encoder: Encoder {
    let container = _EncoderContainer()
    var codingPath: [CodingKey] = []
    var userInfo: [CodingUserInfoKey: Any] = [:]

    func container<Key: CodingKey>(keyedBy type: Key.Type) -> KeyedEncodingContainer<Key> {
        KeyedEncodingContainer(_KeyedContainer<Key>(container: container, codingPath: codingPath))
    }

    func unkeyedContainer() -> UnkeyedEncodingContainer {
        _UnkeyedContainer(container: container, codingPath: codingPath)
    }

    func singleValueContainer() -> SingleValueEncodingContainer {
        _SingleValueContainer(container: container, codingPath: codingPath)
    }
}

private struct _KeyedContainer<Key: CodingKey>: KeyedEncodingContainerProtocol {
    let container: _EncoderContainer
    var codingPath: [CodingKey]

    mutating func encodeNil(forKey key: Key) throws {
        container.values[key.stringValue] = .null
    }

    mutating func encode(_ value: Bool, forKey key: Key) throws {
        container.values[key.stringValue] = .boolean(value)
    }

    mutating func encode(_ value: String, forKey key: Key) throws {
        container.values[key.stringValue] = .string(value)
    }

    mutating func encode(_ value: Double, forKey key: Key) throws {
        container.values[key.stringValue] = .double(value)
    }

    mutating func encode(_ value: Float, forKey key: Key) throws {
        container.values[key.stringValue] = .double(Double(value))
    }

    mutating func encode(_ value: Int, forKey key: Key) throws {
        container.values[key.stringValue] = .integer(Int64(value))
    }

    mutating func encode(_ value: Int8, forKey key: Key) throws {
        container.values[key.stringValue] = .integer(Int64(value))
    }

    mutating func encode(_ value: Int16, forKey key: Key) throws {
        container.values[key.stringValue] = .integer(Int64(value))
    }

    mutating func encode(_ value: Int32, forKey key: Key) throws {
        container.values[key.stringValue] = .integer(Int64(value))
    }

    mutating func encode(_ value: Int64, forKey key: Key) throws {
        container.values[key.stringValue] = .integer(value)
    }

    mutating func encode(_ value: UInt, forKey key: Key) throws {
        container.values[key.stringValue] = .integer(Int64(value))
    }

    mutating func encode(_ value: UInt8, forKey key: Key) throws {
        container.values[key.stringValue] = .integer(Int64(value))
    }

    mutating func encode(_ value: UInt16, forKey key: Key) throws {
        container.values[key.stringValue] = .integer(Int64(value))
    }

    mutating func encode(_ value: UInt32, forKey key: Key) throws {
        container.values[key.stringValue] = .integer(Int64(value))
    }

    mutating func encode(_ value: UInt64, forKey key: Key) throws {
        container.values[key.stringValue] = .integer(Int64(value))
    }

    mutating func encode<T: Encodable>(_ value: T, forKey key: Key) throws {
        // Handle known types that have DBValueConvertible conformance
        if let convertible = value as? DBValueConvertible {
            container.values[key.stringValue] = convertible.toDBValue()
            return
        }
        // Fallback: encode as JSON string for nested/complex types
        let data = try JSONEncoder().encode(value)
        let json = String(data: data, encoding: .utf8) ?? "{}"
        container.values[key.stringValue] = .string(json)
    }

    mutating func nestedContainer<NestedKey: CodingKey>(keyedBy keyType: NestedKey.Type, forKey key: Key) -> KeyedEncodingContainer<NestedKey> {
        // For nested containers, encode as JSON fallback
        KeyedEncodingContainer(_KeyedContainer<NestedKey>(container: container, codingPath: codingPath + [key]))
    }

    mutating func nestedUnkeyedContainer(forKey key: Key) -> UnkeyedEncodingContainer {
        _UnkeyedContainer(container: container, codingPath: codingPath + [key])
    }

    mutating func superEncoder() -> Encoder {
        _Encoder()
    }

    mutating func superEncoder(forKey key: Key) -> Encoder {
        _Encoder()
    }
}

private struct _UnkeyedContainer: UnkeyedEncodingContainer {
    let container: _EncoderContainer
    var codingPath: [CodingKey]
    var count: Int = 0

    mutating func encode<T: Encodable>(_ value: T) throws {
        // Unkeyed containers shouldn't appear at top level for flat models
    }

    mutating func encodeNil() throws {}
    mutating func encode(_ value: Bool) throws { count += 1 }
    mutating func encode(_ value: String) throws { count += 1 }
    mutating func encode(_ value: Double) throws { count += 1 }
    mutating func encode(_ value: Float) throws { count += 1 }
    mutating func encode(_ value: Int) throws { count += 1 }
    mutating func encode(_ value: Int8) throws { count += 1 }
    mutating func encode(_ value: Int16) throws { count += 1 }
    mutating func encode(_ value: Int32) throws { count += 1 }
    mutating func encode(_ value: Int64) throws { count += 1 }
    mutating func encode(_ value: UInt) throws { count += 1 }
    mutating func encode(_ value: UInt8) throws { count += 1 }
    mutating func encode(_ value: UInt16) throws { count += 1 }
    mutating func encode(_ value: UInt32) throws { count += 1 }
    mutating func encode(_ value: UInt64) throws { count += 1 }

    mutating func nestedContainer<NestedKey: CodingKey>(keyedBy keyType: NestedKey.Type) -> KeyedEncodingContainer<NestedKey> {
        KeyedEncodingContainer(_KeyedContainer<NestedKey>(container: container, codingPath: codingPath))
    }

    mutating func nestedUnkeyedContainer() -> UnkeyedEncodingContainer {
        _UnkeyedContainer(container: container, codingPath: codingPath)
    }

    mutating func superEncoder() -> Encoder {
        _Encoder()
    }
}

private struct _SingleValueContainer: SingleValueEncodingContainer {
    let container: _EncoderContainer
    var codingPath: [CodingKey]

    mutating func encodeNil() throws {}
    mutating func encode(_ value: Bool) throws {}
    mutating func encode(_ value: String) throws {}
    mutating func encode(_ value: Double) throws {}
    mutating func encode(_ value: Float) throws {}
    mutating func encode(_ value: Int) throws {}
    mutating func encode(_ value: Int8) throws {}
    mutating func encode(_ value: Int16) throws {}
    mutating func encode(_ value: Int32) throws {}
    mutating func encode(_ value: Int64) throws {}
    mutating func encode(_ value: UInt) throws {}
    mutating func encode(_ value: UInt8) throws {}
    mutating func encode(_ value: UInt16) throws {}
    mutating func encode(_ value: UInt32) throws {}
    mutating func encode(_ value: UInt64) throws {}
    mutating func encode<T: Encodable>(_ value: T) throws {}
}

// MARK: - DBValueDecoder

/// Decodes a [String: DBValue] dictionary into a Codable struct.
internal struct DBValueDecoder {
    static func decode<T: Decodable>(_ type: T.Type, from values: [String: DBValue]) throws -> T {
        let decoder = _ValueDecoder(values: values)
        return try T(from: decoder)
    }
}

private struct _ValueDecoder: Decoder {
    let values: [String: DBValue]
    var codingPath: [CodingKey] = []
    var userInfo: [CodingUserInfoKey: Any] = [:]

    func container<Key: CodingKey>(keyedBy type: Key.Type) throws -> KeyedDecodingContainer<Key> {
        KeyedDecodingContainer(_KeyedDecodingContainer<Key>(values: values, codingPath: codingPath))
    }

    func unkeyedContainer() throws -> UnkeyedDecodingContainer {
        throw DecodingError.typeMismatch([Any].self, .init(codingPath: codingPath, debugDescription: "Unkeyed container not supported at top level"))
    }

    func singleValueContainer() throws -> SingleValueDecodingContainer {
        throw DecodingError.typeMismatch(Any.self, .init(codingPath: codingPath, debugDescription: "Single value container not supported at top level"))
    }
}

private struct _CodingKey: CodingKey {
    var stringValue: String
    var intValue: Int?
    init(stringValue: String) { self.stringValue = stringValue; self.intValue = nil }
    init?(intValue: Int) { self.stringValue = "\(intValue)"; self.intValue = intValue }
}

private struct _KeyedDecodingContainer<Key: CodingKey>: KeyedDecodingContainerProtocol {
    let values: [String: DBValue]
    var codingPath: [CodingKey]
    var allKeys: [Key] { values.keys.compactMap { Key(stringValue: $0) } }

    func contains(_ key: Key) -> Bool {
        values[key.stringValue] != nil
    }

    func decodeNil(forKey key: Key) throws -> Bool {
        guard let value = values[key.stringValue] else { return true }
        return value == .null
    }

    func decode(_ type: Bool.Type, forKey key: Key) throws -> Bool {
        guard let value = values[key.stringValue] else {
            throw decodingError(type, key)
        }
        if case .boolean(let v) = value { return v }
        throw decodingError(type, key)
    }

    func decode(_ type: String.Type, forKey key: Key) throws -> String {
        guard let value = values[key.stringValue] else {
            throw decodingError(type, key)
        }
        if case .string(let v) = value { return v }
        throw decodingError(type, key)
    }

    func decode(_ type: Double.Type, forKey key: Key) throws -> Double {
        guard let value = values[key.stringValue] else {
            throw decodingError(type, key)
        }
        switch value {
        case .double(let v): return v
        case .integer(let v): return Double(v)
        default: throw decodingError(type, key)
        }
    }

    func decode(_ type: Float.Type, forKey key: Key) throws -> Float {
        guard let value = values[key.stringValue] else {
            throw decodingError(type, key)
        }
        switch value {
        case .double(let v): return Float(v)
        case .integer(let v): return Float(v)
        default: throw decodingError(type, key)
        }
    }

    func decode(_ type: Int.Type, forKey key: Key) throws -> Int {
        try decodeInteger(type, forKey: key)
    }

    func decode(_ type: Int8.Type, forKey key: Key) throws -> Int8 {
        try decodeInteger(type, forKey: key)
    }

    func decode(_ type: Int16.Type, forKey key: Key) throws -> Int16 {
        try decodeInteger(type, forKey: key)
    }

    func decode(_ type: Int32.Type, forKey key: Key) throws -> Int32 {
        try decodeInteger(type, forKey: key)
    }

    func decode(_ type: Int64.Type, forKey key: Key) throws -> Int64 {
        try decodeInteger(type, forKey: key)
    }

    func decode(_ type: UInt.Type, forKey key: Key) throws -> UInt {
        try decodeInteger(type, forKey: key)
    }

    func decode(_ type: UInt8.Type, forKey key: Key) throws -> UInt8 {
        try decodeInteger(type, forKey: key)
    }

    func decode(_ type: UInt16.Type, forKey key: Key) throws -> UInt16 {
        try decodeInteger(type, forKey: key)
    }

    func decode(_ type: UInt32.Type, forKey key: Key) throws -> UInt32 {
        try decodeInteger(type, forKey: key)
    }

    func decode(_ type: UInt64.Type, forKey key: Key) throws -> UInt64 {
        try decodeInteger(type, forKey: key)
    }

    func decode<T: Decodable>(_ type: T.Type, forKey key: Key) throws -> T {
        // Handle DBValueConvertible types first
        if let convertibleType = T.self as? any DBValueConvertible.Type {
            guard let value = values[key.stringValue] else {
                throw decodingError(type, key)
            }
            if let result = convertibleType.fromDBValue(value) as? T {
                return result
            }
            throw decodingError(type, key)
        }

        // Nested Codable: try to decode from JSON string
        guard let value = values[key.stringValue] else {
            throw decodingError(type, key)
        }
        if case .string(let json) = value, let data = json.data(using: .utf8) {
            return try JSONDecoder().decode(T.self, from: data)
        }
        throw decodingError(type, key)
    }

    func nestedContainer<NestedKey: CodingKey>(keyedBy type: NestedKey.Type, forKey key: Key) throws -> KeyedDecodingContainer<NestedKey> {
        throw DecodingError.typeMismatch([String: Any].self, .init(codingPath: codingPath + [key], debugDescription: "Nested keyed containers not directly supported"))
    }

    func nestedUnkeyedContainer(forKey key: Key) throws -> UnkeyedDecodingContainer {
        throw DecodingError.typeMismatch([Any].self, .init(codingPath: codingPath + [key], debugDescription: "Nested unkeyed containers not directly supported"))
    }

    func superDecoder() throws -> Decoder {
        _ValueDecoder(values: values, codingPath: codingPath)
    }

    func superDecoder(forKey key: Key) throws -> Decoder {
        _ValueDecoder(values: values, codingPath: codingPath + [key])
    }

    // MARK: - Helpers

    private func decodeInteger<I: FixedWidthInteger>(_ type: I.Type, forKey key: Key) throws -> I {
        guard let value = values[key.stringValue] else {
            throw decodingError(type, key)
        }
        switch value {
        case .integer(let v):
            guard let result = I(exactly: v) else { throw decodingError(type, key) }
            return result
        case .double(let v):
            guard let i64 = Int64(exactly: v), let result = I(exactly: i64) else {
                throw decodingError(type, key)
            }
            return result
        default:
            throw decodingError(type, key)
        }
    }

    private func decodingError(_ type: Any.Type, _ key: Key) -> DecodingError {
        DecodingError.typeMismatch(type, .init(
            codingPath: codingPath + [key],
            debugDescription: "Cannot decode \(type) for key '\(key.stringValue)' from \(values[key.stringValue] as Any)"
        ))
    }
}
