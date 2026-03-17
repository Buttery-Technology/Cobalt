// TypeEncoding.swift — DBValue ↔ PostgreSQL type OIDs + text format
import CobaltCore
import Foundation

public struct TypeEncoding: Sendable {
    // PostgreSQL OIDs
    public static let boolOID: Int32 = 16
    public static let int8OID: Int32 = 20
    public static let float8OID: Int32 = 701
    public static let textOID: Int32 = 25
    public static let byteaOID: Int32 = 17
    public static let nullOID: Int32 = 0

    /// Map a DBValue to its corresponding PostgreSQL type OID
    public static func oidForDBValue(_ value: DBValue) -> Int32 {
        switch value {
        case .null:
            return nullOID
        case .integer:
            return int8OID
        case .double:
            return float8OID
        case .string:
            return textOID
        case .blob:
            return byteaOID
        case .boolean:
            return boolOID
        case .compound:
            return textOID
        }
    }

    /// Encode a DBValue as a PostgreSQL text-format string
    public static func encodeText(_ value: DBValue) -> String {
        switch value {
        case .null:
            return ""
        case .integer(let v):
            return String(v)
        case .double(let v):
            return String(v)
        case .string(let v):
            return v
        case .blob(let data):
            // Hex-encoded bytea format
            return "\\x" + data.map { String(format: "%02x", $0) }.joined()
        case .boolean(let v):
            return v ? "t" : "f"
        case .compound(let values):
            let inner = values.map { encodeText($0) }.joined(separator: ",")
            return "(\(inner))"
        }
    }

    /// Decode a PostgreSQL text-format string back to a DBValue given the type OID
    public static func decodeText(_ text: String, oid: Int32) -> DBValue {
        switch oid {
        case boolOID:
            let lower = text.lowercased()
            return .boolean(lower == "t" || lower == "true" || lower == "1")
        case int8OID:
            if let v = Int64(text) {
                return .integer(v)
            }
            return .string(text)
        case float8OID:
            if let v = Double(text) {
                return .double(v)
            }
            return .string(text)
        case byteaOID:
            if text.hasPrefix("\\x") {
                let hex = String(text.dropFirst(2))
                var data = Data()
                var i = hex.startIndex
                while i < hex.endIndex {
                    let next = hex.index(i, offsetBy: 2, limitedBy: hex.endIndex) ?? hex.endIndex
                    if let byte = UInt8(hex[i..<next], radix: 16) {
                        data.append(byte)
                    }
                    i = next
                }
                return .blob(data)
            }
            return .blob(Data(text.utf8))
        case textOID:
            return .string(text)
        default:
            return .string(text)
        }
    }
}
