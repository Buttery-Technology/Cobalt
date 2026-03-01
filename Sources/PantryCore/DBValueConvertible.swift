import Foundation

/// Protocol bridging Swift types to/from DBValue for type-safe model properties.
public protocol DBValueConvertible: Sendable {
    /// The column type this Swift type maps to in the database schema.
    static var columnType: PantryColumnType { get }
    /// Convert this value to a DBValue for storage.
    func toDBValue() -> DBValue
    /// Convert a DBValue back to this Swift type. Returns nil if the conversion fails.
    static func fromDBValue(_ value: DBValue) -> Self?
}

// MARK: - String

extension String: DBValueConvertible {
    public static var columnType: PantryColumnType { .string }
    public func toDBValue() -> DBValue { .string(self) }
    public static func fromDBValue(_ value: DBValue) -> String? {
        if case .string(let v) = value { return v }
        return nil
    }
}

// MARK: - Int

extension Int: DBValueConvertible {
    public static var columnType: PantryColumnType { .integer }
    public func toDBValue() -> DBValue { .integer(Int64(self)) }
    public static func fromDBValue(_ value: DBValue) -> Int? {
        switch value {
        case .integer(let v): return Int(exactly: v)
        case .double(let v):
            guard let i64 = Int64(exactly: v) else { return nil }
            return Int(exactly: i64)
        default: return nil
        }
    }
}

// MARK: - Int64

extension Int64: DBValueConvertible {
    public static var columnType: PantryColumnType { .integer }
    public func toDBValue() -> DBValue { .integer(self) }
    public static func fromDBValue(_ value: DBValue) -> Int64? {
        switch value {
        case .integer(let v): return v
        case .double(let v): return Int64(exactly: v)
        default: return nil
        }
    }
}

// MARK: - Int32

extension Int32: DBValueConvertible {
    public static var columnType: PantryColumnType { .integer }
    public func toDBValue() -> DBValue { .integer(Int64(self)) }
    public static func fromDBValue(_ value: DBValue) -> Int32? {
        switch value {
        case .integer(let v): return Int32(exactly: v)
        case .double(let v):
            guard let i64 = Int64(exactly: v) else { return nil }
            return Int32(exactly: i64)
        default: return nil
        }
    }
}

// MARK: - Int16

extension Int16: DBValueConvertible {
    public static var columnType: PantryColumnType { .integer }
    public func toDBValue() -> DBValue { .integer(Int64(self)) }
    public static func fromDBValue(_ value: DBValue) -> Int16? {
        switch value {
        case .integer(let v): return Int16(exactly: v)
        case .double(let v):
            guard let i64 = Int64(exactly: v) else { return nil }
            return Int16(exactly: i64)
        default: return nil
        }
    }
}

// MARK: - Int8

extension Int8: DBValueConvertible {
    public static var columnType: PantryColumnType { .integer }
    public func toDBValue() -> DBValue { .integer(Int64(self)) }
    public static func fromDBValue(_ value: DBValue) -> Int8? {
        switch value {
        case .integer(let v): return Int8(exactly: v)
        case .double(let v):
            guard let i64 = Int64(exactly: v) else { return nil }
            return Int8(exactly: i64)
        default: return nil
        }
    }
}

// MARK: - Double

extension Double: DBValueConvertible {
    public static var columnType: PantryColumnType { .double }
    public func toDBValue() -> DBValue { .double(self) }
    public static func fromDBValue(_ value: DBValue) -> Double? {
        switch value {
        case .double(let v): return v
        case .integer(let v): return Double(v)
        default: return nil
        }
    }
}

// MARK: - Float

extension Float: DBValueConvertible {
    public static var columnType: PantryColumnType { .double }
    public func toDBValue() -> DBValue { .double(Double(self)) }
    public static func fromDBValue(_ value: DBValue) -> Float? {
        switch value {
        case .double(let v): return Float(v)
        case .integer(let v): return Float(v)
        default: return nil
        }
    }
}

// MARK: - Bool

extension Bool: DBValueConvertible {
    public static var columnType: PantryColumnType { .boolean }
    public func toDBValue() -> DBValue { .boolean(self) }
    public static func fromDBValue(_ value: DBValue) -> Bool? {
        if case .boolean(let v) = value { return v }
        return nil
    }
}

// MARK: - Data

extension Data: DBValueConvertible {
    public static var columnType: PantryColumnType { .blob }
    public func toDBValue() -> DBValue { .blob(self) }
    public static func fromDBValue(_ value: DBValue) -> Data? {
        if case .blob(let v) = value { return v }
        return nil
    }
}

// MARK: - UUID (stored as .string)

extension UUID: DBValueConvertible {
    public static var columnType: PantryColumnType { .string }
    public func toDBValue() -> DBValue { .string(self.uuidString) }
    public static func fromDBValue(_ value: DBValue) -> UUID? {
        if case .string(let v) = value { return UUID(uuidString: v) }
        return nil
    }
}

// MARK: - Date (stored as .double timestamp)

extension Date: DBValueConvertible {
    public static var columnType: PantryColumnType { .double }
    public func toDBValue() -> DBValue { .double(self.timeIntervalSince1970) }
    public static func fromDBValue(_ value: DBValue) -> Date? {
        switch value {
        case .double(let v): return Date(timeIntervalSince1970: v)
        case .integer(let v): return Date(timeIntervalSince1970: Double(v))
        default: return nil
        }
    }
}

// MARK: - Optional

extension Optional: DBValueConvertible where Wrapped: DBValueConvertible {
    public static var columnType: PantryColumnType { Wrapped.columnType }

    public func toDBValue() -> DBValue {
        switch self {
        case .some(let value): return value.toDBValue()
        case .none: return .null
        }
    }

    /// Returns `.some(.some(value))` on success, `.some(.none)` for NULL, `nil` for conversion failure.
    public static func fromDBValue(_ value: DBValue) -> Optional<Wrapped>? {
        if case .null = value {
            return .some(.none) // Successfully decoded a NULL
        }
        guard let unwrapped = Wrapped.fromDBValue(value) else {
            return nil // Conversion failed
        }
        return .some(.some(unwrapped))
    }
}
