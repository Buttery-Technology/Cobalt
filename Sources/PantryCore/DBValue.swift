import Foundation

/// Represents a value that can be stored in the database
public enum DBValue: Codable, Comparable, Hashable, Sendable {
    case null
    case integer(Int64)
    case double(Double)
    case string(String)
    case blob(Data)
    case boolean(Bool)

    public static func < (lhs: DBValue, rhs: DBValue) -> Bool {
        switch (lhs, rhs) {
        case (.null, .null):
            return false
        case (.null, _):
            return true
        case (_, .null):
            return false
        case let (.integer(a), .integer(b)):
            return a < b
        case let (.double(a), .double(b)):
            // NaN sorts after all non-NaN doubles for total ordering
            if a.isNaN { return false }
            if b.isNaN { return true }
            return a < b
        case let (.integer(a), .double(b)):
            if b.isNaN { return true }
            return Double(a) < b
        case let (.double(a), .integer(b)):
            if a.isNaN { return false }
            return a < Double(b)
        case let (.string(a), .string(b)):
            return a < b
        case let (.boolean(a), .boolean(b)):
            return !a && b
        case let (.blob(a), .blob(b)):
            return a.lexicographicallyPrecedes(b)
        default:
            return lhs.typeOrder < rhs.typeOrder
        }
    }

    public static func == (lhs: DBValue, rhs: DBValue) -> Bool {
        switch (lhs, rhs) {
        case (.null, .null): return true
        case let (.integer(a), .integer(b)): return a == b
        case let (.double(a), .double(b)):
            // NaN == NaN must hold for Equatable reflexivity
            if a.isNaN && b.isNaN { return true }
            return a == b
        case let (.integer(a), .double(b)): return Double(a) == b
        case let (.double(a), .integer(b)): return a == Double(b)
        case let (.string(a), .string(b)): return a == b
        case let (.boolean(a), .boolean(b)): return a == b
        case let (.blob(a), .blob(b)): return a == b
        default: return false
        }
    }

    public func hash(into hasher: inout Hasher) {
        switch self {
        case .null:
            hasher.combine(0)
        case .integer(let v):
            hasher.combine(2)
            hasher.combine(Double(v))
        case .double(let v):
            hasher.combine(2)
            // Canonical NaN so all NaN bit patterns hash identically
            hasher.combine(v.isNaN ? Double.nan : v)
        case .string(let v):
            hasher.combine(4)
            hasher.combine(v)
        case .boolean(let v):
            hasher.combine(1)
            hasher.combine(v)
        case .blob(let v):
            hasher.combine(5)
            hasher.combine(v)
        }
    }

    private var typeOrder: Int {
        switch self {
        case .null: return 0
        case .boolean: return 1
        case .integer: return 2
        case .double: return 3
        case .string: return 4
        case .blob: return 5
        }
    }
}

// MARK: - Literal Conformances

extension DBValue: ExpressibleByIntegerLiteral {
    public init(integerLiteral value: Int64) {
        self = .integer(value)
    }
}

extension DBValue: ExpressibleByFloatLiteral {
    public init(floatLiteral value: Double) {
        self = .double(value)
    }
}

extension DBValue: ExpressibleByStringLiteral {
    public init(stringLiteral value: String) {
        self = .string(value)
    }
}

extension DBValue: ExpressibleByBooleanLiteral {
    public init(booleanLiteral value: Bool) {
        self = .boolean(value)
    }
}

extension DBValue: ExpressibleByNilLiteral {
    public init(nilLiteral: ()) {
        self = .null
    }
}
