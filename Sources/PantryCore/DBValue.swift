import Foundation

/// Represents a value that can be stored in the database
public enum DBValue: Codable, Equatable, Comparable, Hashable, Sendable {
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
            return a < b
        case let (.integer(a), .double(b)):
            return Double(a) < b
        case let (.double(a), .integer(b)):
            return a < Double(b)
        case let (.string(a), .string(b)):
            return a < b
        case let (.boolean(a), .boolean(b)):
            return !a && b
        default:
            return lhs.typeOrder < rhs.typeOrder
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
