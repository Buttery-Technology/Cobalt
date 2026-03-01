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
