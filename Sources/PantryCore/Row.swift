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
