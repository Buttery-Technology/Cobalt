/// Defines the structure of a table in Pantry
public struct PantryTableSchema: Codable, Sendable {
    public let name: String
    public let columns: [PantryColumn]

    public init(name: String, columns: [PantryColumn]) {
        self.name = name
        self.columns = columns
    }

    public var primaryKeyColumn: PantryColumn? {
        columns.first { $0.isPrimaryKey }
    }
}

/// Defines a single column in a table schema
public struct PantryColumn: Codable, Sendable {
    public let name: String
    public let type: PantryColumnType
    public let isPrimaryKey: Bool
    public let isNullable: Bool
    public let defaultValue: DBValue?

    public init(
        name: String,
        type: PantryColumnType,
        isPrimaryKey: Bool = false,
        isNullable: Bool = true,
        defaultValue: DBValue? = nil
    ) {
        self.name = name
        self.type = type
        self.isPrimaryKey = isPrimaryKey
        self.isNullable = isNullable
        self.defaultValue = defaultValue
    }
}

/// Column data types
public enum PantryColumnType: String, Codable, Sendable {
    case integer
    case double
    case string
    case blob
    case boolean
}
