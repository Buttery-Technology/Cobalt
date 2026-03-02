/// Defines the structure of a table in Pantry
public struct PantryTableSchema: Codable, Sendable {
    public let name: String
    public let columns: [PantryColumn]

    /// Pre-computed column name → ordinal index map for O(1) lookup.
    /// Not encoded — rebuilt from `columns` on decode or init.
    public let columnOrdinals: [String: Int]

    public init(name: String, columns: [PantryColumn]) {
        self.name = name
        self.columns = columns
        var ordinals = [String: Int](minimumCapacity: columns.count)
        for (i, col) in columns.enumerated() {
            ordinals[col.name] = i
        }
        self.columnOrdinals = ordinals
    }

    public var primaryKeyColumn: PantryColumn? {
        columns.first { $0.isPrimaryKey }
    }

    // Custom Codable: columnOrdinals is derived, not serialized
    enum CodingKeys: String, CodingKey {
        case name, columns
    }

    public init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        self.name = try container.decode(String.self, forKey: .name)
        self.columns = try container.decode([PantryColumn].self, forKey: .columns)
        var ordinals = [String: Int](minimumCapacity: columns.count)
        for (i, col) in columns.enumerated() {
            ordinals[col.name] = i
        }
        self.columnOrdinals = ordinals
    }

    public func encode(to encoder: Encoder) throws {
        var container = encoder.container(keyedBy: CodingKeys.self)
        try container.encode(name, forKey: .name)
        try container.encode(columns, forKey: .columns)
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

// MARK: - Column Factory Methods

extension PantryColumn {
    /// Primary key string column (non-nullable)
    public static func id(_ name: String = "_id") -> PantryColumn {
        PantryColumn(name: name, type: .string, isPrimaryKey: true, isNullable: false)
    }

    /// String column
    public static func string(_ name: String, nullable: Bool = true, defaultValue: String? = nil) -> PantryColumn {
        PantryColumn(name: name, type: .string, isNullable: nullable, defaultValue: defaultValue.map { .string($0) })
    }

    /// Integer column
    public static func integer(_ name: String, nullable: Bool = true, defaultValue: Int64? = nil) -> PantryColumn {
        PantryColumn(name: name, type: .integer, isNullable: nullable, defaultValue: defaultValue.map { .integer($0) })
    }

    /// Double column
    public static func double(_ name: String, nullable: Bool = true, defaultValue: Double? = nil) -> PantryColumn {
        PantryColumn(name: name, type: .double, isNullable: nullable, defaultValue: defaultValue.map { .double($0) })
    }

    /// Boolean column
    public static func boolean(_ name: String, nullable: Bool = true, defaultValue: Bool? = nil) -> PantryColumn {
        PantryColumn(name: name, type: .boolean, isNullable: nullable, defaultValue: defaultValue.map { .boolean($0) })
    }

    /// Blob column
    public static func blob(_ name: String, nullable: Bool = true) -> PantryColumn {
        PantryColumn(name: name, type: .blob, isNullable: nullable)
    }
}
