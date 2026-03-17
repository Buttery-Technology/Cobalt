/// Defines the structure of a table in Cobalt
public struct CobaltTableSchema: Codable, Sendable {
    public let name: String
    public let columns: [CobaltColumn]

    /// Pre-computed column name → ordinal index map for O(1) lookup.
    /// Not encoded — rebuilt from `columns` on decode or init.
    public let columnOrdinals: [String: Int]

    public init(name: String, columns: [CobaltColumn]) {
        self.name = name
        self.columns = columns
        var ordinals = [String: Int](minimumCapacity: columns.count)
        for (i, col) in columns.enumerated() {
            ordinals[col.name] = i
        }
        self.columnOrdinals = ordinals
    }

    public var primaryKeyColumn: CobaltColumn? {
        columns.first { $0.isPrimaryKey }
    }

    // Custom Codable: columnOrdinals is derived, not serialized
    enum CodingKeys: String, CodingKey {
        case name, columns
    }

    public init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        self.name = try container.decode(String.self, forKey: .name)
        self.columns = try container.decode([CobaltColumn].self, forKey: .columns)
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
public struct CobaltColumn: Codable, Sendable {
    public let name: String
    public let type: CobaltColumnType
    public let isPrimaryKey: Bool
    public let isNullable: Bool
    public let defaultValue: DBValue?
    public let isAutoIncrement: Bool

    public init(
        name: String,
        type: CobaltColumnType,
        isPrimaryKey: Bool = false,
        isNullable: Bool = true,
        defaultValue: DBValue? = nil,
        isAutoIncrement: Bool = false
    ) {
        self.name = name
        self.type = type
        self.isPrimaryKey = isPrimaryKey
        self.isNullable = isNullable
        self.defaultValue = defaultValue
        self.isAutoIncrement = isAutoIncrement
    }

    // Custom Codable for backward compatibility
    enum CodingKeys: String, CodingKey {
        case name, type, isPrimaryKey, isNullable, defaultValue, isAutoIncrement
    }

    public init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        self.name = try container.decode(String.self, forKey: .name)
        self.type = try container.decode(CobaltColumnType.self, forKey: .type)
        self.isPrimaryKey = try container.decode(Bool.self, forKey: .isPrimaryKey)
        self.isNullable = try container.decode(Bool.self, forKey: .isNullable)
        self.defaultValue = try container.decodeIfPresent(DBValue.self, forKey: .defaultValue)
        self.isAutoIncrement = try container.decodeIfPresent(Bool.self, forKey: .isAutoIncrement) ?? false
    }
}

/// Column data types
public enum CobaltColumnType: String, Codable, Sendable {
    case integer
    case double
    case string
    case blob
    case boolean
}

// MARK: - Column Factory Methods

extension CobaltColumn {
    /// Primary key string column (non-nullable)
    public static func id(_ name: String = "_id") -> CobaltColumn {
        CobaltColumn(name: name, type: .string, isPrimaryKey: true, isNullable: false)
    }

    /// String column
    public static func string(_ name: String, nullable: Bool = true, defaultValue: String? = nil) -> CobaltColumn {
        CobaltColumn(name: name, type: .string, isNullable: nullable, defaultValue: defaultValue.map { .string($0) })
    }

    /// Integer column
    public static func integer(_ name: String, nullable: Bool = true, defaultValue: Int64? = nil) -> CobaltColumn {
        CobaltColumn(name: name, type: .integer, isNullable: nullable, defaultValue: defaultValue.map { .integer($0) })
    }

    /// Double column
    public static func double(_ name: String, nullable: Bool = true, defaultValue: Double? = nil) -> CobaltColumn {
        CobaltColumn(name: name, type: .double, isNullable: nullable, defaultValue: defaultValue.map { .double($0) })
    }

    /// Boolean column
    public static func boolean(_ name: String, nullable: Bool = true, defaultValue: Bool? = nil) -> CobaltColumn {
        CobaltColumn(name: name, type: .boolean, isNullable: nullable, defaultValue: defaultValue.map { .boolean($0) })
    }

    /// Blob column
    public static func blob(_ name: String, nullable: Bool = true) -> CobaltColumn {
        CobaltColumn(name: name, type: .blob, isNullable: nullable)
    }
}
