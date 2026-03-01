import Foundation
import PantryCore
import PantryQuery

// MARK: - SortDirection

/// Sort direction for query results.
public enum SortDirection: Sendable {
    case ascending
    case descending
}

// MARK: - ModelQuery Builder

/// A type-safe query builder for PantryModel types.
/// Supports filter, sort, limit, and terminal operations (all, first, count, delete).
public struct ModelQuery<M: PantryModel>: Sendable {
    internal let database: PantryDatabase
    internal let filters: [WhereCondition]
    internal let sorts: [@Sendable ([String: DBValue], [String: DBValue]) -> Bool]
    internal let limitCount: Int?

    internal init(
        database: PantryDatabase,
        filters: [WhereCondition] = [],
        sorts: [@Sendable ([String: DBValue], [String: DBValue]) -> Bool] = [],
        limitCount: Int? = nil
    ) {
        self.database = database
        self.filters = filters
        self.sorts = sorts
        self.limitCount = limitCount
    }

    /// Add a type-safe filter to the query.
    public func filter(_ filter: ModelFilter<M>) -> ModelQuery<M> {
        ModelQuery(
            database: database,
            filters: filters + [filter.condition],
            sorts: sorts,
            limitCount: limitCount
        )
    }

    /// Add a sort on a column.
    public func sort<V>(_ column: Column<M, V>, _ direction: SortDirection) -> ModelQuery<M> {
        let key = column.key
        let comparator: @Sendable ([String: DBValue], [String: DBValue]) -> Bool = { a, b in
            let aVal = a[key] ?? .null
            let bVal = b[key] ?? .null
            switch direction {
            case .ascending: return aVal < bVal
            case .descending: return bVal < aVal
            }
        }
        return ModelQuery(
            database: database,
            filters: filters,
            sorts: sorts + [comparator],
            limitCount: limitCount
        )
    }

    /// Limit the number of results returned.
    public func limit(_ count: Int) -> ModelQuery<M> {
        ModelQuery(
            database: database,
            filters: filters,
            sorts: sorts,
            limitCount: count
        )
    }

    // MARK: - Terminal Operations

    /// Execute the query and return all matching models.
    public func all() async throws -> [M] {
        let condition = combinedCondition()
        let rows = try await database.select(from: M.tableName, where: condition)

        var valueMaps = rows.map { $0.values }

        // Apply sorts in order
        if !sorts.isEmpty {
            let sortsCopy = sorts
            valueMaps.sort { a, b in
                for comparator in sortsCopy {
                    if comparator(a, b) { return true }
                    if comparator(b, a) { return false }
                }
                return false
            }
        }

        // Apply limit
        if let limit = limitCount {
            valueMaps = Array(valueMaps.prefix(limit))
        }

        // Decode
        return try valueMaps.map { try DBValueDecoder.decode(M.self, from: $0) }
    }

    /// Execute the query and return the first matching model.
    public func first() async throws -> M? {
        let results = try await limit(1).all()
        return results.first
    }

    /// Execute the query and return the count of matching rows.
    public func count() async throws -> Int {
        let condition = combinedCondition()
        let rows = try await database.select(from: M.tableName, where: condition)
        return rows.count
    }

    /// Delete all rows matching the query and return the number deleted.
    @discardableResult
    public func delete() async throws -> Int {
        let condition = combinedCondition()
        return try await database.delete(from: M.tableName, where: condition)
    }

    /// Returns true if any rows match the query.
    public func exists() async throws -> Bool {
        try await count() > 0
    }

    // MARK: - Internal

    private func combinedCondition() -> WhereCondition? {
        switch filters.count {
        case 0: return nil
        case 1: return filters[0]
        default: return .and(filters)
        }
    }
}

// MARK: - PantryDatabase Model Extensions

extension PantryDatabase {
    /// Save a model instance (upsert). Auto-creates the table on first use.
    public func save<M: PantryModel>(_ model: M) async throws {
        // Encode model to [String: DBValue]
        let values = try DBValueEncoder.encode(model)

        // Auto-create table if needed
        if !(await tableExists(M.tableName)) {
            let schema = try DBValueEncoder.deriveSchema(from: model)
            do {
                try await createTable(schema)
            } catch PantryError.tableAlreadyExists {
                // Race condition: another call created it
            }
        }

        // Upsert: delete existing row with same id, then insert
        _ = try await delete(from: M.tableName, where: .equals(column: "id", value: .string(model.id)))
        try await insert(into: M.tableName, values: values)
    }

    /// Find a single model by its ID.
    public func find<M: PantryModel>(_ type: M.Type, id: String) async throws -> M? {
        guard await tableExists(M.tableName) else { return nil }
        let rows = try await select(from: M.tableName, where: .equals(column: "id", value: .string(id)))
        guard let row = rows.first else { return nil }
        return try DBValueDecoder.decode(M.self, from: row.values)
    }

    /// Find all instances of a model type.
    public func findAll<M: PantryModel>(_ type: M.Type) async throws -> [M] {
        guard await tableExists(M.tableName) else { return [] }
        let rows = try await select(from: M.tableName)
        return try rows.map { try DBValueDecoder.decode(M.self, from: $0.values) }
    }

    /// Delete a model instance by its ID. Returns the number of rows deleted (0 or 1).
    @discardableResult
    public func delete<M: PantryModel>(_ model: M) async throws -> Int {
        guard await tableExists(M.tableName) else { return 0 }
        return try await delete(from: M.tableName, where: .equals(column: "id", value: .string(model.id)))
    }

    /// Save multiple model instances at once (upsert each).
    public func saveAll<M: PantryModel>(_ models: [M]) async throws {
        for model in models {
            try await save(model)
        }
    }

    /// Create a type-safe query builder for a model type.
    public func query<M: PantryModel>(_ type: M.Type) -> ModelQuery<M> {
        ModelQuery(database: self)
    }
}
