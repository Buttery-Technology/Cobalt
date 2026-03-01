import Foundation
import PantryCore
import PantryQuery

/// Describes a single schema migration step
public struct Migration: Sendable {
    public let version: Int
    public let operations: [MigrationOperation]

    public init(version: Int, operations: [MigrationOperation]) {
        self.version = version
        self.operations = operations
    }
}

/// Individual migration operations that can be applied to a table schema
public enum MigrationOperation: Sendable {
    /// Add a new column with an optional default value. Existing rows get null (or the default).
    case addColumn(PantryColumn)
    /// Remove a column. Existing row data for this column is deleted.
    case dropColumn(String)
    /// Rename a column. Existing row data is moved to the new name.
    case renameColumn(from: String, to: String)
}

// MARK: - PantryDatabase Migration API

extension PantryDatabase {
    /// Apply a sequence of migrations to a table, updating the schema and transforming existing rows.
    /// Migrations are applied in order. Each migration's operations run sequentially within a transaction.
    public func migrate(table: String, migrations: [Migration]) async throws {
        guard await tableExists(table) else {
            throw PantryError.tableNotFound(name: table)
        }

        // Sort migrations by version
        let sorted = migrations.sorted { $0.version < $1.version }

        for migration in sorted {
            try await applyMigration(table: table, operations: migration.operations)
        }
    }

    private func applyMigration(table: String, operations: [MigrationOperation]) async throws {
        for operation in operations {
            switch operation {
            case .addColumn(let column):
                try await addColumnToTable(table: table, column: column)

            case .dropColumn(let columnName):
                try await dropColumnFromTable(table: table, columnName: columnName)

            case .renameColumn(let from, let to):
                try await renameColumnInTable(table: table, from: from, to: to)
            }
        }
    }

    private func addColumnToTable(table: String, column: PantryColumn) async throws {
        guard let schema = await getTableSchema(table) else {
            throw PantryError.tableNotFound(name: table)
        }

        // Check column doesn't already exist
        guard !schema.columns.contains(where: { $0.name == column.name }) else { return }

        // Update schema
        let newSchema = PantryTableSchema(name: schema.name, columns: schema.columns + [column])
        try await updateTableSchema(table, schema: newSchema)

        // If the column has a default value, backfill existing rows
        if let defaultValue = column.defaultValue {
            _ = try await update(table: table, set: [column.name: defaultValue])
        }
    }

    private func dropColumnFromTable(table: String, columnName: String) async throws {
        guard let schema = await getTableSchema(table) else {
            throw PantryError.tableNotFound(name: table)
        }

        // Don't drop primary key
        if let col = schema.columns.first(where: { $0.name == columnName }), col.isPrimaryKey {
            throw PantryError.invalidQuery(description: "Cannot drop primary key column '\(columnName)'")
        }

        // Update schema
        let newColumns = schema.columns.filter { $0.name != columnName }
        let newSchema = PantryTableSchema(name: schema.name, columns: newColumns)
        try await updateTableSchema(table, schema: newSchema)

        // Rewrite all rows to remove the dropped column
        let rows = try await select(from: table)
        for row in rows {
            guard row.values[columnName] != nil else { continue }
            var updated = row.values
            updated.removeValue(forKey: columnName)
            // Delete and re-insert with cleaned data
            if let id = row.values["id"], id != .null {
                _ = try await delete(from: table, where: .equals(column: "id", value: id))
                try await insert(into: table, values: updated)
            }
        }
    }

    private func renameColumnInTable(table: String, from: String, to: String) async throws {
        guard let schema = await getTableSchema(table) else {
            throw PantryError.tableNotFound(name: table)
        }

        // Update schema: rename the column
        let newColumns = schema.columns.map { col -> PantryColumn in
            if col.name == from {
                return PantryColumn(name: to, type: col.type, isPrimaryKey: col.isPrimaryKey, isNullable: col.isNullable, defaultValue: col.defaultValue)
            }
            return col
        }
        let newSchema = PantryTableSchema(name: schema.name, columns: newColumns)
        try await updateTableSchema(table, schema: newSchema)

        // Rewrite all rows to move data from old column name to new
        let rows = try await select(from: table)
        for row in rows {
            guard let value = row.values[from] else { continue }
            var updated = row.values
            updated.removeValue(forKey: from)
            updated[to] = value
            if let id = row.values["id"], id != .null {
                _ = try await delete(from: table, where: .equals(column: "id", value: id))
                try await insert(into: table, values: updated)
            }
        }
    }
}

// MARK: - Auto-Migration for PantryModel

extension PantryDatabase {
    /// Detect schema drift between a model type and its table, auto-applying additive migrations.
    /// Only adds new nullable columns; refuses destructive changes.
    internal func autoMigrate<M: PantryModel>(for model: M) async throws {
        let tableName = M.tableName
        guard let existingSchema = await getTableSchema(tableName) else { return }

        let currentSchema = try DBValueEncoder.deriveSchema(from: model)
        let existingNames = Set(existingSchema.columns.map { $0.name })
        let currentNames = Set(currentSchema.columns.map { $0.name })

        // Find new columns that don't exist in the stored schema
        let newColumnNames = currentNames.subtracting(existingNames)
        guard !newColumnNames.isEmpty else { return }

        // Only auto-migrate nullable (optional) columns
        let newColumns = currentSchema.columns.filter { newColumnNames.contains($0.name) }
        for col in newColumns {
            guard col.isNullable else {
                throw PantryError.invalidQuery(description: "Auto-migration cannot add non-nullable column '\(col.name)'. Use explicit migrate().")
            }
        }

        // Apply additive migration
        let updatedColumns = existingSchema.columns + newColumns
        let updatedSchema = PantryTableSchema(name: tableName, columns: updatedColumns)
        try await updateTableSchema(tableName, schema: updatedSchema)
    }
}
