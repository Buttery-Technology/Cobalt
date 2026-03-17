import Foundation
import CobaltCore

/// Result of executing a SQL statement
public enum QueryResult: Sendable {
    /// SELECT result: array of rows
    case rows([Row])
    /// INSERT/UPDATE/DELETE result: number of affected rows
    case rowCount(Int)
    /// DDL or transaction control: success with no data
    case ok

    /// Extract rows if this is a `.rows` result, otherwise return an empty array.
    public var rows: [Row] {
        if case .rows(let r) = self { return r }
        return []
    }
}

/// Lowers AST expressions to WhereCondition and other query IR types
public struct ASTLowering: Sendable {

    public init() {}

    // MARK: - Expression → WhereCondition

    /// Convert a WHERE clause Expression to a WhereCondition
    public func lowerWhereClause(_ expr: Expression) throws -> WhereCondition {
        switch expr {
        case .binaryOp(let left, let op, let right):
            // AND / OR
            if op == .and {
                let l = try lowerWhereClause(left)
                let r = try lowerWhereClause(right)
                return .and([l, r])
            }
            if op == .or {
                let l = try lowerWhereClause(left)
                let r = try lowerWhereClause(right)
                return .or([l, r])
            }

            // Column comparison: column OP literal
            if let (col, val) = extractColumnAndValue(left, right) {
                switch op {
                case .equal: return .equals(column: col, value: val)
                case .notEqual: return .notEquals(column: col, value: val)
                case .lessThan: return .lessThan(column: col, value: val)
                case .greaterThan: return .greaterThan(column: col, value: val)
                case .lessOrEqual: return .lessThanOrEqual(column: col, value: val)
                case .greaterOrEqual: return .greaterThanOrEqual(column: col, value: val)
                default: break
                }
            }

            // Reversed: literal OP column → flip comparison
            if let (col, val) = extractColumnAndValue(right, left) {
                switch op {
                case .equal: return .equals(column: col, value: val)
                case .notEqual: return .notEquals(column: col, value: val)
                case .lessThan: return .greaterThan(column: col, value: val)
                case .greaterThan: return .lessThan(column: col, value: val)
                case .lessOrEqual: return .greaterThanOrEqual(column: col, value: val)
                case .greaterOrEqual: return .lessThanOrEqual(column: col, value: val)
                default: break
                }
            }

            throw SQLError.unsupported("Complex expression in WHERE clause")

        case .isNull(let inner):
            guard let col = extractColumnName(inner) else {
                throw SQLError.unsupported("IS NULL on non-column expression")
            }
            return .isNull(column: col)

        case .isNotNull(let inner):
            guard let col = extractColumnName(inner) else {
                throw SQLError.unsupported("IS NOT NULL on non-column expression")
            }
            return .isNotNull(column: col)

        case .like(let inner, let pattern):
            guard let col = extractColumnName(inner) else {
                throw SQLError.unsupported("LIKE on non-column expression")
            }
            guard case .stringLiteral(let pat) = pattern else {
                throw SQLError.unsupported("LIKE with non-literal pattern")
            }
            return .like(column: col, pattern: pat)

        case .between(let inner, let low, let high):
            guard let col = extractColumnName(inner) else {
                throw SQLError.unsupported("BETWEEN on non-column expression")
            }
            let lowVal = try lowerToDBValue(low)
            let highVal = try lowerToDBValue(high)
            return .between(column: col, min: lowVal, max: highVal)

        case .inList(let inner, let values):
            guard let col = extractColumnName(inner) else {
                throw SQLError.unsupported("IN on non-column expression")
            }
            let dbValues = try values.map { try lowerToDBValue($0) }
            return .in(column: col, values: dbValues)

        case .unaryOp(.not, let operand):
            // NOT column — shorthand for WHERE column = FALSE
            if case .column(_, let name) = operand {
                return .equals(column: name, value: .boolean(false))
            }
            // NOT (condition) — limited support
            let inner = try lowerWhereClause(operand)
            // Invert simple conditions
            switch inner {
            case .equals(let col, let val): return .notEquals(column: col, value: val)
            case .notEquals(let col, let val): return .equals(column: col, value: val)
            case .isNull(let col): return .isNotNull(column: col)
            case .isNotNull(let col): return .isNull(column: col)
            default:
                throw SQLError.unsupported("NOT with complex expression")
            }

        case .booleanLiteral(true):
            // WHERE TRUE — matches everything (no condition)
            return .and([])

        case .booleanLiteral(false):
            // WHERE FALSE — matches nothing (impossible condition)
            return .equals(column: "__impossible__", value: .string("__never__"))

        case .column(_, let name):
            // WHERE active — shorthand for WHERE active = TRUE
            return .equals(column: name, value: .boolean(true))

        default:
            throw SQLError.unsupported("Unsupported expression in WHERE clause: \(expr)")
        }
    }

    // MARK: - Expression → DBValue

    /// Lower a simple expression to a DBValue (for literals)
    public func lowerToDBValue(_ expr: Expression) throws -> DBValue {
        switch expr {
        case .integerLiteral(let v): return .integer(v)
        case .doubleLiteral(let v): return .double(v)
        case .stringLiteral(let v): return .string(v)
        case .booleanLiteral(let v): return .boolean(v)
        case .nullLiteral: return .null
        case .unaryOp(.negate, .integerLiteral(let v)): return .integer(-v)
        case .unaryOp(.negate, .doubleLiteral(let v)): return .double(-v)
        default:
            throw SQLError.unsupported("Cannot convert expression to value: \(expr)")
        }
    }

    // MARK: - OrderBy Lowering

    /// Convert ORDER BY AST items to engine OrderBy structs
    public func lowerOrderBy(_ items: [OrderByItem]) throws -> [OrderBy] {
        try items.map { item in
            guard let col = extractColumnName(item.expression) else {
                throw SQLError.unsupported("ORDER BY on non-column expression")
            }
            return OrderBy(col, item.ascending ? .ascending : .descending)
        }
    }

    // MARK: - JoinClause Lowering

    /// Convert AST JoinItem to engine JoinClause
    public func lowerJoin(_ item: JoinItem) throws -> JoinClause {
        let joinType: JoinType
        switch item.joinType {
        case .inner: joinType = .inner
        case .left: joinType = .left
        case .right: joinType = .right
        case .cross: joinType = .cross
        }

        guard let tableRef = item.table.tableName else {
            throw SQLError.unsupported("Subquery in JOIN not yet supported")
        }

        // Parse join condition: expect t1.col = t2.col
        guard let condition = item.condition else {
            if joinType == .cross {
                return JoinClause(table: tableRef, type: joinType, on: "", equals: "")
            }
            throw SQLError.semanticError("JOIN requires ON condition")
        }

        guard case .binaryOp(let left, .equal, let right) = condition else {
            throw SQLError.unsupported("JOIN condition must be simple equality")
        }

        guard let leftCol = extractColumnName(left), let rightCol = extractColumnName(right) else {
            throw SQLError.unsupported("JOIN ON must reference columns")
        }

        return JoinClause(table: tableRef, type: joinType, on: leftCol, equals: rightCol)
    }

    // MARK: - SELECT expressions (for GROUP BY)

    /// Convert AST SelectItems to engine SelectExpressions
    public func lowerSelectExpressions(_ items: [SelectItem]) throws -> [SelectExpression] {
        try items.map { item in
            switch item {
            case .allColumns:
                throw SQLError.unsupported("* not supported with GROUP BY")
            case .tableAllColumns(_):
                throw SQLError.unsupported("table.* not supported with GROUP BY")
            case .expression(let expr, _):
                switch expr {
                case .column(_, let name):
                    return .column(name)
                case .aggregate(.count, let arg):
                    if let arg = arg, let col = extractColumnName(arg) {
                        return .count(column: col)
                    }
                    return .count(column: nil)
                case .aggregate(.sum, let arg):
                    guard let arg = arg, let col = extractColumnName(arg) else {
                        throw SQLError.semanticError("SUM requires a column")
                    }
                    return .sum(column: col)
                case .aggregate(.avg, let arg):
                    guard let arg = arg, let col = extractColumnName(arg) else {
                        throw SQLError.semanticError("AVG requires a column")
                    }
                    return .avg(column: col)
                case .aggregate(.min, let arg):
                    guard let arg = arg, let col = extractColumnName(arg) else {
                        throw SQLError.semanticError("MIN requires a column")
                    }
                    return .min(column: col)
                case .aggregate(.max, let arg):
                    guard let arg = arg, let col = extractColumnName(arg) else {
                        throw SQLError.semanticError("MAX requires a column")
                    }
                    return .max(column: col)
                default:
                    throw SQLError.unsupported("Unsupported expression in GROUP BY SELECT list")
                }
            }
        }
    }

    // MARK: - Column list extraction

    /// Extract column names from SELECT items (for column projection)
    public func extractColumns(_ items: [SelectItem]) -> [String]? {
        // Return nil for * (meaning all columns)
        if items.count == 1, case .allColumns = items[0] { return nil }

        var cols: [String] = []
        for item in items {
            switch item {
            case .allColumns: return nil
            case .tableAllColumns(_): return nil
            case .expression(let expr, let alias):
                if let name = alias {
                    cols.append(name)
                } else if let name = extractColumnName(expr) {
                    cols.append(name)
                } else {
                    return nil
                }
            }
        }
        return cols
    }

    // MARK: - DDL Lowering

    /// Convert ColumnDef to CobaltColumn
    public func lowerColumnDef(_ def: ColumnDef) throws -> CobaltColumn {
        let colType: CobaltColumnType
        let isAutoIncrement: Bool
        switch def.dataType {
        case .serial:
            colType = .integer
            isAutoIncrement = true
        case .integer:
            colType = .integer
            isAutoIncrement = false
        case .text, .varchar:
            colType = .string
            isAutoIncrement = false
        case .real:
            colType = .double
            isAutoIncrement = false
        case .blob:
            colType = .blob
            isAutoIncrement = false
        case .boolean:
            colType = .boolean
            isAutoIncrement = false
        }

        var defaultValue: DBValue? = nil
        if let defExpr = def.defaultValue {
            defaultValue = try lowerToDBValue(defExpr)
        }

        // SERIAL columns are implicitly PRIMARY KEY NOT NULL
        let isPrimaryKey = def.isPrimaryKey || isAutoIncrement
        let isNullable = isAutoIncrement ? false : def.isNullable

        return CobaltColumn(
            name: def.name,
            type: colType,
            isPrimaryKey: isPrimaryKey,
            isNullable: isNullable,
            defaultValue: defaultValue,
            isAutoIncrement: isAutoIncrement
        )
    }

    /// Convert CreateTableStatement to CobaltTableSchema
    public func lowerCreateTable(_ stmt: CreateTableStatement) throws -> CobaltTableSchema {
        let columns = try stmt.columns.map { try lowerColumnDef($0) }
        return CobaltTableSchema(name: stmt.name, columns: columns)
    }

    // MARK: - Helpers

    private func extractColumnName(_ expr: Expression) -> String? {
        switch expr {
        case .column(_, let name): return name
        default: return nil
        }
    }

    private func extractColumnAndValue(_ colExpr: Expression, _ valExpr: Expression) -> (String, DBValue)? {
        guard let col = extractColumnName(colExpr) else { return nil }
        switch valExpr {
        case .integerLiteral(let v): return (col, .integer(v))
        case .doubleLiteral(let v): return (col, .double(v))
        case .stringLiteral(let v): return (col, .string(v))
        case .booleanLiteral(let v): return (col, .boolean(v))
        case .nullLiteral: return (col, .null)
        case .unaryOp(.negate, .integerLiteral(let v)): return (col, .integer(-v))
        case .unaryOp(.negate, .doubleLiteral(let v)): return (col, .double(-v))
        default: return nil
        }
    }
}

// MARK: - TableRef Extension

extension TableRef {
    /// Extract the table name if this is a simple table reference
    public var tableName: String? {
        switch self {
        case .table(let name, _): return name
        case .subquery: return nil
        }
    }
}
