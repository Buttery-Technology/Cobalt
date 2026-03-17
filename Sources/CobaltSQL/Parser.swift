import Foundation

/// Recursive descent SQL parser with Pratt precedence for expressions
public struct Parser: Sendable {
    private var tokens: [(Token, SourcePosition)]
    private var pos: Int = 0

    public init(tokens: [(Token, SourcePosition)]) {
        self.tokens = tokens
    }

    /// Parse a single SQL statement
    public mutating func parseStatement() throws -> Statement {
        let tok = current
        switch tok {
        case .keyword(.with):
            // WITH ... SELECT
            let ctes = try parseCTEs()
            var select = try parseSelect()
            select.ctes = ctes
            if let compound = try parseOptionalCompound(left: select) {
                return .compound(compound)
            }
            return .select(select)
        case .keyword(.select):
            let select = try parseSelect()
            if let compound = try parseOptionalCompound(left: select) {
                return .compound(compound)
            }
            return .select(select)
        case .keyword(.insert):
            return .insert(try parseInsert())
        case .keyword(.update):
            return .update(try parseUpdate())
        case .keyword(.delete):
            return .delete(try parseDelete())
        case .keyword(.create):
            return try parseCreate()
        case .keyword(.drop):
            return try parseDrop()
        case .keyword(.alter):
            return .alterTable(try parseAlterTable())
        case .keyword(.explain):
            return try parseExplain()
        case .keyword(.begin):
            advance()
            // Optional TRANSACTION keyword
            if case .keyword(.transaction) = current { advance() }
            return .begin(BeginStatement())
        case .keyword(.commit):
            advance()
            return .commit
        case .keyword(.rollback):
            advance()
            return .rollback
        case .keyword(.vacuum):
            return .vacuum(try parseVacuum())
        case .keyword(.set):
            return .set(try parseSet())
        case .keyword(.show):
            return .show(try parseShow())
        case .keyword(.reset):
            return .reset(try parseReset())
        case .keyword(.discard):
            return .discard(try parseDiscard())
        default:
            throw SQLError.expectedToken(expected: "statement", found: describe(tok), line: currentPos.line, column: currentPos.column)
        }
    }

    // MARK: - WITH (CTEs)

    private mutating func parseCTEs() throws -> [CTEDef] {
        try expect(.keyword(.with))
        var isRecursive = false
        if case .keyword(.recursive) = current {
            isRecursive = true
            advance()
        }

        var ctes: [CTEDef] = []
        repeat {
            let name = try parseIdentifier()

            // Optional column list
            var columns: [String]? = nil
            if case .leftParen = current {
                advance()
                var cols: [String] = []
                repeat {
                    cols.append(try parseIdentifier())
                } while tryConsume(.comma)
                try expect(.rightParen)
                columns = cols
            }

            try expect(.keyword(.as))
            try expect(.leftParen)
            let query = try parseSelect()
            try expect(.rightParen)

            ctes.append(CTEDef(name: name, columns: columns, query: query, isRecursive: isRecursive))
        } while tryConsume(.comma)

        return ctes
    }

    // MARK: - SELECT

    private mutating func parseSelect() throws -> SelectStatement {
        try expect(.keyword(.select))

        var distinct = false
        if case .keyword(.distinct) = current {
            distinct = true
            advance()
        }

        let columns = try parseSelectList()

        var from: TableRef? = nil
        var joins: [JoinItem] = []
        if case .keyword(.from) = current {
            advance()
            from = try parseTableRef()
            joins = try parseJoins()
        }

        let whereClause: Expression? = try parseOptionalWhere()
        let groupBy = try parseOptionalGroupBy()
        let having: Expression? = try parseOptionalHaving()
        let orderBy = try parseOptionalOrderBy()
        let (limit, offset) = try parseOptionalLimitOffset()

        return SelectStatement(
            distinct: distinct,
            columns: columns,
            from: from,
            joins: joins,
            whereClause: whereClause,
            groupBy: groupBy,
            having: having,
            orderBy: orderBy,
            limit: limit,
            offset: offset
        )
    }

    // MARK: - Compound SELECT (UNION / INTERSECT / EXCEPT)

    private mutating func parseOptionalCompound(left: SelectStatement) throws -> CompoundSelectStatement? {
        let opType: SetOperationType
        switch current {
        case .keyword(.union):
            advance()
            if case .keyword(.all) = current {
                advance()
                opType = .unionAll
            } else {
                opType = .union
            }
        case .keyword(.intersect):
            advance()
            opType = .intersect
        case .keyword(.except):
            advance()
            opType = .except
        default:
            return nil
        }

        let right = try parseSelect()
        let orderBy = try parseOptionalOrderBy()
        let (limit, offset) = try parseOptionalLimitOffset()
        return CompoundSelectStatement(left: left, operation: opType, right: right, orderBy: orderBy, limit: limit, offset: offset)
    }

    private mutating func parseSelectList() throws -> [SelectItem] {
        var items: [SelectItem] = []
        repeat {
            if case .star = current {
                advance()
                items.append(.allColumns)
            } else {
                let expr = try parseExpression()
                // Check for table.* after parsing
                if case .column(let table, let name) = expr, name == "*" {
                    items.append(.tableAllColumns(table ?? ""))
                } else {
                    var alias: String? = nil
                    if case .keyword(.as) = current {
                        advance()
                        alias = try parseIdentifier()
                    } else if case .identifier(_) = current, !isKeywordAt(pos) {
                        alias = try parseIdentifier()
                    }
                    items.append(.expression(expr, alias: alias))
                }
            }
        } while tryConsume(.comma)
        return items
    }

    private mutating func parseTableRef() throws -> TableRef {
        if case .leftParen = current {
            advance()
            let select = try parseSelect()
            try expect(.rightParen)
            let alias: String
            if case .keyword(.as) = current { advance() }
            alias = try parseIdentifier()
            return .subquery(select, alias: alias)
        }
        var name = try parseIdentifier()
        // Handle schema-qualified table names: schema.table (e.g., pg_catalog.pg_type, information_schema.tables)
        if case .dot = current {
            advance()
            let tablePart = try parseIdentifier()
            name = "\(name).\(tablePart)"
        }
        var alias: String? = nil
        if case .keyword(.as) = current {
            advance()
            alias = try parseIdentifier()
        } else if case .identifier(_) = current, !isKeywordAt(pos) {
            alias = try parseIdentifier()
        }
        return .table(name: name, alias: alias)
    }

    private mutating func parseJoins() throws -> [JoinItem] {
        var joins: [JoinItem] = []
        while true {
            let joinType: ASTJoinType?
            switch current {
            case .keyword(.inner):
                advance(); try expect(.keyword(.join)); joinType = .inner
            case .keyword(.left):
                advance()
                if case .keyword(.outer) = current { advance() }
                try expect(.keyword(.join))
                joinType = .left
            case .keyword(.right):
                advance()
                if case .keyword(.outer) = current { advance() }
                try expect(.keyword(.join))
                joinType = .right
            case .keyword(.cross):
                advance(); try expect(.keyword(.join)); joinType = .cross
            case .keyword(.join):
                advance(); joinType = .inner
            default:
                joinType = nil
            }
            guard let jt = joinType else { break }
            let table = try parseTableRef()
            var condition: Expression? = nil
            if case .keyword(.on) = current {
                advance()
                condition = try parseExpression()
            }
            joins.append(JoinItem(joinType: jt, table: table, condition: condition))
        }
        return joins
    }

    private mutating func parseOptionalWhere() throws -> Expression? {
        guard case .keyword(.where) = current else { return nil }
        advance()
        return try parseExpression()
    }

    private mutating func parseOptionalGroupBy() throws -> [Expression] {
        guard case .keyword(.group) = current else { return [] }
        advance()
        try expect(.keyword(.by))
        var cols: [Expression] = []
        repeat {
            cols.append(try parseExpression())
        } while tryConsume(.comma)
        return cols
    }

    private mutating func parseOptionalHaving() throws -> Expression? {
        guard case .keyword(.having) = current else { return nil }
        advance()
        return try parseExpression()
    }

    private mutating func parseOptionalOrderBy() throws -> [OrderByItem] {
        guard case .keyword(.order) = current else { return [] }
        advance()
        try expect(.keyword(.by))
        var items: [OrderByItem] = []
        repeat {
            let expr = try parseExpression()
            var ascending = true
            if case .keyword(.asc) = current { advance(); ascending = true }
            else if case .keyword(.desc) = current { advance(); ascending = false }
            items.append(OrderByItem(expression: expr, ascending: ascending))
        } while tryConsume(.comma)
        return items
    }

    private mutating func parseOptionalLimitOffset() throws -> (Expression?, Expression?) {
        var limit: Expression? = nil
        var offset: Expression? = nil
        if case .keyword(.limit) = current {
            advance()
            limit = try parseExpression()
        }
        if case .keyword(.offset) = current {
            advance()
            offset = try parseExpression()
        }
        return (limit, offset)
    }

    // MARK: - INSERT

    private mutating func parseInsert() throws -> InsertStatement {
        try expect(.keyword(.insert))
        try expect(.keyword(.into))
        let table = try parseIdentifier()

        var columns: [String]? = nil
        if case .leftParen = current {
            advance()
            var cols: [String] = []
            repeat {
                cols.append(try parseIdentifier())
            } while tryConsume(.comma)
            try expect(.rightParen)
            columns = cols
        }

        try expect(.keyword(.values))
        var allValues: [[Expression]] = []
        repeat {
            try expect(.leftParen)
            var row: [Expression] = []
            repeat {
                row.append(try parseExpression())
            } while tryConsume(.comma)
            try expect(.rightParen)
            allValues.append(row)
        } while tryConsume(.comma)

        // Optional ON CONFLICT clause
        let onConflict = try parseOptionalOnConflict()

        // Optional RETURNING clause
        let returning = try parseOptionalReturning()

        return InsertStatement(table: table, columns: columns, values: allValues, onConflict: onConflict, returning: returning)
    }

    // MARK: - ON CONFLICT

    private mutating func parseOptionalOnConflict() throws -> OnConflictClause? {
        guard case .keyword(.on) = current else { return nil }
        // Peek ahead to check for CONFLICT
        guard pos + 1 < tokens.count, case .keyword(.conflict) = tokens[pos + 1].0 else { return nil }
        advance() // consume ON
        advance() // consume CONFLICT

        // Optional conflict target: (col1, col2, ...)
        var conflictColumns: [String]? = nil
        if case .leftParen = current {
            advance()
            var cols: [String] = []
            repeat {
                cols.append(try parseIdentifier())
            } while tryConsume(.comma)
            try expect(.rightParen)
            conflictColumns = cols
        }

        try expect(.keyword(.do))

        if case .keyword(.nothing) = current {
            advance()
            return OnConflictClause(columns: conflictColumns, action: .doNothing)
        }

        // DO UPDATE SET ...
        try expect(.keyword(.update))
        try expect(.keyword(.set))

        var assignments: [OnConflictAssignment] = []
        repeat {
            let col = try parseIdentifier()
            try expect(.equals)
            let val = try parseExpression()
            assignments.append(OnConflictAssignment(column: col, value: val))
        } while tryConsume(.comma)

        return OnConflictClause(columns: conflictColumns, action: .doUpdate(assignments: assignments))
    }

    // MARK: - RETURNING

    private mutating func parseOptionalReturning() throws -> [SelectItem]? {
        guard case .keyword(.returning) = current else { return nil }
        advance()
        return try parseSelectList()
    }

    // MARK: - UPDATE

    private mutating func parseUpdate() throws -> UpdateStatement {
        try expect(.keyword(.update))
        let table = try parseIdentifier()
        try expect(.keyword(.set))

        var assignments: [(column: String, value: Expression)] = []
        repeat {
            let col = try parseIdentifier()
            try expect(.equals)
            let val = try parseExpression()
            assignments.append((column: col, value: val))
        } while tryConsume(.comma)

        let whereClause = try parseOptionalWhere()
        let returning = try parseOptionalReturning()
        return UpdateStatement(table: table, assignments: assignments, whereClause: whereClause, returning: returning)
    }

    // MARK: - DELETE

    private mutating func parseDelete() throws -> DeleteStatement {
        try expect(.keyword(.delete))
        try expect(.keyword(.from))
        let table = try parseIdentifier()
        let whereClause = try parseOptionalWhere()
        let returning = try parseOptionalReturning()
        return DeleteStatement(table: table, whereClause: whereClause, returning: returning)
    }

    // MARK: - VACUUM

    private mutating func parseVacuum() throws -> VacuumStatement {
        try expect(.keyword(.vacuum))
        // Optional table name
        var table: String? = nil
        if case .identifier(_) = current {
            table = try parseIdentifier()
        } else if case .keyword(_) = current, current != .eof && current != .semicolon {
            // Allow keywords as table names (e.g., VACUUM data)
            table = try parseIdentifier()
        }
        return VacuumStatement(table: table)
    }

    // MARK: - CREATE

    private mutating func parseCreate() throws -> Statement {
        try expect(.keyword(.create))

        // CREATE OR REPLACE VIEW ...
        var orReplace = false
        if case .keyword(.or) = current {
            advance()
            try expect(.keyword(.replace))
            orReplace = true
        }

        if case .keyword(.view) = current {
            advance()
            return .createView(try parseCreateViewBody(orReplace: orReplace))
        }

        if orReplace {
            throw SQLError.expectedToken(expected: "VIEW after OR REPLACE", found: describe(current), line: currentPos.line, column: currentPos.column)
        }

        if case .keyword(.table) = current {
            return .createTable(try parseCreateTable())
        } else if case .keyword(.unique) = current {
            advance()
            try expect(.keyword(.index))
            return .createIndex(try parseCreateIndexBody(unique: true))
        } else if case .keyword(.index) = current {
            advance()
            return .createIndex(try parseCreateIndexBody(unique: false))
        } else if case .keyword(.trigger) = current {
            advance()
            return .createTrigger(try parseCreateTriggerBody())
        }
        throw SQLError.expectedToken(expected: "TABLE, INDEX, VIEW, or TRIGGER", found: describe(current), line: currentPos.line, column: currentPos.column)
    }

    private mutating func parseCreateViewBody(orReplace: Bool) throws -> CreateViewStatement {
        let name = try parseIdentifier()

        // Optional column alias list
        var columns: [String]? = nil
        if case .leftParen = current {
            advance()
            var cols: [String] = []
            repeat {
                cols.append(try parseIdentifier())
            } while tryConsume(.comma)
            try expect(.rightParen)
            columns = cols
        }

        try expect(.keyword(.as))
        let query = try parseSelect()

        return CreateViewStatement(name: name, orReplace: orReplace, columns: columns, query: query)
    }

    private mutating func parseCreateTriggerBody() throws -> CreateTriggerStatement {
        let name = try parseIdentifier()

        // BEFORE | AFTER
        let timing: String
        if case .keyword(.before) = current {
            timing = "BEFORE"; advance()
        } else if case .keyword(.after) = current {
            timing = "AFTER"; advance()
        } else {
            throw SQLError.expectedToken(expected: "BEFORE or AFTER", found: describe(current), line: currentPos.line, column: currentPos.column)
        }

        // INSERT | UPDATE | DELETE
        let event: String
        if case .keyword(.insert) = current {
            event = "INSERT"; advance()
        } else if case .keyword(.update) = current {
            event = "UPDATE"; advance()
        } else if case .keyword(.delete) = current {
            event = "DELETE"; advance()
        } else {
            throw SQLError.expectedToken(expected: "INSERT, UPDATE, or DELETE", found: describe(current), line: currentPos.line, column: currentPos.column)
        }

        // ON table
        try expect(.keyword(.on))
        let table = try parseIdentifier()

        // FOR EACH ROW | STATEMENT
        try expect(.keyword(.for))
        try expect(.keyword(.each))
        let forEach: String
        if case .keyword(.row) = current {
            forEach = "ROW"; advance()
        } else {
            // Try identifier "STATEMENT" (it's a keyword in our enum)
            let id = try parseIdentifier()
            if id.uppercased() == "STATEMENT" {
                forEach = "STATEMENT"
            } else {
                throw SQLError.expectedToken(expected: "ROW or STATEMENT", found: id, line: currentPos.line, column: currentPos.column)
            }
        }

        // BEGIN stmt1; stmt2; ... END
        try expect(.keyword(.begin))
        var bodyStatements: [String] = []
        // We need to collect raw SQL statements between BEGIN and END.
        // Re-parse inner statements and collect their SQL text from tokens.
        while true {
            if case .keyword(.end) = current { break }
            if case .eof = current { break }
            // Parse one inner statement
            let innerStmt = try parseStatement()
            bodyStatements.append(statementToSQL(innerStmt))
            // Consume optional semicolons
            while case .semicolon = current { advance() }
        }
        try expect(.keyword(.end))

        return CreateTriggerStatement(name: name, timing: timing, event: event, table: table, forEach: forEach, body: bodyStatements)
    }

    /// Convert a parsed Statement back to SQL text (best-effort for trigger body execution).
    private func statementToSQL(_ stmt: Statement) -> String {
        switch stmt {
        case .insert(let ins):
            var sql = "INSERT INTO \(ins.table)"
            if let cols = ins.columns {
                sql += " (\(cols.joined(separator: ", ")))"
            }
            let rows = ins.values.map { row in
                "(" + row.map { exprToSQL($0) }.joined(separator: ", ") + ")"
            }
            sql += " VALUES " + rows.joined(separator: ", ")
            return sql
        case .update(let upd):
            var sql = "UPDATE \(upd.table) SET "
            sql += upd.assignments.map { "\($0.column) = \(exprToSQL($0.value))" }.joined(separator: ", ")
            if let w = upd.whereClause {
                sql += " WHERE \(exprToSQL(w))"
            }
            return sql
        case .delete(let del):
            var sql = "DELETE FROM \(del.table)"
            if let w = del.whereClause {
                sql += " WHERE \(exprToSQL(w))"
            }
            return sql
        case .select(let sel):
            var sql = "SELECT "
            let cols = sel.columns.map { item -> String in
                switch item {
                case .allColumns: return "*"
                case .tableAllColumns(let t): return "\(t).*"
                case .expression(let e, let alias):
                    var s = exprToSQL(e)
                    if let a = alias { s += " AS \(a)" }
                    return s
                }
            }
            sql += cols.joined(separator: ", ")
            if let from = sel.from {
                switch from {
                case .table(let name, _): sql += " FROM \(name)"
                case .subquery: sql += " FROM (subquery)"
                }
            }
            if let w = sel.whereClause {
                sql += " WHERE \(exprToSQL(w))"
            }
            return sql
        default:
            return ""
        }
    }

    /// Convert an expression back to SQL text (best-effort).
    private func exprToSQL(_ expr: Expression) -> String {
        switch expr {
        case .integerLiteral(let v): return "\(v)"
        case .doubleLiteral(let v): return "\(v)"
        case .stringLiteral(let v): return "'\(v.replacingOccurrences(of: "'", with: "''"))'"
        case .booleanLiteral(let v): return v ? "TRUE" : "FALSE"
        case .nullLiteral: return "NULL"
        case .column(let table, let name):
            if let t = table { return "\(t).\(name)" }
            return name
        case .wildcard: return "*"
        case .binaryOp(let left, let op, let right):
            let opStr: String
            switch op {
            case .add: opStr = "+"
            case .subtract: opStr = "-"
            case .multiply: opStr = "*"
            case .divide: opStr = "/"
            case .modulo: opStr = "%"
            case .equal: opStr = "="
            case .notEqual: opStr = "!="
            case .lessThan: opStr = "<"
            case .greaterThan: opStr = ">"
            case .lessOrEqual: opStr = "<="
            case .greaterOrEqual: opStr = ">="
            case .and: opStr = "AND"
            case .or: opStr = "OR"
            case .concat: opStr = "||"
            }
            return "\(exprToSQL(left)) \(opStr) \(exprToSQL(right))"
        case .unaryOp(let op, let operand):
            switch op {
            case .not: return "NOT \(exprToSQL(operand))"
            case .negate: return "-\(exprToSQL(operand))"
            }
        case .function(let name, let args):
            return "\(name)(\(args.map { exprToSQL($0) }.joined(separator: ", ")))"
        case .aggregate(let agg, let expr):
            if let e = expr { return "\(agg.rawValue)(\(exprToSQL(e)))" }
            return "\(agg.rawValue)(*)"
        case .isNull(let e): return "\(exprToSQL(e)) IS NULL"
        case .isNotNull(let e): return "\(exprToSQL(e)) IS NOT NULL"
        case .like(let e, let pattern): return "\(exprToSQL(e)) LIKE \(exprToSQL(pattern))"
        case .notLike(let e, let pattern): return "\(exprToSQL(e)) NOT LIKE \(exprToSQL(pattern))"
        case .between(let e, let low, let high): return "\(exprToSQL(e)) BETWEEN \(exprToSQL(low)) AND \(exprToSQL(high))"
        case .inList(let e, let vals): return "\(exprToSQL(e)) IN (\(vals.map { exprToSQL($0) }.joined(separator: ", ")))"
        case .cast(let e, let dt):
            let typeStr: String
            switch dt {
            case .integer: typeStr = "INTEGER"
            case .text: typeStr = "TEXT"
            case .real: typeStr = "REAL"
            case .blob: typeStr = "BLOB"
            case .boolean: typeStr = "BOOLEAN"
            case .varchar(let n): typeStr = n.map { "VARCHAR(\($0))" } ?? "VARCHAR"
            case .serial: typeStr = "SERIAL"
            }
            return "CAST(\(exprToSQL(e)) AS \(typeStr))"
        case .parameter(let n): return "$\(n)"
        default: return "?"
        }
    }

    private mutating func parseCreateTable() throws -> CreateTableStatement {
        try expect(.keyword(.table))

        var ifNotExists = false
        if case .keyword(.if) = current {
            advance()
            // expect NOT EXISTS
            if case .keyword(.not) = current { advance() }
            if case .keyword(.exists) = current { advance() }
            ifNotExists = true
        }

        let name = try parseIdentifier()
        try expect(.leftParen)

        var columns: [ColumnDef] = []
        repeat {
            columns.append(try parseColumnDef())
        } while tryConsume(.comma)

        try expect(.rightParen)
        return CreateTableStatement(name: name, ifNotExists: ifNotExists, columns: columns)
    }

    private mutating func parseColumnDef() throws -> ColumnDef {
        let name = try parseIdentifier()
        let dataType = try parseDataType()

        var isPrimaryKey = false
        var isNullable = true
        var isUnique = false
        var defaultValue: Expression? = nil

        // Parse column constraints
        while true {
            if case .keyword(.primary) = current {
                advance()
                try expect(.keyword(.key))
                isPrimaryKey = true
                isNullable = false
            } else if case .keyword(.not) = current {
                advance()
                try expect(.keyword(.null))
                isNullable = false
            } else if case .keyword(.null) = current {
                advance()
                isNullable = true
            } else if case .keyword(.unique) = current {
                advance()
                isUnique = true
            } else if case .keyword(.default) = current {
                advance()
                defaultValue = try parseExpression()
            } else {
                break
            }
        }

        return ColumnDef(name: name, dataType: dataType, isPrimaryKey: isPrimaryKey,
                        isNullable: isNullable, isUnique: isUnique, defaultValue: defaultValue)
    }

    private mutating func parseDataType() throws -> SQLDataType {
        let tok = current
        switch tok {
        case .keyword(.integer), .keyword(.serial):
            advance()
            return tok == .keyword(.serial) ? .serial : .integer
        case .keyword(.text):
            advance(); return .text
        case .keyword(.real):
            advance(); return .real
        case .keyword(.blob):
            advance(); return .blob
        case .keyword(.boolean):
            advance(); return .boolean
        case .keyword(.varchar):
            advance()
            if case .leftParen = current {
                advance()
                let len: Int?
                if case .integerLiteral(let v) = current {
                    len = Int(v)
                    advance()
                } else { len = nil }
                try expect(.rightParen)
                return .varchar(len)
            }
            return .varchar(nil)
        case .identifier(let name):
            advance()
            let upper = name.uppercased()
            switch upper {
            case "INT", "BIGINT", "SMALLINT": return .integer
            case "DOUBLE", "FLOAT", "NUMERIC", "DECIMAL": return .real
            case "CHAR", "CHARACTER": return .text
            case "BOOL": return .boolean
            default: return .text
            }
        default:
            throw SQLError.expectedToken(expected: "data type", found: describe(tok), line: currentPos.line, column: currentPos.column)
        }
    }

    private mutating func parseCreateIndexBody(unique: Bool) throws -> CreateIndexStatement {
        let name: String?
        if case .keyword(.on) = current {
            name = nil
        } else {
            name = try parseIdentifier()
        }
        try expect(.keyword(.on))
        let table = try parseIdentifier()
        try expect(.leftParen)
        var columns: [String] = []
        repeat {
            columns.append(try parseIdentifier())
        } while tryConsume(.comma)
        try expect(.rightParen)
        return CreateIndexStatement(name: name, table: table, columns: columns, unique: unique)
    }

    // MARK: - DROP

    private mutating func parseDrop() throws -> Statement {
        try expect(.keyword(.drop))
        if case .keyword(.table) = current {
            advance()
            var ifExists = false
            if case .keyword(.if) = current {
                advance()
                if case .keyword(.exists) = current { advance() }
                ifExists = true
            }
            let name = try parseIdentifier()
            return .dropTable(DropTableStatement(name: name, ifExists: ifExists))
        } else if case .keyword(.view) = current {
            advance()
            var ifExists = false
            if case .keyword(.if) = current {
                advance()
                if case .keyword(.exists) = current { advance() }
                ifExists = true
            }
            let name = try parseIdentifier()
            return .dropView(DropViewStatement(name: name, ifExists: ifExists))
        } else if case .keyword(.index) = current {
            advance()
            let name = try parseIdentifier()
            return .dropIndex(DropIndexStatement(name: name))
        } else if case .keyword(.trigger) = current {
            advance()
            let name = try parseIdentifier()
            return .dropTrigger(DropTriggerStatement(name: name))
        }
        throw SQLError.expectedToken(expected: "TABLE, INDEX, VIEW, or TRIGGER", found: describe(current), line: currentPos.line, column: currentPos.column)
    }

    // MARK: - ALTER TABLE

    private mutating func parseAlterTable() throws -> AlterTableStatement {
        try expect(.keyword(.alter))
        try expect(.keyword(.table))
        let table = try parseIdentifier()

        let action: AlterAction
        switch current {
        case .keyword(.add):
            advance()
            if case .keyword(.column) = current { advance() }
            action = .addColumn(try parseColumnDef())
        case .keyword(.drop):
            advance()
            if case .keyword(.column) = current { advance() }
            let colName = try parseIdentifier()
            action = .dropColumn(colName)
        case .keyword(.rename):
            advance()
            if case .keyword(.column) = current { advance() }
            let from = try parseIdentifier()
            try expect(.keyword(.to))
            let to = try parseIdentifier()
            action = .renameColumn(from: from, to: to)
        default:
            throw SQLError.expectedToken(expected: "ADD, DROP, or RENAME", found: describe(current), line: currentPos.line, column: currentPos.column)
        }

        return AlterTableStatement(table: table, action: action)
    }

    // MARK: - EXPLAIN

    private mutating func parseExplain() throws -> Statement {
        try expect(.keyword(.explain))
        var analyze = false
        if case .keyword(.analyze) = current {
            analyze = true
            advance()
        }
        let stmt = try parseStatement()
        return .explain(ExplainStatement(analyze: analyze, statement: stmt))
    }

    // MARK: - SET / SHOW / RESET / DISCARD

    private mutating func parseSet() throws -> SetStatement {
        try expect(.keyword(.set))
        let name = try parseIdentifier()
        // Accept both SET name = value and SET name TO value
        if case .equals = current {
            advance()
        } else if case .keyword(.to) = current {
            advance()
        } else {
            throw SQLError.expectedToken(expected: "= or TO", found: describe(current),
                                         line: currentPos.line, column: currentPos.column)
        }
        // Collect value tokens until semicolon or EOF (value can be a string, identifier, number, etc.)
        let value = try parseSetValue()
        return SetStatement(name: name, value: value)
    }

    /// Parse the value portion of a SET statement — handles quoted strings, identifiers, numbers,
    /// and multi-token values like `'ISO, MDY'` or `"$user", public`.
    private mutating func parseSetValue() throws -> String {
        var parts: [String] = []
        loop: while true {
            switch current {
            case .stringLiteral(let s):
                parts.append(s)
                advance()
            case .integerLiteral(let v):
                parts.append("\(v)")
                advance()
            case .doubleLiteral(let v):
                parts.append("\(v)")
                advance()
            case .identifier(let s):
                parts.append(s)
                advance()
            case .keyword(let kw):
                parts.append(kw.rawValue.lowercased())
                advance()
            case .semicolon, .eof:
                break loop
            case .comma:
                parts.append(",")
                advance()
            default:
                break loop
            }
        }
        if parts.isEmpty {
            throw SQLError.expectedToken(expected: "value", found: describe(current),
                                         line: currentPos.line, column: currentPos.column)
        }
        return parts.joined(separator: " ")
    }

    private mutating func parseShow() throws -> ShowStatement {
        try expect(.keyword(.show))
        if case .keyword(.all) = current {
            advance()
            return ShowStatement(name: "ALL")
        }
        let name = try parseIdentifier()
        return ShowStatement(name: name)
    }

    private mutating func parseReset() throws -> ResetStatement {
        try expect(.keyword(.reset))
        if case .keyword(.all) = current {
            advance()
            return ResetStatement(name: "ALL")
        }
        let name = try parseIdentifier()
        return ResetStatement(name: name)
    }

    private mutating func parseDiscard() throws -> DiscardStatement {
        try expect(.keyword(.discard))
        if case .keyword(.all) = current {
            advance()
            return DiscardStatement(target: "ALL")
        }
        let target = try parseIdentifier()
        return DiscardStatement(target: target.uppercased())
    }

    // MARK: - Expression Parser (Pratt Precedence)

    private mutating func parseExpression(minPrecedence: Int = 0) throws -> Expression {
        var left = try parsePrimary()

        // Handle :: type cast (highest precedence postfix operator)
        while case .doubleColon = current {
            advance()
            let dataType = try parseDataType()
            left = .cast(left, dataType)
        }

        while true {
            guard let (op, prec) = binaryOp(current), prec >= minPrecedence else { break }
            advance()
            let right = try parseExpression(minPrecedence: prec + 1)
            left = .binaryOp(left: left, op: op, right: right)
        }

        // Handle postfix operators: IS NULL, IS NOT NULL, IN, BETWEEN, LIKE, NOT IN, NOT LIKE, NOT BETWEEN
        left = try parsePostfixOps(left)

        return left
    }

    private mutating func parsePrimary() throws -> Expression {
        let tok = current

        switch tok {
        case .integerLiteral(let v):
            advance(); return .integerLiteral(v)
        case .doubleLiteral(let v):
            advance(); return .doubleLiteral(v)
        case .stringLiteral(let v):
            advance(); return .stringLiteral(v)
        case .keyword(.true):
            advance(); return .booleanLiteral(true)
        case .keyword(.false):
            advance(); return .booleanLiteral(false)
        case .keyword(.null):
            advance(); return .nullLiteral
        case .star:
            advance(); return .wildcard
        case .minus:
            advance()
            let operand = try parsePrimary()
            // Fold constant negation
            if case .integerLiteral(let v) = operand { return .integerLiteral(-v) }
            if case .doubleLiteral(let v) = operand { return .doubleLiteral(-v) }
            return .unaryOp(op: .negate, operand: operand)
        case .keyword(.not):
            advance()
            if case .keyword(.exists) = current {
                advance()
                try expect(.leftParen)
                let select = try parseSelect()
                try expect(.rightParen)
                return .unaryOp(op: .not, operand: .exists(select))
            }
            let operand = try parseExpression(minPrecedence: precedence(of: .not))
            return .unaryOp(op: .not, operand: operand)
        case .keyword(.exists):
            advance()
            try expect(.leftParen)
            let select = try parseSelect()
            try expect(.rightParen)
            return .exists(select)
        case .keyword(.case):
            return try parseCaseExpression()
        case .keyword(.cast):
            return try parseCast()
        case .parameter(let n):
            advance(); return .parameter(n)
        case .leftParen:
            advance()
            // Could be subquery or grouped expression
            if case .keyword(.select) = current {
                let select = try parseSelect()
                try expect(.rightParen)
                return .subquery(select)
            }
            let expr = try parseExpression()
            try expect(.rightParen)
            return expr
        // Aggregate functions
        case .keyword(.count), .keyword(.sum), .keyword(.avg), .keyword(.min), .keyword(.max):
            return try parseAggregateFunction()
        case .identifier(let name):
            advance()
            // Function call?
            if case .leftParen = current {
                advance()
                var args: [Expression] = []
                if current != .rightParen {
                    repeat {
                        args.append(try parseExpression())
                    } while tryConsume(.comma)
                }
                try expect(.rightParen)
                return .function(name: name, args: args)
            }
            // Qualified column: name.column
            if case .dot = current {
                advance()
                if case .star = current {
                    advance()
                    return .column(table: name, name: "*")
                }
                let colName = try parseIdentifier()
                return .column(table: name, name: colName)
            }
            return .column(table: nil, name: name)
        case .keyword(let kw) where Self.identifierKeywords.contains(kw):
            // Non-syntax keywords used as column/table names or function names (e.g., key, replace, etc.)
            let name = kw.rawValue.lowercased()
            advance()
            // Function call?
            if case .leftParen = current {
                advance()
                var args: [Expression] = []
                if current != .rightParen {
                    repeat {
                        args.append(try parseExpression())
                    } while tryConsume(.comma)
                }
                try expect(.rightParen)
                return .function(name: name, args: args)
            }
            if case .dot = current {
                advance()
                if case .star = current {
                    advance()
                    return .column(table: name, name: "*")
                }
                let colName = try parseIdentifier()
                return .column(table: name, name: colName)
            }
            return .column(table: nil, name: name)
        default:
            throw SQLError.expectedToken(expected: "expression", found: describe(tok), line: currentPos.line, column: currentPos.column)
        }
    }

    private mutating func parsePostfixOps(_ expr: Expression) throws -> Expression {
        var result = expr

        while true {
            if case .keyword(.is) = current {
                advance()
                if case .keyword(.not) = current {
                    advance()
                    try expect(.keyword(.null))
                    result = .isNotNull(result)
                } else {
                    try expect(.keyword(.null))
                    result = .isNull(result)
                }
            } else if case .keyword(.not) = current {
                let saved = pos
                advance()
                if case .keyword(.in) = current {
                    advance()
                    let inExpr = try parseInList(result)
                    result = .unaryOp(op: .not, operand: inExpr)
                } else if case .keyword(.like) = current {
                    advance()
                    let pattern = try parseExpression(minPrecedence: 10)
                    result = .notLike(result, pattern: pattern)
                } else if case .keyword(.between) = current {
                    advance()
                    let low = try parseExpression(minPrecedence: 10)
                    try expect(.keyword(.and))
                    let high = try parseExpression(minPrecedence: 10)
                    result = .unaryOp(op: .not, operand: .between(result, low: low, high: high))
                } else {
                    pos = saved
                    break
                }
            } else if case .keyword(.in) = current {
                advance()
                result = try parseInList(result)
            } else if case .keyword(.between) = current {
                advance()
                let low = try parseExpression(minPrecedence: 10)
                try expect(.keyword(.and))
                let high = try parseExpression(minPrecedence: 10)
                result = .between(result, low: low, high: high)
            } else if case .keyword(.like) = current {
                advance()
                let pattern = try parseExpression(minPrecedence: 10)
                result = .like(result, pattern: pattern)
            } else {
                break
            }
        }

        return result
    }

    private mutating func parseInList(_ expr: Expression) throws -> Expression {
        try expect(.leftParen)
        if case .keyword(.select) = current {
            let select = try parseSelect()
            try expect(.rightParen)
            return .inSubquery(expr, select)
        }
        var values: [Expression] = []
        repeat {
            values.append(try parseExpression())
        } while tryConsume(.comma)
        try expect(.rightParen)
        return .inList(expr, values)
    }

    private mutating func parseAggregateFunction() throws -> Expression {
        let aggType: AggregateType
        switch current {
        case .keyword(.count): aggType = .count
        case .keyword(.sum): aggType = .sum
        case .keyword(.avg): aggType = .avg
        case .keyword(.min): aggType = .min
        case .keyword(.max): aggType = .max
        default:
            throw SQLError.expectedToken(expected: "aggregate function", found: describe(current),
                                         line: currentPos.line, column: currentPos.column)
        }
        advance()
        try expect(.leftParen)

        let arg: Expression?
        if case .star = current {
            advance()
            arg = nil
        } else {
            arg = try parseExpression()
        }
        try expect(.rightParen)
        return .aggregate(aggType, arg)
    }

    private mutating func parseCaseExpression() throws -> Expression {
        try expect(.keyword(.case))
        var operand: Expression? = nil
        if case .keyword(.when) = current { /* no operand */ }
        else { operand = try parseExpression() }

        var whens: [(condition: Expression, result: Expression)] = []
        while case .keyword(.when) = current {
            advance()
            let condition = try parseExpression()
            try expect(.keyword(.then))
            let result = try parseExpression()
            whens.append((condition: condition, result: result))
        }

        var elseResult: Expression? = nil
        if case .keyword(.else) = current {
            advance()
            elseResult = try parseExpression()
        }
        try expect(.keyword(.end))

        return .caseExpr(operand: operand, whens: whens, elseResult: elseResult)
    }

    private mutating func parseCast() throws -> Expression {
        try expect(.keyword(.cast))
        try expect(.leftParen)
        let expr = try parseExpression()
        try expect(.keyword(.as))
        let dataType = try parseDataType()
        try expect(.rightParen)
        return .cast(expr, dataType)
    }

    // MARK: - Keywords Usable as Identifiers

    /// Keywords that can be used as column/table names in expression context.
    /// Excludes keywords that start SQL clauses or operators (AND, OR, FROM, WHERE, etc.)
    private static let identifierKeywords: Set<Keyword> = [
        .key, .column, .index, .table, .to, .add, .rename, .constraint, .cascade, .restrict,
        .references, .foreign, .check, .primary, .unique, .serial, .integer, .text, .real,
        .blob, .boolean, .varchar, .analyze, .transaction, .partition, .over, .rows, .range,
        .unbounded, .preceding, .following, .current, .row, .recursive, .with,
        .rowNumber, .rank, .denseRank, .lag, .lead, .asc, .desc,
        .trigger, .before, .after, .each, .vacuum,
        .show, .reset, .discard,
        .replace, .returning, .conflict, .do, .nothing, .view,
        .left, .right,
    ]

    // MARK: - Precedence

    private func binaryOp(_ token: Token) -> (BinaryOperator, Int)? {
        switch token {
        case .keyword(.or): return (.or, 1)
        case .keyword(.and): return (.and, 2)
        case .equals: return (.equal, 4)
        case .notEquals: return (.notEqual, 4)
        case .lessThan: return (.lessThan, 5)
        case .greaterThan: return (.greaterThan, 5)
        case .lessOrEqual: return (.lessOrEqual, 5)
        case .greaterOrEqual: return (.greaterOrEqual, 5)
        case .plus: return (.add, 6)
        case .minus: return (.subtract, 6)
        case .star: return (.multiply, 7)
        case .slash: return (.divide, 7)
        case .percent: return (.modulo, 7)
        case .concat: return (.concat, 3)
        default: return nil
        }
    }

    private func precedence(of op: UnaryOperator) -> Int {
        switch op {
        case .not: return 3
        case .negate: return 8
        }
    }

    // MARK: - Helpers

    private var current: Token {
        pos < tokens.count ? tokens[pos].0 : .eof
    }

    private var currentPos: SourcePosition {
        pos < tokens.count ? tokens[pos].1 : SourcePosition(line: 0, column: 0)
    }

    private mutating func advance() {
        pos += 1
    }

    @discardableResult
    private mutating func expect(_ token: Token) throws -> Token {
        if current == token {
            let t = current
            advance()
            return t
        }
        throw SQLError.expectedToken(expected: describe(token), found: describe(current),
                                     line: currentPos.line, column: currentPos.column)
    }

    private mutating func tryConsume(_ token: Token) -> Bool {
        if current == token {
            advance()
            return true
        }
        return false
    }

    private mutating func parseIdentifier() throws -> String {
        switch current {
        case .identifier(let name):
            advance(); return name
        case .keyword(let kw):
            // Allow some keywords as identifiers in certain contexts
            advance(); return kw.rawValue.lowercased()
        default:
            throw SQLError.expectedToken(expected: "identifier", found: describe(current),
                                         line: currentPos.line, column: currentPos.column)
        }
    }

    private func isKeywordAt(_ index: Int) -> Bool {
        guard index < tokens.count else { return false }
        if case .keyword(_) = tokens[index].0 {
            // Check if it's a keyword that could start a clause
            switch tokens[index].0 {
            case .keyword(.from), .keyword(.where), .keyword(.order), .keyword(.group),
                 .keyword(.having), .keyword(.limit), .keyword(.offset), .keyword(.join),
                 .keyword(.inner), .keyword(.left), .keyword(.right), .keyword(.cross),
                 .keyword(.on), .keyword(.union), .keyword(.intersect), .keyword(.except),
                 .keyword(.values), .keyword(.set), .keyword(.returning):
                return true
            default:
                return false
            }
        }
        return false
    }

    private func describe(_ token: Token) -> String {
        switch token {
        case .keyword(let kw): return kw.rawValue
        case .identifier(let name): return "identifier '\(name)'"
        case .integerLiteral(let v): return "integer \(v)"
        case .doubleLiteral(let v): return "double \(v)"
        case .stringLiteral(let v): return "string '\(v)'"
        case .eof: return "end of input"
        case .leftParen: return "("
        case .rightParen: return ")"
        case .comma: return ","
        case .equals: return "="
        case .star: return "*"
        case .dot: return "."
        case .semicolon: return ";"
        default: return "\(token)"
        }
    }
}

// MARK: - Convenience

extension Parser {
    /// Parse a SQL string into a Statement
    public static func parse(_ sql: String) throws -> Statement {
        var lexer = Lexer(sql)
        let tokens = try lexer.tokenize()
        var parser = Parser(tokens: tokens)
        return try parser.parseStatement()
    }
}
