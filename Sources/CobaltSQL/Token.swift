/// SQL keywords recognized by the lexer
public enum Keyword: String, Sendable, CaseIterable {
    case select = "SELECT"
    case from = "FROM"
    case `where` = "WHERE"
    case and = "AND"
    case or = "OR"
    case not = "NOT"
    case insert = "INSERT"
    case into = "INTO"
    case values = "VALUES"
    case update = "UPDATE"
    case set = "SET"
    case delete = "DELETE"
    case create = "CREATE"
    case drop = "DROP"
    case table = "TABLE"
    case alter = "ALTER"
    case add = "ADD"
    case column = "COLUMN"
    case rename = "RENAME"
    case to = "TO"
    case index = "INDEX"
    case on = "ON"
    case `in` = "IN"
    case between = "BETWEEN"
    case like = "LIKE"
    case `is` = "IS"
    case null = "NULL"
    case `true` = "TRUE"
    case `false` = "FALSE"
    case `as` = "AS"
    case order = "ORDER"
    case by = "BY"
    case asc = "ASC"
    case desc = "DESC"
    case limit = "LIMIT"
    case offset = "OFFSET"
    case distinct = "DISTINCT"
    case join = "JOIN"
    case inner = "INNER"
    case left = "LEFT"
    case right = "RIGHT"
    case cross = "CROSS"
    case outer = "OUTER"
    case group = "GROUP"
    case having = "HAVING"
    case count = "COUNT"
    case sum = "SUM"
    case avg = "AVG"
    case min = "MIN"
    case max = "MAX"
    case union = "UNION"
    case all = "ALL"
    case intersect = "INTERSECT"
    case except = "EXCEPT"
    case exists = "EXISTS"
    case `case` = "CASE"
    case when = "WHEN"
    case then = "THEN"
    case `else` = "ELSE"
    case end = "END"
    case cast = "CAST"
    case begin = "BEGIN"
    case commit = "COMMIT"
    case rollback = "ROLLBACK"
    case transaction = "TRANSACTION"
    case explain = "EXPLAIN"
    case analyze = "ANALYZE"
    case `with` = "WITH"
    case recursive = "RECURSIVE"
    case over = "OVER"
    case partition = "PARTITION"
    case primary = "PRIMARY"
    case key = "KEY"
    case unique = "UNIQUE"
    case `default` = "DEFAULT"
    case constraint = "CONSTRAINT"
    case foreign = "FOREIGN"
    case references = "REFERENCES"
    case check = "CHECK"
    case cascade = "CASCADE"
    case restrict = "RESTRICT"
    case integer = "INTEGER"
    case text = "TEXT"
    case real = "REAL"
    case blob = "BLOB"
    case boolean = "BOOLEAN"
    case varchar = "VARCHAR"
    case serial = "SERIAL"
    case `if` = "IF"
    case rowNumber = "ROW_NUMBER"
    case rank = "RANK"
    case denseRank = "DENSE_RANK"
    case lag = "LAG"
    case lead = "LEAD"
    case rows = "ROWS"
    case range = "RANGE"
    case unbounded = "UNBOUNDED"
    case preceding = "PRECEDING"
    case following = "FOLLOWING"
    case current = "CURRENT"
    case row = "ROW"
    case trigger = "TRIGGER"
    case before = "BEFORE"
    case after = "AFTER"
    case `for` = "FOR"
    case each = "EACH"
    case vacuum = "VACUUM"
    case show = "SHOW"
    case reset = "RESET"
    case discard = "DISCARD"
    case returning = "RETURNING"
    case conflict = "CONFLICT"
    case `do` = "DO"
    case nothing = "NOTHING"
    case view = "VIEW"
    case replace = "REPLACE"

    /// Lookup table for case-insensitive keyword matching
    static let lookup: [String: Keyword] = {
        var dict = [String: Keyword](minimumCapacity: allCases.count)
        for kw in allCases {
            dict[kw.rawValue] = kw
        }
        return dict
    }()
}

/// Position in source text
public struct SourcePosition: Sendable, Equatable {
    public let line: Int
    public let column: Int

    public init(line: Int, column: Int) {
        self.line = line
        self.column = column
    }
}

/// A token produced by the SQL lexer
public enum Token: Sendable, Equatable {
    // Literals
    case integerLiteral(Int64)
    case doubleLiteral(Double)
    case stringLiteral(String)
    case blobLiteral(Data)

    // Identifiers and keywords
    case identifier(String)
    case keyword(Keyword)

    // Operators
    case plus           // +
    case minus          // -
    case star           // *
    case slash          // /
    case percent        // %
    case equals         // =
    case notEquals      // != or <>
    case lessThan       // <
    case greaterThan    // >
    case lessOrEqual    // <=
    case greaterOrEqual // >=
    case concat         // ||

    // Punctuation
    case leftParen      // (
    case rightParen     // )
    case comma          // ,
    case semicolon      // ;
    case dot            // .
    case colon          // :
    case doubleColon    // ::

    // Parameter
    case parameter(Int) // $1, $2, ...

    // End of input
    case eof
}

import Foundation
