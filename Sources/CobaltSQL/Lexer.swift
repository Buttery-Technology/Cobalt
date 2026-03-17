import Foundation

/// Single-pass hand-written SQL tokenizer
public struct Lexer: Sendable {
    private let source: [Character]
    private var pos: Int = 0
    private var line: Int = 1
    private var col: Int = 1

    public init(_ sql: String) {
        self.source = Array(sql)
    }

    /// Tokenize the entire input into an array of (Token, SourcePosition)
    public mutating func tokenize() throws -> [(Token, SourcePosition)] {
        var tokens: [(Token, SourcePosition)] = []
        while true {
            let tok = try nextToken()
            tokens.append(tok)
            if case .eof = tok.0 { break }
        }
        return tokens
    }

    private mutating func nextToken() throws -> (Token, SourcePosition) {
        skipWhitespaceAndComments()

        let startPos = SourcePosition(line: line, column: col)

        guard pos < source.count else {
            return (.eof, startPos)
        }

        let ch = source[pos]

        // String literals
        if ch == "'" {
            return (try lexString(), startPos)
        }

        // Numbers
        if ch.isNumber || (ch == "." && pos + 1 < source.count && source[pos + 1].isNumber) {
            return (try lexNumber(), startPos)
        }

        // Identifiers and keywords
        if ch.isLetter || ch == "_" || ch == "\"" {
            return (lexIdentifierOrKeyword(), startPos)
        }

        // Parameter ($1, $2, ...)
        if ch == "$" && pos + 1 < source.count && source[pos + 1].isNumber {
            advance()
            var numStr = ""
            while pos < source.count && source[pos].isNumber {
                numStr.append(source[pos])
                advance()
            }
            return (.parameter(Int(numStr) ?? 0), startPos)
        }

        // Operators and punctuation
        switch ch {
        case "+": advance(); return (.plus, startPos)
        case "-":
            advance()
            if pos < source.count && source[pos] == "-" {
                // Line comment (shouldn't reach here due to skipWhitespaceAndComments, but safety)
                while pos < source.count && source[pos] != "\n" { advance() }
                return try nextToken()
            }
            return (.minus, startPos)
        case "*": advance(); return (.star, startPos)
        case "/": advance(); return (.slash, startPos)
        case "%": advance(); return (.percent, startPos)
        case "=": advance(); return (.equals, startPos)
        case "<":
            advance()
            if pos < source.count && source[pos] == "=" {
                advance(); return (.lessOrEqual, startPos)
            }
            if pos < source.count && source[pos] == ">" {
                advance(); return (.notEquals, startPos)
            }
            return (.lessThan, startPos)
        case ">":
            advance()
            if pos < source.count && source[pos] == "=" {
                advance(); return (.greaterOrEqual, startPos)
            }
            return (.greaterThan, startPos)
        case "!":
            advance()
            if pos < source.count && source[pos] == "=" {
                advance(); return (.notEquals, startPos)
            }
            throw SQLError.unexpectedCharacter("!", line: line, column: col - 1)
        case "|":
            advance()
            if pos < source.count && source[pos] == "|" {
                advance(); return (.concat, startPos)
            }
            throw SQLError.unexpectedCharacter("|", line: line, column: col - 1)
        case "(": advance(); return (.leftParen, startPos)
        case ")": advance(); return (.rightParen, startPos)
        case ",": advance(); return (.comma, startPos)
        case ";": advance(); return (.semicolon, startPos)
        case ".": advance(); return (.dot, startPos)
        case ":":
            advance()
            if pos < source.count && source[pos] == ":" {
                advance(); return (.doubleColon, startPos)
            }
            return (.colon, startPos)
        default:
            throw SQLError.unexpectedCharacter(ch, line: line, column: col)
        }
    }

    private mutating func lexString() throws -> Token {
        advance() // skip opening quote
        var result = ""
        while pos < source.count {
            let ch = source[pos]
            if ch == "'" {
                advance()
                // Escaped quote ''
                if pos < source.count && source[pos] == "'" {
                    result.append("'")
                    advance()
                } else {
                    return .stringLiteral(result)
                }
            } else {
                result.append(ch)
                advance()
            }
        }
        throw SQLError.unexpectedEOF
    }

    private mutating func lexNumber() throws -> Token {
        var numStr = ""
        var isDouble = false

        while pos < source.count && (source[pos].isNumber || source[pos] == ".") {
            if source[pos] == "." {
                if isDouble { break } // second dot = stop
                isDouble = true
            }
            numStr.append(source[pos])
            advance()
        }

        // Scientific notation
        if pos < source.count && (source[pos] == "e" || source[pos] == "E") {
            isDouble = true
            numStr.append(source[pos])
            advance()
            if pos < source.count && (source[pos] == "+" || source[pos] == "-") {
                numStr.append(source[pos])
                advance()
            }
            while pos < source.count && source[pos].isNumber {
                numStr.append(source[pos])
                advance()
            }
        }

        if isDouble {
            guard let value = Double(numStr) else {
                throw SQLError.unexpectedCharacter(numStr.first ?? " ", line: line, column: col)
            }
            return .doubleLiteral(value)
        } else {
            guard let value = Int64(numStr) else {
                throw SQLError.unexpectedCharacter(numStr.first ?? " ", line: line, column: col)
            }
            return .integerLiteral(value)
        }
    }

    private mutating func lexIdentifierOrKeyword() -> Token {
        // Quoted identifier "name"
        if source[pos] == "\"" {
            advance()
            var name = ""
            while pos < source.count && source[pos] != "\"" {
                name.append(source[pos])
                advance()
            }
            if pos < source.count { advance() } // skip closing quote
            return .identifier(name)
        }

        var name = ""
        while pos < source.count && (source[pos].isLetter || source[pos].isNumber || source[pos] == "_") {
            name.append(source[pos])
            advance()
        }

        let upper = name.uppercased()
        if let kw = Keyword.lookup[upper] {
            return .keyword(kw)
        }
        return .identifier(name)
    }

    private mutating func skipWhitespaceAndComments() {
        while pos < source.count {
            let ch = source[pos]
            if ch.isWhitespace {
                advance()
            } else if ch == "-" && pos + 1 < source.count && source[pos + 1] == "-" {
                // Line comment
                while pos < source.count && source[pos] != "\n" { advance() }
            } else if ch == "/" && pos + 1 < source.count && source[pos + 1] == "*" {
                // Block comment
                advance(); advance()
                while pos + 1 < source.count {
                    if source[pos] == "*" && source[pos + 1] == "/" {
                        advance(); advance()
                        break
                    }
                    advance()
                }
            } else {
                break
            }
        }
    }

    private mutating func advance() {
        if pos < source.count {
            if source[pos] == "\n" {
                line += 1
                col = 1
            } else {
                col += 1
            }
            pos += 1
        }
    }
}
