/// Errors produced during SQL parsing and lowering
public enum SQLError: Error, Sendable {
    /// Lexer encountered an unexpected character
    case unexpectedCharacter(Character, line: Int, column: Int)
    /// Parser expected a specific token but found something else
    case expectedToken(expected: String, found: String, line: Int, column: Int)
    /// Unexpected end of input
    case unexpectedEOF
    /// A syntactically valid but semantically invalid construct
    case semanticError(String)
    /// Feature not yet supported
    case unsupported(String)
}
