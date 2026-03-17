// ConnectionHandler.swift — Per-connection NIO channel handler
import NIOCore
import NIOPosix
import Cobalt
import CobaltSQL
import CobaltCore
import Foundation

final class ConnectionHandler: ChannelInboundHandler, @unchecked Sendable {
    typealias InboundIn = ByteBuffer
    typealias OutboundOut = ByteBuffer

    private let database: CobaltDatabase
    private var txStatus: TransactionStatus = .idle
    private var startupDone = false
    private let decoder = FrontendMessageDecoder()
    private let encoder = BackendMessageEncoder()
    private var accumulated = ByteBuffer()

    // Extended query protocol state
    private var preparedStatements: [String: PreparedStatement] = [:]
    private var portals: [String: Portal] = [:]

    struct PreparedStatement {
        let query: String
        let paramTypes: [Int32]
    }

    struct Portal {
        let statement: PreparedStatement
        let paramValues: [String?]  // text-format parameter values
        let resolvedSQL: String     // query with parameters substituted
    }

    init(database: CobaltDatabase) {
        self.database = database
    }

    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        var buf = unwrapInboundIn(data)
        accumulated.writeBuffer(&buf)

        if !startupDone {
            handleStartup(context: context)
        } else {
            handleMessages(context: context)
        }
    }

    private func handleStartup(context: ChannelHandlerContext) {
        guard let message = decoder.decodeStartup(&accumulated) else { return }

        switch message {
        case .sslRequest:
            // Deny SSL — send 'N'
            var out = context.channel.allocator.buffer(capacity: 1)
            out.writeInteger(UInt8(ascii: "N"))
            context.writeAndFlush(wrapOutboundOut(out), promise: nil)
            // Client will retry with a regular startup message
            return

        case .startup:
            startupDone = true
            var out = context.channel.allocator.buffer(capacity: 256)

            encoder.encode(.authenticationOk, into: &out)

            // Send some standard parameter status messages
            let params: [(String, String)] = [
                ("server_version", "15.0"),
                ("server_encoding", "UTF8"),
                ("client_encoding", "UTF8"),
                ("DateStyle", "ISO, MDY"),
                ("integer_datetimes", "on"),
                ("standard_conforming_strings", "on"),
                ("application_name", ""),
                ("is_superuser", "on"),
                ("session_authorization", "cobalt"),
                ("TimeZone", "UTC"),
            ]
            for (name, value) in params {
                encoder.encode(.parameterStatus(name: name, value: value), into: &out)
            }

            encoder.encode(.backendKeyData(processID: Int32(ProcessInfo.processInfo.processIdentifier), secretKey: 0), into: &out)
            encoder.encode(.readyForQuery(txStatus), into: &out)

            context.writeAndFlush(wrapOutboundOut(out), promise: nil)

            // Process any remaining bytes as regular messages
            if accumulated.readableBytes > 0 {
                handleMessages(context: context)
            }

        default:
            break
        }
    }

    private func handleMessages(context: ChannelHandlerContext) {
        while accumulated.readableBytes >= 5 {
            guard let message = decoder.decode(&accumulated) else { break }

            switch message {
            case .query(let sql):
                handleQuery(sql, context: context)

            case .parse(let name, let query, let paramTypes):
                handleParse(name: name, query: query, paramTypes: paramTypes, context: context)

            case .bind(let portal, let statement, let paramValues):
                handleBind(portal: portal, statement: statement, paramValues: paramValues, context: context)

            case .describe(let type, let name):
                handleDescribe(type: type, name: name, context: context)

            case .execute(let portal, _):
                handleExecute(portal: portal, context: context)

            case .sync:
                var out = context.channel.allocator.buffer(capacity: 8)
                encoder.encode(.readyForQuery(txStatus), into: &out)
                context.writeAndFlush(wrapOutboundOut(out), promise: nil)

            case .terminate:
                context.close(promise: nil)
                return

            default:
                break
            }
        }
    }

    // MARK: - Extended Query Protocol

    private func handleParse(name: String, query: String, paramTypes: [Int32], context: ChannelHandlerContext) {
        preparedStatements[name] = PreparedStatement(query: query, paramTypes: paramTypes)
        var out = context.channel.allocator.buffer(capacity: 16)
        encoder.encode(.parseComplete, into: &out)
        context.writeAndFlush(wrapOutboundOut(out), promise: nil)
    }

    private func handleBind(portal: String, statement: String, paramValues: [Data?], context: ChannelHandlerContext) {
        guard let stmt = preparedStatements[statement] else {
            var out = context.channel.allocator.buffer(capacity: 256)
            encoder.encode(.errorResponse(
                severity: "ERROR",
                code: "26000",
                message: "prepared statement \"\(statement)\" does not exist"
            ), into: &out)
            context.writeAndFlush(wrapOutboundOut(out), promise: nil)
            return
        }

        // Convert Data? params to String? (text format)
        let textParams: [String?] = paramValues.map { data in
            guard let data = data else { return nil }
            return String(data: data, encoding: .utf8)
        }

        let resolvedSQL = ConnectionHandler.substituteParameters(query: stmt.query, params: textParams)
        portals[portal] = Portal(statement: stmt, paramValues: textParams, resolvedSQL: resolvedSQL)

        var out = context.channel.allocator.buffer(capacity: 16)
        encoder.encode(.bindComplete, into: &out)
        context.writeAndFlush(wrapOutboundOut(out), promise: nil)
    }

    private func handleDescribe(type: DescribeType, name: String, context: ChannelHandlerContext) {
        var out = context.channel.allocator.buffer(capacity: 256)

        switch type {
        case .statement:
            if let stmt = preparedStatements[name] {
                // Send ParameterDescription
                let paramOIDs = stmt.paramTypes.isEmpty
                    ? Array(repeating: Int32(0), count: countPlaceholders(in: stmt.query))
                    : stmt.paramTypes
                encoder.encode(.parameterDescription(paramOIDs), into: &out)

                // Send RowDescription or NoData based on query type
                let upper = stmt.query.uppercased().trimmingCharacters(in: .whitespacesAndNewlines)
                if upper.hasPrefix("SELECT") || upper.hasPrefix("WITH") || upper.contains("RETURNING") {
                    // We can't know columns until we execute, send NoData for now
                    // (most drivers tolerate this and use the Execute response)
                    encoder.encode(.noData, into: &out)
                } else {
                    encoder.encode(.noData, into: &out)
                }
            } else {
                encoder.encode(.errorResponse(
                    severity: "ERROR",
                    code: "26000",
                    message: "prepared statement \"\(name)\" does not exist"
                ), into: &out)
            }

        case .portal:
            if portals[name] != nil {
                // Portal describe - same approach, send NoData
                encoder.encode(.noData, into: &out)
            } else {
                encoder.encode(.errorResponse(
                    severity: "ERROR",
                    code: "34000",
                    message: "portal \"\(name)\" does not exist"
                ), into: &out)
            }
        }

        context.writeAndFlush(wrapOutboundOut(out), promise: nil)
    }

    private func handleExecute(portal portalName: String, context: ChannelHandlerContext) {
        guard let portal = portals[portalName] else {
            var out = context.channel.allocator.buffer(capacity: 256)
            encoder.encode(.errorResponse(
                severity: "ERROR",
                code: "34000",
                message: "portal \"\(portalName)\" does not exist"
            ), into: &out)
            context.writeAndFlush(wrapOutboundOut(out), promise: nil)
            return
        }

        let sql = portal.resolvedSQL
        let db = self.database
        let eventLoop = context.eventLoop
        nonisolated(unsafe) let ctx = context

        let promise = eventLoop.makePromise(of: Void.self)

        promise.completeWithTask {
            do {
                let result = try await db.execute(sql: sql)
                eventLoop.execute {
                    self.sendExecuteResult(result, sql: sql, context: ctx)
                }
            } catch {
                eventLoop.execute {
                    self.sendExecuteError(error, context: ctx)
                }
            }
        }
    }

    /// Send result for Execute (no ReadyForQuery — that comes with Sync)
    private func sendExecuteResult(_ result: QueryResult, sql: String, context: ChannelHandlerContext) {
        var out = context.channel.allocator.buffer(capacity: 1024)

        switch result {
        case .rows(let rows):
            if rows.isEmpty {
                encoder.encode(.commandComplete("SELECT 0"), into: &out)
            } else {
                let columnNames = rows[0].values.keys.sorted()
                for row in rows {
                    let columns: [[UInt8]?] = columnNames.map { name in
                        guard let value = row.values[name], value != .null else {
                            return nil
                        }
                        return Array(TypeEncoding.encodeText(value).utf8)
                    }
                    encoder.encode(.dataRow(columns), into: &out)
                }
                encoder.encode(.commandComplete("SELECT \(rows.count)"), into: &out)
            }

        case .rowCount(let count):
            let tag = commandTag(for: sql, count: count)
            encoder.encode(.commandComplete(tag), into: &out)

        case .ok:
            let tag = commandTag(for: sql, count: 0)
            encoder.encode(.commandComplete(tag), into: &out)
        }

        context.writeAndFlush(wrapOutboundOut(out), promise: nil)
    }

    /// Send error for Execute (no ReadyForQuery — that comes with Sync)
    private func sendExecuteError(_ error: Error, context: ChannelHandlerContext) {
        let code = ConnectionHandler.sqlStateCode(for: error)
        if txStatus == .inTransaction {
            txStatus = .failed
        }
        var out = context.channel.allocator.buffer(capacity: 256)
        encoder.encode(.errorResponse(
            severity: "ERROR",
            code: code,
            message: "\(error)"
        ), into: &out)
        context.writeAndFlush(wrapOutboundOut(out), promise: nil)
    }

    /// Count $N placeholders in a query string
    private func countPlaceholders(in query: String) -> Int {
        var maxN = 0
        var i = query.startIndex
        while i < query.endIndex {
            if query[i] == "$" {
                let start = query.index(after: i)
                var end = start
                while end < query.endIndex && query[end].isNumber {
                    end = query.index(after: end)
                }
                if end > start, let n = Int(query[start..<end]) {
                    maxN = max(maxN, n)
                }
            }
            i = query.index(after: i)
        }
        return maxN
    }

    /// Substitute $1, $2, ... placeholders with parameter values
    static func substituteParameters(query: String, params: [String?]) -> String {
        var result = query
        // Replace in reverse order so $10 is replaced before $1
        for i in stride(from: params.count, through: 1, by: -1) {
            let placeholder = "$\(i)"
            if let value = params[i - 1] {
                let escaped = ConnectionHandler.escapeParameterValue(value)
                result = result.replacingOccurrences(of: placeholder, with: escaped)
            } else {
                result = result.replacingOccurrences(of: placeholder, with: "NULL")
            }
        }
        return result
    }

    /// Escape a parameter value for safe substitution into SQL.
    /// Numbers are passed through unquoted; strings are single-quoted with proper escaping.
    private static func escapeParameterValue(_ value: String) -> String {
        // If the value looks like a number, don't quote it
        if isNumericLiteral(value) {
            return value
        }
        // Strip NUL bytes, escape backslashes, then escape single quotes
        var sanitized = value.replacingOccurrences(of: "\0", with: "")
        sanitized = sanitized.replacingOccurrences(of: "\\", with: "\\\\")
        sanitized = sanitized.replacingOccurrences(of: "'", with: "''")
        return "'\(sanitized)'"
    }

    /// Check if a string is a valid numeric literal (integer or decimal, optional leading minus).
    private static func isNumericLiteral(_ s: String) -> Bool {
        guard !s.isEmpty else { return false }
        var idx = s.startIndex
        if s[idx] == "-" {
            idx = s.index(after: idx)
            guard idx < s.endIndex else { return false }
        }
        var hasDot = false
        var hasDigit = false
        while idx < s.endIndex {
            let c = s[idx]
            if c == "." {
                if hasDot { return false }
                hasDot = true
            } else if c.isNumber {
                hasDigit = true
            } else {
                return false
            }
            idx = s.index(after: idx)
        }
        return hasDigit
    }

    // MARK: - Simple Query Protocol

    private func handleQuery(_ sql: String, context: ChannelHandlerContext) {
        let trimmed = sql.trimmingCharacters(in: .whitespacesAndNewlines)

        if trimmed.isEmpty {
            var out = context.channel.allocator.buffer(capacity: 32)
            encoder.encode(.emptyQueryResponse, into: &out)
            encoder.encode(.readyForQuery(txStatus), into: &out)
            context.writeAndFlush(wrapOutboundOut(out), promise: nil)
            return
        }

        // Execute asynchronously — context is only used on its own event loop,
        // so the capture is safe despite ChannelHandlerContext not being Sendable.
        let db = self.database
        let eventLoop = context.eventLoop
        nonisolated(unsafe) let ctx = context

        let promise = eventLoop.makePromise(of: Void.self)

        promise.completeWithTask {
            do {
                let result = try await db.execute(sql: trimmed)
                eventLoop.execute {
                    self.sendQueryResult(result, sql: trimmed, context: ctx)
                }
            } catch {
                eventLoop.execute {
                    self.sendError(error, context: ctx)
                }
            }
        }
    }

    private func sendQueryResult(_ result: QueryResult, sql: String, context: ChannelHandlerContext) {
        // Track transaction state changes
        let upper = sql.uppercased().trimmingCharacters(in: .whitespacesAndNewlines)
        if upper.hasPrefix("BEGIN") {
            txStatus = .inTransaction
        } else if upper.hasPrefix("COMMIT") || upper.hasPrefix("ROLLBACK") {
            txStatus = .idle
        }

        var out = context.channel.allocator.buffer(capacity: 1024)

        switch result {
        case .rows(let rows):
            // Determine columns from first row, or empty
            if rows.isEmpty {
                // Send empty row description + command complete
                encoder.encode(.rowDescription([]), into: &out)
                encoder.encode(.commandComplete("SELECT 0"), into: &out)
            } else {
                // Build field descriptions from column names (sorted for determinism)
                let columnNames = rows[0].values.keys.sorted()
                let fields = columnNames.enumerated().map { (idx, name) -> FieldDescription in
                    let sampleValue = rows[0].values[name] ?? .null
                    return FieldDescription(
                        name: name,
                        tableOID: 0,
                        columnIndex: Int16(idx),
                        typeOID: TypeEncoding.oidForDBValue(sampleValue),
                        typeSize: -1,
                        typeMod: -1,
                        format: 0
                    )
                }
                encoder.encode(.rowDescription(fields), into: &out)

                for row in rows {
                    let columns: [[UInt8]?] = columnNames.map { name in
                        guard let value = row.values[name], value != .null else {
                            return nil
                        }
                        return Array(TypeEncoding.encodeText(value).utf8)
                    }
                    encoder.encode(.dataRow(columns), into: &out)
                }
                encoder.encode(.commandComplete("SELECT \(rows.count)"), into: &out)
            }

        case .rowCount(let count):
            let tag = commandTag(for: sql, count: count)
            encoder.encode(.commandComplete(tag), into: &out)

        case .ok:
            let tag = commandTag(for: sql, count: 0)
            encoder.encode(.commandComplete(tag), into: &out)
        }

        encoder.encode(.readyForQuery(txStatus), into: &out)
        context.writeAndFlush(wrapOutboundOut(out), promise: nil)
    }

    private func sendError(_ error: Error, context: ChannelHandlerContext) {
        let code = ConnectionHandler.sqlStateCode(for: error)
        if txStatus == .inTransaction {
            txStatus = .failed
        }
        var out = context.channel.allocator.buffer(capacity: 256)
        encoder.encode(.errorResponse(
            severity: "ERROR",
            code: code,
            message: "\(error)"
        ), into: &out)
        encoder.encode(.readyForQuery(txStatus), into: &out)
        context.writeAndFlush(wrapOutboundOut(out), promise: nil)
    }

    private func commandTag(for sql: String, count: Int) -> String {
        let upper = sql.uppercased().trimmingCharacters(in: .whitespacesAndNewlines)
        if upper.hasPrefix("INSERT") {
            return "INSERT 0 \(count)"
        } else if upper.hasPrefix("UPDATE") {
            return "UPDATE \(count)"
        } else if upper.hasPrefix("DELETE") {
            return "DELETE \(count)"
        } else if upper.hasPrefix("CREATE") {
            if upper.contains("VIEW") { return "CREATE VIEW" }
            return "CREATE TABLE"
        } else if upper.hasPrefix("DROP") {
            if upper.contains("VIEW") { return "DROP VIEW" }
            return "DROP TABLE"
        } else if upper.hasPrefix("BEGIN") {
            return "BEGIN"
        } else if upper.hasPrefix("COMMIT") {
            return "COMMIT"
        } else if upper.hasPrefix("ROLLBACK") {
            return "ROLLBACK"
        } else if upper.hasPrefix("SET") {
            return "SET"
        } else if upper.hasPrefix("SHOW") {
            return "SHOW"
        } else if upper.hasPrefix("RESET") {
            return "RESET"
        } else if upper.hasPrefix("DISCARD") {
            return "DISCARD ALL"
        } else {
            return "SELECT \(count)"
        }
    }

    func errorCaught(context: ChannelHandlerContext, error: Error) {
        context.close(promise: nil)
    }

    func channelInactive(context: ChannelHandlerContext) {
        context.fireChannelInactive()
    }

    // MARK: - SQLSTATE Codes

    static func sqlStateCode(for error: Error) -> String {
        guard let cobaltError = error as? CobaltError else {
            return "XX000"  // internal_error
        }
        switch cobaltError {
        case .tableNotFound:
            return "42P01"  // undefined_table
        case .tableAlreadyExists:
            return "42P07"  // duplicate_table
        case .columnNotFound:
            return "42703"  // undefined_column
        case .primaryKeyViolation:
            return "23505"  // unique_violation
        case .notNullConstraintViolation:
            return "23502"  // not_null_violation
        case .typeMismatch:
            return "42804"  // datatype_mismatch
        case .invalidQuery:
            return "42601"  // syntax_error
        case .serializationConflict:
            return "40001"  // serialization_failure
        case .deadlockDetected:
            return "40P01"  // deadlock_detected
        case .uniqueConstraintViolation:
            return "23505"  // unique_violation
        case .foreignKeyViolation:
            return "23503"  // foreign_key_violation
        case .checkConstraintViolation:
            return "23514"  // check_violation
        default:
            return "XX000"  // internal_error
        }
    }
}
