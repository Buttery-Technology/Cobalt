import Testing
import Foundation
import NIOCore
@testable import CobaltServer
@testable import CobaltCore

// MARK: - TypeEncoding Tests

@Suite("TypeEncoding")
struct TypeEncodingTests {

    @Test func oidMapping() {
        #expect(TypeEncoding.oidForDBValue(.boolean(true)) == TypeEncoding.boolOID)
        #expect(TypeEncoding.oidForDBValue(.integer(42)) == TypeEncoding.int8OID)
        #expect(TypeEncoding.oidForDBValue(.double(3.14)) == TypeEncoding.float8OID)
        #expect(TypeEncoding.oidForDBValue(.string("hello")) == TypeEncoding.textOID)
        #expect(TypeEncoding.oidForDBValue(.blob(Data([0x01]))) == TypeEncoding.byteaOID)
        #expect(TypeEncoding.oidForDBValue(.null) == TypeEncoding.nullOID)
    }

    @Test func textEncodingRoundTrip_integer() {
        let original: DBValue = .integer(12345)
        let text = TypeEncoding.encodeText(original)
        let decoded = TypeEncoding.decodeText(text, oid: TypeEncoding.int8OID)
        #expect(decoded == original)
    }

    @Test func textEncodingRoundTrip_double() {
        let original: DBValue = .double(3.14)
        let text = TypeEncoding.encodeText(original)
        let decoded = TypeEncoding.decodeText(text, oid: TypeEncoding.float8OID)
        #expect(decoded == original)
    }

    @Test func textEncodingRoundTrip_string() {
        let original: DBValue = .string("hello world")
        let text = TypeEncoding.encodeText(original)
        let decoded = TypeEncoding.decodeText(text, oid: TypeEncoding.textOID)
        #expect(decoded == original)
    }

    @Test func textEncodingRoundTrip_boolean() {
        let trueVal: DBValue = .boolean(true)
        let falseVal: DBValue = .boolean(false)
        #expect(TypeEncoding.encodeText(trueVal) == "t")
        #expect(TypeEncoding.encodeText(falseVal) == "f")
        #expect(TypeEncoding.decodeText("t", oid: TypeEncoding.boolOID) == trueVal)
        #expect(TypeEncoding.decodeText("f", oid: TypeEncoding.boolOID) == falseVal)
        #expect(TypeEncoding.decodeText("true", oid: TypeEncoding.boolOID) == trueVal)
    }

    @Test func textEncodingRoundTrip_bytea() {
        let original: DBValue = .blob(Data([0xDE, 0xAD, 0xBE, 0xEF]))
        let text = TypeEncoding.encodeText(original)
        #expect(text == "\\xdeadbeef")
        let decoded = TypeEncoding.decodeText(text, oid: TypeEncoding.byteaOID)
        #expect(decoded == original)
    }
}

// MARK: - Backend Message Encoding Tests

@Suite("BackendMessageEncoding")
struct BackendMessageEncodingTests {

    let encoder = BackendMessageEncoder()

    @Test func authenticationOkEncoding() {
        var buf = ByteBuffer()
        encoder.encode(.authenticationOk, into: &buf)
        // Tag 'R' (1 byte) + length 8 (4 bytes) + auth type 0 (4 bytes) = 9 bytes
        #expect(buf.readableBytes == 9)
        #expect(buf.readInteger(as: UInt8.self) == UInt8(ascii: "R"))
        #expect(buf.readInteger(as: Int32.self) == 8)
        #expect(buf.readInteger(as: Int32.self) == 0)
    }

    @Test func readyForQueryEncoding() {
        var buf = ByteBuffer()
        encoder.encode(.readyForQuery(.idle), into: &buf)
        #expect(buf.readableBytes == 6)
        #expect(buf.readInteger(as: UInt8.self) == UInt8(ascii: "Z"))
        #expect(buf.readInteger(as: Int32.self) == 5)
        #expect(buf.readInteger(as: UInt8.self) == TransactionStatus.idle.rawValue)
    }

    @Test func commandCompleteEncoding() {
        var buf = ByteBuffer()
        encoder.encode(.commandComplete("SELECT 5"), into: &buf)
        #expect(buf.readInteger(as: UInt8.self) == UInt8(ascii: "C"))
        let length = buf.readInteger(as: Int32.self)!
        // length = 4 + "SELECT 5".count + 1(null) = 13
        #expect(length == 13)
    }

    @Test func errorResponseEncoding() {
        var buf = ByteBuffer()
        encoder.encode(.errorResponse(severity: "ERROR", code: "42000", message: "test error"), into: &buf)
        #expect(buf.readInteger(as: UInt8.self) == UInt8(ascii: "E"))
        // Just verify we can read the length and it's reasonable
        let length = buf.readInteger(as: Int32.self)!
        #expect(length > 10)
    }

    @Test func rowDescriptionEncoding() {
        var buf = ByteBuffer()
        let fields = [
            FieldDescription(name: "id", typeOID: TypeEncoding.int8OID),
            FieldDescription(name: "name", typeOID: TypeEncoding.textOID),
        ]
        encoder.encode(.rowDescription(fields), into: &buf)
        #expect(buf.readInteger(as: UInt8.self) == UInt8(ascii: "T"))
        let length = buf.readInteger(as: Int32.self)!
        #expect(length > 0)
        let fieldCount = buf.readInteger(as: Int16.self)!
        #expect(fieldCount == 2)
    }

    @Test func dataRowEncoding() {
        var buf = ByteBuffer()
        let columns: [[UInt8]?] = [
            Array("42".utf8),
            nil,
            Array("hello".utf8),
        ]
        encoder.encode(.dataRow(columns), into: &buf)
        #expect(buf.readInteger(as: UInt8.self) == UInt8(ascii: "D"))
        let length = buf.readInteger(as: Int32.self)!
        #expect(length > 0)
        let colCount = buf.readInteger(as: Int16.self)!
        #expect(colCount == 3)
        // First column: length 2, "42"
        #expect(buf.readInteger(as: Int32.self) == 2)
        #expect(buf.readBytes(length: 2) == Array("42".utf8))
        // Second column: NULL (-1)
        #expect(buf.readInteger(as: Int32.self) == -1)
        // Third column: length 5, "hello"
        #expect(buf.readInteger(as: Int32.self) == 5)
        #expect(buf.readBytes(length: 5) == Array("hello".utf8))
    }

    @Test func parseCompleteEncoding() {
        var buf = ByteBuffer()
        encoder.encode(.parseComplete, into: &buf)
        #expect(buf.readableBytes == 5)
        #expect(buf.readInteger(as: UInt8.self) == UInt8(ascii: "1"))
        #expect(buf.readInteger(as: Int32.self) == 4)
    }

    @Test func parameterStatusEncoding() {
        var buf = ByteBuffer()
        encoder.encode(.parameterStatus(name: "server_version", value: "15.0"), into: &buf)
        #expect(buf.readInteger(as: UInt8.self) == UInt8(ascii: "S"))
        let length = buf.readInteger(as: Int32.self)!
        // 4 + "server_version\0".count + "15.0\0".count = 4 + 15 + 5 = 24
        #expect(length == 24)
    }
}

// MARK: - Frontend Message Decoding Tests

@Suite("FrontendMessageDecoding")
struct FrontendMessageDecodingTests {

    let decoder = FrontendMessageDecoder()

    @Test func decodeSimpleQuery() {
        var buf = ByteBuffer()
        let query = "SELECT 1"
        // Tag 'Q', length = 4 + query.count + 1
        buf.writeInteger(UInt8(ascii: "Q"))
        buf.writeInteger(Int32(4 + query.utf8.count + 1))
        buf.writeString(query)
        buf.writeInteger(UInt8(0))

        let msg = decoder.decode(&buf)
        if case .query(let q) = msg {
            #expect(q == "SELECT 1")
        } else {
            Issue.record("Expected .query, got \(String(describing: msg))")
        }
    }

    @Test func decodeTerminate() {
        var buf = ByteBuffer()
        buf.writeInteger(UInt8(ascii: "X"))
        buf.writeInteger(Int32(4))

        let msg = decoder.decode(&buf)
        if case .terminate = msg {
            // ok
        } else {
            Issue.record("Expected .terminate, got \(String(describing: msg))")
        }
    }

    @Test func decodeStartupMessage() {
        var buf = ByteBuffer()
        // Build startup message: length(4) + version(4) + params + null terminator
        var body = ByteBuffer()
        body.writeInteger(Int32(196608)) // version 3.0
        body.writeString("user")
        body.writeInteger(UInt8(0))
        body.writeString("testuser")
        body.writeInteger(UInt8(0))
        body.writeString("database")
        body.writeInteger(UInt8(0))
        body.writeString("testdb")
        body.writeInteger(UInt8(0))
        body.writeInteger(UInt8(0)) // final terminator

        let totalLength = Int32(4 + body.readableBytes)
        buf.writeInteger(totalLength)
        buf.writeBuffer(&body)

        let msg = decoder.decodeStartup(&buf)
        if case .startup(let params) = msg {
            #expect(params["user"] == "testuser")
            #expect(params["database"] == "testdb")
        } else {
            Issue.record("Expected .startup, got \(String(describing: msg))")
        }
    }

    @Test func decodeSync() {
        var buf = ByteBuffer()
        buf.writeInteger(UInt8(ascii: "S"))
        buf.writeInteger(Int32(4))

        let msg = decoder.decode(&buf)
        if case .sync = msg {
            // ok
        } else {
            Issue.record("Expected .sync, got \(String(describing: msg))")
        }
    }

    @Test func decodeReturnsNilForIncompleteData() {
        var buf = ByteBuffer()
        buf.writeInteger(UInt8(ascii: "Q"))
        // Only 2 bytes of length — incomplete
        buf.writeInteger(UInt8(0))

        let msg = decoder.decode(&buf)
        #expect(msg == nil)
    }
}

// Equatable conformance for testing
extension FrontendMessage: Equatable {
    public static func == (lhs: FrontendMessage, rhs: FrontendMessage) -> Bool {
        switch (lhs, rhs) {
        case (.terminate, .terminate): return true
        case (.sync, .sync): return true
        case (.sslRequest, .sslRequest): return true
        case (.query(let a), .query(let b)): return a == b
        case (.startup(let a), .startup(let b)): return a == b
        case (.parse(let n1, let q1, let p1), .parse(let n2, let q2, let p2)):
            return n1 == n2 && q1 == q2 && p1 == p2
        case (.bind(let p1, let s1, let v1), .bind(let p2, let s2, let v2)):
            return p1 == p2 && s1 == s2 && v1 == v2
        case (.describe(let t1, let n1), .describe(let t2, let n2)):
            return t1.tag == t2.tag && n1 == n2
        case (.execute(let p1, let m1), .execute(let p2, let m2)):
            return p1 == p2 && m1 == m2
        default: return false
        }
    }
}

// Need DBValue import for TypeEncoding tests
import CobaltCore

// MARK: - Extended Query Protocol Tests

/// Write a null-terminated string into a ByteBuffer (avoids ambiguity with NIOCore's version)
private func writeNTS(_ buf: inout ByteBuffer, _ string: String) {
    buf.writeString(string)
    buf.writeInteger(UInt8(0))
}

@Suite("ExtendedQueryProtocol")
struct ExtendedQueryProtocolTests {

    let decoder = FrontendMessageDecoder()
    let encoder = BackendMessageEncoder()

    @Test func decodeParseMessage() {
        var buf = ByteBuffer()
        // Build a Parse message: tag 'P', name "", query "SELECT $1", 1 param type (0)
        let name = ""
        let query = "SELECT $1"
        let bodyLength = (name.utf8.count + 1) + (query.utf8.count + 1) + 2 + 4 // name\0 + query\0 + int16(numParams) + int32(paramOID)
        buf.writeInteger(UInt8(ascii: "P"))
        buf.writeInteger(Int32(4 + bodyLength))
        writeNTS(&buf, name)
        writeNTS(&buf, query)
        buf.writeInteger(Int16(1))   // 1 param type
        buf.writeInteger(Int32(0))   // unspecified OID

        let msg = decoder.decode(&buf)
        if case .parse(let n, let q, let p) = msg {
            #expect(n == "")
            #expect(q == "SELECT $1")
            #expect(p == [0])
        } else {
            Issue.record("Expected .parse, got \(String(describing: msg))")
        }
    }

    @Test func decodeParseMessageNamedStatement() {
        var buf = ByteBuffer()
        let name = "my_stmt"
        let query = "INSERT INTO t VALUES ($1, $2)"
        let bodyLength = (name.utf8.count + 1) + (query.utf8.count + 1) + 2 // name\0 + query\0 + int16(0 params)
        buf.writeInteger(UInt8(ascii: "P"))
        buf.writeInteger(Int32(4 + bodyLength))
        writeNTS(&buf, name)
        writeNTS(&buf, query)
        buf.writeInteger(Int16(0))   // 0 param types

        let msg = decoder.decode(&buf)
        if case .parse(let n, let q, let p) = msg {
            #expect(n == "my_stmt")
            #expect(q == "INSERT INTO t VALUES ($1, $2)")
            #expect(p.isEmpty)
        } else {
            Issue.record("Expected .parse, got \(String(describing: msg))")
        }
    }

    @Test func decodeBindMessageWithParams() {
        var buf = ByteBuffer()
        let portal = ""
        let statement = ""
        let param1 = "hello"
        let param1Bytes = Array(param1.utf8)

        // Bind: portal\0, statement\0, int16(numFormats), int16(numParams), [int32(len) + bytes]..., int16(numResultFormats)
        var body = ByteBuffer()
        writeNTS(&body, portal)
        writeNTS(&body, statement)
        body.writeInteger(Int16(0))  // 0 format codes
        body.writeInteger(Int16(2))  // 2 parameters
        // param 1: "hello"
        body.writeInteger(Int32(param1Bytes.count))
        body.writeBytes(param1Bytes)
        // param 2: NULL
        body.writeInteger(Int32(-1))
        body.writeInteger(Int16(0))  // 0 result format codes

        buf.writeInteger(UInt8(ascii: "B"))
        buf.writeInteger(Int32(4 + body.readableBytes))
        buf.writeBuffer(&body)

        let msg = decoder.decode(&buf)
        if case .bind(let p, let s, let v) = msg {
            #expect(p == "")
            #expect(s == "")
            #expect(v.count == 2)
            #expect(v[0] == Data(param1.utf8))
            #expect(v[1] == nil)
        } else {
            Issue.record("Expected .bind, got \(String(describing: msg))")
        }
    }

    @Test func parameterSubstitutionBasic() {
        let query = "SELECT * FROM users WHERE id = $1 AND name = $2"
        let result = ConnectionHandler.substituteParameters(query: query, params: ["42", "Alice"])
        #expect(result == "SELECT * FROM users WHERE id = 42 AND name = 'Alice'")
    }

    @Test func parameterSubstitutionWithNull() {
        let query = "INSERT INTO t VALUES ($1, $2)"
        let result = ConnectionHandler.substituteParameters(query: query, params: ["hello", nil])
        #expect(result == "INSERT INTO t VALUES ('hello', NULL)")
    }

    @Test func parameterSubstitutionEscapesSingleQuotes() {
        let query = "INSERT INTO t VALUES ($1)"
        let result = ConnectionHandler.substituteParameters(query: query, params: ["it's a test"])
        #expect(result == "INSERT INTO t VALUES ('it''s a test')")
    }

    @Test func parameterSubstitutionHighNumberedParams() {
        let query = "SELECT $1, $2, $10"
        var params: [String?] = Array(repeating: "x", count: 10)
        params[0] = "a"
        params[1] = "b"
        params[9] = "j"
        let result = ConnectionHandler.substituteParameters(query: query, params: params)
        #expect(result == "SELECT 'a', 'b', 'j'")
    }

    @Test func parameterDescriptionEncoding() {
        var buf = ByteBuffer()
        encoder.encode(.parameterDescription([Int32(23), Int32(25)]), into: &buf)
        #expect(buf.readInteger(as: UInt8.self) == UInt8(ascii: "t"))
        let length = buf.readInteger(as: Int32.self)!
        // 4 + 2 + 2*4 = 14
        #expect(length == 14)
        let count = buf.readInteger(as: Int16.self)!
        #expect(count == 2)
        #expect(buf.readInteger(as: Int32.self) == 23)
        #expect(buf.readInteger(as: Int32.self) == 25)
    }

    @Test func decodeDescribeStatement() {
        var buf = ByteBuffer()
        let name = "my_stmt"
        let bodyLength = 1 + name.utf8.count + 1 // type byte + name\0
        buf.writeInteger(UInt8(ascii: "D"))
        buf.writeInteger(Int32(4 + bodyLength))
        buf.writeInteger(UInt8(ascii: "S"))
        writeNTS(&buf, name)

        let msg = decoder.decode(&buf)
        if case .describe(let type, let n) = msg {
            #expect(type.tag == UInt8(ascii: "S"))
            #expect(n == "my_stmt")
        } else {
            Issue.record("Expected .describe, got \(String(describing: msg))")
        }
    }

    @Test func decodeDescribePortal() {
        var buf = ByteBuffer()
        let name = ""
        let bodyLength = 1 + name.utf8.count + 1
        buf.writeInteger(UInt8(ascii: "D"))
        buf.writeInteger(Int32(4 + bodyLength))
        buf.writeInteger(UInt8(ascii: "P"))
        writeNTS(&buf, name)

        let msg = decoder.decode(&buf)
        if case .describe(let type, let n) = msg {
            #expect(type.tag == UInt8(ascii: "P"))
            #expect(n == "")
        } else {
            Issue.record("Expected .describe, got \(String(describing: msg))")
        }
    }

    @Test func decodeExecuteMessage() {
        var buf = ByteBuffer()
        let portal = ""
        let bodyLength = portal.utf8.count + 1 + 4 // portal\0 + maxRows
        buf.writeInteger(UInt8(ascii: "E"))
        buf.writeInteger(Int32(4 + bodyLength))
        writeNTS(&buf, portal)
        buf.writeInteger(Int32(0))  // maxRows = 0 (unlimited)

        let msg = decoder.decode(&buf)
        if case .execute(let p, let m) = msg {
            #expect(p == "")
            #expect(m == 0)
        } else {
            Issue.record("Expected .execute, got \(String(describing: msg))")
        }
    }
}

// MARK: - PubSub Tests

/// Thread-safe counter for PubSub test callbacks.
private final class AtomicCounter: @unchecked Sendable {
    private var _value: Int = 0
    private let lock = NSLock()
    var value: Int { lock.lock(); defer { lock.unlock() }; return _value }
    func increment() { lock.lock(); defer { lock.unlock() }; _value += 1 }
}

/// Thread-safe message collector for PubSub test callbacks.
private final class MessageCollector: @unchecked Sendable {
    private var _messages: [(String, String)] = []
    private let lock = NSLock()
    var messages: [(String, String)] { lock.lock(); defer { lock.unlock() }; return _messages }
    func append(_ ch: String, _ payload: String) { lock.lock(); defer { lock.unlock() }; _messages.append((ch, payload)) }
}

@Suite("PubSub")
struct PubSubTests {

    @Test func listenAndNotify() async {
        let pubsub = PubSub()
        let collector = MessageCollector()

        await pubsub.listen(channel: "events", subscriberID: "sub1") { ch, payload in
            collector.append(ch, payload)
        }
        await pubsub.notify(channel: "events", payload: "hello")

        let msgs = collector.messages
        #expect(msgs.count == 1)
        #expect(msgs[0].0 == "events")
        #expect(msgs[0].1 == "hello")
    }

    @Test func unlistenStopsNotifications() async {
        let pubsub = PubSub()
        let counter = AtomicCounter()

        await pubsub.listen(channel: "ch", subscriberID: "s1") { _, _ in counter.increment() }
        await pubsub.notify(channel: "ch", payload: "a")
        #expect(counter.value == 1)

        await pubsub.unlisten(channel: "ch", subscriberID: "s1")
        await pubsub.notify(channel: "ch", payload: "b")
        #expect(counter.value == 1)  // no increment after unlisten
    }

    @Test func multipleSubscribers() async {
        let pubsub = PubSub()
        let counter1 = AtomicCounter()
        let counter2 = AtomicCounter()

        await pubsub.listen(channel: "ch", subscriberID: "s1") { _, _ in counter1.increment() }
        await pubsub.listen(channel: "ch", subscriberID: "s2") { _, _ in counter2.increment() }
        await pubsub.notify(channel: "ch", payload: "test")

        #expect(counter1.value == 1)
        #expect(counter2.value == 1)
    }

    @Test func subscriberCount() async {
        let pubsub = PubSub()
        let cb: @Sendable (String, String) -> Void = { _, _ in }

        #expect(await pubsub.subscriberCount(channel: "ch") == 0)
        await pubsub.listen(channel: "ch", subscriberID: "s1", callback: cb)
        #expect(await pubsub.subscriberCount(channel: "ch") == 1)
        await pubsub.listen(channel: "ch", subscriberID: "s2", callback: cb)
        #expect(await pubsub.subscriberCount(channel: "ch") == 2)
        await pubsub.unlisten(channel: "ch", subscriberID: "s1")
        #expect(await pubsub.subscriberCount(channel: "ch") == 1)
    }

    @Test func activeChannels() async {
        let pubsub = PubSub()
        let cb: @Sendable (String, String) -> Void = { _, _ in }

        await pubsub.listen(channel: "alpha", subscriberID: "s1", callback: cb)
        await pubsub.listen(channel: "beta", subscriberID: "s1", callback: cb)

        let channels = await pubsub.activeChannels()
        #expect(channels == ["alpha", "beta"])
    }

    @Test func notifyNoSubscribers() async {
        let pubsub = PubSub()
        // Should not crash
        await pubsub.notify(channel: "empty", payload: "nothing")
    }
}

// MARK: - SQLSTATE Code Tests

@Suite("SQLSTATECodes")
struct SQLSTATECodeTests {

    @Test func tableNotFoundReturns42P01() {
        let code = ConnectionHandler.sqlStateCode(for: CobaltError.tableNotFound(name: "foo"))
        #expect(code == "42P01")
    }

    @Test func tableAlreadyExistsReturns42P07() {
        let code = ConnectionHandler.sqlStateCode(for: CobaltError.tableAlreadyExists(name: "foo"))
        #expect(code == "42P07")
    }

    @Test func columnNotFoundReturns42703() {
        let code = ConnectionHandler.sqlStateCode(for: CobaltError.columnNotFound(name: "col"))
        #expect(code == "42703")
    }

    @Test func primaryKeyViolationReturns23505() {
        let code = ConnectionHandler.sqlStateCode(for: CobaltError.primaryKeyViolation)
        #expect(code == "23505")
    }

    @Test func notNullViolationReturns23502() {
        let code = ConnectionHandler.sqlStateCode(for: CobaltError.notNullConstraintViolation(column: "col"))
        #expect(code == "23502")
    }

    @Test func typeMismatchReturns42804() {
        let code = ConnectionHandler.sqlStateCode(for: CobaltError.typeMismatch(column: "col", expected: "INTEGER"))
        #expect(code == "42804")
    }

    @Test func invalidQueryReturns42601() {
        let code = ConnectionHandler.sqlStateCode(for: CobaltError.invalidQuery(description: "bad"))
        #expect(code == "42601")
    }

    @Test func serializationConflictReturns40001() {
        let code = ConnectionHandler.sqlStateCode(for: CobaltError.serializationConflict)
        #expect(code == "40001")
    }

    @Test func deadlockReturns40P01() {
        let code = ConnectionHandler.sqlStateCode(for: CobaltError.deadlockDetected)
        #expect(code == "40P01")
    }

    @Test func unknownErrorReturnsXX000() {
        struct SomeError: Error {}
        let code = ConnectionHandler.sqlStateCode(for: SomeError())
        #expect(code == "XX000")
    }

    @Test func defaultCobaltErrorReturnsXX000() {
        let code = ConnectionHandler.sqlStateCode(for: CobaltError.databaseClosed)
        #expect(code == "XX000")
    }
}
