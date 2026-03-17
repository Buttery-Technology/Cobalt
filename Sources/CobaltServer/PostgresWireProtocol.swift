// PostgresWireProtocol.swift — PostgreSQL v3 wire protocol message types
import NIOCore
import Foundation

// MARK: - Frontend Messages (client → server)

public enum DescribeType: Sendable {
    case statement
    case portal

    var tag: UInt8 {
        switch self {
        case .statement: return UInt8(ascii: "S")
        case .portal: return UInt8(ascii: "P")
        }
    }
}

public enum FrontendMessage: Sendable {
    case startup(parameters: [String: String])
    case query(String)
    case terminate
    case parse(name: String, query: String, paramTypes: [Int32])
    case bind(portal: String, statement: String, paramValues: [Data?])
    case describe(type: DescribeType, name: String)
    case execute(portal: String, maxRows: Int32)
    case sync
    case sslRequest
}

// MARK: - Backend Messages (server → client)

public enum TransactionStatus: UInt8, Sendable {
    case idle = 0x49          // 'I'
    case inTransaction = 0x54 // 'T'
    case failed = 0x45        // 'E'
}

public struct FieldDescription: Sendable, Equatable {
    public let name: String
    public let tableOID: Int32
    public let columnIndex: Int16
    public let typeOID: Int32
    public let typeSize: Int16
    public let typeMod: Int32
    public let format: Int16  // 0=text, 1=binary

    public init(name: String, tableOID: Int32 = 0, columnIndex: Int16 = 0,
                typeOID: Int32 = 25, typeSize: Int16 = -1, typeMod: Int32 = -1,
                format: Int16 = 0) {
        self.name = name
        self.tableOID = tableOID
        self.columnIndex = columnIndex
        self.typeOID = typeOID
        self.typeSize = typeSize
        self.typeMod = typeMod
        self.format = format
    }
}

public enum BackendMessage: Sendable {
    case authenticationOk
    case parameterStatus(name: String, value: String)
    case backendKeyData(processID: Int32, secretKey: Int32)
    case readyForQuery(TransactionStatus)
    case rowDescription([FieldDescription])
    case dataRow([[UInt8]?])
    case commandComplete(String)
    case errorResponse(severity: String, code: String, message: String)
    case parseComplete
    case bindComplete
    case noData
    case emptyQueryResponse
    case parameterDescription([Int32])
}

// MARK: - Frontend Message Decoding

public struct FrontendMessageDecoder: Sendable {

    public init() {}

    /// Decode a startup message (no tag byte, first message on connection).
    /// Returns nil if there aren't enough bytes yet.
    public func decodeStartup(_ buffer: inout ByteBuffer) -> FrontendMessage? {
        guard buffer.readableBytes >= 4 else { return nil }
        let savedIndex = buffer.readerIndex

        guard let length = buffer.readInteger(as: Int32.self) else {
            buffer.moveReaderIndex(to: savedIndex)
            return nil
        }

        let totalLength = Int(length)
        // Need length - 4 more bytes (length field itself is counted)
        guard buffer.readableBytes >= totalLength - 4 else {
            buffer.moveReaderIndex(to: savedIndex)
            return nil
        }

        // Read protocol version
        guard let version = buffer.readInteger(as: Int32.self) else {
            buffer.moveReaderIndex(to: savedIndex)
            return nil
        }

        // SSL request has magic number 80877103
        if version == 80877103 {
            return .sslRequest
        }

        // Parse key-value pairs (null-terminated strings, terminated by extra null)
        var params: [String: String] = [:]
        while true {
            guard let key = buffer.readNullTerminatedString() else { break }
            if key.isEmpty { break }
            guard let value = buffer.readNullTerminatedString() else { break }
            params[key] = value
        }

        return .startup(parameters: params)
    }

    /// Decode a regular (post-startup) message. Returns nil if not enough bytes.
    public func decode(_ buffer: inout ByteBuffer) -> FrontendMessage? {
        guard buffer.readableBytes >= 5 else { return nil }
        let savedIndex = buffer.readerIndex

        guard let tagByte = buffer.readInteger(as: UInt8.self),
              let length = buffer.readInteger(as: Int32.self) else {
            buffer.moveReaderIndex(to: savedIndex)
            return nil
        }

        let bodyLength = Int(length) - 4
        guard bodyLength >= 0, buffer.readableBytes >= bodyLength else {
            buffer.moveReaderIndex(to: savedIndex)
            return nil
        }

        switch tagByte {
        case UInt8(ascii: "Q"):
            // Simple query
            let query = buffer.readNullTerminatedString() ?? ""
            return .query(query)

        case UInt8(ascii: "X"):
            // Terminate
            return .terminate

        case UInt8(ascii: "P"):
            // Parse
            let name = buffer.readNullTerminatedString() ?? ""
            let query = buffer.readNullTerminatedString() ?? ""
            let numParams = Int(buffer.readInteger(as: Int16.self) ?? 0)
            var paramTypes: [Int32] = []
            for _ in 0..<numParams {
                paramTypes.append(buffer.readInteger(as: Int32.self) ?? 0)
            }
            return .parse(name: name, query: query, paramTypes: paramTypes)

        case UInt8(ascii: "B"):
            // Bind
            let portal = buffer.readNullTerminatedString() ?? ""
            let statement = buffer.readNullTerminatedString() ?? ""
            // Skip format codes
            let numFormats = Int(buffer.readInteger(as: Int16.self) ?? 0)
            for _ in 0..<numFormats {
                _ = buffer.readInteger(as: Int16.self)
            }
            let numParams = Int(buffer.readInteger(as: Int16.self) ?? 0)
            var paramValues: [Data?] = []
            for _ in 0..<numParams {
                let paramLen = buffer.readInteger(as: Int32.self) ?? -1
                if paramLen == -1 {
                    paramValues.append(nil)
                } else {
                    if let bytes = buffer.readBytes(length: Int(paramLen)) {
                        paramValues.append(Data(bytes))
                    } else {
                        paramValues.append(nil)
                    }
                }
            }
            // Skip result format codes
            let numResultFormats = Int(buffer.readInteger(as: Int16.self) ?? 0)
            for _ in 0..<numResultFormats {
                _ = buffer.readInteger(as: Int16.self)
            }
            return .bind(portal: portal, statement: statement, paramValues: paramValues)

        case UInt8(ascii: "D"):
            // Describe
            let typeByte = buffer.readInteger(as: UInt8.self) ?? UInt8(ascii: "S")
            let name = buffer.readNullTerminatedString() ?? ""
            let descType: DescribeType = typeByte == UInt8(ascii: "P") ? .portal : .statement
            return .describe(type: descType, name: name)

        case UInt8(ascii: "E"):
            // Execute
            let portal = buffer.readNullTerminatedString() ?? ""
            let maxRows = buffer.readInteger(as: Int32.self) ?? 0
            return .execute(portal: portal, maxRows: maxRows)

        case UInt8(ascii: "S"):
            // Sync
            return .sync

        default:
            // Skip unknown message body
            buffer.moveReaderIndex(forwardBy: bodyLength)
            return nil
        }
    }
}

// MARK: - Backend Message Encoding

public struct BackendMessageEncoder: Sendable {

    public init() {}

    public func encode(_ message: BackendMessage, into buffer: inout ByteBuffer) {
        switch message {
        case .authenticationOk:
            buffer.writeInteger(UInt8(ascii: "R"))
            buffer.writeInteger(Int32(8))  // length
            buffer.writeInteger(Int32(0))  // auth ok

        case .parameterStatus(let name, let value):
            buffer.writeInteger(UInt8(ascii: "S"))
            let nameBytes = name.utf8.count + 1
            let valueBytes = value.utf8.count + 1
            buffer.writeInteger(Int32(4 + nameBytes + valueBytes))
            buffer.writeNullTerminatedString(name)
            buffer.writeNullTerminatedString(value)

        case .backendKeyData(let processID, let secretKey):
            buffer.writeInteger(UInt8(ascii: "K"))
            buffer.writeInteger(Int32(12)) // length
            buffer.writeInteger(processID)
            buffer.writeInteger(secretKey)

        case .readyForQuery(let status):
            buffer.writeInteger(UInt8(ascii: "Z"))
            buffer.writeInteger(Int32(5))  // length
            buffer.writeInteger(status.rawValue)

        case .rowDescription(let fields):
            buffer.writeInteger(UInt8(ascii: "T"))
            // Calculate length
            let lengthIndex = buffer.writerIndex
            buffer.writeInteger(Int32(0)) // placeholder
            buffer.writeInteger(Int16(fields.count))
            for field in fields {
                buffer.writeNullTerminatedString(field.name)
                buffer.writeInteger(field.tableOID)
                buffer.writeInteger(field.columnIndex)
                buffer.writeInteger(field.typeOID)
                buffer.writeInteger(field.typeSize)
                buffer.writeInteger(field.typeMod)
                buffer.writeInteger(field.format)
            }
            let length = Int32(buffer.writerIndex - lengthIndex)
            buffer.setInteger(length, at: lengthIndex)

        case .dataRow(let columns):
            buffer.writeInteger(UInt8(ascii: "D"))
            let lengthIndex = buffer.writerIndex
            buffer.writeInteger(Int32(0)) // placeholder
            buffer.writeInteger(Int16(columns.count))
            for column in columns {
                if let col = column {
                    buffer.writeInteger(Int32(col.count))
                    buffer.writeBytes(col)
                } else {
                    buffer.writeInteger(Int32(-1)) // NULL
                }
            }
            let length = Int32(buffer.writerIndex - lengthIndex)
            buffer.setInteger(length, at: lengthIndex)

        case .commandComplete(let tag):
            buffer.writeInteger(UInt8(ascii: "C"))
            let tagBytes = tag.utf8.count + 1
            buffer.writeInteger(Int32(4 + tagBytes))
            buffer.writeNullTerminatedString(tag)

        case .errorResponse(let severity, let code, let message):
            buffer.writeInteger(UInt8(ascii: "E"))
            let lengthIndex = buffer.writerIndex
            buffer.writeInteger(Int32(0)) // placeholder
            // Severity
            buffer.writeInteger(UInt8(ascii: "S"))
            buffer.writeNullTerminatedString(severity)
            // SQLSTATE code
            buffer.writeInteger(UInt8(ascii: "C"))
            buffer.writeNullTerminatedString(code)
            // Message
            buffer.writeInteger(UInt8(ascii: "M"))
            buffer.writeNullTerminatedString(message)
            // Terminator
            buffer.writeInteger(UInt8(0))
            let length = Int32(buffer.writerIndex - lengthIndex)
            buffer.setInteger(length, at: lengthIndex)

        case .parseComplete:
            buffer.writeInteger(UInt8(ascii: "1"))
            buffer.writeInteger(Int32(4))

        case .bindComplete:
            buffer.writeInteger(UInt8(ascii: "2"))
            buffer.writeInteger(Int32(4))

        case .noData:
            buffer.writeInteger(UInt8(ascii: "n"))
            buffer.writeInteger(Int32(4))

        case .emptyQueryResponse:
            buffer.writeInteger(UInt8(ascii: "I"))
            buffer.writeInteger(Int32(4))

        case .parameterDescription(let oids):
            buffer.writeInteger(UInt8(ascii: "t"))
            // length = 4 (self) + 2 (count) + 4*N (oids)
            let length = Int32(4 + 2 + oids.count * 4)
            buffer.writeInteger(length)
            buffer.writeInteger(Int16(oids.count))
            for oid in oids {
                buffer.writeInteger(oid)
            }
        }
    }
}

// MARK: - ByteBuffer Helpers

extension ByteBuffer {
    mutating func readNullTerminatedString() -> String? {
        guard let nullIndex = withUnsafeReadableBytes({ ptr -> Int? in
            for i in 0..<ptr.count {
                if ptr[i] == 0 { return i }
            }
            return nil
        }) else {
            return nil
        }

        guard let string = readString(length: nullIndex) else { return nil }
        moveReaderIndex(forwardBy: 1) // skip null terminator
        return string
    }

    @discardableResult
    mutating func writeNullTerminatedString(_ string: String) -> Int {
        let written = writeString(string)
        writeInteger(UInt8(0))
        return written + 1
    }
}
