import Foundation
import CobaltCore

/// Evaluates built-in SQL functions and CAST conversions.
public struct BuiltinFunctions: Sendable {

    /// Evaluate a built-in function call by name.
    /// - Parameters:
    ///   - name: Function name (case-insensitive).
    ///   - args: Evaluated argument values.
    /// - Returns: The function result as a `DBValue`.
    public static func evaluate(name: String, args: [DBValue]) throws -> DBValue {
        switch name.lowercased() {

        // MARK: - String Functions

        case "length":
            guard args.count == 1 else { throw SQLError.semanticError("length() requires 1 argument") }
            return lengthFn(args[0])

        case "upper":
            guard args.count == 1 else { throw SQLError.semanticError("upper() requires 1 argument") }
            return upperFn(args[0])

        case "lower":
            guard args.count == 1 else { throw SQLError.semanticError("lower() requires 1 argument") }
            return lowerFn(args[0])

        case "trim":
            guard args.count == 1 else { throw SQLError.semanticError("trim() requires 1 argument") }
            return trimFn(args[0])

        case "concat":
            guard args.count >= 1 else { throw SQLError.semanticError("concat() requires at least 1 argument") }
            return concatFn(args)

        case "replace":
            guard args.count == 3 else { throw SQLError.semanticError("replace() requires 3 arguments") }
            return replaceFn(args[0], args[1], args[2])

        case "substring", "substr":
            guard args.count == 3 else { throw SQLError.semanticError("substring() requires 3 arguments") }
            return substringFn(args[0], args[1], args[2])

        // MARK: - Math Functions

        case "abs":
            guard args.count == 1 else { throw SQLError.semanticError("abs() requires 1 argument") }
            return absFn(args[0])

        case "ceil", "ceiling":
            guard args.count == 1 else { throw SQLError.semanticError("ceil() requires 1 argument") }
            return ceilFn(args[0])

        case "floor":
            guard args.count == 1 else { throw SQLError.semanticError("floor() requires 1 argument") }
            return floorFn(args[0])

        case "round":
            guard args.count >= 1 && args.count <= 2 else { throw SQLError.semanticError("round() requires 1 or 2 arguments") }
            let decimals = args.count == 2 ? args[1] : .integer(0)
            return roundFn(args[0], decimals)

        case "power", "pow":
            guard args.count == 2 else { throw SQLError.semanticError("power() requires 2 arguments") }
            return powerFn(args[0], args[1])

        case "sqrt":
            guard args.count == 1 else { throw SQLError.semanticError("sqrt() requires 1 argument") }
            return sqrtFn(args[0])

        case "mod":
            guard args.count == 2 else { throw SQLError.semanticError("mod() requires 2 arguments") }
            return modFn(args[0], args[1])

        // MARK: - Conditional Functions

        case "coalesce":
            guard args.count >= 1 else { throw SQLError.semanticError("coalesce() requires at least 1 argument") }
            return coalesceFn(args)

        case "nullif":
            guard args.count == 2 else { throw SQLError.semanticError("nullif() requires 2 arguments") }
            return nullifFn(args[0], args[1])

        case "greatest":
            guard args.count >= 1 else { throw SQLError.semanticError("greatest() requires at least 1 argument") }
            return greatestFn(args)

        case "least":
            guard args.count >= 1 else { throw SQLError.semanticError("least() requires at least 1 argument") }
            return leastFn(args)

        // MARK: - Additional String Functions

        case "left":
            guard args.count == 2 else { throw SQLError.semanticError("left() requires 2 arguments") }
            return leftFn(args[0], args[1])

        case "right":
            guard args.count == 2 else { throw SQLError.semanticError("right() requires 2 arguments") }
            return rightFn(args[0], args[1])

        case "lpad":
            guard args.count == 2 || args.count == 3 else { throw SQLError.semanticError("lpad() requires 2 or 3 arguments") }
            return lpadFn(args)

        case "rpad":
            guard args.count == 2 || args.count == 3 else { throw SQLError.semanticError("rpad() requires 2 or 3 arguments") }
            return rpadFn(args)

        case "repeat":
            guard args.count == 2 else { throw SQLError.semanticError("repeat() requires 2 arguments") }
            return repeatFn(args[0], args[1])

        case "reverse":
            guard args.count == 1 else { throw SQLError.semanticError("reverse() requires 1 argument") }
            return reverseFn(args[0])

        case "position":
            guard args.count == 2 else { throw SQLError.semanticError("position() requires 2 arguments") }
            return positionFn(args[0], args[1])

        case "char_length", "character_length":
            guard args.count == 1 else { throw SQLError.semanticError("char_length() requires 1 argument") }
            return lengthFn(args[0])

        case "octet_length":
            guard args.count == 1 else { throw SQLError.semanticError("octet_length() requires 1 argument") }
            return octetLengthFn(args[0])

        case "md5":
            guard args.count == 1 else { throw SQLError.semanticError("md5() requires 1 argument") }
            return md5Fn(args[0])

        case "gen_random_uuid":
            guard args.count == 0 else { throw SQLError.semanticError("gen_random_uuid() takes no arguments") }
            return .string(UUID().uuidString.lowercased())

        // MARK: - Date Functions

        case "now", "current_timestamp":
            return .double(Date().timeIntervalSince1970)

        default:
            throw SQLError.semanticError("Unknown function: \(name)")
        }
    }

    // MARK: - CAST Support

    /// Evaluate a CAST expression.
    public static func cast(_ value: DBValue, to type: SQLDataType) throws -> DBValue {
        switch type {
        case .integer, .serial:
            return try castToInteger(value)
        case .real:
            return try castToDouble(value)
        case .text, .varchar:
            return castToString(value)
        case .boolean:
            return try castToBoolean(value)
        case .blob:
            throw SQLError.unsupported("CAST to BLOB not supported")
        }
    }

    // MARK: - String Function Implementations

    private static func lengthFn(_ val: DBValue) -> DBValue {
        switch val {
        case .string(let s): return .integer(Int64(s.count))
        case .null: return .null
        default: return .integer(Int64(toString(val).count))
        }
    }

    private static func upperFn(_ val: DBValue) -> DBValue {
        switch val {
        case .string(let s): return .string(s.uppercased())
        case .null: return .null
        default: return .string(toString(val).uppercased())
        }
    }

    private static func lowerFn(_ val: DBValue) -> DBValue {
        switch val {
        case .string(let s): return .string(s.lowercased())
        case .null: return .null
        default: return .string(toString(val).lowercased())
        }
    }

    private static func trimFn(_ val: DBValue) -> DBValue {
        switch val {
        case .string(let s):
            return .string(s.trimmingCharacters(in: .whitespaces))
        case .null: return .null
        default: return .string(toString(val).trimmingCharacters(in: .whitespaces))
        }
    }

    private static func concatFn(_ args: [DBValue]) -> DBValue {
        var result = ""
        for arg in args {
            if case .null = arg { return .null }
            result += toString(arg)
        }
        return .string(result)
    }

    private static func replaceFn(_ val: DBValue, _ from: DBValue, _ to: DBValue) -> DBValue {
        if case .null = val { return .null }
        if case .null = from { return .null }
        if case .null = to { return .null }
        let s = toString(val)
        let f = toString(from)
        let t = toString(to)
        return .string(s.replacingOccurrences(of: f, with: t))
    }

    private static func substringFn(_ val: DBValue, _ start: DBValue, _ len: DBValue) -> DBValue {
        if case .null = val { return .null }
        guard let startIdx = toInt(start), let length = toInt(len) else { return .null }
        let s = toString(val)
        // SQL substring is 1-based
        let zeroStart = max(0, startIdx - 1)
        guard zeroStart < s.count else { return .string("") }
        let begin = s.index(s.startIndex, offsetBy: zeroStart)
        let endOffset = min(s.count, zeroStart + max(0, length))
        let end = s.index(s.startIndex, offsetBy: endOffset)
        return .string(String(s[begin..<end]))
    }

    // MARK: - Math Function Implementations

    private static func absFn(_ val: DBValue) -> DBValue {
        switch val {
        case .integer(let v): return .integer(abs(v))
        case .double(let v): return .double(abs(v))
        case .null: return .null
        default: return .null
        }
    }

    private static func ceilFn(_ val: DBValue) -> DBValue {
        switch val {
        case .integer(let v): return .integer(v)
        case .double(let v): return .double(Foundation.ceil(v))
        case .null: return .null
        default: return .null
        }
    }

    private static func floorFn(_ val: DBValue) -> DBValue {
        switch val {
        case .integer(let v): return .integer(v)
        case .double(let v): return .double(Foundation.floor(v))
        case .null: return .null
        default: return .null
        }
    }

    private static func roundFn(_ val: DBValue, _ decimals: DBValue) -> DBValue {
        guard let d = toInt(decimals) else { return .null }
        switch val {
        case .integer(let v):
            if d >= 0 { return .integer(v) }
            let factor = Int64(Foundation.pow(10.0, Double(-d)))
            return .integer((v / factor) * factor)
        case .double(let v):
            let factor = Foundation.pow(10.0, Double(d))
            return .double((v * factor).rounded() / factor)
        case .null: return .null
        default: return .null
        }
    }

    private static func powerFn(_ base: DBValue, _ exp: DBValue) -> DBValue {
        guard let b = toDouble(base), let e = toDouble(exp) else { return .null }
        return .double(Foundation.pow(b, e))
    }

    private static func sqrtFn(_ val: DBValue) -> DBValue {
        guard let v = toDouble(val) else { return .null }
        return .double(Foundation.sqrt(v))
    }

    private static func modFn(_ a: DBValue, _ b: DBValue) -> DBValue {
        switch (a, b) {
        case (.integer(let x), .integer(let y)):
            guard y != 0 else { return .null }
            return .integer(x % y)
        case (.null, _), (_, .null): return .null
        default:
            guard let x = toDouble(a), let y = toDouble(b), y != 0 else { return .null }
            return .double(x.truncatingRemainder(dividingBy: y))
        }
    }

    // MARK: - Conditional Function Implementations

    private static func coalesceFn(_ args: [DBValue]) -> DBValue {
        for arg in args {
            if case .null = arg { continue }
            return arg
        }
        return .null
    }

    private static func nullifFn(_ a: DBValue, _ b: DBValue) -> DBValue {
        if a == b { return .null }
        return a
    }

    private static func greatestFn(_ args: [DBValue]) -> DBValue {
        var best: DBValue = .null
        for arg in args {
            if case .null = arg { continue }
            if case .null = best { best = arg; continue }
            if arg > best { best = arg }
        }
        return best
    }

    private static func leastFn(_ args: [DBValue]) -> DBValue {
        var best: DBValue = .null
        for arg in args {
            if case .null = arg { continue }
            if case .null = best { best = arg; continue }
            if arg < best { best = arg }
        }
        return best
    }

    // MARK: - Additional String Function Implementations

    private static func leftFn(_ val: DBValue, _ n: DBValue) -> DBValue {
        if case .null = val { return .null }
        guard let count = toInt(n) else { return .null }
        let s = toString(val)
        let end = min(max(0, count), s.count)
        return .string(String(s.prefix(end)))
    }

    private static func rightFn(_ val: DBValue, _ n: DBValue) -> DBValue {
        if case .null = val { return .null }
        guard let count = toInt(n) else { return .null }
        let s = toString(val)
        let take = min(max(0, count), s.count)
        return .string(String(s.suffix(take)))
    }

    private static func lpadFn(_ args: [DBValue]) -> DBValue {
        if case .null = args[0] { return .null }
        guard let targetLen = toInt(args[1]) else { return .null }
        let s = toString(args[0])
        let fill = args.count >= 3 ? toString(args[2]) : " "
        if s.count >= targetLen { return .string(String(s.prefix(targetLen))) }
        let padNeeded = targetLen - s.count
        var pad = ""
        while pad.count < padNeeded {
            pad += fill
        }
        pad = String(pad.prefix(padNeeded))
        return .string(pad + s)
    }

    private static func rpadFn(_ args: [DBValue]) -> DBValue {
        if case .null = args[0] { return .null }
        guard let targetLen = toInt(args[1]) else { return .null }
        let s = toString(args[0])
        let fill = args.count >= 3 ? toString(args[2]) : " "
        if s.count >= targetLen { return .string(String(s.prefix(targetLen))) }
        let padNeeded = targetLen - s.count
        var pad = ""
        while pad.count < padNeeded {
            pad += fill
        }
        pad = String(pad.prefix(padNeeded))
        return .string(s + pad)
    }

    private static func repeatFn(_ val: DBValue, _ n: DBValue) -> DBValue {
        if case .null = val { return .null }
        guard let count = toInt(n) else { return .null }
        let s = toString(val)
        if count <= 0 { return .string("") }
        return .string(String(repeating: s, count: count))
    }

    private static func reverseFn(_ val: DBValue) -> DBValue {
        if case .null = val { return .null }
        return .string(String(toString(val).reversed()))
    }

    private static func positionFn(_ sub: DBValue, _ str: DBValue) -> DBValue {
        if case .null = sub { return .null }
        if case .null = str { return .null }
        let substring = toString(sub)
        let string = toString(str)
        if let range = string.range(of: substring) {
            let pos = string.distance(from: string.startIndex, to: range.lowerBound) + 1
            return .integer(Int64(pos))
        }
        return .integer(0)
    }

    private static func octetLengthFn(_ val: DBValue) -> DBValue {
        if case .null = val { return .null }
        let s = toString(val)
        return .integer(Int64(s.utf8.count))
    }

    private static func md5Fn(_ val: DBValue) -> DBValue {
        if case .null = val { return .null }
        let s = toString(val)
        let data = Array(s.utf8)
        // MD5 implementation
        let digest = md5Digest(data)
        let hex = digest.map { String(format: "%02x", $0) }.joined()
        return .string(hex)
    }

    /// Minimal MD5 implementation (RFC 1321) for the md5() SQL function.
    private static func md5Digest(_ message: [UInt8]) -> [UInt8] {
        // Per-round shift amounts
        let s: [UInt32] = [
            7, 12, 17, 22, 7, 12, 17, 22, 7, 12, 17, 22, 7, 12, 17, 22,
            5,  9, 14, 20, 5,  9, 14, 20, 5,  9, 14, 20, 5,  9, 14, 20,
            4, 11, 16, 23, 4, 11, 16, 23, 4, 11, 16, 23, 4, 11, 16, 23,
            6, 10, 15, 21, 6, 10, 15, 21, 6, 10, 15, 21, 6, 10, 15, 21
        ]
        // Pre-computed T table
        let K: [UInt32] = [
            0xd76aa478, 0xe8c7b756, 0x242070db, 0xc1bdceee,
            0xf57c0faf, 0x4787c62a, 0xa8304613, 0xfd469501,
            0x698098d8, 0x8b44f7af, 0xffff5bb1, 0x895cd7be,
            0x6b901122, 0xfd987193, 0xa679438e, 0x49b40821,
            0xf61e2562, 0xc040b340, 0x265e5a51, 0xe9b6c7aa,
            0xd62f105d, 0x02441453, 0xd8a1e681, 0xe7d3fbc8,
            0x21e1cde6, 0xc33707d6, 0xf4d50d87, 0x455a14ed,
            0xa9e3e905, 0xfcefa3f8, 0x676f02d9, 0x8d2a4c8a,
            0xfffa3942, 0x8771f681, 0x6d9d6122, 0xfde5380c,
            0xa4beea44, 0x4bdecfa9, 0xf6bb4b60, 0xbebfbc70,
            0x289b7ec6, 0xeaa127fa, 0xd4ef3085, 0x04881d05,
            0xd9d4d039, 0xe6db99e5, 0x1fa27cf8, 0xc4ac5665,
            0xf4292244, 0x432aff97, 0xab9423a7, 0xfc93a039,
            0x655b59c3, 0x8f0ccc92, 0xffeff47d, 0x85845dd1,
            0x6fa87e4f, 0xfe2ce6e0, 0xa3014314, 0x4e0811a1,
            0xf7537e82, 0xbd3af235, 0x2ad7d2bb, 0xeb86d391
        ]

        var msg = message
        let originalLen = message.count
        msg.append(0x80)
        while msg.count % 64 != 56 {
            msg.append(0)
        }
        // Append original length in bits as 64-bit little-endian
        let bitLen = UInt64(originalLen) * 8
        for i in 0..<8 {
            msg.append(UInt8(truncatingIfNeeded: bitLen >> (i * 8)))
        }

        var a0: UInt32 = 0x67452301
        var b0: UInt32 = 0xefcdab89
        var c0: UInt32 = 0x98badcfe
        var d0: UInt32 = 0x10325476

        // Process each 512-bit block
        for chunkStart in stride(from: 0, to: msg.count, by: 64) {
            var M = [UInt32](repeating: 0, count: 16)
            for i in 0..<16 {
                let offset = chunkStart + i * 4
                M[i] = UInt32(msg[offset]) | (UInt32(msg[offset+1]) << 8) |
                        (UInt32(msg[offset+2]) << 16) | (UInt32(msg[offset+3]) << 24)
            }

            var A = a0, B = b0, C = c0, D = d0

            for i in 0..<64 {
                let F: UInt32
                let g: Int
                if i < 16 {
                    F = (B & C) | (~B & D)
                    g = i
                } else if i < 32 {
                    F = (D & B) | (~D & C)
                    g = (5 * i + 1) % 16
                } else if i < 48 {
                    F = B ^ C ^ D
                    g = (3 * i + 5) % 16
                } else {
                    F = C ^ (B | ~D)
                    g = (7 * i) % 16
                }
                let temp = D
                D = C
                C = B
                let sum = A &+ F &+ K[i] &+ M[g]
                B = B &+ ((sum << s[i]) | (sum >> (32 - s[i])))
                A = temp
            }
            a0 = a0 &+ A; b0 = b0 &+ B; c0 = c0 &+ C; d0 = d0 &+ D
        }

        var result = [UInt8](repeating: 0, count: 16)
        for i in 0..<4 {
            result[i]    = UInt8(truncatingIfNeeded: a0 >> (i * 8))
            result[i+4]  = UInt8(truncatingIfNeeded: b0 >> (i * 8))
            result[i+8]  = UInt8(truncatingIfNeeded: c0 >> (i * 8))
            result[i+12] = UInt8(truncatingIfNeeded: d0 >> (i * 8))
        }
        return result
    }

    // MARK: - CAST Implementations

    private static func castToInteger(_ val: DBValue) throws -> DBValue {
        switch val {
        case .integer: return val
        case .double(let v): return .integer(Int64(v))
        case .string(let s):
            if let i = Int64(s) { return .integer(i) }
            if let d = Double(s) { return .integer(Int64(d)) }
            throw SQLError.semanticError("Cannot cast '\(s)' to INTEGER")
        case .boolean(let v): return .integer(v ? 1 : 0)
        case .null: return .null
        default: throw SQLError.semanticError("Cannot cast value to INTEGER")
        }
    }

    private static func castToDouble(_ val: DBValue) throws -> DBValue {
        switch val {
        case .double: return val
        case .integer(let v): return .double(Double(v))
        case .string(let s):
            guard let d = Double(s) else {
                throw SQLError.semanticError("Cannot cast '\(s)' to REAL")
            }
            return .double(d)
        case .boolean(let v): return .double(v ? 1.0 : 0.0)
        case .null: return .null
        default: throw SQLError.semanticError("Cannot cast value to REAL")
        }
    }

    private static func castToString(_ val: DBValue) -> DBValue {
        switch val {
        case .null: return .null
        default: return .string(toString(val))
        }
    }

    private static func castToBoolean(_ val: DBValue) throws -> DBValue {
        switch val {
        case .boolean: return val
        case .integer(let v): return .boolean(v != 0)
        case .double(let v): return .boolean(v != 0.0)
        case .string(let s):
            let lower = s.lowercased()
            if lower == "true" || lower == "1" { return .boolean(true) }
            if lower == "false" || lower == "0" { return .boolean(false) }
            throw SQLError.semanticError("Cannot cast '\(s)' to BOOLEAN")
        case .null: return .null
        default: throw SQLError.semanticError("Cannot cast value to BOOLEAN")
        }
    }

    // MARK: - Conversion Helpers

    private static func toString(_ val: DBValue) -> String {
        switch val {
        case .string(let s): return s
        case .integer(let v): return String(v)
        case .double(let v):
            // Avoid trailing ".0" for whole numbers
            if v == Foundation.floor(v) && !v.isInfinite && !v.isNaN {
                return String(Int64(v))
            }
            return String(v)
        case .boolean(let v): return v ? "true" : "false"
        case .null: return "NULL"
        case .blob(let d): return d.base64EncodedString()
        case .compound(let vals): return vals.map { toString($0) }.joined(separator: ",")
        }
    }

    private static func toDouble(_ val: DBValue) -> Double? {
        switch val {
        case .double(let v): return v
        case .integer(let v): return Double(v)
        case .null: return nil
        default: return nil
        }
    }

    private static func toInt(_ val: DBValue) -> Int? {
        switch val {
        case .integer(let v): return Int(v)
        case .double(let v): return Int(v)
        case .null: return nil
        default: return nil
        }
    }
}
