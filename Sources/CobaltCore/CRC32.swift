import Foundation

/// IEEE 802.3 CRC32 implementation with standard polynomial 0xEDB88320
public enum CRC32 {
    /// Precomputed lookup table for IEEE polynomial
    private static let table: [UInt32] = {
        (0..<256).map { i -> UInt32 in
            var crc = UInt32(i)
            for _ in 0..<8 {
                if crc & 1 != 0 {
                    crc = (crc >> 1) ^ 0xEDB88320
                } else {
                    crc = crc >> 1
                }
            }
            return crc
        }
    }()

    /// Calculate CRC32 checksum for the given data
    public static func checksum(_ data: Data) -> UInt32 {
        var crc: UInt32 = 0xFFFFFFFF
        for byte in data {
            let index = Int((crc ^ UInt32(byte)) & 0xFF)
            crc = (crc >> 8) ^ table[index]
        }
        return crc ^ 0xFFFFFFFF
    }

    /// Calculate CRC32 checksum for a byte buffer
    public static func checksum(_ bytes: [UInt8]) -> UInt32 {
        var crc: UInt32 = 0xFFFFFFFF
        for byte in bytes {
            let index = Int((crc ^ UInt32(byte)) & 0xFF)
            crc = (crc >> 8) ^ table[index]
        }
        return crc ^ 0xFFFFFFFF
    }

    /// Calculate CRC32 checksum over a raw buffer pointer region
    public static func checksum(_ buffer: UnsafeRawBufferPointer, count: Int) -> UInt32 {
        guard let base = buffer.baseAddress else { return 0 }
        var crc: UInt32 = 0xFFFFFFFF
        let bytes = base.assumingMemoryBound(to: UInt8.self)
        for i in 0..<count {
            let index = Int((crc ^ UInt32(bytes[i])) & 0xFF)
            crc = (crc >> 8) ^ table[index]
        }
        return crc ^ 0xFFFFFFFF
    }
}
