import Foundation

/// Serializable snapshot of a BloomFilter's state for persistence
public struct BloomFilterSnapshot: Codable, Sendable {
    public let bitArray: [UInt64]
    public let hashFunctions: Int
    public let size: Int
}

/// Probabilistic data structure for fast negative lookups.
/// A negative result is definitive; a positive result may be a false positive.
/// Uses a packed UInt64 bitset for ~8x memory reduction over [Bool].
public struct BloomFilter: Sendable, Codable {
    private var bitArray: [UInt64]
    private let hashFunctions: Int
    private let size: Int

    /// Create a bloom filter sized for the expected number of elements
    /// - Parameters:
    ///   - expectedElements: Anticipated number of distinct elements
    ///   - falsePositiveRate: Target false positive probability (default 1%)
    public init(expectedElements: Int, falsePositiveRate: Double = 0.01) {
        let n = max(expectedElements, 1)
        let m = -Double(n) * log(falsePositiveRate) / pow(log(2), 2)
        self.size = max(Int(ceil(m)), 1)
        self.hashFunctions = max(Int(ceil(Double(size) / Double(n) * log(2))), 1)
        // Allocate packed bit array: ceil(size / 64) words
        self.bitArray = Array(repeating: 0, count: (size + 63) / 64)
    }

    /// Add an element to the filter
    public mutating func add(_ element: String) {
        for i in 0..<hashFunctions {
            let bit = getHash(element, seed: i) % size
            bitArray[bit / 64] |= (1 << (bit % 64))
        }
    }

    /// Check if an element might be in the set.
    /// Returns false → definitely not present. Returns true → possibly present.
    public func contains(_ element: String) -> Bool {
        for i in 0..<hashFunctions {
            let bit = getHash(element, seed: i) % size
            if bitArray[bit / 64] & (1 << (bit % 64)) == 0 {
                return false
            }
        }
        return true
    }

    /// Capture the current state as a serializable snapshot
    public var snapshot: BloomFilterSnapshot {
        BloomFilterSnapshot(bitArray: bitArray, hashFunctions: hashFunctions, size: size)
    }

    /// Restore a bloom filter from a persisted snapshot
    public static func fromSnapshot(_ snap: BloomFilterSnapshot) -> BloomFilter? {
        guard snap.size > 0, snap.hashFunctions > 0, snap.bitArray.count == (snap.size + 63) / 64 else { return nil }
        return BloomFilter(bitArray: snap.bitArray, hashFunctions: snap.hashFunctions, size: snap.size)
    }

    /// Private memberwise init for snapshot restoration
    private init(bitArray: [UInt64], hashFunctions: Int, size: Int) {
        self.bitArray = bitArray
        self.hashFunctions = hashFunctions
        self.size = size
    }

    /// Deterministic FNV-1a hash (safe across process restarts unlike Swift's Hasher)
    private func getHash(_ element: String, seed: Int) -> Int {
        var hash: UInt64 = 14695981039346656037 // FNV offset basis
        // Mix in the seed
        var seedBits = UInt64(bitPattern: Int64(seed))
        for _ in 0..<8 {
            hash ^= seedBits & 0xFF
            hash &*= 1099511628211 // FNV prime
            seedBits >>= 8
        }
        // Hash the element bytes
        for byte in element.utf8 {
            hash ^= UInt64(byte)
            hash &*= 1099511628211
        }
        return Int(hash & UInt64(Int.max))
    }
}
