import Foundation

/// Probabilistic data structure for fast negative lookups.
/// A negative result is definitive; a positive result may be a false positive.
public struct BloomFilter: Sendable, Codable {
    private var bitArray: [Bool]
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
        self.bitArray = Array(repeating: false, count: size)
    }

    /// Add an element to the filter
    public mutating func add(_ element: String) {
        for i in 0..<hashFunctions {
            let hash = getHash(element, seed: i) % size
            bitArray[hash] = true
        }
    }

    /// Check if an element might be in the set.
    /// Returns false → definitely not present. Returns true → possibly present.
    public func contains(_ element: String) -> Bool {
        for i in 0..<hashFunctions {
            let hash = getHash(element, seed: i) % size
            if !bitArray[hash] {
                return false
            }
        }
        return true
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
