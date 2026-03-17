public enum CobaltConstants {
    public static let PAGE_SIZE: Int = 8192 // 8KB page
    public static let PAGE_HEADER_SIZE: Int = 28
    public static let SLOT_SIZE: Int = 6 // 2 bytes offset + 4 bytes length
    public static let ENCRYPTED_PAGE_SIZE: Int = 8220 // 12 nonce + 8192 ciphertext + 16 tag
    public static let NONCE_SIZE: Int = 12
    public static let AUTH_TAG_SIZE: Int = 16
    public static let SYSTEM_PAGE_DB_METADATA: Int = 0
    public static let SYSTEM_PAGE_TABLE_REGISTRY_START: Int = 1
    public static let ENCRYPTION_KEY_SIZE: Int = 32 // AES-256
    public static let CHECKSUM_SIZE: Int = 4 // CRC32 stored in last 4 bytes of page
    /// Usable end offset for record data (last 4 bytes reserved for checksum)
    public static let PAGE_USABLE_END: Int = PAGE_SIZE - CHECKSUM_SIZE

    /// Maximum inline record data size before overflow is needed
    /// (page - header - slot - record_header(12) - overflow_flag(1) - total_length(4) - overflow_pageID(4) - checksum)
    public static let MAX_INLINE_RECORD_SIZE: Int = PAGE_USABLE_END - PAGE_HEADER_SIZE - SLOT_SIZE - 12 - 9

    /// Overflow page payload: page data minus header, 4B next-overflow-pageID pointer, and checksum
    public static let OVERFLOW_PAGE_PAYLOAD: Int = PAGE_USABLE_END - PAGE_HEADER_SIZE - 4
}

/// Metadata stored as a JSON record on page 0
public struct DBMetadata: Codable, Sendable {
    public var freeListHead: Int
    public var indexRegistryPageID: Int
    public var freeSpaceBitmapPageID: Int
    public var checkpointLSN: UInt64

    public init(freeListHead: Int = 0, indexRegistryPageID: Int = 0, freeSpaceBitmapPageID: Int = 0, checkpointLSN: UInt64 = 0) {
        self.freeListHead = freeListHead
        self.indexRegistryPageID = indexRegistryPageID
        self.freeSpaceBitmapPageID = freeSpaceBitmapPageID
        self.checkpointLSN = checkpointLSN
    }

    public init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        freeListHead = try container.decode(Int.self, forKey: .freeListHead)
        indexRegistryPageID = try container.decode(Int.self, forKey: .indexRegistryPageID)
        freeSpaceBitmapPageID = try container.decodeIfPresent(Int.self, forKey: .freeSpaceBitmapPageID) ?? 0
        checkpointLSN = try container.decodeIfPresent(UInt64.self, forKey: .checkpointLSN) ?? 0
    }
}

/// Free space categories for the bitmap (2 bits per page)
public enum SpaceCategory: UInt8, Sendable {
    case full = 0       // < 256B free
    case low = 1        // 256B–2KB free
    case available = 2  // 2KB–6KB free
    case empty = 3      // > 6KB free
}
