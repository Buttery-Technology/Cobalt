public enum PantryConstants {
    public static let PAGE_SIZE: Int = 8192 // 8KB page
    public static let PAGE_HEADER_SIZE: Int = 28
    public static let SLOT_SIZE: Int = 6 // 2 bytes offset + 4 bytes length
    public static let ENCRYPTED_PAGE_SIZE: Int = 8220 // 12 nonce + 8192 ciphertext + 16 tag
    public static let NONCE_SIZE: Int = 12
    public static let AUTH_TAG_SIZE: Int = 16
    public static let SYSTEM_PAGE_DB_METADATA: Int = 0
    public static let SYSTEM_PAGE_TABLE_REGISTRY_START: Int = 1
    public static let ENCRYPTION_KEY_SIZE: Int = 32 // AES-256

    /// Maximum inline record data size before overflow is needed
    /// (page - header - slot - record_header(12) - overflow_flag(1) - total_length(4) - overflow_pageID(4))
    public static let MAX_INLINE_RECORD_SIZE: Int = PAGE_SIZE - PAGE_HEADER_SIZE - SLOT_SIZE - 12 - 9

    /// Overflow page payload: page data minus header and 4B next-overflow-pageID pointer
    public static let OVERFLOW_PAGE_PAYLOAD: Int = PAGE_SIZE - PAGE_HEADER_SIZE - 4
}

/// Metadata stored as a JSON record on page 0
public struct DBMetadata: Codable, Sendable {
    public var freeListHead: Int
    public var indexRegistryPageID: Int
    public var freeSpaceBitmapPageID: Int

    public init(freeListHead: Int = 0, indexRegistryPageID: Int = 0, freeSpaceBitmapPageID: Int = 0) {
        self.freeListHead = freeListHead
        self.indexRegistryPageID = indexRegistryPageID
        self.freeSpaceBitmapPageID = freeSpaceBitmapPageID
    }
}

/// Free space categories for the bitmap (2 bits per page)
public enum SpaceCategory: UInt8, Sendable {
    case full = 0       // < 256B free
    case low = 1        // 256B–2KB free
    case available = 2  // 2KB–6KB free
    case empty = 3      // > 6KB free
}
