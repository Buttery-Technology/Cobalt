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
}

/// Metadata stored as a JSON record on page 0
public struct DBMetadata: Codable, Sendable {
    public var freeListHead: Int
    public var indexRegistryPageID: Int

    public init(freeListHead: Int = 0, indexRegistryPageID: Int = 0) {
        self.freeListHead = freeListHead
        self.indexRegistryPageID = indexRegistryPageID
    }
}
