import Foundation
import PantryCore

/// Configuration for creating a PantryDatabase instance
public struct PantryConfiguration: Sendable {
    /// Path to the database file on disk
    public let path: String

    /// Optional 32-byte encryption key for AES-256-GCM at-rest encryption
    public let encryptionKey: Data?

    /// Maximum number of pages cached in memory
    public let bufferPoolCapacity: Int

    /// Default transaction isolation level
    public let isolationLevel: IsolationLevel

    /// Whether to enable write-ahead logging
    public let walEnabled: Bool

    public init(
        path: String,
        encryptionKey: Data? = nil,
        bufferPoolCapacity: Int = 1000,
        isolationLevel: IsolationLevel = .readCommitted,
        walEnabled: Bool = true
    ) {
        self.path = path
        self.encryptionKey = encryptionKey
        self.bufferPoolCapacity = bufferPoolCapacity
        self.isolationLevel = isolationLevel
        self.walEnabled = walEnabled
    }
}
