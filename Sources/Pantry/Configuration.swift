import Foundation
import PantryCore
#if canImport(Security)
import Security
#endif

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

    public init(
        path: String,
        encryptionKey: Data? = nil,
        bufferPoolCapacity: Int = 1000,
        isolationLevel: IsolationLevel = .readCommitted
    ) {
        precondition(!path.isEmpty, "Database path must not be empty")
        precondition(bufferPoolCapacity > 0, "Buffer pool capacity must be positive")
        if let key = encryptionKey {
            precondition(key.count == 32, "Encryption key must be exactly 32 bytes for AES-256")
        }
        self.path = path
        self.encryptionKey = encryptionKey
        self.bufferPoolCapacity = bufferPoolCapacity
        self.isolationLevel = isolationLevel
    }

    // MARK: - Default Path Helpers

    /// Returns the default directory for Pantry databases.
    /// Uses `~/Library/Application Support/Pantry/`, falling back to the temp directory.
    public static func defaultDirectory() -> String {
        let fm = FileManager.default
        if let appSupport = fm.urls(for: .applicationSupportDirectory, in: .userDomainMask).first {
            let dir = appSupport.appendingPathComponent("Pantry").path
            try? fm.createDirectory(atPath: dir, withIntermediateDirectories: true)
            return dir
        }
        return NSTemporaryDirectory()
    }

    /// Builds a full database path from a short name.
    /// e.g. `databasePath(name: "myapp")` → `~/Library/Application Support/Pantry/myapp.pantry`
    public static func databasePath(name: String = "default") -> String {
        let dir = defaultDirectory()
        return (dir as NSString).appendingPathComponent("\(name).pantry")
    }

    // MARK: - Key Management

    /// Generate a cryptographically random 32-byte key suitable for AES-256-GCM encryption.
    public static func generateKey() -> Data {
        var bytes = [UInt8](repeating: 0, count: 32)
        let status = SecRandomCopyBytes(kSecRandomDefault, bytes.count, &bytes)
        precondition(status == errSecSuccess, "Failed to generate random key")
        return Data(bytes)
    }

    /// Loads or creates an encryption key for the given database path.
    /// The key is stored at `<dbPath>.key`. If the file exists and contains 32 bytes, it is reused.
    /// Otherwise a new key is generated and written to disk.
    internal static func resolveEncryptionKey(for dbPath: String) throws -> Data {
        let keyPath = dbPath + ".key"
        let fm = FileManager.default

        if fm.fileExists(atPath: keyPath),
           let existing = fm.contents(atPath: keyPath),
           existing.count == 32 {
            return existing
        }

        let newKey = generateKey()
        let dir = (keyPath as NSString).deletingLastPathComponent
        try fm.createDirectory(atPath: dir, withIntermediateDirectories: true)
        let keyURL = URL(fileURLWithPath: keyPath)
        try newKey.write(to: keyURL, options: .atomic)
        try fm.setAttributes([.posixPermissions: 0o600], ofItemAtPath: keyPath)
        return newKey
    }
}
