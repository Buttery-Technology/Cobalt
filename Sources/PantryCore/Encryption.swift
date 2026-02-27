import Foundation
import Crypto

/// Protocol for page-level encryption/decryption
public protocol EncryptionProvider: Sendable {
    func encrypt(_ data: Data) throws -> Data
    func decrypt(_ data: Data) throws -> Data
}

/// AES-256-GCM encryption provider with fresh nonce per write
public struct AESGCMEncryptionProvider: EncryptionProvider, Sendable {
    private let key: SymmetricKey

    /// Initialize with a 32-byte key for AES-256
    public init(key: Data) throws {
        guard key.count == PantryConstants.ENCRYPTION_KEY_SIZE else {
            throw PantryError.invalidEncryptionKey
        }
        self.key = SymmetricKey(data: key)
    }

    /// Encrypt data: returns [nonce: 12 bytes][ciphertext][tag: 16 bytes]
    public func encrypt(_ plaintext: Data) throws -> Data {
        do {
            let nonce = AES.GCM.Nonce()
            let sealedBox = try AES.GCM.seal(plaintext, using: key, nonce: nonce)

            var result = Data()
            result.append(contentsOf: nonce)
            result.append(sealedBox.ciphertext)
            result.append(sealedBox.tag)
            return result
        } catch {
            throw PantryError.encryptionFailed(description: error.localizedDescription)
        }
    }

    /// Decrypt data formatted as [nonce: 12][ciphertext][tag: 16]
    public func decrypt(_ data: Data) throws -> Data {
        let nonceSize = PantryConstants.NONCE_SIZE
        let tagSize = PantryConstants.AUTH_TAG_SIZE
        guard data.count > nonceSize + tagSize else {
            throw PantryError.decryptionFailed(description: "Data too short")
        }

        do {
            let nonceData = data.prefix(nonceSize)
            let ciphertext = data.dropFirst(nonceSize).dropLast(tagSize)
            let tag = data.suffix(tagSize)

            let nonce = try AES.GCM.Nonce(data: nonceData)
            let sealedBox = try AES.GCM.SealedBox(nonce: nonce, ciphertext: ciphertext, tag: tag)
            return try AES.GCM.open(sealedBox, using: key)
        } catch let error as PantryError {
            throw error
        } catch {
            throw PantryError.decryptionFailed(description: error.localizedDescription)
        }
    }
}
