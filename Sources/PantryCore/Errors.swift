public enum PantryError: Error, Sendable {
    // Storage errors
    case pageReadError(description: String? = nil)
    case pageWriteError(description: String? = nil)
    case pageNotFound(pageID: Int)
    case pageOverflow
    case corruptPage(pageID: Int)
    case unrecoverablePage(pageID: Int)
    case invalidDatabasePath(description: String? = nil)
    case databaseFileCorrupt
    case databaseClosed
    case fileOpenError(description: String? = nil)

    // Record errors
    case recordNotFound(id: UInt64)
    case recordTooLarge(size: Int)

    // Schema / table errors
    case tableNotFound(name: String)
    case tableAlreadyExists(name: String)
    case columnNotFound(name: String)
    case typeMismatch(column: String, expected: String)
    case notNullConstraintViolation(column: String)
    case primaryKeyViolation
    case schemaSerializationError

    // Transaction errors
    case transactionNotFound
    case invalidTransactionState
    case serializationConflict
    case writeWriteConflict
    case deadlockDetected
    case lockTimeout

    // Encryption errors
    case encryptionFailed(description: String? = nil)
    case decryptionFailed(description: String? = nil)
    case encryptionKeyRequired
    case invalidEncryptionKey
    case corruptedKeyFile(path: String)

    // WAL errors
    case invalidLogRecordType
    case pageNotInLog
    case corruptLogRecord
    case logReadError
    case walWriteError(description: String? = nil)

    // Index errors
    case indexCorrupted(description: String)
    case indexNotFound(table: String, column: String)

    // Query errors
    case invalidQuery(description: String)
}
