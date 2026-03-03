import Foundation

/// Coordinates transaction lifecycle, using a closure to flush pages (avoids circular reference)
public actor TransactionManager: Sendable {
    private var activeTransactions: [UInt64: TransactionContext] = [:]
    private var nextTransactionID: UInt64 = 1
    private let logManager: WriteAheadLog
    private let defaultIsolationLevel: IsolationLevel

    /// MVCC: global monotonic version counter, incremented on each commit
    private var globalVersion: UInt64 = 0

    /// Write conflict index: recordID → txID of the single active writer.
    /// First-writer-wins: if a second txn tries to write the same record, it's a conflict.
    private var recordWriteOwners: [UInt64: UInt64] = [:]

    /// MVCC: tracks the lowest snapshot version still in use by an active transaction.
    /// Versions below this are safe for garbage collection. Updated on begin/commit/rollback.
    private var cachedMinSnapshotVersion: UInt64 = 0

    /// Closure injected by StorageEngine to flush modified pages
    public var pageFlusher: (@Sendable (Set<Int>) async throws -> Void)?

    /// Closure injected by StorageEngine to evict rolled-back pages from buffer pool
    public var pageInvalidator: (@Sendable (Set<Int>) async -> Void)?

    /// Closure injected by StorageEngine to flush all dirty pages (for checkpoint)
    public var allPageFlusher: (@Sendable () async throws -> Void)?

    public init(logManager: WriteAheadLog, defaultIsolationLevel: IsolationLevel = .readCommitted) async throws {
        self.logManager = logManager
        self.defaultIsolationLevel = defaultIsolationLevel

        // Recover nextTransactionID from WAL to prevent txID collisions after restart
        let maxTxID = try await logManager.recoverMaxTransactionID()
        if maxTxID > 0 {
            self.nextTransactionID = maxTxID + 1
        }
    }

    // MARK: - Transaction Management

    public func beginTransaction(isolationLevel: IsolationLevel? = nil) async throws -> TransactionContext {
        let txID = nextTransactionID
        nextTransactionID += 1

        let level = isolationLevel ?? defaultIsolationLevel
        // MVCC: capture current global version as the transaction's snapshot
        let txContext = TransactionContext(transactionID: txID, isolationLevel: level, snapshotVersion: globalVersion)

        // WAL write first — if it fails, no phantom transaction is registered
        try await logManager.logTransactionBegin(txID: txID, isolationLevel: level)
        activeTransactions[txID] = txContext
        recomputeMinSnapshotVersion()
        return txContext
    }

    public func commitTransaction(_ txContext: TransactionContext) async throws {
        guard let activeContext = activeTransactions[txContext.transactionID],
              activeContext.isActive else {
            throw PantryError.invalidTransactionState
        }

        if txContext.isolationLevel == .serializable {
            try validateSerializableTransaction(txContext)
        }

        // MVCC: write-write conflict detection for repeatable read and above
        if txContext.isolationLevel.rawValue >= IsolationLevel.repeatableRead.rawValue {
            try await detectWriteWriteConflicts(txContext)
        }

        // WAL protocol: commit record must be durable BEFORE pages are flushed
        try await logManager.logTransactionCommit(txID: txContext.transactionID, isolationLevel: txContext.isolationLevel)

        // Transaction is committed in WAL — mark it committed and remove from active
        // BEFORE flushing pages, so a pageFlusher failure doesn't leave it in limbo
        txContext.commit()
        cleanupConflictIndex(txID: txContext.transactionID, writtenIDs: txContext.writtenRecordIDs)
        activeTransactions.removeValue(forKey: txContext.transactionID)

        // MVCC: advance global version and recompute minimum snapshot
        globalVersion += 1
        recomputeMinSnapshotVersion()

        // Modified pages remain dirty in buffer pool — WAL provides durability.
        // Background writer flushes them asynchronously (like SQLite WAL mode).
    }

    /// Register a record write for conflict tracking.
    /// First-writer-wins: if another active txn already owns this record, throws immediately.
    public func registerWrite(txID: UInt64, recordID: UInt64) throws {
        if let existingOwner = recordWriteOwners[recordID], existingOwner != txID {
            throw PantryError.writeWriteConflict
        }
        recordWriteOwners[recordID] = txID
    }

    /// MVCC: detect write-write conflicts — now a no-op since conflicts are caught eagerly in registerWrite.
    private func detectWriteWriteConflicts(_ txContext: TransactionContext) async throws {
        // Conflicts are detected eagerly at write time via registerWrite().
        // This method is retained for the commit path but no work is needed.
    }

    public func rollbackTransaction(_ txContext: TransactionContext) async throws {
        guard let activeContext = activeTransactions[txContext.transactionID],
              activeContext.isActive else {
            throw PantryError.invalidTransactionState
        }

        try await logManager.undoTransaction(txID: txContext.transactionID)
        try await logManager.logTransactionRollback(txID: txContext.transactionID, isolationLevel: txContext.isolationLevel)

        // Evict rolled-back pages from buffer pool so subsequent reads get the restored on-disk version
        let modifiedPages = txContext.modifiedPages
        if let invalidator = pageInvalidator {
            await invalidator(modifiedPages)
        }

        txContext.rollback()
        cleanupConflictIndex(txID: txContext.transactionID, writtenIDs: txContext.writtenRecordIDs)
        activeTransactions.removeValue(forKey: txContext.transactionID)
        recomputeMinSnapshotVersion()
    }

    /// Remove a transaction's entries from the conflict index
    private func cleanupConflictIndex(txID: UInt64, writtenIDs: Set<UInt64>) {
        for recordID in writtenIDs {
            if recordWriteOwners[recordID] == txID {
                recordWriteOwners.removeValue(forKey: recordID)
            }
        }
    }

    // MARK: - Validation

    private func validateSerializableTransaction(_ txContext: TransactionContext) throws {
        for pageID in txContext.readPages {
            for (otherTxID, otherTx) in activeTransactions {
                if otherTxID == txContext.transactionID { continue }
                if otherTx.writePages.contains(pageID) {
                    throw PantryError.serializationConflict
                }
            }
        }
    }

    // MARK: - Status

    public func getActiveTransactionCount() -> Int {
        activeTransactions.count
    }

    public func isTransactionActive(txID: UInt64) -> Bool {
        activeTransactions[txID]?.isActive ?? false
    }

    public func getActiveTransactionIDs() -> [UInt64] {
        Array(activeTransactions.keys)
    }

    /// MVCC: return current global version (for monitoring/debugging)
    public func getCurrentVersion() -> UInt64 {
        globalVersion
    }

    /// MVCC: minimum snapshot version across all active transactions.
    /// Versions below this are invisible to all active readers and safe for garbage collection.
    public func getMinimumActiveSnapshotVersion() -> UInt64 {
        cachedMinSnapshotVersion
    }

    /// Recompute the cached minimum snapshot version from active transactions
    private func recomputeMinSnapshotVersion() {
        if activeTransactions.isEmpty {
            cachedMinSnapshotVersion = globalVersion
        } else {
            cachedMinSnapshotVersion = activeTransactions.values.reduce(UInt64.max) { min($0, $1.snapshotVersion) }
        }
    }

    // MARK: - Checkpointing

    /// LSN of the most recent checkpoint (for persistence by StorageEngine)
    public private(set) var lastCheckpointLSN: UInt64 = 0

    public func createCheckpoint() async throws {
        guard activeTransactions.isEmpty else {
            throw PantryError.invalidTransactionState
        }

        // MVCC cleanup: no active transactions, so all conflict tracking can be purged.
        // This prevents orphaned entries from accumulating after crashes.
        if !recordWriteOwners.isEmpty {
            recordWriteOwners.removeAll()
        }
        cachedMinSnapshotVersion = globalVersion

        // Flush all dirty pages to disk first
        if let flusher = allPageFlusher {
            try await flusher()
        }

        // Write checkpoint record then truncate WAL
        let count = UInt32(activeTransactions.count)
        let lsn = try await logManager.createCheckpoint(activeTransactionCount: count)
        lastCheckpointLSN = lsn
        try await logManager.truncate()
    }

    // MARK: - Deadlock Detection

    public func detectAndResolveDeadlocks() async throws {
        let longRunningTxs = identifyLongRunningTransactions()
        for txID in longRunningTxs {
            if let txContext = activeTransactions[txID] {
                try await rollbackTransaction(txContext)
            }
        }
    }

    /// Configurable long-running transaction timeout (seconds)
    private var longRunningTxTimeoutSeconds: TimeInterval = 30

    public func setLongRunningTxTimeout(_ seconds: Int) {
        longRunningTxTimeoutSeconds = TimeInterval(seconds)
    }

    private func identifyLongRunningTransactions() -> [UInt64] {
        let thresholdSeconds: TimeInterval = longRunningTxTimeoutSeconds
        let now = Date()
        return activeTransactions.compactMap { (txID, context) in
            let runningTime = now.timeIntervalSince(context.startTime)
            return runningTime > thresholdSeconds ? txID : nil
        }
    }
}
