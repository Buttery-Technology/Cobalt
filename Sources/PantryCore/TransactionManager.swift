import Foundation

/// Coordinates transaction lifecycle, using a closure to flush pages (avoids circular reference)
public actor TransactionManager: Sendable {
    private var activeTransactions: [UInt64: TransactionContext] = [:]
    private var nextTransactionID: UInt64 = 1
    private let logManager: WriteAheadLog
    private let defaultIsolationLevel: IsolationLevel

    /// MVCC: global monotonic version counter, incremented on each commit
    private var globalVersion: UInt64 = 0

    /// Write conflict index: recordID → set of txIDs that have written it
    /// Enables O(n) conflict detection instead of O(n*m)
    private var recordWriteOwners: [UInt64: Set<UInt64>] = [:]

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
              await activeContext.isActive else {
            throw PantryError.invalidTransactionState
        }

        if txContext.isolationLevel == .serializable {
            try await validateSerializableTransaction(txContext)
        }

        // MVCC: write-write conflict detection for repeatable read and above
        if txContext.isolationLevel.rawValue >= IsolationLevel.repeatableRead.rawValue {
            try await detectWriteWriteConflicts(txContext)
        }

        // WAL protocol: commit record must be durable BEFORE pages are flushed
        try await logManager.logTransactionCommit(txID: txContext.transactionID, isolationLevel: txContext.isolationLevel)

        // Transaction is committed in WAL — mark it committed and remove from active
        // BEFORE flushing pages, so a pageFlusher failure doesn't leave it in limbo
        await txContext.commit()
        cleanupConflictIndex(txID: txContext.transactionID, writtenIDs: await txContext.writtenRecordIDs)
        activeTransactions.removeValue(forKey: txContext.transactionID)

        // MVCC: advance global version and recompute minimum snapshot
        globalVersion += 1
        recomputeMinSnapshotVersion()

        // Now safe to flush modified pages to disk
        let modifiedPages = await txContext.modifiedPages
        if let flusher = pageFlusher {
            try await flusher(modifiedPages)
        }
    }

    /// Register a record write for conflict tracking
    public func registerWrite(txID: UInt64, recordID: UInt64) {
        recordWriteOwners[recordID, default: []].insert(txID)
    }

    /// MVCC: detect write-write conflicts using the conflict index — O(n) per commit
    private func detectWriteWriteConflicts(_ txContext: TransactionContext) async throws {
        let myWrittenIDs = await txContext.writtenRecordIDs
        guard !myWrittenIDs.isEmpty else { return }

        let myTxID = txContext.transactionID
        for recordID in myWrittenIDs {
            if let owners = recordWriteOwners[recordID] {
                // Conflict if another active transaction also wrote this record
                let otherWriters = owners.subtracting([myTxID])
                if !otherWriters.isEmpty {
                    throw PantryError.writeWriteConflict
                }
            }
        }
    }

    public func rollbackTransaction(_ txContext: TransactionContext) async throws {
        guard let activeContext = activeTransactions[txContext.transactionID],
              await activeContext.isActive else {
            throw PantryError.invalidTransactionState
        }

        try await logManager.undoTransaction(txID: txContext.transactionID)
        try await logManager.logTransactionRollback(txID: txContext.transactionID, isolationLevel: txContext.isolationLevel)

        // Evict rolled-back pages from buffer pool so subsequent reads get the restored on-disk version
        let modifiedPages = await txContext.modifiedPages
        if let invalidator = pageInvalidator {
            await invalidator(modifiedPages)
        }

        await txContext.rollback()
        cleanupConflictIndex(txID: txContext.transactionID, writtenIDs: await txContext.writtenRecordIDs)
        activeTransactions.removeValue(forKey: txContext.transactionID)
        recomputeMinSnapshotVersion()
    }

    /// Remove a transaction's entries from the conflict index
    private func cleanupConflictIndex(txID: UInt64, writtenIDs: Set<UInt64>) {
        for recordID in writtenIDs {
            recordWriteOwners[recordID]?.remove(txID)
            if recordWriteOwners[recordID]?.isEmpty == true {
                recordWriteOwners.removeValue(forKey: recordID)
            }
        }
    }

    // MARK: - Validation

    private func validateSerializableTransaction(_ txContext: TransactionContext) async throws {
        for pageID in await txContext.readPages {
            for (otherTxID, otherTx) in activeTransactions {
                if otherTxID == txContext.transactionID { continue }
                if await otherTx.writePages.contains(pageID) {
                    throw PantryError.serializationConflict
                }
            }
        }
    }

    // MARK: - Status

    public func getActiveTransactionCount() -> Int {
        activeTransactions.count
    }

    public func isTransactionActive(txID: UInt64) async -> Bool {
        await activeTransactions[txID]?.isActive ?? false
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
        try await logManager.createCheckpoint(activeTransactionCount: count)
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

    private func identifyLongRunningTransactions() -> [UInt64] {
        let thresholdSeconds: TimeInterval = 30
        let now = Date()
        return activeTransactions.compactMap { (txID, context) in
            let runningTime = now.timeIntervalSince(context.startTime)
            return runningTime > thresholdSeconds ? txID : nil
        }
    }
}
