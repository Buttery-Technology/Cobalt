import Foundation

/// Coordinates transaction lifecycle, using a closure to flush pages (avoids circular reference)
public actor TransactionManager: Sendable {
    private var activeTransactions: [UInt64: TransactionContext] = [:]
    private var nextTransactionID: UInt64 = 1
    private let logManager: WriteAheadLog
    private let defaultIsolationLevel: IsolationLevel

    /// Closure injected by StorageEngine to flush modified pages
    public var pageFlusher: (@Sendable (Set<Int>) async throws -> Void)?

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
        let txContext = TransactionContext(transactionID: txID, isolationLevel: level)

        // WAL write first — if it fails, no phantom transaction is registered
        try await logManager.logTransactionBegin(txID: txID, isolationLevel: level)
        activeTransactions[txID] = txContext
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

        // WAL protocol: commit record must be durable BEFORE pages are flushed
        try await logManager.logTransactionCommit(txID: txContext.transactionID)

        // Transaction is committed in WAL — mark it committed and remove from active
        // BEFORE flushing pages, so a pageFlusher failure doesn't leave it in limbo
        await txContext.commit()
        activeTransactions.removeValue(forKey: txContext.transactionID)

        // Now safe to flush modified pages to disk
        let modifiedPages = await txContext.modifiedPages
        if let flusher = pageFlusher {
            try await flusher(modifiedPages)
        }
    }

    public func rollbackTransaction(_ txContext: TransactionContext) async throws {
        guard let activeContext = activeTransactions[txContext.transactionID],
              await activeContext.isActive else {
            throw PantryError.invalidTransactionState
        }

        try await logManager.undoTransaction(txID: txContext.transactionID)
        try await logManager.logTransactionRollback(txID: txContext.transactionID)
        await txContext.rollback()
        activeTransactions.removeValue(forKey: txContext.transactionID)
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

    // MARK: - Checkpointing

    public func createCheckpoint() async throws {
        let count = UInt32(activeTransactions.count)
        try await logManager.createCheckpoint(activeTransactionCount: count)
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
