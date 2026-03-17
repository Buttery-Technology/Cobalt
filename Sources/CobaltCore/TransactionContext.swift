import Foundation

/// Tracks the state and resources of a single transaction
/// Converted from actor to class with internal locking to eliminate actor hops.
public final class TransactionContext: @unchecked Sendable {
    public let transactionID: UInt64
    public let startTime: Date
    public let isolationLevel: IsolationLevel
    /// MVCC: snapshot version captured at transaction start for repeatable reads
    public let snapshotVersion: UInt64

    private struct State {
        var state: TransactionState = .active
        var modifiedPages = Set<Int>()
        var endTime: Date?
        var heldLocks = [ResourceLock]()
        var readPages = Set<Int>()
        var writePages = Set<Int>()
        var beforeImagePages = Set<Int>()
        var writtenRecordIDs = Set<UInt64>()
    }
    private let _state: CobaltLock<State>

    /// A lightweight sentinel used to reserve the transaction slot during async BEGIN.
    public static let sentinel = TransactionContext(transactionID: UInt64.max, isolationLevel: .readCommitted, snapshotVersion: 0)

    public init(transactionID: UInt64, isolationLevel: IsolationLevel = .readCommitted, snapshotVersion: UInt64 = 0) {
        self.transactionID = transactionID
        self.startTime = Date()
        self.isolationLevel = isolationLevel
        self.snapshotVersion = snapshotVersion
        self._state = CobaltLock(State())
    }

    public var state: TransactionState {
        _state.withLock { $0.state }
    }

    public var modifiedPages: Set<Int> {
        _state.withLock { $0.modifiedPages }
    }

    public var endTime: Date? {
        _state.withLock { $0.endTime }
    }

    public var heldLocks: [ResourceLock] {
        _state.withLock { $0.heldLocks }
    }

    public var readPages: Set<Int> {
        _state.withLock { $0.readPages }
    }

    public var writePages: Set<Int> {
        _state.withLock { $0.writePages }
    }

    public var beforeImagePages: Set<Int> {
        _state.withLock { $0.beforeImagePages }
    }

    public var writtenRecordIDs: Set<UInt64> {
        _state.withLock { $0.writtenRecordIDs }
    }

    public func recordAccess(pageID: Int, isWrite: Bool) {
        _state.withLock { s in
            if isWrite {
                s.writePages.insert(pageID)
            } else {
                s.readPages.insert(pageID)
            }
        }
    }

    public func markModified(pageID: Int) {
        _state.withLock { $0.modifiedPages.insert(pageID) }
    }

    public func recordBeforeImage(pageID: Int) {
        _state.withLock { $0.beforeImagePages.insert(pageID) }
    }

    public func addLock(_ lock: ResourceLock) {
        _state.withLock { $0.heldLocks.append(lock) }
    }

    /// MVCC: track a record ID as written by this transaction
    public func recordWrite(recordID: UInt64) {
        _state.withLock { $0.writtenRecordIDs.insert(recordID) }
    }

    public func commit() {
        _state.withLock { s in
            s.state = .committed
            s.endTime = Date()
        }
    }

    public func rollback() {
        _state.withLock { s in
            s.state = .rolledBack
            s.endTime = Date()
        }
    }

    public var isActive: Bool {
        _state.withLock { $0.state == .active }
    }

    public var duration: TimeInterval? {
        _state.withLock { s in
            guard let end = s.endTime else { return nil }
            return end.timeIntervalSince(startTime)
        }
    }

    /// Check if a before image has been recorded for this page (non-mutating)
    public func hasBeforeImage(pageID: Int) -> Bool {
        _state.withLock { $0.beforeImagePages.contains(pageID) }
    }
}

/// Transaction lifecycle states
public enum TransactionState: Sendable {
    case active
    case committed
    case rolledBack
}

/// Represents a lock held on a database resource
public struct ResourceLock: Sendable {
    public enum LockType: Sendable {
        case shared
        case exclusive
        case intent
    }

    public let resourceType: ResourceType
    public let resourceID: Int
    public let lockType: LockType

    public init(resourceType: ResourceType, resourceID: Int, lockType: LockType) {
        self.resourceType = resourceType
        self.resourceID = resourceID
        self.lockType = lockType
    }
}

/// Types of lockable resources
public enum ResourceType: Sendable {
    case database
    case table
    case page
    case row
}
