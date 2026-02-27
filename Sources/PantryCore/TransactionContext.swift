import Foundation

/// Tracks the state and resources of a single transaction
public actor TransactionContext: Sendable {
    public let transactionID: UInt64
    public private(set) var state: TransactionState = .active
    public private(set) var modifiedPages = Set<Int>()
    public let startTime: Date
    public private(set) var endTime: Date?
    public let isolationLevel: IsolationLevel
    public private(set) var heldLocks = [ResourceLock]()
    public private(set) var readPages = Set<Int>()
    public private(set) var writePages = Set<Int>()

    public init(transactionID: UInt64, isolationLevel: IsolationLevel = .readCommitted) {
        self.transactionID = transactionID
        self.startTime = Date()
        self.isolationLevel = isolationLevel
    }

    public func recordAccess(pageID: Int, isWrite: Bool) {
        if isWrite {
            writePages.insert(pageID)
        } else {
            readPages.insert(pageID)
        }
    }

    public func markModified(pageID: Int) {
        modifiedPages.insert(pageID)
    }

    public func addLock(_ lock: ResourceLock) {
        heldLocks.append(lock)
    }

    public func commit() {
        state = .committed
        endTime = Date()
    }

    public func rollback() {
        state = .rolledBack
        endTime = Date()
    }

    public var isActive: Bool {
        state == .active
    }

    public var duration: TimeInterval? {
        guard let end = endTime else { return nil }
        return end.timeIntervalSince(startTime)
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
