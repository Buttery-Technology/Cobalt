/// Timing of trigger execution relative to the triggering event
public enum TriggerTiming: String, Sendable, Codable {
    case before = "BEFORE"
    case after = "AFTER"
}

/// The DML event that fires the trigger
public enum TriggerEvent: String, Sendable, Codable {
    case insert = "INSERT"
    case update = "UPDATE"
    case delete = "DELETE"
}

/// Granularity of trigger execution
public enum TriggerForEach: String, Sendable, Codable {
    case row = "ROW"
    case statement = "STATEMENT"
}

/// Definition of a trigger
public struct TriggerDef: Sendable, Codable {
    public let name: String
    public let table: String
    public let timing: TriggerTiming
    public let event: TriggerEvent
    public let forEach: TriggerForEach
    public let body: [String]  // SQL statements to execute

    public init(name: String, table: String, timing: TriggerTiming, event: TriggerEvent, forEach: TriggerForEach, body: [String]) {
        self.name = name
        self.table = table
        self.timing = timing
        self.event = event
        self.forEach = forEach
        self.body = body
    }
}

/// Manages trigger registration, removal, and lookup
public final class TriggerManager: @unchecked Sendable {
    private var triggers: [String: [TriggerDef]] = [:]  // keyed by table name

    public init() {}

    /// Register a new trigger
    public func registerTrigger(_ trigger: TriggerDef) {
        let key = trigger.table.lowercased()
        if triggers[key] == nil {
            triggers[key] = [trigger]
        } else {
            triggers[key]!.append(trigger)
        }
    }

    /// Remove a trigger by name
    public func removeTrigger(name: String) {
        let lowerName = name.lowercased()
        for (table, defs) in triggers {
            let filtered = defs.filter { $0.name.lowercased() != lowerName }
            if filtered.isEmpty {
                triggers.removeValue(forKey: table)
            } else if filtered.count != defs.count {
                triggers[table] = filtered
            }
        }
    }

    /// Get all triggers for a table matching the given timing and event
    public func getTriggersForTable(_ table: String, timing: TriggerTiming, event: TriggerEvent) -> [TriggerDef] {
        let key = table.lowercased()
        guard let defs = triggers[key] else { return [] }
        return defs.filter { $0.timing == timing && $0.event == event }
    }

    /// List all registered triggers
    public func listTriggers() -> [TriggerDef] {
        triggers.values.flatMap { $0 }
    }
}
