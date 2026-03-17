import Foundation

/// Represents a database role (user/group).
public struct Role: Sendable, Codable {
    public let name: String
    public let isSuperuser: Bool
    public let canLogin: Bool
    public let passwordHash: String?

    public init(name: String, isSuperuser: Bool = false, canLogin: Bool = true, passwordHash: String? = nil) {
        self.name = name
        self.isSuperuser = isSuperuser
        self.canLogin = canLogin
        self.passwordHash = passwordHash
    }
}

/// Database permission types.
public enum Permission: String, Sendable, Codable, CaseIterable {
    case select = "SELECT"
    case insert = "INSERT"
    case update = "UPDATE"
    case delete = "DELETE"
    case create = "CREATE"
    case drop = "DROP"
    case all = "ALL"
}

/// A grant of a permission to a role, optionally scoped to a table.
public struct RoleGrant: Sendable, Codable {
    public let role: String
    public let permission: Permission
    public let table: String?  // nil = global

    public init(role: String, permission: Permission, table: String? = nil) {
        self.role = role
        self.permission = permission
        self.table = table
    }
}

/// Manages roles and their permissions.
public final class RoleManager: @unchecked Sendable {
    private var roles: [String: Role] = [:]
    private var grants: [RoleGrant] = []
    private let lock = NSLock()

    public init() {
        // Create default superuser role
        roles["cobalt"] = Role(name: "cobalt", isSuperuser: true, canLogin: true, passwordHash: nil)
    }

    /// Create a new role. Throws if the role already exists.
    public func createRole(_ role: Role) throws {
        lock.lock()
        defer { lock.unlock() }
        guard roles[role.name] == nil else {
            throw RoleError.roleAlreadyExists(role.name)
        }
        roles[role.name] = role
    }

    /// Drop a role by name. Throws if the role does not exist.
    public func dropRole(_ name: String) throws {
        lock.lock()
        defer { lock.unlock() }
        guard roles.removeValue(forKey: name) != nil else {
            throw RoleError.roleNotFound(name)
        }
        // Remove all grants for this role
        grants.removeAll { $0.role == name }
    }

    /// Grant a permission to a role, optionally on a specific table.
    public func grant(_ permission: Permission, to role: String, on table: String? = nil) {
        lock.lock()
        defer { lock.unlock() }
        // Avoid duplicate grants
        let exists = grants.contains { $0.role == role && $0.permission == permission && $0.table == table }
        if !exists {
            grants.append(RoleGrant(role: role, permission: permission, table: table))
        }
    }

    /// Revoke a permission from a role, optionally on a specific table.
    public func revoke(_ permission: Permission, from role: String, on table: String? = nil) {
        lock.lock()
        defer { lock.unlock() }
        grants.removeAll { $0.role == role && $0.permission == permission && $0.table == table }
    }

    /// Check whether a role has a specific permission, optionally on a specific table.
    public func hasPermission(_ role: String, _ permission: Permission, on table: String? = nil) -> Bool {
        lock.lock()
        defer { lock.unlock() }

        // Superusers have all permissions
        if let r = roles[role], r.isSuperuser {
            return true
        }

        return grants.contains { grant in
            guard grant.role == role else { return false }

            // ALL permission matches everything
            if grant.permission == .all {
                // Global ALL or table-specific ALL
                if grant.table == nil || grant.table == table {
                    return true
                }
            }

            // Exact match
            if grant.permission == permission {
                // Global grant (nil table) covers all tables
                if grant.table == nil { return true }
                // Table-specific grant
                if grant.table == table { return true }
            }

            return false
        }
    }

    /// List all registered roles.
    public func listRoles() -> [Role] {
        lock.lock()
        defer { lock.unlock() }
        return Array(roles.values)
    }

    /// List all grants.
    public func listGrants() -> [RoleGrant] {
        lock.lock()
        defer { lock.unlock() }
        return grants
    }
}

/// Errors related to role management.
public enum RoleError: Error, Sendable {
    case roleAlreadyExists(String)
    case roleNotFound(String)
}
