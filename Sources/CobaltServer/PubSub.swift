import Foundation

/// Actor-based publish/subscribe system for LISTEN/NOTIFY support.
public actor PubSub {
    /// channel -> [subscriberID -> callback(channel, payload)]
    private var subscriptions: [String: [String: @Sendable (String, String) -> Void]] = [:]

    public init() {}

    /// Subscribe to a channel. The callback receives (channel, payload) on notify.
    public func listen(channel: String, subscriberID: String, callback: @escaping @Sendable (String, String) -> Void) {
        if subscriptions[channel] == nil {
            subscriptions[channel] = [:]
        }
        subscriptions[channel]?[subscriberID] = callback
    }

    /// Unsubscribe from a channel.
    public func unlisten(channel: String, subscriberID: String) {
        subscriptions[channel]?.removeValue(forKey: subscriberID)
        if subscriptions[channel]?.isEmpty == true {
            subscriptions.removeValue(forKey: channel)
        }
    }

    /// Send a notification to all subscribers on a channel.
    public func notify(channel: String, payload: String) async {
        guard let subs = subscriptions[channel] else { return }
        for (_, callback) in subs {
            callback(channel, payload)
        }
    }

    /// Returns the number of subscribers on a channel.
    public func subscriberCount(channel: String) -> Int {
        return subscriptions[channel]?.count ?? 0
    }

    /// Returns all channels that have at least one subscriber.
    public func activeChannels() -> [String] {
        return subscriptions.keys.filter { subscriptions[$0]?.isEmpty == false }.sorted()
    }
}
