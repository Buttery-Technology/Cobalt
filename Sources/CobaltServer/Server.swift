// Server.swift — TCP listener for the PostgreSQL wire protocol
import NIOCore
import NIOPosix
import Cobalt

public actor CobaltServer {
    private let database: CobaltDatabase
    private let host: String
    private let port: Int
    private var channel: Channel?
    private var group: MultiThreadedEventLoopGroup?

    public init(database: CobaltDatabase, host: String = "127.0.0.1", port: Int = 5433) {
        self.database = database
        self.host = host
        self.port = port
    }

    public func start() async throws {
        let group = MultiThreadedEventLoopGroup(numberOfThreads: System.coreCount)
        self.group = group

        let db = self.database
        let bootstrap = ServerBootstrap(group: group)
            .serverChannelOption(.backlog, value: 256)
            .serverChannelOption(.socketOption(.so_reuseaddr), value: 1)
            .childChannelInitializer { channel in
                channel.pipeline.addHandler(ConnectionHandler(database: db))
            }
            .childChannelOption(.socketOption(.so_reuseaddr), value: 1)
            .childChannelOption(.maxMessagesPerRead, value: 16)

        let channel = try await bootstrap.bind(host: host, port: port).get()
        self.channel = channel
    }

    public func stop() async throws {
        try await channel?.close()
        try await group?.shutdownGracefully()
        self.channel = nil
        self.group = nil
    }
}
