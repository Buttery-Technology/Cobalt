// swift-tools-version: 6.2
import PackageDescription

let package = Package(
    name: "Cobalt",
    platforms: [.macOS(.v15)],
    products: [
        .library(name: "CobaltCore", targets: ["CobaltCore"]),
        .library(name: "CobaltIndex", targets: ["CobaltIndex"]),
        .library(name: "CobaltQuery", targets: ["CobaltQuery"]),
        .library(name: "CobaltSQL", targets: ["CobaltSQL"]),
        .library(name: "Cobalt", targets: ["Cobalt"]),
        .library(name: "CobaltServer", targets: ["CobaltServer"]),
    ],
    dependencies: [
        .package(url: "https://github.com/apple/swift-crypto.git", .upToNextMajor(from: "3.14.0")),
        .package(url: "https://github.com/apple/swift-nio.git", from: "2.0.0"),
    ],
    targets: [
        .target(name: "CobaltCore", dependencies: [.product(name: "Crypto", package: "swift-crypto")]),
        .target(name: "CobaltIndex", dependencies: ["CobaltCore"]),
        .target(name: "CobaltQuery", dependencies: ["CobaltCore", "CobaltIndex"]),
        .target(name: "CobaltSQL", dependencies: ["CobaltCore"]),
        .target(name: "Cobalt", dependencies: ["CobaltCore", "CobaltIndex", "CobaltQuery", "CobaltSQL"]),
        .target(name: "CobaltServer", dependencies: [
            "Cobalt", "CobaltSQL", "CobaltQuery",
            .product(name: "NIO", package: "swift-nio"),
            .product(name: "NIOCore", package: "swift-nio"),
            .product(name: "NIOPosix", package: "swift-nio"),
        ]),
        .testTarget(name: "CobaltServerTests", dependencies: ["CobaltServer"]),
        .testTarget(name: "CobaltCoreTests", dependencies: ["CobaltCore"]),
        .testTarget(name: "CobaltIndexTests", dependencies: ["CobaltIndex"]),
        .testTarget(name: "CobaltQueryTests", dependencies: ["CobaltQuery"]),
        .testTarget(name: "CobaltSQLTests", dependencies: ["CobaltSQL", "CobaltQuery", "Cobalt"]),
        .testTarget(name: "CobaltTests", dependencies: ["Cobalt"]),
        .systemLibrary(name: "CSQLite"),
        .executableTarget(
            name: "CobaltBenchmark",
            dependencies: ["Cobalt", "CSQLite"],
            linkerSettings: [
                .unsafeFlags(["-L/opt/homebrew/opt/sqlite/lib"]),
            ]
        ),
    ]
)
