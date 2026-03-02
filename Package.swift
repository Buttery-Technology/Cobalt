// swift-tools-version: 6.2
import PackageDescription

let package = Package(
    name: "Pantry",
    platforms: [.macOS(.v15)],
    products: [
        .library(name: "PantryCore", targets: ["PantryCore"]),
        .library(name: "PantryIndex", targets: ["PantryIndex"]),
        .library(name: "PantryQuery", targets: ["PantryQuery"]),
        .library(name: "Pantry", targets: ["Pantry"]),
    ],
    dependencies: [
        .package(url: "https://github.com/apple/swift-crypto.git", .upToNextMajor(from: "3.14.0")),
    ],
    targets: [
        .target(name: "PantryCore", dependencies: [.product(name: "Crypto", package: "swift-crypto")]),
        .target(name: "PantryIndex", dependencies: ["PantryCore"]),
        .target(name: "PantryQuery", dependencies: ["PantryCore", "PantryIndex"]),
        .target(name: "Pantry", dependencies: ["PantryCore", "PantryIndex", "PantryQuery"]),
        .testTarget(name: "PantryCoreTests", dependencies: ["PantryCore"]),
        .testTarget(name: "PantryIndexTests", dependencies: ["PantryIndex"]),
        .testTarget(name: "PantryQueryTests", dependencies: ["PantryQuery"]),
        .testTarget(name: "PantryTests", dependencies: ["Pantry"]),
        .systemLibrary(name: "CSQLite"),
        .executableTarget(
            name: "PantryBenchmark",
            dependencies: ["Pantry", "CSQLite"],
            linkerSettings: [
                .unsafeFlags(["-L/opt/homebrew/opt/sqlite/lib"]),
            ]
        ),
    ]
)
