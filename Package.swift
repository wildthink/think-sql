// swift-tools-version:5.5
// The swift-tools-version declares the minimum version of Swift required to build this package.

import PackageDescription

let package = Package(
    name: "think-sql",
    platforms: [
        .macOS(.v10_15), .iOS(.v13), .tvOS(.v13), .watchOS(.v5),
    ],
    products: [
        // Products define the executables and libraries a package produces, and make them visible to other packages.
        .library(
            name: "ThinkSQL",
            targets: ["ThinkSQL"]),
        .library(
            name: "Pipeline",
            targets: ["Pipeline"]),
        .library(
            name: "SQLite",
            targets: ["SQLite"]),
        .library(
            name: "SQLiteKit",
            targets: ["SQLiteKit"]),
    ],
    dependencies: [
//        .package(
//            name: "CombineExtensions",
//            url: "https://github.com/shareup/combine-extensions.git",
//            from: "4.0.0"
//        ),
        .package(
            url: "https://github.com/Flight-School/AnyCodable",
            from: "0.6.0"
        ),
        .package(
            url: "https://github.com/karwa/uniqueid",
            .upToNextMajor(from: "1.0.0")
        ),
        .package(
            name: "Synchronized",
            url: "https://github.com/shareup/synchronized.git",
            from: "3.0.0"
        ),
    ],
    targets: [
        // Targets are the basic building blocks of a package. A target can define a module or a test suite.
        // Targets can depend on other targets in this package, and on products in packages this package depends on.
        .target(
            name: "ThinkSQL",
            dependencies: [
                "SQLite", "Pipeline", "AnyCodable",
                "SQLiteKit",
                .product(name: "UniqueID", package: "uniqueid")
        ]),
        .target(
            name: "Pipeline",
            dependencies: []),
        .target(
            name: "SQLiteKit",
            dependencies: []),
        .target(
            name: "SQLite",
            dependencies: ["Synchronized"]),
        .testTarget(
            name: "ThinkSQLTests",
            dependencies: ["ThinkSQL"]),
    ]
)
