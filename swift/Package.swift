// swift-tools-version: 5.9
import PackageDescription

let package = Package(
    name: "FarmBucketDemo-SPM",
    platforms: [
        .macOS(.v13), .iOS(.v15)
    ],
    products: [
        .library(name: "FarmHashKit", targets: ["FarmHashKit"]),
        .executable(name: "FarmBucketDemo", targets: ["FarmBucketDemo"]),
        .executable(name: "BucketAccuracy", targets: ["BucketAccuracy"])
    ],
    targets: [
        .target(
            name: "FarmHashKit",
            publicHeadersPath: "include",
            cxxSettings: [
                .unsafeFlags(["-std=c++17"])
            ]
        ),
        .executableTarget(
            name: "FarmBucketDemo",
            dependencies: ["FarmHashKit"]
        ),
        .executableTarget(
            name: "BucketAccuracy",
            dependencies: ["FarmHashKit"],
            path: "Sources/BucketAccuracy"
        )
    ]
)
