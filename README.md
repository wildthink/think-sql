# ThinkSQL

[![Swift](https://img.shields.io/badge/swift-5.2-green.svg?longCache=true&style=flat)](https://developer.apple.com/swift/)
[![License](https://img.shields.io/badge/license-MIT-green.svg?longCache=true&style=flat)](/LICENSE)
![Build](https://github.com/shareup/sqlite/workflows/Build/badge.svg)

## Introduction

`ThinkSQL` is an amalgam of several of my favorite Swift SQLite wrapper frameworks.

## Installation

### Swift Package Manager

To use SQLite with the Swift Package Manager, add a dependency to your Package.swift file:

```swift
let package = Package(
  dependencies: [
    .package(url: "https://github.com/wildthink/think-sql.git", .upToNextMajor(from: "1.0.0"))
  ]
)
```

### Features

- Supports Combine Publishing and SwiftUI integration
- Supports Codable types "out-of-the-box"
- Easy to pick between system and custom Sqlite3 (sqlite.c) library
- Support multiple embedded database options
- Schema enabled
  - Extend Coders with DBValue <-> Type transformers
  - Entity-Component-System support
- 
