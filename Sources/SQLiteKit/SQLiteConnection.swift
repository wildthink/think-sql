//import SQLite

public protocol SQLiteDatabase {
    var logger: Logger { get }
    
    func query(
        _ query: String,
        _ binds: [SQLiteData],
        logger: Logger,
        _ onRow: @escaping (SQLiteRow) -> Void
    ) throws
    
    func withConnection<T>(_: @escaping (SQLiteConnection) -> T)
}

extension SQLiteDatabase {
    public func query(
        _ query: String,
        _ binds: [SQLiteData] = [],
        _ onRow: @escaping (SQLiteRow) -> Void
    ) throws {
        try self.query(query, binds, logger: self.logger, onRow)
    }
    
    public func query(
        _ query: String,
        _ binds: [SQLiteData] = []
    ) throws -> [SQLiteRow] {
        var rows: [SQLiteRow] = []
        try self.query(query, binds, logger: self.logger) { row in
            rows.append(row)
        }
        return rows
        // .map { rows }
    }
}

extension SQLiteDatabase {
    public func logging(to logger: Logger) -> SQLiteDatabase {
        _SQLiteDatabaseCustomLogger(database: self, logger: logger)
    }
}

private struct _SQLiteDatabaseCustomLogger: SQLiteDatabase {
    let database: SQLiteDatabase
    let logger: Logger
    
    func withConnection<T>(_ closure: @escaping (SQLiteConnection) -> T) {
        self.database.withConnection(closure)
    }
    
    func query(
        _ query: String,
        _ binds: [SQLiteData],
        logger: Logger,
        _ onRow: @escaping (SQLiteRow) -> Void
    ) throws {
        try self.database.query(query, binds, logger: logger, onRow)
    }
}

public final class SQLiteConnection: SQLiteDatabase {
    
    /// Available SQLite storage methods.
    public enum Storage {
        /// In-memory storage. Not persisted between application launches.
        /// Good for unit testing or caching.
        case memory

        /// File-based storage, persisted between application launches.
        case file(path: String)
    }
    
    internal var handle: OpaquePointer?
    public let logger: Logger
    
    public var isClosed: Bool {
        return self.handle == nil
    }

    public static func open(
        storage: Storage = .memory,
        logger: Logger = .init(label: "codes.vapor.sqlite")
    ) throws -> SQLiteConnection {
        let path: String
        switch storage {
        case .memory:
            path = ":memory:"
        case .file(let file):
            path = file
        }

//        let promise = eventLoop.makePromise(of: SQLiteConnection.self)
//        threadPool.submit { state in
            var handle: OpaquePointer?
            let options = SQLITE_OPEN_CREATE | SQLITE_OPEN_READWRITE | SQLITE_OPEN_FULLMUTEX | SQLITE_OPEN_URI
            if sqlite3_open_v2(path, &handle, options, nil) == SQLITE_OK, sqlite3_busy_handler(handle, { _, _ in 1 }, nil) == SQLITE_OK {
                let connection = SQLiteConnection(
                    handle: handle,
                    logger: logger
                )
                logger.debug("Connected to sqlite db: \(path)")
                return connection
//                promise.succeed(connection)
            } else {
                logger.error("Failed to connect to sqlite db: \(path)")
                throw SQLiteError(reason: .cantOpen, message: "Cannot open SQLite database: \(storage)")
//                promise.fail(SQLiteError(reason: .cantOpen, message: "Cannot open SQLite database: \(storage)"))
            }
        }
//        return promise.futureResult
//    }

    init(
        handle: OpaquePointer?,
        logger: Logger
    ) {
        self.handle = handle
        self.logger = logger
    }

    public func lastAutoincrementID() -> Int {
        Int(sqlite3_last_insert_rowid(self.handle))
        //            promise.succeed(numericCast(rowid))
//        let promise = self.eventLoop.makePromise(of: Int.self)
//        self.threadPool.submit { _ in
//            let rowid = sqlite3_last_insert_rowid(self.handle)
//            promise.succeed(numericCast(rowid))
//        }
//        return promise.futureResult
    }

    internal var errorMessage: String? {
        if let raw = sqlite3_errmsg(self.handle) {
            return String(cString: raw)
        } else {
            return nil
        }
    }
    
    public func withConnection<T>(_ closure: @escaping (SQLiteConnection) -> T) {
        _ = closure(self)
    }
    
//    public func query(_ query: String, _ binds: [SQLiteData], logger: Logger, _ onRow: @escaping (SQLiteRow) -> Void) {
//        <#code#>
//    }

    public func query(
        _ query: String,
        _ binds: [SQLiteData],
        logger: Logger,
        _ onRow: @escaping (SQLiteRow) -> Void
    ) throws {
        logger.debug("\(query) \(binds)")
//        let promise = self.eventLoop.makePromise(of: Void.self)
//        self.threadPool.submit { state in
//            do {
                let statement = try SQLiteStatement(query: query, on: self)
                try statement.bind(binds)
                let columns = try statement.columns()
//                var callbacks: [EventLoopFuture<Void>] = []
                while let row = try statement.nextRow(for: columns) {
//                    let callback = self.eventLoop.submit {
                        onRow(row)
//                    }
//                    callbacks.append(callback)
                }
//                EventLoopFuture<Void>.andAllSucceed(callbacks, on: self.eventLoop)
//                    .cascade(to: promise)
//            } catch {
//                promise.fail(error)
//            }
        }
//        return promise.futureResult
//    }

    public func close() {
//        let promise = self.eventLoop.makePromise(of: Void.self)
//        self.threadPool.submit { state in
            sqlite3_close(self.handle)
//            self.eventLoop.submit {
                self.handle = nil
//            }.cascade(to: promise)
//        }
//        return promise.futureResult
    }

    deinit {
        assert(self.handle == nil, "SQLiteConnection was not closed before deinitializing")
    }
}
