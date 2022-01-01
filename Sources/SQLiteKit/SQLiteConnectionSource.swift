//import Logging

// jmj
public struct Logger {
    var label: String
    
    public init(label: String) {
        self.label = label
    }
    
    public func warning(_ msg: String) {
        print(msg)
    }
    
    public func debug(_ msg: String) {
        print(msg)
    }
    
    public func error(_ msg: String) {
        print(msg)
    }
    
}

public struct SQLiteConnectionSource {
    private let configuration: SQLiteConfiguration

    private var connectionStorage: SQLiteConnection.Storage {
        switch self.configuration.storage {
        case .memory(let identifier):
            return .file(
                path: "file:\(identifier)?mode=memory&cache=shared"
            )
        case .file(let path):
            return .file(path: path)
        }
    }
    
    public init(
        configuration: SQLiteConfiguration
    ) {
        self.configuration = configuration
    }

    public func makeConnection(
        logger: Logger
    ) throws -> SQLiteConnection {
        return try SQLiteConnection.open(
            storage: self.connectionStorage,
            logger: logger
        )
//            .flatMap { conn in
//            if self.configuration.enableForeignKeys {
//                return conn.query("PRAGMA foreign_keys = ON")
//                    .map { _ in conn }
//            } else {
//                return // eventLoop.makeSucceededFuture(conn)
//            }
//        }
    }
}

//extension SQLiteConnection: ConnectionPoolItem { }
