// jmj - replace with SQLiteConnection extension below
/*
extension SQLiteDatabase {
    public func sql() -> SQLDatabase {
        _SQLiteSQLDatabase(database: self)
    }
}

private struct _SQLiteSQLDatabase: SQLDatabase {
    
    let database: SQLiteDatabase
        
    var logger: Logger {
        return self.database.logger
    }
    
    var dialect: SQLDialect {
        SQLiteDialect()
    }
    
    func execute(
        sql query: SQLExpression,
        _ onRow: @escaping (SQLRow) -> ()
    ) {
        var serializer = SQLSerializer(database: self)
        query.serialize(to: &serializer)
        let binds: [SQLiteData]
        do {
            binds = try serializer.binds.map { encodable in
                return try SQLiteDataEncoder().encode(encodable)
            }
        } catch {
            return // self.eventLoop.makeFailedFuture(error)
        }
        try? self.database.query(
            serializer.sql,
            binds,
            logger: self.logger
        ) { row in
            onRow(row)
        }
    }
}
*/

// jmj
/*
extension SQLiteConnection: SQLDatabase {
        
    public var dialect: SQLDialect {
        SQLiteDialect()
    }
    
    public func execute(
        sql query: SQLExpression,
        _ onRow: @escaping (SQLRow) -> ()
    ) {
        var serializer = SQLSerializer(database: self)
        query.serialize(to: &serializer)
        let binds: [SQLiteData]
        do {
            binds = try serializer.binds.map { encodable in
                return try SQLiteDataEncoder().encode(encodable)
            }
        } catch {
            return // self.eventLoop.makeFailedFuture(error)
        }
        try? self.query(
            serializer.sql,
            binds,
            logger: self.logger
        ) { row in
            onRow(row)
        }
    }
}
*/

