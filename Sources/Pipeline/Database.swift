//
// Copyright © 2021 Stephen F. Booth <me@sbooth.org>
// Part of https://github.com/sbooth/Pipeline
// MIT license
//

import Foundation

import Combine

/// An `sqlite3 *` object.
///
/// - seealso: [SQLite Database Connection Handle](https://sqlite.org/c3ref/sqlite3.html)
public typealias SQLiteDatabaseConnection = OpaquePointer

/// The content pointer is constant and will never change.
///
/// - seealso: [Constants Defining Special Destructor Behavior](https://sqlite.org/c3ref/c_static.html)
public let SQLiteStaticStorage = unsafeBitCast(0, to: sqlite3_destructor_type.self)

/// The content will likely change in the near future and that SQLite should make its own private copy of the content before returning.
///
/// - seealso: [Constants Defining Special Destructor Behavior](https://sqlite.org/c3ref/c_static.html)
public let SQLiteTransientStorage = unsafeBitCast(-1, to: sqlite3_destructor_type.self)

/// An [SQLite](https://sqlite.org) database.
public final class Database {
	/// The underlying `sqlite3 *` database
	let databaseConnection: SQLiteDatabaseConnection

	/// The database's custom busy handler
	var busyHandler: UnsafeMutablePointer<BusyHandler>?

	/// The subject sending events from `sqlite3_update_hook()`
	private lazy var tableChangeEventSubject: PassthroughSubject<TableChangeEvent, Never> = {
		let subject = PassthroughSubject<TableChangeEvent, Never>()
		let ptr = Unmanaged.passUnretained(subject).toOpaque()
		_ = sqlite3_update_hook(databaseConnection, { context, operation, database_name, table_name, rowid in
			let subject = Unmanaged<PassthroughSubject<TableChangeEvent, Never>>.fromOpaque(context.unsafelyUnwrapped).takeUnretainedValue()
			let changeType = RowChangeType(operation)
			let database = String(utf8String: database_name.unsafelyUnwrapped).unsafelyUnwrapped
			let table = String(utf8String: table_name.unsafelyUnwrapped).unsafelyUnwrapped
			let event = TableChangeEvent(changeType: changeType, database: database, table: table, rowid: rowid)
			subject.send(event)
		}, Unmanaged.passUnretained(subject).toOpaque())
		return subject
	}()

	/// Creates a database from an existing `sqlite3 *` database connection handle.
	///
	/// - attention: The database takes ownership of `databaseConnection`.  The result of further use of `databaseConnection` is undefined.
	///
	/// - parameter databaseConnection: An `sqlite3 *` database connection handle.
	public init(databaseConnection: SQLiteDatabaseConnection) {
		self.databaseConnection = databaseConnection
	}

	deinit {
		_ = sqlite3_close(databaseConnection)
//		_ = sqlite3_close_v2(databaseConnection)
		busyHandler?.deinitialize(count: 1)
		busyHandler?.deallocate()
	}

	/// Creates a temporary database.
	///
	/// - parameter inMemory: Whether the temporary database should be created in-memory or on-disk.
	///
	/// - throws: An error if the database could not be created.
	public convenience init(inMemory: Bool = true) throws {
		var db: SQLiteDatabaseConnection?
		let path = inMemory ? ":memory:" : ""
		let result = sqlite3_open_v2(path, &db, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, nil)
		guard result == SQLITE_OK else {
			_ = sqlite3_close(db)
			throw SQLiteError(code: result)
		}
		precondition(db != nil)
		self.init(databaseConnection: db.unsafelyUnwrapped)
	}

	/// Creates a read-only database from a file.
	///
	/// - parameter url: The location of the SQLite database.
	///
	/// - throws: An error if the database could not be created.
	public convenience init(readingFrom url: URL) throws {
		var db: SQLiteDatabaseConnection?
		try url.withUnsafeFileSystemRepresentation { path in
			let result = sqlite3_open_v2(path, &db, SQLITE_OPEN_READONLY, nil)
			guard result == SQLITE_OK else {
				_ = sqlite3_close(db)
				throw SQLiteError(code: result)
			}
		}
		precondition(db != nil)
		self.init(databaseConnection: db.unsafelyUnwrapped)
	}

	/// Creates a database from a file.
	///
	/// - parameter url: The location of the SQLite database.
	/// - parameter create: Whether to create the database if it doesn't exist.
	///
	/// - throws: An error if the database could not be created.
	public convenience init(url: URL, create: Bool = true) throws {
		var db: SQLiteDatabaseConnection?
		try url.withUnsafeFileSystemRepresentation { path in
			var flags = SQLITE_OPEN_READWRITE
			if create {
				flags |= SQLITE_OPEN_CREATE
			}
			let result = sqlite3_open_v2(path, &db, flags, nil)
			guard result == SQLITE_OK else {
				_ = sqlite3_close(db)
				throw SQLiteError(code: result)
			}
		}
		precondition(db != nil)
		self.init(databaseConnection: db.unsafelyUnwrapped)
	}
}

extension Database {
	/// `true` if this database is read only, `false` otherwise.
	public var isReadOnly: Bool {
		sqlite3_db_readonly(self.databaseConnection, nil) == 1
	}

	/// The rowid of the most recent successful `INSERT` into a rowid table or virtual table.
	public var lastInsertRowid: Int64 {
		get {
			sqlite3_last_insert_rowid(databaseConnection)
		}
		set {
			sqlite3_set_last_insert_rowid(databaseConnection, newValue)
		}
	}

	/// The number of rows modified, inserted, or deleted by the most recently completed `INSERT`, `UPDATE` or `DELETE` statement.
	public var changes: Int {
		// TODO: Update to sqlite3_changes64
		Int(sqlite3_changes(databaseConnection))
	}

	/// The total number of rows inserted, modified, or deleted by all `INSERT`, `UPDATE` or `DELETE` statements.
	public var totalChanges: Int {
		// TODO: Update to sqlite3_total_changes64
		Int(sqlite3_total_changes(databaseConnection))
	}

	/// Interrupts a long-running query.
	public func interrupt() {
		sqlite3_interrupt(databaseConnection)
	}

	/// Returns the location of the file associated with database `name`.
	///
	/// - note: The main database file has the name *main*
	///
	/// - parameter name: The name of the attached database whose location is desired
	///
	/// - throws: An error if there is no attached database with the specified name, or if `name` is a temporary or in-memory database
	///
	/// - returns: The URL for the file associated with database `name`
	public func url(forDatabase name: String = "main") throws -> URL {
		guard let path = sqlite3_db_filename(databaseConnection, name) else {
			throw Database.Error(message: "The database \"\(name)\" does not exist or is a temporary or in-memory database")
		}
		return URL(fileURLWithPath: String(cString: path))
	}

	/// Performs a low-level SQLite database operation.
	///
	/// **Use of this function should be avoided whenever possible**
	///
	/// - parameter block: A closure performing the database operation.
	/// - parameter databaseConnection: The raw `sqlite3 *` database connection handle.
	///
	/// - throws: Any error thrown in `block`.
	///
	/// - returns: The value returned by `block`.
	public func withUnsafeRawSQLiteDatabaseConnection<T>(block: (_ databaseConnection: SQLiteDatabaseConnection) throws -> (T)) rethrows -> T {
		try block(databaseConnection)
	}
}

extension Database {
	/// Executes an SQL statement.
	///
	/// - parameter sql: The SQL statement to execute
	///
	/// - throws: An error if `sql` could not be compiled or executed.
	public func execute(sql: String) throws {
		let result = sqlite3_exec(databaseConnection, sql, nil, nil, nil)
		guard result == SQLITE_OK else {
			throw SQLiteError(fromDatabaseConnection: databaseConnection)
		}
	}

	/// Compiles and returns an SQL statement.
	///
	/// - parameter sql: The SQL statement to compile.
	///
	/// - throws: An error if `sql` could not be compiled.
	///
	/// - returns: A compiled SQL statement.
	public func prepare(sql: String) throws -> Statement {
		try Statement(database: self, sql: sql)
	}
}

extension Database {
	/// Returns the result or error code associated with the most recent `sqlite3_` API call
	public var errorCode: Int32 {
		sqlite3_errcode(databaseConnection)
	}

	/// Returns the result or extended error code associated with the most recent `sqlite3_` API call
	public var extendedErrorCode: Int32 {
		sqlite3_extended_errcode(databaseConnection)
	}

	/// Returns the result or error message associated with the most recent `sqlite3_` API call
	public var errorMessage: String {
		String(cString: sqlite3_errmsg(databaseConnection))
	}
}

extension Database {
	/// Returns `true` if the last `sqlite3_` API call succeeded
	public var success: Bool {
		let result = errorCode
		return result == SQLITE_OK || result == SQLITE_ROW || result == SQLITE_DONE
	}
}

extension Database {
	/// Possible types of row changes.
	public enum	RowChangeType {
		/// A row was inserted.
		case insert
		/// A row was deleted.
		case delete
		/// A row was updated.
		case update
	}

	/// An insert, delete, or update event on a rowid table.
	public struct TableChangeEvent {
		/// The type of row change.
		public let changeType: RowChangeType
		/// The name of the database containing the table that changed.
		public let database: String
		/// The name of the table that changed.
		public let table: String
		/// The rowid of the row that changed.
		public let rowid: Int64
	}

	/// Returns a publisher for changes to rowid tables.
	///
	/// - returns: A publisher for changes to the database's rowid tables.
	public var tableChangeEventPublisher: AnyPublisher<TableChangeEvent, Never> {
		tableChangeEventSubject
			.eraseToAnyPublisher()
	}
}

extension Database.RowChangeType {
	/// Convenience initializer for conversion of `SQLITE_` values.
	///
	/// - parameter operation: The second argument to the callback function passed to `sqlite3_update_hook()`.
	init(_ operation: Int32) {
		switch operation {
		case SQLITE_INSERT:
			self = .insert
		case SQLITE_DELETE:
			self = .delete
		case SQLITE_UPDATE:
			self = .update
		default:
			fatalError("Unexpected SQLite row change type \(operation)")
		}
	}
}
