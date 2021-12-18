//
// Copyright © 2021 Stephen F. Booth <me@sbooth.org>
// Part of https://github.com/sbooth/Pipeline
// MIT license
//

import Foundation


extension Database {
	/// A native data type that may be stored in an SQLite database.
	///
	/// - seealso: [Datatypes In SQLite Version 3](https://sqlite.org/datatype3.html)
	public enum ColumnType {
		/// An integer value.
		case integer
		/// A floating-point value.
		case float
		/// A text value.
		case text
		/// A blob (untyped bytes) value.
		case blob
		/// A null value.
		case null
	}
}

extension Database {
	/// A result row containing one or more columns.
	public struct Row {
		/// The owning statement
		public let statement: Statement
	}
}

extension Database.Row {
	/// The number of columns in the result row.
	///
	/// - seealso: [Number of columns in a result set](https://sqlite.org/c3ref/data_count.html)
	public var columnCount: Int {
		Int(sqlite3_data_count(statement.preparedStatement))
	}

	/// The names of the columns.
	///
	/// - note: Column names are not guaranteed to be unique.
	public var columnNames: [String] {
		statement.columnNames
	}

	/// Returns the name of the column at `index`.
	///
	/// - note: Column indexes are 0-based.  The leftmost column in a result row has index 0.
	///
	/// - precondition: `index >= 0`
	/// - requires: `index < self.columnCount`
	///
	/// - parameter index: The index of the desired column.
	///
	/// - throws: An error if `index` is out of bounds
	///
	/// - returns: The name of the column.
	public func nameOfColumn(_ index: Int) throws -> String {
		return try statement.name(ofColumn: index)
	}

	/// Returns the initial data type of the column at `index`.
	///
	/// - note: Column indexes are 0-based.  The leftmost column in a result row has index 0.
	///
	/// - precondition: `index >= 0`
	/// - requires: `index < self.columnCount`
	///
	/// - parameter index: The index of the desired column.
	///
	/// - throws: An error if `index` is out of bounds
	///
	/// - returns: The data type of the column.
	///
	/// - seealso: [Result values from a query](https://sqlite.org/c3ref/column_blob.html)
	public func typeofColumn(_ index: Int) throws -> Database.ColumnType {
		precondition(index >= 0)
		let type = sqlite3_column_type(statement.preparedStatement, Int32(index))
		guard statement.database.success else {
			throw SQLiteError(fromDatabaseConnection: statement.database.databaseConnection)
		}
		switch type {
		case SQLITE_INTEGER:
			return .integer
		case SQLITE_FLOAT:
			return .float
		case SQLITE_TEXT:
			return .text
		case SQLITE_BLOB:
			return .blob
		case SQLITE_NULL:
			return .null
		default:
			fatalError("Unknown SQLite column type \(type) encountered for column \(index)")
		}
	}
}

extension Database.Row {
	/// Returns the value of the column at `index`.
	///
	/// - note: Column indexes are 0-based.  The leftmost column in a row has index 0.
	/// - note: Automatic type conversion may be performed by SQLite depending on the column's initial data type.
	///
	/// - precondition: `index >= 0`
	/// - requires: `index < self.columnCount`
	///
	/// - parameter index: The index of the desired column.
	///
	/// - throws: An error if `index` is out of bounds.
	///
	/// - returns: The column's value.
	///
	/// - seealso: [Result values from a query](https://sqlite.org/c3ref/column_blob.html)
	public func int64(forColumn index: Int) throws -> Int64 {
		precondition(index >= 0)
		let i = sqlite3_column_int64(statement.preparedStatement, Int32(index))
		guard statement.database.success else {
			throw SQLiteError(fromDatabaseConnection: statement.database.databaseConnection)
		}
		return i
	}

	/// Returns the value of the column at `index`.
	///
	/// - note: Column indexes are 0-based.  The leftmost column in a row has index 0.
	/// - note: Automatic type conversion may be performed by SQLite depending on the column's initial data type.
	///
	/// - precondition: `index >= 0`
	/// - requires: `index < self.columnCount`
	///
	/// - parameter index: The index of the desired column.
	///
	/// - throws: An error if `index` is out of bounds.
	///
	/// - returns: The column's value.
	///
	/// - seealso: [Result values from a query](https://sqlite.org/c3ref/column_blob.html)
	public func double(forColumn index: Int) throws -> Double {
		precondition(index >= 0)
		let f = sqlite3_column_double(statement.preparedStatement, Int32(index))
		guard statement.database.success else {
			throw SQLiteError(fromDatabaseConnection: statement.database.databaseConnection)
		}
		return f
	}

	/// Returns the value of the column at `index`.
	///
	/// - note: Column indexes are 0-based.  The leftmost column in a row has index 0.
	/// - note: Automatic type conversion may be performed by SQLite depending on the column's initial data type.
	///
	/// - precondition: `index >= 0`
	/// - requires: `index < self.columnCount`
	///
	/// - parameter index: The index of the desired column.
	///
	/// - throws: An error if `index` is out of bounds.
	///
	/// - returns: The column's value.
	///
	/// - seealso: [Result values from a query](https://sqlite.org/c3ref/column_blob.html)
	public func string(forColumn index: Int) throws -> String {
		precondition(index >= 0)
		let t = String(cString: sqlite3_column_text(statement.preparedStatement, Int32(index)))
		guard statement.database.success else {
			throw SQLiteError(fromDatabaseConnection: statement.database.databaseConnection)
		}
		return t
	}

	/// Returns the value of the column at `index`.
	///
	/// - note: Column indexes are 0-based.  The leftmost column in a row has index 0.
	/// - note: Automatic type conversion may be performed by SQLite depending on the column's initial data type.
	///
	/// - precondition: `index >= 0`
	/// - requires: `index < self.columnCount`
	///
	/// - parameter index: The index of the desired column.
	///
	/// - throws: An error if `index` is out of bounds.
	///
	/// - returns: The column's value.
	///
	/// - seealso: [Result values from a query](https://sqlite.org/c3ref/column_blob.html)
	public func data(forColumn index: Int) throws -> Data {
		precondition(index >= 0)
		let byteCount: Int = Int(sqlite3_column_bytes(statement.preparedStatement, Int32(index)))
		guard statement.database.success else {
			throw SQLiteError(fromDatabaseConnection: statement.database.databaseConnection)
		}
		return Data(bytes: sqlite3_column_blob(statement.preparedStatement, Int32(index)).assumingMemoryBound(to: UInt8.self), count: byteCount)
	}
}

extension Database.Row {
	/// Returns the value of the column at `index`.
	///
	/// - note: Column indexes are 0-based.  The leftmost column in a row has index 0.
	/// - note: Automatic type conversion may be performed by SQLite depending on the column's initial data type.
	///
	/// - precondition: `index >= 0`
	/// - requires: `index < self.columnCount`
	///
	/// - parameter index: The index of the desired column.
	///
	/// - throws: An error if `index` is out of bounds.
	///
	/// - returns: The column's value or `nil` if null.
	///
	/// - seealso: [Result values from a query](https://sqlite.org/c3ref/column_blob.html)
	public func int64OrNil(forColumn index: Int) throws -> Int64? {
		if try typeofColumn(index) == .null {
			return nil
		}
		return try int64(forColumn: index)
	}

	/// Returns the value of the column at `index`.
	///
	/// - note: Column indexes are 0-based.  The leftmost column in a row has index 0.
	/// - note: Automatic type conversion may be performed by SQLite depending on the column's initial data type.
	///
	/// - precondition: `index >= 0`
	/// - requires: `index < self.columnCount`
	///
	/// - parameter index: The index of the desired column.
	///
	/// - throws: An error if `index` is out of bounds.
	///
	/// - returns: The column's value or `nil` if null.
	///
	/// - seealso: [Result values from a query](https://sqlite.org/c3ref/column_blob.html)
	public func doubleOrNil(forColumn index: Int) throws -> Double? {
		if try typeofColumn(index) == .null {
			return nil
		}
		return try double(forColumn: index)
	}

	/// Returns the value of the column at `index`.
	///
	/// - note: Column indexes are 0-based.  The leftmost column in a row has index 0.
	/// - note: Automatic type conversion may be performed by SQLite depending on the column's initial data type.
	///
	/// - precondition: `index >= 0`
	/// - requires: `index < self.columnCount`
	///
	/// - parameter index: The index of the desired column.
	///
	/// - throws: An error if `index` is out of bounds.
	///
	/// - returns: The column's value or `nil` if null.
	///
	/// - seealso: [Result values from a query](https://sqlite.org/c3ref/column_blob.html)
	public func stringOrNil(forColumn index: Int) throws -> String? {
		if try typeofColumn(index) == .null {
			return nil
		}
		return try string(forColumn: index)
	}

	/// Returns the value of the column at `index`.
	///
	/// - note: Column indexes are 0-based.  The leftmost column in a row has index 0.
	/// - note: Automatic type conversion may be performed by SQLite depending on the column's initial data type.
	///
	/// - precondition: `index >= 0`
	/// - requires: `index < self.columnCount`
	///
	/// - parameter index: The index of the desired column.
	///
	/// - throws: An error if `index` is out of bounds.
	///
	/// - returns: The column's value or `nil` if null.
	///
	/// - seealso: [Result values from a query](https://sqlite.org/c3ref/column_blob.html)
	public func dataOrNil(forColumn index: Int) throws -> Data? {
		if try typeofColumn(index) == .null {
			return nil
		}
		return try data(forColumn: index)
	}
}

extension Database.Row {
	/// Returns the value of the column at `index`.
	///
	/// - note: Column indexes are 0-based.  The leftmost column in a row has index 0.
	/// - note: Automatic type conversion may be performed by SQLite depending on the column's initial data type.
	///
	/// - precondition: `index >= 0`
	/// - requires: `index < self.columnCount`
	///
	/// - parameter index: The index of the desired column.
	///
	/// - throws: An error if `index` is out of bounds.
	///
	/// - returns: The column's value.
	///
	/// - seealso: [Result values from a query](https://sqlite.org/c3ref/column_blob.html)
	public func int(forColumn index: Int) throws -> Int {
		return Int(try int64(forColumn: index))
	}

	/// Returns the value of the column at `index`.
	///
	/// - note: Column indexes are 0-based.  The leftmost column in a row has index 0.
	/// - note: Automatic type conversion may be performed by SQLite depending on the column's initial data type.
	///
	/// - precondition: `index >= 0`
	/// - requires: `index < self.columnCount`
	///
	/// - parameter index: The index of the desired column.
	///
	/// - throws: An error if `index` is out of bounds.
	///
	/// - returns: The column's value.
	///
	/// - seealso: [Result values from a query](https://sqlite.org/c3ref/column_blob.html)
	public func uint(forColumn index: Int) throws -> UInt {
		return UInt(try int64(forColumn: index))
	}

	/// Returns the value of the column at `index`.
	///
	/// - note: Column indexes are 0-based.  The leftmost column in a row has index 0.
	/// - note: Automatic type conversion may be performed by SQLite depending on the column's initial data type.
	///
	/// - precondition: `index >= 0`
	/// - requires: `index < self.columnCount`
	///
	/// - parameter index: The index of the desired column.
	///
	/// - throws: An error if `index` is out of bounds.
	///
	/// - returns: The column's value.
	///
	/// - seealso: [Result values from a query](https://sqlite.org/c3ref/column_blob.html)
	public func float(forColumn index: Int) throws -> Float {
		return Float(try double(forColumn: index))
	}

	/// Returns the value of the column at `index`.
	///
	/// - note: Column indexes are 0-based.  The leftmost column in a row has index 0.
	/// - note: Automatic type conversion may be performed by SQLite depending on the column's initial data type.
	///
	/// - precondition: `index >= 0`
	/// - requires: `index < self.columnCount`
	///
	/// - parameter index: The index of the desired column.
	///
	/// - throws: An error if `index` is out of bounds.
	///
	/// - returns: `true` if the column's value is not 0.
	///
	/// - seealso: [Result values from a query](https://sqlite.org/c3ref/column_blob.html)
	public func bool(forColumn index: Int) throws -> Bool {
		return try int64(forColumn: index) != 0
	}
}

extension Database.Row {
	/// Returns the value of the column at `index`.
	///
	/// - note: Column indexes are 0-based.  The leftmost column in a row has index 0.
	/// - note: Automatic type conversion may be performed by SQLite depending on the column's initial data type.
	///
	/// - precondition: `index >= 0`
	/// - requires: `index < self.columnCount`
	///
	/// - parameter index: The index of the desired column.
	///
	/// - throws: An error if `index` is out of bounds.
	///
	/// - returns: The column's value.
	public func uuid(forColumn index: Int) throws -> UUID {
		let s = try string(forColumn: index)
		guard let uuid = UUID(uuidString: s) else {
			throw Database.Error(message: "\"\(s)\" is not a valid UUID")
		}
		return uuid
	}

	/// Returns the value of the column at `index`.
	///
	/// - note: Column indexes are 0-based.  The leftmost column in a row has index 0.
	/// - note: Automatic type conversion may be performed by SQLite depending on the column's initial data type.
	///
	/// - precondition: `index >= 0`
	/// - requires: `index < self.columnCount`
	///
	/// - parameter index: The index of the desired column.
	///
	/// - throws: An error if `index` is out of bounds.
	///
	/// - returns: The column's value or `nil` if null.
	public func uuidOrNil(forColumn index: Int) throws -> UUID? {
		if try typeofColumn(index) == .null {
			return nil
		}
		return try uuid(forColumn: index)
	}

	/// Returns the value of the column at `index`.
	///
	/// - note: Column indexes are 0-based.  The leftmost column in a row has index 0.
	/// - note: Automatic type conversion may be performed by SQLite depending on the column's initial data type.
	///
	/// - precondition: `index >= 0`
	/// - requires: `index < self.columnCount`
	///
	/// - parameter index: The index of the desired column.
	///
	/// - throws: An error if `index` is out of bounds.
	///
	/// - returns: The column's value.
	public func url(forColumn index: Int) throws -> URL {
		let s = try string(forColumn: index)
		guard let url = URL(string: s) else {
			throw Database.Error(message: "\"\(s)\" is not a valid URL")
		}
		return url
	}

	/// Returns the value of the column at `index`.
	///
	/// - note: Column indexes are 0-based.  The leftmost column in a row has index 0.
	/// - note: Automatic type conversion may be performed by SQLite depending on the column's initial data type.
	///
	/// - precondition: `index >= 0`
	/// - requires: `index < self.columnCount`
	///
	/// - parameter index: The index of the desired column.
	///
	/// - throws: An error if `index` is out of bounds.
	///
	/// - returns: The column's value or `nil` if null.
	public func urlOrNil(forColumn index: Int) throws -> URL? {
		if try typeofColumn(index) == .null {
			return nil
		}
		return try url(forColumn: index)
	}
}
