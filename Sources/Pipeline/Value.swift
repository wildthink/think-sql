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
	public enum Value {
		/// An integer value.
		case integer(Int64)
		/// A floating-point value.
		case float(Double)
		/// A text value.
		case text(String)
		/// A blob (untyped bytes) value.
		case blob(Data)
		/// A null value.
		case null
	}
}

extension Database.Row {
	/// Returns the value of the column at `index`.
	///
	/// - note: Column indexes are 0-based.  The leftmost column in a row has index 0.
	///
	/// - precondition: `index >= 0`
	/// - requires: `index < self.columnCount`
	///
	/// - parameter index: The index of the desired column
	///
	/// - throws: An error if `index` is out of bounds.
	///
	/// - returns: The column's value.
	public func value(forColumn index: Int) throws -> Database.Value {
		precondition(index >= 0)
		let type = sqlite3_column_type(statement.preparedStatement, Int32(index))
		guard statement.database.success else {
			throw SQLiteError(fromDatabaseConnection: statement.database.databaseConnection)
		}
		switch type {
		case SQLITE_INTEGER:
			return .integer(sqlite3_column_int64(statement.preparedStatement, Int32(index)))
		case SQLITE_FLOAT:
			return .float(sqlite3_column_double(statement.preparedStatement, Int32(index)))
		case SQLITE_TEXT:
			return .text(String(cString: sqlite3_column_text(statement.preparedStatement, Int32(index))))
		case SQLITE_BLOB:
			let byteCount = Int(sqlite3_column_bytes(statement.preparedStatement, Int32(index)))
			let data = Data(bytes: sqlite3_column_blob(statement.preparedStatement, Int32(index)).assumingMemoryBound(to: UInt8.self), count: byteCount)
			return .blob(data)
		case SQLITE_NULL:
			return .null
		default:
			fatalError("Unknown SQLite column type \(type) encountered for column \(index)")
		}
	}
}

extension Database.Row {
	/// Returns the values of all columns in the row.
	///
	/// - returns: An array of the row's values.
	public func values() throws -> [Database.Value] {
		var values: [Database.Value] = []
		for i in 0 ..< statement.columnCount {
			values.append(try value(forColumn: i))
		}
		return values
	}

	/// Returns the names and values of all columns in the row.
	///
	/// - warning: This method will fail at runtime if the column names are not unique.
	///
	/// - returns: A dictionary of the row's values keyed by column name.
	public func valueDictionary() throws -> [String: Database.Value] {
		return try Dictionary(uniqueKeysWithValues: statement.columnNames.enumerated().map({ ($0.element, try value(forColumn: $0.offset)) }))
	}
}

extension Database.Row {
	/// Returns the value of the column at `index`.
	///
	/// - note: Column indexes are 0-based.  The leftmost column in a row has index 0.
	///
	/// - precondition: `index >= 0`
	/// - requires: `index < self.columnCount`
	///
	/// - parameter index: The index of the desired column.
	///
	/// - returns: The column's value or `nil` if null, the column doesn't exist, or contains an illegal value.
	public subscript(forColumn index: Int) -> Database.Value? {
		return try? value(forColumn: index)
	}
}

extension Database.Value: Equatable {
	public static func == (lhs: Database.Value, rhs: Database.Value) -> Bool {
		switch (lhs, rhs) {
		case (.integer(let i1), .integer(let i2)):
			return i1 == i2
		case (.float(let f1), .float(let f2)):
			return f1 == f2
		case (.text(let t1), .text(let t2)):
			return t1 == t2
		case (.blob(let b1), .blob(let b2)):
			return b1 == b2
		case (.null, .null):
			// SQL null compares unequal to everything, including null.
			// Is that really the desired behavior here?
			return false
		default:
			return false
		}
	}
}

extension Database.Value: ExpressibleByNilLiteral {
	public init(nilLiteral: ()) {
		self = .null
	}
}

extension Database.Value: ExpressibleByIntegerLiteral {
	public init(integerLiteral value: IntegerLiteralType) {
		self = .integer(Int64(value))
	}
}

extension Database.Value: ExpressibleByFloatLiteral {
	public init(floatLiteral value: FloatLiteralType) {
		self = .float(value)
	}
}

extension Database.Value: ExpressibleByStringLiteral {
	public init(stringLiteral value: StringLiteralType) {
		self = .text(value)
	}
}

extension Database.Value: ExpressibleByBooleanLiteral {
	public init(booleanLiteral value: BooleanLiteralType) {
		self = .integer(value ? 1 : 0)
	}
}

extension Database.Value: CustomStringConvertible {
	/// A description of the type and value of `self`.
	public var description: String {
		switch self {
		case .integer(let i):
			return ".integer(\(i))"
		case .float(let f):
			return ".float(\(f))"
		case .text(let t):
			return ".text(\"\(t)\")"
		case .blob(let b):
			return ".blob(\(b))"
		case .null:
			return ".null"
		}
	}
}
