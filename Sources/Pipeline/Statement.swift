//
// Copyright © 2021 Stephen F. Booth <me@sbooth.org>
// Part of https://github.com/sbooth/Pipeline
// MIT license
//

import Foundation


/// An `sqlite3_stmt *` object.
///
/// - seealso: [SQLite Prepared Statement Object](https://sqlite.org/c3ref/stmt.html)
public typealias SQLitePreparedStatement = OpaquePointer

extension Database {
	/// A compiled SQL statement with support for SQL parameter binding.
	public final class Statement {
		/// The owning database
		public let database: Database
		/// The underlying `sqlite3_stmt *` object
		let preparedStatement: SQLitePreparedStatement

		/// Creates a compiled SQL statement.
		///
		/// - parameter database: The owning database.
		/// - parameter preparedStatement: An `sqlite3_stmt *` prepared statement object..
		///
		/// - throws: An error if `sql` could not be compiled
		public init(database: Database, preparedStatement: SQLitePreparedStatement) {
			precondition(sqlite3_db_handle(preparedStatement) == database.databaseConnection)
			self.database = database
			self.preparedStatement = preparedStatement
		}

		deinit {
			_ = sqlite3_finalize(preparedStatement)
		}

		/// Creates a compiled SQL statement.
		///
		/// - parameter database: The owning database.
		/// - parameter sql: The SQL statement to compile.
		///
		/// - throws: An error if `sql` could not be compiled.
		public convenience init(database: Database, sql: String) throws {
			var stmt: SQLitePreparedStatement?
			let result = sqlite3_prepare_v2(database.databaseConnection, sql, -1, &stmt, nil)
			guard result == SQLITE_OK else {
				throw SQLiteError(fromDatabaseConnection: database.databaseConnection)
			}
			precondition(stmt != nil)
			self.init(database: database, preparedStatement: stmt.unsafelyUnwrapped)
		}

		/// `true` if this statement makes no direct changes to the database, `false` otherwise.
		///
		/// - seealso: [Read-only statements in SQLite](https://sqlite.org/c3ref/stmt_readonly.html)
		public var isReadOnly: Bool {
			return sqlite3_stmt_readonly(preparedStatement) != 0
		}

		/// The number of columns in the result set.
		public var columnCount: Int {
			Int(sqlite3_column_count(preparedStatement))
		}

		/// The names of the columns.
		///
		/// - note: Column names are not guaranteed to be unique.
		public lazy var columnNames: [String] = {
			let count = sqlite3_column_count(preparedStatement)
			var names: [String] = []
			for i in 0 ..< count {
				if let s = sqlite3_column_name(preparedStatement, i) {
					names.append(String(cString: s))
				}
			}
			return names
		}()

		/// Returns the name of the column at `index`.
		///
		/// - note: Column indexes are 0-based.  The leftmost column in a result row has index 0.
		///
		/// - precondition: `index >= 0`
		/// - requires: `index < self.columnCount`
		///
		/// - parameter index: The index of the desired column.
		///
		/// - throws: An error if `index` is out of bounds.
		///
		/// - returns: The name of the column for the specified index
		public func name(ofColumn index: Int) throws -> String {
			precondition(index >= 0)
			guard let name = sqlite3_column_name(preparedStatement, Int32(index)) else {
				throw Database.Error(message: "Column index \(index) out of bounds")
			}
			return String(cString: name)
		}
	}
}

extension Database.Statement {
	/// Performs a low-level SQLite statement operation.
	///
	/// - attention: **Use of this function should be avoided whenever possible.**
	///
	/// - parameter block: A closure performing the statement operation.
	/// - parameter preparedStatement: The raw `sqlite3_stmt *` prepared statement object.
	///
	/// - throws: Any error thrown in `block`.
	///
	/// - returns: The value returned by `block`.
	public func withUnsafeSQLitePreparedStatement<T>(block: (_ preparedStatement: SQLitePreparedStatement) throws -> (T)) rethrows -> T {
		try block(preparedStatement)
	}
}

extension Database.Statement {
	/// Executes the statement and discards any result rows.
	///
	/// - throws: An error if the statement could not be executed.
	public func execute() throws {
		var result = sqlite3_step(preparedStatement)
		while result == SQLITE_ROW {
			result = sqlite3_step(preparedStatement)
		}
		guard result == SQLITE_DONE else {
			throw SQLiteError(fromPreparedStatement: preparedStatement)
		}
	}

	/// Executes the statement and applies `block` to each result row.
	///
	/// - parameter block: A closure applied to each result row.
	/// - parameter row: A result row of returned data.
	///
	/// - throws: Any error thrown in `block` or an error if the statement did not successfully run to completion
	public func results(_ block: ((_ row: Database.Row) throws -> ())) throws {
		var result = sqlite3_step(preparedStatement)
		while result == SQLITE_ROW {
			try block(Database.Row(statement: self))
			result = sqlite3_step(preparedStatement)
		}
		guard result == SQLITE_DONE else {
			throw SQLiteError(fromPreparedStatement: preparedStatement)
		}
	}

	/// Returns the next result row or `nil` if none.
	///
	/// - returns: The next result row of returned data.
	///
	/// - throws: An error if the statement encountered an execution error.
	public func nextRow() throws -> Database.Row? {
		switch sqlite3_step(preparedStatement) {
		case SQLITE_ROW:
			return Database.Row(statement: self)
		case SQLITE_DONE:
			return nil
		default:
			throw SQLiteError(fromPreparedStatement: preparedStatement)
		}
	}

	/// Resets the statement to its initial state, ready to be re-executed.
	///
	/// - note: This function does not change the value of  any bound SQL parameters.
	///
	/// - throws: An error if the statement could not be reset.
	public func reset() throws {
		guard sqlite3_reset(preparedStatement) == SQLITE_OK else {
			throw SQLiteError(fromPreparedStatement: preparedStatement)
		}
	}
}

extension Database.Statement {
	/// The original SQL text of the statement.
	public var sql: String {
		let str = sqlite3_sql(preparedStatement)
		precondition(str != nil)
		return String(cString: str.unsafelyUnwrapped)
	}

#if SQLITE_ENABLE_NORMALIZE
	/// The normalized SQL text of the statement.
	public var normalizedSQL: String {
		guard let str = sqlite3_normalized_sql(preparedStatement) else {
			return ""
		}
		return String(cString: str)
	}
#endif

	/// The SQL text of the statement with bound parameters expanded.
	public var expandedSQL: String {
		guard let str = sqlite3_expanded_sql(preparedStatement) else {
			return ""
		}
		defer {
			sqlite3_free(str)
		}
		return String(cString: str)
	}
}

extension Database.Statement {
	/// The number of SQL parameters in this statement.
	public var parameterCount: Int {
		Int(sqlite3_bind_parameter_count(preparedStatement))
	}

	/// Returns the name of the SQL parameter at `index`.
	///
	/// - note: Parameter indexes are 1-based.  The leftmost parameter in a statement has index 1.
	///
	/// - precondition: `index > 0`
	/// - requires: `index < parameterCount`
	///
	/// - parameter index: The index of the desired SQL parameter.
	///
	/// - returns: The name of the specified parameter.
	public func nameOfParameter(_ index: Int) -> String {
		precondition(index > 0)
		return String(cString: sqlite3_bind_parameter_name(preparedStatement, Int32(index)))
	}

	/// Returns the index of the SQL parameter with `name`.
	///
	/// - parameter name: The name of the desired SQL parameter.
	///
	/// - returns: The index of the specified parameter.
	public func indexOfParameter(_ name: String) throws -> Int {
		let index = sqlite3_bind_parameter_index(preparedStatement, name)
		guard index != 0 else {
			throw Database.Error(message: "SQL parameter \(name) not found")
		}
		return Int(index)
	}

	/// Clears all statement bindings by setting SQL parameters to null.
	///
	/// - throws: An error if the bindings could not be cleared.
	public func clearBindings() throws {
		guard sqlite3_clear_bindings(preparedStatement) == SQLITE_OK else {
			throw SQLiteError(fromPreparedStatement: preparedStatement)
		}
	}
}

extension Database.Statement {
	/// Binds database `NULL` to the SQL parameter at `index`.
	///
	/// - note: Parameter indexes are 1-based.  The leftmost parameter in a statement has index 1.
	///
	/// - precondition: `index > 0`
	/// - requires: `index < parameterCount`
	///
	/// - parameter index: The index of the SQL parameter to bind.
	///
	/// - throws: An error if `NULL` couldn't be bound to the specified parameter.
	public func bindNull(toParameter index: Int) throws {
		precondition(index > 0)
		guard sqlite3_bind_null(preparedStatement, Int32(index)) == SQLITE_OK else {
			throw SQLiteError(fromPreparedStatement: preparedStatement)
		}
	}

	/// Binds `value` to the SQL parameter at `index`.
	///
	/// - note: Parameter indexes are 1-based.  The leftmost parameter in a statement has index 1.
	///
	/// - precondition: `index > 0`
	/// - requires: `index < parameterCount`
	///
	/// - parameter value: The desired value of the SQL parameter.
	/// - parameter index: The index of the SQL parameter to bind.
	///
	/// - throws: An error if `value` couldn't be bound.
	public func bind(_ value: Int64, toParameter index: Int) throws {
		precondition(index > 0)
		guard sqlite3_bind_int64(preparedStatement, Int32(index), value) == SQLITE_OK else {
			throw SQLiteError(fromPreparedStatement: preparedStatement)
		}
	}

	/// Binds `value` to the SQL parameter at `index`.
	///
	/// - note: Parameter indexes are 1-based.  The leftmost parameter in a statement has index 1.
	///
	/// - precondition: `index > 0`
	/// - requires: `index < parameterCount`
	///
	/// - parameter value: The desired value of the SQL parameter.
	/// - parameter index: The index of the SQL parameter to bind.
	///
	/// - throws: An error if `value` couldn't be bound.
	public func bind(_ value: Double, toParameter index: Int) throws {
		precondition(index > 0)
		guard sqlite3_bind_double(preparedStatement, Int32(index), value) == SQLITE_OK else {
			throw SQLiteError(fromPreparedStatement: preparedStatement)
		}
	}

	/// Binds `value` to the SQL parameter at `index`.
	///
	/// - note: Parameter indexes are 1-based.  The leftmost parameter in a statement has index 1.
	///
	/// - precondition: `index > 0`
	/// - requires: `index < parameterCount`
	///
	/// - parameter value: The desired value of the SQL parameter.
	/// - parameter index: The index of the SQL parameter to bind.
	///
	/// - throws: An error if `value` couldn't be bound.
	public func bind(_ value: String, toParameter index: Int) throws {
		precondition(index > 0)
		try value.withCString {
			guard sqlite3_bind_text(preparedStatement, Int32(index), $0, -1, SQLiteTransientStorage) == SQLITE_OK else {
				throw SQLiteError(fromPreparedStatement: preparedStatement)
			}
		}
	}

	/// Binds `value` to the SQL parameter at `index`.
	///
	/// - note: Parameter indexes are 1-based.  The leftmost parameter in a statement has index 1.
	///
	/// - precondition: `index > 0`
	/// - requires: `index < parameterCount`
	///
	/// - parameter value: The desired value of the SQL parameter.
	/// - parameter index: The index of the SQL parameter to bind.
	///
	/// - throws: An error if `value` couldn't be bound.
	public func bind(_ value: Data, toParameter index: Int) throws {
		precondition(index > 0)
		try value.withUnsafeBytes {
			guard sqlite3_bind_blob(preparedStatement, Int32(index), $0.baseAddress, Int32(value.count), SQLiteTransientStorage) == SQLITE_OK else {
				throw SQLiteError(fromPreparedStatement: preparedStatement)
			}
		}
	}
}

extension Database.Statement {
	/// Binds `value` or null to the SQL parameter at `index`.
	///
	/// - note: Parameter indexes are 1-based.  The leftmost parameter in a statement has index 1.
	///
	/// - precondition: `index > 0`
	/// - requires: `index < parameterCount`
	///
	/// - parameter value: The desired value of the SQL parameter.
	/// - parameter index: The index of the SQL parameter to bind.
	///
	/// - throws: An error if `value` couldn't be bound.
	public func bind(_ value: Int64?, toParameter index: Int) throws {
		if let value = value {
			try bind(value, toParameter: index)
		} else {
			try bindNull(toParameter: index)
		}
	}

	/// Binds `value` or null to the SQL parameter at `index`.
	///
	/// - note: Parameter indexes are 1-based.  The leftmost parameter in a statement has index 1.
	///
	/// - precondition: `index > 0`
	/// - requires: `index < parameterCount`
	///
	/// - parameter value: The desired value of the SQL parameter.
	/// - parameter index: The index of the SQL parameter to bind.
	///
	/// - throws: An error if `value` couldn't be bound.
	public func bind(_ value: Double?, toParameter index: Int) throws {
		if let value = value {
			try bind(value, toParameter: index)
		} else {
			try bindNull(toParameter: index)
		}
	}

	/// Binds `value` or null to the SQL parameter at `index`.
	///
	/// - note: Parameter indexes are 1-based.  The leftmost parameter in a statement has index 1.
	///
	/// - precondition: `index > 0`
	/// - requires: `index < parameterCount`
	///
	/// - parameter value: The desired value of the SQL parameter.
	/// - parameter index: The index of the SQL parameter to bind.
	///
	/// - throws: An error if `value` couldn't be bound.
	public func bind(_ value: String?, toParameter index: Int) throws {
		if let value = value {
			try bind(value, toParameter: index)
		} else {
			try bindNull(toParameter: index)
		}
	}

	/// Binds `value` or null to the SQL parameter at `index`.
	///
	/// - note: Parameter indexes are 1-based.  The leftmost parameter in a statement has index 1.
	///
	/// - precondition: `index > 0`
	/// - requires: `index < parameterCount`
	///
	/// - parameter value: The desired value of the SQL parameter.
	/// - parameter index: The index of the SQL parameter to bind.
	///
	/// - throws: An error if `value` couldn't be bound.
	public func bind(_ value: Data?, toParameter index: Int) throws {
		if let value = value {
			try bind(value, toParameter: index)
		} else {
			try bindNull(toParameter: index)
		}
	}
}

extension Database.Statement {
	/// Binds `value` to the SQL parameter at `index`.
	///
	/// - note: Parameter indexes are 1-based.  The leftmost parameter in a statement has index 1.
	///
	/// - precondition: `index > 0`
	/// - requires: `index < parameterCount`
	///
	/// - parameter value: The desired value of the SQL parameter.
	/// - parameter index: The index of the SQL parameter to bind.
	///
	/// - throws: An error if `value` couldn't be bound.
	public func bind(_ value: Int, toParameter index: Int) throws {
		try bind(Int64(value), toParameter: index)
	}

	/// Binds `value` or null to the SQL parameter at `index`.
	///
	/// - note: Parameter indexes are 1-based.  The leftmost parameter in a statement has index 1.
	///
	/// - precondition: `index > 0`
	/// - requires: `index < parameterCount`
	///
	/// - parameter value: The desired value of the SQL parameter.
	/// - parameter index: The index of the SQL parameter to bind.
	///
	/// - throws: An error if `value` couldn't be bound.
	public func bind(_ value: Int?, toParameter index: Int) throws {
		if let value = value {
			try bind(Int64(value), toParameter: index)
		} else {
			try bindNull(toParameter: index)
		}
	}

	/// Binds `value` to the SQL parameter at `index`.
	///
	/// - note: Parameter indexes are 1-based.  The leftmost parameter in a statement has index 1.
	///
	/// - precondition: `index > 0`
	/// - requires: `index < parameterCount`
	///
	/// - parameter value: The desired value of the SQL parameter.
	/// - parameter index: The index of the SQL parameter to bind.
	///
	/// - throws: An error if `value` couldn't be bound.
	public func bind(_ value: UInt, toParameter index: Int) throws {
		try bind(Int64(value), toParameter: index)
	}

	/// Binds `value` or null to the SQL parameter at `index`.
	///
	/// - note: Parameter indexes are 1-based.  The leftmost parameter in a statement has index 1.
	///
	/// - precondition: `index > 0`
	/// - requires: `index < parameterCount`
	///
	/// - parameter value: The desired value of the SQL parameter.
	/// - parameter index: The index of the SQL parameter to bind.
	///
	/// - throws: An error if `value` couldn't be bound.
	public func bind(_ value: UInt?, toParameter index: Int) throws {
		if let value = value {
			try bind(Int64(value), toParameter: index)
		} else {
			try bindNull(toParameter: index)
		}
	}

	/// Binds `value` to the SQL parameter at `index`.
	///
	/// - note: Parameter indexes are 1-based.  The leftmost parameter in a statement has index 1.
	///
	/// - precondition: `index > 0`
	/// - requires: `index < parameterCount`
	///
	/// - parameter value: The desired value of the SQL parameter.
	/// - parameter index: The index of the SQL parameter to bind.
	///
	/// - throws: An error if `value` couldn't be bound.
	public func bind(_ value: Float, toParameter index: Int) throws {
		try bind(Double(value), toParameter: index)
	}

	/// Binds `value` or null to the SQL parameter at `index`.
	///
	/// - note: Parameter indexes are 1-based.  The leftmost parameter in a statement has index 1.
	///
	/// - precondition: `index > 0`
	/// - requires: `index < parameterCount`
	///
	/// - parameter value: The desired value of the SQL parameter.
	/// - parameter index: The index of the SQL parameter to bind.
	///
	/// - throws: An error if `value` couldn't be bound.
	public func bind(_ value: Float?, toParameter index: Int) throws {
		if let value = value {
			try bind(Double(value), toParameter: index)
		} else {
			try bindNull(toParameter: index)
		}
	}
}

extension Database.Statement {
	/// Binds `value` to the SQL parameter at `index`.
	///
	/// - note: Parameter indexes are 1-based.  The leftmost parameter in a statement has index 1.
	///
	/// - precondition: `index > 0`
	/// - requires: `index < parameterCount`
	///
	/// - parameter value: The desired value of the SQL parameter.
	/// - parameter index: The index of the SQL parameter to bind.
	///
	/// - throws: An error if `value` couldn't be bound.
	public func bind(_ value: UUID, toParameter index: Int) throws {
		try bind(value.uuidString.lowercased(), toParameter: index)
	}

	/// Binds `value` or null to the SQL parameter at `index`.
	///
	/// - note: Parameter indexes are 1-based.  The leftmost parameter in a statement has index 1.
	///
	/// - precondition: `index > 0`
	/// - requires: `index < parameterCount`
	///
	/// - parameter value: The desired value of the SQL parameter.
	/// - parameter index: The index of the SQL parameter to bind.
	///
	/// - throws: An error if `value` couldn't be bound.
	public func bind(_ value: UUID?, toParameter index: Int) throws {
		if let value = value {
			try bind(value, toParameter: index)
		} else {
			try bindNull(toParameter: index)
		}
	}

	/// Binds `value` to the SQL parameter at `index`.
	///
	/// - note: Parameter indexes are 1-based.  The leftmost parameter in a statement has index 1.
	///
	/// - precondition: `index > 0`
	/// - requires: `index < parameterCount`
	///
	/// - parameter value: The desired value of the SQL parameter.
	/// - parameter index: The index of the SQL parameter to bind.
	///
	/// - throws: An error if `value` couldn't be bound.
	public func bind(_ value: URL, toParameter index: Int) throws {
		try bind(value.absoluteString, toParameter: index)
	}

	/// Binds `value` or null to the SQL parameter at `index`.
	///
	/// - note: Parameter indexes are 1-based.  The leftmost parameter in a statement has index 1.
	///
	/// - precondition: `index > 0`
	/// - requires: `index < parameterCount`
	///
	/// - parameter value: The desired value of the SQL parameter.
	/// - parameter index: The index of the SQL parameter to bind.
	///
	/// - throws: An error if `value` couldn't be bound.
	public func bind(_ value: URL?, toParameter index: Int) throws {
		if let value = value {
			try bind(value, toParameter: index)
		} else {
			try bindNull(toParameter: index)
		}
	}
}

#if SQLite3_CARRAY

extension Database.Statement {
	/// Binds `values` to the SQL parameter at `index` using the sqlite3 carray extension.
	///
	/// - note: Parameter indexes are 1-based.  The leftmost parameter in a statement has index 1.
	///
	/// - precondition: `index > 0`
	/// - requires: `index < parameterCount`
	///
	/// - parameter values: An array of values to bind to the SQL parameter.
	/// - parameter index: The index of the SQL parameter to bind.
	///
	/// - throws: An error if `values` couldn't be bound.
	///
	/// - seealso: [The Carray() Table-Valued Function](https://www.sqlite.org/carray.html)
	public func bind<C: Collection>(_ values: C, toParameter index: Int) throws where C.Element == Int32 {
		precondition(index > 0)
		let mem = UnsafeMutableBufferPointer<Int32>.allocate(capacity: values.count)
		_ = mem.initialize(from: values)
		guard sqlite3_carray_bind(preparedStatement, Int32(index), mem.baseAddress, Int32(values.count), CARRAY_INT32, { $0?.deallocate() }) == SQLITE_OK else {
			throw SQLiteError(fromPreparedStatement: preparedStatement)
		}
	}

	/// Binds `values` to the SQL parameter at `index` using the sqlite3 carray extension.
	///
	/// - note: Parameter indexes are 1-based.  The leftmost parameter in a statement has index 1.
	///
	/// - precondition: `index > 0`
	/// - requires: `index < parameterCount`
	///
	/// - parameter values: An array of values to bind to the SQL parameter.
	/// - parameter index: The index of the SQL parameter to bind.
	///
	/// - throws: An error if `values` couldn't be bound.
	///
	/// - seealso: [The Carray() Table-Valued Function](https://www.sqlite.org/carray.html)
	public func bind<C: Collection>(_ values: C, toParameter index: Int) throws where C.Element == Int64 {
		precondition(index > 0)
		let mem = UnsafeMutableBufferPointer<Int64>.allocate(capacity: values.count)
		_ = mem.initialize(from: values)
		guard sqlite3_carray_bind(preparedStatement, Int32(index), mem.baseAddress, Int32(values.count), CARRAY_INT64, { $0?.deallocate() }) == SQLITE_OK else {
			throw SQLiteError(fromPreparedStatement: preparedStatement)
		}
	}

	/// Binds `values` to the SQL parameter at `index` using the sqlite3 carray extension.
	///
	/// - note: Parameter indexes are 1-based.  The leftmost parameter in a statement has index 1.
	///
	/// - precondition: `index > 0`
	/// - requires: `index < parameterCount`
	///
	/// - parameter values: An array of values to bind to the SQL parameter.
	/// - parameter index: The index of the SQL parameter to bind.
	///
	/// - throws: An error if `values` couldn't be bound.
	///
	/// - seealso: [The Carray() Table-Valued Function](https://www.sqlite.org/carray.html)
	public func bind<C: Collection>(_ values: C, toParameter index: Int) throws where C.Element == Double {
		precondition(index > 0)
		let mem = UnsafeMutableBufferPointer<Double>.allocate(capacity: values.count)
		_ = mem.initialize(from: values)
		guard sqlite3_carray_bind(preparedStatement, Int32(index), mem.baseAddress, Int32(values.count), CARRAY_DOUBLE, { $0?.deallocate() }) == SQLITE_OK else {
			throw SQLiteError(fromPreparedStatement: preparedStatement)
		}
	}

	/// Binds `values` to the SQL parameter at `index` using the sqlite3 carray extension.
	///
	/// - note: Parameter indexes are 1-based.  The leftmost parameter in a statement has index 1.
	///
	/// - precondition: `index > 0`
	/// - requires: `index < parameterCount`
	///
	/// - parameter values: An array of values to bind to the SQL parameter.
	/// - parameter index: The index of the SQL parameter to bind.
	///
	/// - throws: An error if `values` couldn't be bound.
	///
	/// - seealso: [The Carray() Table-Valued Function](https://www.sqlite.org/carray.html)
	public func bind<C: Collection>(_ values: C, toParameter index: Int) throws where C.Element == String {
		precondition(index > 0)
		let count = values.count

		let utf8_character_counts = values.map { $0.utf8.count + 1 }
		let utf8_offsets = [ 0 ] + scan(utf8_character_counts, 0, +)
		let utf8_buf_size = utf8_offsets.last!

		let ptr_size = MemoryLayout<UnsafePointer<Int8>>.stride * count
		let alloc_size = ptr_size + utf8_buf_size

		let mem = UnsafeMutableRawPointer.allocate(byteCount: alloc_size, alignment: MemoryLayout<UnsafePointer<Int8>>.alignment)

		let ptrs = mem.bindMemory(to: UnsafeMutablePointer<Int8>.self, capacity: count)
		let utf8 = (mem + ptr_size).bindMemory(to: Int8.self, capacity: utf8_buf_size)

		for(i, s) in values.enumerated() {
			let pos = utf8 + utf8_offsets[i]
			ptrs[i] = pos
			memcpy(pos, s, utf8_offsets[i + 1] - utf8_offsets[i])
		}

		guard sqlite3_carray_bind(preparedStatement, Int32(index), mem, Int32(values.count), CARRAY_TEXT, { $0?.deallocate() }) == SQLITE_OK else {
			throw SQLiteError(fromPreparedStatement: preparedStatement)
		}
	}
}

/// Computes the accumulated result  of `seq`
private func accumulate<S: Sequence, U>(_ seq: S, _ initial: U, _ combine: (U, S.Element) -> U) -> [U] {
	var result: [U] = []
	result.reserveCapacity(seq.underestimatedCount)
	var runningResult = initial
	for element in seq {
		runningResult = combine(runningResult, element)
		result.append(runningResult)
	}
	return result
}

/// Computes the prefix sum of `seq`.
private func scan<S: Sequence, U>(_ seq: S, _ initial: U, _ combine: (U, S.Element) -> U) -> [U] {
	var result: [U] = []
	result.reserveCapacity(seq.underestimatedCount)
	var runningResult = initial
	for element in seq {
		runningResult = combine(runningResult, element)
		result.append(runningResult)
	}
	return result
}

#endif
