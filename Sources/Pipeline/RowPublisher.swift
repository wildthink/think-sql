//
// Copyright © 2021 Stephen F. Booth <me@sbooth.org>
// Part of https://github.com/sbooth/Pipeline
// MIT license
//

import Foundation

import Combine

extension Database {
	/// Creates and returns a publisher for an SQL statement's result rows.
	///
	/// - parameter sql: The SQL statement to compile.
	/// - parameter bindings: A closure binding desired SQL parameters.
	///
	/// - returns: A publisher for the statement's result rows.
	public func rowPublisher(sql: String, bindings: @escaping (_ statement: Statement) throws -> Void = { _ in }) -> AnyPublisher<Row, SQLiteError> {
		Publishers.RowPublisher(database: self, sql: sql, bindings: bindings)
			.eraseToAnyPublisher()
	}
}

extension Publishers {
	struct RowPublisher: Publisher {
		typealias Output = Database.Row
		typealias Failure = SQLiteError

		private let database: Database
		private let sql: String
		private let bindings: (_ statement: Database.Statement) throws -> Void

		fileprivate init(database: Database, sql: String, bindings: @escaping (_ statement: Database.Statement) throws -> Void) {
			self.database = database
			self.sql = sql
			self.bindings = bindings
		}

		func receive<S>(subscriber: S) where S: Subscriber, Failure == S.Failure, Output == S.Input {
			do {
				let statement = try database.prepare(sql: sql)
				try bindings(statement)
				let subscription = Subscription(subscriber: subscriber, statement: statement)
				subscriber.receive(subscription: subscription)
			} catch let error as SQLiteError {
				Fail<Output, Failure>(error: error).subscribe(subscriber)
			} catch {
				Fail<Output, Failure>(error: SQLiteError(code: SQLITE_ERROR, details: "Unknown error creating a row publisher subscription. Did the binding closure throw something other than SQLiteError?")).subscribe(subscriber)
			}
		}
	}
}

extension Publishers.RowPublisher {
	private final class Subscription<S>: Combine.Subscription where S: Subscriber, S.Input == Output, S.Failure == Failure {
		/// The subscriber.
		private let subscriber: AnySubscriber<Output, Failure>
		/// The current subscriber demand.
		private var demand: Subscribers.Demand = .none
		/// The statement providing the result rows.
		private let statement: Database.Statement

		fileprivate init(subscriber: S, statement: Database.Statement) {
			self.subscriber = AnySubscriber(subscriber)
			self.statement = statement
		}

		func request(_ demand: Subscribers.Demand) {
			self.demand = demand
			while self.demand != .none {
				let result = sqlite3_step(statement.preparedStatement)
				switch result {
				case SQLITE_ROW:
					self.demand -= 1
					self.demand += subscriber.receive(Database.Row(statement: statement))
				case SQLITE_DONE:
					subscriber.receive(completion: .finished)
					self.demand = .none
				default:
					subscriber.receive(completion: .failure(SQLiteError(fromPreparedStatement: statement.preparedStatement)))
					self.demand = .none
				}
			}
		}

		func cancel() {
			demand = .none
		}
	}
}
