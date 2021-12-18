//
// Copyright © 2021 Stephen F. Booth <me@sbooth.org>
// Part of https://github.com/sbooth/Pipeline
// MIT license
//

import Foundation
import Combine

/// A type that can be initialized from a result row.
public protocol RowMapping {
	/// Creates an instance mapping some or all values in `row` to `self`.
	///
	/// - parameter row: A `Row` object.
	///
	/// - throws: An error if initialization fails.
	init(row: Database.Row) throws
}

extension Publisher {
	/// Returns a publisher mapping upstream result rows to elements of `type`.
	///
	/// - parameter type: The type of item to create from the row.
	public func mapRows<T>(type: T.Type) -> Publishers.TryMap<Self, T> where Output == Database.Row, T: RowMapping {
//	public func mapRows<T>(type: T.Type) -> AnyPublisher<RowMapping, Error> where Output == Database.Row, T: RowMapping {
		return self
			.tryMap {
				try T(row: $0)
			}
//			.eraseToAnyPublisher()
	}
}
