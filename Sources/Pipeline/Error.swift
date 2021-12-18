//
// Copyright © 2021 Stephen F. Booth <me@sbooth.org>
// Part of https://github.com/sbooth/Pipeline
// MIT license
//

import Foundation

extension Database {
	/// An error supplying a message and description.
	public struct Error: Swift.Error {
		/// A brief message describing the error.
		public let message: String

		/// A more detailed description of the error's cause.
		public let details: String?
	}
}

extension Database.Error {
	/// Creates an error with the given message.
	///
	/// - parameter message: A brief message describing the error.
	public init(message: String) {
		self.message = message
		self.details = nil
	}
}

extension Database.Error: CustomStringConvertible {
	public var description: String {
		if let details = details {
			return "\(message): \(details)"
		} else {
			return message
		}
	}
}
