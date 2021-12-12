import Foundation
import SQLite3

public typealias SQLiteArguments = Dictionary<String, SQLiteValue>
public typealias SQLiteRow = Dictionary<String, SQLiteValue>

public enum SQLiteValue: Hashable {
    case data(Data)
    case double(Double)
    case integer(Int64)
    case null
    case text(String)
}

// jmj
/// An `sqlite3_value *` object.
///
/// - seealso: [Obtaining SQL Values](https://sqlite.org/c3ref/value_blob.html)
public extension SQLiteValue {
    /// Creates an instance containing `value`.
    ///
    /// - parameter value: An `sqlite3_value *` object.
    init(sqliteValue value: OpaquePointer) {
        let type = sqlite3_value_type(value)
        switch type {
            case SQLITE_INTEGER:
                self = .integer(sqlite3_value_int64(value))
            case SQLITE_FLOAT:
                self = .double(sqlite3_value_double(value))
            case SQLITE_TEXT:
                self = .text(String(cString: sqlite3_value_text(value)))
            case SQLITE_BLOB:
                self = .data(Data(bytes: sqlite3_value_blob(value), count: Int(sqlite3_value_bytes(value))))
            case SQLITE_NULL:
                self = .null
            default:
                fatalError("Unknown SQLite value type \(type) encountered")
        }
    }
}

extension SQLiteValue {
    public var boolValue: Bool? {
        guard case .integer(let int) = self else { return nil }
        return int == 0 ? false : true
    }

    public var dataValue: Data? {
        guard case .data(let data) = self else { return nil }
        return data
    }

    public var doubleValue: Double? {
        // jmj
        switch self {
            case .double(let value):
                return value
            case .integer(let value):
                return Double(value)
            case .text(let value):
                return Double(value)
            default:
                return nil
        }
//        guard case .double(let double) = self else { return nil }
//        return double
    }

    public var intValue: Int? {
        // jmj
        switch self {
            case .double(let value):
                return Int(value)
            case .integer(let value):
                return Int(value)
            case .text(let value):
                return Int(value)
            default:
                return nil
        }
//        guard case .integer(let int) = self else { return nil }
//        return Int(int)
    }

    public var int64Value: Int64? {
        // jmj
        switch self {
            case .double(let value):
                return Int64(value)
            case .integer(let value):
                return Int64(value)
            case .text(let value):
                return Int64(value)
            default:
                return nil
        }
//        guard case .integer(let int) = self else { return nil }
//        return int
    }

    public var stringValue: String? {
        // jmj
        switch self {
            case .double(let value):
                return String(value)
            case .integer(let value):
                return String(value)
            case .text(let value):
                return value
            default:
                return nil
        }
//        guard case .text(let string) = self else { return nil }
//        return string
    }
    
    // jmj
    public var anyValue: Any? {
        switch self {
            case .data(let value): return value
            case .double(let value): return value
            case .integer(let value): return value
            case .text(let value): return value
            case .null: return nil
        }
    }
}

extension Array where Element == UInt8 {
    public var sqliteValue: SQLiteValue {
        .data(Data(self))
    }
}

extension BinaryInteger {
    public var sqliteValue: SQLiteValue {
        .integer(Int64(self))
    }
}

extension Bool {
    public var sqliteValue: SQLiteValue {
        .integer(self ? 1 : 0)
    }
}

extension Data {
    public var sqliteValue: SQLiteValue {
        .data(self)
    }
}

extension Date {
    public var sqliteValue: SQLiteValue {
        .text(PreciseDateFormatter.string(from: self))
    }
}

extension StringProtocol {
    public var sqliteValue: SQLiteValue {
        .text(String(self))
    }
}

extension Optional where Wrapped == Array<UInt8> {
    public var sqliteValue: SQLiteValue {
        switch self {
        case .none:
            return .null
        case let .some(bytes):
            return .data(Data(bytes))
        }
    }
}

extension Optional where Wrapped: BinaryInteger {
    public var sqliteValue: SQLiteValue {
        switch self {
        case .none:
            return .null
        case let .some(int):
            return int.sqliteValue
        }
    }
}

extension Optional where Wrapped == Bool {
    public var sqliteValue: SQLiteValue {
        switch self {
        case .none:
            return .null
        case let .some(bool):
            return bool.sqliteValue
        }
    }
}

extension Optional where Wrapped == Data {
    public var sqliteValue: SQLiteValue {
        switch self {
        case .none:
            return .null
        case let .some(data):
            return .data(data)
        }
    }
}

extension Optional where Wrapped == Date {
    public var sqliteValue: SQLiteValue {
        switch self {
        case .none:
            return .null
        case let .some(date):
            return date.sqliteValue
        }
    }
}

extension Optional where Wrapped: StringProtocol {
    public var sqliteValue: SQLiteValue {
        switch self {
        case .none:
            return .null
        case let .some(string):
            return string.sqliteValue
        }
    }
}
