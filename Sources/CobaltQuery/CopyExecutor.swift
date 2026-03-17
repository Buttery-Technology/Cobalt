import Foundation
import CobaltCore

/// Provides CSV import/export (COPY) functionality.
public struct CopyExecutor: Sendable {

    /// Export rows as a CSV string.
    /// The first line contains column headers, subsequent lines contain values.
    public static func exportCSV(rows: [Row], columns: [String]) -> String {
        var lines: [String] = []

        // Header line
        lines.append(columns.joined(separator: ","))

        // Data lines
        for row in rows {
            let fields = columns.map { col -> String in
                guard let value = row.values[col] else { return "" }
                return escapeCSVField(formatDBValue(value))
            }
            lines.append(fields.joined(separator: ","))
        }

        return lines.joined(separator: "\n")
    }

    /// Parse a CSV string into an array of row dictionaries.
    /// If `columns` is provided, uses those as keys. Otherwise, uses the first line as headers.
    public static func parseCSV(_ csv: String, columns: [String]? = nil) -> [[String: DBValue]] {
        let lines = csv.components(separatedBy: "\n").filter { !$0.isEmpty }
        guard !lines.isEmpty else { return [] }

        let headers: [String]
        let dataStartIndex: Int

        if let cols = columns, !cols.isEmpty {
            headers = cols
            // Check if first line is a header matching columns
            let firstFields = parseCSVLine(lines[0])
            if firstFields == cols {
                dataStartIndex = 1
            } else {
                dataStartIndex = 0
            }
        } else {
            headers = parseCSVLine(lines[0])
            dataStartIndex = 1
        }

        var result: [[String: DBValue]] = []

        for i in dataStartIndex..<lines.count {
            let fields = parseCSVLine(lines[i])
            var dict: [String: DBValue] = [:]
            for (j, header) in headers.enumerated() {
                if j < fields.count {
                    dict[header] = inferDBValue(fields[j])
                } else {
                    dict[header] = .null
                }
            }
            result.append(dict)
        }

        return result
    }

    // MARK: - Private helpers

    private static func formatDBValue(_ value: DBValue) -> String {
        switch value {
        case .null: return ""
        case .integer(let v): return "\(v)"
        case .double(let v): return "\(v)"
        case .string(let v): return v
        case .boolean(let v): return v ? "true" : "false"
        case .blob(let v): return v.base64EncodedString()
        case .compound: return ""
        }
    }

    private static func escapeCSVField(_ field: String) -> String {
        if field.contains(",") || field.contains("\"") || field.contains("\n") {
            let escaped = field.replacingOccurrences(of: "\"", with: "\"\"")
            return "\"\(escaped)\""
        }
        return field
    }

    /// Parse a single CSV line, handling quoted fields.
    private static func parseCSVLine(_ line: String) -> [String] {
        var fields: [String] = []
        var current = ""
        var inQuotes = false
        var chars = line.makeIterator()

        while let c = chars.next() {
            if inQuotes {
                if c == "\"" {
                    // Check for escaped quote
                    if let next = chars.next() {
                        if next == "\"" {
                            current.append("\"")
                        } else {
                            inQuotes = false
                            if next == "," {
                                fields.append(current)
                                current = ""
                            } else {
                                current.append(next)
                            }
                        }
                    } else {
                        inQuotes = false
                    }
                } else {
                    current.append(c)
                }
            } else {
                if c == "\"" {
                    inQuotes = true
                } else if c == "," {
                    fields.append(current)
                    current = ""
                } else {
                    current.append(c)
                }
            }
        }
        fields.append(current)
        return fields
    }

    /// Infer a DBValue from a CSV string field.
    private static func inferDBValue(_ field: String) -> DBValue {
        let trimmed = field.trimmingCharacters(in: .whitespaces)
        if trimmed.isEmpty { return .null }
        if trimmed.lowercased() == "null" { return .null }
        if trimmed.lowercased() == "true" { return .boolean(true) }
        if trimmed.lowercased() == "false" { return .boolean(false) }
        if let intVal = Int64(trimmed) { return .integer(intVal) }
        if let dblVal = Double(trimmed), trimmed.contains(".") { return .double(dblVal) }
        return .string(field)
    }
}
