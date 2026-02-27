import Foundation

/// Page flags stored as a bitmask in the page header
public struct PageFlags: OptionSet, Sendable {
    public let rawValue: UInt32

    public init(rawValue: UInt32) {
        self.rawValue = rawValue
    }

    public static let system       = PageFlags(rawValue: 1 << 0)
    public static let indexNode    = PageFlags(rawValue: 1 << 1)
    public static let dataPage     = PageFlags(rawValue: 1 << 2)
    public static let overflow     = PageFlags(rawValue: 1 << 3)
    public static let tableRegistry = PageFlags(rawValue: 1 << 4)
}

/// A fixed-size 8KB database page with slotted record layout
public struct DatabasePage: Sendable {
    // Page header (fixed 28 bytes)
    public var pageID: Int          // 8 bytes
    public var nextPageID: Int      // 8 bytes
    public var recordCount: Int     // 4 bytes
    public var freeSpaceOffset: Int // 4 bytes
    public var flags: UInt32        // 4 bytes

    // Record slot array (variable, grows from start)
    public var recordSlots: [(offset: Int, length: Int)] = []

    // Raw page data buffer
    public var data: Data

    // Typed records
    public var records: [Record] = []

    public init(
        pageID: Int,
        nextPageID: Int = 0,
        recordCount: Int = 0,
        freeSpaceOffset: Int = PantryConstants.PAGE_SIZE,
        flags: UInt32 = 0,
        data: Data = Data(count: PantryConstants.PAGE_SIZE)
    ) {
        self.pageID = pageID
        self.nextPageID = nextPageID
        self.recordCount = recordCount
        self.freeSpaceOffset = freeSpaceOffset
        self.flags = flags
        self.data = data
    }

    /// Parse records from the raw page data buffer
    public mutating func loadRecords() {
        records = []
        var position = 0

        pageID = data.withUnsafeBytes { $0.loadUnaligned(fromByteOffset: position, as: Int.self) }
        position += 8

        nextPageID = data.withUnsafeBytes { $0.loadUnaligned(fromByteOffset: position, as: Int.self) }
        position += 8

        recordCount = Int(data.withUnsafeBytes { $0.loadUnaligned(fromByteOffset: position, as: Int32.self) })
        position += 4

        freeSpaceOffset = Int(data.withUnsafeBytes { $0.loadUnaligned(fromByteOffset: position, as: Int32.self) })
        position += 4

        flags = data.withUnsafeBytes { $0.loadUnaligned(fromByteOffset: position, as: UInt32.self) }
        position += 4

        recordSlots = []
        for _ in 0..<recordCount {
            let offset = data.withUnsafeBytes { Int($0.loadUnaligned(fromByteOffset: position, as: UInt16.self)) }
            position += 2

            let length = data.withUnsafeBytes { Int($0.loadUnaligned(fromByteOffset: position, as: UInt32.self)) }
            position += 4

            recordSlots.append((offset: offset, length: length))
        }

        for slot in recordSlots {
            guard slot.offset + slot.length <= data.count else { continue }
            let recordData = data.subdata(in: slot.offset..<(slot.offset + slot.length))
            if let record = Record.deserialize(from: recordData) {
                records.append(record)
            }
        }
    }

    /// Serialize all records back into the raw page data buffer
    public mutating func saveRecords() throws {
        var newData = Data(count: PantryConstants.PAGE_SIZE)

        let headerSize = PantryConstants.PAGE_HEADER_SIZE
        let slotSize = PantryConstants.SLOT_SIZE

        var dataPosition = PantryConstants.PAGE_SIZE
        var newSlots: [(offset: Int, length: Int)] = []

        for record in records {
            let recordData = record.serialize()
            let recordLength = recordData.count

            let nextSlotEnd = headerSize + (newSlots.count + 1) * slotSize
            if nextSlotEnd > (dataPosition - recordLength) {
                throw PantryError.pageOverflow
            }

            dataPosition -= recordLength
            newSlots.append((offset: dataPosition, length: recordLength))
            newData.replaceSubrange(dataPosition..<(dataPosition + recordLength), with: recordData)
        }

        freeSpaceOffset = dataPosition
        recordCount = records.count

        var position = 0

        withUnsafeBytes(of: pageID) { buffer in
            newData.replaceSubrange(position..<(position + 8), with: buffer)
        }
        position += 8

        withUnsafeBytes(of: nextPageID) { buffer in
            newData.replaceSubrange(position..<(position + 8), with: buffer)
        }
        position += 8

        var rc = Int32(recordCount)
        withUnsafeBytes(of: &rc) { buffer in
            newData.replaceSubrange(position..<(position + 4), with: buffer)
        }
        position += 4

        var fso = Int32(freeSpaceOffset)
        withUnsafeBytes(of: &fso) { buffer in
            newData.replaceSubrange(position..<(position + 4), with: buffer)
        }
        position += 4

        withUnsafeBytes(of: flags) { buffer in
            newData.replaceSubrange(position..<(position + 4), with: buffer)
        }
        position += 4

        for slot in newSlots {
            let slotOffset = UInt16(slot.offset)
            withUnsafeBytes(of: slotOffset) { buffer in
                newData.replaceSubrange(position..<(position + 2), with: buffer)
            }
            position += 2

            let slotLength = UInt32(slot.length)
            withUnsafeBytes(of: slotLength) { buffer in
                newData.replaceSubrange(position..<(position + 4), with: buffer)
            }
            position += 4
        }

        data = newData
        recordSlots = newSlots
    }

    /// Add a record to this page. Returns false if not enough space.
    public mutating func addRecord(_ record: Record) -> Bool {
        let recordData = record.serialize()
        let recordSize = recordData.count
        let slotSize = PantryConstants.SLOT_SIZE

        // Account for the new slot entry AND the record data
        let headerEnd = PantryConstants.PAGE_HEADER_SIZE + ((recordCount + 1) * slotSize)
        let newFreeOffset = freeSpaceOffset - recordSize

        if headerEnd > newFreeOffset {
            return false
        }

        records.append(record)
        recordCount += 1
        freeSpaceOffset = newFreeOffset
        return true
    }

    /// Delete a record by ID. Returns false if not found.
    public mutating func deleteRecord(id: UInt64) -> Bool {
        guard let index = records.firstIndex(where: { $0.id == id }) else {
            return false
        }
        records.remove(at: index)
        recordCount -= 1
        return true
    }

    /// Available free space in bytes
    public func getFreeSpace() -> Int {
        let headerEnd = PantryConstants.PAGE_HEADER_SIZE + (recordCount * PantryConstants.SLOT_SIZE)
        return freeSpaceOffset - headerEnd
    }

    public var isEmpty: Bool {
        recordCount == 0
    }

    public var isSystemPage: Bool {
        PageFlags(rawValue: flags).contains(.system)
    }

    public var pageFlags: PageFlags {
        get { PageFlags(rawValue: flags) }
        set { flags = newValue.rawValue }
    }
}
