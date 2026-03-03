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

        // Cap recordCount to prevent out-of-bounds reads on corrupted pages
        let maxSlots = (PantryConstants.PAGE_SIZE - PantryConstants.PAGE_HEADER_SIZE) / PantryConstants.SLOT_SIZE
        if recordCount < 0 || recordCount > maxSlots {
            recordCount = 0
        }

        // Validate freeSpaceOffset bounds — corrupted values can cause out-of-bounds access
        if freeSpaceOffset < PantryConstants.PAGE_HEADER_SIZE || freeSpaceOffset > PantryConstants.PAGE_SIZE {
            freeSpaceOffset = PantryConstants.PAGE_SIZE
            recordCount = 0
        }

        recordSlots = []
        for _ in 0..<recordCount {
            guard position + PantryConstants.SLOT_SIZE <= data.count else { break }

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

    /// Parse records from the raw page data buffer, but only deserialize records
    /// whose IDs are in the given set. Reads the 8-byte record ID from each slot header
    /// and skips full deserialization for non-matching records.
    /// Both `records` and `recordSlots` are filtered to only contain matching entries,
    /// keeping their indices aligned for correct use with `replaceRecordAndPatch`.
    /// NOTE: Pages loaded this way must NOT use `saveRecords()` — only in-place
    /// patching via `replaceRecordAndPatch` is safe (the data buffer is intact).
    public mutating func loadRecordsSelective(ids: Set<UInt64>) {
        records = []
        recordSlots = []
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

        let maxSlots = (PantryConstants.PAGE_SIZE - PantryConstants.PAGE_HEADER_SIZE) / PantryConstants.SLOT_SIZE
        if recordCount < 0 || recordCount > maxSlots {
            recordCount = 0
        }

        if freeSpaceOffset < PantryConstants.PAGE_HEADER_SIZE || freeSpaceOffset > PantryConstants.PAGE_SIZE {
            freeSpaceOffset = PantryConstants.PAGE_SIZE
            recordCount = 0
        }

        // Parse all slots from the header, then filter by matching IDs
        var allSlots: [(offset: Int, length: Int)] = []
        for _ in 0..<recordCount {
            guard position + PantryConstants.SLOT_SIZE <= data.count else { break }

            let offset = data.withUnsafeBytes { Int($0.loadUnaligned(fromByteOffset: position, as: UInt16.self)) }
            position += 2

            let length = data.withUnsafeBytes { Int($0.loadUnaligned(fromByteOffset: position, as: UInt32.self)) }
            position += 4

            allSlots.append((offset: offset, length: length))
        }

        for slot in allSlots {
            guard slot.offset + slot.length <= data.count else { continue }
            guard slot.offset + 8 <= data.count else { continue }
            let recordID = data.withUnsafeBytes {
                $0.loadUnaligned(fromByteOffset: slot.offset, as: UInt64.self)
            }
            guard ids.contains(recordID) else { continue }
            let recordData = data.subdata(in: slot.offset..<(slot.offset + slot.length))
            if let record = Record.deserialize(from: recordData) {
                recordSlots.append(slot)
                records.append(record)
            }
        }
    }

    /// Serialize all records back into the raw page data buffer.
    /// Uses direct buffer writes to avoid intermediate Data allocations and unnecessary zero-fills.
    public mutating func saveRecords() throws {
        let headerSize = PantryConstants.PAGE_HEADER_SIZE
        let slotSize = PantryConstants.SLOT_SIZE

        var dataPosition = PantryConstants.PAGE_SIZE
        var newSlots: [(offset: Int, length: Int)] = []
        var overflowed = false

        // Snapshot values needed inside the closure to avoid self-access conflicts
        let currentRecords = records
        let currentPageID = pageID
        let currentNextPageID = nextPageID
        let currentFlags = flags

        data.withUnsafeMutableBytes { buf in
            // Write records directly into page buffer (growing from end backward)
            for record in currentRecords {
                let recordLength = record.serializedSize
                let nextSlotEnd = headerSize + (newSlots.count + 1) * slotSize
                guard nextSlotEnd < (dataPosition - recordLength) else {
                    overflowed = true; break
                }
                dataPosition -= recordLength
                newSlots.append((offset: dataPosition, length: recordLength))
                record.serializeInto(buf, at: dataPosition)
            }

            guard !overflowed else { return }

            // Zero only the free-space gap between slot directory end and first record
            let slotsEnd = headerSize + newSlots.count * slotSize
            if slotsEnd < dataPosition {
                memset(buf.baseAddress!.advanced(by: slotsEnd), 0, dataPosition - slotsEnd)
            }

            // Write header
            buf.storeBytes(of: currentPageID, toByteOffset: 0, as: Int.self)
            buf.storeBytes(of: currentNextPageID, toByteOffset: 8, as: Int.self)
            buf.storeBytes(of: Int32(currentRecords.count), toByteOffset: 16, as: Int32.self)
            buf.storeBytes(of: Int32(dataPosition), toByteOffset: 20, as: Int32.self)
            buf.storeBytes(of: currentFlags, toByteOffset: 24, as: UInt32.self)

            // Write slot directory
            var pos = headerSize
            for slot in newSlots {
                buf.storeBytes(of: UInt16(slot.offset), toByteOffset: pos, as: UInt16.self)
                pos += 2
                buf.storeBytes(of: UInt32(slot.length), toByteOffset: pos, as: UInt32.self)
                pos += 4
            }
        }

        if overflowed { throw PantryError.pageOverflow }

        freeSpaceOffset = dataPosition
        recordCount = records.count
        recordSlots = newSlots
    }

    /// Add a record to this page. Returns false if not enough space.
    public mutating func addRecord(_ record: Record) -> Bool {
        addRecord(record, knownSerializedSize: record.serializedSize)
    }

    /// Add a record with a pre-computed serialized size to avoid double serialization.
    public mutating func addRecord(_ record: Record, knownSerializedSize: Int) -> Bool {
        let slotSize = PantryConstants.SLOT_SIZE

        // Account for the new slot entry AND the record data
        let headerEnd = PantryConstants.PAGE_HEADER_SIZE + ((recordCount + 1) * slotSize)
        let newFreeOffset = freeSpaceOffset - knownSerializedSize

        if headerEnd >= newFreeOffset {
            return false
        }

        records.append(record)
        recordCount += 1
        freeSpaceOffset = newFreeOffset
        return true
    }

    /// Replace a record in-place if the new record fits. Returns true on success.
    /// Sets `lastPatchIndex` for fast single-record serialization via `saveRecordPatch()`.
    public mutating func replaceRecord(id: UInt64, with newRecord: Record) -> Bool {
        guard let index = records.firstIndex(where: { $0.id == id }) else {
            return false
        }
        let oldSize = index < recordSlots.count ? recordSlots[index].length : records[index].serializedSize
        let newSize = newRecord.serializedSize
        let sizeDiff = newSize - oldSize
        // Check if new record fits: freeSpaceOffset shrinks by sizeDiff
        let headerEnd = PantryConstants.PAGE_HEADER_SIZE + (recordCount * PantryConstants.SLOT_SIZE)
        let newFreeOffset = freeSpaceOffset - sizeDiff
        if headerEnd >= newFreeOffset {
            return false
        }
        records[index] = newRecord
        freeSpaceOffset = newFreeOffset
        // If a previous replace wasn't patched, invalidate (multi-replace needs full save)
        if lastPatchIndex != nil {
            lastPatchIndex = nil
            patchInvalidated = true
        } else {
            lastPatchIndex = (index, oldSize, newSize)
        }
        return true
    }

    /// Replace a same-size record AND immediately patch the data buffer in-place.
    /// Returns true only when the new record is exactly the same serialized size as the old one.
    /// When true, the page's `data` buffer is already up-to-date — no `saveRecords()` needed,
    /// just update the buffer pool and flush.
    public mutating func replaceRecordAndPatch(id: UInt64, with newRecord: Record) -> Bool {
        guard let index = records.firstIndex(where: { $0.id == id }),
              index < recordSlots.count else { return false }
        let slot = recordSlots[index]
        let newData = newRecord.serialize()
        guard newData.count == slot.length else { return false }
        // Update records array and patch data buffer in-place
        records[index] = newRecord
        data.replaceSubrange(slot.offset..<(slot.offset + slot.length), with: newData)
        allPatched = true
        return true
    }

    /// True when all modifications since last save were applied via replaceRecordAndPatch.
    /// When set, saveRecords() can be skipped — the data buffer is already correct.
    public var allPatched = false

    /// Tracks the last replaceRecord operation for fast patching: (recordIndex, oldSize, newSize)
    public var lastPatchIndex: (index: Int, oldSize: Int, newSize: Int)?
    /// Set when multiple replaceRecord calls happen without patching — forces full save
    public var patchInvalidated = false

    /// Fast save when a single record was replaced and its size didn't change.
    /// Patches the data buffer in-place instead of re-serializing all records.
    /// Returns true if the fast path was used, false if full saveRecords() is needed.
    public mutating func saveRecordPatch() -> Bool {
        guard !patchInvalidated,
              let patch = lastPatchIndex,
              patch.oldSize == patch.newSize,
              patch.index < recordSlots.count else {
            lastPatchIndex = nil
            patchInvalidated = false
            return false
        }
        let slot = recordSlots[patch.index]
        let newData = records[patch.index].serialize()
        guard newData.count == slot.length else {
            lastPatchIndex = nil
            patchInvalidated = false
            return false
        }
        // Patch the record data directly in the buffer
        data.replaceSubrange(slot.offset..<(slot.offset + slot.length), with: newData)
        lastPatchIndex = nil
        patchInvalidated = false
        return true
    }

    /// Delete a record by ID. Returns false if not found.
    public mutating func deleteRecord(id: UInt64) -> Bool {
        guard let index = records.firstIndex(where: { $0.id == id }) else {
            return false
        }
        let removedSize = index < recordSlots.count ? recordSlots[index].length : records[index].serializedSize
        records.remove(at: index)
        if index < recordSlots.count { recordSlots.remove(at: index) }
        recordCount -= 1
        freeSpaceOffset += removedSize
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

    /// Compute the free space category for bitmap tracking
    public func spaceCategory() -> SpaceCategory {
        let free = getFreeSpace()
        if free > 6144 { return .empty }
        if free > 2048 { return .available }
        if free >= 256 { return .low }
        return .full
    }
}
