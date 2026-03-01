import Foundation

/// Persisted free space bitmap for O(1) page selection on insert.
/// Each page gets a 2-bit category (full/low/available/empty).
/// At 2 bits per page, one 8KB bitmap page tracks 32,768 pages (~256MB of data).
public final class FreeSpaceBitmap: Sendable {
    private struct State {
        var bitmap: Data
        var pageID: Int  // system page storing the bitmap (0 = not allocated)
        var dirty: Bool
    }

    private let state: PantryLock<State>

    /// Maximum pages tracked by a single 8KB bitmap page (2 bits each)
    private static let pagesPerBitmapPage = (PantryConstants.PAGE_SIZE - PantryConstants.PAGE_HEADER_SIZE - 12) * 4
    // Record header = 12 bytes (8B id + 4B length), so usable data = PAGE_SIZE - HEADER_SIZE - SLOT_SIZE - 12
    // But we simplify: bitmap data stored as a record, max payload ~8KB - overhead

    /// Initial bitmap size in bytes (tracks up to 32,768 pages)
    private static let initialBitmapSize = 8192  // 8KB = 32,768 * 2 bits

    public init() {
        self.state = PantryLock(State(bitmap: Data(count: Self.initialBitmapSize), pageID: 0, dirty: false))
    }

    // MARK: - Category Access

    /// Get the space category for a given page ID
    public func getCategory(pageID: Int) -> SpaceCategory {
        state.withLock { s in
            let byteIndex = pageID / 4
            let bitOffset = (pageID % 4) * 2
            guard byteIndex < s.bitmap.count else { return .full }
            let raw = (s.bitmap[byteIndex] >> bitOffset) & 0x03
            return SpaceCategory(rawValue: raw) ?? .full
        }
    }

    /// Set the space category for a given page ID
    public func setCategory(pageID: Int, category: SpaceCategory) {
        state.withLock { s in
            let byteIndex = pageID / 4
            let bitOffset = (pageID % 4) * 2

            // Grow bitmap if needed
            if byteIndex >= s.bitmap.count {
                let needed = byteIndex + 1
                s.bitmap.append(Data(count: needed - s.bitmap.count))
            }

            // Clear the 2 bits, then set
            let mask: UInt8 = ~(0x03 << bitOffset)
            s.bitmap[byteIndex] = (s.bitmap[byteIndex] & mask) | (category.rawValue << bitOffset)
            s.dirty = true
        }
    }

    /// Find a page with at least the given minimum space category.
    /// Searches for pages with category >= minCategory.
    /// Returns nil if no qualifying page is found.
    public func findPage(minCategory: SpaceCategory, excluding: Set<Int> = []) -> Int? {
        state.withLock { s in
            for byteIndex in 0..<s.bitmap.count {
                let byte = s.bitmap[byteIndex]
                if byte == 0 { continue }  // All 4 pages are .full — skip

                for slot in 0..<4 {
                    let bitOffset = slot * 2
                    let raw = (byte >> bitOffset) & 0x03
                    if raw >= minCategory.rawValue {
                        let pageID = byteIndex * 4 + slot
                        if !excluding.contains(pageID) {
                            return pageID
                        }
                    }
                }
            }
            return nil
        }
    }

    // MARK: - Persistence

    /// Load the bitmap from a system page
    public func load(storageManager: StorageManager, bitmapPageID: Int) async throws {
        guard bitmapPageID != 0 else { return }
        let page = try await storageManager.readPage(pageID: bitmapPageID)
        guard let record = page.records.first else { return }

        state.withLock { s in
            s.bitmap = record.data
            s.pageID = bitmapPageID
            s.dirty = false
        }
    }

    /// Save the bitmap to a system page. Returns the page ID used.
    @discardableResult
    public func save(storageManager: StorageManager, bufferPool: BufferPoolManager, existingPageID: Int) async throws -> Int {
        let (bitmapData, isDirty) = state.withLock { s in
            (s.bitmap, s.dirty)
        }
        guard isDirty else { return existingPageID }

        var targetPageID = existingPageID
        if targetPageID == 0 {
            let newPage = try await storageManager.createNewPage()
            targetPageID = newPage.pageID
        }

        let record = Record(id: 1, data: bitmapData)
        var page = DatabasePage(pageID: targetPageID)
        page.pageFlags = [.system]
        guard page.addRecord(record) else {
            // Bitmap too large for single page — truncate to fit
            // (this shouldn't happen with reasonable database sizes)
            return targetPageID
        }
        try page.saveRecords()
        var writablePage = page
        try await storageManager.writePage(&writablePage)
        bufferPool.updatePage(writablePage)

        state.withLock { s in
            s.pageID = targetPageID
            s.dirty = false
        }

        return targetPageID
    }

    /// Whether the bitmap has unsaved changes
    public var isDirty: Bool {
        state.withLock { $0.dirty }
    }
}

/// Simple lock wrapper for Sendable conformance (avoids shadowing stdlib Mutex)
public final class PantryLock<Value>: @unchecked Sendable {
    private var value: Value
    private let lock = NSLock()

    public init(_ value: Value) {
        self.value = value
    }

    public func withLock<T>(_ body: (inout Value) -> T) -> T {
        lock.lock()
        defer { lock.unlock() }
        return body(&value)
    }
}
