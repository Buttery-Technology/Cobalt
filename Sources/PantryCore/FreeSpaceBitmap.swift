import Foundation

/// Persisted free space bitmap for O(1) page selection on insert.
/// Each page gets a 2-bit category (full/low/available/empty).
/// At 2 bits per page, one 8KB bitmap page tracks 32,768 pages (~256MB of data).
public final class FreeSpaceBitmap: Sendable {
    private struct State {
        var bitmap: Data
        var pageID: Int  // system page storing the bitmap (0 = not allocated)
        var dirty: Bool
        /// Super-bitmap: 1 bit per 64 bytes of bitmap. Bit=1 means region has at least one non-full page.
        /// Enables O(1) skip over fully-full regions instead of linear scan.
        var superBitmap: [UInt64] = []
    }

    /// Read-write lock: concurrent reads (findPage, getCategory) don't block each other.
    /// Only writes (setCategory) take exclusive access.
    private let state: PantryRWLock<State>

    /// Maximum pages tracked by a single 8KB bitmap page (2 bits each)
    private static let pagesPerBitmapPage = (PantryConstants.PAGE_SIZE - PantryConstants.PAGE_HEADER_SIZE - 12) * 4
    // Record header = 12 bytes (8B id + 4B length), so usable data = PAGE_SIZE - HEADER_SIZE - SLOT_SIZE - 12
    // But we simplify: bitmap data stored as a record, max payload ~8KB - overhead

    /// Initial bitmap size in bytes (tracks up to 32,768 pages)
    private static let initialBitmapSize = 8192  // 8KB = 32,768 * 2 bits

    public init() {
        self.state = PantryRWLock(State(bitmap: Data(count: Self.initialBitmapSize), pageID: 0, dirty: false))
    }

    // MARK: - Category Access

    /// Get the space category for a given page ID
    public func getCategory(pageID: Int) -> SpaceCategory {
        state.withReadLock { s in
            let byteIndex = pageID / 4
            let bitOffset = (pageID % 4) * 2
            guard byteIndex < s.bitmap.count else { return .full }
            let raw = (s.bitmap[byteIndex] >> bitOffset) & 0x03
            return SpaceCategory(rawValue: raw) ?? .full
        }
    }

    /// Set the space category for a given page ID
    public func setCategory(pageID: Int, category: SpaceCategory) {
        state.withWriteLock { s in
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

            // Update super-bitmap: region index = byteIndex / 64
            Self.updateSuperBitmap(&s, regionOfByte: byteIndex)
        }
    }

    /// Update one super-bitmap entry for the region containing the given byte
    private static func updateSuperBitmap(_ s: inout State, regionOfByte byteIndex: Int) {
        let regionIndex = byteIndex / 64
        let wordIndex = regionIndex / 64
        let bitIndex = regionIndex % 64

        // Grow super-bitmap if needed
        while wordIndex >= s.superBitmap.count {
            s.superBitmap.append(0)
        }

        // Scan the 64-byte region to check if any page is non-full
        let regionStart = regionIndex * 64
        let regionEnd = min(regionStart + 64, s.bitmap.count)
        var hasNonFull = false
        for i in regionStart..<regionEnd {
            if s.bitmap[i] != 0 {
                hasNonFull = true
                break
            }
        }

        if hasNonFull {
            s.superBitmap[wordIndex] |= (1 << bitIndex)
        } else {
            s.superBitmap[wordIndex] &= ~(1 << bitIndex)
        }
    }

    /// Find a page with at least the given minimum space category.
    /// Uses super-bitmap to skip fully-full regions in O(regions) instead of O(pages).
    /// Returns nil if no qualifying page is found.
    public func findPage(minCategory: SpaceCategory, excluding: Set<Int> = []) -> Int? {
        state.withReadLock { s in
            // Fast path: use super-bitmap to skip empty regions
            if !s.superBitmap.isEmpty {
                for wordIdx in 0..<s.superBitmap.count {
                    var word = s.superBitmap[wordIdx]
                    while word != 0 {
                        let bitIdx = word.trailingZeroBitCount
                        let regionIndex = wordIdx * 64 + bitIdx

                        let regionStart = regionIndex * 64
                        let regionEnd = min(regionStart + 64, s.bitmap.count)

                        for byteIndex in regionStart..<regionEnd {
                            let byte = s.bitmap[byteIndex]
                            if byte == 0 { continue }
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
                        word &= word &- 1  // Clear lowest set bit
                    }
                }
                return nil
            }

            // Fallback: linear scan (before super-bitmap is built)
            for byteIndex in 0..<s.bitmap.count {
                let byte = s.bitmap[byteIndex]
                if byte == 0 { continue }

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

    /// Rebuild the entire super-bitmap from the current bitmap data
    private static func rebuildSuperBitmap(_ s: inout State) {
        let totalRegions = (s.bitmap.count + 63) / 64
        let totalWords = (totalRegions + 63) / 64
        s.superBitmap = [UInt64](repeating: 0, count: totalWords)

        for regionIndex in 0..<totalRegions {
            let regionStart = regionIndex * 64
            let regionEnd = min(regionStart + 64, s.bitmap.count)
            var hasNonFull = false
            for i in regionStart..<regionEnd {
                if s.bitmap[i] != 0 { hasNonFull = true; break }
            }
            if hasNonFull {
                let wordIndex = regionIndex / 64
                let bitIndex = regionIndex % 64
                s.superBitmap[wordIndex] |= (1 << bitIndex)
            }
        }
    }

    // MARK: - Persistence

    /// Load the bitmap from a system page
    public func load(storageManager: StorageManager, bitmapPageID: Int) async throws {
        guard bitmapPageID != 0 else { return }
        let page = try await storageManager.readPage(pageID: bitmapPageID)
        guard let record = page.records.first else { return }

        state.withWriteLock { s in
            s.bitmap = record.data
            s.pageID = bitmapPageID
            s.dirty = false
            // Rebuild super-bitmap from loaded data
            Self.rebuildSuperBitmap(&s)
        }
    }

    /// Save the bitmap to a system page. Returns the page ID used.
    @discardableResult
    public func save(storageManager: StorageManager, bufferPool: BufferPoolManager, existingPageID: Int) async throws -> Int {
        let (bitmapData, isDirty) = state.withReadLock { s in
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

        state.withWriteLock { s in
            s.pageID = targetPageID
            s.dirty = false
        }

        return targetPageID
    }

    /// Whether the bitmap has unsaved changes
    public var isDirty: Bool {
        state.withReadLock { $0.dirty }
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

/// Read-write lock wrapper: multiple concurrent readers, exclusive writers.
/// Used by FreeSpaceBitmap so concurrent findPage/getCategory calls don't block each other.
public final class PantryRWLock<Value>: @unchecked Sendable {
    private var value: Value
    private var rwlock = pthread_rwlock_t()

    public init(_ value: Value) {
        self.value = value
        pthread_rwlock_init(&rwlock, nil)
    }

    deinit {
        pthread_rwlock_destroy(&rwlock)
    }

    /// Acquire shared read lock — multiple readers can proceed concurrently.
    public func withReadLock<T>(_ body: (Value) -> T) -> T {
        pthread_rwlock_rdlock(&rwlock)
        defer { pthread_rwlock_unlock(&rwlock) }
        return body(value)
    }

    /// Acquire exclusive write lock — blocks all other readers and writers.
    public func withWriteLock<T>(_ body: (inout Value) -> T) -> T {
        pthread_rwlock_wrlock(&rwlock)
        defer { pthread_rwlock_unlock(&rwlock) }
        return body(&value)
    }
}
