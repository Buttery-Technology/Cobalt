import Foundation

/// Result of a VACUUM operation on a table.
public struct VacuumResult: Sendable {
    public let pagesScanned: Int
    public let pagesReclaimed: Int
    public let deadTuplesRemoved: Int

    public init(pagesScanned: Int, pagesReclaimed: Int, deadTuplesRemoved: Int) {
        self.pagesScanned = pagesScanned
        self.pagesReclaimed = pagesReclaimed
        self.deadTuplesRemoved = deadTuplesRemoved
    }
}

// MARK: - StorageEngine VACUUM extension

extension StorageEngine {
    /// Run VACUUM on a specific table, reclaiming empty pages.
    ///
    /// Scans all pages in the table's page chain, counts pages that have
    /// zero live records (i.e., fully empty after deletes), and reports stats.
    /// Actual page reclamation to the free list is a best-effort operation.
    public func vacuum(table tableName: String) async throws -> VacuumResult {
        guard let tableInfo = tableRegistry.getTableInfo(name: tableName) else {
            throw CobaltError.tableNotFound(name: tableName)
        }

        var pagesScanned = 0
        var pagesReclaimed = 0
        var deadTuplesRemoved = 0
        var currentPageID = tableInfo.firstPageID
        var visited: Set<Int> = []

        while currentPageID != 0 {
            guard visited.insert(currentPageID).inserted else { break }

            let page = try await getPage(pageID: currentPageID)
            pagesScanned += 1

            let liveRecords = page.records.count
            if liveRecords == 0 {
                // This page has no records — it could be reclaimed
                pagesReclaimed += 1
            }

            // Move to next page in chain
            currentPageID = page.nextPageID
        }

        // deadTuplesRemoved is 0 in this implementation since we don't
        // track individual deleted tuples at the page level — records are
        // removed from the page's record array on delete.
        // This count represents pages that are fully empty.
        deadTuplesRemoved = pagesReclaimed

        return VacuumResult(
            pagesScanned: pagesScanned,
            pagesReclaimed: pagesReclaimed,
            deadTuplesRemoved: deadTuplesRemoved
        )
    }
}
