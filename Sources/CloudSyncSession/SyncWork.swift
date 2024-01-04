import CloudKit

public let maxRecommendedRecordsPerOperation = 400

public enum SyncWork: Identifiable {
    public enum Result {
        case modify(ModifyOperation.Response)
        case fetch(FetchOperation.Response)
        case fetchRecords(FetchRecordsOperation.Response)
        case fetchShareParticipants(FetchShareParticipantsOperation.Response)
        case createZone(Bool)
        case createSubscription(Bool)
    }

    case modify(ModifyOperation)
    case fetch(FetchOperation)
    case fetchRecords(FetchRecordsOperation)
    case fetchShareParticipants(FetchShareParticipantsOperation)
    case createZone(CreateZoneOperation)
    case createSubscription(CreateSubscriptionOperation)

    public var id: UUID {
        switch self {
        case let .modify(operation):
            return operation.id
        case let .fetch(operation):
            return operation.id
        case let .createZone(operation):
            return operation.id
        case let .createSubscription(operation):
            return operation.id
        case let .fetchRecords(operation):
            return operation.id
        case let .fetchShareParticipants(operation):
            return operation.id
        }
    }

    var retryCount: Int {
        switch self {
        case let .modify(operation):
            return operation.retryCount
        case let .fetch(operation):
            return operation.retryCount
        case let .createZone(operation):
            return operation.retryCount
        case let .createSubscription(operation):
            return operation.retryCount
        case let .fetchRecords(operation):
            return operation.retryCount
        case let .fetchShareParticipants(operation):
            return operation.retryCount
        }
    }

    var retried: SyncWork {
        switch self {
        case var .modify(operation):
            operation.retryCount += 1

            return .modify(operation)
        case var .fetch(operation):
            operation.retryCount += 1

            return .fetch(operation)
        case var .createZone(operation):
            operation.retryCount += 1

            return .createZone(operation)
        case var .createSubscription(operation):
            operation.retryCount += 1

            return .createSubscription(operation)
        case var .fetchRecords(operation):
            operation.retryCount += 1

            return .fetchRecords(operation)
            
        case var .fetchShareParticipants(operation):
            operation.retryCount += 1
            
            return .fetchShareParticipants(operation)
        }
    }

    var checkpointID: UUID? {
        switch self {
        case let .modify(operation):
            return operation.checkpointID
        default:
            return nil
        }
    }

    var debugDescription: String {
        switch self {
        case let .modify(operation):
            return "Modify with \(operation.records.count) records to save and \(operation.recordIDsToDelete.count) to delete"
        case .fetch:
            return "Fetch"
        case .createZone:
            return "Create zone"
        case .createSubscription:
            return "Create subscription"
        case .fetchRecords:
            return "Fetch records"
        case .fetchShareParticipants:
            return "Fetch share participants"
        }
    }
}

protocol SyncOperation {
    var retryCount: Int { get set }
}

public struct FetchRecordsOperation: Identifiable, SyncOperation {
    public struct Response {
        public let retrievedRecords: [CKRecord]
        public let hasMore: Bool

        public init(retrievedRecords: [CKRecord], hasMore: Bool) {
            self.retrievedRecords = retrievedRecords
            self.hasMore = hasMore
        }
    }

    public let id = UUID()

    public internal(set) var recordIDs: [CKRecord.ID] = []
    public internal(set) var retryCount: Int = 0

    public init(recordIDs: [CKRecord.ID]) {
        self.recordIDs = recordIDs
    }
}

public struct FetchShareParticipantsOperation: Identifiable, SyncOperation {
    public struct Response {
        public let participants: [CKShare.Participant]
        public let hasMore: Bool

        public init(participants: [CKShare.Participant], hasMore: Bool) {
            self.participants = participants
            self.hasMore = hasMore
        }
    }

    public let id = UUID()
    public internal(set) var retryCount: Int = 0
    public init() {}
}

public struct FetchOperation: Identifiable, SyncOperation {
    public struct Response {
        public let changeToken: CKServerChangeToken?
        public let changedRecords: [CKRecord]
        public let deletedRecordIDs: [CKRecord.ID]
        public let hasMore: Bool

        public init(changeToken: CKServerChangeToken? = nil, changedRecords: [CKRecord], deletedRecordIDs: [CKRecord.ID], hasMore: Bool) {
            self.changeToken = changeToken
            self.changedRecords = changedRecords
            self.deletedRecordIDs = deletedRecordIDs
            self.hasMore = hasMore
        }
    }

    public let id = UUID()

    public internal(set) var changeToken: CKServerChangeToken?
    public internal(set) var retryCount: Int = 0

    public init(changeToken: CKServerChangeToken?) {
        self.changeToken = changeToken
    }
}

public struct ModifyOperation: Identifiable, SyncOperation {
    public struct Response {
        public let savedRecords: [CKRecord]
        public let deletedRecordIDs: [CKRecord.ID]

        public init(savedRecords: [CKRecord], deletedRecordIDs: [CKRecord.ID]) {
            self.savedRecords = savedRecords
            self.deletedRecordIDs = deletedRecordIDs
        }
    }

    public let id = UUID()
    public let checkpointID: UUID?
    public let userInfo: [String: Any]?

    public internal(set) var records: [CKRecord]
    public internal(set) var recordIDsToDelete: [CKRecord.ID]
    public internal(set) var retryCount: Int = 0

    public init(records: [CKRecord], recordIDsToDelete: [CKRecord.ID], checkpointID: UUID?, userInfo: [String: Any]?) {
        self.records = records
        self.recordIDsToDelete = recordIDsToDelete
        self.checkpointID = checkpointID
        self.userInfo = userInfo
    }

    var shouldSplit: Bool {
        return records.count + recordIDsToDelete.count > maxRecommendedRecordsPerOperation
    }

    var split: [ModifyOperation] {
        let splitRecords: [[CKRecord]] = records.chunked(into: maxRecommendedRecordsPerOperation)
        let splitRecordIDsToDelete: [[CKRecord.ID]] = recordIDsToDelete.chunked(into: maxRecommendedRecordsPerOperation)

        return splitRecords.map { ModifyOperation(records: $0, recordIDsToDelete: [], checkpointID: nil, userInfo: userInfo) } +
            splitRecordIDsToDelete.enumerated().map { ModifyOperation(records: [], recordIDsToDelete: $0.element, checkpointID: $0.offset == splitRecordIDsToDelete.count - 1 ? checkpointID : nil, userInfo: userInfo) }
    }

    var splitInHalf: [ModifyOperation] {
        let firstHalfRecords = Array(records[0 ..< records.count / 2])
        let secondHalfRecords = Array(records[records.count / 2 ..< records.count])

        let firstHalfRecordIDsToDelete = Array(recordIDsToDelete[0 ..< recordIDsToDelete.count / 2])
        let secondHalfRecordIDsToDelete = Array(recordIDsToDelete[recordIDsToDelete.count / 2 ..< recordIDsToDelete.count])

        return [
            ModifyOperation(records: firstHalfRecords, recordIDsToDelete: firstHalfRecordIDsToDelete, checkpointID: nil, userInfo: userInfo),
            ModifyOperation(records: secondHalfRecords, recordIDsToDelete: secondHalfRecordIDsToDelete, checkpointID: checkpointID, userInfo: userInfo),
        ]
    }
}

public struct CreateZoneOperation: Identifiable, SyncOperation {
    public internal(set) var zoneID: CKRecordZone.ID
    public internal(set) var retryCount: Int = 0

    public let id = UUID()

    public init(zoneID: CKRecordZone.ID) {
        self.zoneID = zoneID
    }
}

public struct CreateSubscriptionOperation: Identifiable, SyncOperation {
    public internal(set) var zoneID: CKRecordZone.ID
    public internal(set) var retryCount: Int = 0

    public let id = UUID()

    public init(zoneID: CKRecordZone.ID) {
        self.zoneID = zoneID
    }
}

private extension Array {
    func chunked(into size: Int) -> [[Element]] {
        return stride(from: 0, to: count, by: size).map {
            Array(self[$0 ..< Swift.min($0 + size, count)])
        }
    }
}
