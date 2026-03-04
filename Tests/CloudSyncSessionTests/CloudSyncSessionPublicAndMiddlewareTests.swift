import CloudKit
@testable import CloudSyncSession
import Combine
import XCTest

private final class PublicAPIMockOperationHandler: OperationHandler {
    var fetchResult: Result<FetchOperation.Response, Error> = .success(
        FetchOperation.Response(changedRecords: [], deletedRecordIDs: [], hasMore: false)
    )
    var fetchRecordsResult: Result<FetchRecordsOperation.Response, Error> = .success(
        FetchRecordsOperation.Response(retrievedRecords: [], hasMore: false)
    )
    var fetchShareParticipantsResult: Result<FetchShareParticipantsOperation.Response, Error> = .success(
        FetchShareParticipantsOperation.Response(participants: [], hasMore: false)
    )
    var modifyResult: Result<ModifyOperation.Response, Error> = .success(
        ModifyOperation.Response(savedRecords: [], deletedRecordIDs: [])
    )
    var createZoneResult: Result<Bool, Error> = .success(true)
    var createSubscriptionResult: Result<Bool, Error> = .success(true)
    var leaveSharingResult: Result<Bool, Error> = .success(true)

    private(set) var fetchCallCount = 0
    private(set) var fetchRecordsCallCount = 0
    private(set) var fetchShareParticipantsCallCount = 0
    private(set) var modifyCallCount = 0
    private(set) var createZoneCallCount = 0
    private(set) var createSubscriptionCallCount = 0
    private(set) var leaveSharingCallCount = 0

    func handle(fetchOperation _: FetchOperation, completion: @escaping (Result<FetchOperation.Response, Error>) -> Void) {
        fetchCallCount += 1
        completion(fetchResult)
    }

    func handle(fetchRecordsOperation _: FetchRecordsOperation, completion: @escaping (Result<FetchRecordsOperation.Response, Error>) -> Void) {
        fetchRecordsCallCount += 1
        completion(fetchRecordsResult)
    }

    func handle(modifyOperation _: ModifyOperation, completion: @escaping (Result<ModifyOperation.Response, Error>) -> Void) {
        modifyCallCount += 1
        completion(modifyResult)
    }

    func handle(createZoneOperation _: CreateZoneOperation, completion: @escaping (Result<Bool, Error>) -> Void) {
        createZoneCallCount += 1
        completion(createZoneResult)
    }

    func handle(createSubscriptionOperation _: CreateSubscriptionOperation, completion: @escaping (Result<Bool, Error>) -> Void) {
        createSubscriptionCallCount += 1
        completion(createSubscriptionResult)
    }

    func handle(fetchShareParticipants _: FetchShareParticipantsOperation, completion: @escaping (Result<FetchShareParticipantsOperation.Response, Error>) -> Void) {
        fetchShareParticipantsCallCount += 1
        completion(fetchShareParticipantsResult)
    }

    func leaveSharing(completion: @escaping (Result<Bool, Error>) -> Void) {
        leaveSharingCallCount += 1
        completion(leaveSharingResult)
    }
}

private final class NoopOperationHandler: OperationHandler {
    func handle(fetchOperation _: FetchOperation, completion: @escaping (Result<FetchOperation.Response, Error>) -> Void) {
        completion(.success(FetchOperation.Response(changedRecords: [], deletedRecordIDs: [], hasMore: false)))
    }

    func handle(fetchRecordsOperation _: FetchRecordsOperation, completion: @escaping (Result<FetchRecordsOperation.Response, Error>) -> Void) {
        completion(.success(FetchRecordsOperation.Response(retrievedRecords: [], hasMore: false)))
    }

    func handle(modifyOperation _: ModifyOperation, completion: @escaping (Result<ModifyOperation.Response, Error>) -> Void) {
        completion(.success(ModifyOperation.Response(savedRecords: [], deletedRecordIDs: [])))
    }

    func handle(createZoneOperation _: CreateZoneOperation, completion: @escaping (Result<Bool, Error>) -> Void) {
        completion(.success(true))
    }

    func handle(createSubscriptionOperation _: CreateSubscriptionOperation, completion: @escaping (Result<Bool, Error>) -> Void) {
        completion(.success(true))
    }

    func handle(fetchShareParticipants _: FetchShareParticipantsOperation, completion: @escaping (Result<FetchShareParticipantsOperation.Response, Error>) -> Void) {
        completion(.success(FetchShareParticipantsOperation.Response(participants: [], hasMore: false)))
    }

    func leaveSharing(completion: @escaping (Result<Bool, Error>) -> Void) {
        completion(.success(true))
    }
}

private let publicTestZoneID = CKRecordZone.ID(zoneName: "public-tests", ownerName: CKCurrentUserDefaultName)

final class CloudSyncSessionPublicAndMiddlewareTests: XCTestCase {
    func testStartUnhaltsSession() {
        var tasks = Set<AnyCancellable>()
        let expectation = self.expectation(description: "session starts and unhalts")
        let handler = PublicAPIMockOperationHandler()
        let session = CloudSyncSession(
            operationHandler: handler,
            zoneID: publicTestZoneID,
            resolveConflict: { _, _ in nil },
            resolveExpiredChangeToken: { nil }
        )
        session.state = SyncState(isHalted: true)

        session.$state
            .sink { state in
                if !state.isHalted {
                    expectation.fulfill()
                }
            }
            .store(in: &tasks)

        session.start()

        wait(for: [expectation], timeout: 1)
    }

    func testStopHaltsSession() {
        var tasks = Set<AnyCancellable>()
        let expectation = self.expectation(description: "session halts")
        let handler = PublicAPIMockOperationHandler()
        let session = CloudSyncSession(
            operationHandler: handler,
            zoneID: publicTestZoneID,
            resolveConflict: { _, _ in nil },
            resolveExpiredChangeToken: { nil }
        )
        session.state = SyncState(
            hasGoodAccountStatus: true,
            hasCreatedZone: true,
            hasCreatedSubscription: true,
            isHalted: false
        )

        session.$state
            .sink { state in
                if state.isHalted {
                    expectation.fulfill()
                }
            }
            .store(in: &tasks)

        session.stop()

        wait(for: [expectation], timeout: 1)
    }

    func testResetClearsState() {
        let handler = PublicAPIMockOperationHandler()
        let session = CloudSyncSession(
            operationHandler: handler,
            zoneID: publicTestZoneID,
            resolveConflict: { _, _ in nil },
            resolveExpiredChangeToken: { nil }
        )
        session.state = SyncState(
            hasGoodAccountStatus: true,
            hasCreatedZone: true,
            hasCreatedSubscription: true,
            isHalted: true
        )

        session.reset()

        XCTAssertNil(session.state.hasGoodAccountStatus)
        XCTAssertNil(session.state.hasCreatedZone)
        XCTAssertNil(session.state.hasCreatedSubscription)
        XCTAssertFalse(session.state.isHalted)
    }

    func testFetchRecordsDispatchesToHandler() {
        let expectation = self.expectation(description: "fetch records called")
        let handler = PublicAPIMockOperationHandler()
        let session = CloudSyncSession(
            operationHandler: handler,
            zoneID: publicTestZoneID,
            resolveConflict: { _, _ in nil },
            resolveExpiredChangeToken: { nil }
        )
        session.state = SyncState(
            hasGoodAccountStatus: true,
            hasCreatedZone: true,
            hasCreatedSubscription: true
        )

        session.fetchRecords(FetchRecordsOperation(recordIDs: [CKRecord.ID(recordName: UUID().uuidString)]))

        DispatchQueue.main.asyncAfter(deadline: .now() + .milliseconds(300)) {
            XCTAssertEqual(handler.fetchRecordsCallCount, 1)
            expectation.fulfill()
        }

        wait(for: [expectation], timeout: 1)
    }

    func testFetchParticipantsDispatchesToHandler() {
        let expectation = self.expectation(description: "fetch participants called")
        let handler = PublicAPIMockOperationHandler()
        let session = CloudSyncSession(
            operationHandler: handler,
            zoneID: publicTestZoneID,
            resolveConflict: { _, _ in nil },
            resolveExpiredChangeToken: { nil }
        )
        session.state = SyncState(
            hasGoodAccountStatus: true,
            hasCreatedZone: true,
            hasCreatedSubscription: true
        )

        session.fetchParticipants(FetchShareParticipantsOperation())

        DispatchQueue.main.asyncAfter(deadline: .now() + .milliseconds(300)) {
            XCTAssertEqual(handler.fetchShareParticipantsCallCount, 1)
            expectation.fulfill()
        }

        wait(for: [expectation], timeout: 1)
    }

    func testFetchRecordsFailureHaltsSession() {
        var tasks = Set<AnyCancellable>()
        let expectation = self.expectation(description: "session halts after fetch records failure")
        let handler = PublicAPIMockOperationHandler()
        handler.fetchRecordsResult = .failure(CKError(.notAuthenticated))

        let session = CloudSyncSession(
            operationHandler: handler,
            zoneID: publicTestZoneID,
            resolveConflict: { _, _ in nil },
            resolveExpiredChangeToken: { nil }
        )
        session.state = SyncState(
            hasGoodAccountStatus: true,
            hasCreatedZone: true,
            hasCreatedSubscription: true
        )

        session.$state
            .sink { state in
                if state.isHalted {
                    expectation.fulfill()
                }
            }
            .store(in: &tasks)

        session.fetchRecords(FetchRecordsOperation(recordIDs: [CKRecord.ID(recordName: UUID().uuidString)]))

        wait(for: [expectation], timeout: 1)
    }

    func testFetchParticipantsFailureHaltsSession() {
        var tasks = Set<AnyCancellable>()
        let expectation = self.expectation(description: "session halts after fetch participants failure")
        let handler = PublicAPIMockOperationHandler()
        handler.fetchShareParticipantsResult = .failure(CKError(.notAuthenticated))

        let session = CloudSyncSession(
            operationHandler: handler,
            zoneID: publicTestZoneID,
            resolveConflict: { _, _ in nil },
            resolveExpiredChangeToken: { nil }
        )
        session.state = SyncState(
            hasGoodAccountStatus: true,
            hasCreatedZone: true,
            hasCreatedSubscription: true
        )

        session.$state
            .sink { state in
                if state.isHalted {
                    expectation.fulfill()
                }
            }
            .store(in: &tasks)

        session.fetchParticipants(FetchShareParticipantsOperation())

        wait(for: [expectation], timeout: 1)
    }

    func testLeaveSharingDelegatesToOperationHandler() {
        let expectation = self.expectation(description: "leave sharing completion")
        let handler = PublicAPIMockOperationHandler()
        let session = CloudSyncSession(
            operationHandler: handler,
            zoneID: publicTestZoneID,
            resolveConflict: { _, _ in nil },
            resolveExpiredChangeToken: { nil }
        )

        session.leaveSharing { result in
            switch result {
            case .success:
                break
            case let .failure(error):
                XCTFail("unexpected failure: \(error)")
            }
            expectation.fulfill()
        }

        wait(for: [expectation], timeout: 1)
        XCTAssertEqual(handler.leaveSharingCallCount, 1)
    }
}

final class AccountStatusMiddlewareTests: XCTestCase {
    func testStartLooksUpAccountStatusWhenUnknown() {
        var tasks = Set<AnyCancellable>()
        let expectation = self.expectation(description: "account status updated")
        let session = CloudSyncSession(
            operationHandler: NoopOperationHandler(),
            zoneID: publicTestZoneID,
            resolveConflict: { _, _ in nil },
            resolveExpiredChangeToken: { nil }
        )
        var lookupCount = 0
        let middleware = AccountStatusMiddleware(
            session: session,
            accountStatusLookup: { completion in
                lookupCount += 1
                completion(.available, nil)
            }
        )

        session.$state
            .sink { state in
                if state.hasGoodAccountStatus == true {
                    expectation.fulfill()
                }
            }
            .store(in: &tasks)

        _ = middleware.run(next: { $0 }, event: .start)

        wait(for: [expectation], timeout: 1)
        XCTAssertEqual(lookupCount, 1)
    }

    func testStartDoesNotLookupAccountStatusWhenKnown() {
        let session = CloudSyncSession(
            operationHandler: NoopOperationHandler(),
            zoneID: publicTestZoneID,
            resolveConflict: { _, _ in nil },
            resolveExpiredChangeToken: { nil }
        )
        session.state = SyncState(hasGoodAccountStatus: true)
        var lookupCount = 0
        let middleware = AccountStatusMiddleware(
            session: session,
            accountStatusLookup: { completion in
                lookupCount += 1
                completion(.available, nil)
            }
        )

        _ = middleware.run(next: { $0 }, event: .start)

        XCTAssertEqual(lookupCount, 0)
    }
}

final class ZoneMiddlewareTests: XCTestCase {
    func testStartQueuesZoneAndSubscriptionCreationWhenUnknown() {
        var tasks = Set<AnyCancellable>()
        let expectation = self.expectation(description: "zone and subscription queued")
        let session = CloudSyncSession(
            operationHandler: NoopOperationHandler(),
            zoneID: publicTestZoneID,
            resolveConflict: { _, _ in nil },
            resolveExpiredChangeToken: { nil }
        )
        let middleware = ZoneMiddleware(session: session)

        session.$state
            .sink { state in
                if state.createZoneQueue.count == 1 && state.createSubscriptionQueue.count == 1 {
                    expectation.fulfill()
                }
            }
            .store(in: &tasks)

        _ = middleware.run(next: { $0 }, event: .start)

        wait(for: [expectation], timeout: 1)
    }

    func testStartSkipsQueueWhenZoneAndSubscriptionAreKnown() {
        let session = CloudSyncSession(
            operationHandler: NoopOperationHandler(),
            zoneID: publicTestZoneID,
            resolveConflict: { _, _ in nil },
            resolveExpiredChangeToken: { nil }
        )
        session.state = SyncState(
            hasGoodAccountStatus: true,
            hasCreatedZone: true,
            hasCreatedSubscription: true
        )
        let middleware = ZoneMiddleware(session: session)

        _ = middleware.run(next: { $0 }, event: .start)

        XCTAssertTrue(session.state.createZoneQueue.isEmpty)
        XCTAssertTrue(session.state.createSubscriptionQueue.isEmpty)
    }
}
