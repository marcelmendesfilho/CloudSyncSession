import CloudKit
@testable import CloudSyncSession
import os.log
import XCTest

private final class CapturingOperationScheduler: CloudKitOperationScheduling {
    private(set) var queuedOperations = [Operation]()
    private(set) var deadlines = [DispatchTime]()

    func enqueue(
        operation: Operation,
        deadline: DispatchTime,
        onOperationQueued: @escaping () -> Void
    ) {
        queuedOperations.append(operation)
        deadlines.append(deadline)
        onOperationQueued()
    }
}

private let handlerTestZoneID = CKRecordZone.ID(zoneName: "handler-tests", ownerName: CKCurrentUserDefaultName)

final class CloudKitOperationHandlerTests: XCTestCase {
    private let log = OSLog(subsystem: "CloudSyncSession.Tests", category: "CloudKitOperationHandler")

    private func makeHandler(
        databaseScope: CKDatabase.Scope = .private,
        scheduler: CapturingOperationScheduler
    ) -> CloudKitOperationHandler {
        CloudKitOperationHandler(
            zoneID: handlerTestZoneID,
            subscriptionID: "test-subscription",
            log: log,
            operationScheduler: scheduler,
            now: DispatchTime.now,
            configureDatabaseOperation: { _ in },
            databaseScope: { databaseScope }
        )
    }

    func testModifySuccessDecreasesThrottleDuration() throws {
        let scheduler = CapturingOperationScheduler()
        let handler = makeHandler(scheduler: scheduler)
        handler.throttleDuration = 3
        let record = CKRecord(recordType: "T", recordID: CKRecord.ID(recordName: UUID().uuidString))
        let expectation = self.expectation(description: "modify completion")

        handler.handle(
            modifyOperation: ModifyOperation(records: [record], recordIDsToDelete: [], checkpointID: nil, userInfo: nil)
        ) { result in
            switch result {
            case let .success(response):
                XCTAssertEqual(response.savedRecords.count, 1)
            case let .failure(error):
                XCTFail("unexpected failure: \(error)")
            }

            expectation.fulfill()
        }

        let operation = try XCTUnwrap(scheduler.queuedOperations.first as? CKModifyRecordsOperation)
        operation.modifyRecordsCompletionBlock?([record], [], nil)

        wait(for: [expectation], timeout: 1)
        XCTAssertEqual(handler.throttleDuration, 2)
    }

    func testModifyFailureUsesRetryAfterToIncreaseThrottleDuration() throws {
        let scheduler = CapturingOperationScheduler()
        let handler = makeHandler(scheduler: scheduler)
        handler.throttleDuration = 2
        let record = CKRecord(recordType: "T", recordID: CKRecord.ID(recordName: UUID().uuidString))
        let expectation = self.expectation(description: "modify failure completion")
        let error = CKError(.networkFailure, userInfo: [CKErrorRetryAfterKey: NSNumber(value: 7)])

        handler.handle(
            modifyOperation: ModifyOperation(records: [record], recordIDsToDelete: [], checkpointID: nil, userInfo: nil)
        ) { result in
            switch result {
            case .success:
                XCTFail("expected failure")
            case let .failure(returnedError):
                XCTAssertNotNil(returnedError as? CKError)
            }

            expectation.fulfill()
        }

        let operation = try XCTUnwrap(scheduler.queuedOperations.first as? CKModifyRecordsOperation)
        operation.modifyRecordsCompletionBlock?(nil, nil, error)

        wait(for: [expectation], timeout: 1)
        XCTAssertEqual(handler.throttleDuration, 7)
    }

    func testFetchRecordsReturnsRetrievedRecords() throws {
        let scheduler = CapturingOperationScheduler()
        let handler = makeHandler(scheduler: scheduler)
        let recordID = CKRecord.ID(recordName: UUID().uuidString)
        let record = CKRecord(recordType: "T", recordID: recordID)
        let expectation = self.expectation(description: "fetch records completion")

        handler.handle(
            fetchRecordsOperation: FetchRecordsOperation(recordIDs: [recordID])
        ) { result in
            switch result {
            case let .success(response):
                XCTAssertEqual(response.retrievedRecords.count, 1)
                XCTAssertEqual(response.retrievedRecords.first?.recordID, recordID)
                XCTAssertFalse(response.hasMore)
            case let .failure(error):
                XCTFail("unexpected failure: \(error)")
            }

            expectation.fulfill()
        }

        let operation = try XCTUnwrap(scheduler.queuedOperations.first as? CKFetchRecordsOperation)
        operation.perRecordResultBlock?(recordID, .success(record))
        operation.fetchRecordsResultBlock?(.success(()))

        wait(for: [expectation], timeout: 1)
    }

    func testFetchShareParticipantsFailurePropagatesError() throws {
        let scheduler = CapturingOperationScheduler()
        let handler = makeHandler(scheduler: scheduler)
        let expectation = self.expectation(description: "fetch participants completion")
        let error = CKError(.networkFailure)

        handler.handle(fetchShareParticipants: FetchShareParticipantsOperation()) { result in
            switch result {
            case .success:
                XCTFail("expected failure")
            case let .failure(returnedError):
                XCTAssertNotNil(returnedError as? CKError)
            }

            expectation.fulfill()
        }

        let operation = try XCTUnwrap(scheduler.queuedOperations.first as? CKFetchShareParticipantsOperation)
        operation.fetchShareParticipantsResultBlock?(.failure(error))

        wait(for: [expectation], timeout: 1)
    }

    func testCreateZoneNotFoundCreatesZone() throws {
        let scheduler = CapturingOperationScheduler()
        let handler = makeHandler(scheduler: scheduler)
        let expectation = self.expectation(description: "create zone completion")

        handler.handle(createZoneOperation: CreateZoneOperation(zoneID: handlerTestZoneID)) { result in
            switch result {
            case .success:
                break
            case let .failure(error):
                XCTFail("unexpected failure: \(error)")
            }
            expectation.fulfill()
        }

        let checkOperation = try XCTUnwrap(scheduler.queuedOperations.first as? CKFetchRecordZonesOperation)
        checkOperation.fetchRecordZonesCompletionBlock?(nil, CKError(.zoneNotFound))

        let createOperation = try XCTUnwrap(scheduler.queuedOperations.last as? CKModifyRecordZonesOperation)
        createOperation.modifyRecordZonesCompletionBlock?(nil, nil, nil)

        wait(for: [expectation], timeout: 1)
    }

    func testCreateSubscriptionMissingCreatesSubscription() throws {
        let scheduler = CapturingOperationScheduler()
        let handler = makeHandler(scheduler: scheduler)
        let expectation = self.expectation(description: "create subscription completion")

        handler.handle(createSubscriptionOperation: CreateSubscriptionOperation(zoneID: handlerTestZoneID)) { result in
            switch result {
            case .success:
                break
            case let .failure(error):
                XCTFail("unexpected failure: \(error)")
            }
            expectation.fulfill()
        }

        let checkOperation = try XCTUnwrap(scheduler.queuedOperations.first as? CKFetchSubscriptionsOperation)
        checkOperation.fetchSubscriptionCompletionBlock?([:], nil)

        let createOperation = try XCTUnwrap(scheduler.queuedOperations.last as? CKModifySubscriptionsOperation)
        createOperation.modifySubscriptionsCompletionBlock?(nil, nil, nil)

        wait(for: [expectation], timeout: 1)
    }

    func testLeaveSharingFailsForNonSharedDatabase() {
        let scheduler = CapturingOperationScheduler()
        let handler = makeHandler(scheduler: scheduler)
        let expectation = self.expectation(description: "leave sharing failure")

        handler.leaveSharing { result in
            switch result {
            case .success:
                XCTFail("expected failure")
            case .failure:
                break
            }
            expectation.fulfill()
        }

        wait(for: [expectation], timeout: 1)
        XCTAssertTrue(scheduler.queuedOperations.isEmpty)
    }

    func testLeaveSharingCallsCompletionOnceOnSuccess() throws {
        let scheduler = CapturingOperationScheduler()
        let handler = makeHandler(databaseScope: .shared, scheduler: scheduler)
        let expectation = self.expectation(description: "leave sharing success")
        expectation.assertForOverFulfill = true
        var completionCallCount = 0
        let shareRecordID = CKRecord.ID(recordName: UUID().uuidString)
        let shareRecord = CKRecord(recordType: "cloudkit.share", recordID: shareRecordID)

        handler.leaveSharing { result in
            completionCallCount += 1
            switch result {
            case .success:
                break
            case let .failure(error):
                XCTFail("unexpected failure: \(error)")
            }
            expectation.fulfill()
        }

        let queryOperation = try XCTUnwrap(scheduler.queuedOperations.first as? CKQueryOperation)
        queryOperation.recordMatchedBlock?(shareRecordID, .success(shareRecord))
        queryOperation.queryResultBlock?(.success(nil))

        let modifyOperation = try XCTUnwrap(scheduler.queuedOperations.last as? CKModifyRecordsOperation)
        modifyOperation.modifyRecordsCompletionBlock?(nil, [shareRecordID], nil)

        wait(for: [expectation], timeout: 1)
        XCTAssertEqual(completionCallCount, 1)
    }

    func testDefaultOperationSchedulerEnqueuesAndExecutesOperation() {
        let scheduler = DefaultCloudKitOperationScheduler()
        let operationExecuted = expectation(description: "operation executed")
        let queuedCallbackCalled = expectation(description: "queued callback called")
        let operation = BlockOperation {
            operationExecuted.fulfill()
        }

        scheduler.enqueue(
            operation: operation,
            deadline: .now(),
            onOperationQueued: {
                queuedCallbackCalled.fulfill()
            }
        )

        wait(for: [operationExecuted, queuedCallbackCalled], timeout: 1)
    }
}
