import CloudKit
import os.log

struct ErrorMiddleware: Middleware {
    var session: CloudSyncSession

    private let log = OSLog(
        subsystem: "com.algebraiclabs.CloudSyncSession",
        category: "error middleware"
    )

    func run(next: (SyncEvent) -> SyncEvent, event: SyncEvent) -> SyncEvent {
        switch event {
        case .fetchFailure(let error, _):
            if let event = mapErrorToEvent(error: error) {
                return next(event)
            }

            return next(event)
        case .modifyFailure(let error, let operation):
            if let event = mapErrorToEvent(error: error, operation: operation) {
                return next(event)
            }

            return next(event)
        default:
            return next(event)
        }
    }

    func mapErrorToEvent(error: Error) -> SyncEvent? {
        if let ckError = error as? CKError {
            switch ckError.code {
            case .notAuthenticated,
                 .managedAccountRestricted,
                 .quotaExceeded,
                 .badDatabase,
                 .incompatibleVersion,
                 .permissionFailure,
                 .missingEntitlement,
                 .badContainer,
                 .constraintViolation,
                 .referenceViolation,
                 .invalidArguments,
                 .serverRejectedRequest,
                 .resultsTruncated,
                 .changeTokenExpired,
                 .batchRequestFailed:
                return .halt
            case .internalError,
                 .networkUnavailable,
                 .networkFailure,
                 .serviceUnavailable,
                 .zoneBusy,
                 .requestRateLimited:
                return .backoff
            case .serverResponseLost:
                return .retry
            case .partialFailure:
                return nil
            case .serverRecordChanged:
                return .conflict
            case .limitExceeded:
                return .splitThenRetry
            case .zoneNotFound, .userDeletedZone:
                return .createZone
            case .assetNotAvailable,
                 .assetFileNotFound,
                 .assetFileModified,
                 .participantMayNeedVerification,
                 .alreadyShared,
                 .tooManyParticipants,
                 .unknownItem,
                 .operationCancelled:
                return nil
            @unknown default:
                return nil
            }
        } else {
            return .halt
        }
    }

    func mapErrorToEvent(error: Error, operation: ModifyOperation) -> SyncEvent? {
        if let ckError = error as? CKError {
            switch ckError.code {
            case .partialFailure:
                guard let partialErrors = ckError.userInfo[CKPartialErrorsByItemIDKey] as? [CKRecord.ID: Error] else {
                    return .halt
                }

                let recordIDsNotSavedOrDeleted = partialErrors.keys

                let batchRequestFailedRecordIDs = partialErrors.filter({ (_, error) in
                    if let error = error as? CKError, error.code == .batchRequestFailed {
                        return true
                    }

                    return false
                }).keys

                let serverRecordChangedErrors = partialErrors.filter({ (_, error) in
                    if let error = error as? CKError, error.code == .serverRecordChanged {
                        return true
                    }

                    return false
                }).values

                let resolvedConflictsToSave = serverRecordChangedErrors.compactMap { error in
                    self.resolveConflict(error: error)
                }

                if resolvedConflictsToSave.count != serverRecordChangedErrors.count {
                    // If couldn't handle conflict for some of the records, abort
                    return .halt
                }

                let batchRequestRecordsToSave = operation.records.filter { record in
                    !resolvedConflictsToSave.map { $0.recordID }.contains(record.recordID)
                        && batchRequestFailedRecordIDs.contains(record.recordID)
                }
                //                    let failedRecordIDsToDelete = recordIDsToDelete.filter(recordIDsNotSavedOrDeleted.contains)

                let allResolvedRecordsToSave = batchRequestRecordsToSave + resolvedConflictsToSave

                guard !allResolvedRecordsToSave.isEmpty else {
                    return nil
                }

                return .resolveConflict(allResolvedRecordsToSave)
            default:
                if let error = mapErrorToEvent(error: error) {
                    return error
                }
            }
        }

        return .halt
    }

    func resolveConflict(error: Error) -> CKRecord? {
        guard let effectiveError = error as? CKError else {
            os_log(
                "resolveConflict called on an error that was not a CKError. The error was %{public}@",
                log: log,
                type: .fault,
                String(describing: self))


            return nil
        }

        guard effectiveError.code == .serverRecordChanged else {
            os_log(
                "resolveConflict called on a CKError that was not a serverRecordChanged error. The error was %{public}@",
                log: log,
                type: .fault,
                String(describing: effectiveError))

            return nil
        }

        guard let clientRecord = effectiveError.clientRecord else {
            os_log(
                "Failed to obtain client record from serverRecordChanged error. The error was %{public}@",
                log: log,
                type: .fault,
                String(describing: effectiveError))

            return nil
        }

        guard let serverRecord = effectiveError.serverRecord else {
            os_log(
                "Failed to obtain server record from serverRecordChanged error. The error was %{public}@",
                log: log,
                type: .fault,
                String(describing: effectiveError))

            return nil
        }

        os_log(
            "CloudKit conflict with record of type %{public}@. Running conflict resolver", log: log,
            type: .error, serverRecord.recordType)

        guard let resolveConflict = session.resolveConflict else {
            return nil
        }

        guard let resolvedRecord = resolveConflict(clientRecord, serverRecord) else {
            return nil
        }

        // Always return the server record so we don't end up in a conflict loop (the server record has the change tag we want to use)
        // https://developer.apple.com/documentation/cloudkit/ckerror/2325208-serverrecordchanged
        resolvedRecord.allKeys().forEach { serverRecord[$0] = resolvedRecord[$0] }

        return serverRecord
    }
}