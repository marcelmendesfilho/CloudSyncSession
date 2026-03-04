import CloudKit
import os.log

/// Middleware that looks up the account status when the session starts and dispatches `accountStatusChanged` events.
public struct AccountStatusMiddleware: Middleware {
    typealias AccountStatusLookup = (@escaping (CKAccountStatus, Error?) -> Void) -> Void

    public var session: CloudSyncSession
    let ckContainer: CKContainer?
    let accountStatusLookup: AccountStatusLookup

    private let log = OSLog(
        subsystem: "com.ryanashcraft.CloudSyncSession",
        category: "Account Status Middleware"
    )

    /**
     Creates an account status middleware struct, which should be appended to the chain of session middlewares.

     - Parameter session: The cloud sync session.
     - Parameter ckContainer: The CloudKit container.
     */
    public init(session: CloudSyncSession, ckContainer: CKContainer) {
        self.session = session
        self.ckContainer = ckContainer
        accountStatusLookup = { completion in
            ckContainer.accountStatus(completionHandler: completion)
        }
    }

    init(session: CloudSyncSession, accountStatusLookup: @escaping AccountStatusLookup) {
        self.session = session
        ckContainer = nil
        self.accountStatusLookup = accountStatusLookup
    }

    public func run(next: (SyncEvent) -> SyncEvent, event: SyncEvent) -> SyncEvent {
        switch event {
        case .start:
            if session.state.hasGoodAccountStatus == nil {
                accountStatusLookup { status, error in
                    if let error = error {
                        os_log("Failed to fetch account status: %{public}@", log: self.log, type: .error, String(describing: error))
                    }

                    self.session.dispatch(event: .accountStatusChanged(status))
                }
            }
        default:
            break
        }

        return next(event)
    }
}
