import Foundation

protocol CloudKitOperationScheduling {
    func enqueue(
        operation: Operation,
        deadline: DispatchTime,
        onOperationQueued: @escaping () -> Void
    )
}

final class DefaultCloudKitOperationScheduler: CloudKitOperationScheduling {
    private let operationQueue: OperationQueue = {
        let queue = OperationQueue()
        queue.maxConcurrentOperationCount = 1

        return queue
    }()

    func enqueue(
        operation: Operation,
        deadline: DispatchTime,
        onOperationQueued: @escaping () -> Void
    ) {
        DispatchQueue.main.asyncAfter(deadline: deadline) {
            self.operationQueue.addOperation(operation)
            self.operationQueue.addOperation {
                onOperationQueued()
            }
        }
    }
}
