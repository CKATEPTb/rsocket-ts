/** Reentrancy-safe synchronous signal delivery shared by transport adapters. */

/**
 * Serializes nested synchronous signals while keeping the ordinary path
 * allocation-free. Queued values are snapshotted because their emitter may
 * mutate a buffer as soon as its nested callback returns.
 */
export class SerialSignalDispatcher<T> {
    private pending: T[] | undefined;
    private pendingHead = 0;
    private dispatching = false;

    /** Configures one stable consumer and a snapshot operation for nested values. */
    constructor(
        private readonly consumer: (value: T) => void,
        private readonly snapshot: (value: T) => T
    ) {
    }

    /** Whether a consumer callback is currently on the stack. */
    get isDispatching(): boolean {
        return this.dispatching;
    }

    /** Delivers one value now or queues it behind the active callback. */
    dispatch(value: T): void {
        if (this.dispatching) {
            (this.pending ??= []).push(this.snapshot(value));
            return;
        }
        this.dispatching = true;
        try {
            let current = value;
            while (true) {
                this.consumer(current);
                const pending = this.pending;
                if (pending === undefined || this.pendingHead >= pending.length) break;
                current = pending[this.pendingHead] as T;
                this.pendingHead += 1;
            }
        } finally {
            this.dispatching = false;
            this.clear();
        }
    }

    /** Releases nested values after cancellation or terminal failure. */
    clear(): void {
        this.pending = undefined;
        this.pendingHead = 0;
    }
}
