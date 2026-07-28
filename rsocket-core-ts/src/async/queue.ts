/** Allocation-conscious asynchronous FIFO used by endpoint adapters. */

/** Shared terminal iterator result reused after completion. */
const DONE: IteratorResult<never> = Object.freeze({done: true, value: undefined as never});
/** Shared resolved terminal promise reused by completed queues. */
const DONE_PROMISE = Promise.resolve(DONE);

/** Ordered single-consumer queue bridging callback signals to async iteration. */
export class AsyncQueue<T> implements AsyncIterable<T>, AsyncIterator<T> {
    private readonly values: Array<T | undefined> = [];
    private readonly waiters: Array<QueueWaiter<T> | undefined> = [];
    private readIndex = 0;
    private waiterIndex = 0;
    private closed = false;
    private failed = false;
    private failure: unknown;

    /** Delivers a value to the oldest waiter or appends it to the FIFO. */
    push(value: T): void {
        if (this.closed || this.failed) return;
        const waiter = this.nextWaiter();
        if (waiter !== undefined) waiter.resolve({done: false, value});
        else this.values.push(value);
    }

    /** Completes every pending and future read. */
    complete(): void {
        if (this.closed || this.failed) return;
        this.closed = true;
        this.flushTerminal();
    }

    /** Rejects every pending and future read with one failure. */
    error(error: unknown): void {
        if (this.closed || this.failed) return;
        this.failed = true;
        this.failure = error;
        this.flushTerminal();
    }

    /** Reads one queued value or waits for the next signal. */
    next(): Promise<IteratorResult<T>> {
        if (this.readIndex < this.values.length) {
            const value = this.values[this.readIndex] as T;
            this.values[this.readIndex] = undefined;
            this.readIndex += 1;
            this.compactValues();
            return Promise.resolve({done: false, value});
        }
        if (this.failed) return Promise.reject(this.failure);
        if (this.closed) return this.donePromise();
        return new Promise<IteratorResult<T>>((resolve, reject) => this.waiters.push({resolve, reject}));
    }

    /** Stops consumer-side iteration and releases queued references. */
    return(): Promise<IteratorResult<T>> {
        this.closed = true;
        this.failed = false;
        this.failure = undefined;
        this.values.length = 0;
        this.readIndex = 0;
        this.flushTerminal();
        return this.donePromise();
    }

    /** Returns this queue as its own async iterator. */
    [Symbol.asyncIterator](): AsyncIterator<T> {
        return this;
    }

    /** Resolves or rejects consumers already waiting for a value. */
    private flushTerminal(): void {
        let waiter: QueueWaiter<T> | undefined;
        while ((waiter = this.nextWaiter()) !== undefined) {
            if (this.failed) waiter.reject(this.failure);
            else waiter.resolve(this.done());
        }
        this.waiters.length = 0;
        this.waiterIndex = 0;
    }

    /** Releases consumed value slots without repeated array shifts. */
    private compactValues(): void {
        if (this.readIndex === this.values.length) {
            this.values.length = 0;
            this.readIndex = 0;
            return;
        }
        if (this.readIndex < 256 || this.readIndex * 2 < this.values.length) return;
        compactArray(this.values, this.readIndex);
        this.readIndex = 0;
    }

    /** Returns the oldest pending consumer in O(1) amortized time. */
    private nextWaiter(): QueueWaiter<T> | undefined {
        if (this.waiterIndex >= this.waiters.length) return undefined;
        const waiter = this.waiters[this.waiterIndex];
        this.waiters[this.waiterIndex] = undefined;
        this.waiterIndex += 1;
        this.compactWaiters();
        return waiter;
    }

    /** Releases consumed waiter slots without repeated array shifts. */
    private compactWaiters(): void {
        if (this.waiterIndex === this.waiters.length) {
            this.waiters.length = 0;
            this.waiterIndex = 0;
            return;
        }
        if (this.waiterIndex < 256 || this.waiterIndex * 2 < this.waiters.length) return;
        compactArray(this.waiters, this.waiterIndex);
        this.waiterIndex = 0;
    }

    /** Returns the shared terminal result with this queue's value type. */
    private done(): IteratorResult<T> {
        return DONE as IteratorResult<T>;
    }

    /** Returns the shared resolved terminal promise with this queue's value type. */
    private donePromise(): Promise<IteratorResult<T>> {
        return DONE_PROMISE as Promise<IteratorResult<T>>;
    }
}

/** One pending asynchronous queue read. */
interface QueueWaiter<T> {
    /** Resolves the pending read. */
    resolve(result: IteratorResult<T>): void;
    /** Rejects the pending read. */
    reject(error: unknown): void;
}

/** Removes an array's consumed prefix without allocating a splice result. */
function compactArray<T>(values: T[], consumed: number): void {
    const remaining = values.length - consumed;
    values.copyWithin(0, consumed);
    values.length = remaining;
}
