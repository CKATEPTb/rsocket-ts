/** Deterministic in-memory WebTransport session pair shared by package tests. */
import type {
    RSocketWebTransportBidirectionalStream,
    RSocketWebTransportCloseInfo,
    RSocketWebTransportDatagrams,
    RSocketWebTransportReadable,
    RSocketWebTransportReader,
    RSocketWebTransportSession,
    RSocketWebTransportWritable,
    RSocketWebTransportWriter
} from "rsocket-core-ts";

/** One byte write observed by the fake QUIC scheduler. */
export interface WebTransportTestWrite {
    /** Endpoint that initiated the write. */
    readonly side: "requester" | "responder";
    /** Native WebTransport channel carrying the bytes. */
    readonly kind: "bidi" | "uni" | "datagram";
    /** Stable synthetic stream id, or `-1` for datagrams. */
    readonly stream: number;
    /** Standalone bytes retained for assertions. */
    readonly bytes: Uint8Array;
    /** Whether this write has been delivered or intentionally dropped. */
    delivered: boolean;
    /** Delivers the write exactly once. */
    deliver(): void;
}

/** Controls delivery behavior of an in-memory WebTransport pair. */
export interface WebTransportTestPairOptions {
    /** Delivers writes in microtasks; disable it to reorder writes manually. */
    readonly autoFlush?: boolean;
    /** Returns true to drop one unreliable datagram. */
    readonly dropDatagram?: (write: WebTransportTestWrite) => boolean;
    /** Advertised maximum outgoing datagram size. */
    readonly maxDatagramSize?: number;
    /** Exposes the earlier `datagrams.writable` shape instead of `createWritable()`. */
    readonly writableDatagrams?: boolean;
}

/** One reliable stream allocation observed by the fake session. */
export interface WebTransportTestStreamRequest {
    /** Endpoint that requested the stream. */
    readonly side: "requester" | "responder";
    /** Directionality of the created stream. */
    readonly kind: "bidi" | "uni";
    /** Whether the mapping waits for peer stream credit. */
    readonly waitUntilAvailable: boolean;
}

/** Two connected sessions plus their deterministic write scheduler. */
export interface WebTransportTestPair {
    /** Requester-side session. */
    readonly requester: RSocketWebTransportSession;
    /** Responder-side session. */
    readonly responder: RSocketWebTransportSession;
    /** Every byte write in enqueue order. */
    readonly writes: WebTransportTestWrite[];
    /** Every reliable stream allocation in request order. */
    readonly streamRequests: WebTransportTestStreamRequest[];
    /** Delivers every pending write matching the optional predicate. */
    flush(predicate?: (write: WebTransportTestWrite) => boolean): void;
    /** Closes both sides and every active stream. */
    close(info?: RSocketWebTransportCloseInfo): void;
}

/** Creates two sessions whose stream APIs mirror the browser WebTransport surface. */
export function createWebTransportTestPair(
    options: WebTransportTestPairOptions = {}
): WebTransportTestPair {
    const writes: WebTransportTestWrite[] = [];
    const streamRequests: WebTransportTestStreamRequest[] = [];
    const requesterBidi = new AsyncQueue<RSocketWebTransportBidirectionalStream>();
    const responderBidi = new AsyncQueue<RSocketWebTransportBidirectionalStream>();
    const requesterUni = new AsyncQueue<RSocketWebTransportReadable<Uint8Array>>();
    const responderUni = new AsyncQueue<RSocketWebTransportReadable<Uint8Array>>();
    const requesterDatagrams = new AsyncQueue<Uint8Array>();
    const responderDatagrams = new AsyncQueue<Uint8Array>();
    const requesterClosed = deferred<RSocketWebTransportCloseInfo | undefined>();
    const responderClosed = deferred<RSocketWebTransportCloseInfo | undefined>();
    const byteQueues = new Set<AsyncQueue<Uint8Array>>();
    let nextStream = 0;
    let closed = false;

    const schedule = (
        side: "requester" | "responder",
        kind: WebTransportTestWrite["kind"],
        stream: number,
        bytes: Uint8Array,
        target: AsyncQueue<Uint8Array>
    ): void => {
        if (closed) throw new Error("WebTransport test session is closed");
        const standalone = new Uint8Array(bytes);
        const write: WebTransportTestWrite = {
            side,
            kind,
            stream,
            bytes: standalone,
            delivered: false,
            deliver() {
                if (write.delivered) return;
                write.delivered = true;
                if (kind !== "datagram" || options.dropDatagram?.(write) !== true) {
                    target.push(new Uint8Array(standalone));
                }
            }
        };
        writes.push(write);
        if (options.autoFlush !== false) queueMicrotask(() => write.deliver());
    };

    const close = (info?: RSocketWebTransportCloseInfo): void => {
        if (closed) return;
        closed = true;
        requesterBidi.complete();
        responderBidi.complete();
        requesterUni.complete();
        responderUni.complete();
        requesterDatagrams.complete();
        responderDatagrams.complete();
        for (const queue of byteQueues) queue.complete();
        byteQueues.clear();
        requesterClosed.resolve(info);
        responderClosed.resolve(info);
    };

    const session = (
        side: "requester" | "responder",
        localBidi: AsyncQueue<RSocketWebTransportBidirectionalStream>,
        remoteBidi: AsyncQueue<RSocketWebTransportBidirectionalStream>,
        localUni: AsyncQueue<RSocketWebTransportReadable<Uint8Array>>,
        remoteUni: AsyncQueue<RSocketWebTransportReadable<Uint8Array>>,
        localDatagrams: AsyncQueue<Uint8Array>,
        remoteDatagrams: AsyncQueue<Uint8Array>,
        closedPromise: Promise<RSocketWebTransportCloseInfo | undefined>
    ): RSocketWebTransportSession => {
        const peerSide = side === "requester" ? "responder" : "requester";
        const createDatagramWritable = () =>
            writable((bytes: Uint8Array) => schedule(side, "datagram", -1, bytes, remoteDatagrams));
        const datagrams: RSocketWebTransportDatagrams = {
            readable: localDatagrams,
            ...(options.writableDatagrams === true
                ? {writable: createDatagramWritable()}
                : {createWritable: createDatagramWritable}),
            ...(options.maxDatagramSize === undefined
                ? {}
                : options.writableDatagrams === true
                    ? {outgoingMaxDatagramSize: options.maxDatagramSize}
                    : {maxDatagramSize: options.maxDatagramSize})
        };
        return {
            ready: Promise.resolve(),
            closed: closedPromise,
            reliability: "supports-unreliable",
            incomingBidirectionalStreams: localBidi,
            incomingUnidirectionalStreams: localUni,
            datagrams,
            async createBidirectionalStream(streamOptions) {
                streamRequests.push({
                    side,
                    kind: "bidi",
                    waitUntilAvailable: streamOptions?.waitUntilAvailable === true
                });
                const stream = nextStream++;
                const localIncoming = new AsyncQueue<Uint8Array>();
                const remoteIncoming = new AsyncQueue<Uint8Array>();
                byteQueues.add(localIncoming);
                byteQueues.add(remoteIncoming);
                const local: RSocketWebTransportBidirectionalStream = {
                    readable: localIncoming,
                    writable: writable(
                        (bytes) => schedule(side, "bidi", stream, bytes, remoteIncoming),
                        () => remoteIncoming.complete()
                    )
                };
                const remote: RSocketWebTransportBidirectionalStream = {
                    readable: remoteIncoming,
                    writable: writable(
                        (bytes) => schedule(peerSide, "bidi", stream, bytes, localIncoming),
                        () => localIncoming.complete()
                    )
                };
                remoteBidi.push(remote);
                return local;
            },
            async createUnidirectionalStream(streamOptions) {
                streamRequests.push({
                    side,
                    kind: "uni",
                    waitUntilAvailable: streamOptions?.waitUntilAvailable === true
                });
                const stream = nextStream++;
                const incoming = new AsyncQueue<Uint8Array>();
                byteQueues.add(incoming);
                remoteUni.push(incoming);
                return writable(
                    (bytes) => schedule(side, "uni", stream, bytes, incoming),
                    () => incoming.complete()
                );
            },
            close
        };
    };

    const requester = session(
        "requester",
        requesterBidi,
        responderBidi,
        requesterUni,
        responderUni,
        requesterDatagrams,
        responderDatagrams,
        requesterClosed.promise
    );
    const responder = session(
        "responder",
        responderBidi,
        requesterBidi,
        responderUni,
        requesterUni,
        responderDatagrams,
        requesterDatagrams,
        responderClosed.promise
    );
    return {
        requester,
        responder,
        writes,
        streamRequests,
        flush(predicate = () => true) {
            for (const write of writes) if (!write.delivered && predicate(write)) write.deliver();
        },
        close
    };
}

/** Creates one lockable writable facade around synchronous queue operations. */
function writable<T>(
    writeValue: (value: T) => void,
    closeValue: () => void = () => undefined
): RSocketWebTransportWritable<T> {
    let locked = false;
    return {
        getWriter(): RSocketWebTransportWriter<T> {
            if (locked) throw new TypeError("WebTransport test writable is locked");
            locked = true;
            let released = false;
            return {
                write(value) {
                    if (released) throw new TypeError("WebTransport test writer is released");
                    writeValue(value);
                },
                close() {
                    if (released) return;
                    closeValue();
                },
                abort(reason) {
                    if (released) return;
                    closeValue();
                    void reason;
                },
                releaseLock() {
                    if (released) return;
                    released = true;
                    locked = false;
                }
            };
        }
    };
}

/** Single-reader asynchronous queue used for native stream and object delivery. */
class AsyncQueue<T> implements RSocketWebTransportReadable<T> {
    private readonly values: T[] = [];
    private readonly waiters: Array<(value: ReadableStreamReadResult<T>) => void> = [];
    private head = 0;
    private locked = false;
    private done = false;

    /** Pushes one value or resolves the oldest pending read. */
    push(value: T): void {
        if (this.done) return;
        const waiter = this.waiters.shift();
        if (waiter !== undefined) waiter({done: false, value});
        else this.values.push(value);
    }

    /** Completes pending and future reads. */
    complete(): void {
        if (this.done) return;
        this.done = true;
        const complete = {done: true, value: undefined} as ReadableStreamReadResult<T>;
        for (const waiter of this.waiters.splice(0)) waiter(complete);
        this.values.length = 0;
        this.head = 0;
    }

    /** Acquires the queue's only reader. */
    getReader(): RSocketWebTransportReader<T> {
        if (this.locked) throw new TypeError("WebTransport test readable is locked");
        this.locked = true;
        let released = false;
        return {
            read: () => {
                if (released) return Promise.reject(new TypeError("WebTransport test reader is released"));
                if (this.head < this.values.length) {
                    const value = this.values[this.head] as T;
                    this.head += 1;
                    if (this.head >= 64 && this.head * 2 >= this.values.length) {
                        this.values.splice(0, this.head);
                        this.head = 0;
                    }
                    return Promise.resolve({done: false, value});
                }
                if (this.done) {
                    return Promise.resolve({done: true, value: undefined} as ReadableStreamReadResult<T>);
                }
                return new Promise<ReadableStreamReadResult<T>>((resolve) => this.waiters.push(resolve));
            },
            cancel: () => {
                this.complete();
                return Promise.resolve();
            },
            releaseLock: () => {
                if (released) return;
                released = true;
                this.locked = false;
            }
        };
    }
}

/** Minimal promise deferred used for `WebTransport.closed`. */
function deferred<T>(): {
    readonly promise: Promise<T>;
    readonly resolve: (value: T) => void;
} {
    let resolve!: (value: T) => void;
    const promise = new Promise<T>((complete) => {
        resolve = complete;
    });
    return {promise, resolve};
}
