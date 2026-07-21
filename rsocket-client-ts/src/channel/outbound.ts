/**
 * Request-channel outbound publisher implementation.
 *
 * RSocket request-channel is duplex: the requester sends payloads while also
 * receiving payloads. This class owns the requester-to-responder direction and
 * waits for responder `REQUEST_N` before sending any payload after the initial
 * `REQUEST_CHANNEL` frame.
 */
import {
    type Metadata,
    type MimeType,
    type Payload,
    PayloadFlag,
    PayloadFrame,
    RequestChannelFlag
} from "rsocket-frames-ts";
import {
    addReactiveDemand,
    encodePayloadInput,
    isPromiseLike,
    observeAbort,
    type RSocketPayloadInput
} from "rsocket-core-ts";
import {channelInputIterator, type RSocketChannelInputIterator} from "@/channel/input.js";
import {type OutboundChannel, type StreamSession} from "@/stream/index.js";
import type {RSocketChannelInput} from "@/client/types.js";

/**
 * Value that may already be available or may resolve from an async iterator.
 */
type MaybePromise<T> = T | PromiseLike<T>;

/**
 * Result produced by any supported request-channel iterator.
 */
type ChannelIteratorResult = IteratorResult<RSocketPayloadInput<any, any>>;

/** Allocates, registers, and sends the initial channel frame after its first item is ready. */
type ChannelRequestStarter = (
    flags: RequestChannelFlag,
    metadata?: Metadata<any>,
    payload?: Payload<any>
) => number;

/** Internal rejection used only to release a pending iterator read on cancellation. */
const CHANNEL_READ_ABORTED = Symbol("RSocket.channelReadAborted");
/** Shared rejection handler that isolates asynchronous iterator cleanup failures. */
const IGNORE_CHANNEL_CLEANUP_ERROR = (): void => undefined;

/**
 * Reads one iterator item without forcing synchronous iterables through `await`.
 */
function readChannelInput(
    iterator: RSocketChannelInputIterator<any, any>
): MaybePromise<ChannelIteratorResult> {
    return iterator.next() as MaybePromise<ChannelIteratorResult>;
}

/**
 * Makes an asynchronous iterator read cancellable without retaining the
 * channel coordinator when the source promise never settles.
 */
function awaitChannelInput<T>(value: PromiseLike<T>, signal: AbortSignal): Promise<T> {
    if (signal.aborted) return Promise.reject(CHANNEL_READ_ABORTED);
    return new Promise<T>((resolve, reject) => {
        let settled = false;
        let releaseAbort: (() => void) | undefined;
        const cleanup = (): void => {
            const release = releaseAbort;
            releaseAbort = undefined;
            release?.();
        };
        const finish = (callback: () => void): void => {
            if (settled) return;
            settled = true;
            cleanup();
            callback();
        };
        const onAbort = (): void => finish(() => reject(CHANNEL_READ_ABORTED));

        try {
            const release = observeAbort(signal, onAbort);
            if (settled) release();
            else releaseAbort = release;
            value.then(
                (result) => finish(() => resolve(result)),
                (error) => finish(() => reject(error))
            );
        } catch (error) {
            finish(() => reject(error));
        }
    });
}

/**
 * Sends request-channel outbound payloads while respecting responder demand.
 */
export class RequestChannelOutbound implements OutboundChannel {
    private demand = 0;
    private aborted = false;
    private streamId: number | undefined;
    private iterator: RSocketChannelInputIterator<any, any> | undefined;
    private iteratorClosed = false;
    /** Whether the peer has observed this stream's initial REQUEST_CHANNEL. */
    private requestStarted = false;
    private demandWaiter: (() => void) | undefined;
    private readonly suspensionAbortController = new AbortController();

    /**
     * Creates the outbound channel coordinator.
     */
    constructor(
        private readonly session: StreamSession,
        private readonly input: RSocketChannelInput,
        private readonly dataMimeType: MimeType<any>,
        private readonly metadataMimeType: MimeType<any>,
        private readonly startRequest: ChannelRequestStarter,
        private readonly onComplete: () => void,
        private readonly onError: (error: unknown, requestStarted: boolean) => void
    ) {
    }

    /**
     * Starts draining the configured publisher.
     */
    start(): void {
        void this.drain();
    }

    /**
     * Adds responder demand received through REQUEST_N.
     */
    addDemand(n: number): void {
        if (this.aborted || n <= 0) return;
        this.demand = addReactiveDemand(this.demand, n);
        this.wakeDemand();
    }

    /**
     * Cancels the outbound publisher and closes its iterator if possible.
     */
    abort(_error?: unknown): void {
        if (this.aborted) return;
        this.aborted = true;
        this.suspensionAbortController.abort();
        this.wakeDemand();
        this.closeIterator();
    }

    /**
     * Sends the initial frame, then waits for demand before sending more payloads.
     */
    private async drain(): Promise<void> {
        let iterator: RSocketChannelInputIterator<any, any> | undefined;
        const abortSignal = this.suspensionAbortController.signal;

        try {
            iterator = channelInputIterator(this.input);
            this.iterator = iterator;
            const initialWritable = this.waitUntilWritable();
            if (initialWritable !== undefined) await initialWritable;
            if (this.aborted) return;
            const firstRead = readChannelInput(iterator);
            const first = isPromiseLike(firstRead)
                ? await awaitChannelInput(firstRead, abortSignal)
                : firstRead;
            if (this.aborted) return;
            const firstWritable = this.waitUntilWritable();
            if (firstWritable !== undefined) await firstWritable;
            if (this.aborted) return;

            if (first.done) {
                this.iteratorClosed = true;
                this.streamId = this.startRequest(RequestChannelFlag.COMPLETE);
                this.requestStarted = true;
                if (this.aborted) return;
                this.onComplete();
                return;
            }

            const initial = encodePayloadInput(first.value, this.dataMimeType, this.metadataMimeType);
            this.streamId = this.startRequest(RequestChannelFlag.NONE, initial.metadata, initial.payload);
            this.requestStarted = true;
            if (this.aborted) return;

            let pending: MaybePromise<ChannelIteratorResult> | undefined;
            while (!this.aborted) {
                if (pending === undefined) {
                    if (this.demand <= 0) await this.awaitDemand();
                    if (this.aborted) return;
                    const readWritable = this.waitUntilWritable();
                    if (readWritable !== undefined) await readWritable;
                    if (this.aborted) return;
                    pending = readChannelInput(iterator);
                }

                const next = isPromiseLike(pending)
                    ? await awaitChannelInput(pending, abortSignal)
                    : pending;
                pending = undefined;
                if (this.aborted) return;

                if (next.done) {
                    this.iteratorClosed = true;
                    this.session.sendFrame(new PayloadFrame(this.requireStreamId(), PayloadFlag.COMPLETE));
                    if (this.aborted) return;
                    this.onComplete();
                    return;
                }

                if (this.demand <= 0) await this.awaitDemand();
                if (this.aborted) return;
                const sendWritable = this.waitUntilWritable();
                if (sendWritable !== undefined) await sendWritable;
                if (this.aborted) return;

                this.demand -= 1;
                const encoded = encodePayloadInput(next.value, this.dataMimeType, this.metadataMimeType);
                this.session.sendFrame(
                    new PayloadFrame(
                        this.requireStreamId(),
                        PayloadFlag.NEXT,
                        encoded.metadata,
                        encoded.payload
                    )
                );
                if (this.aborted) return;

                // One bounded look-ahead lets terminal completion pass without
                // consuming another wire credit; a value still waits for demand.
                if (this.demand <= 0) pending = readChannelInput(iterator);
            }
        } catch (error) {
            if (this.aborted) return;
            this.aborted = true;
            this.onError(error, this.requestStarted);
        } finally {
            if (iterator !== undefined) {
                if (this.iterator === iterator) this.iterator = undefined;
                this.closeIterator(iterator);
            }
        }
    }

    /**
     * Waits until responder demand is available or the channel is aborted.
     */
    private async awaitDemand(): Promise<void> {
        while (!this.aborted && this.demand <= 0) {
            await new Promise<void>((resolve) => {
                this.demandWaiter = resolve;
            });
        }
    }

    /** Pauses publisher reads while the logical session waits for Resume. */
    private waitUntilWritable(): Promise<void> | undefined {
        return this.session.waitUntilWritable(this.suspensionAbortController.signal);
    }

    /** Returns the ID assigned together with the initial REQUEST_CHANNEL frame. */
    private requireStreamId(): number {
        if (this.streamId !== undefined) return this.streamId;
        throw new Error("RSocket request-channel has not sent its initial frame");
    }

    /**
     * Wakes a pending demand waiter.
     */
    private wakeDemand(): void {
        const waiter = this.demandWaiter;
        this.demandWaiter = undefined;
        waiter?.();
    }

    /**
     * Calls `return()` on the async iterator to release publisher resources.
     */
    private closeIterator(iterator = this.iterator): void {
        if (iterator === undefined || this.iteratorClosed) return;
        this.iteratorClosed = true;
        try {
            const returned = iterator.return?.();
            if (returned !== undefined && isPromiseLike(returned)) {
                void Promise.resolve(returned).catch(IGNORE_CHANNEL_CLEANUP_ERROR);
            }
        } catch {
            // The stream is already being cancelled; iterator cleanup errors cannot be reported reliably.
        }
    }
}
