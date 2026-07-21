/**
 * Reactive Streams implementation for RSocket request-stream and request-channel
 * responses.
 */
import {Flux, type Subscriber, type Subscription} from "reactor-core-ts";
import {CancelFrame, ErrorFrame, type Frame, FrameErrorCode, PayloadFrame, RequestNFrame} from "rsocket-frames-ts";
import {
    addReactiveDemand,
    cancelSubscription,
    createPullAsyncIterable,
    decodeFramePayload,
    errorFromFrame,
    errorPayload,
    MAX_REQUEST_N,
    normalizeReactiveDemand,
    payloadHasMoreFragments,
    RSocketProtocolError,
    type RSocketPayloadFrame
} from "rsocket-core-ts";

/** No-op subscription used when stream setup fails before a real subscription exists. */
const EMPTY_SUBSCRIPTION: Subscription = Object.freeze({
    /** Ignores demand because the stream is already terminal. */
    request() {
    },
    /** Ignores cancellation because there is no upstream to cancel. */
    cancel() {
    }
});

/** Internal key used to access an RSocket Flux subscription without nesting another Reactor subscription. */
const DIRECT_SUBSCRIPTION = Symbol("RSocket.directSubscription");

/**
 * Optional hook for subscriptions that need to delay signals until after
 * `onSubscribe` has been delivered by `RSocketFlux`.
 */
interface AfterSubscribeSubscription extends Subscription {
    /** Called immediately after the downstream subscriber receives this subscription. */
    afterSubscribe(): void;
}

/**
 * Detects subscriptions with a post-`onSubscribe` hook.
 */
function hasAfterSubscribe(subscription: Subscription): subscription is AfterSubscribeSubscription {
    return typeof (subscription as Partial<AfterSubscribeSubscription>).afterSubscribe === "function";
}

/** Deferred source accepted by `RSocketFlux.deferSource(...)` without Promise thenable assimilation. */
type DeferredFluxSource<T extends RSocketPayloadFrame> = PromiseLike<{
    /** Concrete source resolved after an asynchronous readiness boundary. */
    readonly source: RSocketFlux<T>;
}>;

/** Breakable indirection preventing a pending source Promise from retaining a cancelled subscription. */
interface DeferredSubscriptionReference<T extends RSocketPayloadFrame> {
    current: DeferredRSocketFluxSubscription<T> | undefined;
}

/**
 * Minimal session operations needed by stream subscriptions.
 */
export interface StreamSession {
    /** Sends a frame through the active requester session. */
    sendFrame(frame: Frame): void;

    /** Sends an initial REQUEST frame while consuming one requester lease credit. */
    sendRequestFrame(frame: Frame): void;

    /** Removes a stream controller and any stored fragments. */
    unregisterStream(streamId: number): void;

    /** Closes the session because a protocol violation was detected. */
    protocolError(error: RSocketProtocolError): void;

    /** Waits until a suspended logical session can write through a resumed transport. */
    waitUntilWritable(abortSignal: AbortSignal): Promise<void> | undefined;
}

/**
 * Handles incoming frames for a single stream id.
 */
export interface StreamController {
    /** Client stream id owned by this controller. */
    readonly streamId: number;

    /** Checks one wire fragment before it can allocate reassembly storage. */
    acceptPayloadFragment(frame: PayloadFrame, continuation?: boolean): boolean;

    /** Handles a PAYLOAD frame for this stream. */
    handlePayload(frame: PayloadFrame): void;

    /** Handles an ERROR frame for this stream. */
    handleError(frame: ErrorFrame): void;

    /** Handles responder demand for request-channel outbound payloads. */
    handleRequestN(frame: RequestNFrame): void;

    /** Handles responder cancellation. */
    handleCancel(): void;

    /** Fails the stream and notifies the subscriber if appropriate. */
    fail(error: unknown): void;
}

/**
 * Outbound request-channel direction controlled by responder demand.
 */
export interface OutboundChannel {
    /** Adds demand received from responder REQUEST_N frames. */
    addDemand(n: number): void;

    /** Aborts the outbound publisher and releases resources. */
    abort(error?: unknown): void;
}

/**
 * Starts a stream after initial subscriber demand arrives.
 */
export type StartStream<D = unknown, M = unknown> = (
    initialRequestN: number,
    subscription: RSocketStreamSubscription<D, M>
) => void;

/**
 * `Flux` implementation that starts an RSocket stream only after demand.
 */
export class RSocketFlux<T extends RSocketPayloadFrame = RSocketPayloadFrame> extends Flux<T> {
    /**
     * Creates a Flux backed by an RSocket stream subscription factory.
     */
    constructor(private readonly subscriptionFactory: (subscriber: Subscriber<T>) => Subscription) {
        super((signal) => createAsyncIterable(signal, subscriptionFactory));
    }

    /**
     * Defers selection of an RSocket source while preserving synchronous
     * Reactive Streams `onSubscribe` delivery and accumulated demand.
     */
    static deferSource<T extends RSocketPayloadFrame = RSocketPayloadFrame>(
        sourceFactory: (signal: AbortSignal) => RSocketFlux<T> | DeferredFluxSource<T>
    ): RSocketFlux<T> {
        return new RSocketFlux<T>((subscriber) => {
            const abortController = new AbortController();
            let source: RSocketFlux<T> | DeferredFluxSource<T>;
            try {
                source = sourceFactory(abortController.signal);
            } catch (error) {
                abortController.abort();
                throw error;
            }
            if (source instanceof RSocketFlux) {
                abortController.abort();
                return source[DIRECT_SUBSCRIPTION](subscriber);
            }
            return new DeferredRSocketFluxSubscription(subscriber, source, abortController);
        });
    }

    /**
     * Subscribes a Reactive Streams subscriber to an RSocket stream.
     */
    protected override subscribeActual(subscriber: Subscriber<T>): void {
        let subscription: Subscription;
        try {
            subscription = this.subscriptionFactory(subscriber);
        } catch (error) {
            try {
                subscriber.onSubscribe(EMPTY_SUBSCRIPTION);
                subscriber.onError(error);
            } catch {
                // Subscriber callbacks are user code; setup has already failed.
            }
            return;
        }
        try {
            subscriber.onSubscribe(subscription);
        } catch {
            cancelSubscription(subscription);
            return;
        }
        if (hasAfterSubscribe(subscription)) subscription.afterSubscribe();
    }

    /**
     * Creates the underlying subscription for internal facade composition.
     */
    [DIRECT_SUBSCRIPTION](subscriber: Subscriber<T>): Subscription {
        return this.subscriptionFactory(subscriber);
    }
}

/** Bridges an asynchronously resolved source into one stable downstream subscription. */
class DeferredRSocketFluxSubscription<T extends RSocketPayloadFrame>
    implements AfterSubscribeSubscription {
    private upstream: Subscription | undefined;
    private pendingRequested = 0;
    private pendingTerminal: (() => void) | undefined;
    private subscribed = false;
    private cancelled = false;
    private terminal = false;
    private subscriber: Subscriber<T> | undefined;
    private sourceReference: DeferredSubscriptionReference<T> | undefined;
    /** Aborts asynchronous source resolution when this bridge no longer needs it. */
    private readinessAbortController: AbortController | undefined;

    /** Resolves and attaches the concrete stream source. */
    constructor(
        subscriber: Subscriber<T>,
        source: DeferredFluxSource<T>,
        readinessAbortController: AbortController
    ) {
        this.subscriber = subscriber;
        this.readinessAbortController = readinessAbortController;
        const reference: DeferredSubscriptionReference<T> = {current: this};
        this.sourceReference = reference;
        void Promise.resolve(source).then((resolved) => {
            const concrete = resolved.source;
            const current = reference.current;
            reference.current = undefined;
            current?.sourceResolved(reference, concrete);
        }).catch((error) => {
            const current = reference.current;
            reference.current = undefined;
            current?.sourceFailed(reference, error);
        });
    }

    /** Requests upstream immediately or accumulates demand until source resolution. */
    request(n: number): void {
        if (this.cancelled || this.terminal) return;
        if (this.upstream !== undefined) {
            try {
                this.upstream.request(n);
            } catch (error) {
                this.terminate(error, this.subscriber);
            }
            return;
        }
        try {
            this.pendingRequested = addReactiveDemand(this.pendingRequested, normalizeReactiveDemand(n));
        } catch (error) {
            this.fail(error);
        }
    }

    /** Cancels pending source resolution or the attached source. */
    cancel(): void {
        if (this.cancelled) return;
        this.cancelled = true;
        this.pendingRequested = 0;
        this.pendingTerminal = undefined;
        this.subscriber = undefined;
        this.releasePendingSource();
        const upstream = this.upstream;
        this.upstream = undefined;
        cancelSubscription(upstream);
    }

    /** Releases a terminal signal that raced with downstream `onSubscribe`. */
    afterSubscribe(): void {
        this.subscribed = true;
        const terminal = this.pendingTerminal;
        this.pendingTerminal = undefined;
        terminal?.();
    }

    /** Subscribes this bridge to the resolved source. */
    private subscribeTo(source: RSocketFlux<T>): void {
        if (this.cancelled || this.terminal) return;
        try {
            source.subscribe({
                onSubscribe: (subscription) => this.attach(subscription),
                onNext: (value) => this.next(value),
                onError: (error) => this.fail(error),
                onComplete: () => this.complete()
            });
        } catch (error) {
            this.fail(error);
        }
    }

    /** Attaches a source only if this subscription still owns the pending resolution. */
    private sourceResolved(reference: DeferredSubscriptionReference<T>, source: RSocketFlux<T>): void {
        if (this.sourceReference !== reference) return;
        this.releasePendingSource();
        this.subscribeTo(source);
    }

    /** Delivers a deferred source error only while the subscription is still active. */
    private sourceFailed(reference: DeferredSubscriptionReference<T>, error: unknown): void {
        if (this.sourceReference !== reference) return;
        this.releasePendingSource();
        this.fail(error);
    }

    /** Attaches upstream and replays demand accumulated during readiness. */
    private attach(subscription: Subscription): void {
        if (this.cancelled || this.terminal || this.upstream !== undefined) {
            cancelSubscription(subscription);
            return;
        }
        this.upstream = subscription;
        const pendingRequested = this.pendingRequested;
        this.pendingRequested = 0;
        if (pendingRequested <= 0) return;
        try {
            subscription.request(pendingRequested);
        } catch (error) {
            this.terminate(error, this.subscriber);
        }
    }

    /** Delivers one value while containing subscriber callback failures. */
    private next(value: T): void {
        if (this.cancelled || this.terminal) return;
        const subscriber = this.subscriber;
        if (subscriber === undefined) return;
        try {
            subscriber.onNext(value);
        } catch (error) {
            this.terminate(error, subscriber);
        }
    }

    /** Fails downstream exactly once. */
    private fail(error: unknown): void {
        if (this.cancelled || this.terminal) return;
        if (!this.subscribed) {
            this.pendingTerminal = () => this.fail(error);
            return;
        }
        this.terminate(error, this.subscriber);
    }

    /** Completes downstream exactly once. */
    private complete(): void {
        if (this.cancelled || this.terminal) return;
        if (!this.subscribed) {
            this.pendingTerminal = () => this.complete();
            return;
        }
        this.terminal = true;
        this.pendingRequested = 0;
        this.pendingTerminal = undefined;
        this.releasePendingSource();
        const subscriber = this.subscriber;
        this.subscriber = undefined;
        this.upstream = undefined;
        try {
            subscriber?.onComplete();
        } catch {
            // Completion callback failures cannot alter an already terminal stream.
        }
    }

    /** Cancels upstream and emits one terminal error. */
    private terminate(error: unknown, subscriber: Subscriber<T> | undefined): void {
        if (this.cancelled || this.terminal) return;
        this.terminal = true;
        this.cancelled = true;
        this.pendingRequested = 0;
        this.pendingTerminal = undefined;
        this.subscriber = undefined;
        this.releasePendingSource();
        const upstream = this.upstream;
        this.upstream = undefined;
        cancelSubscription(upstream);
        try {
            subscriber?.onError(error);
        } catch {
            // Subscriber callbacks are isolated from connection state.
        }
    }

    /** Removes the only strong link held by an unresolved source callback. */
    private releasePendingSource(): void {
        const reference = this.sourceReference;
        this.sourceReference = undefined;
        if (reference?.current === this) reference.current = undefined;
        this.readinessAbortController?.abort();
        this.readinessAbortController = undefined;
    }
}

/**
 * Bridges a Reactive Streams subscription into an async iterable.
 *
 * Each `next()` call requests one response payload, which keeps async iterator
 * consumption aligned with RSocket backpressure.
 */
function createAsyncIterable<T extends RSocketPayloadFrame>(
    signal: AbortSignal,
    subscriptionFactory: (subscriber: Subscriber<T>) => Subscription
): AsyncIterable<T> {
    return createPullAsyncIterable(signal, (subscriber) => {
        const subscription = subscriptionFactory(subscriber);
        subscriber.onSubscribe(subscription);
        if (hasAfterSubscribe(subscription)) subscription.afterSubscribe();
    });
}

/**
 * Subscription that maps Reactive Streams demand and cancellation to RSocket.
 */
export class RSocketStreamSubscription<D = unknown, M = unknown> implements Subscription, StreamController {
    private assignedStreamId: number | undefined;
    private started = false;
    private requestStarted = false;
    private cancelled = false;
    private responseTerminated = false;
    /** Suppresses demand re-entered from the NEXT callback of a final payload. */
    private completing = false;
    private outboundTerminated = true;
    private requested = 0;
    /** Response credits currently granted to the peer but not yet consumed. */
    private wireRequested = 0;
    private outbound: OutboundChannel | undefined;
    private disposed = false;
    private session: StreamSession | undefined;
    private startStream: StartStream<D, M> | undefined;
    /** Downstream callbacks released as soon as this subscription terminates. */
    private subscriber: Subscriber<RSocketPayloadFrame<D, M>> | undefined;

    /**
     * Creates a stream subscription for a specific client stream id.
     */
    constructor(
        session: StreamSession,
        subscriber: Subscriber<RSocketPayloadFrame<D, M>>,
        startStream: StartStream<D, M>
    ) {
        this.session = session;
        this.subscriber = subscriber;
        this.startStream = startStream;
    }

    /**
     * Client stream ID, or zero before initial demand allocates one.
     */
    get streamId(): number {
        return this.assignedStreamId ?? 0;
    }

    /** Rejects response payload fragments before buffering when no wire credit exists. */
    acceptPayloadFragment(frame: PayloadFrame, continuation = false): boolean {
        if (this.cancelled || this.responseTerminated) return false;
        if (continuation || (!frame.isNext() && !payloadHasMoreFragments(frame.header.flags)) ||
            (this.requested > 0 && this.wireRequested > 0)) return true;
        this.reportUndemandedPayload();
        return false;
    }

    /** Assigns the stream ID only after request preparation has succeeded. */
    assignStreamId(streamId: number): void {
        if (this.disposed || this.assignedStreamId !== undefined) {
            throw new RSocketProtocolError("RSocket stream ID is already assigned or the stream is disposed");
        }
        this.assignedStreamId = streamId;
    }

    /**
     * Attaches the outbound half of a request-channel interaction.
     */
    attachOutbound(outbound: OutboundChannel): void {
        this.outbound = outbound;
        this.outboundTerminated = false;
    }

    /** Marks the initial REQUEST frame as visible to the responder. */
    markRequestStarted(): void {
        if (this.disposed) return;
        this.requestStarted = true;
    }

    /** Grants demand accumulated while the initial REQUEST frame was being written. */
    flushInitialDemand(): void {
        if (this.disposed || this.responseTerminated) return;
        try {
            this.grantAvailableDemand();
        } catch (error) {
            this.fail(error);
        }
    }

    /** Restores pre-request state when initial serialization failed before completion. */
    markRequestUnsent(): void {
        this.requestStarted = false;
    }

    /**
     * Marks request-channel outbound publishing complete.
     */
    markOutboundComplete(): void {
        this.outbound = undefined;
        this.outboundTerminated = true;
        this.disposeIfDone();
    }

    /**
     * Requests more response payloads from the responder.
     */
    request(n: number): void {
        if (this.cancelled || this.responseTerminated || this.completing) return;

        try {
            this.requested = addReactiveDemand(this.requested, normalizeReactiveDemand(n));
            this.requestDemand();
        } catch (error) {
            this.failAndCancel(error);
        }
    }

    /**
     * Cancels the stream and sends CANCEL if the stream was already started.
     */
    cancel(): void {
        if (this.cancelled || (this.responseTerminated && this.outboundTerminated)) return;
        this.cancelled = true;
        this.subscriber = undefined;
        const outbound = this.outbound;
        this.outbound = undefined;
        outbound?.abort();
        if (this.requestStarted) {
            try {
                this.session?.sendFrame(new CancelFrame(this.streamId));
            } catch {
                // The stream is already locally cancelled; a closed socket cannot observe CANCEL.
            }
        }
        this.dispose();
    }

    /**
     * Delivers a PAYLOAD frame to the subscriber while enforcing demand.
     */
    handlePayload(frame: PayloadFrame): void {
        if (this.cancelled || this.responseTerminated) return;
        const hasFollows = frame.hasFollows();
        const isNext = frame.isNext();
        const isComplete = frame.isComplete();
        if (hasFollows) {
            this.failAndCancel(new RSocketProtocolError(
                "Unexpected fragmented PAYLOAD reached stream handler",
                {code: FrameErrorCode.INVALID, streamId: this.streamId}
            ));
            return;
        }
        if (!isNext && !isComplete) {
            this.failAndCancel(new RSocketProtocolError(
                "Responder sent PAYLOAD without NEXT or COMPLETE",
                {code: FrameErrorCode.INVALID, streamId: this.streamId}
            ));
            return;
        }

        this.completing = isComplete;
        if (isNext) {
            if (this.requested <= 0 || this.wireRequested <= 0) {
                this.reportUndemandedPayload();
                return;
            }
            if (this.requested !== Number.POSITIVE_INFINITY) this.requested -= 1;
            this.wireRequested -= 1;

            const subscriber = this.subscriber;
            if (subscriber === undefined) return;
            try {
                subscriber.onNext(decodeFramePayload<D, M>(frame));
            } catch (error) {
                this.cancel();
                signalSubscriberError(subscriber, error);
                return;
            }
            if (this.cancelled || this.responseTerminated) return;
        }

        if (isComplete) {
            this.completing = false;
            this.completeResponse();
            return;
        }

        if (isNext) {
            try {
                this.replenishDemand();
            } catch (error) {
                this.fail(error);
            }
        }

    }

    /**
     * Converts a stream ERROR frame to a subscriber error.
     */
    handleError(frame: ErrorFrame): void {
        this.fail(errorFromFrame(frame));
    }

    /**
     * Routes responder demand to the request-channel outbound publisher.
     */
    handleRequestN(frame: RequestNFrame): void {
        if (this.outbound === undefined) {
            return;
        }

        if (!Number.isInteger(frame.request) || frame.request <= 0 || frame.request > MAX_REQUEST_N) {
            this.session?.protocolError(
                new RSocketProtocolError("Responder sent invalid REQUEST_N", {
                    code: FrameErrorCode.CONNECTION_ERROR,
                    streamId: this.streamId
                })
            );
            return;
        }

        this.outbound.addDemand(frame.request);
    }

    /**
     * Ignores responder CANCEL on a requester-owned stream.
     */
    handleCancel(): void {
        // This side is the requester; an unexpected responder CANCEL is ignored.
    }

    /**
     * Fails the stream locally and notifies the subscriber once.
     */
    fail(error: unknown): void {
        if (this.cancelled) return;
        const shouldSignal = !this.responseTerminated;
        const subscriber = shouldSignal ? this.subscriber : undefined;
        this.subscriber = undefined;
        this.cancelled = true;
        const outbound = this.outbound;
        this.outbound = undefined;
        outbound?.abort(error);
        this.dispose();
        if (subscriber !== undefined) signalSubscriberError(subscriber, error);
    }

    /**
     * Fails request-channel locally and notifies the peer when its initial
     * REQUEST_CHANNEL was already sent.
     */
    failOutbound(error: unknown, notifyPeer = true): void {
        if (this.cancelled) return;
        const subscriber = this.responseTerminated ? undefined : this.subscriber;
        this.subscriber = undefined;
        this.cancelled = true;
        this.outbound = undefined;
        this.outboundTerminated = true;
        if (notifyPeer && this.requestStarted) {
            try {
                this.session?.sendFrame(new ErrorFrame(this.streamId, FrameErrorCode.APPLICATION_ERROR, errorPayload(error)));
            } catch {
                // The subscriber still needs the local failure even if the socket is closed.
            }
        }
        this.dispose();
        if (subscriber !== undefined) signalSubscriberError(subscriber, error);
    }

    /** Starts the stream or grants currently available downstream demand. */
    private requestDemand(): void {
        if (!this.started) {
            const startStream = this.startStream;
            if (startStream === undefined) return;
            this.started = true;
            this.startStream = undefined;
            const initialRequestN = Math.min(this.requested, MAX_REQUEST_N);
            this.wireRequested = initialRequestN;
            startStream(initialRequestN, this);
            return;
        }

        if (!this.requestStarted) return;
        this.grantAvailableDemand();
    }

    /** Grants at most one full protocol window without emitting millions of frames. */
    private grantAvailableDemand(): void {
        const session = this.session;
        if (session === undefined) return;
        const grant = Math.min(this.requested, MAX_REQUEST_N) - this.wireRequested;
        if (grant <= 0) return;
        this.wireRequested += grant;
        try {
            session.sendFrame(new RequestNFrame(this.streamId, grant));
        } catch (error) {
            this.wireRequested -= grant;
            throw error;
        }
    }

    /** Refills a large demand window only after half of its credits were consumed. */
    private replenishDemand(): void {
        if (this.wireRequested <= (MAX_REQUEST_N >>> 1)) this.grantAvailableDemand();
    }

    /** Terminates the connection when the responder exceeds granted response credits. */
    private reportUndemandedPayload(): void {
        this.session?.protocolError(
            new RSocketProtocolError("Responder sent PAYLOAD without requester demand", {
                code: FrameErrorCode.CONNECTION_ERROR,
                streamId: this.streamId
            })
        );
    }

    /** Terminates a malformed stream and cancels it when already known to the peer. */
    private failAndCancel(error: unknown): void {
        if (this.requestStarted) {
            try {
                this.session?.sendFrame(new CancelFrame(this.streamId));
            } catch {
                // Local termination still has to notify the subscriber when the transport is unavailable.
            }
        }
        this.fail(error);
    }

    /**
     * Completes the response half and disposes when outbound is also finished.
     */
    private completeResponse(): void {
        if (this.responseTerminated) return;
        this.responseTerminated = true;
        const subscriber = this.subscriber;
        this.subscriber = undefined;
        try {
            subscriber?.onComplete();
        } catch {
            // Subscriber terminal callbacks must not turn a completed stream into a protocol failure.
        } finally {
            this.disposeIfDone();
        }
    }

    /**
     * Disposes only when both response and outbound halves are done.
     */
    private disposeIfDone(): void {
        if (this.responseTerminated && this.outboundTerminated) this.dispose();
    }

    /**
     * Removes this stream from the owning session.
     */
    private dispose(): void {
        if (this.disposed) return;
        this.disposed = true;
        const session = this.session;
        this.session = undefined;
        this.startStream = undefined;
        this.outbound = undefined;
        this.subscriber = undefined;
        if (this.assignedStreamId !== undefined) session?.unregisterStream(this.assignedStreamId);
    }
}

/** Delivers a terminal error without allowing user code to escape dispatch. */
function signalSubscriberError<D, M>(subscriber: Subscriber<RSocketPayloadFrame<D, M>>, error: unknown): void {
    try {
        subscriber.onError(error);
    } catch {
        // Subscriber terminal callbacks are user code; the stream is already terminal.
    }
}
