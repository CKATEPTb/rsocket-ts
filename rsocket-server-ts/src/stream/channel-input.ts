/** Demand-aware requester-to-responder side of request-channel. */
import {Flux, type Subscriber, type Subscription} from "reactor-core-ts";
import {type PayloadFrame, RequestNFrame} from "rsocket-frames-ts";
import {
    addReactiveDemand,
    createPullAsyncIterable,
    MAX_REQUEST_N,
    normalizeReactiveDemand,
    payloadHasMoreFragments,
    RSocketProtocolError,
    type RSocketPayloadFrame
} from "rsocket-core-ts";
import type {ResponderSession} from "@/session/types.js";

const EMPTY_SUBSCRIPTION: Subscription = Object.freeze({request() {}, cancel() {}});

/** Owner callbacks for channel half-close and local cancellation. */
export interface ChannelInputLifecycle {
    /** Reports requester half-close. */
    inputComplete(): void;
    /** Reports invalid input or subscriber failure. */
    inputError(error: unknown): void;
    /** Reports local subscriber cancellation. */
    inputCancel(): void;
}

/** Flux facade preserving network demand for both subscribers and operators. */
export class RSocketChannelRequests extends Flux<RSocketPayloadFrame> {
    /** Creates a Flux over one single-subscriber inbound channel. */
    constructor(private readonly input: ChannelInput) {
        super((signal) => input.iterable(signal));
    }

    /** Attaches a direct Reactive Streams subscriber. */
    protected override subscribeActual(subscriber: Subscriber<RSocketPayloadFrame>): void {
        this.input.subscribe(subscriber);
    }
}

/** Single-consumer inbound channel with one-element prefetch. */
export class ChannelInput {
    private readonly pending: RSocketPayloadFrame[] = [];
    private pendingHead = 0;
    private subscriber: Subscriber<RSocketPayloadFrame> | undefined;
    private downstreamDemand = 0;
    private remoteCredits = 0;
    private requesterComplete = false;
    private terminated = false;
    private terminalError: unknown;
    private subscribed = false;
    private draining = false;
    private drainAgain = false;

    /** Creates an inbound channel before its owning stream is registered. */
    constructor(
        private readonly session: ResponderSession,
        private readonly streamId: number,
        initial: RSocketPayloadFrame | undefined,
        complete: boolean,
        private readonly lifecycle: ChannelInputLifecycle
    ) {
        if (initial !== undefined) this.pending.push(initial);
        this.requesterComplete = complete;
    }

    /** Grants the mandatory first continuation credit after stream registration. */
    start(): void {
        if (this.terminated) return;
        this.grant(1);
        this.drain();
    }

    /** Exposes the request direction to a declarative channel controller. */
    asFlux(): RSocketChannelRequests {
        return new RSocketChannelRequests(this);
    }

    /** Checks continuation credit before any fragmented NEXT payload is retained. */
    acceptsPayloadFragment(frame: PayloadFrame, continuation = false): boolean {
        if (this.terminated || this.requesterComplete) return false;
        if (continuation || (!frame.isNext() && !payloadHasMoreFragments(frame.header.flags)) ||
            this.remoteCredits > 0) return true;
        this.lifecycle.inputError(new RSocketProtocolError(
            "RSocket requester sent channel PAYLOAD without responder demand",
            {streamId: this.streamId}
        ));
        return false;
    }

    /** Attaches the only legal subscriber and replays queued/terminal state. */
    subscribe(subscriber: Subscriber<RSocketPayloadFrame>): void {
        if (this.subscribed) {
            try {
                subscriber.onSubscribe(EMPTY_SUBSCRIPTION);
                subscriber.onError(new RSocketProtocolError("RSocket request-channel input supports one subscriber"));
            } catch {
                // A rejected duplicate subscriber never owns protocol state.
            }
            return;
        }
        this.subscribed = true;
        if (this.terminated) {
            try {
                subscriber.onSubscribe(EMPTY_SUBSCRIPTION);
                if (this.terminalError === undefined) subscriber.onComplete();
                else subscriber.onError(this.terminalError);
            } catch {
                // Terminal state is already immutable and owns no protocol resources.
            }
            return;
        }
        this.subscriber = subscriber;
        try {
            subscriber.onSubscribe({
                request: (n) => this.request(n),
                cancel: () => this.cancel()
            });
        } catch (error) {
            this.subscriber = undefined;
            this.lifecycle.inputError(error);
            return;
        }
        this.drain();
    }

    /** Receives one reassembled NEXT payload after checking granted credit. */
    next(payload: RSocketPayloadFrame): void {
        if (this.terminated || this.requesterComplete) return;
        if (this.remoteCredits <= 0) {
            this.lifecycle.inputError(new RSocketProtocolError(
                "RSocket requester sent channel PAYLOAD without responder demand",
                {streamId: this.streamId}
            ));
            return;
        }
        this.remoteCredits -= 1;
        this.pending.push(payload);
        this.drain();
    }

    /** Marks the requester direction complete after pending values are consumed. */
    complete(): void {
        if (this.terminated || this.requesterComplete) return;
        this.requesterComplete = true;
        this.drain();
    }

    /** Terminates local subscribers without sending another protocol signal. */
    terminate(error?: unknown): void {
        if (this.terminated) return;
        this.terminated = true;
        this.terminalError = error;
        this.pending.length = 0;
        this.pendingHead = 0;
        this.downstreamDemand = 0;
        this.remoteCredits = 0;
        const subscriber = this.subscriber;
        this.subscriber = undefined;
        if (subscriber === undefined) return;
        try {
            if (error === undefined) subscriber.onComplete();
            else subscriber.onError(error);
        } catch {
            // User callbacks cannot alter an already terminal protocol stream.
        }
    }

    /** Creates a pull-based adapter used by Reactor operators. */
    iterable(signal: AbortSignal): AsyncIterable<RSocketPayloadFrame> {
        return createPullAsyncIterable(signal, (subscriber) => this.subscribe(subscriber));
    }

    /** Adds downstream demand, drains buffered values, then grants missing credits. */
    private request(value: number): void {
        if (this.terminated) return;
        let request: number;
        try {
            request = normalizeReactiveDemand(value);
        } catch (error) {
            this.lifecycle.inputError(error);
            return;
        }
        this.downstreamDemand = addReactiveDemand(this.downstreamDemand, request);
        this.drain();
    }

    /** Cancels the entire channel when controller code cancels its request input. */
    private cancel(): void {
        if (this.terminated) return;
        this.lifecycle.inputCancel();
    }

    /** Delivers queued values while demand exists and handles requester half-close. */
    private drain(): void {
        if (this.draining) {
            this.drainAgain = true;
            return;
        }
        this.draining = true;
        try {
            do {
                this.drainAgain = false;
                this.drainOnce();
            } while (this.drainAgain && !this.terminated);
        } finally {
            this.draining = false;
        }
    }

    /** Performs one serial delivery pass while reentrant work is coalesced. */
    private drainOnce(): void {
        const subscriber = this.subscriber;
        while (!this.terminated && subscriber !== undefined && this.downstreamDemand > 0 && this.pendingHead < this.pending.length) {
            const value = this.pending[this.pendingHead] as RSocketPayloadFrame;
            this.pendingHead += 1;
            this.downstreamDemand -= 1;
            try {
                subscriber.onNext(value);
            } catch (error) {
                this.lifecycle.inputError(error);
                return;
            }
        }
        this.compactPending();
        if (this.terminated) return;
        if (this.requesterComplete && this.pendingHead === this.pending.length) {
            this.terminated = true;
            this.terminalError = undefined;
            this.subscriber = undefined;
            try {
                subscriber?.onComplete();
            } catch {
                // Completion callback failures cannot alter a completed protocol half-stream.
            } finally {
                this.lifecycle.inputComplete();
            }
            return;
        }
        this.grantForDemand();
    }

    /** Maintains enough remote credit for outstanding downstream demand. */
    private grantForDemand(): void {
        if (this.requesterComplete || this.terminated) return;
        const queued = this.pending.length - this.pendingHead;
        const available = queued + this.remoteCredits;
        if (this.downstreamDemand === Number.POSITIVE_INFINITY) {
            if (available > (MAX_REQUEST_N >>> 1)) return;
            this.grant(MAX_REQUEST_N - available);
            return;
        }
        if (this.downstreamDemand <= available) return;
        this.grant(this.downstreamDemand - available);
    }

    /** Sends one or more bounded REQUEST_N frames and tracks their credits. */
    private grant(value: number): void {
        let remaining = value;
        while (!this.terminated && remaining > 0) {
            const request = Math.min(remaining, MAX_REQUEST_N - this.remoteCredits);
            if (request <= 0) return;
            this.remoteCredits += request;
            remaining -= request;
            try {
                this.session.send(new RequestNFrame(this.streamId, request));
            } catch (error) {
                this.lifecycle.inputError(error);
                return;
            }
        }
    }

    /** Releases consumed queue prefixes without repeated shifts. */
    private compactPending(): void {
        if (this.pendingHead === this.pending.length) {
            this.pending.length = 0;
            this.pendingHead = 0;
            return;
        }
        if (this.pendingHead < 64 || this.pendingHead * 2 < this.pending.length) return;
        this.pending.copyWithin(0, this.pendingHead);
        this.pending.length -= this.pendingHead;
        this.pendingHead = 0;
    }
}
