/** Demand-controlled adaptation from controller publishers to PAYLOAD frames. */
import {Flux, type Subscriber, type Subscription} from "reactor-core-ts";
import {PayloadFlag, PayloadFrame} from "rsocket-frames-ts";
import {
    addReactiveDemand,
    cancelSubscription,
    isPublisher,
    type PublisherInput,
    type RSocketPayloadInput
} from "rsocket-core-ts";
import type {ResponderSession} from "@/session/types.js";

/** Lifecycle callbacks used by request-stream and request-channel owners. */
interface OutputLifecycle {
    /** Reports responder half-close. */
    complete(): void;
    /** Reports publisher or encoding failure. */
    error(error: unknown): void;
}

/** Subscriber that forwards only peer-authorized response elements. */
export class DemandControlledOutput implements Subscriber<RSocketPayloadInput<any, any>> {
    private subscription: Subscription | undefined;
    private demand: number;
    private pendingUpstreamDemand = 0;
    private requesting = false;
    private ended = false;

    /** Creates an output with demand carried by the initial request frame. */
    constructor(
        private readonly session: ResponderSession,
        private readonly streamId: number,
        initialDemand: number,
        private readonly lifecycle: OutputLifecycle
    ) {
        this.demand = initialDemand;
    }

    /** Subscribes the controller response source exactly once. */
    start(source: PublisherInput<RSocketPayloadInput<any, any>>): void {
        try {
            if (isPublisher(source)) source.subscribe(this);
            else Flux.from(source).subscribe(this);
        } catch (error) {
            this.fail(error);
        }
    }

    /** Stores upstream and forwards already accumulated peer demand. */
    onSubscribe(subscription: Subscription): void {
        if (this.ended || this.subscription !== undefined) {
            cancelSubscription(subscription);
            return;
        }
        this.subscription = subscription;
        if (this.demand > 0) this.requestUpstream(this.demand);
    }

    /** Encodes and writes one response while consuming one peer credit. */
    onNext(value: RSocketPayloadInput<any, any>): void {
        if (this.ended) return;
        if (this.subscription === undefined || this.demand <= 0) {
            this.fail(new Error(this.subscription === undefined
                ? "Controller publisher emitted before onSubscribe"
                : "Controller publisher emitted without requester demand"));
            return;
        }
        this.demand -= 1;
        try {
            const encoded = this.session.encode(value);
            this.session.send(new PayloadFrame(
                this.streamId,
                PayloadFlag.NEXT,
                encoded.metadata,
                encoded.payload
            ));
        } catch (error) {
            this.fail(error);
        }
    }

    /** Converts controller failure to a stream-scoped ERROR. */
    onError(error: unknown): void {
        if (!this.ended && this.subscription === undefined) {
            this.fail(new Error("Controller publisher failed before onSubscribe", {cause: error}));
            return;
        }
        this.fail(error);
    }

    /** Sends the responder half-close after all emitted payloads. */
    onComplete(): void {
        if (this.ended) return;
        if (this.subscription === undefined) {
            this.fail(new Error("Controller publisher completed before onSubscribe"));
            return;
        }
        this.ended = true;
        this.subscription = undefined;
        try {
            this.session.send(new PayloadFrame(this.streamId, PayloadFlag.COMPLETE));
            this.lifecycle.complete();
        } catch (error) {
            this.lifecycle.error(error);
        }
    }

    /** Adds cumulative client REQUEST_N credits and forwards them upstream. */
    request(request: number): void {
        if (this.ended) return;
        this.demand = addReactiveDemand(this.demand, request);
        this.requestUpstream(request);
    }

    /** Cancels controller work without emitting another terminal frame. */
    cancel(): void {
        if (this.ended) return;
        this.ended = true;
        this.pendingUpstreamDemand = 0;
        const subscription = this.subscription;
        this.subscription = undefined;
        cancelSubscription(subscription);
    }

    /** Whether the response direction already terminated. */
    get isComplete(): boolean {
        return this.ended;
    }

    /** Serializes reentrant peer demand before invoking the controller subscription. */
    private requestUpstream(request: number): void {
        const subscription = this.subscription;
        if (this.ended || subscription === undefined || request <= 0) return;
        this.pendingUpstreamDemand = addReactiveDemand(this.pendingUpstreamDemand, request);
        if (this.requesting) return;

        this.requesting = true;
        try {
            while (!this.ended && this.subscription === subscription && this.pendingUpstreamDemand > 0) {
                const pending = this.pendingUpstreamDemand;
                this.pendingUpstreamDemand = 0;
                subscription.request(pending);
            }
        } catch (error) {
            this.fail(error);
        } finally {
            this.requesting = false;
            if (this.ended) this.pendingUpstreamDemand = 0;
        }
    }

    /** Terminates upstream and delegates protocol error conversion to the owner. */
    private fail(error: unknown): void {
        if (this.ended) return;
        this.ended = true;
        this.pendingUpstreamDemand = 0;
        const subscription = this.subscription;
        this.subscription = undefined;
        cancelSubscription(subscription);
        this.lifecycle.error(error);
    }
}
