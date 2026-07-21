/** Request-response controller publisher adaptation. */
import {Mono, type Subscriber, type Subscription} from "reactor-core-ts";
import {PayloadFlag, PayloadFrame} from "rsocket-frames-ts";
import {cancelSubscription, isPromiseLike, isPublisher, type RSocketPayloadInput} from "rsocket-core-ts";
import type {RSocketHandlerResult} from "@/controllers/types.js";
import type {ResponderSession, ResponderStream} from "@/session/types.js";

/** Active request-response that permits exactly one controller value. */
export class RequestResponseResponder implements Subscriber<RSocketPayloadInput<any, any>>, ResponderStream {
    private subscription: Subscription | undefined;
    private ended = false;
    private emitted = false;
    private publisher = false;

    /** Creates one request-response state before it is inserted into the session map. */
    constructor(
        private readonly session: ResponderSession,
        readonly streamId: number,
        private readonly result: RSocketHandlerResult<RSocketPayloadInput<any, any> | undefined>
    ) {
    }

    /** Ignores requester PAYLOAD without allocating fragment storage. */
    acceptPayloadFragment(): boolean {
        return false;
    }

    /** Subscribes the handler result after stream registration is visible. */
    start(): void {
        if (this.result === undefined) {
            this.onComplete();
            return;
        }
        const publisher = isPublisher(this.result);
        if (!publisher && !isPromiseLike(this.result)) {
            this.onNext(this.result);
            return;
        }
        this.publisher = true;
        try {
            if (publisher) this.result.subscribe(this);
            else Mono.from(this.result).subscribe(this);
        } catch (error) {
            this.fail(error);
        }
    }

    /** Requests the only legal response value. */
    onSubscribe(subscription: Subscription): void {
        if (this.ended || this.subscription !== undefined) {
            cancelSubscription(subscription);
            return;
        }
        this.subscription = subscription;
        try {
            subscription.request(1);
        } catch (error) {
            this.onError(error);
        }
    }

    /** Sends one NEXT|COMPLETE response and cancels any overproducing source. */
    onNext(value: RSocketPayloadInput<any, any>): void {
        if (this.ended) return;
        if (this.publisher && this.subscription === undefined) {
            this.fail(new Error("Controller publisher emitted before onSubscribe"));
            return;
        }
        this.emitted = true;
        this.ended = true;
        const subscription = this.subscription;
        this.subscription = undefined;
        cancelSubscription(subscription);
        try {
            const encoded = this.session.encode(value);
            this.session.send(new PayloadFrame(
                this.streamId,
                PayloadFlag.NEXT | PayloadFlag.COMPLETE,
                encoded.metadata,
                encoded.payload
            ));
            this.session.unregister(this.streamId);
        } catch (error) {
            this.session.streamError(this.streamId, error);
        }
    }

    /** Sends an application error for a failed controller result. */
    onError(error: unknown): void {
        if (!this.ended && this.publisher && this.subscription === undefined) {
            this.fail(new Error("Controller publisher failed before onSubscribe", {cause: error}));
            return;
        }
        this.fail(error);
    }

    /** Sends an application error and releases the controller subscription. */
    private fail(error: unknown): void {
        if (this.ended) return;
        this.ended = true;
        const subscription = this.subscription;
        this.subscription = undefined;
        cancelSubscription(subscription);
        this.session.streamError(this.streamId, error);
    }

    /** Sends an empty COMPLETE when the handler emitted no response value. */
    onComplete(): void {
        if (this.ended || this.emitted) return;
        if (this.publisher && this.subscription === undefined) {
            this.fail(new Error("Controller publisher completed before onSubscribe"));
            return;
        }
        this.ended = true;
        this.subscription = undefined;
        try {
            this.session.send(new PayloadFrame(this.streamId, PayloadFlag.COMPLETE));
            this.session.unregister(this.streamId);
        } catch (error) {
            this.session.streamError(this.streamId, error);
        }
    }

    /** Ignores REQUEST_N because request-response has fixed response demand. */
    handleRequestN(): void {
    }

    /** Ignores requester PAYLOAD because it is not part of this interaction sequence. */
    handlePayload(): void {
    }

    /** Ignores requester ERROR because only CANCEL terminates request-response input. */
    handleError(_error: unknown): void {
    }

    /** Cancels controller work when the requester sends CANCEL. */
    handleCancel(): void {
        this.terminate();
    }

    /** Releases the publisher subscription and stream registration. */
    terminate(_error?: unknown): void {
        if (this.ended) return;
        this.ended = true;
        const subscription = this.subscription;
        this.subscription = undefined;
        cancelSubscription(subscription);
        this.session.unregister(this.streamId);
    }
}
