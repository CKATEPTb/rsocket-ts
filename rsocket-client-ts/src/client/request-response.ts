/**
 * Request-response stream controller used by one requester session.
 */
import {ErrorFrame, PayloadFrame, RequestNFrame} from "rsocket-frames-ts";
import {
    decodeFramePayload,
    errorFromFrame,
    RSocketProtocolError,
    type RSocketPayloadFrame
} from "rsocket-core-ts";
import type {StreamController, StreamSession} from "@/stream/index.js";

/**
 * Sink callbacks supplied by `Mono.create(...)`.
 */
export interface MonoRequestSink<D = unknown, M = unknown> {
    /** Resolves the request-response Mono with an optional payload. */
    success(value?: RSocketPayloadFrame<D, M>): void;

    /** Rejects the request-response Mono with a failure. */
    error(error: unknown): void;
}

/**
 * Stream controller for request-response interactions.
 *
 * Request-response is represented internally as a one-result stream so that the
 * main frame dispatcher can use the same stream registry for all interactions.
 */
export class MonoRequestController<D = unknown, M = unknown> implements StreamController {
    readonly streamId: number;
    private settled = false;

    /**
     * Creates a stream controller that resolves or rejects the supplied `Mono`
     * sink when the responder sends PAYLOAD, ERROR, or CANCEL.
     */
    constructor(
        private readonly session: StreamSession,
        streamId: number,
        private readonly sink: MonoRequestSink<D, M>
    ) {
        this.streamId = streamId;
    }

    /** Accepts the single response payload and any of its fragments. */
    acceptPayloadFragment(_frame: PayloadFrame): boolean {
        return true;
    }

    /**
     * Completes the request-response `Mono` from the first responder PAYLOAD.
     */
    handlePayload(frame: PayloadFrame): void {
        if (this.settled) return;
        if (frame.hasFollows()) {
            this.fail(new RSocketProtocolError("Unexpected fragmented PAYLOAD reached request-response handler", {streamId: this.streamId}));
            return;
        }
        // Handling-the-unexpected rules require request-response to assume COMPLETE.
        this.complete(frame.isNext() ? decodeFramePayload<D, M>(frame) : undefined);
    }

    /**
     * Converts an RSocket ERROR frame into a rejected `Mono`.
     */
    handleError(frame: ErrorFrame): void {
        this.fail(errorFromFrame(frame));
    }

    /**
     * Ignores unexpected REQUEST_N according to RSocket's lenient frame rules.
     */
    handleRequestN(_frame: RequestNFrame): void {
        // Request-response has no request publisher for responder demand.
    }

    /**
     * Ignores responder CANCEL because only this requester may cancel the stream.
     */
    handleCancel(): void {
        // Unexpected frames that do not alter the request-response sequence are ignored.
    }

    /**
     * Fails the interaction once and unregisters it from the session registry.
     */
    fail(error: unknown): void {
        if (this.settled) return;
        this.settled = true;
        this.session.unregisterStream(this.streamId);
        this.sink.error(error);
    }

    /**
     * Resolves the interaction once and unregisters it from the session registry.
     */
    private complete(value?: RSocketPayloadFrame<D, M>): void {
        this.settled = true;
        this.session.unregisterStream(this.streamId);
        this.sink.success(value);
    }
}
