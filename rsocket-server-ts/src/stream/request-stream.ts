/** Request-stream lifecycle built on demand-controlled controller output. */
import type {PayloadFrame} from "rsocket-frames-ts";
import type {PublisherInput, RSocketPayloadInput} from "rsocket-core-ts";
import type {ResponderSession, ResponderStream} from "@/session/types.js";
import {DemandControlledOutput, type OutputLifecycle} from "@/stream/output.js";

/** One active request-stream response publisher. */
export class RequestStreamResponder implements ResponderStream, OutputLifecycle {
    private readonly output: DemandControlledOutput;
    private ended = false;

    /** Creates a controller stream state before session registration. */
    constructor(
        private readonly session: ResponderSession,
        readonly streamId: number,
        initialRequest: number,
        private readonly source: PublisherInput<RSocketPayloadInput<any, any>>
    ) {
        this.output = new DemandControlledOutput(session, streamId, initialRequest, this);
    }

    /** Ignores requester PAYLOAD outside the request-stream sequence. */
    acceptPayloadFragment(): boolean {
        return false;
    }

    /** Subscribes the source after the session can route synchronous terminal signals. */
    start(): void {
        this.output.start(this.source);
    }

    /** Adds response demand from the requester. */
    handleRequestN(request: number): void {
        this.output.request(request);
    }

    /** Ignores requester payloads outside the request-stream sequence. */
    handlePayload(_frame: PayloadFrame): void {
    }

    /** Ignores requester errors because CANCEL is the request-stream terminal signal. */
    handleError(_error: unknown): void {
    }

    /** Cancels controller work without sending a terminal response. */
    handleCancel(): void {
        this.terminate();
    }

    /** Cancels and unregisters this stream exactly once. */
    terminate(_error?: unknown): void {
        if (this.ended) return;
        this.ended = true;
        this.output.cancel();
        this.session.unregister(this.streamId);
    }

    /** Releases state after a successful COMPLETE frame. */
    outputComplete(): void {
        if (this.ended) return;
        this.ended = true;
        this.session.unregister(this.streamId);
    }

    /** Converts local source failures into stream ERROR frames. */
    outputError(error: unknown): void {
        if (this.ended) return;
        this.ended = true;
        this.output.cancel();
        this.session.streamError(this.streamId, error);
    }
}
