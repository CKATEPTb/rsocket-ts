/** Temporary stream ownership while a synchronous controller selects its output source. */
import type {PayloadFrame} from "rsocket-frames-ts";
import {addReactiveDemand} from "rsocket-core-ts";
import type {ResponderStream} from "@/session/types.js";

/** Reserves a request stream so reentrant peer frames cannot bypass its lifecycle. */
export class PendingResponderStream implements ResponderStream {
    private demand = 0;
    private ended = false;

    /** Retains the stream ID and terminal cleanup until the concrete responder is ready. */
    constructor(
        readonly streamId: number,
        private readonly onTerminate: () => void
    ) {
    }

    /** Rejects payload buffering for interactions without a requester payload direction. */
    acceptPayloadFragment(): boolean {
        return false;
    }

    /** Accumulates response credits that arrive while controller code is on the stack. */
    handleRequestN(request: number): void {
        if (!this.ended) this.demand = addReactiveDemand(this.demand, request);
    }

    /** Ignores requester payloads that are not part of these interaction models. */
    handlePayload(_frame: PayloadFrame): void {
    }

    /** Ignores requester ERROR consistently with request-response and request-stream responders. */
    handleError(_error: unknown): void {
    }

    /** Applies cancellation before a controller result can be subscribed. */
    handleCancel(): void {
        this.terminate();
    }

    /** Releases the reserved stream exactly once. */
    terminate(_error?: unknown): void {
        if (this.ended) return;
        this.ended = true;
        this.demand = 0;
        this.onTerminate();
    }

    /** Transfers credits accumulated before the concrete stream was installed. */
    takeDemand(): number {
        const demand = this.demand;
        this.demand = 0;
        return demand;
    }
}
