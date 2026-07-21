/** Full request-channel lifecycle with independent request and response demand. */
import {FrameErrorCode, type PayloadFrame} from "rsocket-frames-ts";
import {
    decodeFramePayload,
    type PublisherInput,
    type RSocketPayloadFrame,
    type RSocketPayloadInput
} from "rsocket-core-ts";
import {RSocketRequestError} from "@/errors/index.js";
import type {ResponderSession, ResponderStream} from "@/session/types.js";
import {ChannelInput, type RSocketChannelRequests} from "@/stream/channel-input.js";
import {DemandControlledOutput} from "@/stream/output.js";

/** One bidirectional stream that remains registered until both halves complete. */
export class RequestChannelResponder implements ResponderStream {
    private readonly input: ChannelInput;
    private readonly output: DemandControlledOutput;
    private requesterComplete = false;
    private responderComplete = false;
    private ended = false;
    private inputStarted = false;

    /** Creates both channel directions without subscribing the handler yet. */
    constructor(
        private readonly session: ResponderSession,
        readonly streamId: number,
        initialRequestN: number,
        initial: RSocketPayloadFrame | undefined,
        requesterComplete: boolean
    ) {
        this.requesterComplete = requesterComplete;
        this.input = new ChannelInput(session, streamId, initial, requesterComplete, {
            complete: () => this.completeRequester(),
            error: (error) => this.fail(error),
            cancel: () => this.fail(new RSocketRequestError(
                "Request-channel input was cancelled by controller code",
                FrameErrorCode.CANCELED
            ))
        });
        this.output = new DemandControlledOutput(session, streamId, initialRequestN, {
            complete: () => this.completeResponder(),
            error: (error) => this.fail(error)
        });
    }

    /** Flux supplied to the declarative controller. */
    requests(): RSocketChannelRequests {
        return this.input.asFlux();
    }

    /** Whether either channel direction has already terminated the stream. */
    get isTerminated(): boolean {
        return this.ended;
    }

    /** Rejects post-completion or uncredited payload fragments before buffering. */
    acceptPayloadFragment(frame: PayloadFrame, continuation = false): boolean {
        if (this.ended || this.requesterComplete) return false;
        return this.input.acceptsPayloadFragment(frame, continuation);
    }

    /** Sends the mandatory initial continuation credit exactly once. */
    startInput(): void {
        if (this.inputStarted) return;
        this.inputStarted = true;
        this.input.start();
    }

    /** Starts the response publisher after this stream is registered. */
    start(source: PublisherInput<RSocketPayloadInput<any, any>>): void {
        if (this.ended) return;
        this.startInput();
        this.output.start(source);
    }

    /** Adds requester demand for the response direction. */
    handleRequestN(request: number): void {
        this.output.request(request);
    }

    /** Delivers one reassembled requester payload and optional half-close. */
    handlePayload(frame: PayloadFrame): void {
        if (this.ended || this.requesterComplete) return;
        if (frame.isNext()) this.input.next(decodeFramePayload(frame));
        if (frame.isComplete()) {
            this.requesterComplete = true;
            this.input.complete();
            this.completeIfDone();
        }
    }

    /** Terminates both directions when the requester sends ERROR. */
    handleError(error: unknown): void {
        this.terminate(error);
    }

    /** Terminates both directions when the requester sends CANCEL. */
    handleCancel(): void {
        this.terminate();
    }

    /** Releases both publisher directions and stream state. */
    terminate(error?: unknown): void {
        if (this.ended) return;
        this.ended = true;
        this.output.cancel();
        this.input.terminate(error);
        this.session.unregister(this.streamId);
    }

    /** Marks requester half-close and releases the stream when both halves ended. */
    private completeRequester(): void {
        this.requesterComplete = true;
        this.completeIfDone();
    }

    /** Marks responder half-close and releases the stream when both halves ended. */
    private completeResponder(): void {
        this.responderComplete = true;
        this.completeIfDone();
    }

    /** Finishes successful channels only after both COMPLETE signals. */
    private completeIfDone(): void {
        if (this.ended || !this.requesterComplete || !this.responderComplete) return;
        this.ended = true;
        this.session.unregister(this.streamId);
    }

    /** Sends one stream ERROR and cancels both local publishers. */
    private fail(error: unknown): void {
        if (this.ended) return;
        this.ended = true;
        this.output.cancel();
        this.input.terminate(error);
        this.session.streamError(this.streamId, error);
    }
}
