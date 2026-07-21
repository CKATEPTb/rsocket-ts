import {afterEach, describe, expect, it} from "vitest";
import type {Publisher, Subscriber, Subscription} from "reactor-core-ts";
import {
    CancelFrame,
    FrameFlag,
    FrameErrorCode,
    ErrorFrame,
    type Frame,
    RequestFireAndForgetFrame,
    RequestResponseFlag,
    RequestNFrame,
    RequestResponseFrame,
    WellKnownMimeType
} from "rsocket-frames-ts";
import {
    compositeMetadata,
    emitOutboundFrameFragments,
    errorPayload,
    outboundFrameLength,
    route
} from "rsocket-core-ts";
import {
    FireAndForgetController,
    RequestChannelController,
    RequestResponseController,
    RequestStreamController,
    RSocketRequestError
} from "@/index.js";
import {connectTestPair, type ConnectedTestPair} from "./helpers.js";
import {ManualPublisher} from "./manual-publisher.js";
import {waitFor} from "./wait.js";

class ThrowingController extends RequestResponseController<void, void> {
    protected readonly route = "throw";

    override handle(): never {
        throw new Error("handler failed");
    }
}

class RejectedController extends RequestResponseController<void, void> {
    protected readonly route = "reject";

    override handle(): never {
        throw new RSocketRequestError("request rejected", FrameErrorCode.REJECTED);
    }
}

const APPLICATION_DEFINED_ERROR = FrameErrorCode.fromByte(0x00000301);

class ApplicationDefinedErrorController extends RequestResponseController<void, void> {
    protected readonly route = "application-defined-error";

    override handle(): never {
        throw new RSocketRequestError("application-defined failure", APPLICATION_DEFINED_ERROR);
    }
}

class InvalidConnectionErrorController extends RequestResponseController<void, void> {
    protected readonly route = "invalid-connection-error";

    override handle(): never {
        throw new RSocketRequestError(
            "invalid connection error",
            FrameErrorCode.CONNECTION_ERROR
        );
    }
}

class PendingResponseController extends RequestResponseController<void, number> {
    protected readonly route = "pending-response";
    calls = 0;

    constructor(private readonly source: ManualPublisher<number>) {
        super();
    }

    override handle(): ManualPublisher<number> {
        this.calls += 1;
        return this.source;
    }
}

class ThrowingDemandPublisher implements Publisher<number> {
    cancelled = false;

    subscribe(subscriber: Subscriber<number>): void {
        subscriber.onSubscribe({
            request: () => {
                throw new Error("request failed");
            },
            cancel: () => {
                this.cancelled = true;
            }
        });
    }
}

/** Publisher whose cancellation violates Reactive Streams by throwing. */
class ThrowingCancelPublisher implements Publisher<number> {
    cancelCalls = 0;

    /** Attaches a permanently pending subscription with hostile cleanup. */
    subscribe(subscriber: Subscriber<number>): void {
        subscriber.onSubscribe({
            request: () => undefined,
            cancel: () => {
                this.cancelCalls += 1;
                throw new Error("cancel failed");
            }
        });
    }
}

/** Keeps one response active until the requester cancels it. */
class ThrowingCancelResponseController extends RequestResponseController<void, number> {
    protected readonly route = "throwing-cancel";

    /** Retains the supplied hostile publisher for each request. */
    constructor(private readonly source: ThrowingCancelPublisher) {
        super();
    }

    /** Starts response work that never emits by itself. */
    override handle(): Publisher<number> {
        return this.source;
    }
}

class FailedStreamController extends RequestStreamController<void, number> {
    protected readonly route = "failed-stream";

    constructor(private readonly source: ManualPublisher<number>) {
        super();
    }

    override handle(): ManualPublisher<number> {
        return this.source;
    }
}

class RecordingFireAndForgetController extends FireAndForgetController<string> {
    protected readonly route = "record-fragment";
    calls = 0;

    override handle(): void {
        this.calls += 1;
    }
}

/** Records whether a channel handler runs after its initial server credit is observed. */
class RecordingChannelController extends RequestChannelController<void, never> {
    protected readonly route = "record-channel";
    calls = 0;

    /** Returns an empty response sequence while recording controller dispatch. */
    override handle(): readonly never[] {
        this.calls += 1;
        return [];
    }
}

describe("server interaction errors", () => {
    let pair: ConnectedTestPair | undefined;

    afterEach(async () => {
        pair?.client.close();
        await pair?.server.close().block();
        pair = undefined;
    });

    it("returns REJECTED for an unregistered interaction route", async () => {
        pair = await connectTestPair([]);

        await expect(pair.client.requestResponse({metadata: route("missing")}).block())
            .rejects.toMatchObject({code: FrameErrorCode.REJECTED, streamId: 1});
        expect(pair.client.isClosed).toBe(false);
    });

    it("maps ordinary controller failures to APPLICATION_ERROR", async () => {
        pair = await connectTestPair([ThrowingController]);

        await expect(pair.client.requestResponse({metadata: route("throw")}).block())
            .rejects.toMatchObject({
                message: "handler failed",
                code: FrameErrorCode.APPLICATION_ERROR,
                streamId: 1
            });
        expect(pair.client.isClosed).toBe(false);
    });

    it("preserves an explicit legal stream error code", async () => {
        pair = await connectTestPair([RejectedController]);

        await expect(pair.client.requestResponse({metadata: route("reject")}).block())
            .rejects.toMatchObject({message: "request rejected", code: FrameErrorCode.REJECTED});
    });

    it("preserves application-defined stream error codes", async () => {
        pair = await connectTestPair([ApplicationDefinedErrorController]);

        await expect(pair.client.requestResponse({metadata: route("application-defined-error")}).block())
            .rejects.toMatchObject({message: "application-defined failure", code: APPLICATION_DEFINED_ERROR});
        expect(pair.client.isClosed).toBe(false);
    });

    it("contains an invalid connection-scoped controller error to its request stream", async () => {
        pair = await connectTestPair([InvalidConnectionErrorController]);

        await expect(pair.client.requestResponse({metadata: route("invalid-connection-error")}).block())
            .rejects.toMatchObject({
                message: "RSocketRequestError requires a stream-scoped error code",
                code: FrameErrorCode.APPLICATION_ERROR
            });
        expect(pair.client.isClosed).toBe(false);
    });

    it("forwards asynchronous request-stream failures and cancels state", async () => {
        const source = new ManualPublisher<number>();
        pair = await connectTestPair([new FailedStreamController(source)]);
        const result = pair.client.requestStream({metadata: route("failed-stream")}).toArray();
        await waitFor(() => source.requested > 0);

        source.error(new Error("stream failed"));

        await expect(result).rejects.toMatchObject({
            message: "stream failed",
            code: FrameErrorCode.APPLICATION_ERROR
        });
    });

    it("ignores REQUEST_N outside the request-response sequence", async () => {
        const source = new ManualPublisher<number>();
        pair = await connectTestPair([new PendingResponseController(source)]);
        const response = pair.client.requestResponse({metadata: route("pending-response")}).block();
        await waitFor(() => source.requested === 1);

        pair.clientTransport.write(new RequestNFrame(1, 1).toUint8Array());
        await Promise.resolve();

        expect(source.cancelled).toBe(false);
        expect(pair.client.isClosed).toBe(false);
        source.next(5);
        await expect(response).resolves.toMatchObject({data: 5});
    });

    it("cancels request-response publisher work when the client unsubscribes", async () => {
        const source = new ManualPublisher<number>();
        pair = await connectTestPair([new PendingResponseController(source)]);
        let subscription: Subscription | undefined;
        pair.client.requestResponse({metadata: route("pending-response")}).subscribe({
            onSubscribe(value) {
                subscription = value;
            },
            onNext() {
            },
            onError() {
            },
            onComplete() {
            }
        });
        await waitFor(() => source.requested === 1);

        subscription?.cancel();

        await waitFor(() => source.cancelled);
    });

    it("releases request state even when a controller subscription throws from cancel", async () => {
        const source = new ThrowingCancelPublisher();
        pair = await connectTestPair([new ThrowingCancelResponseController(source)]);
        let subscription: Subscription | undefined;
        pair.client.requestResponse({metadata: route("throwing-cancel")}).subscribe({
            onSubscribe(value) {
                subscription = value;
            },
            onNext() {
            },
            onError() {
            },
            onComplete() {
            }
        });
        await waitFor(() => activeServerStreams(pair!.server) === 1);

        subscription?.cancel();

        await waitFor(() => activeServerStreams(pair!.server) === 0);
        expect(source.cancelCalls).toBe(1);
        expect(pair.client.isClosed).toBe(false);
    });

    it("waits for active streams before applying CONNECTION_CLOSE", async () => {
        const source = new ManualPublisher<number>();
        pair = await connectTestPair([new PendingResponseController(source)]);
        const response = pair.client.requestResponse({metadata: route("pending-response")}).block();
        await waitFor(() => source.requested === 1);

        pair.clientTransport.write(new ErrorFrame(
            0,
            FrameErrorCode.CONNECTION_CLOSE,
            errorPayload("graceful close")
        ).toUint8Array());
        await Promise.resolve();

        expect(pair.serverTransport.isOpen).toBe(true);
        expect(source.cancelled).toBe(false);
        source.next(11);
        await expect(response).resolves.toMatchObject({data: 11});
        await waitFor(() => !pair!.serverTransport.isOpen);
    });

    it("finishes graceful close after a fragmented request is cancelled", async () => {
        pair = await connectTestPair([]);
        pair.clientTransport.write(new RequestResponseFrame(
            1,
            RequestResponseFlag.FOLLOWS,
            compositeMetadata(route("unfinished")),
            WellKnownMimeType.APPLICATION_OCTET_STREAM.toPayload(Uint8Array.of(1))
        ).toUint8Array());
        pair.clientTransport.write(new ErrorFrame(
            0,
            FrameErrorCode.CONNECTION_CLOSE,
            errorPayload("graceful close")
        ).toUint8Array());
        await Promise.resolve();

        expect(pair.serverTransport.isOpen).toBe(true);
        pair.clientTransport.write(new CancelFrame(1).toUint8Array());

        await waitFor(() => !pair!.serverTransport.isOpen);
    });

    it("finishes graceful close after a fragmented fire-and-forget completes", async () => {
        const controller = new RecordingFireAndForgetController();
        pair = await connectTestPair([controller]);
        const request = new RequestFireAndForgetFrame(
            1,
            FrameFlag.NONE,
            compositeMetadata(route("record-fragment")),
            WellKnownMimeType.APPLICATION_JSON.toPayload("fragmented-".repeat(40))
        );
        const length = outboundFrameLength(request);
        if (length === undefined) throw new Error("Expected a fragmentable fire-and-forget frame");
        const fragments: Frame[] = [];
        emitOutboundFrameFragments(request, length, 96, (frame) => fragments.push(frame));
        expect(fragments.length).toBeGreaterThan(1);

        pair.clientTransport.write((fragments[0] as Frame).toUint8Array());
        pair.clientTransport.write(new ErrorFrame(
            0,
            FrameErrorCode.CONNECTION_CLOSE,
            errorPayload("graceful close")
        ).toUint8Array());
        await Promise.resolve();
        expect(pair.serverTransport.isOpen).toBe(true);

        for (let index = 1; index < fragments.length; index += 1) {
            pair.clientTransport.write((fragments[index] as Frame).toUint8Array());
        }

        await waitFor(() => !pair!.serverTransport.isOpen);
        expect(controller.calls).toBe(1);
    });

    it("does not dispatch an interaction after an activity listener terminates the session", async () => {
        const controller = new RecordingFireAndForgetController();
        pair = await connectTestPair([controller], {
            activityListener: ({connection, direction, frame}) => {
                if (direction === "receive" && frame instanceof RequestFireAndForgetFrame) {
                    void connection.disconnect("stopped by activity listener").block();
                }
            }
        });

        await pair.client.fireAndForget({
            data: "ignored",
            metadata: route("record-fragment")
        }).block().catch(() => undefined);

        await waitFor(() => !pair!.serverTransport.isOpen);
        expect(controller.calls).toBe(0);
    });

    it("does not invoke a channel controller after initial credit activity terminates the session", async () => {
        const controller = new RecordingChannelController();
        pair = await connectTestPair([controller], {
            activityListener: ({connection, direction, frame}) => {
                if (direction === "send" && frame instanceof RequestNFrame) {
                    connection.disconnect("stopped before channel handler").subscribe();
                }
            }
        });

        await expect(pair.client.requestChannel([
            {metadata: route("record-channel")}
        ]).toArray()).rejects.toThrow("stopped before channel handler");

        expect(controller.calls).toBe(0);
    });

    it("ignores a repeated initial request on an active stream", async () => {
        const source = new ManualPublisher<number>();
        const controller = new PendingResponseController(source);
        pair = await connectTestPair([controller]);
        const response = pair.client.requestResponse({metadata: route("pending-response")}).block();
        await waitFor(() => source.requested === 1);

        pair.clientTransport.write(new RequestResponseFrame(
            1,
            FrameFlag.NONE,
            compositeMetadata(route("pending-response")),
            WellKnownMimeType.APPLICATION_OCTET_STREAM.toPayload(Uint8Array.of(0xff))
        ).toUint8Array());
        await Promise.resolve();

        expect(controller.calls).toBe(1);
        expect(pair.client.isClosed).toBe(false);
        source.next(7);
        await expect(response).resolves.toMatchObject({data: 7});
    });

    it("cancels request-response publishers whose demand operation fails", async () => {
        const source = new ThrowingDemandPublisher();
        class FailedDemandController extends RequestResponseController<void, number> {
            protected readonly route = "failed-demand";

            override handle(): Publisher<number> {
                return source;
            }
        }
        pair = await connectTestPair([FailedDemandController]);

        await expect(pair.client.requestResponse({metadata: route("failed-demand")}).block())
            .rejects.toMatchObject({message: "request failed", code: FrameErrorCode.APPLICATION_ERROR});
        expect(source.cancelled).toBe(true);
    });
});

/** Counts interaction states retained by the only test session. */
function activeServerStreams(server: ConnectedTestPair["server"]): number {
    const sessions = (server as unknown as {
        readonly sessions: Set<{readonly interactions: {readonly streams: Map<number, unknown>}}>;
    }).sessions;
    return [...sessions][0]?.interactions.streams.size ?? 0;
}
