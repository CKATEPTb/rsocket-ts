import {afterEach, describe, expect, it} from "vitest";
import type {Publisher, Subscriber, Subscription} from "reactor-core-ts";
import type {RSocketPayloadFrame} from "rsocket-core-ts";
import {route} from "rsocket-core-ts";
import {FrameType, WellKnownMimeType} from "rsocket-frames-ts";
import {prependChannelPayload, RSocketClient} from "./client-engine.js";
import {RequestChannelController, RequestStreamController, RSocketServer} from "@/index.js";
import {connectTestPair, type ConnectedTestPair, testClientOptions} from "./helpers.js";
import {fireAndForgetValues, RecordController} from "./controllers.js";
import {ManualPublisher} from "./manual-publisher.js";
import {memoryTransportPair} from "./memory-transport.js";
import {nextTurn, waitFor} from "./wait.js";
import {EchoController} from "./controllers.js";

class ResumeStreamController extends RequestStreamController<void, number> {
    protected readonly route = "resume-stream";

    constructor(readonly source: Publisher<number>) {
        super();
    }

    override handle(): Publisher<number> {
        return this.source;
    }
}

/** Second independently routed stream used to verify replay ordering. */
class SecondResumeStreamController extends RequestStreamController<void, number> {
    protected readonly route = "resume-stream-two";

    constructor(readonly source: Publisher<number>) {
        super();
    }

    override handle(): Publisher<number> {
        return this.source;
    }
}

/** Echoes each resumed request-channel item through the response direction. */
class ResumeChannelController extends RequestChannelController<number, number> {
    protected readonly route = "resume-channel";

    override handle(requests: import("reactor-core-ts").Flux<RSocketPayloadFrame<number>>) {
        return requests.map(({data}) => data as number);
    }
}

/** Exposes a controllable text stream for fragmented replay tests. */
class ResumeTextStreamController extends RequestStreamController<void, string> {
    protected readonly route = "resume-text-stream";

    constructor(readonly source: Publisher<string>) {
        super();
    }

    override handle(): Publisher<string> {
        return this.source;
    }
}

/** Controllable source that can emit synchronously from a later request(n). */
class ResumeOrderingPublisher implements Publisher<number> {
    private subscriber: Subscriber<number> | undefined;
    private demand = 0;
    private nextRequestedValue: number | undefined;

    /** Current outstanding server-side response demand. */
    get requested(): number {
        return this.demand;
    }

    /** Installs one demand-aware subscriber. */
    subscribe(subscriber: Subscriber<number>): void {
        this.subscriber = subscriber;
        subscriber.onSubscribe({
            request: (value) => {
                this.demand += value;
                const next = this.nextRequestedValue;
                if (next === undefined) return;
                this.nextRequestedValue = undefined;
                this.next(next);
            },
            cancel: () => {
                this.subscriber = undefined;
            }
        });
    }

    /** Emits one manually controlled value under existing demand. */
    next(value: number): void {
        if (this.demand <= 0 || this.subscriber === undefined) throw new Error("No response demand");
        this.demand -= 1;
        this.subscriber.onNext(value);
    }

    /** Arms one value to be emitted synchronously by the next request(n). */
    emitOnRequest(value: number): void {
        this.nextRequestedValue = value;
    }
}

describe("server-side protocol Resume", () => {
    let pair: ConnectedTestPair | undefined;

    afterEach(async () => {
        pair?.client.abandonResume();
        await pair?.server.close().block();
        pair = undefined;
    });

    it("preserves active streams and replays responses produced while disconnected", async () => {
        const source = new ManualPublisher<number>();
        const controller = new ResumeStreamController(source);
        pair = await connectTestPair([controller], {resume: {ttlMs: 2_000}}, {resumeToken: "resume-1"});
        const values: number[] = [];
        let subscription: Subscription | undefined;
        pair.client.requestStream({metadata: route("resume-stream")}).subscribe({
            onSubscribe(value) {
                subscription = value;
                value.request(2);
            },
            onNext(value) {
                values.push(value.data as number);
            },
            onError(error) {
                throw error;
            },
            onComplete() {
            }
        } satisfies Subscriber<RSocketPayloadFrame>);

        await waitFor(() => source.requested >= 2);
        source.next(1);
        await waitFor(() => values.length === 1);
        expect(values).toEqual([1]);
        expect(pair.client.isClosed).toBe(false);
        pair.clientTransport.drop();
        await nextTurn();
        expect(pair.client.isSuspended).toBe(true);
        source.next(2);

        const replacement = memoryTransportPair();
        const accepted = pair.server.accept(replacement.server).block();
        const resumed = RSocketClient.resume({
            transport: () => replacement.client,
            setup: {
                keepAliveMs: 60_000,
                lifetimeMs: 60_000,
                metadataMimeType: WellKnownMimeType.MESSAGE_RSOCKET_COMPOSITE_METADATA,
                dataMimeType: WellKnownMimeType.APPLICATION_JSON,
                resumeToken: "resume-1"
            }
        }, {client: pair.client});
        const [serverConnection, client] = await Promise.all([accepted, resumed]);

        expect(serverConnection).toBe(pair.serverConnection);
        expect(client).toBe(pair.client);
        expect(values).toEqual([1, 2]);
        subscription?.cancel();
    });

    it("replays retained responses before processing client frames after RESUME_OK", async () => {
        const source = new ResumeOrderingPublisher();
        pair = await connectTestPair(
            [new ResumeStreamController(source)],
            {resume: {ttlMs: 2_000}},
            {resumeToken: "ordered-resume"}
        );
        const values: number[] = [];
        let subscription: Subscription | undefined;
        pair.client.requestStream({metadata: route("resume-stream")}).subscribe({
            onSubscribe(value) {
                subscription = value;
                value.request(2);
            },
            onNext(value) {
                values.push(value.data as number);
            },
            onError(error) {
                throw error;
            },
            onComplete() {
            }
        } satisfies Subscriber<RSocketPayloadFrame>);

        await waitFor(() => source.requested === 2);
        source.next(1);
        await waitFor(() => values.length === 1);
        pair.clientTransport.drop();
        await nextTurn();
        source.next(2);
        subscription?.request(1);
        source.emitOnRequest(3);

        const replacement = memoryTransportPair();
        const accepted = pair.server.accept(replacement.server).block();
        const resumed = RSocketClient.resume(
            testClientOptions(replacement.client, {resumeToken: "ordered-resume"}),
            {client: pair.client}
        );
        await Promise.all([accepted, resumed]);
        await waitFor(() => values.length === 3);

        expect(values).toEqual([1, 2, 3]);
        subscription?.cancel();
    });

    it("flushes responses produced reentrantly while retained frames are replayed", async () => {
        const source = new ResumeOrderingPublisher();
        let emitDuringReplay = false;
        pair = await connectTestPair(
            [new ResumeStreamController(source)],
            {
                resume: {ttlMs: 2_000},
                activityListener: ({direction, frame}) => {
                    if (!emitDuringReplay || direction !== "send" || frame.type !== FrameType.PAYLOAD) return;
                    emitDuringReplay = false;
                    source.next(3);
                }
            },
            {resumeToken: "reentrant-server-output"}
        );
        const values: number[] = [];
        let subscription: Subscription | undefined;
        pair.client.requestStream({metadata: route("resume-stream")}).subscribe({
            onSubscribe(value) {
                subscription = value;
                value.request(3);
            },
            onNext(value) {
                values.push(value.data as number);
            },
            onError(error) {
                throw error;
            },
            onComplete() {
            }
        } satisfies Subscriber<RSocketPayloadFrame>);

        await waitFor(() => source.requested === 3);
        source.next(1);
        await waitFor(() => values.length === 1);
        pair.clientTransport.drop();
        await waitFor(() => pair!.client.isSuspended);
        source.next(2);
        emitDuringReplay = true;

        const replacement = memoryTransportPair();
        const accepted = pair.server.accept(replacement.server).block();
        const resumed = RSocketClient.resume(
            testClientOptions(replacement.client, {resumeToken: "reentrant-server-output"}),
            {client: pair.client}
        );
        await Promise.all([accepted, resumed]);
        await waitFor(() => values.length === 3);

        expect(values).toEqual([1, 2, 3]);
        subscription?.cancel();
    });

    it("preserves client frame order when replay processing triggers reentrant writes", async () => {
        const first = new ResumeOrderingPublisher();
        const second = new ResumeOrderingPublisher();
        pair = await connectTestPair(
            [new ResumeStreamController(first), new SecondResumeStreamController(second)],
            {resume: {ttlMs: 2_000}},
            {resumeToken: "reentrant-resume"}
        );
        let firstSubscription: Subscription | undefined;
        let secondSubscription: Subscription | undefined;
        pair.client.requestStream({metadata: route("resume-stream")}).subscribe({
            onSubscribe(value) {
                firstSubscription = value;
                value.request(1);
            },
            onNext(value) {
                if (value.data === 2) secondSubscription?.cancel();
            },
            onError(error) {
                throw error;
            },
            onComplete() {
                }
        } satisfies Subscriber<RSocketPayloadFrame>);
        pair.client.requestStream({metadata: route("resume-stream-two")}).subscribe({
            onSubscribe(value) {
                secondSubscription = value;
                value.request(1);
            },
            onNext() {
            },
            onError(error) {
                throw error;
            },
            onComplete() {
            }
        } satisfies Subscriber<RSocketPayloadFrame>);
        await waitFor(() => first.requested === 1 && second.requested === 1);
        pair.clientTransport.drop();
        await waitFor(() => pair!.client.isSuspended);

        firstSubscription?.request(1);
        secondSubscription?.request(1);
        first.emitOnRequest(2);

        const replacement = memoryTransportPair();
        const accepted = pair.server.accept(replacement.server).block();
        const resumed = RSocketClient.resume(
            testClientOptions(replacement.client, {resumeToken: "reentrant-resume"}),
            {client: pair.client}
        );
        await Promise.all([accepted, resumed]);

        expect(second.requested).toBe(2);
        expect(pair.client.isClosed).toBe(false);
        firstSubscription?.cancel();
    });

    it("replays a request whose transport write was not delivered", async () => {
        pair = await connectTestPair(
            [EchoController],
            {resume: {ttlMs: 2_000}},
            {resumeToken: "request-replay"}
        );
        pair.clientTransport.dropBeforeDeliveryAfter(0);
        const response = pair.client.requestResponse({data: {id: 9}, metadata: route("echo")}).block();
        await waitFor(() => pair!.client.isSuspended);

        const replacement = memoryTransportPair();
        const accepted = pair.server.accept(replacement.server).block();
        const resumed = RSocketClient.resume(
            testClientOptions(replacement.client, {resumeToken: "request-replay"}),
            {client: pair.client}
        );
        await Promise.all([accepted, resumed]);

        await expect(response).resolves.toMatchObject({data: {id: 9}});
    });

    it("replays an undelivered fire-and-forget exactly once", async () => {
        fireAndForgetValues.length = 0;
        pair = await connectTestPair(
            [RecordController],
            {resume: {ttlMs: 2_000}},
            {resumeToken: "fnf-replay"}
        );
        pair.clientTransport.dropBeforeDeliveryAfter(0);

        await pair.client.fireAndForget({data: 42, metadata: route("record")}).block();
        await waitFor(() => pair!.client.isSuspended);
        expect(fireAndForgetValues).toEqual([]);

        const replacement = memoryTransportPair();
        const accepted = pair.server.accept(replacement.server).block();
        const resumed = RSocketClient.resume(
            testClientOptions(replacement.client, {resumeToken: "fnf-replay"}),
            {client: pair.client}
        );
        await Promise.all([accepted, resumed]);
        await waitFor(() => fireAndForgetValues.length === 1);

        expect(fireAndForgetValues).toEqual([42]);
    });

    it("preserves request-channel demand and queued input across Resume", async () => {
        const outbound = new ManualPublisher<{data: number}>();
        pair = await connectTestPair(
            [ResumeChannelController],
            {resume: {ttlMs: 2_000}},
            {resumeToken: "channel-resume"}
        );
        const values: number[] = [];
        let subscription: Subscription | undefined;
        pair.client.requestChannel(prependChannelPayload(
            {metadata: route("resume-channel")},
            outbound
        )).subscribe({
            onSubscribe(value) {
                subscription = value;
                value.request(2);
            },
            onNext(value) {
                values.push(value.data as number);
            },
            onError(error) {
                throw error;
            },
            onComplete() {
            }
        } satisfies Subscriber<RSocketPayloadFrame>);
        await waitFor(() => outbound.requested >= 1);
        outbound.next({data: 1});
        await waitFor(() => values.length === 1 && outbound.requested >= 2);

        pair.clientTransport.drop();
        await waitFor(() => pair!.client.isSuspended);
        outbound.next({data: 2});

        const replacement = memoryTransportPair();
        const accepted = pair.server.accept(replacement.server).block();
        const resumed = RSocketClient.resume(
            testClientOptions(replacement.client, {resumeToken: "channel-resume"}),
            {client: pair.client}
        );
        await Promise.all([accepted, resumed]);
        await waitFor(() => values.length === 2);

        expect(values).toEqual([1, 2]);
        subscription?.cancel();
    });

    it("reports a failed non-replayable connection frame while retaining Resume state", async () => {
        pair = await connectTestPair([], {resume: {ttlMs: 2_000}}, {resumeToken: "metadata-loss"});
        pair.serverTransport.dropBeforeDeliveryAfter(0);

        await expect(pair.serverConnection
            .metadataPush("authorization", WellKnownMimeType.TEXT_PLAIN)
            .block()).rejects.toThrow("outcome is unknown");

        await waitFor(() => pair!.client.isSuspended);
    });

    it("continues a fragmented request from the server's exact received position", async () => {
        pair = await connectTestPair(
            [EchoController],
            {resume: {ttlMs: 2_000}, maxFrameLength: 128},
            {resumeToken: "fragment-replay", maxFrameLength: 128}
        );
        pair.clientTransport.dropBeforeDeliveryAfter(1);
        const data = {value: "fragment-".repeat(200)};
        const response = pair.client.requestResponse({data, metadata: route("echo")}).block();
        await waitFor(() => pair!.client.isSuspended);

        const replacement = memoryTransportPair();
        const accepted = pair.server.accept(replacement.server).block();
        const resumed = RSocketClient.resume(
            testClientOptions(replacement.client, {
                resumeToken: "fragment-replay",
                maxFrameLength: 128
            }),
            {client: pair.client}
        );
        await Promise.all([accepted, resumed]);

        await expect(response).resolves.toMatchObject({data});
    });

    it("replays every fragment of a response produced while disconnected", async () => {
        const source = new ManualPublisher<string>();
        pair = await connectTestPair(
            [new ResumeTextStreamController(source)],
            {resume: {ttlMs: 2_000}, maxFrameLength: 128},
            {resumeToken: "response-fragments", maxFrameLength: 128}
        );
        const values: string[] = [];
        let subscription: Subscription | undefined;
        pair.client.requestStream({metadata: route("resume-text-stream")}).subscribe({
            onSubscribe(value) {
                subscription = value;
                value.request(1);
            },
            onNext(value) {
                values.push(value.data as string);
            },
            onError(error) {
                throw error;
            },
            onComplete() {
            }
        } satisfies Subscriber<RSocketPayloadFrame>);
        await waitFor(() => source.requested === 1);
        pair.clientTransport.drop();
        await waitFor(() => pair!.client.isSuspended);
        const expected = "fragmented-response-".repeat(100);
        source.next(expected);

        const replacement = memoryTransportPair();
        const accepted = pair.server.accept(replacement.server).block();
        const resumed = RSocketClient.resume(
            testClientOptions(replacement.client, {
                resumeToken: "response-fragments",
                maxFrameLength: 128
            }),
            {client: pair.client}
        );
        await Promise.all([accepted, resumed]);
        await waitFor(() => values.length === 1);

        expect(values).toEqual([expected]);
        subscription?.cancel();
    });

    it("rejects an unknown token after server state is lost", async () => {
        pair = await connectTestPair([], {resume: {ttlMs: 2_000}}, {resumeToken: "lost-token"});
        pair.clientTransport.drop();
        await nextTurn();
        const restarted = new RSocketServer({controllers: [], resume: {ttlMs: 2_000}});
        const replacement = memoryTransportPair();
        const accepted = restarted.accept(replacement.server).block().catch((error) => error);

        await expect(RSocketClient.resume({
            transport: () => replacement.client,
            setup: {
                keepAliveMs: 60_000,
                lifetimeMs: 60_000,
                metadataMimeType: WellKnownMimeType.MESSAGE_RSOCKET_COMPOSITE_METADATA,
                dataMimeType: WellKnownMimeType.APPLICATION_JSON,
                resumeToken: "lost-token"
            }
        }, {client: pair.client})).rejects.toMatchObject({code: 4});
        await accepted;
        await restarted.close().block();
    });

    it("rejects a duplicate Resume token while its logical session is active", async () => {
        pair = await connectTestPair([], {resume: {ttlMs: 2_000}}, {resumeToken: "duplicate-token"});
        const duplicate = memoryTransportPair();
        const accepted = pair.server.accept(duplicate.server).block().catch((error) => error);
        const duplicateClient = await RSocketClient.connect(
            testClientOptions(duplicate.client, {resumeToken: "duplicate-token"})
        );
        await waitFor(() => duplicateClient.isClosed);

        expect(await accepted).toMatchObject({message: expect.stringContaining("already active")});
        duplicateClient.close();
    });

    it("terminates resumable state when its bounded replay buffer cannot retain an error", async () => {
        const source = new ManualPublisher<number>();
        pair = await connectTestPair(
            [new ResumeStreamController(source)],
            {resume: {ttlMs: 2_000, maxBufferBytes: 6}},
            {resumeToken: "overflow-token"}
        );
        let subscription: Subscription | undefined;
        pair.client.requestStream({metadata: route("resume-stream")}).subscribe({
            onSubscribe(value) {
                subscription = value;
                value.request(1);
            },
            onNext() {
            },
            onError() {
            },
            onComplete() {
            }
        });
        await waitFor(() => source.requested === 1);
        pair.clientTransport.drop();
        await nextTurn();

        source.next(1);
        const replacement = memoryTransportPair();
        const accepted = pair.server.accept(replacement.server).block().catch((error) => error);

        await expect(RSocketClient.resume(
            testClientOptions(replacement.client, {resumeToken: "overflow-token"}),
            {client: pair.client}
        )).rejects.toMatchObject({code: 4});
        expect(await accepted).toMatchObject({message: expect.stringContaining("Unknown or expired")});
        subscription?.cancel();
    });

    it("expires suspended state after the configured Resume TTL", async () => {
        pair = await connectTestPair([], {resume: {ttlMs: 10}}, {resumeToken: "expired-token"});
        pair.clientTransport.drop();
        await nextTurn();
        const retained = pair.server as unknown as {
            readonly sessions: Set<unknown>;
            readonly resumes: {readonly size: number};
        };
        await waitFor(() => retained.sessions.size === 0);
        expect(retained.resumes.size).toBe(0);
        const replacement = memoryTransportPair();
        const accepted = pair.server.accept(replacement.server).block().catch((error) => error);

        await expect(RSocketClient.resume({
            transport: () => replacement.client,
            setup: {
                keepAliveMs: 60_000,
                lifetimeMs: 60_000,
                metadataMimeType: WellKnownMimeType.MESSAGE_RSOCKET_COMPOSITE_METADATA,
                dataMimeType: WellKnownMimeType.APPLICATION_JSON,
                resumeToken: "expired-token"
            }
        }, {client: pair.client})).rejects.toMatchObject({code: 4});
        await accepted;
    });
});
