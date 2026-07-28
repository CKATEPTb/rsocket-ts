/** End-to-end requester/responder tests over the WebTransport mapping. */
import {RSocket} from "rsocket-client-ts";
import type {Subscriber, Subscription} from "reactor-core-ts";
import {FrameType, WellKnownMimeType} from "rsocket-frames-ts";
import {afterEach, beforeEach, describe, expect, it} from "vitest";
import {route, type RSocketPayloadFrame} from "rsocket-core-ts";
import {
    RequestChannelController,
    RequestStreamController,
    RSocketServer,
    type RSocketControllerRegistration,
    type RSocketServerConnection,
    type RSocketServerOptions
} from "@/index.js";
import {
    DoubleChannelController,
    EchoController,
    fireAndForgetValues,
    RangeController,
    RecordController
} from "./controllers.js";
import {
    createWebTransportTestPair,
    type WebTransportTestPair
} from "../../test-support/webtransport-pair.js";
import {ManualPublisher} from "./manual-publisher.js";

/** Resources closed after every integration test. */
const servers: RSocketServer[] = [];
const sockets: Array<{connect(): unknown}> = [];

/** Demand-controlled stream used to verify REQUEST_N across WebTransport. */
class WebTransportManualStreamController extends RequestStreamController<void, number> {
    protected readonly route = "webtransport-manual-stream";

    /** Retains the manually driven source. */
    constructor(private readonly source: ManualPublisher<number>) {
        super();
    }

    /** Returns values only as the test supplies them under downstream demand. */
    override handle(): ManualPublisher<number> {
        return this.source;
    }
}

/** Echoes request-channel values while preserving bidirectional demand. */
class WebTransportDemandChannelController extends RequestChannelController<number, number> {
    protected readonly route = "webtransport-demand-channel";

    /** Returns channel values unchanged for clear demand assertions. */
    override handle(requests: import("reactor-core-ts").Flux<RSocketPayloadFrame<number>>) {
        return requests.map(({data}) => data as number);
    }
}

describe("RSocket WebTransport requester/server integration", () => {
    beforeEach(() => {
        fireAndForgetValues.length = 0;
    });

    afterEach(async () => {
        for (const server of servers.splice(0)) await server.close().block();
        sockets.length = 0;
    });

    it("runs every interaction model on its mapped WebTransport lane", async () => {
        const fixture = await connectWebTransport([
            RecordController,
            EchoController,
            RangeController,
            DoubleChannelController
        ]);

        await fixture.socket.fireAndForget(42, route("record")).block();
        const response = await fixture.socket.requestResponse({id: 7}, route("echo")).block();
        const stream = await fixture.socket.requestStream(4, route("range")).toArray();
        const channel = await fixture.socket.requestChannel(
            [{data: 1}, {data: 2}, {data: 3}],
            route("double")
        ).toArray();

        expect(fireAndForgetValues).toEqual([42]);
        expect(response?.data).toEqual({id: 7});
        expect(stream.map((payload) => payload.data)).toEqual([0, 1, 2, 3]);
        expect(channel.map((payload) => payload.data)).toEqual([2, 4, 6]);
        expect(uniqueMappedStreams(fixture.pair, "bidi")).toBeGreaterThanOrEqual(4);
        expect(uniqueMappedStreams(fixture.pair, "uni")).toBe(1);
        fixture.connected.disconnect();
    });

    it("preserves request-stream and request-channel backpressure across bidi streams", async () => {
        const streamSource = new ManualPublisher<number>();
        const channelSource = new ManualPublisher<{data: number}>();
        const fixture = await connectWebTransport([
            new WebTransportManualStreamController(streamSource),
            WebTransportDemandChannelController
        ]);
        let streamSubscription: Subscription | undefined;
        const streamValues: number[] = [];
        fixture.socket.requestStream(undefined, route("webtransport-manual-stream")).subscribe({
            onSubscribe(subscription) {
                streamSubscription = subscription;
            },
            onNext(payload) {
                streamValues.push(payload.data as number);
            },
            onError(error) {
                throw error;
            },
            onComplete() {
            }
        } satisfies Subscriber<RSocketPayloadFrame>);

        await nextTurn();
        expect(streamSource.requested).toBe(0);
        streamSubscription?.request(2);
        await eventually(() => streamSource.requested === 2);
        streamSource.next(10);
        streamSource.next(20);
        await eventually(() => streamValues.length === 2);
        expect(streamValues).toEqual([10, 20]);

        let channelSubscription: Subscription | undefined;
        const channelValues: number[] = [];
        fixture.socket.requestChannel(channelSource, route("webtransport-demand-channel")).subscribe({
            onSubscribe(subscription) {
                channelSubscription = subscription;
                subscription.request(1);
            },
            onNext(payload) {
                channelValues.push(payload.data as number);
            },
            onError(error) {
                throw error;
            },
            onComplete() {
            }
        } satisfies Subscriber<RSocketPayloadFrame>);

        await eventually(() => channelSource.requested === 1);
        channelSource.next({data: 1});
        await eventually(() => channelValues.length === 1);
        await eventually(() => channelSource.requested === 2);
        channelSource.next({data: 2});
        await nextTurn();
        expect(channelValues).toEqual([1]);
        channelSubscription?.request(1);
        await eventually(() => channelValues.length === 2);
        expect(channelValues).toEqual([1, 2]);
        streamSubscription?.cancel();
        channelSubscription?.cancel();
        fixture.connected.disconnect();
    });

    it("carries LEASE, KEEPALIVE, and METADATA_PUSH on their mapped lanes", async () => {
        const activity: Array<{direction: string; type: FrameType}> = [];
        const pushed: unknown[] = [];
        const fixture = await connectWebTransport([EchoController], {
            lease: true,
            keepAlive: 10,
            lifetime: 1_000
        }, {
            server: {
                lease: {ttlMs: 1_000, requests: 10},
                activityListener: ({direction, frame}) => activity.push({direction, type: frame.type}),
                metadataPush: (metadata) => {
                    pushed.push(metadata.payload);
                }
            }
        });

        await fixture.socket.metadataPush(route("connection.metadata")).block();
        await fixture.serverConnection.keepAlive(Uint8Array.of(1)).block();
        await eventually(() =>
            activity.some(({direction, type}) => direction === "send" && type === FrameType.LEASE) &&
            activity.some(({direction, type}) => direction === "receive" && type === FrameType.KEEPALIVE) &&
            pushed.length === 1
        );
        expect((await fixture.socket.requestResponse("leased", route("echo")).block())?.data).toBe("leased");
        fixture.connected.disconnect();
    });

    it("automatically fragments a large request and response across interaction records", async () => {
        const fixture = await connectWebTransport([EchoController], {fragmentSize: 256}, {
            server: {maxFrameLength: 256}
        });
        const values = Array.from({length: 35_536}, () => 0);

        const response = await fixture.socket.requestResponse(values, route("echo")).block();

        expect(response?.data).toEqual(values);
        const interactionWrites = fixture.pair.writes.filter((write) =>
            write.kind === "bidi" && write.stream !== 0 && write.bytes.byteLength > 6
        );
        expect(interactionWrites.length).toBeGreaterThan(10);
        fixture.connected.disconnect();
    });

    it("delivers media datagrams in both directions without entering frame positions", async () => {
        const serverMedia: Uint8Array[] = [];
        const clientMedia: Uint8Array[] = [];
        const fixture = await connectWebTransport([EchoController], {}, {
            server: {
                media: (payload) => {
                    serverMedia.push(new Uint8Array(payload));
                }
            },
            transport: {
                media: (payload) => clientMedia.push(new Uint8Array(payload))
            }
        });

        await fixture.socket.media(Uint8Array.of(1, 2, 3)).block();
        await fixture.serverConnection.media(Uint8Array.of(4, 5, 6)).block();
        await eventually(() => serverMedia.length === 1 && clientMedia.length === 1);

        expect(serverMedia).toEqual([Uint8Array.of(1, 2, 3)]);
        expect(clientMedia).toEqual([Uint8Array.of(4, 5, 6)]);
        fixture.connected.disconnect();
    });

    it("skips a lost best-effort FNF and continues with the next reliable request", async () => {
        const pair = createWebTransportTestPair({dropDatagram: () => true});
        const fixture = await connectWebTransport([RecordController, EchoController], {}, {
            pair,
            transport: {unreliableFireAndForget: true},
            accept: {unreliableFireAndForget: true}
        });

        await fixture.socket.fireAndForget(42, route("record")).block();
        const response = await fixture.socket.requestResponse("still-alive", route("echo")).block();

        expect(response?.data).toBe("still-alive");
        expect(fireAndForgetValues).toEqual([]);
        fixture.connected.disconnect();
    });

    it("resumes the logical session on a fresh WebTransport session", async () => {
        const receivedTypes: FrameType[] = [];
        const streamSource = new ManualPublisher<number>();
        const server = new RSocketServer({
            controllers: [EchoController, new WebTransportManualStreamController(streamSource)],
            resume: {ttlMs: 5_000},
            activityListener: ({direction, frame}) => {
                if (direction === "receive") receivedTypes.push(frame.type);
            }
        });
        servers.push(server);
        const pairs: WebTransportTestPair[] = [];
        const accepts: Array<Promise<RSocketServerConnection | undefined>> = [];
        let resolveReconnected!: () => void;
        const reconnected = new Promise<void>((resolve) => {
            resolveReconnected = resolve;
        });
        const socket = new RSocket({
            transport: {
                type: "webtransport",
                url: "https://rsocket.test/session",
                factory: () => {
                    const pair = createWebTransportTestPair();
                    pairs.push(pair);
                    accepts.push(server.acceptWebTransport(pair.responder).block());
                    return pair.requester;
                }
            },
            setup: setupOptions(),
            reconnect: {resume: {ttl: 5_000}},
            events: {
                connected: ({reconnect}) => {
                    if (reconnect) resolveReconnected();
                }
            }
        });
        sockets.push(socket);
        const connected = await socket.connect().block();
        expect(connected).toBeDefined();
        await accepts[0];
        expect((await socket.requestResponse("before", route("echo")).block())?.data).toBe("before");
        const streamValues: number[] = [];
        const streamErrors: unknown[] = [];
        let streamSubscription: Subscription | undefined;
        socket.requestStream(undefined, route("webtransport-manual-stream")).subscribe({
            onSubscribe(subscription) {
                streamSubscription = subscription;
                subscription.request(1);
            },
            onNext(payload) {
                streamValues.push(payload.data as number);
            },
            onError(error) {
                streamErrors.push(error);
            },
            onComplete() {
            }
        } satisfies Subscriber<RSocketPayloadFrame>);
        await eventually(() => streamSource.requested === 1);
        streamSource.next(10);
        await eventually(() => streamValues.length === 1);

        pairs[0]?.close({closeCode: 1, reason: "network changed"});
        await Promise.race([
            reconnected,
            new Promise<never>((_, reject) => setTimeout(() => reject(new Error("Resume timed out")), 4_000))
        ]);
        await accepts[1];

        expect(receivedTypes).toContain(FrameType.RESUME);
        streamSubscription?.request(1);
        await eventually(() => streamSource.requested === 2);
        streamSource.next(20);
        await eventually(() => streamValues.length === 2);
        expect(streamValues).toEqual([10, 20]);
        expect(streamErrors).toEqual([]);
        expect((await socket.requestResponse("after", route("echo")).block())?.data).toBe("after");
        streamSubscription?.cancel();
        connected?.disconnect();
    }, 10_000);
});

/** Optional fixture settings kept separate from SETUP fragmentation controls. */
interface FixtureOptions {
    readonly pair?: WebTransportTestPair;
    readonly server?: Partial<Omit<RSocketServerOptions, "controllers">>;
    readonly transport?: {
        readonly unreliableFireAndForget?: boolean;
        readonly media?: (payload: Uint8Array) => void;
    };
    readonly accept?: {
        readonly unreliableFireAndForget?: boolean;
    };
}

/** Connected public requester and responder over one fake WebTransport pair. */
interface WebTransportFixture {
    readonly pair: WebTransportTestPair;
    readonly serverConnection: RSocketServerConnection;
    readonly socket: RSocket<any, any>;
    readonly connected: NonNullable<Awaited<ReturnType<ReturnType<RSocket<any, any>["connect"]>["block"]>>>;
}

/** Opens SETUP concurrently on both public package facades. */
async function connectWebTransport(
    controllers: readonly RSocketControllerRegistration[],
    setup: {
        readonly fragmentSize?: number;
        readonly lease?: boolean;
        readonly keepAlive?: number;
        readonly lifetime?: number;
    } = {},
    options: FixtureOptions = {}
): Promise<WebTransportFixture> {
    const pair = options.pair ?? createWebTransportTestPair();
    const server = new RSocketServer({controllers, ...options.server});
    servers.push(server);
    const accepted = server.acceptWebTransport(pair.responder, options.accept).block();
    const socket = new RSocket<any, any>({
        transport: {
            type: "webtransport",
            url: "https://rsocket.test/session",
            factory: () => pair.requester,
            ...options.transport
        },
        setup: setupOptions(setup)
    });
    sockets.push(socket);
    const [connected, serverConnection] = await Promise.all([socket.connect().block(), accepted]);
    if (connected === undefined || serverConnection === undefined) {
        throw new Error("WebTransport SETUP completed without a connected facade");
    }
    return {pair, serverConnection, socket, connected};
}

/** Stable test SETUP codecs and timers. */
function setupOptions(options: {
    readonly fragmentSize?: number;
    readonly lease?: boolean;
    readonly keepAlive?: number;
    readonly lifetime?: number;
} = {}) {
    return {
        keepAlive: options.keepAlive ?? 60_000,
        lifetime: options.lifetime ?? 60_000,
        ...(options.lease === undefined ? {} : {lease: options.lease}),
        mimetype: {
            metadata: WellKnownMimeType.MESSAGE_RSOCKET_COMPOSITE_METADATA,
            data: WellKnownMimeType.APPLICATION_JSON
        },
        ...(options.fragmentSize === undefined ? {} : {fragmentSize: options.fragmentSize})
    };
}

/** Yields to one native transport/publisher turn. */
function nextTurn(): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, 0));
}

/** Counts distinct native streams used for one mapped lane family. */
function uniqueMappedStreams(pair: WebTransportTestPair, kind: "bidi" | "uni"): number {
    return new Set(pair.writes.filter((write) => write.kind === kind).map((write) => write.stream)).size;
}

/** Waits for asynchronous datagram callbacks without a fixed sleep. */
async function eventually(condition: () => boolean): Promise<void> {
    for (let attempt = 0; attempt < 100; attempt += 1) {
        if (condition()) return;
        await new Promise((resolve) => setTimeout(resolve, 0));
    }
    throw new Error("Timed out waiting for WebTransport integration condition");
}
