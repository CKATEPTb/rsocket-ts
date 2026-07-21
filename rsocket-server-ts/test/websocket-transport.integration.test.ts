import type {Subscription} from "reactor-core-ts";
import {describe, expect, it} from "vitest";
import {WellKnownMimeType} from "rsocket-frames-ts";
import {route} from "rsocket-core-ts";
import {RSocket} from "rsocket-client-ts";
import {RSocketServer, type RSocketServerOptions} from "@/index.js";
import {ReactiveAcceptedWebSocketConnection} from "@/websocket/connection.js";
import {RangeController} from "./controllers.js";
import {
    defineTransportConformance,
    type TransportFixture
} from "./transport-conformance.js";
import {waitFor} from "./wait.js";
import {TestNodeWebSocket, type TestWebSocket, webSocketPair} from "./websocket-pair.js";

defineTransportConformance("RSocket WebSocket transport conformance", openWebSocketFixture);

describe("RSocket WebSocket transport lifecycle", () => {
    it("permits only one subscription for a concrete accepted WebSocket", async () => {
        const server = new RSocketServer();
        const sockets = webSocketPair();
        const accepting = server.acceptWebSocket(sockets.server);
        const first = accepting.block().catch((error) => error);

        await expect(accepting.block()).rejects.toThrow("already being accepted");
        await server.close("test shutdown").block();
        await expect(first).resolves.toBeInstanceOf(Error);
    });

    it("closes an accepted socket when transport initialization fails", async () => {
        const server = new RSocketServer();
        const sockets = webSocketPair();
        const broken = new Proxy(sockets.server, {
            set(target, property, value) {
                if (property === "binaryType") throw new Error("binary mode rejected");
                return Reflect.set(target, property, value);
            }
        });

        const accepted = server.acceptWebSocket(broken);
        await expect(accepted.block()).rejects.toThrow("initialization failed");
        expect(sockets.server.readyState).toBe(3);
        await server.close().block();
    });

    it("resumes an active stream after a WebSocket connection is dropped", async () => {
        let setupCount = 0;
        const server = new RSocketServer({
            controllers: [RangeController],
            maxFrameLength: 128,
            resume: {ttlMs: 30_000},
            accept: () => {
                setupCount += 1;
            }
        });
        const sockets: Array<{client: TestWebSocket; server: TestWebSocket}> = [];
        const acceptFailures: unknown[] = [];
        const accepted: Promise<unknown>[] = [];
        const reconnectEvents: string[] = [];
        const requester = webSocketRequester(() => {
            const pair = webSocketPair();
            sockets.push(pair);
            accepted.push(server.acceptWebSocket(pair.server).block().catch((error) => {
                acceptFailures.push(error);
                return undefined;
            }));
            return pair.client;
        }, {
            reconnect: {resume: {ttl: 30_000}},
            events: {
                connected: (event) => {
                    if (event.reconnect) reconnectEvents.push(event.type);
                }
            }
        });
        const connected = await requester.connect().block();
        if (connected === undefined) throw new Error("WebSocket requester did not connect");
        await accepted[0];
        const values: number[] = [];
        const failures: unknown[] = [];
        let completed = false;
        let subscription: Subscription | undefined;

        try {
            requester.requestStream(3, route("range")).subscribe({
                onSubscribe(next) {
                    subscription = next;
                    next.request(1);
                },
                onNext(payload) {
                    values.push(payload.data as number);
                },
                onError(error) {
                    failures.push(error);
                },
                onComplete() {
                    completed = true;
                }
            });

            await waitFor(() => values.length === 1);
            sockets[0]?.client.terminate();
            subscription?.request(2);

            await waitFor(() => completed && reconnectEvents.length === 1, 5_000);
            await accepted[1];
            expect(values).toEqual([0, 1, 2]);
            expect(failures).toEqual([]);
            expect(sockets).toHaveLength(2);
            expect(setupCount).toBe(1);
            expect(acceptFailures).toEqual([]);
        } finally {
            subscription?.cancel();
            connected.disconnect();
            await server.close().block();
        }
    }, 10_000);

    it("rejects non-binary browser-style messages and closes both peers", async () => {
        const server = new RSocketServer();
        const sockets = webSocketPair();
        const accepted = server.acceptWebSocket(sockets.server).block();
        let closed = false;
        const requester = webSocketRequester(() => sockets.client, {
            events: {closed: () => { closed = true; }}
        });
        const connected = await requester.connect().block();
        await accepted;

        try {
            sockets.client.sendText("not binary");
            await waitFor(() => sockets.server.readyState === 3);
            expect(sockets.client.readyState).toBe(3);
            expect(closed).toBe(true);
        } finally {
            connected?.disconnect();
            await server.close().block();
        }
    });

    it("rejects text buffers reported by the Node ws event API", async () => {
        const socket = new TestNodeWebSocket();
        const connection = new ReactiveAcceptedWebSocketConnection(socket);
        const frames = connection.frames.toArray();
        await Promise.resolve();

        socket.emitMessage(new Uint8Array(6), false);

        await expect(frames).rejects.toThrow("binary messages");
    });

    it("forwards standalone binary messages without another payload allocation", async () => {
        const socket = new TestNodeWebSocket();
        const connection = new ReactiveAcceptedWebSocketConnection(socket);
        const received: Uint8Array[] = [];
        const subscription = connection.frames.subscribe((bytes) => received.push(bytes));
        const bytes = Uint8Array.of(1, 2, 3);

        await Promise.resolve();
        socket.emitMessage(bytes, true);
        await waitFor(() => received.length === 1);

        expect(received).toEqual([bytes]);
        expect(received[0]).toBe(bytes);
        subscription.dispose();
    });

    it("bounds UTF-8 close reasons without losing server shutdown", async () => {
        const server = new RSocketServer();
        const sockets = webSocketPair();
        const accepted = server.acceptWebSocket(sockets.server).block();
        let closed = false;
        const requester = webSocketRequester(() => sockets.client, {
            events: {closed: () => { closed = true; }}
        });
        const connected = await requester.connect().block();
        const connection = await accepted;
        if (connection === undefined) throw new Error("WebSocket server did not accept the connection");

        try {
            await connection.disconnect("failure-ä".repeat(100)).block();
            await waitFor(() => sockets.server.readyState === 3);
            expect(new TextEncoder().encode(sockets.server.lastCloseReason ?? "").byteLength)
                .toBeLessThanOrEqual(123);
            expect(closed).toBe(true);
        } finally {
            connected?.disconnect();
            await server.close().block();
        }
    });
});

/** Opens an accepted WebSocket pair for the shared transport contract. */
async function openWebSocketFixture(options: RSocketServerOptions): Promise<TransportFixture> {
    const server = new RSocketServer(options);
    const sockets = webSocketPair();
    const accepted = server.acceptWebSocket(sockets.server).block();
    const requester = webSocketRequester(() => sockets.client);
    let connected: {disconnect(): unknown} | undefined;
    try {
        connected = await requester.connect().block();
        if (connected === undefined) throw new Error("WebSocket requester did not connect");
        await accepted;
        return {
            requester,
            async close() {
                connected?.disconnect();
                await server.close().block();
            }
        };
    } catch (error) {
        connected?.disconnect();
        sockets.client.terminate();
        await server.close().block();
        throw error;
    }
}

/** Creates the public requester configuration shared by WebSocket integration tests. */
function webSocketRequester(
    factory: () => TestWebSocket,
    overrides: Partial<ConstructorParameters<typeof RSocket>[0]> = {}
): RSocket {
    return new RSocket({
        ...overrides,
        transport: {type: "websocket", url: "ws://rsocket.test", factory},
        reconnect: overrides.reconnect ?? false,
        setup: {
            keepAlive: 60_000,
            lifetime: 60_000,
            fragmentSize: 128,
            mimetype: {
                metadata: WellKnownMimeType.MESSAGE_RSOCKET_COMPOSITE_METADATA,
                data: WellKnownMimeType.APPLICATION_JSON
            },
            ...overrides.setup
        }
    });
}
