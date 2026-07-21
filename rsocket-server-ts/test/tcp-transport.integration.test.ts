import {createServer, type Server, type Socket} from "node:net";
import type {Subscription} from "reactor-core-ts";
import {describe, expect, it} from "vitest";
import {WellKnownMimeType} from "rsocket-frames-ts";
import {route} from "rsocket-core-ts";
import {RSocket} from "rsocket-client-ts";
import {
    RSocketServer,
    type RSocketServerOptions,
    type RSocketTcpServerListener
} from "@/index.js";
import {ReactiveAcceptedTcpConnection} from "@/tcp/connection.js";
import {RangeController} from "./controllers.js";
import {
    defineTransportConformance,
    type TransportFixture
} from "./transport-conformance.js";
import {waitFor} from "./wait.js";

defineTransportConformance("RSocket TCP transport conformance", openTcpFixture);

describe("RSocket TCP transport lifecycle", () => {
    it("resumes an active stream after a real TCP connection is dropped", async () => {
        let setupCount = 0;
        const server = new RSocketServer({
            controllers: [RangeController],
            maxFrameLength: 128,
            resume: {ttlMs: 30_000},
            accept: () => {
                setupCount += 1;
            }
        });
        const sockets: Socket[] = [];
        const acceptFailures: unknown[] = [];
        const native = createServer((socket) => {
            sockets.push(socket);
            server.accept(new ReactiveAcceptedTcpConnection(socket, 128)).subscribe(
                () => undefined,
                (error) => acceptFailures.push(error)
            );
        });
        const port = await listen(native);
        const reconnectEvents: string[] = [];
        const requester = tcpRequester(port, {
            reconnect: {resume: {ttl: 30_000}},
            events: {
                connected: (event) => {
                    if (event.reconnect) reconnectEvents.push(event.type);
                }
            }
        });
        const connected = await requester.connect().block();
        if (connected === undefined) throw new Error("TCP requester did not connect");
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
            sockets[0]?.destroy();
            subscription?.request(2);

            await waitFor(() => completed && reconnectEvents.length === 1, 5_000);
            expect(values).toEqual([0, 1, 2]);
            expect(failures).toEqual([]);
            expect(sockets).toHaveLength(2);
            expect(setupCount).toBe(1);
            expect(acceptFailures).toEqual([]);
        } finally {
            subscription?.cancel();
            connected.disconnect();
            for (const socket of sockets) socket.destroy();
            await server.close().block();
            await closeNativeServer(native);
        }
    }, 10_000);

    it("completes listener event streams when the TCP listener closes", async () => {
        const server = new RSocketServer();
        const listener = await server.listenTcp({port: 0}).block();
        if (listener === undefined) throw new Error("TCP listener did not start");
        const native = (listener as unknown as {readonly native: Server}).native;
        const nativeClose = native.close;
        let errorListenersDuringClose = 0;
        let connectionsComplete = false;
        let errorsComplete = false;

        native.close = function (this: Server, callback?: (error?: Error) => void): Server {
            errorListenersDuringClose = this.listenerCount("error");
            return nativeClose.call(this, callback);
        } as Server["close"];

        listener.connections.subscribe({
            onSubscribe(subscription) {
                subscription.request(Number.MAX_SAFE_INTEGER);
            },
            onNext() {},
            onError(error) {
                throw error;
            },
            onComplete() {
                connectionsComplete = true;
            }
        });
        listener.errors.subscribe({
            onSubscribe(subscription) {
                subscription.request(Number.MAX_SAFE_INTEGER);
            },
            onNext() {},
            onError(error) {
                throw error;
            },
            onComplete() {
                errorsComplete = true;
            }
        });

        try {
            expect(native.listenerCount("connection")).toBe(1);
            expect(native.listenerCount("error")).toBe(1);
            await listener.close().block();
            expect(errorListenersDuringClose).toBe(1);
            expect(connectionsComplete).toBe(true);
            expect(errorsComplete).toBe(true);
            expect(native.listenerCount("connection")).toBe(0);
            expect(native.listenerCount("error")).toBe(0);
        } finally {
            await server.close().block();
        }
    });
});

/** Opens the public built-in TCP listener for the shared transport contract. */
async function openTcpFixture(options: RSocketServerOptions): Promise<TransportFixture> {
    const server = new RSocketServer(options);
    let listener: RSocketTcpServerListener | undefined;
    let connected: {disconnect(): unknown} | undefined;
    try {
        listener = await server.listenTcp({host: "127.0.0.1", port: 0}).block();
        if (listener === undefined) throw new Error("TCP listener did not start");
        const requester = tcpRequester(listener.address.port);
        connected = await requester.connect().block();
        if (connected === undefined) throw new Error("TCP requester did not connect");
        return {
            requester,
            async close() {
                connected?.disconnect();
                await server.close().block();
            }
        };
    } catch (error) {
        connected?.disconnect();
        await listener?.close().block();
        await server.close().block();
        throw error;
    }
}

/** Creates the public requester configuration shared by TCP integration tests. */
function tcpRequester(
    port: number,
    overrides: Partial<ConstructorParameters<typeof RSocket>[0]> = {}
): RSocket {
    return new RSocket({
        ...overrides,
        transport: {type: "tcp", host: "127.0.0.1", port},
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

/** Starts one native TCP listener and returns its ephemeral port. */
function listen(server: Server): Promise<number> {
    return new Promise((resolve, reject) => {
        server.once("error", reject);
        server.listen(0, "127.0.0.1", () => {
            const address = server.address();
            if (address === null || typeof address === "string") {
                reject(new Error("TCP listener did not expose an IP port"));
                return;
            }
            resolve(address.port);
        });
    });
}

/** Closes a native test listener after all accepted sockets are gone. */
function closeNativeServer(server: Server): Promise<void> {
    if (!server.listening) return Promise.resolve();
    return new Promise((resolve, reject) => server.close((error) => {
        if (error === undefined) resolve();
        else reject(error);
    }));
}
