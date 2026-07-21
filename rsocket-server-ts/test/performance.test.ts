/** End-to-end server throughput and retained-state regressions over public transports. */
import {describe, expect, it} from "vitest";
import {WellKnownMimeType} from "rsocket-frames-ts";
import {route} from "rsocket-core-ts";
import {RSocket} from "rsocket-client-ts";
import {RSocketServer, type RSocketServerConnection} from "@/index.js";
import {EchoController, RangeController} from "./controllers.js";
import {waitFor} from "./wait.js";
import {webSocketPair} from "./websocket-pair.js";

const REQUESTS = 2_000;
const STREAM_ITEMS = 20_000;
const MAX_TEST_DURATION_MS = 15_000;

/** Internal server collections whose bounds prove terminal-state cleanup. */
interface ServerRetainedState {
    readonly sessions: Set<{
        readonly interactions: {
            readonly streams: Map<number, unknown>;
            readonly payloadFragments: Map<number, unknown>;
            readonly requestFragments: {readonly fragments: Map<number, unknown>};
        };
    }>;
    readonly pendingAccepts: Map<unknown, unknown>;
}

/** Connected public client/server fixture used by transport performance tests. */
interface PerformanceFixture {
    readonly requester: RSocket;
    readonly connected: {disconnect(): void};
    close(): Promise<void>;
}

describe.each(["tcp", "websocket"] as const)("server %s performance and retention", (transport) => {
    it("sustains interactions without retaining completed streams or fragments", async () => {
        const server = new RSocketServer({controllers: [EchoController, RangeController]});
        const fixture = transport === "tcp"
            ? await tcpFixture(server)
            : await webSocketFixture(server);
        const started = performance.now();
        let checksum = 0;

        try {
            for (let index = 0; index < REQUESTS; index += 1) {
                const response = await fixture.requester.requestResponse(index, route("echo")).block();
                checksum += response?.data as number;
            }
            const stream = await fixture.requester.requestStream(STREAM_ITEMS, route("range")).toArray();
            if (transport === "tcp") {
                const large = "x".repeat(128 * 1024);
                const response = await fixture.requester.requestResponse(large, route("echo")).block();
                expect(response?.data).toBe(large);
            }
            const state = server as unknown as ServerRetainedState;
            const session = [...state.sessions][0];

            expect(checksum).toBe(REQUESTS * (REQUESTS - 1) / 2);
            expect(stream).toHaveLength(STREAM_ITEMS);
            expect(stream[0]?.data).toBe(0);
            expect(stream.at(-1)?.data).toBe(STREAM_ITEMS - 1);
            expect(state.pendingAccepts.size).toBe(0);
            expect(state.sessions.size).toBe(1);
            expect(session?.interactions.streams.size).toBe(0);
            expect(session?.interactions.payloadFragments.size).toBe(0);
            expect(session?.interactions.requestFragments.fragments.size).toBe(0);
            expect(performance.now() - started).toBeLessThan(MAX_TEST_DURATION_MS);
        } finally {
            fixture.connected.disconnect();
            await fixture.close();
        }

        await waitFor(() => (server as unknown as ServerRetainedState).sessions.size === 0);
        expect((server as unknown as ServerRetainedState).pendingAccepts.size).toBe(0);
    }, 20_000);
});

/** Opens the built-in TCP listener and a public TCP requester. */
async function tcpFixture(server: RSocketServer): Promise<PerformanceFixture> {
    const listener = await server.listenTcp({host: "127.0.0.1", port: 0}).block();
    if (listener === undefined) throw new Error("TCP performance listener did not start");
    const requester = performanceRequester({type: "tcp", host: "127.0.0.1", port: listener.address.port});
    const connected = await requester.connect().block();
    if (connected === undefined) throw new Error("TCP performance requester did not connect");
    return {
        requester,
        connected,
        close: () => server.close().block()
    };
}

/** Opens an in-process WebSocket pair through the public client and server adapters. */
async function webSocketFixture(server: RSocketServer): Promise<PerformanceFixture> {
    const sockets = webSocketPair();
    const accepted = server.acceptWebSocket(sockets.server).block();
    const requester = performanceRequester({
        type: "websocket",
        url: "ws://performance.test",
        factory: () => sockets.client
    });
    const connected = await requester.connect().block();
    const serverConnection: RSocketServerConnection | undefined = await accepted;
    if (connected === undefined || serverConnection === undefined) {
        throw new Error("WebSocket performance pair did not connect");
    }
    return {
        requester,
        connected,
        close: () => server.close().block()
    };
}

/** Creates one public requester with stable benchmark protocol settings. */
function performanceRequester(transport: ConstructorParameters<typeof RSocket>[0]["transport"]): RSocket {
    return new RSocket({
        transport,
        reconnect: false,
        setup: {
            keepAlive: 60_000,
            lifetime: 120_000,
            mimetype: {
                metadata: WellKnownMimeType.MESSAGE_RSOCKET_COMPOSITE_METADATA,
                data: WellKnownMimeType.APPLICATION_JSON
            }
        }
    });
}
