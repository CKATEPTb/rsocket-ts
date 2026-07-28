/** Browser facade selection and WebTransport integration tests. */
import {describe, expect, it} from "vitest";
import {WellKnownMimeType} from "rsocket-frames-ts";
import {route} from "rsocket-core-ts";
import {
    RequestResponseController,
    RSocketServer
} from "rsocket-server-ts";
import {RSocket} from "@/index.js";
import {browserClientOptions} from "@/rsocket/options.js";
import {createWebTransportTestPair} from "../../test-support/webtransport-pair.js";

/** Echo route used by the browser/server integration boundary. */
class BrowserEchoController extends RequestResponseController<unknown, unknown> {
    protected readonly route = "echo";

    /** Returns the decoded JSON request unchanged. */
    override handle(data: unknown): unknown {
        return data;
    }
}

describe("browser WebTransport facade", () => {
    it("selects WebTransport for https: and keeps WebSocket settings isolated", () => {
        const pair = createWebTransportTestPair();
        const options = browserClientOptions("https://example.com/rsocket", {
            webTransport: {
                factory: () => pair.requester,
                unreliableFireAndForget: true
            }
        });

        expect(options.transport.type).toBe("webtransport");
        expect(() => browserClientOptions("https://example.com/rsocket", {
            setup: {protocols: "rsocket"}
        })).toThrow("WebSocket-only");
        expect(() => browserClientOptions("wss://example.com/rsocket", {
            webTransport: {factory: () => pair.requester}
        })).toThrow("requires an https:");
    });

    it("runs browser request-response and media over an accepted WebTransport session", async () => {
        const pair = createWebTransportTestPair();
        const serverMedia: Uint8Array[] = [];
        const clientMedia: Uint8Array[] = [];
        const server = new RSocketServer({
            controllers: [BrowserEchoController],
            media: (payload) => {
                serverMedia.push(new Uint8Array(payload));
            }
        });
        const accepted = server.acceptWebTransport(pair.responder).block();
        const socket = new RSocket("https://example.com/rsocket", {
            webTransport: {
                factory: () => pair.requester,
                media: (payload) => clientMedia.push(new Uint8Array(payload))
            },
            setup: {
                keepAlive: 60_000,
                lifetime: 60_000,
                mimetype: {
                    metadata: WellKnownMimeType.MESSAGE_RSOCKET_COMPOSITE_METADATA,
                    data: WellKnownMimeType.APPLICATION_JSON
                }
            }
        });

        const connected = await socket.connect().block();
        const serverConnection = await accepted;
        const response = await socket.requestResponse({browser: true}, route("echo")).block();
        await socket.media(Uint8Array.of(1, 2)).block();
        await serverConnection?.media(Uint8Array.of(3, 4)).block();
        await eventually(() => serverMedia.length === 1 && clientMedia.length === 1);

        expect(response?.data).toEqual({browser: true});
        expect(serverMedia).toEqual([Uint8Array.of(1, 2)]);
        expect(clientMedia).toEqual([Uint8Array.of(3, 4)]);
        connected?.disconnect();
        await server.close().block();
    });
});

/** Waits for datagram delivery without relying on a fixed scheduler delay. */
async function eventually(condition: () => boolean): Promise<void> {
    for (let attempt = 0; attempt < 100; attempt += 1) {
        if (condition()) return;
        await new Promise((resolve) => setTimeout(resolve, 0));
    }
    throw new Error("Timed out waiting for browser WebTransport condition");
}
