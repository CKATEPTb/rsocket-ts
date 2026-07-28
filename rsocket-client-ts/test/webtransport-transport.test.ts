/** Requester WebTransport option, lifecycle, and media tests. */
import {FrameType} from "rsocket-frames-ts";
import {describe, expect, it} from "vitest";
import {
    createWebTransportConnection,
    readFrameTypeAndFlags
} from "rsocket-core-ts";
import {RSocket} from "@/index.js";
import {
    createWebTransport,
    normalizeWebTransportUrl
} from "@/webtransport/connection.js";
import {createWebTransportTestPair} from "../../test-support/webtransport-pair.js";

describe("requester WebTransport adapter", () => {
    it("opens SETUP through a custom session factory and carries media both ways", async () => {
        const pair = createWebTransportTestPair();
        const responder = createWebTransportConnection(pair.responder, {role: "responder"});
        const frames: Uint8Array[] = [];
        const responderMedia: Uint8Array[] = [];
        const requesterMedia: Uint8Array[] = [];
        responder.frames.subscribe((frame) => frames.push(frame));
        responder.media.subscribe((payload) => responderMedia.push(payload));
        await responder.opened.block();
        const socket = new RSocket({
            transport: {
                type: "webtransport",
                url: "https://rsocket.test/client",
                factory: () => pair.requester,
                media: (payload) => requesterMedia.push(payload)
            },
            setup: {keepAlive: 60_000, lifetime: 60_000}
        });

        const connected = await socket.connect().block();
        await eventually(() => frames.length === 1);
        expect(readFrameTypeAndFlags(frames[0] as Uint8Array) >>> 10).toBe(FrameType.SETUP);

        await socket.media(Uint8Array.of(1, 2)).block();
        responder.writeMedia(Uint8Array.of(3, 4));
        await eventually(() => responderMedia.length === 1 && requesterMedia.length === 1);
        expect(responderMedia).toEqual([Uint8Array.of(1, 2)]);
        expect(requesterMedia).toEqual([Uint8Array.of(3, 4)]);
        connected?.disconnect();
    });

    it("validates secure absolute endpoints before creating a session", () => {
        expect(normalizeWebTransportUrl("https://example.com/rsocket")).toBe("https://example.com/rsocket");
        expect(() => normalizeWebTransportUrl("ws://example.com/rsocket")).toThrow("https:");
        expect(() => normalizeWebTransportUrl("/relative")).toThrow("absolute");
        expect(() => normalizeWebTransportUrl("https://example.com/#fragment")).toThrow("fragment");
    });

    it("requests unreliable capability from the native constructor for best-effort FNF", async () => {
        const pair = createWebTransportTestPair();
        const scope = globalThis as unknown as {WebTransport?: unknown};
        const previous = scope.WebTransport;
        let observed: {readonly url: string; readonly requireUnreliable?: boolean} | undefined;
        function NativeWebTransport(
            this: unknown,
            url: string,
            options?: {readonly requireUnreliable?: boolean}
        ) {
            observed = {url, ...options};
            return pair.requester;
        }
        scope.WebTransport = NativeWebTransport;
        try {
            const transport = createWebTransport({
                url: "https://rsocket.test/native",
                unreliableFireAndForget: true
            })({});

            await transport.opened.block();

            expect(observed).toEqual({
                url: "https://rsocket.test/native",
                requireUnreliable: true
            });
            transport.close();
        } finally {
            if (previous === undefined) delete scope.WebTransport;
            else scope.WebTransport = previous;
        }
    });

    it("honors transport handshake timeout and closes a stalled session", async () => {
        const pair = createWebTransportTestPair();
        const session = {
            ...pair.requester,
            ready: new Promise<void>(() => undefined)
        };
        const transport = createWebTransport({
            url: "https://rsocket.test/timeout",
            factory: () => session
        });
        const connection = transport({timeoutMs: 5});

        await expect(connection.opened.block()).rejects.toThrow("timed out after 5ms");
        connection.close();
    });

    it("rejects best-effort FNF when protocol Resume is enabled", async () => {
        const pair = createWebTransportTestPair();
        const socket = new RSocket({
            transport: {
                type: "webtransport",
                url: "https://rsocket.test/resume",
                factory: () => pair.requester,
                unreliableFireAndForget: true
            },
            reconnect: {resume: {ttl: 1_000}}
        });

        await expect(socket.connect().block()).rejects.toThrow("incompatible with RSocket Resume");
        expect(pair.writes).toEqual([]);
    });

    it("rejects media payloads larger than the native datagram limit", async () => {
        const pair = createWebTransportTestPair({maxDatagramSize: 8});
        const responder = createWebTransportConnection(pair.responder, {role: "responder"});
        responder.frames.subscribe();
        await responder.opened.block();
        const socket = new RSocket({
            transport: {
                type: "webtransport",
                url: "https://rsocket.test/media",
                factory: () => pair.requester
            }
        });
        const connected = await socket.connect().block();

        await expect(socket.media(new Uint8Array(3)).block()).rejects.toThrow("exceeds configured");
        connected?.disconnect();
    });
});

/** Waits for mapping promise continuations without relying on a fixed delay. */
async function eventually(condition: () => boolean): Promise<void> {
    for (let attempt = 0; attempt < 100; attempt += 1) {
        if (condition()) return;
        await new Promise((resolve) => setTimeout(resolve, 0));
    }
    throw new Error("Timed out waiting for requester WebTransport condition");
}
