/** Real Node TCP integration tests for SETUP, request-response, and RESUME. */
import {createServer, type Server, type Socket} from "node:net";
import type {Subscription} from "reactor-core-ts";
import {afterEach, describe, expect, it} from "vitest";
import {
    FrameCodec,
    MetadataPushFrame,
    PayloadFlag,
    PayloadFrame,
    RequestFireAndForgetFrame,
    RequestNFrame,
    RequestResponseFrame,
    RequestStreamFrame,
    ResumeFrame,
    ResumeOkFrame,
    SetupFrame,
    WellKnownMimeType,
    type Frame
} from "rsocket-frames-ts";
import {RSocket} from "@";

const dataMimeType = WellKnownMimeType.APPLICATION_JSON;
const metadataMimeType = WellKnownMimeType.MESSAGE_RSOCKET_COMPOSITE_METADATA;
const servers: Server[] = [];
const clients: Array<{disconnect(): unknown}> = [];

afterEach(async () => {
    for (const client of clients.splice(0)) client.disconnect();
    await Promise.all(servers.splice(0).map(closeServer));
});

describe("RSocket TCP client", () => {
    it("carries fire-and-forget and metadata-push as independent TCP frames", async () => {
        const seen: Frame[] = [];
        const port = await listen((_socket, frame) => seen.push(frame));
        const client = tcpClient(port);
        const connected = (await client.connect().block())!;
        clients.push(connected);

        await client.fireAndForget({event: "opened"}).block();
        await client.metadataPush(
            WellKnownMimeType.TEXT_PLAIN.toMetadata("session-metadata")
        ).block();
        await waitFor(() => seen.some((frame) => frame instanceof MetadataPushFrame));

        expect(seen.some((frame) => frame instanceof SetupFrame)).toBe(true);
        expect(seen.some((frame) => frame instanceof RequestFireAndForgetFrame)).toBe(true);
        expect(seen.some((frame) => frame instanceof MetadataPushFrame)).toBe(true);
    });

    it("performs request-response when the TCP response is split into tiny writes", async () => {
        const seen: Frame[] = [];
        const port = await listen((socket, frame, codec) => {
            seen.push(frame);
            if (!(frame instanceof RequestResponseFrame)) return;
            const response = codec.serialize(new PayloadFrame(
                frame.header.streamId,
                PayloadFlag.NEXT | PayloadFlag.COMPLETE,
                undefined,
                dataMimeType.toPayload({name: "TCP"})
            ));
            writePieces(socket, response, [1, 2, 4]);
        });
        const client = tcpClient(port);
        const connected = (await client.connect().block())!;
        clients.push(connected);

        await expect(client.requestResponse({id: 1}).block()).resolves.toMatchObject({data: {name: "TCP"}});
        expect(seen.some((value) => value instanceof SetupFrame)).toBe(true);
        expect(seen.some((value) => value instanceof RequestResponseFrame)).toBe(true);
    });

    it("resumes a logical session and replays a request after TCP loss", async () => {
        let connection = 0;
        let firstRequestStreamId = 0;
        const port = await listen((socket, frame, codec) => {
            if (frame instanceof SetupFrame) {
                connection += 1;
                return;
            }
            if (connection === 1 && frame instanceof RequestResponseFrame) {
                firstRequestStreamId = frame.header.streamId;
                socket.destroy();
                return;
            }
            if (frame instanceof ResumeFrame) {
                connection += 1;
                socket.write(codec.serialize(new ResumeOkFrame(0n)));
                return;
            }
            if (connection === 2 && frame instanceof RequestResponseFrame) {
                socket.write(codec.serialize(new PayloadFrame(
                    frame.header.streamId,
                    PayloadFlag.NEXT | PayloadFlag.COMPLETE,
                    undefined,
                    dataMimeType.toPayload({resumed: true})
                )));
            }
        });
        const client = tcpClient(port, true);
        const connected = (await client.connect().block())!;
        clients.push(connected);
        const response = client.requestResponse({id: 2}).block();

        await expect(response).resolves.toMatchObject({data: {resumed: true}});
        expect(firstRequestStreamId).toBe(1);
        expect(connection).toBe(2);
    });

    it("maps TCP request-stream demand to REQUEST_STREAM and REQUEST_N", async () => {
        const port = await listen((socket, frame, codec) => {
            if (frame instanceof RequestStreamFrame) {
                socket.write(codec.serialize(new PayloadFrame(
                    frame.header.streamId,
                    PayloadFlag.NEXT,
                    undefined,
                    dataMimeType.toPayload({index: 1})
                )));
                return;
            }
            if (frame instanceof RequestNFrame) {
                socket.write(codec.serialize(new PayloadFrame(
                    frame.header.streamId,
                    PayloadFlag.NEXT | PayloadFlag.COMPLETE,
                    undefined,
                    dataMimeType.toPayload({index: 2})
                )));
            }
        });
        const client = tcpClient(port);
        const connected = (await client.connect().block())!;
        clients.push(connected);
        const values: unknown[] = [];
        let completed = false;
        let subscription: Subscription | undefined;

        client.requestStream({topic: "updates"}).subscribe({
            onSubscribe(next) {
                subscription = next;
                next.request(1);
            },
            onNext(payload) {
                values.push(payload.data);
            },
            onError(error) {
                throw error;
            },
            onComplete() {
                completed = true;
            }
        });

        await waitFor(() => values.length === 1);
        expect(values).toEqual([{index: 1}]);
        expect(completed).toBe(false);

        subscription?.request(1);
        await waitFor(() => completed);
        expect(values).toEqual([{index: 1}, {index: 2}]);
    });

    it("terminates a session after a remote half-close", async () => {
        const port = await listen((socket, frame) => {
            if (frame instanceof SetupFrame) socket.end();
        });
        let closed = false;
        const client = new RSocket({
            transport: {type: "tcp", host: "127.0.0.1", port},
            reconnect: false,
            events: {closed: () => { closed = true; }},
            setup: {
                keepAlive: 60_000,
                lifetime: 120_000,
                mimetype: {data: dataMimeType, metadata: metadataMimeType}
            }
        });
        const connected = (await client.connect().block())!;
        clients.push(connected);

        await waitFor(() => closed);

        expect(closed).toBe(true);
    });

    it("applies fragmentSize as the inbound TCP frame limit", async () => {
        let closeError: unknown;
        const port = await listen((socket, frame) => {
            if (frame instanceof SetupFrame) {
                socket.write(Uint8Array.of(0, 0, 129));
            }
        });
        const client = new RSocket({
            transport: {type: "tcp", host: "127.0.0.1", port},
            reconnect: false,
            events: {closed: (event) => { closeError = event.error; }},
            setup: {
                keepAlive: 60_000,
                lifetime: 120_000,
                fragmentSize: 128,
                mimetype: {data: dataMimeType, metadata: metadataMimeType}
            }
        });
        const connected = await client.connect().block();
        if (connected !== undefined) clients.push(connected);

        await waitFor(() => closeError !== undefined);

        expect(closeError).toEqual(expect.objectContaining({message: expect.stringContaining("129")}));
    });
});

/** Starts a TCP server that decodes and dispatches complete RSocket frames. */
async function listen(
    onFrame: (socket: Socket, frame: Frame, codec: FrameCodec) => void
): Promise<number> {
    const server = createServer((socket) => {
        const codec = new FrameCodec({
            transport: "tcp",
            mimetype: {data: dataMimeType, metadata: metadataMimeType}
        });
        socket.on("data", (chunk) => {
            if (typeof chunk === "string") throw new TypeError("TCP test server expects binary chunks");
            for (const frame of codec.deserialize(chunk)) onFrame(socket, frame, codec);
        });
        socket.on("end", () => {
            try {
                codec.finish();
            } catch {
                socket.destroy();
            }
        });
    });
    servers.push(server);
    await new Promise<void>((resolve, reject) => {
        server.once("error", reject);
        server.listen(0, "127.0.0.1", resolve);
    });
    const address = server.address();
    if (address === null || typeof address === "string") throw new Error("TCP test server did not expose a port");
    return address.port;
}

/** Creates a connector with stable protocol settings for integration tests. */
function tcpClient(port: number, resume = false): RSocket {
    return new RSocket({
        transport: {type: "tcp", host: "127.0.0.1", port},
        reconnect: resume ? {resume: {ttl: 30_000}} : false,
        setup: {
            keepAlive: 60_000,
            lifetime: 120_000,
            mimetype: {data: dataMimeType, metadata: metadataMimeType}
        }
    });
}

/** Writes one packet through deliberately uneven TCP chunks. */
function writePieces(socket: Socket, packet: Uint8Array, sizes: readonly number[]): void {
    let offset = 0;
    for (const size of sizes) {
        const end = Math.min(offset + size, packet.byteLength);
        if (end > offset) socket.write(packet.subarray(offset, end));
        offset = end;
    }
    if (offset < packet.byteLength) socket.write(packet.subarray(offset));
}

/** Closes a test server after active client sockets have ended. */
function closeServer(server: Server): Promise<void> {
    return new Promise((resolve) => server.close(() => resolve()));
}

/** Waits for an asynchronous transport state transition. */
async function waitFor(predicate: () => boolean): Promise<void> {
    for (let attempt = 0; attempt < 100; attempt += 1) {
        if (predicate()) return;
        await new Promise((resolve) => setTimeout(resolve, 5));
    }
    throw new Error("Timed out waiting for TCP state transition");
}
