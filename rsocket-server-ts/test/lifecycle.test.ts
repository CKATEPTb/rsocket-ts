import {afterEach, describe, expect, it, vi} from "vitest";
import {
    FrameErrorCode,
    FrameFlag,
    FrameType,
    ErrorFrame,
    KeepaliveFrame,
    Metadata,
    RequestNFrame,
    RequestFireAndForgetFrame,
    SetupFrame,
    WellKnownMimeType
} from "rsocket-frames-ts";
import {compositeMetadata, deserializeFrame, readFrameTypeAndFlags, route} from "rsocket-core-ts";
import {RSocketClient} from "./client-engine.js";
import {
    RSocketServer,
    type RSocketServerFrameActivity,
    type RSocketServerOptions,
    type RSocketSetupContext
} from "@/index.js";
import {fireAndForgetValues, RecordController} from "./controllers.js";
import {
    connectTestPair,
    type ConnectedTestPair,
    testClientOptions
} from "./helpers.js";
import {MemoryTransport, memoryTransportPair} from "./memory-transport.js";
import {waitFor} from "./wait.js";

describe("server connection lifecycle", () => {
    let pair: ConnectedTestPair | undefined;

    afterEach(async () => {
        pair?.client.close();
        await pair?.server.close().block();
        pair = undefined;
    });

    it("exposes every negotiated SETUP field and payload to the accept policy", async () => {
        let setup: RSocketSetupContext | undefined;
        const setupMetadata = compositeMetadata(route("setup"));
        pair = await connectTestPair([], {
            resume: {ttlMs: 1_000},
            lease: {ttlMs: 1_000, requests: 1},
            accept: (value) => {
                setup = value;
            }
        }, {
            resumeToken: "setup-token",
            keepAliveMs: 125,
            lifetimeMs: 500,
            honorLease: true,
            payload: {data: {client: "web"}, metadata: setupMetadata}
        });

        expect(setup).toMatchObject({
            data: {client: "web"},
            keepAliveMs: 125,
            lifetimeMs: 500,
            majorVersion: 1,
            minorVersion: 0,
            honorsLease: true,
            resumeToken: "setup-token",
            metadataMimeType: WellKnownMimeType.MESSAGE_RSOCKET_COMPOSITE_METADATA,
            dataMimeType: WellKnownMimeType.APPLICATION_JSON
        });
        expect(setup?.connection).toBe(pair.serverConnection);
        expect((setup?.metadata as Metadata<any>[])[0]?.payload).toEqual(["setup"]);
    });

    it("rejects SETUP lease negotiation when no server lease is configured", async () => {
        const server = new RSocketServer();
        const transport = memoryTransportPair();
        const accepted = server.accept(transport.server).block().catch((error) => error);
        const client = await RSocketClient.connect(testClientOptions(transport.client, {honorLease: true}));

        try {
            await waitFor(() => client.isClosed);
            expect(await accepted).toMatchObject({message: expect.stringContaining("Lease is not configured")});
            const rejection = deserializeFrame(
                transport.server.sent[0] as Uint8Array,
                WellKnownMimeType.MESSAGE_RSOCKET_COMPOSITE_METADATA,
                WellKnownMimeType.APPLICATION_JSON
            ) as ErrorFrame;
            expect(rejection.code).toBe(FrameErrorCode.UNSUPPORTED_SETUP);
        } finally {
            client.close();
            await server.close().block();
        }
    });

    it("rejects SETUP through server policy before exposing a connection", async () => {
        let rejectedConnection: RSocketSetupContext["connection"] | undefined;
        const server = new RSocketServer({
            controllers: [],
            accept: (setup) => {
                rejectedConnection = setup.connection;
                return false;
            }
        });
        const transport = memoryTransportPair();
        const accepted = server.accept(transport.server).block().catch((error) => error);

        const client = await RSocketClient.connect(testClientOptions(transport.client));
        await waitFor(() => client.isClosed);
        expect(await accepted).toMatchObject({message: expect.stringContaining("rejected")});
        const rejection = deserializeFrame(
            transport.server.sent[0] as Uint8Array,
            WellKnownMimeType.MESSAGE_RSOCKET_COMPOSITE_METADATA,
            WellKnownMimeType.APPLICATION_JSON
        ) as ErrorFrame;
        expect(rejection.code).toBe(FrameErrorCode.REJECTED_SETUP);
        await expect(rejectedConnection?.keepAlive().block()).rejects.toThrow("connection is closed");
        await server.close().block();
    });

    it("rejects accidentally asynchronous SETUP policies instead of ignoring their result", async () => {
        const accept = (() => Promise.resolve(true)) as unknown as NonNullable<RSocketServerOptions["accept"]>;
        const server = new RSocketServer({accept});
        const transport = memoryTransportPair();
        const accepted = server.accept(transport.server).block().catch((error) => error);
        const client = await RSocketClient.connect(testClientOptions(transport.client));

        await waitFor(() => client.isClosed);
        expect(await accepted).toMatchObject({message: expect.stringContaining("must return synchronously")});
        await server.close().block();
    });

    it("buffers reentrant peer traffic until the synchronous SETUP policy finishes", async () => {
        let keepAlive: Promise<unknown> | undefined;
        pair = await connectTestPair([], {
            accept: ({connection}) => {
                keepAlive = connection.keepAlive(Uint8Array.of(1, 2, 3)).block();
            }
        });

        await keepAlive;
        await waitFor(() => frameCount(pair!.clientTransport.sent, FrameType.KEEPALIVE) > 0);

        expect(pair.client.isClosed).toBe(false);
        expect(pair.serverTransport.isOpen).toBe(true);
        expect(pair.serverTransport.sent.some((bytes) => frameType(bytes) === FrameType.ERROR)).toBe(false);
    });

    it("releases timers when session activation fails after SETUP acceptance", async () => {
        vi.useFakeTimers();
        const server = new RSocketServer({
            resume: {ttlMs: 2_000},
            lease: {ttlMs: 1_000, requests: 1}
        });
        try {
            const transport = memoryTransportPair();
            transport.server.dropBeforeDeliveryAfter(0);
            const accepted = server.accept(transport.server).block().catch((error) => error);
            const client = await RSocketClient.connect(testClientOptions(transport.client, {
                resumeToken: "failed-activation",
                honorLease: true
            })).catch(() => undefined);
            client?.abandonResume();

            expect(await accepted).toBeInstanceOf(Error);
            expect(vi.getTimerCount()).toBe(0);
        } finally {
            await server.close().block();
            vi.useRealTimers();
        }
    });

    it("does not expose a session or retain timers when SETUP activity closes it", async () => {
        vi.useFakeTimers();
        const server = new RSocketServer({
            activityListener: ({connection, direction, frame}) => {
                if (direction === "receive" && frame instanceof SetupFrame) {
                    connection.disconnect("closed during setup activity").subscribe();
                }
            }
        });
        try {
            const transport = memoryTransportPair();
            const accepted = server.accept(transport.server).block().catch((error) => error);
            const connected = RSocketClient.connect(testClientOptions(transport.client)).catch((error) => error);
            const [serverResult, clientResult] = await Promise.all([accepted, connected]);

            expect(serverResult).toBeInstanceOf(Error);
            if (clientResult instanceof RSocketClient) clientResult.close();
            expect((server as unknown as {sessions: Set<unknown>}).sessions.size).toBe(0);
            expect(vi.getTimerCount()).toBe(0);
        } finally {
            await server.close().block();
            vi.useRealTimers();
        }
    });

    it("does not expose a session when initial LEASE delivery closes it reentrantly", async () => {
        vi.useFakeTimers();
        const server = new RSocketServer({
            lease: {ttlMs: 1_000, requests: 1},
            activityListener: ({connection, direction, frame}) => {
                if (direction === "send" && frame.type === FrameType.LEASE) {
                    connection.disconnect("closed during initial lease").subscribe();
                }
            }
        });
        try {
            const transport = memoryTransportPair();
            const accepted = server.accept(transport.server).block().catch((error) => error);
            const connected = RSocketClient.connect(testClientOptions(transport.client, {
                honorLease: true
            })).catch((error) => error);
            const [serverResult, clientResult] = await Promise.all([accepted, connected]);

            expect(serverResult).toBeInstanceOf(Error);
            if (clientResult instanceof RSocketClient) clientResult.close();
            expect((server as unknown as {sessions: Set<unknown>}).sessions.size).toBe(0);
            expect(vi.getTimerCount()).toBe(0);
        } finally {
            await server.close().block();
            vi.useRealTimers();
        }
    });

    it("does not extend Resume retention after a failed physical resume attempt", async () => {
        vi.useFakeTimers();
        try {
            pair = await connectTestPair([], {resume: {ttlMs: 100}}, {resumeToken: "fixed-resume-deadline"});
            pair.clientTransport.drop();
            await Promise.resolve();
            expect(pair.client.isSuspended).toBe(true);

            vi.advanceTimersByTime(60);
            const replacement = memoryTransportPair();
            replacement.server.dropBeforeDeliveryAfter(0);
            const accepted = pair.server.accept(replacement.server).block().catch((error) => error);
            const resumed = RSocketClient.resume(
                testClientOptions(replacement.client, {resumeToken: "fixed-resume-deadline"}),
                {client: pair.client}
            ).catch((error) => error);
            const [acceptError, resumeError] = await Promise.all([accepted, resumed]);
            expect(acceptError).toBeInstanceOf(Error);
            expect(resumeError).toBeInstanceOf(Error);

            const retained = pair.server as unknown as {readonly sessions: Set<unknown>};
            vi.advanceTimersByTime(39);
            expect(retained.sessions.size).toBe(1);
            vi.advanceTimersByTime(1);
            expect(retained.sessions.size).toBe(0);
        } finally {
            pair?.client.abandonResume();
            await pair?.server.close().block();
            pair = undefined;
            vi.useRealTimers();
        }
    });

    it("resumes with an independently copied opaque binary token", async () => {
        const expectedToken = Uint8Array.of(0xff, 0x00, 0x80, 0x61);
        const mutableToken = expectedToken.slice();
        pair = await connectTestPair([], {resume: {ttlMs: 1_000}}, {resumeToken: mutableToken});
        mutableToken.fill(0);
        pair.clientTransport.drop();
        await waitFor(() => pair!.client.isSuspended);

        const replacement = memoryTransportPair();
        const accepted = pair.server.accept(replacement.server).block();
        const resumed = RSocketClient.resume(
            testClientOptions(replacement.client, {resumeToken: expectedToken.slice()}),
            {client: pair.client}
        );
        const [connection, client] = await Promise.all([accepted, resumed]);

        expect(connection).toBe(pair.serverConnection);
        expect(client).toBe(pair.client);
        expect(client.isSuspended).toBe(false);
    });

    it("isolates the retained Resume token from setup callback mutation", async () => {
        const token = Uint8Array.of(0xff, 0x00, 0x80, 0x61);
        pair = await connectTestPair([], {
            resume: {ttlMs: 1_000},
            accept: (context) => {
                if (context.resumeToken instanceof Uint8Array) context.resumeToken.fill(0);
            }
        }, {resumeToken: token});
        pair.clientTransport.drop();
        await waitFor(() => pair!.client.isSuspended);

        const replacement = memoryTransportPair();
        const accepted = pair.server.accept(replacement.server).block();
        const resumed = RSocketClient.resume(
            testClientOptions(replacement.client, {resumeToken: token}),
            {client: pair.client}
        );
        const [connection, client] = await Promise.all([accepted, resumed]);

        expect(connection).toBe(pair.serverConnection);
        expect(client.isSuspended).toBe(false);
    });

    it("closes transports still waiting for SETUP when the server stops", async () => {
        const server = new RSocketServer();
        const transport = memoryTransportPair();
        const accepted = server.accept(transport.server).block();
        const listener = server.listenTcp({port: 0});

        await server.close("Maintenance").block();

        await expect(accepted).rejects.toThrow("Maintenance");
        expect(transport.client.isOpen).toBe(false);
        expect(transport.server.isOpen).toBe(false);
        await expect(listener.block()).rejects.toThrow("closed");
        const rejected = memoryTransportPair();
        await expect(server.accept(rejected.server).block()).rejects.toThrow("closed");
        expect(rejected.client.isOpen).toBe(false);
        expect(rejected.server.isOpen).toBe(false);
    });

    it("permits only one subscription to an accept operation for one transport", async () => {
        const server = new RSocketServer();
        const transport = memoryTransportPair();
        const accepting = server.accept(transport.server);
        const first = accepting.block();

        await expect(accepting.block()).rejects.toThrow("already being accepted");

        const client = await RSocketClient.connect(testClientOptions(transport.client));
        await expect(first).resolves.toBeDefined();
        client.close();
        await server.close().block();
    });

    it("completes SETUP when disposing the transport opening subscription throws", async () => {
        const server = new RSocketServer();
        const transport = memoryTransportPair();
        let disposed = false;
        Object.defineProperty(transport.server, "opened", {
            value: {
                subscribe: () => ({
                    dispose: () => {
                        disposed = true;
                        throw new Error("opening cleanup failed");
                    }
                })
            }
        });
        const accepted = server.accept(transport.server).block();
        const client = await RSocketClient.connect(testClientOptions(transport.client));

        try {
            await expect(accepted).resolves.toBeDefined();
            expect(disposed).toBe(true);
        } finally {
            client.close();
            await server.close().block();
        }
    });

    it("permits only one subscription to one TCP listener operation", async () => {
        const server = new RSocketServer();
        const opening = server.listenTcp({host: "127.0.0.1", port: 0});
        const listener = await opening.block();

        await expect(opening.block()).rejects.toThrow("already being started");

        await listener?.close().block();
        await server.close().block();
    });

    it("attempts every listener close and releases ownership when one close fails", async () => {
        const server = new RSocketServer();
        const failure = new Error("first listener failed");
        let secondClosed = false;
        const listeners = (server as unknown as {
            listeners: Set<{close(): {block(): Promise<void | undefined>}}>
        }).listeners;
        listeners.add({close: () => ({block: () => Promise.reject(failure)})});
        listeners.add({close: () => ({block: () => {
            secondClosed = true;
            return Promise.resolve(undefined);
        }})});

        await expect(server.close().block()).rejects.toBe(failure);

        expect(secondClosed).toBe(true);
        expect(listeners.size).toBe(0);
    });

    it("closes transports that never send the mandatory first handshake frame", async () => {
        vi.useFakeTimers();
        const server = new RSocketServer({handshakeTimeoutMs: 25});
        const transport = memoryTransportPair();
        try {
            const accepted = server.accept(transport.server).block();
            const rejected = expect(accepted).rejects.toThrow("handshake timed out");
            await vi.advanceTimersByTimeAsync(26);

            await rejected;
            expect(transport.client.isOpen).toBe(false);
            expect(transport.server.isOpen).toBe(false);
            expect((server as unknown as {pendingAccepts: Map<unknown, unknown>}).pendingAccepts.size).toBe(0);
        } finally {
            await server.close().block();
            vi.useRealTimers();
        }
    });

    it("detaches a terminated public connection from its protocol session", async () => {
        pair = await connectTestPair([]);
        const connection = pair.serverConnection;
        const setup = connection.setup;

        pair.client.close();
        await waitFor(() => (pair!.server as unknown as {sessions: Set<unknown>}).sessions.size === 0);

        expect(connection.setup).toBe(setup);
        await expect(connection.keepAlive().block()).rejects.toThrow("connection is closed");
    });

    it("echoes periodic client KEEPALIVE data and remains alive under traffic", async () => {
        const activity: RSocketServerFrameActivity[] = [];
        pair = await connectTestPair([], {
            activityListener: (event) => activity.push(event)
        }, {keepAliveMs: 10, lifetimeMs: 40});

        await waitFor(() => activity.some(({direction, frame}) =>
            direction === "send" && frame instanceof KeepaliveFrame
        ));
        await new Promise((resolve) => setTimeout(resolve, 55));

        const received = activity.find(({direction, frame}) =>
            direction === "receive" && frame instanceof KeepaliveFrame
        )?.frame as KeepaliveFrame | undefined;
        const sent = activity.find(({direction, frame}) =>
            direction === "send" && frame instanceof KeepaliveFrame
        )?.frame as KeepaliveFrame | undefined;
        expect(received?.isRequireRespond()).toBe(true);
        expect(sent?.isRequireRespond()).toBe(false);
        expect(sent?.payload?.toUint8Array()).toEqual(received?.payload?.toUint8Array());
        expect(pair.clientTransport.isOpen).toBe(true);
    });

    it("closes an inactive connection after the client-negotiated lifetime", async () => {
        pair = await connectTestPair([], {}, {keepAliveMs: 1_000, lifetimeMs: 20});

        await waitFor(() => !pair!.serverTransport.isOpen);

        expect(pair.clientTransport.isOpen).toBe(false);
        expect(pair.serverTransport.isOpen).toBe(false);
    });

    it("does not let non-KEEPALIVE traffic extend the negotiated lifetime", async () => {
        vi.useFakeTimers();
        try {
            vi.setSystemTime(1_000_000);
            pair = await connectTestPair([], {}, {keepAliveMs: 10_000, lifetimeMs: 1_000});
            await vi.advanceTimersByTimeAsync(600);
            pair.clientTransport.write(new RequestNFrame(99, 1).toUint8Array());

            await vi.advanceTimersByTimeAsync(401);

            expect(pair.clientTransport.isOpen).toBe(false);
            expect(pair.serverTransport.isOpen).toBe(false);
        } finally {
            vi.useRealTimers();
        }
    });

    it("sends server-initiated KEEPALIVE only when its Mono is subscribed", async () => {
        pair = await connectTestPair([]);
        const connected = pair;
        const before = frameCount(pair.serverTransport.sent, FrameType.KEEPALIVE);
        const keepAlive = pair.serverConnection.keepAlive(new Uint8Array([1, 2, 3]));

        expect(frameCount(pair.serverTransport.sent, FrameType.KEEPALIVE)).toBe(before);
        await keepAlive.block();
        await waitFor(() => frameCount(connected.clientTransport.sent, FrameType.KEEPALIVE) > 0);

        expect(frameCount(pair.serverTransport.sent, FrameType.KEEPALIVE)).toBe(before + 1);
    });

    it("keeps KEEPALIVE implied positions at zero when Resume was not negotiated", async () => {
        const activity: RSocketServerFrameActivity[] = [];
        pair = await connectTestPair([RecordController], {
            activityListener: (event) => activity.push(event)
        });

        await pair.client.fireAndForget({data: 1, metadata: route("record")}).block();
        await pair.serverConnection.keepAlive().block();

        const keepalive = activity.filter(({direction, frame}) =>
            direction === "send" && frame instanceof KeepaliveFrame && frame.isRequireRespond()
        ).at(-1)?.frame as KeepaliveFrame | undefined;
        expect(keepalive?.lastReceivedPosition).toBe(0n);
    });

    it("grants and enforces requester lease credits negotiated in SETUP", async () => {
        pair = await connectTestPair(
            [RecordController],
            {lease: {ttlMs: 5_000, requests: 1}},
            {honorLease: true}
        );

        expect(pair.serverTransport.sent.some((bytes) => frameType(bytes) === FrameType.LEASE)).toBe(true);
        await pair.client.fireAndForget({data: 1, metadata: route("record")}).block();
        await expect(pair.client.fireAndForget({data: 2, metadata: route("record")}).block())
            .rejects.toThrow("No active RSocket lease");
    });

    it("rejects requests that bypass exhausted requester lease credits", async () => {
        fireAndForgetValues.length = 0;
        pair = await connectTestPair(
            [RecordController],
            {lease: {ttlMs: 5_000, requests: 1}},
            {honorLease: true}
        );
        await pair.client.fireAndForget({data: 1, metadata: route("record")}).block();
        await waitFor(() => fireAndForgetValues.length === 1);

        pair.clientTransport.write(new RequestFireAndForgetFrame(
            3,
            FrameFlag.NONE,
            compositeMetadata(route("record")),
            WellKnownMimeType.APPLICATION_JSON.toPayload(2)
        ).toUint8Array());
        await waitFor(() => pair!.serverTransport.sent.some((bytes) =>
            frameType(bytes) === FrameType.ERROR
        ));

        const rejection = pair.serverTransport.sent
            .filter((bytes) => frameType(bytes) === FrameType.ERROR)
            .map((bytes) => deserializeFrame(
                bytes,
                WellKnownMimeType.MESSAGE_RSOCKET_COMPOSITE_METADATA,
                WellKnownMimeType.APPLICATION_JSON
            ) as ErrorFrame)
            .at(-1);
        expect(rejection).toMatchObject({code: FrameErrorCode.REJECTED, header: {streamId: 3}});
        expect(fireAndForgetValues).toEqual([1]);
        expect(pair.client.isClosed).toBe(false);
    });

    it("publishes a new lease before a synchronous peer consumes its first credit", async () => {
        fireAndForgetValues.length = 0;
        const clientTransport = new MemoryTransport();
        let injected = false;
        class ReentrantLeaseTransport extends MemoryTransport {
            /** Injects a requester frame before the LEASE write returns to the session. */
            override write(bytes: Uint8Array): void {
                super.write(bytes);
                if (injected || frameType(bytes) !== FrameType.LEASE) return;
                injected = true;
                clientTransport.write(new RequestFireAndForgetFrame(
                    1,
                    FrameFlag.NONE,
                    compositeMetadata(route("record")),
                    WellKnownMimeType.APPLICATION_JSON.toPayload(7)
                ).toUint8Array());
            }
        }
        const serverTransport = new ReentrantLeaseTransport();
        clientTransport.link(serverTransport);
        serverTransport.link(clientTransport);
        const server = new RSocketServer({
            controllers: [RecordController],
            lease: {ttlMs: 5_000, requests: 1}
        });
        const accepted = server.accept(serverTransport).block();
        const connected = RSocketClient.connect(testClientOptions(clientTransport, {honorLease: true}));
        const [connection, client] = await Promise.all([accepted, connected]);

        try {
            expect(connection).toBeDefined();
            await waitFor(() => fireAndForgetValues.length === 1);
            expect(fireAndForgetValues).toEqual([7]);
            expect(serverTransport.sent.some((bytes) => frameType(bytes) === FrameType.ERROR)).toBe(false);
        } finally {
            client.close();
            await server.close().block();
        }
    });

    it("rejects manual lease updates when the client did not negotiate leasing", async () => {
        pair = await connectTestPair([]);

        await expect(pair.serverConnection.lease({ttlMs: 100, requests: 1}).block())
            .rejects.toThrow("did not enable");
        expect(pair.serverTransport.sent.some((bytes) => frameType(bytes) === FrameType.LEASE)).toBe(false);
    });
});

/** Reads a raw frame type without allocating a frame object. */
function frameType(bytes: Uint8Array): FrameType {
    return (readFrameTypeAndFlags(bytes) >>> 10) as FrameType;
}

/** Counts raw frames of one protocol type. */
function frameCount(frames: readonly Uint8Array[], type: FrameType): number {
    return frames.filter((bytes) => frameType(bytes) === type).length;
}
