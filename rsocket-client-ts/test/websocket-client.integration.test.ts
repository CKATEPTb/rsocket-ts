/** Public requester integration tests through the WebSocket transport. */
import type {Subscription} from "reactor-core-ts";
import {describe, expect, it} from "vitest";
import {
    MetadataPushFrame,
    PayloadFlag,
    PayloadFrame,
    RequestFireAndForgetFrame,
    RequestNFrame,
    RequestResponseFrame,
    RequestStreamFrame,
    SetupFrame,
    WellKnownMimeType
} from "rsocket-frames-ts";
import {RSocket} from "@";
import type {RSocketReconnectSignals} from "@/reconnect/signals.js";
import {FakeWebSocket} from "./websocket-fake.js";

const dataMimeType = WellKnownMimeType.APPLICATION_JSON;
const metadataMimeType = WellKnownMimeType.MESSAGE_RSOCKET_COMPOSITE_METADATA;
/** Private reconnect hook shared with the browser adapter in production. */
const RECONNECT_SIGNALS = Symbol.for("rsocket-client-ts.reconnect-signals");

describe("RSocket WebSocket client integration", () => {
    it("does not duplicate the initial connection when a wake signal fires during registration", async () => {
        const wires: FakeWebSocket[] = [];
        const socket = new RSocket(withReconnectSignals({
            transport: {
                type: "websocket",
                url: "ws://localhost/rsocket",
                factory: () => {
                    const wire = new FakeWebSocket();
                    wires.push(wire);
                    return wire;
                }
            },
            reconnect: true,
            setup: {
                keepAlive: 60_000,
                lifetime: 120_000,
                mimetype: {data: dataMimeType, metadata: metadataMimeType}
            }
        }, {
            isAvailable: () => true,
            waitUntilAvailable: () => Promise.resolve(),
            onWake(listener) {
                listener();
                return () => undefined;
            }
        }));

        const ready = socket.connect().block();
        expect(wires).toHaveLength(1);
        wires[0]?.open();

        const connected = await ready;
        connected?.disconnect();
    });

    it("opens WebSocket through the root constructor and returns only the connected surface", async () => {
        const {socket, wire, connected} = await connect();

        expect(wire.decodeSent(0, metadataMimeType, dataMimeType)).toBeInstanceOf(SetupFrame);
        expect(Object.keys(connected).sort()).toEqual([
            "disconnect",
            "fireAndForget",
            "metadataPush",
            "metadataUpdate",
            "process",
            "requestChannel",
            "requestResponse",
            "requestStream"
        ]);

        connected.disconnect();
        expect(wire.readyState).toBe(3);
        expect(socket.connect).toBeTypeOf("function");
    });

    it("maps the public setup lease option to the SETUP LEASE flag", async () => {
        const wire = new FakeWebSocket();
        const socket = new RSocket({
            transport: {type: "websocket", url: "ws://localhost/rsocket", factory: () => wire},
            reconnect: false,
            setup: {
                lease: true,
                keepAlive: 60_000,
                lifetime: 120_000,
                mimetype: {data: dataMimeType, metadata: metadataMimeType}
            }
        });
        const ready = socket.connect().block();
        wire.open();
        const connected = await ready;
        const setup = wire.decodeSent(0, metadataMimeType, dataMimeType) as SetupFrame;

        expect(setup.isRespectLease()).toBe(true);
        connected?.disconnect();
    });

    it("performs request-response through binary WebSocket messages", async () => {
        const {socket, wire, connected} = await connect();
        const response = socket.requestResponse({id: 7}).block();
        const request = wire.decodeSent(1, metadataMimeType, dataMimeType) as RequestResponseFrame;

        wire.serverSend(new PayloadFrame(
            request.header.streamId,
            PayloadFlag.NEXT | PayloadFlag.COMPLETE,
            undefined,
            dataMimeType.toPayload({name: "Ada"})
        ));

        await expect(response).resolves.toMatchObject({data: {name: "Ada"}});
        connected.disconnect();
    });

    it("keeps fire-and-forget and metadata-push cold until subscription", async () => {
        const {socket, wire, connected} = await connect();
        const fireAndForget = socket.fireAndForget({event: "opened"});
        const metadataPush = socket.metadataPush(
            WellKnownMimeType.TEXT_PLAIN.toMetadata("session-metadata")
        );

        expect(wire.sent).toHaveLength(1);
        await fireAndForget.block();
        await metadataPush.block();

        expect(wire.decodeSent(1, metadataMimeType, dataMimeType)).toBeInstanceOf(RequestFireAndForgetFrame);
        expect(wire.decodeSent(2, metadataMimeType, dataMimeType)).toBeInstanceOf(MetadataPushFrame);
        connected.disconnect();
    });

    it("maps WebSocket stream demand to REQUEST_STREAM and REQUEST_N", async () => {
        const {socket, wire, connected} = await connect();
        const values: unknown[] = [];
        let subscription: Subscription | undefined;
        let completed = false;

        socket.requestStream({topic: "updates"}).subscribe({
            onSubscribe(next) {
                subscription = next;
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

        expect(wire.sent).toHaveLength(1);
        subscription?.request(1);
        const request = wire.decodeSent(1, metadataMimeType, dataMimeType) as RequestStreamFrame;
        expect(request.request).toBe(1);

        wire.serverSend(new PayloadFrame(
            request.header.streamId,
            PayloadFlag.NEXT,
            undefined,
            dataMimeType.toPayload({index: 1})
        ));
        await Promise.resolve();
        expect(values).toEqual([{index: 1}]);

        subscription?.request(2);
        const requestN = wire.decodeSent(2, metadataMimeType, dataMimeType) as RequestNFrame;
        expect(requestN).toBeInstanceOf(RequestNFrame);
        expect(requestN.request).toBe(2);

        wire.serverSend(new PayloadFrame(request.header.streamId, PayloadFlag.COMPLETE));
        await Promise.resolve();
        expect(completed).toBe(true);
        connected.disconnect();
    });

    it("does not reconnect WebSocket unless reconnect is configured", async () => {
        const wires: FakeWebSocket[] = [];
        const socket = createSocket(() => {
            const wire = new FakeWebSocket();
            wires.push(wire);
            return wire;
        });
        const ready = socket.connect().block();
        wires[0]?.open();
        const connected = await ready;

        wires[0]?.close(1006, "network lost");
        await new Promise((resolve) => setTimeout(resolve, 10));

        expect(wires).toHaveLength(1);
        connected?.disconnect();
    });
});

/** Adds deterministic runtime signals without widening the public requester class. */
function withReconnectSignals<T extends ConstructorParameters<typeof RSocket>[0]>(
    options: T,
    signals: RSocketReconnectSignals
): T {
    Object.defineProperty(options, RECONNECT_SIGNALS, {value: signals});
    return options;
}

/** Opens one deterministic WebSocket-backed public requester. */
async function connect() {
    const wire = new FakeWebSocket();
    const socket = createSocket(() => wire);
    const ready = socket.connect().block();
    wire.open();
    const connected = await ready;
    if (connected === undefined) throw new Error("RSocket connected without a facade");
    return {socket, wire, connected};
}

/** Creates a public requester around one test WebSocket factory. */
function createSocket(factory: () => FakeWebSocket): RSocket {
    return new RSocket({
        transport: {type: "websocket", url: "ws://localhost/rsocket", factory},
        reconnect: false,
        setup: {
            keepAlive: 60_000,
            lifetime: 120_000,
            mimetype: {data: dataMimeType, metadata: metadataMimeType}
        }
    });
}
