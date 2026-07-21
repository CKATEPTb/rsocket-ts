/** Transport-neutral protocol contract tests for the requester engine. */
import {describe, expect, it} from "vitest";
import type {ByteReader} from "bebyte";
import type {Subscription} from "reactor-core-ts";
import {
    CancelFrame,
    ErrorFrame,
    FrameErrorCode,
    FrameFlag,
    FrameType,
    KeepaliveFrame,
    LeaseFrame,
    Metadata,
    MetadataPushFrame,
    MimeType,
    Payload,
    PayloadFlag,
    PayloadFrame,
    RequestResponseFrame,
    RequestNFrame,
    RequestChannelFrame,
    RequestFireAndForgetFrame,
    RequestStreamFrame,
    ResumeFrame,
    ResumeOkFrame,
    SetupFrame,
    WellKnownMimeType
} from "rsocket-frames-ts";
import {MAX_REQUEST_N, route, RSocketConnectionError, type RSocketPayloadFrame} from "rsocket-core-ts";
import {RSocketClient} from "@/client/index.js";
import {FakeTransportConnection} from "./fake-transport.js";
import {
    capturedFrame as frame,
    clientOptions as options,
    dataMimeType,
    metadataMimeType,
    tick
} from "./client-test-helpers.js";

/** MIME codec used to verify failures before a request reaches the wire. */
class ThrowingMimeType extends MimeType<unknown> {
    /** Makes a pre-send codec failure observable. */
    protected override serializePayload(): never {
        throw new Error("codec failed");
    }
}

/** Payload that fails only when frame serialization starts after ID allocation. */
class LazyThrowingPayload extends Payload<Uint8Array> {
    /** Creates a payload whose codec phase succeeds. */
    constructor() {
        super(WellKnownMimeType.APPLICATION_OCTET_STREAM, new Uint8Array());
    }

    /** Makes the pre-wire serialization failure observable. */
    override toUint8Array(): never {
        throw new Error("payload serialization failed");
    }
}

/** JSON-compatible codec that rejects a deterministic malformed wire marker. */
class RejectingJsonMimeType extends MimeType<unknown> {
    /** Encodes valid test requests with the standard JSON codec. */
    protected override serializePayload(value: unknown): Payload<unknown> {
        return dataMimeType.toPayload(value) as Payload<unknown>;
    }

    /** Rejects malformed input while decoding all other JSON values normally. */
    protected override deserializePayload(reader: ByteReader): Payload<unknown> {
        const bytes = reader.viewRemaining();
        if (bytes[0] === 0xff) throw new TypeError("malformed application payload");
        return dataMimeType.toPayload(bytes) as Payload<unknown>;
    }
}

describe("RSocketClient transport contract", () => {
    it("classifies synchronous transport factory failures", async () => {
        const cause = new Error("native constructor failed");

        await expect(RSocketClient.connect({
            ...options(new FakeTransportConnection()),
            transport: () => {
                throw cause;
            }
        })).rejects.toMatchObject({
            name: "RSocketConnectionError",
            cause
        } satisfies Partial<RSocketConnectionError>);
    });

    it("sends SETUP and performs a cold request-response", async () => {
        const transport = new FakeTransportConnection();
        const client = await RSocketClient.connect(options(transport));

        expect(frame(transport.sent[0])).toBeInstanceOf(SetupFrame);
        const responsePromise = client.requestResponse({data: {id: 7}}).block();
        await tick();

        const request = frame(transport.sent[1]);
        expect(request).toBeInstanceOf(RequestResponseFrame);
        transport.receive(new PayloadFrame(
            request.header.streamId,
            PayloadFlag.NEXT | PayloadFlag.COMPLETE,
            undefined,
            dataMimeType.toPayload({name: "Ada"})
        ).toUint8Array());

        await expect(responsePromise).resolves.toMatchObject({data: {name: "Ada"}});
        client.close();
    });

    it.each([
        ["RESUME", () => new ResumeFrame("unexpected", 0n, 0n)],
        ["RESUME_OK", () => new ResumeOkFrame(0n)]
    ])("ignores an unexpected responder %s frame after SETUP", async (_name, signal) => {
        const transport = new FakeTransportConnection();
        const client = await RSocketClient.connect(options(transport));

        transport.receive(signal().toUint8Array());
        await tick();

        expect(client.isClosed).toBe(false);
        expect(transport.sent).toHaveLength(1);
        client.close();
    });

    it("assumes COMPLETE on a request-response PAYLOAD that omits it", async () => {
        const transport = new FakeTransportConnection();
        const client = await RSocketClient.connect(options(transport));
        const response = client.requestResponse({data: {id: 7}}).block();
        await tick();
        const request = frame(transport.sent[1]);

        transport.receive(new PayloadFrame(
            request.header.streamId,
            PayloadFlag.NEXT,
            undefined,
            dataMimeType.toPayload({name: "Ada"})
        ).toUint8Array());

        await expect(response).resolves.toMatchObject({data: {name: "Ada"}});
        expect(transport.sent).toHaveLength(2);
        expect(client.isClosed).toBe(false);
        client.close();
    });

    it("treats a flagless request-response PAYLOAD as an empty completion", async () => {
        const transport = new FakeTransportConnection();
        const client = await RSocketClient.connect(options(transport));
        const response = client.requestResponse({data: {id: 7}}).block();
        await tick();
        const request = frame(transport.sent[1]);

        transport.receive(new PayloadFrame(request.header.streamId, 0).toUint8Array());

        await expect(response).resolves.toBeUndefined();
        expect(transport.sent).toHaveLength(2);
        expect(client.isClosed).toBe(false);
        client.close();
    });

    it.each([
        ["KEEPALIVE", () => new KeepaliveFrame()],
        ["METADATA_PUSH", () => new MetadataPushFrame(metadataMimeType.toMetadata([]))],
        ["malformed METADATA_PUSH", () => new MetadataPushFrame(
            new Metadata(WellKnownMimeType.APPLICATION_OCTET_STREAM, Uint8Array.of(0xff))
        )]
    ])("does not treat %s as SETUP acceptance", async (_name, signal) => {
        const transport = new FakeTransportConnection();
        const client = await RSocketClient.connect(options(transport));

        transport.receive(signal().toUint8Array());
        await tick();
        expect(client.isClosed).toBe(false);
        expect(transport.sent).toHaveLength(1);
        transport.receive(new ErrorFrame(0, FrameErrorCode.REJECTED_SETUP).toUint8Array());
        await tick();

        expect(client.isClosed).toBe(true);
        expect(transport.isOpen).toBe(false);
    });

    it("ignores ERROR with an otherwise invalid code on an unknown stream", async () => {
        const transport = new FakeTransportConnection();
        const client = await RSocketClient.connect(options(transport));

        const invalid = new ErrorFrame(99, FrameErrorCode.APPLICATION_ERROR).toUint8Array();
        new DataView(invalid.buffer, invalid.byteOffset, invalid.byteLength)
            .setUint32(6, FrameErrorCode.CONNECTION_ERROR);
        transport.receive(invalid);
        await tick();

        expect(client.isClosed).toBe(false);
        expect(transport.isOpen).toBe(true);
        expect(transport.sent).toHaveLength(1);
        client.close();
    });

    it("ignores a malformed initial request on an active stream ID before decoding", async () => {
        const transport = new FakeTransportConnection();
        const client = await RSocketClient.connect(options(transport));
        const response = client.requestResponse({data: {id: 7}}).block();
        await tick();
        const request = frame(transport.sent[1]);
        const duplicate = new RequestResponseFrame(
            request.header.streamId,
            FrameFlag.NONE,
            WellKnownMimeType.APPLICATION_OCTET_STREAM.toMetadata(Uint8Array.of(1), false)
        ).toUint8Array().slice();
        duplicate[6] = 0x7f;

        transport.receive(duplicate);
        transport.receive(new PayloadFrame(
            request.header.streamId,
            PayloadFlag.NEXT | PayloadFlag.COMPLETE,
            undefined,
            dataMimeType.toPayload({name: "Ada"})
        ).toUint8Array());

        await expect(response).resolves.toMatchObject({data: {name: "Ada"}});
        expect(client.isClosed).toBe(false);
        expect(transport.isOpen).toBe(true);
        client.close();
    });

    it("ignores an invalid metadata length marked IGNORE and consumes the server stream ID", async () => {
        const transport = new FakeTransportConnection();
        const client = await RSocketClient.connect(options(transport));
        const malformed = new RequestResponseFrame(
            2,
            FrameFlag.IGNORE,
            WellKnownMimeType.APPLICATION_OCTET_STREAM.toMetadata(Uint8Array.of(1), false)
        ).toUint8Array().slice();
        malformed[6] = 0x7f;

        transport.receive(malformed);
        transport.receive(new RequestFireAndForgetFrame(4, FrameFlag.NONE).toUint8Array());
        await tick();

        expect(client.isClosed).toBe(false);
        expect(transport.isOpen).toBe(true);
        expect(transport.sent).toHaveLength(1);
        client.close();
    });

    it("keeps an ignored fragmented server request occupied until its final PAYLOAD", async () => {
        const transport = new FakeTransportConnection();
        const client = await RSocketClient.connect(options(transport));
        const malformed = new RequestResponseFrame(
            2,
            FrameFlag.IGNORE | PayloadFlag.FOLLOWS,
            WellKnownMimeType.APPLICATION_OCTET_STREAM.toMetadata(Uint8Array.of(1), false)
        ).toUint8Array().slice();
        malformed[6] = 0x7f;

        transport.receive(malformed);
        transport.receive(new RequestResponseFrame(2, FrameFlag.NONE).toUint8Array());
        transport.receive(new PayloadFrame(2, PayloadFlag.NEXT).toUint8Array());
        transport.receive(new RequestFireAndForgetFrame(4, FrameFlag.NONE).toUint8Array());
        await tick();

        expect(client.isClosed).toBe(false);
        expect(transport.isOpen).toBe(true);
        expect(transport.sent).toHaveLength(1);
        client.close();
    });

    it("does not answer an unsupported responder fire-and-forget interaction", async () => {
        const transport = new FakeTransportConnection();
        const client = await RSocketClient.connect(options(transport));

        transport.receive(new RequestFireAndForgetFrame(2, 0).toUint8Array());
        await tick();

        expect(transport.sent).toHaveLength(1);
        expect(client.isClosed).toBe(false);
        client.close();
    });

    it("fails only the stream whose application payload cannot be decoded", async () => {
        const transport = new FakeTransportConnection();
        const base = options(transport);
        const client = await RSocketClient.connect({
            ...base,
            setup: {...base.setup, dataMimeType: new RejectingJsonMimeType("application/x-strict-json")}
        });
        const malformed = client.requestResponse({data: {id: 1}}).block();
        await tick();
        const first = frame(transport.sent[1]);

        transport.receive(new PayloadFrame(
            first.header.streamId,
            PayloadFlag.NEXT,
            undefined,
            WellKnownMimeType.APPLICATION_OCTET_STREAM.toPayload(Uint8Array.of(0xff))
        ).toUint8Array());

        await expect(malformed).rejects.toBeDefined();
        expect(client.isClosed).toBe(false);
        expect(frame(transport.sent[2])).toBeInstanceOf(CancelFrame);

        const valid = client.requestResponse({data: {id: 2}}).block();
        await tick();
        const second = frame(transport.sent[3]);
        expect(second.header.streamId).toBe(3);
        transport.receive(new PayloadFrame(
            second.header.streamId,
            PayloadFlag.NEXT | PayloadFlag.COMPLETE,
            undefined,
            dataMimeType.toPayload({ok: true})
        ).toUint8Array());
        await expect(valid).resolves.toMatchObject({data: {ok: true}});
        client.close();
    });

    it("fragments an oversized request according to the configured frame limit", async () => {
        const transport = new FakeTransportConnection();
        const client = await RSocketClient.connect({...options(transport), maxFrameLength: 96});
        const pending = client.requestResponse(
            {data: Uint8Array.from({length: 240}, (_value, index) => index)},
            {dataMimeType: WellKnownMimeType.APPLICATION_OCTET_STREAM}
        ).subscribe(() => undefined, () => undefined);
        await tick();

        const bytes = transport.sent.slice(1);
        const fragments = bytes.map((value) => frame(
            value,
            metadataMimeType,
            WellKnownMimeType.APPLICATION_OCTET_STREAM
        ));
        expect(fragments.length).toBeGreaterThan(1);
        expect(bytes.every((value) => value.byteLength <= 96)).toBe(true);
        expect(fragments[0]).toBeInstanceOf(RequestResponseFrame);
        expect(fragments.slice(1).every((value) => value instanceof PayloadFrame)).toBe(true);
        expect((fragments[0] as RequestResponseFrame).hasFollows()).toBe(true);
        expect((fragments.at(-1) as PayloadFrame).hasFollows()).toBe(false);

        pending.dispose();
        client.close();
    });

    it("consumes lease credit only after request encoding succeeds", async () => {
        const transport = new FakeTransportConnection();
        const base = options(transport);
        const client = await RSocketClient.connect({
            ...base,
            setup: {...base.setup, honorLease: true}
        });
        await expect(client.fireAndForget({data: {denied: true}}).block())
            .rejects.toThrow("No active RSocket lease");

        transport.receive(new LeaseFrame(10_000, 1).toUint8Array());
        await tick();
        await expect(client.fireAndForget(
            {data: {invalid: true}},
            {dataMimeType: new ThrowingMimeType("application/x-test-failure")}
        ).block()).rejects.toThrow("codec failed");
        await expect(client.fireAndForget({data: {accepted: true}}).block()).resolves.toBeUndefined();
        await expect(client.fireAndForget({data: {exhausted: true}}).block())
            .rejects.toThrow("No active RSocket lease");

        expect(frame(transport.sent[1])).toMatchObject({
            header: {streamId: 1}
        });
        expect(frame(transport.sent[1])).toBeInstanceOf(RequestFireAndForgetFrame);
        client.close();
    });

    it("does not consume a request-response stream ID when encoding fails", async () => {
        const transport = new FakeTransportConnection();
        const client = await RSocketClient.connect(options(transport));

        await expect(client.requestResponse(
            {data: {invalid: true}},
            {dataMimeType: new ThrowingMimeType("application/x-failing-response")}
        ).block()).rejects.toThrow("codec failed");
        const valid = client.requestResponse({data: {valid: true}}).subscribe(() => undefined, () => undefined);
        await tick();

        expect(frame(transport.sent[1]).header.streamId).toBe(1);
        valid.dispose();
        client.close();
    });

    it("does not consume a request-stream ID when encoding fails", async () => {
        const transport = new FakeTransportConnection();
        const client = await RSocketClient.connect(options(transport));
        let failedSubscription: Subscription | undefined;
        let failure: unknown;
        client.requestStream(
            {data: {invalid: true}},
            {dataMimeType: new ThrowingMimeType("application/x-failing-stream")}
        ).subscribe({
            onSubscribe: (value) => {
                failedSubscription = value;
            },
            onNext: () => undefined,
            onError: (error) => {
                failure = error;
            },
            onComplete: () => undefined
        });

        failedSubscription?.request(1);
        await tick();
        expect(failure).toMatchObject({message: "codec failed"});

        const valid = client.requestResponse({data: {valid: true}}).subscribe(() => undefined, () => undefined);
        await tick();
        expect(frame(transport.sent[1]).header.streamId).toBe(1);
        valid.dispose();
        client.close();
    });

    it("reuses a stream ID after request-response frame serialization fails locally", async () => {
        const transport = new FakeTransportConnection();
        const client = await RSocketClient.connect(options(transport));

        await expect(client.requestResponse(new LazyThrowingPayload()).block())
            .rejects.toThrow("payload serialization failed");
        const valid = client.requestResponse({data: {valid: true}}).subscribe(() => undefined, () => undefined);
        await tick();

        expect(frame(transport.sent[1]).header.streamId).toBe(1);
        valid.dispose();
        client.close();
    });

    it("reuses a stream ID after fire-and-forget frame serialization fails locally", async () => {
        const transport = new FakeTransportConnection();
        const client = await RSocketClient.connect(options(transport));

        await expect(client.fireAndForget(new LazyThrowingPayload()).block())
            .rejects.toThrow("payload serialization failed");
        await expect(client.fireAndForget({data: {valid: true}}).block()).resolves.toBeUndefined();

        expect(frame(transport.sent[1]).header.streamId).toBe(1);
        client.close();
    });

    it("reuses a stream ID after request-stream frame serialization fails locally", async () => {
        const transport = new FakeTransportConnection();
        const client = await RSocketClient.connect(options(transport));
        let subscription: Subscription | undefined;
        let failure: unknown;
        client.requestStream(new LazyThrowingPayload()).subscribe({
            onSubscribe: (value) => {
                subscription = value;
            },
            onNext: () => undefined,
            onError: (error) => {
                failure = error;
            },
            onComplete: () => undefined
        });

        subscription?.request(1);
        await tick();
        expect(failure).toMatchObject({message: "payload serialization failed"});
        await expect(client.fireAndForget({}).block()).resolves.toBeUndefined();
        expect(frame(transport.sent[1]).header.streamId).toBe(1);
        client.close();
    });

    it.each([5, 6.5, 0x1000000])("rejects invalid maxFrameLength %d before opening transport", async (maxFrameLength) => {
        const transport = new FakeTransportConnection();
        let opens = 0;

        await expect(RSocketClient.connect({
            ...options(transport),
            maxFrameLength,
            transport: () => {
                opens += 1;
                return transport;
            }
        })).rejects.toThrow("maxFrameLength");
        expect(opens).toBe(0);
    });

    it.each([
        ["keepAliveMs", 1.5],
        ["lifetimeMs", 1.5]
    ] as const)("rejects fractional setup.%s before opening transport", async (field, value) => {
        const transport = new FakeTransportConnection();
        let opens = 0;

        await expect(RSocketClient.connect({
            ...options(transport),
            setup: {...options(transport).setup, [field]: value},
            transport: () => {
                opens += 1;
                return transport;
            }
        })).rejects.toThrow(`setup.${field === "keepAliveMs" ? "keepAlive" : "lifetime"}`);
        expect(opens).toBe(0);
    });

    it("rejects an invalid connect timeout before opening transport", async () => {
        const transport = new FakeTransportConnection();
        let opens = 0;

        await expect(RSocketClient.connect({
            ...options(transport),
            connectTimeoutMs: Number.NaN,
            transport: () => {
                opens += 1;
                return transport;
            }
        })).rejects.toThrow("connectTimeout");
        expect(opens).toBe(0);
    });

    it("rejects invalid request-response timeouts without consuming a stream ID", async () => {
        const transport = new FakeTransportConnection();
        const client = await RSocketClient.connect(options(transport));

        for (const timeout of [0, -1, 1.5, Number.NaN, MAX_REQUEST_N + 1]) {
            await expect(client.requestResponse({data: {timeout}}, {timeout}).block())
                .rejects.toThrow("request-response timeout");
        }
        expect(transport.sent).toHaveLength(1);

        const request = client.requestResponse({data: {valid: true}}).subscribe(() => undefined, () => undefined);
        await tick();
        expect(frame(transport.sent[1]).header.streamId).toBe(1);
        request.dispose();
        client.close();
    });

    it("rejects an incoming frame larger than the configured frame limit before decoding", async () => {
        const transport = new FakeTransportConnection();
        const client = await RSocketClient.connect({...options(transport), maxFrameLength: 1_024});

        transport.receive(new Uint8Array(1_025));
        await tick();

        expect(transport.isOpen).toBe(false);
        expect(frame(transport.sent[1])).toBeInstanceOf(ErrorFrame);
        client.close();
    });

    it("does not open request-stream until downstream grants demand", async () => {
        const transport = new FakeTransportConnection();
        const client = await RSocketClient.connect(options(transport));
        let subscription: Subscription | undefined;
        const values: RSocketPayloadFrame[] = [];

        client.requestStream({data: {topic: "events"}}).subscribe({
            onSubscribe: (next) => {
                subscription = next;
            },
            onNext: (value) => values.push(value),
            onError: (error) => {
                throw error;
            },
            onComplete: () => undefined
        });
        expect(transport.sent).toHaveLength(1);

        subscription?.request(2);
        const request = frame(transport.sent[1]);
        expect(request).toBeInstanceOf(RequestStreamFrame);
        expect((request as RequestStreamFrame).request).toBe(2);

        transport.receive(new PayloadFrame(
            request.header.streamId,
            PayloadFlag.NEXT,
            undefined,
            dataMimeType.toPayload({index: 1})
        ).toUint8Array());
        await tick();
        expect(values).toHaveLength(1);
        subscription?.cancel();
        client.close();
    });

    it("rejects PAYLOAD without NEXT or COMPLETE without closing multiplexed transport", async () => {
        const transport = new FakeTransportConnection();
        const client = await RSocketClient.connect(options(transport));
        const errors: unknown[] = [];
        let subscription: Subscription | undefined;

        client.requestStream({data: {topic: "events"}}).subscribe({
            onSubscribe: (next) => {
                subscription = next;
            },
            onNext: () => undefined,
            onError: (error) => errors.push(error),
            onComplete: () => undefined
        });
        subscription?.request(1);
        const request = frame(transport.sent[1]);

        transport.receive(new PayloadFrame(request.header.streamId, 0).toUint8Array());
        await tick();

        expect(errors).toEqual([
            expect.objectContaining({
                code: FrameErrorCode.INVALID,
                message: "RSocket PAYLOAD must set FOLLOWS, NEXT, COMPLETE, or a valid combination",
                streamId: request.header.streamId
            })
        ]);
        expect(frame(transport.sent[2])).toBeInstanceOf(CancelFrame);
        expect(client.isClosed).toBe(false);
        client.close();
    });

    it.each([
        ["NEXT|FOLLOWS", PayloadFlag.NEXT | PayloadFlag.FOLLOWS],
        ["FOLLOWS", PayloadFlag.FOLLOWS]
    ])("rejects an uncredited fragmented PAYLOAD with %s before retaining bytes", async (_name, flags) => {
        const transport = new FakeTransportConnection();
        const client = await RSocketClient.connect(options(transport));
        const values: RSocketPayloadFrame[] = [];
        const errors: unknown[] = [];
        let subscription: Subscription | undefined;
        client.requestStream({data: {topic: "events"}}).subscribe({
            onSubscribe: (next) => {
                subscription = next;
            },
            onNext: (value) => values.push(value),
            onError: (error) => errors.push(error),
            onComplete: () => undefined
        });
        subscription?.request(1);
        const request = frame(transport.sent[1]);
        transport.receive(new PayloadFrame(
            request.header.streamId,
            PayloadFlag.NEXT,
            undefined,
            dataMimeType.toPayload({index: 1})
        ).toUint8Array());
        await tick();

        transport.receive(new PayloadFrame(
            request.header.streamId,
            flags,
            undefined,
            WellKnownMimeType.APPLICATION_OCTET_STREAM.toPayload(Uint8Array.of(1, 2, 3))
        ).toUint8Array());
        await tick();

        expect(values).toHaveLength(1);
        expect(errors[0]).toMatchObject({message: "Responder sent PAYLOAD without requester demand"});
        expect((client as unknown as {fragments: Map<number, unknown>}).fragments.size).toBe(0);
        expect(client.isClosed).toBe(true);
    });

    it("treats FOLLOWS as ignored when COMPLETE terminates an uncredited PAYLOAD", async () => {
        const transport = new FakeTransportConnection();
        const client = await RSocketClient.connect(options(transport));
        const values: RSocketPayloadFrame[] = [];
        const errors: unknown[] = [];
        let completed = false;
        let subscription: Subscription | undefined;
        client.requestStream({data: {topic: "events"}}).subscribe({
            onSubscribe: (next) => {
                subscription = next;
            },
            onNext: (value) => values.push(value),
            onError: (error) => errors.push(error),
            onComplete: () => {
                completed = true;
            }
        });
        subscription?.request(1);
        const request = frame(transport.sent[1]);
        transport.receive(new PayloadFrame(
            request.header.streamId,
            PayloadFlag.NEXT,
            undefined,
            dataMimeType.toPayload({index: 1})
        ).toUint8Array());
        await tick();

        transport.receive(new PayloadFrame(
            request.header.streamId,
            PayloadFlag.FOLLOWS | PayloadFlag.COMPLETE
        ).toUint8Array());
        await tick();

        expect(values).toHaveLength(1);
        expect(errors).toEqual([]);
        expect(completed).toBe(true);
        expect(client.isClosed).toBe(false);
        client.close();
    });

    it("drops late response fragments after a channel response has completed", async () => {
        const transport = new FakeTransportConnection();
        const client = await RSocketClient.connect(options(transport));
        let completed = false;
        const input = {
            [Symbol.asyncIterator]() {
                let first = true;
                return {
                    next: () => {
                        if (first) {
                            first = false;
                            return Promise.resolve({done: false as const, value: {data: {id: 1}}});
                        }
                        return new Promise<IteratorResult<{data: {id: number}}>>(() => undefined);
                    },
                    return: () => Promise.resolve({done: true as const, value: undefined})
                };
            }
        };

        client.requestChannel(input).subscribe({
            onSubscribe: (subscription) => subscription.request(2),
            onNext: () => undefined,
            onError: () => undefined,
            onComplete: () => {
                completed = true;
            }
        });
        await tick();
        const request = frame(transport.sent[1]);
        expect(request).toBeInstanceOf(RequestChannelFrame);

        transport.receive(new PayloadFrame(request.header.streamId, PayloadFlag.COMPLETE).toUint8Array());
        await tick();
        expect(completed).toBe(true);

        transport.receive(new PayloadFrame(
            request.header.streamId,
            PayloadFlag.NEXT | PayloadFlag.FOLLOWS,
            undefined,
            WellKnownMimeType.APPLICATION_OCTET_STREAM.toPayload(Uint8Array.of(1, 2, 3))
        ).toUint8Array());
        await tick();

        expect((client as unknown as {fragments: Map<number, unknown>}).fragments.size).toBe(0);
        expect(client.isClosed).toBe(false);
        client.close();
    });

    it("sends accumulated response demand only after the initial REQUEST_CHANNEL", async () => {
        const transport = new FakeTransportConnection();
        const client = await RSocketClient.connect(options(transport));
        let resolveFirst: (value: IteratorResult<{data: {id: number}}>) => void = () => undefined;
        const first = new Promise<IteratorResult<{data: {id: number}}>>((resolve) => {
            resolveFirst = resolve;
        });
        const input = {
            [Symbol.asyncIterator]: () => ({
                next: () => first,
                return: () => Promise.resolve({done: true, value: undefined as never})
            })
        };
        let subscription: Subscription | undefined;
        client.requestChannel(input).subscribe({
            onSubscribe: (value) => {
                subscription = value;
            },
            onNext: () => undefined,
            onError: () => undefined,
            onComplete: () => undefined
        });

        subscription?.request(1);
        subscription?.request(2);
        await tick();
        expect(transport.sent).toHaveLength(1);

        resolveFirst({done: false, value: {data: {id: 1}}});
        await tick();
        await tick();
        const request = frame(transport.sent[1]) as RequestChannelFrame;
        const demand = frame(transport.sent[2]) as RequestNFrame;
        expect(request).toBeInstanceOf(RequestChannelFrame);
        expect(request.request).toBe(1);
        expect(demand).toBeInstanceOf(RequestNFrame);
        expect(demand.header.streamId).toBe(request.header.streamId);
        expect(demand.request).toBe(2);
        subscription?.cancel();
        client.close();
    });

    it("does not allocate a request-channel ID before its first source item is ready", async () => {
        const transport = new FakeTransportConnection();
        const client = await RSocketClient.connect(options(transport));
        let subscription: Subscription | undefined;
        let failure: unknown;
        client.requestChannel({
            [Symbol.asyncIterator]: () => ({
                next: () => Promise.reject(new Error("channel source failed"))
            })
        }).subscribe({
            onSubscribe: (value) => {
                subscription = value;
            },
            onNext: () => undefined,
            onError: (error) => {
                failure = error;
            },
            onComplete: () => undefined
        });

        subscription?.request(1);
        await tick();
        await tick();
        expect(failure).toMatchObject({message: "channel source failed"});

        const valid = client.requestResponse({data: {valid: true}}).subscribe(() => undefined, () => undefined);
        await tick();
        expect(frame(transport.sent[1]).header.streamId).toBe(1);
        valid.dispose();
        client.close();
    });

    it("skips an occupied stream ID after the allocator wraps", async () => {
        const transport = new FakeTransportConnection();
        const client = await RSocketClient.connect(options(transport));
        const first = client.requestResponse({data: {id: 1}}).subscribe(() => undefined, () => undefined);
        await tick();
        (client as unknown as {nextStreamId: number}).nextStreamId = 1;

        const second = client.requestResponse({data: {id: 2}}).subscribe(() => undefined, () => undefined);
        await tick();

        expect(frame(transport.sent[1]).header.streamId).toBe(1);
        expect(frame(transport.sent[2]).header.streamId).toBe(3);
        first.dispose();
        second.dispose();
        client.close();
    });

    it("replays tracked frames after a transport-independent RESUME", async () => {
        const first = new FakeTransportConnection();
        const activity: Array<{direction: string; frame: {type: number}}> = [];
        const clientOptions = {
            ...options(first, "resume-token"),
            activityListener: (event: {direction: string; frame: {type: number}}) => activity.push(event)
        };
        const client = await RSocketClient.connect(clientOptions);
        first.receive(new LeaseFrame(60_000, 100).toUint8Array());
        await tick();
        const response = client.requestResponse({data: {id: 9}}).subscribe(() => undefined, () => undefined);
        await tick();
        const requestBytes = first.sent[1] as Uint8Array;

        first.disconnect();
        await tick();
        expect(client.isSuspended).toBe(true);

        const second = new FakeTransportConnection();
        const resumePromise = RSocketClient.resume({...clientOptions, transport: () => second}, {client});
        await tick();
        expect(frame(second.sent[0])).toBeInstanceOf(ResumeFrame);

        second.receive(new ResumeOkFrame(0n).toUint8Array());
        await expect(resumePromise).resolves.toBe(client);
        expect(second.sent[1]).toEqual(requestBytes);
        expect(activity).toEqual(expect.arrayContaining([
            expect.objectContaining({direction: "send", frame: expect.objectContaining({type: FrameType.RESUME})}),
            expect.objectContaining({direction: "receive", frame: expect.objectContaining({type: FrameType.RESUME_OK})})
        ]));

        response.dispose();
        client.close();
    });

    it("replays every fragment of an oversized request byte-for-byte after RESUME", async () => {
        const first = new FakeTransportConnection();
        const clientOptions = {...options(first, "resume-token"), maxFrameLength: 256};
        const client = await RSocketClient.connect(clientOptions);
        const pending = client.requestResponse({data: {value: "x".repeat(1_024)}})
            .subscribe(() => undefined, () => undefined);
        await tick();
        const originalFragments = first.sent.slice(1).map((bytes) => bytes.slice());
        expect(originalFragments.length).toBeGreaterThan(1);
        expect(originalFragments.every((bytes) => bytes.byteLength <= 256)).toBe(true);

        first.disconnect();
        await tick();
        const second = new FakeTransportConnection();
        const resume = RSocketClient.resume({...clientOptions, transport: () => second}, {client});
        await tick();
        second.receive(new ResumeOkFrame(0n).toUint8Array());
        await resume;

        expect(second.sent.slice(1)).toEqual(originalFragments);
        pending.dispose();
        client.close();
    });

    it("accepts a Resume acknowledgement for an ambiguously failed transport write", async () => {
        const first = new AmbiguousWriteTransport();
        const clientOptions = options(first, "resume-token");
        const client = await RSocketClient.connect(clientOptions);
        first.failAfterNextWrite = true;

        const response = client.requestResponse({data: {id: 10}}).block();
        await tick();
        const requestBytes = first.sent[1] as Uint8Array;
        const request = frame(requestBytes);
        expect(client.isSuspended).toBe(true);

        const second = new FakeTransportConnection();
        const resumePromise = RSocketClient.resume({...clientOptions, transport: () => second}, {client});
        await tick();
        second.receive(new ResumeOkFrame(BigInt(requestBytes.byteLength)).toUint8Array());
        await expect(resumePromise).resolves.toBe(client);

        expect(second.sent).toHaveLength(1);
        second.receive(new PayloadFrame(
            request.header.streamId,
            PayloadFlag.NEXT | PayloadFlag.COMPLETE,
            undefined,
            dataMimeType.toPayload({accepted: true})
        ).toUint8Array());
        await expect(response).resolves.toMatchObject({data: {accepted: true}});
        client.close();
    });

    it("does not replay a non-positional METADATA_PUSH after an ambiguous write", async () => {
        const first = new AmbiguousWriteTransport();
        const clientOptions = options(first, "resume-token");
        const client = await RSocketClient.connect(clientOptions);
        first.failAfterNextWrite = true;

        await expect(client.metadataPush(route("authorization.refresh")).block())
            .rejects.toThrow("write outcome is unknown");
        expect(frame(first.sent[1])).toBeInstanceOf(MetadataPushFrame);
        expect(client.isSuspended).toBe(true);

        const second = new FakeTransportConnection();
        const resumePromise = RSocketClient.resume({...clientOptions, transport: () => second}, {client});
        await tick();
        second.receive(new ResumeOkFrame(0n).toUint8Array());
        await expect(resumePromise).resolves.toBe(client);

        expect(second.sent).toHaveLength(1);
        expect(frame(second.sent[0])).toBeInstanceOf(ResumeFrame);
        client.close();
    });

    it("does not lose responder frames delivered immediately after RESUME_OK", async () => {
        const first = new FakeTransportConnection();
        const clientOptions = options(first, "resume-token");
        const client = await RSocketClient.connect(clientOptions);
        const values: RSocketPayloadFrame[] = [];
        let subscription: Subscription | undefined;
        client.requestStream({data: {topic: "events"}}).subscribe({
            onSubscribe: (value) => {
                subscription = value;
            },
            onNext: (value) => values.push(value),
            onError: () => undefined,
            onComplete: () => undefined
        });
        subscription?.request(1);
        const request = frame(first.sent[1]);
        const acknowledgedPosition = BigInt((first.sent[1] as Uint8Array).byteLength);

        first.disconnect();
        await tick();
        const second = new FakeTransportConnection();
        const resumePromise = RSocketClient.resume({...clientOptions, transport: () => second}, {client});
        await tick();

        second.receive(new ResumeOkFrame(acknowledgedPosition).toUint8Array());
        second.receive(new PayloadFrame(
            request.header.streamId,
            PayloadFlag.NEXT | PayloadFlag.COMPLETE,
            undefined,
            dataMimeType.toPayload({index: 1})
        ).toUint8Array());

        await resumePromise;
        expect(values).toHaveLength(1);
        expect(values[0]).toMatchObject({data: {index: 1}});
        client.close();
    });

    it("rejects session configuration changes before opening a Resume transport", async () => {
        const first = new FakeTransportConnection();
        const clientOptions = options(first, "resume-token");
        const client = await RSocketClient.connect(clientOptions);
        first.disconnect();
        await tick();
        let opened = 0;

        await expect(RSocketClient.resume({
            ...clientOptions,
            transport: () => {
                opened += 1;
                return new FakeTransportConnection();
            },
            setup: {...clientOptions.setup, lifetimeMs: 60_001}
        }, {client})).rejects.toThrow("cannot change the established session configuration");

        expect(opened).toBe(0);
        client.close();
    });

    it("closes an in-flight Resume transport when retained state is abandoned", async () => {
        const first = new FakeTransportConnection();
        const clientOptions = options(first, "resume-token");
        const client = await RSocketClient.connect(clientOptions);
        first.disconnect();
        await tick();

        const second = new FakeTransportConnection();
        const resume = RSocketClient.resume({...clientOptions, transport: () => second}, {client});
        const result = resume.catch((error) => error);
        await tick();
        expect(frame(second.sent[0])).toBeInstanceOf(ResumeFrame);

        const expired = new RSocketConnectionError("RSocket Resume TTL expired");
        client.abandonResume(expired);

        expect(await result).toBe(expired);
        expect(second.isOpen).toBe(false);
        expect(second.listenerCount).toBe(0);
    });

    it("coalesces response demand queued while a resumable transport is unavailable", async () => {
        const first = new FakeTransportConnection();
        const clientOptions = options(first, "resume-token");
        const client = await RSocketClient.connect(clientOptions);
        let subscription: Subscription | undefined;
        client.requestStream({data: {topic: "events"}}).subscribe({
            onSubscribe: (value) => {
                subscription = value;
            },
            onNext: () => undefined,
            onError: () => undefined,
            onComplete: () => undefined
        });
        subscription?.request(1);
        const acknowledgedPosition = BigInt((first.sent[1] as Uint8Array).byteLength);

        first.disconnect();
        await tick();
        subscription?.request(2);
        subscription?.request(3);

        const second = new FakeTransportConnection();
        const resumePromise = RSocketClient.resume({...clientOptions, transport: () => second}, {client});
        await tick();
        second.receive(new ResumeOkFrame(acknowledgedPosition).toUint8Array());
        await resumePromise;

        expect(second.sent).toHaveLength(2);
        const demand = frame(second.sent[1]);
        expect(demand).toBeInstanceOf(RequestNFrame);
        expect((demand as RequestNFrame).request).toBe(5);
        subscription?.cancel();
        client.close();
    });

    it("preserves queued REQUEST_N credit above one protocol frame", async () => {
        const first = new FakeTransportConnection();
        const clientOptions = options(first, "resume-token");
        const client = await RSocketClient.connect(clientOptions);
        const internals = client as unknown as {
            queueFrame(frame: RequestNFrame): void;
            pendingFrames: RequestNFrame[] | undefined;
        };

        internals.queueFrame(new RequestNFrame(1, MAX_REQUEST_N));
        internals.queueFrame(new RequestNFrame(1, MAX_REQUEST_N));

        const demand = internals.pendingFrames ?? [];
        expect(demand).toHaveLength(2);
        expect(demand.every((request) => request instanceof RequestNFrame)).toBe(true);
        expect(demand.map((request) => request.request)).toEqual([MAX_REQUEST_N, MAX_REQUEST_N]);
        client.close();
    });

    it("drops queued stream work superseded by cancellation during Resume", async () => {
        const first = new FakeTransportConnection();
        const clientOptions = options(first, "resume-token");
        const client = await RSocketClient.connect(clientOptions);
        let subscription: Subscription | undefined;
        client.requestStream({data: {topic: "events"}}).subscribe({
            onSubscribe: (value) => {
                subscription = value;
            },
            onNext: () => undefined,
            onError: () => undefined,
            onComplete: () => undefined
        });
        subscription?.request(1);
        const acknowledgedPosition = BigInt((first.sent[1] as Uint8Array).byteLength);

        first.disconnect();
        await tick();
        subscription?.request(2);
        subscription?.request(3);
        subscription?.cancel();

        const second = new FakeTransportConnection();
        const resumePromise = RSocketClient.resume({...clientOptions, transport: () => second}, {client});
        await tick();
        second.receive(new ResumeOkFrame(acknowledgedPosition).toUint8Array());
        await resumePromise;

        expect(second.sent).toHaveLength(2);
        expect(frame(second.sent[1])).toBeInstanceOf(CancelFrame);
        client.close();
    });

    it("does not send CANCEL when a request was queued before reaching the wire", async () => {
        const first = new FakeTransportConnection();
        const clientOptions = options(first, "resume-token");
        const client = await RSocketClient.connect(clientOptions);
        let subscription: Subscription | undefined;
        const disconnectingCodec = Object.create(dataMimeType) as MimeType<any> & {
            toPayload(value: unknown): ReturnType<typeof dataMimeType.toPayload>;
        };
        disconnectingCodec.toPayload = (value) => {
            first.disconnect();
            return dataMimeType.toPayload(value);
        };

        client.requestStream(
            {data: {topic: "events"}},
            {dataMimeType: disconnectingCodec}
        ).subscribe({
            onSubscribe: (value) => {
                subscription = value;
            },
            onNext: () => undefined,
            onError: () => undefined,
            onComplete: () => undefined
        });
        subscription?.request(1);
        expect(client.isSuspended).toBe(true);

        subscription?.cancel();
        const second = new FakeTransportConnection();
        const resumePromise = RSocketClient.resume({...clientOptions, transport: () => second}, {client});
        await tick();
        second.receive(new ResumeOkFrame(0n).toUint8Array());
        await resumePromise;

        expect(second.sent).toHaveLength(1);
        client.close();
    });

    it("pauses request-channel publisher reads while the transport is suspended", async () => {
        const first = new FakeTransportConnection();
        const client = await RSocketClient.connect(options(first, "resume-token"));
        let pulls = 0;
        let subscription: Subscription | undefined;
        const input = {
            async *[Symbol.asyncIterator]() {
                for (let index = 0; index < 100; index += 1) {
                    pulls += 1;
                    yield {data: {index}};
                }
            }
        };
        client.requestChannel(input).subscribe({
            onSubscribe: (value) => {
                subscription = value;
            },
            onNext: () => undefined,
            onError: () => undefined,
            onComplete: () => undefined
        });
        subscription?.request(1);
        await tick();
        const request = frame(first.sent[1]);
        expect(request).toBeInstanceOf(RequestChannelFrame);
        expect(pulls).toBe(1);

        first.receive(new RequestNFrame(request.header.streamId, 100).toUint8Array());
        first.disconnect();
        await tick();
        await tick();

        expect(client.isSuspended).toBe(true);
        expect(pulls).toBe(1);
        subscription?.cancel();
        client.close();
    });

    it("releases a request-channel iterator whose pending read never settles", async () => {
        const transport = new FakeTransportConnection();
        const client = await RSocketClient.connect(options(transport));
        const pending = new Promise<IteratorResult<{data: {id: number}}>>(() => undefined);
        const iterator = {
            next: () => pending,
            return: () => Promise.resolve({done: true, value: undefined as never})
        };
        const input = {
            [Symbol.asyncIterator]: () => iterator
        };
        let subscription: Subscription | undefined;

        client.requestChannel(input).subscribe({
            onSubscribe: (value) => {
                subscription = value;
            },
            onNext: () => undefined,
            onError: () => undefined,
            onComplete: () => undefined
        });
        subscription?.request(1);
        await tick();

        const streams = (client as unknown as {
            streams: Map<number, {outbound?: object}>;
        }).streams;
        const outbound = (subscription as unknown as {outbound?: {iterator?: unknown}}).outbound;
        expect(outbound?.iterator).toBe(iterator);
        expect(streams.size).toBe(0);

        subscription?.cancel();
        await tick();

        expect(outbound?.iterator).toBeUndefined();
        expect(streams.size).toBe(0);
        expect(transport.sent).toHaveLength(1);
        client.close();
    });
});

/** Transport that accepts one write before reporting an ambiguous failure. */
class AmbiguousWriteTransport extends FakeTransportConnection {
    /** Whether the next accepted write should throw after recording its bytes. */
    failAfterNextWrite = false;

    /** Records the write first, matching transports that fail after handing bytes to the OS. */
    override write(frame: Uint8Array): void {
        super.write(frame);
        if (!this.failAfterNextWrite) return;
        this.failAfterNextWrite = false;
        throw new Error("write outcome is unknown");
    }
}
