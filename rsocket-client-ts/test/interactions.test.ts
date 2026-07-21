/** Independent interaction and lifecycle tests for the transport-neutral requester. */
import {describe, expect, it, vi} from "vitest";
import type {Subscription} from "reactor-core-ts";
import {
    CancelFrame,
    ErrorFrame,
    FrameErrorCode,
    FrameFlag,
    KeepaliveFlag,
    KeepaliveFrame,
    MetadataPushFrame,
    PayloadFlag,
    PayloadFrame,
    RequestChannelFrame,
    RequestFireAndForgetFrame,
    RequestNFrame,
    RequestResponseFrame,
    RequestStreamFrame,
    WellKnownMimeType,
    type Frame
} from "rsocket-frames-ts";
import {
    emitOutboundFrameFragments,
    outboundFrameLength,
    route,
    type RSocketPayloadFrame
} from "rsocket-core-ts";
import {RSocketClient} from "@/client/index.js";
import {FakeTransportConnection} from "./fake-transport.js";
import {
    capturedFrame,
    clientOptions,
    connectClient,
    dataMimeType,
    metadataMimeType,
    tick
} from "./client-test-helpers.js";

describe("RSocketClient interactions", () => {
    it("keeps fire-and-forget and metadata-push cold until subscription", async () => {
        const transport = new FakeTransportConnection();
        const client = await connectClient(transport);
        const fireAndForget = client.fireAndForget({
            data: {event: "opened"},
            metadata: route("analytics.opened")
        });
        const metadataPush = client.metadataPush(route("session.refresh"));

        expect(transport.sent).toHaveLength(1);
        await fireAndForget.block();
        expect(capturedFrame(transport.sent[1])).toBeInstanceOf(RequestFireAndForgetFrame);
        await metadataPush.block();
        expect(capturedFrame(transport.sent[2])).toBeInstanceOf(MetadataPushFrame);
        client.close();
    });

    it("maps a stream ERROR frame to a rejected request-response Mono", async () => {
        const transport = new FakeTransportConnection();
        const client = await connectClient(transport);
        const response = client.requestResponse({data: {id: 1}}).block();
        const request = capturedFrame(transport.sent[1]) as RequestResponseFrame;

        transport.receive(new ErrorFrame(
            request.header.streamId,
            FrameErrorCode.APPLICATION_ERROR,
            WellKnownMimeType.TEXT_PLAIN.toPayload("denied")
        ).toUint8Array());

        await expect(response).rejects.toMatchObject({
            message: "denied",
            code: FrameErrorCode.APPLICATION_ERROR,
            streamId: request.header.streamId
        });
        expect(client.isClosed).toBe(false);
        client.close();
    });

    it("sends CANCEL once when request-response is disposed and ignores a late response", async () => {
        const transport = new FakeTransportConnection();
        const client = await connectClient(transport);
        const values: RSocketPayloadFrame[] = [];
        const disposable = client.requestResponse({data: {id: 1}})
            .subscribe((value) => values.push(value), () => undefined);
        const request = capturedFrame(transport.sent[1]) as RequestResponseFrame;

        disposable.dispose();
        disposable.dispose();
        expect(capturedFrame(transport.sent[2])).toBeInstanceOf(CancelFrame);
        expect(transport.sent).toHaveLength(3);

        transport.receive(responsePayload(request.header.streamId, {late: true}).toUint8Array());
        await tick();
        expect(values).toEqual([]);
        client.close();
    });

    it("maps request-stream demand to the initial request and later REQUEST_N frames", async () => {
        const transport = new FakeTransportConnection();
        const client = await connectClient(transport);
        const values: RSocketPayloadFrame[] = [];
        let completed = false;
        let subscription: Subscription | undefined;
        client.requestStream({data: {topic: "orders"}}).subscribe({
            onSubscribe: (value) => {
                subscription = value;
            },
            onNext: (value) => values.push(value),
            onError: () => undefined,
            onComplete: () => {
                completed = true;
            }
        });

        expect(transport.sent).toHaveLength(1);
        subscription?.request(2);
        const request = capturedFrame(transport.sent[1]) as RequestStreamFrame;
        expect(request.request).toBe(2);
        subscription?.request(3);
        expect(capturedFrame(transport.sent[2])).toMatchObject({request: 3});
        expect(capturedFrame(transport.sent[2])).toBeInstanceOf(RequestNFrame);

        transport.receive(new PayloadFrame(
            request.header.streamId,
            PayloadFlag.NEXT,
            undefined,
            dataMimeType.toPayload({index: 1})
        ).toUint8Array());
        transport.receive(new PayloadFrame(
            request.header.streamId,
            PayloadFlag.COMPLETE
        ).toUint8Array());
        await tick();

        expect(values).toHaveLength(1);
        expect(values[0]?.data).toEqual({index: 1});
        expect(completed).toBe(true);
        client.close();
    });

    it("sends CANCEL when frame diagnostics synchronously cancel an initial stream", async () => {
        const transport = new FakeTransportConnection();
        let subscription: Subscription | undefined;
        const client = await RSocketClient.connect({
            ...clientOptions(transport),
            activityListener: ({direction, frame}) => {
                if (direction === "send" && frame instanceof RequestStreamFrame) subscription?.cancel();
            }
        });
        client.requestStream({data: {topic: "orders"}}).subscribe({
            onSubscribe: (value) => {
                subscription = value;
            },
            onNext: () => undefined,
            onError: () => undefined,
            onComplete: () => undefined
        });

        subscription?.request(1);

        expect(capturedFrame(transport.sent[1])).toBeInstanceOf(RequestStreamFrame);
        expect(capturedFrame(transport.sent[2])).toBeInstanceOf(CancelFrame);
        expect(transport.sent).toHaveLength(3);
        expect(client.isClosed).toBe(false);
        client.close();
    });

    it("fails invalid downstream demand before allocating a stream ID", async () => {
        const transport = new FakeTransportConnection();
        const client = await connectClient(transport);
        const errors: unknown[] = [];
        let subscription: Subscription | undefined;
        client.requestStream({data: {topic: "orders"}}).subscribe({
            onSubscribe: (value) => {
                subscription = value;
            },
            onNext: () => undefined,
            onError: (error) => errors.push(error),
            onComplete: () => undefined
        });

        subscription?.request(0);
        await tick();

        expect(errors).toEqual([
            expect.objectContaining({message: "Reactive Streams demand must be a positive safe integer"})
        ]);
        expect(transport.sent).toHaveLength(1);
        client.close();
    });

    it("waits for responder channel demand before reading the second outbound item", async () => {
        const transport = new FakeTransportConnection();
        const client = await connectClient(transport);
        let reads = 0;
        const input: Iterable<{data: {index: number}}> = {
            *[Symbol.iterator]() {
                reads += 1;
                yield {data: {index: 1}};
                reads += 1;
                yield {data: {index: 2}};
            }
        };
        const values: RSocketPayloadFrame[] = [];
        let subscription: Subscription | undefined;
        client.requestChannel(input).subscribe({
            onSubscribe: (value) => {
                subscription = value;
            },
            onNext: (value) => values.push(value),
            onError: () => undefined,
            onComplete: () => undefined
        });

        subscription?.request(1);
        await tick();
        const request = capturedFrame(transport.sent[1]) as RequestChannelFrame;
        expect(request.request).toBe(1);
        expect(reads).toBe(1);
        expect(transport.sent).toHaveLength(2);

        transport.receive(new RequestNFrame(request.header.streamId, 1).toUint8Array());
        await tick();
        expect(reads).toBe(2);
        expect(capturedFrame(transport.sent[2])).toBeInstanceOf(PayloadFrame);

        transport.receive(responsePayload(request.header.streamId, {accepted: true}).toUint8Array());
        await tick();
        expect(values[0]?.data).toEqual({accepted: true});
        subscription?.cancel();
        client.close();
    });

    it("keeps the request-channel outbound half open after response completion", async () => {
        const transport = new FakeTransportConnection();
        const client = await connectClient(transport);
        let responseCompleted = false;
        let subscription: Subscription | undefined;
        client.requestChannel([
            {data: {index: 1}},
            {data: {index: 2}}
        ]).subscribe({
            onSubscribe: (value) => {
                subscription = value;
            },
            onNext: () => undefined,
            onError: () => undefined,
            onComplete: () => {
                responseCompleted = true;
            }
        });

        subscription?.request(1);
        await tick();
        const request = capturedFrame(transport.sent[1]) as RequestChannelFrame;

        transport.receive(new PayloadFrame(request.header.streamId, PayloadFlag.COMPLETE).toUint8Array());
        await tick();
        expect(responseCompleted).toBe(true);

        transport.receive(new RequestNFrame(request.header.streamId, 1).toUint8Array());
        await tick();
        const continuation = capturedFrame(transport.sent[2]);
        expect(continuation).toBeInstanceOf(PayloadFrame);
        expect(continuation.header.streamId).toBe(request.header.streamId);

        client.close();
    });

    it("sends async channel completion without requiring an extra responder credit", async () => {
        const transport = new FakeTransportConnection();
        const client = await connectClient(transport);
        let subscription: Subscription | undefined;
        const input = {
            async *[Symbol.asyncIterator]() {
                yield {data: {index: 1}};
                yield {data: {index: 2}};
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
        const request = capturedFrame(transport.sent[1]) as RequestChannelFrame;
        transport.receive(new RequestNFrame(request.header.streamId, 1).toUint8Array());
        await tick();
        await tick();

        const continuation = capturedFrame(transport.sent[2]) as PayloadFrame;
        const completion = capturedFrame(transport.sent[3]) as PayloadFrame;
        expect(continuation).toBeInstanceOf(PayloadFrame);
        expect(continuation.isNext()).toBe(true);
        expect(completion).toBeInstanceOf(PayloadFrame);
        expect(completion.isComplete()).toBe(true);

        client.close();
    });

    it("reassembles a fragmented request-response payload before decoding it", async () => {
        const transport = new FakeTransportConnection();
        const base = clientOptions(transport);
        const client = await RSocketClient.connect({...base, maxFrameLength: 96});
        const response = client.requestResponse({data: {id: 1}}).block();
        const request = capturedFrame(transport.sent[1]) as RequestResponseFrame;
        const expected = {value: "fragmented-".repeat(40)};
        const payload = responsePayload(request.header.streamId, expected);
        const length = outboundFrameLength(payload);
        if (length === undefined) throw new Error("Expected a fragmentable PAYLOAD frame");
        const fragments: Frame[] = [];
        emitOutboundFrameFragments(payload, length, 96, (fragment) => fragments.push(fragment));

        expect(fragments.length).toBeGreaterThan(1);
        for (const fragment of fragments) transport.receive(fragment.toUint8Array());

        await expect(response).resolves.toMatchObject({data: expected});
        client.close();
    });

    it("discards an unfinished response fragment sequence when its sender cancels it", async () => {
        const transport = new FakeTransportConnection();
        const base = clientOptions(transport);
        const client = await RSocketClient.connect({...base, maxFrameLength: 96});
        const response = client.requestResponse({data: {id: 1}}).block();
        const request = capturedFrame(transport.sent[1]) as RequestResponseFrame;
        const partial = responsePayload(request.header.streamId, {value: "discarded-".repeat(40)});
        const length = outboundFrameLength(partial);
        if (length === undefined) throw new Error("Expected a fragmentable PAYLOAD frame");
        const fragments: Frame[] = [];
        emitOutboundFrameFragments(partial, length, 96, (fragment) => fragments.push(fragment));

        transport.receive((fragments[0] as Frame).toUint8Array());
        transport.receive(new CancelFrame(request.header.streamId).toUint8Array());
        transport.receive(responsePayload(request.header.streamId, {value: "fresh"}).toUint8Array());

        await expect(response).resolves.toMatchObject({data: {value: "fresh"}});
        expect(client.isClosed).toBe(false);
        client.close();
    });

    it("closes the connection when a responder exceeds granted stream demand", async () => {
        const transport = new FakeTransportConnection();
        const client = await connectClient(transport);
        let subscription: Subscription | undefined;
        client.requestStream({data: {topic: "orders"}}).subscribe({
            onSubscribe: (value) => {
                subscription = value;
            },
            onNext: () => undefined,
            onError: () => undefined,
            onComplete: () => undefined
        });
        subscription?.request(1);
        const request = capturedFrame(transport.sent[1]) as RequestStreamFrame;

        transport.receive(new PayloadFrame(
            request.header.streamId,
            PayloadFlag.NEXT,
            undefined,
            dataMimeType.toPayload({index: 1})
        ).toUint8Array());
        transport.receive(new PayloadFrame(
            request.header.streamId,
            PayloadFlag.NEXT,
            undefined,
            dataMimeType.toPayload({index: 2})
        ).toUint8Array());
        await tick();

        expect(client.isClosed).toBe(true);
        expect(transport.isOpen).toBe(false);
    });

    it("ignores responder SETUP and METADATA_PUSH with a non-zero stream ID", async () => {
        const transport = new FakeTransportConnection();
        const client = await connectClient(transport);
        const repeatedSetup = (transport.sent[0] as Uint8Array).slice();
        repeatedSetup[3] = 2;

        transport.receive(repeatedSetup);
        transport.receive(new MetadataPushFrame(metadataMimeType.toMetadata([]), 7).toUint8Array());
        await tick();

        expect(client.isClosed).toBe(false);
        expect(transport.isOpen).toBe(true);
        expect(transport.sent).toHaveLength(1);
        client.close();
    });

    it("rejects an unsupported responder request before application MIME decoding", async () => {
        const transport = new FakeTransportConnection();
        const client = await connectClient(transport);
        const invalidJson = WellKnownMimeType.APPLICATION_OCTET_STREAM.toPayload(Uint8Array.of(0xff));

        transport.receive(new RequestResponseFrame(2, FrameFlag.NONE, undefined, invalidJson).toUint8Array());
        await tick();

        expect(client.isClosed).toBe(false);
        expect(capturedFrame(transport.sent[1])).toMatchObject({
            header: {streamId: 2},
            code: FrameErrorCode.REJECTED
        });
        client.close();
    });

    it("enforces sequential even IDs for server-initiated requests", async () => {
        const transport = new FakeTransportConnection();
        const client = await connectClient(transport);

        transport.receive(new RequestResponseFrame(4, FrameFlag.NONE).toUint8Array());
        await tick();

        expect(client.isClosed).toBe(true);
        expect(capturedFrame(transport.sent[1])).toMatchObject({
            header: {streamId: 0},
            code: FrameErrorCode.CONNECTION_ERROR
        });
    });

    it("advances server stream IDs across unsupported sequential requests", async () => {
        const transport = new FakeTransportConnection();
        const client = await connectClient(transport);

        transport.receive(new RequestResponseFrame(2, FrameFlag.NONE).toUint8Array());
        transport.receive(new RequestResponseFrame(4, FrameFlag.NONE).toUint8Array());
        await tick();

        expect(client.isClosed).toBe(false);
        expect(transport.sent).toHaveLength(3);
        expect(capturedFrame(transport.sent[2])).toMatchObject({
            header: {streamId: 4},
            code: FrameErrorCode.REJECTED
        });
        client.close();
    });

    it("ignores unknown ignorable frames without consuming server request IDs", async () => {
        const transport = new FakeTransportConnection();
        const client = await connectClient(transport);

        transport.receive(unknownFrame(0, FrameFlag.IGNORE));
        transport.receive(unknownFrame(7, FrameFlag.IGNORE));
        transport.receive(new RequestResponseFrame(2, FrameFlag.NONE).toUint8Array());
        await tick();

        expect(client.isClosed).toBe(false);
        expect(transport.sent).toHaveLength(2);
        expect(capturedFrame(transport.sent[1])).toMatchObject({
            header: {streamId: 2},
            code: FrameErrorCode.REJECTED
        });
        client.close();
    });

    it("closes on an unknown required frame instead of consuming a server request ID", async () => {
        const transport = new FakeTransportConnection();
        const client = await connectClient(transport);

        transport.receive(unknownFrame(2, FrameFlag.NONE));
        await tick();

        expect(client.isClosed).toBe(true);
        expect(transport.sent).toHaveLength(2);
        expect(capturedFrame(transport.sent[1])).toMatchObject({
            header: {streamId: 0},
            code: FrameErrorCode.CONNECTION_ERROR
        });
    });

});

describe("RSocketClient connection lifecycle", () => {
    it("notifies the close-listener snapshot even when a callback unsubscribes another listener", async () => {
        const transport = new FakeTransportConnection();
        const client = await connectClient(transport);
        const calls: string[] = [];
        let removeSecond: () => void = () => undefined;
        client.onClose(() => {
            calls.push("first");
            removeSecond();
        });
        removeSecond = client.onClose(() => calls.push("second"));

        client.close();

        expect(calls).toEqual(["first", "second"]);
    });

    it("echoes KEEPALIVE data when the responder requests a response", async () => {
        const transport = new FakeTransportConnection();
        const client = await connectClient(transport);
        const data = Uint8Array.of(1, 2, 3);

        transport.receive(new KeepaliveFrame(
            KeepaliveFlag.RESPOND,
            0n,
            WellKnownMimeType.APPLICATION_OCTET_STREAM.toPayload(data)
        ).toUint8Array());
        await tick();

        const response = capturedFrame(
            transport.sent[1],
            WellKnownMimeType.APPLICATION_OCTET_STREAM,
            WellKnownMimeType.APPLICATION_OCTET_STREAM
        ) as KeepaliveFrame;
        expect(response).toBeInstanceOf(KeepaliveFrame);
        expect(response.header.flags & KeepaliveFlag.RESPOND).toBe(0);
        expect(response.payload?.payload).toEqual(data);
        client.close();
    });

    it("fails active requests and notifies close listeners once after non-resumable transport loss", async () => {
        const transport = new FakeTransportConnection();
        const client = await connectClient(transport);
        const closes: unknown[] = [];
        client.onClose((error) => closes.push(error));
        const response = client.requestResponse({data: {id: 1}}).block();

        transport.disconnect("network lost");
        await expect(response).rejects.toThrow("network lost");

        expect(client.isClosed).toBe(true);
        expect(client.isSuspended).toBe(false);
        expect(closes).toHaveLength(1);
        client.close();
        expect(closes).toHaveLength(1);
    });

    it("expires peer silence through the public lifetime check", async () => {
        const transport = new FakeTransportConnection();
        const client = await connectClient(transport);

        expect(client.checkLifetime(Date.now())).toBe(true);
        expect(client.checkLifetime(Date.now() + 120_001)).toBe(false);
        expect(client.isClosed).toBe(true);
        expect(transport.isOpen).toBe(false);
    });

    it("does not treat ordinary responder payloads as KEEPALIVE acknowledgements", async () => {
        vi.useFakeTimers();
        let client: RSocketClient | undefined;
        try {
            vi.setSystemTime(1_000_000);
            const transport = new FakeTransportConnection();
            client = await connectClient(transport);
            vi.setSystemTime(1_060_000);
            transport.receive(new PayloadFrame(
                2,
                PayloadFlag.NEXT,
                undefined,
                dataMimeType.toPayload({ignored: true})
            ).toUint8Array());

            expect(client.checkLifetime(1_120_001)).toBe(false);
            expect(transport.isOpen).toBe(false);
        } finally {
            client?.close();
            vi.useRealTimers();
        }
    });
});

/** Creates one terminal JSON response frame. */
function responsePayload(streamId: number, value: unknown): PayloadFrame {
    return new PayloadFrame(
        streamId,
        PayloadFlag.NEXT | PayloadFlag.COMPLETE,
        undefined,
        dataMimeType.toPayload(value)
    );
}

/** Encodes one opaque frame type with caller-selected header flags. */
function unknownFrame(streamId: number, flags: FrameFlag): Uint8Array {
    const bytes = new Uint8Array(7);
    bytes[0] = streamId >>> 24;
    bytes[1] = streamId >>> 16;
    bytes[2] = streamId >>> 8;
    bytes[3] = streamId;
    const typeAndFlags = (0x10 << 10) | flags;
    bytes[4] = typeAndFlags >>> 8;
    bytes[5] = typeAndFlags;
    bytes[6] = 0xaa;
    return bytes;
}
