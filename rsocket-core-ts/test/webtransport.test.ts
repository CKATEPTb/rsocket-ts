/** RSocket-over-WebTransport mapping codec, ordering, and lane tests. */
import {
    FireAndForgetFlag,
    FrameType,
    PayloadFlag
} from "rsocket-frames-ts";
import {describe, expect, it} from "vitest";
import {
    createWebTransportConnection
} from "@/index.js";
import {
    RSOCKET_WEBTRANSPORT_FNF_DATAGRAM_HEADER_LENGTH,
    RSocketWebTransportDatagramKind,
    RSocketWebTransportStreamKind
} from "@/webtransport/constants.js";
import {RSocketWebTransportOrderedReceiver} from "@/webtransport/ordered.js";
import {
    decodeWebTransportDatagramPreface,
    decodeWebTransportStreamPreface,
    encodeWebTransportDatagram,
    encodeWebTransportStreamPreface
} from "@/webtransport/preface.js";
import {
    encodeWebTransportRecord,
    encodeWebTransportSkipRecord,
    RSocketWebTransportRecordDecoder
} from "@/webtransport/records.js";
import {createWebTransportTestPair} from "../../test-support/webtransport-pair.js";

describe("WebTransport mapping codec", () => {
    it("validates versioned reliable and datagram prefaces", () => {
        const control = encodeWebTransportStreamPreface(RSocketWebTransportStreamKind.CONTROL);
        const media = encodeWebTransportDatagram(
            RSocketWebTransportDatagramKind.MEDIA,
            Uint8Array.of(1, 2, 3)
        );

        expect(decodeWebTransportStreamPreface(control)).toBe(RSocketWebTransportStreamKind.CONTROL);
        expect(decodeWebTransportDatagramPreface(media)).toBe(RSocketWebTransportDatagramKind.MEDIA);
        expect(media.slice(6)).toEqual(Uint8Array.of(1, 2, 3));

        control[0] = 0;
        expect(() => decodeWebTransportStreamPreface(control)).toThrow("invalid magic");
    });

    it("decodes split, coalesced, and skip records without retaining completed bytes", () => {
        const first = rawFrame(0, FrameType.SETUP);
        const second = rawFrame(1, FrameType.REQUEST_RESPONSE, 0, 12);
        const bytes = concatenate(
            encodeWebTransportRecord(0n, first),
            encodeWebTransportSkipRecord(1n, 1),
            encodeWebTransportRecord(2n, second)
        );
        const decoded: Array<{
            ordinal: bigint;
            frame: Uint8Array | undefined;
            skippedFireAndForgetStreamId: number | undefined;
        }> = [];
        const decoder = new RSocketWebTransportRecordDecoder();

        const collect = (
            ordinal: bigint,
            frame: Uint8Array | undefined,
            skippedFireAndForgetStreamId: number | undefined
        ): void => {
            decoded.push({ordinal, frame, skippedFireAndForgetStreamId});
        };
        decoder.push(bytes.subarray(0, 5), collect);
        decoder.push(bytes.subarray(5, 19), collect);
        decoder.push(bytes.subarray(19), collect);
        decoder.finish();

        expect(decoded).toEqual([
            {ordinal: 0n, frame: first, skippedFireAndForgetStreamId: undefined},
            {ordinal: 1n, frame: undefined, skippedFireAndForgetStreamId: 1},
            {ordinal: 2n, frame: second, skippedFireAndForgetStreamId: undefined}
        ]);
        expect(decoder.bufferedBytes).toBe(0);
    });

    it("rejects an incomplete reliable record at stream end", () => {
        const decoder = new RSocketWebTransportRecordDecoder();
        decoder.push(Uint8Array.of(0, 1, 2), () => undefined);
        expect(() => decoder.finish()).toThrow("incomplete record");
    });

    it("decodes a large record from one-byte chunks with one retained body allocation", () => {
        const frame = rawFrame(1, FrameType.REQUEST_RESPONSE, 0, 8 * 1024);
        const record = encodeWebTransportRecord(7n, frame);
        const decoded: Uint8Array[] = [];
        const decoder = new RSocketWebTransportRecordDecoder();

        for (let offset = 0; offset < record.byteLength; offset += 1) {
            decoder.push(record.subarray(offset, offset + 1), (_ordinal, value) => {
                if (value !== undefined) decoded.push(value);
            });
        }
        decoder.finish();

        expect(decoded).toEqual([frame]);
        expect(decoder.bufferedBytes).toBe(0);
    });

    it("restores global order and bounds bytes retained behind a gap", () => {
        const receiver = new RSocketWebTransportOrderedReceiver<string>(8);
        const values: string[] = [];
        receiver.accept(1n, "second", 6, (value) => values.push(value));
        expect(values).toEqual([]);
        expect(receiver.bufferedBytes).toBe(6);
        receiver.accept(0n, "first", 0, (value) => values.push(value));
        expect(values).toEqual(["first", "second"]);
        expect(receiver.bufferedBytes).toBe(0);
        expect(() => receiver.accept(3n, "too large", 9, () => undefined)).toThrow("reorder buffer");
    });
});

describe("ReactiveWebTransportConnection", () => {
    it("maps control, per-request bidi, reliable uni, and reverse-direction frames", async () => {
        const pair = createWebTransportTestPair();
        const requester = createWebTransportConnection(pair.requester, {role: "requester"});
        const responder = createWebTransportConnection(pair.responder, {role: "responder"});
        const requesterFrames: Uint8Array[] = [];
        const responderFrames: Uint8Array[] = [];
        const requesterSubscription = requester.frames.subscribe((frame) => requesterFrames.push(frame));
        const responderSubscription = responder.frames.subscribe((frame) => responderFrames.push(frame));
        await Promise.all([requester.opened.block(), responder.opened.block()]);

        const setup = rawFrame(0, FrameType.SETUP);
        const response = rawFrame(1, FrameType.REQUEST_RESPONSE);
        const stream = rawFrame(3, FrameType.REQUEST_STREAM);
        const channel = rawFrame(5, FrameType.REQUEST_CHANNEL);
        const fnf = rawFrame(7, FrameType.REQUEST_FNF);
        const metadata = rawFrame(0, FrameType.METADATA_PUSH);
        requester.write(setup);
        requester.write(response);
        requester.write(stream);
        requester.write(channel);
        requester.write(fnf);
        requester.write(metadata);
        await eventually(() => responderFrames.length === 6);

        const responsePayload = rawFrame(1, FrameType.PAYLOAD, PayloadFlag.NEXT | PayloadFlag.COMPLETE);
        responder.write(responsePayload);
        await eventually(() => requesterFrames.length === 1);

        expect(responderFrames).toEqual([setup, response, stream, channel, fnf, metadata]);
        expect(requesterFrames).toEqual([responsePayload]);
        const prefaces = pair.writes.filter((write) => write.bytes.byteLength === 6);
        expect(prefaces.filter((write) =>
            write.kind === "bidi" && write.bytes[5] === RSocketWebTransportStreamKind.CONTROL
        )).toHaveLength(1);
        expect(prefaces.filter((write) =>
            write.kind === "bidi" && write.bytes[5] === RSocketWebTransportStreamKind.INTERACTION
        )).toHaveLength(3);
        expect(prefaces.filter((write) =>
            write.kind === "uni" && write.bytes[5] === RSocketWebTransportStreamKind.RELIABLE
        )).toHaveLength(1);
        expect(pair.streamRequests.every(({waitUntilAvailable}) => waitUntilAvailable)).toBe(true);

        requesterSubscription.dispose();
        responderSubscription.dispose();
        requester.close();
    });

    it("withholds later interaction records until an earlier control record arrives", async () => {
        const pair = createWebTransportTestPair({autoFlush: false});
        const requester = createWebTransportConnection(pair.requester, {role: "requester"});
        const responder = createWebTransportConnection(pair.responder, {role: "responder"});
        const received: Uint8Array[] = [];
        requester.frames.subscribe();
        responder.frames.subscribe((frame) => received.push(frame));
        await Promise.all([requester.opened.block(), responder.opened.block()]);
        await nextTurn();

        pair.flush((write) => write.stream === 0 && write.bytes.byteLength === 6);
        await nextTurn();
        const setup = rawFrame(0, FrameType.SETUP);
        const request = rawFrame(1, FrameType.REQUEST_RESPONSE);
        requester.write(setup);
        requester.write(request);
        await nextTurn();

        pair.flush((write) => write.stream !== 0);
        await nextTurn();
        expect(received).toEqual([]);
        pair.flush((write) => write.stream === 0);
        await eventually(() => received.length === 2);
        expect(received).toEqual([setup, request]);
        requester.close();
    });

    it("uses a reliable skip marker when a best-effort FNF datagram is lost", async () => {
        const pair = createWebTransportTestPair({
            dropDatagram: (write) => write.bytes[5] === RSocketWebTransportDatagramKind.FIRE_AND_FORGET
        });
        const requester = createWebTransportConnection(pair.requester, {
            role: "requester",
            unreliableFireAndForget: true
        });
        const responder = createWebTransportConnection(pair.responder, {
            role: "responder",
            unreliableFireAndForget: true
        });
        const received: Uint8Array[] = [];
        requester.frames.subscribe();
        responder.frames.subscribe((frame) => received.push(frame));
        await Promise.all([requester.opened.block(), responder.opened.block()]);

        const setup = rawFrame(0, FrameType.SETUP);
        const fnf = rawFrame(1, FrameType.REQUEST_FNF);
        const response = rawFrame(3, FrameType.REQUEST_RESPONSE);
        requester.write(setup);
        requester.write(fnf);
        requester.write(response);

        await eventually(() => received.length === 2);
        expect(received).toEqual([setup, response]);
        expect(pair.writes.some((write) =>
            write.kind === "datagram" &&
            write.bytes[5] === RSocketWebTransportDatagramKind.FIRE_AND_FORGET
        )).toBe(true);
        requester.close();
    });

    it("keeps fragmented FNF reliable and delivers media outside the frame sequence", async () => {
        const pair = createWebTransportTestPair();
        const requester = createWebTransportConnection(pair.requester, {
            role: "requester",
            unreliableFireAndForget: true
        });
        const responder = createWebTransportConnection(pair.responder, {
            role: "responder",
            unreliableFireAndForget: true
        });
        const received: Uint8Array[] = [];
        const media: Uint8Array[] = [];
        requester.frames.subscribe();
        responder.frames.subscribe((frame) => received.push(frame));
        responder.media.subscribe((payload) => media.push(payload));
        await Promise.all([requester.opened.block(), responder.opened.block()]);

        requester.write(rawFrame(0, FrameType.SETUP));
        requester.write(rawFrame(1, FrameType.REQUEST_FNF, FireAndForgetFlag.FOLLOWS));
        requester.write(rawFrame(1, FrameType.PAYLOAD, PayloadFlag.NEXT));
        requester.writeMedia(Uint8Array.of(9, 8, 7));

        await eventually(() => received.length === 3 && media.length === 1);
        expect(pair.writes.some((write) =>
            write.kind === "datagram" &&
            write.bytes[5] === RSocketWebTransportDatagramKind.FIRE_AND_FORGET
        )).toBe(false);
        expect(media).toEqual([Uint8Array.of(9, 8, 7)]);
        requester.close();
    });

    it("supports the earlier datagram writable while preferring the current API", async () => {
        const pair = createWebTransportTestPair({writableDatagrams: true});
        const requester = createWebTransportConnection(pair.requester, {role: "requester"});
        const responder = createWebTransportConnection(pair.responder, {role: "responder"});
        const media: Uint8Array[] = [];
        requester.frames.subscribe(undefined, () => undefined);
        responder.frames.subscribe(undefined, () => undefined);
        responder.media.subscribe((payload) => media.push(payload));
        await Promise.all([requester.opened.block(), responder.opened.block()]);

        requester.writeMedia(Uint8Array.of(4, 2));

        await eventually(() => media.length === 1);
        expect(media).toEqual([Uint8Array.of(4, 2)]);
        requester.close();
    });

    it("terminates the mapping when a datagram has an invalid preface", async () => {
        const pair = createWebTransportTestPair();
        const requester = createWebTransportConnection(pair.requester, {role: "requester"});
        const responder = createWebTransportConnection(pair.responder, {role: "responder"});
        let failure: unknown;
        requester.frames.subscribe(undefined, () => undefined);
        responder.frames.subscribe(undefined, (error) => {
            failure = error;
        });
        await Promise.all([requester.opened.block(), responder.opened.block()]);
        const writable = pair.requester.datagrams?.createWritable?.();
        if (writable === undefined) throw new Error("Current datagram test API is unavailable");

        await writable.getWriter().write(Uint8Array.of(0, 1, 2));

        await eventually(() => failure !== undefined);
        expect(failure).toHaveProperty("message", "RSocket WebTransport preface is incomplete");
    });

    it("rejects an FNF datagram above the configured raw-frame limit", async () => {
        const pair = createWebTransportTestPair();
        const requester = createWebTransportConnection(pair.requester, {role: "requester"});
        const responder = createWebTransportConnection(pair.responder, {
            role: "responder",
            maxFrameLength: 6,
            unreliableFireAndForget: true
        });
        let failure: unknown;
        requester.frames.subscribe(undefined, () => undefined);
        responder.frames.subscribe(undefined, (error) => {
            failure = error;
        });
        await Promise.all([requester.opened.block(), responder.opened.block()]);
        const writable = pair.requester.datagrams?.createWritable?.();
        if (writable === undefined) throw new Error("Current datagram test API is unavailable");
        const packet = encodeWebTransportDatagram(
            RSocketWebTransportDatagramKind.FIRE_AND_FORGET,
            rawFrame(1, FrameType.REQUEST_FNF, 0, 1),
            RSOCKET_WEBTRANSPORT_FNF_DATAGRAM_HEADER_LENGTH
        );

        await writable.getWriter().write(packet);

        await eventually(() => failure !== undefined);
        expect((failure as Error).message).toContain("exceeds configured maxFrameLength 6");
    });

    it("rejects an FNF skip marker outside the connection-control stream", async () => {
        const pair = createWebTransportTestPair();
        const requester = createWebTransportConnection(pair.requester, {
            role: "requester",
            unreliableFireAndForget: true
        });
        const responder = createWebTransportConnection(pair.responder, {
            role: "responder",
            unreliableFireAndForget: true
        });
        let failure: unknown;
        requester.frames.subscribe(undefined, () => undefined);
        responder.frames.subscribe(undefined, (error) => {
            failure = error;
        });
        await Promise.all([requester.opened.block(), responder.opened.block()]);
        const stream = await pair.requester.createUnidirectionalStream({waitUntilAvailable: true});
        const writer = stream.getWriter();

        await writer.write(encodeWebTransportStreamPreface(RSocketWebTransportStreamKind.RELIABLE));
        await writer.write(encodeWebTransportSkipRecord(0n, 1));

        await eventually(() => failure !== undefined);
        expect(failure).toHaveProperty(
            "message",
            "WebTransport FNF skip marker requires the negotiated control stream"
        );
    });

    it("rejects best-effort FNF on a reliable-only WebTransport session", async () => {
        const pair = createWebTransportTestPair();
        const requester = createWebTransportConnection({
            ...pair.requester,
            reliability: "reliable-only"
        }, {
            role: "requester",
            unreliableFireAndForget: true
        });
        requester.frames.subscribe(undefined, () => undefined);

        await expect(requester.opened.block()).rejects.toThrow("requires an unreliable-capable session");
        expect(pair.streamRequests).toEqual([]);
        await expect(Promise.resolve(pair.responder.closed)).resolves.toBeDefined();
    });

    it("charges reliable skip markers against the reorder memory limit", async () => {
        const pair = createWebTransportTestPair({autoFlush: false});
        const requester = createWebTransportConnection(pair.requester, {
            role: "requester",
            maxFrameLength: 6,
            maxReorderBufferBytes: 6,
            unreliableFireAndForget: true
        });
        const responder = createWebTransportConnection(pair.responder, {
            role: "responder",
            maxFrameLength: 6,
            maxReorderBufferBytes: 6,
            unreliableFireAndForget: true
        });
        const received: Uint8Array[] = [];
        let failure: unknown;
        requester.frames.subscribe(undefined, () => undefined);
        responder.frames.subscribe((frame) => received.push(frame), (error) => {
            failure = error;
        });
        await Promise.all([requester.opened.block(), responder.opened.block()]);
        requester.write(rawFrame(0, FrameType.SETUP));
        await nextTurn();
        pair.flush((write) => write.kind === "bidi" && write.stream === 0);
        await eventually(() => received.length === 1);

        requester.write(rawFrame(1, FrameType.REQUEST_RESPONSE));
        requester.write(rawFrame(3, FrameType.REQUEST_FNF));
        await eventually(() => pair.writes.some((write) =>
            write.kind === "bidi" && write.stream === 0 && write.bytes.byteLength === 15
        ));
        pair.flush((write) =>
            write.kind === "bidi" && write.stream === 0 && write.bytes.byteLength === 15
        );

        await eventually(() => failure !== undefined);
        expect(failure).toHaveProperty("message", "RSocket WebTransport reorder buffer exceeds 6 bytes");
    });
});

/** Builds a raw frame with a valid six-byte RSocket header. */
function rawFrame(streamId: number, type: FrameType, flags = 0, payloadBytes = 0): Uint8Array {
    const frame = new Uint8Array(6 + payloadBytes);
    frame[0] = streamId >>> 24;
    frame[1] = streamId >>> 16;
    frame[2] = streamId >>> 8;
    frame[3] = streamId;
    const typeAndFlags = (type << 10) | flags;
    frame[4] = typeAndFlags >>> 8;
    frame[5] = typeAndFlags;
    return frame;
}

/** Concatenates mapping packets to exercise coalesced stream reads. */
function concatenate(...parts: Uint8Array[]): Uint8Array {
    const result = new Uint8Array(parts.reduce((length, part) => length + part.byteLength, 0));
    let offset = 0;
    for (const part of parts) {
        result.set(part, offset);
        offset += part.byteLength;
    }
    return result;
}

/** Waits until an asynchronous transport condition becomes true. */
async function eventually(condition: () => boolean): Promise<void> {
    for (let attempt = 0; attempt < 100; attempt += 1) {
        if (condition()) return;
        await nextTurn();
    }
    throw new Error("Timed out waiting for WebTransport test condition");
}

/** Yields to native stream and mapping promise continuations. */
function nextTurn(): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, 0));
}
