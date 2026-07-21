import {runInNewContext} from "node:vm";
import {
    CancelFrame,
    FrameCodec,
    FrameFlag,
    FrameType,
    MAX_FRAME_SIZE,
    RequestResponseFrame,
    WellKnownMimeType,
    type FrameTransport
} from "@/index";
import {LengthPrefixedFrameDecoder} from "@/frame/transport/LengthPrefixedFrameDecoder";

const metadataType = WellKnownMimeType.APPLICATION_OCTET_STREAM;
const payloadType = WellKnownMimeType.APPLICATION_OCTET_STREAM;

function codec(transport: FrameTransport, maxFrameSize?: number): FrameCodec {
    const base = {
        transport,
        mimetype: {metadata: metadataType, data: payloadType}
    } as const;
    return maxFrameSize === undefined
        ? new FrameCodec(base)
        : new FrameCodec({...base, maxFrameSize});
}

function join(...chunks: Uint8Array[]): Uint8Array {
    const joined = new Uint8Array(chunks.reduce((length, chunk) => length + chunk.length, 0));
    let offset = 0;
    for (const chunk of chunks) {
        joined.set(chunk, offset);
        offset += chunk.length;
    }
    return joined;
}

describe("RSocket transport framing", () => {
    test("uses one codec API for unprefixed WebSocket and length-prefixed TCP", () => {
        const frame = new CancelFrame(1);

        expect(codec("websocket").serialize(frame)).toEqual(new Uint8Array([
            0x00, 0x00, 0x00, 0x01,
            FrameType.CANCEL << 2, 0x00
        ]));
        expect(codec("tcp").serialize(frame)).toEqual(new Uint8Array([
            0x00, 0x00, 0x06,
            0x00, 0x00, 0x00, 0x01,
            FrameType.CANCEL << 2, 0x00
        ]));
    });

    test("captures transport configuration instead of retaining mutable options", () => {
        const options: {
            transport: FrameTransport;
            mimetype: {metadata: typeof metadataType; data: typeof payloadType};
        } = {
            transport: "websocket",
            mimetype: {metadata: metadataType, data: payloadType}
        };
        const frameCodec = new FrameCodec(options);
        options.transport = "tcp";

        expect(frameCodec.serialize(new CancelFrame(1))).toHaveLength(6);
    });

    test("rejects unsupported transports and non-byte input", () => {
        expect(() => new FrameCodec({
            transport: "udp" as FrameTransport,
            mimetype: {metadata: metadataType, data: payloadType}
        })).toThrow("Unsupported frame transport");
        expect(() => codec("websocket").deserialize(new ArrayBuffer(6) as never))
            .toThrow("Uint8Array");
    });

    test.each(["websocket", "tcp"] as const)("round-trips one frame over %s", transport => {
        const frameCodec = codec(transport);
        const encoded = frameCodec.serialize(new CancelFrame(31));
        const decoded = frameCodec.deserialize(encoded);

        expect(decoded).toHaveLength(1);
        expect(decoded[0]).toBeInstanceOf(CancelFrame);
        expect(decoded[0]!.header.streamId).toBe(31);
        expect(frameCodec.serialize(decoded[0]!)).toEqual(encoded);
        frameCodec.finish();
    });

    test("correctly reads Uint8Array views with a non-zero byte offset", () => {
        const frameCodec = codec("websocket");
        const frame = frameCodec.serialize(new CancelFrame(9));
        const container = new Uint8Array(frame.length + 8);
        container.set(frame, 4);

        const [decoded] = frameCodec.deserialize(container.subarray(4, 4 + frame.length));

        expect(decoded!.header.streamId).toBe(9);
        expect(decoded!.type).toBe(FrameType.CANCEL);
        expect(decoded!.toUint8Array().buffer).not.toBe(container.buffer);
    });

    test.each(["websocket", "tcp"] as const)("accepts Uint8Array chunks from another realm over %s", transport => {
        const frameCodec = codec(transport);
        const encoded = frameCodec.serialize(new CancelFrame(13));
        const foreign = runInNewContext("Uint8Array.from(bytes)", {bytes: Array.from(encoded)}) as Uint8Array;

        const [decoded] = frameCodec.deserialize(foreign);

        expect(decoded).toBeInstanceOf(CancelFrame);
        expect(decoded!.header.streamId).toBe(13);
        frameCodec.finish();
    });

    test("does not retain a pooled TCP chunk around one decoded frame", () => {
        const packet = codec("tcp").serialize(new CancelFrame(11));
        const pooled = new Uint8Array(packet.length + 1024);
        pooled.set(packet, 128);
        const decoder = codec("tcp");

        const [decoded] = decoder.deserialize(pooled.subarray(128, 128 + packet.length));
        const retained = decoded!.toUint8Array();

        expect(retained).toHaveLength(6);
        expect(retained.buffer).not.toBe(pooled.buffer);
    });

    test("reassembles a TCP frame split at every byte boundary", () => {
        const packet = codec("tcp").serialize(new CancelFrame(101));

        for (let split = 0; split <= packet.length; split++) {
            const frameCodec = codec("tcp");
            const first = frameCodec.deserialize(packet.subarray(0, split));
            const second = frameCodec.deserialize(packet.subarray(split));

            expect(first.length + second.length).toBe(1);
            expect((first[0] ?? second[0])!.header.streamId).toBe(101);
            frameCodec.finish();
        }
    });

    test("reassembles one-byte TCP reads", () => {
        const packet = codec("tcp").serialize(new CancelFrame(103));
        const frameCodec = codec("tcp");
        const frames = Array.from(packet).flatMap(byte => frameCodec.deserialize(new Uint8Array([byte])));

        expect(frames).toHaveLength(1);
        expect(frames[0]).toBeInstanceOf(CancelFrame);
        expect(frames[0]!.header.streamId).toBe(103);
        frameCodec.finish();
    });

    test("decodes coalesced TCP frames and retains an incomplete tail", () => {
        const encoder = codec("tcp");
        const packets = [1, 3, 5].map(streamId => encoder.serialize(new CancelFrame(streamId)));
        const joined = join(...packets);
        const decoder = codec("tcp");

        expect(decoder.deserialize(joined.subarray(0, joined.length - 2)).map(frame => frame.header.streamId))
            .toEqual([1, 3]);
        expect(decoder.deserialize(joined.subarray(joined.length - 2)).map(frame => frame.header.streamId))
            .toEqual([5]);
        decoder.finish();
    });

    test("completes one pending TCP packet before decoding the remaining chunk", () => {
        const encoder = codec("tcp");
        const packets = [1, 3, 5].map(streamId => encoder.serialize(new CancelFrame(streamId)));
        const decoder = codec("tcp");

        expect(decoder.deserialize(packets[0]!.subarray(0, 1))).toEqual([]);
        const frames = decoder.deserialize(join(packets[0]!.subarray(1), packets[1]!, packets[2]!));

        expect(frames.map(frame => frame.header.streamId)).toEqual([1, 3, 5]);
        decoder.finish();
    });

    test("preserves frame order across deterministic randomized TCP chunks", () => {
        const expected = Array.from({length: 100}, (_, index) => index * 2 + 1);
        const encoder = codec("tcp");
        const bytes = join(...expected.map(streamId => encoder.serialize(new CancelFrame(streamId))));

        for (let seed = 1; seed <= 12; seed++) {
            const decoder = codec("tcp");
            const actual: number[] = [];
            let state = seed;
            let offset = 0;
            while (offset < bytes.length) {
                state = state * 1_664_525 + 1_013_904_223 >>> 0;
                const end = Math.min(bytes.length, offset + 1 + state % 37);
                actual.push(...decoder.deserialize(bytes.subarray(offset, end)).map(frame => frame.header.streamId));
                offset = end;
            }
            decoder.finish();
            expect(actual).toEqual(expected);
        }
    });

    test("releases a large pending TCP allocation after completion and finish", () => {
        const data = new Uint8Array(128 * 1024);
        const packet = codec("tcp").serialize(new RequestResponseFrame(
            1,
            FrameFlag.NONE,
            undefined,
            payloadType.toPayload(data)
        ));
        const decoder = new LengthPrefixedFrameDecoder(metadataType, payloadType);

        expect(decoder.push(packet.subarray(0, packet.length - 1))).toEqual([]);
        expect(decoder.push(packet.subarray(packet.length - 1))).toHaveLength(1);
        expect((decoder as unknown as {stream: {storage: Uint8Array}}).stream.storage.byteLength).toBe(0);

        decoder.push(packet.subarray(0, 2));
        decoder.reset();
        expect((decoder as unknown as {stream: {storage: Uint8Array}}).stream.storage.byteLength).toBe(0);

        const small = new LengthPrefixedFrameDecoder(metadataType, payloadType);
        const cancel = codec("tcp").serialize(new CancelFrame(1));
        small.push(cancel.subarray(0, 2));
        small.push(cancel.subarray(2));
        expect((small as unknown as {stream: {storage: Uint8Array}}).stream.storage.byteLength).toBeGreaterThan(0);
        small.finish();
        expect((small as unknown as {stream: {storage: Uint8Array}}).stream.storage.byteLength).toBe(0);
    });

    test("rejects malformed, incomplete, and coalesced WebSocket framing", () => {
        const tcp = codec("tcp");
        expect(tcp.deserialize(new Uint8Array([0, 0]))).toEqual([]);
        expect(() => tcp.finish()).toThrow("incomplete");
        tcp.finish();

        expect(() => codec("tcp").deserialize(new Uint8Array([0, 0, 5]))).toThrow("RSocket frame size");

        const websocket = codec("websocket");
        const raw = websocket.serialize(new CancelFrame(1));
        expect(() => websocket.deserialize(join(raw, raw))).toThrow("unexpected trailing byte");
    });

    test("enforces a defensive frame limit before buffering a TCP body", () => {
        const decoder = new LengthPrefixedFrameDecoder(metadataType, payloadType, {maxFrameSize: 6});
        decoder.push(new Uint8Array([0]));
        expect((decoder as unknown as {stream: {storage: Uint8Array}}).stream.storage.byteLength).toBeLessThanOrEqual(9);
        decoder.reset();
        expect(() => decoder.push(new Uint8Array([0, 0, 7]))).toThrow("RSocket frame size");
        expect(() => codec("tcp", MAX_FRAME_SIZE + 1)).toThrow("maxFrameSize");

        const limitedWebSocket = codec("websocket", 6);
        expect(() => limitedWebSocket.serialize(new RequestResponseFrame(
            1,
            FrameFlag.NONE,
            undefined,
            payloadType.toPayload(new Uint8Array([1]))
        ))).toThrow("RSocket frame size");
    });

    test("does not reserve an unreceived TCP body from its length prefix", () => {
        const frameSize = 8 * 1024 * 1024;
        const decoder = new LengthPrefixedFrameDecoder(metadataType, payloadType);

        expect(decoder.push(new Uint8Array([
            frameSize >>> 16,
            frameSize >>> 8 & 0xff,
            frameSize & 0xff
        ]))).toEqual([]);

        expect((decoder as unknown as {stream: {storage: Uint8Array}}).stream.storage.byteLength)
            .toBeLessThanOrEqual(128);
        decoder.reset();
    });
});
