/** Deterministic property-style frame and transport roundtrip tests. */
import {
    FrameCodec,
    FrameFlag,
    Metadata,
    PayloadFlag,
    PayloadFrame,
    RequestResponseFlag,
    RequestResponseFrame,
    WellKnownMimeType,
    type Frame
} from "@/index";

const binaryMimeType = WellKnownMimeType.APPLICATION_OCTET_STREAM;
const CASES = 256;
const EMPTY_BYTES = new Uint8Array(0);

describe("deterministic frame roundtrips", () => {
    test("preserves randomized payload shapes over WebSocket and arbitrarily chunked TCP", () => {
        const random = pseudoRandom(0x5eedc0de);

        for (let index = 0; index < CASES; index += 1) {
            const frame = randomFrame(random, index);
            const websocket = codec("websocket");
            const tcp = codec("tcp");
            const websocketBytes = websocket.serialize(frame);
            const tcpBytes = tcp.serialize(frame);

            const [websocketFrame] = websocket.deserialize(websocketBytes);
            const tcpFrames: Frame[] = [];
            let offset = 0;
            while (offset < tcpBytes.length) {
                const length = Math.min(tcpBytes.length - offset, 1 + random(31));
                tcpFrames.push(...tcp.deserialize(tcpBytes.subarray(offset, offset + length)));
                offset += length;
            }
            tcp.finish();

            expect(websocketFrame).toBeDefined();
            expect(tcpFrames).toHaveLength(1);
            assertEquivalent(websocketFrame as Frame, frame);
            assertEquivalent(tcpFrames[0] as Frame, frame);
            expect(websocket.serialize(websocketFrame as Frame)).toEqual(websocketBytes);
            expect(tcp.serialize(tcpFrames[0] as Frame)).toEqual(tcpBytes);
        }
    });
});

/** Creates one binary frame codec for a selected transport. */
function codec(transport: "websocket" | "tcp"): FrameCodec {
    return new FrameCodec({
        transport,
        mimetype: {metadata: binaryMimeType, data: binaryMimeType}
    });
}

/** Creates a valid randomized REQUEST_RESPONSE or PAYLOAD frame. */
function randomFrame(random: (maximum: number) => number, index: number): Frame {
    const streamId = ((random(0x3fffffff) << 1) | 1) >>> 0;
    const metadata = random(3) === 0
        ? undefined
        : new Metadata(binaryMimeType, randomBytes(random, random(513)));
    const payload = random(3) === 0
        ? undefined
        : binaryMimeType.toPayload(randomBytes(random, random(1_025)));

    if ((index & 1) === 0) {
        const flags = random(2) === 0 ? RequestResponseFlag.NONE : RequestResponseFlag.FOLLOWS;
        return new RequestResponseFrame(streamId, flags, metadata, payload);
    }

    let flags = FrameFlag.NONE;
    if (random(2) !== 0) flags |= PayloadFlag.NEXT;
    if (random(2) !== 0) flags |= PayloadFlag.COMPLETE;
    if (random(2) !== 0) flags |= PayloadFlag.FOLLOWS;
    return new PayloadFrame(streamId, flags, metadata, payload);
}

/** Asserts semantic fields and payload bytes independent of transport framing. */
function assertEquivalent(actual: Frame, expected: Frame): void {
    expect(actual.type).toBe(expected.type);
    expect(actual.header.streamId).toBe(expected.header.streamId);
    expect(actual.header.flags).toBe(expected.header.flags);
    expect(partBytes(actual.metadata)).toEqual(partBytes(expected.metadata));
    expect(payloadBytes(actual.payload)).toEqual(payloadBytes(expected.payload));
}

/** Returns serialized bytes for one optional frame part. */
function partBytes(part: {toUint8Array(): Uint8Array} | undefined): Uint8Array | undefined {
    return part?.toUint8Array();
}

/** Normalizes absent data and zero bytes, which have the same RSocket wire representation. */
function payloadBytes(part: {toUint8Array(): Uint8Array} | undefined): Uint8Array {
    return part?.toUint8Array() ?? EMPTY_BYTES;
}

/** Produces deterministic byte content of a selected length. */
function randomBytes(random: (maximum: number) => number, length: number): Uint8Array {
    return Uint8Array.from({length}, () => random(256));
}

/** Creates a deterministic unsigned xorshift generator bounded to `[0, maximum)`. */
function pseudoRandom(seed: number): (maximum: number) => number {
    let state = seed >>> 0;
    return (maximum) => {
        state ^= state << 13;
        state ^= state >>> 17;
        state ^= state << 5;
        return (state >>> 0) % maximum;
    };
}
