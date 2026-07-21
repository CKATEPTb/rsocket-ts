/** Throughput and retained-state regressions for shared transport and Resume primitives. */
import {describe, expect, it} from "vitest";
import {CancelFrame} from "rsocket-frames-ts";
import {encodeTcpFrame, RSocketReplayBuffer, TcpFrameDecoder} from "@";

const FRAME_COUNT = 50_000;
const MAX_TEST_DURATION_MS = 10_000;

describe("Core performance primitives", () => {
    it("decodes a large coalesced TCP read in linear time without retained bytes", () => {
        const packet = encodeTcpFrame(new CancelFrame(1).toUint8Array());
        const bytes = repeatedBytes(packet, FRAME_COUNT);
        const decoder = new TcpFrameDecoder();
        const started = performance.now();
        let decoded = 0;

        decoder.push(bytes, () => {
            decoded += 1;
        });
        decoder.finish();

        expect(decoded).toBe(FRAME_COUNT);
        expect(decoder.bufferedBytes).toBe(0);
        expect(performance.now() - started).toBeLessThan(MAX_TEST_DURATION_MS);
    });

    it("releases acknowledged replay entries under sustained traffic", () => {
        const frame = new CancelFrame(1);
        const bytes = frame.toUint8Array();
        const replay = new RSocketReplayBuffer({maxBytes: bytes.byteLength * FRAME_COUNT, retainFrames: false});
        const started = performance.now();
        let position = 0n;

        for (let index = 0; index < FRAME_COUNT; index += 1) {
            position = replay.record(position, frame, bytes);
        }
        replay.acknowledge(position, position);

        const state = replay as unknown as {
            entries: unknown[];
            retainedBytes: number;
            head: number;
        };
        expect(state.entries).toHaveLength(0);
        expect(state.retainedBytes).toBe(0);
        expect(state.head).toBe(0);
        expect(performance.now() - started).toBeLessThan(MAX_TEST_DURATION_MS);
    });
});

/** Repeats one small packet in a single coalesced transport allocation. */
function repeatedBytes(packet: Uint8Array, count: number): Uint8Array {
    const result = new Uint8Array(packet.byteLength * count);
    for (let index = 0; index < count; index += 1) result.set(packet, index * packet.byteLength);
    return result;
}
