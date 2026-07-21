/** Position and memory-bound tests for the shared Resume replay buffer. */
import {RequestNFrame} from "rsocket-frames-ts";
import {describe, expect, it} from "vitest";
import {RSocketReplayBuffer} from "@";

function entry(streamId: number): readonly [RequestNFrame, Uint8Array] {
    const frame = new RequestNFrame(streamId, 1);
    return [frame, frame.toUint8Array()];
}

describe("Core Resume replay buffer", () => {
    it("ignores stale acknowledgements and reclaims acknowledged bytes", () => {
        const [first, bytes] = entry(1);
        const buffer = new RSocketReplayBuffer({maxBytes: bytes.byteLength});
        const firstEnd = buffer.record(0n, first, bytes);
        buffer.acknowledge(firstEnd, firstEnd);
        const [second, secondBytes] = entry(3);
        const current = buffer.record(firstEnd, second, secondBytes);
        const replayed: Uint8Array[] = [];

        expect(() => buffer.acknowledge(0n, current)).not.toThrow();
        buffer.replayFrom(firstEnd, current, (_frame, value) => replayed.push(value));

        expect(replayed).toEqual([secondBytes]);
        expect(buffer.canReplayFrom(0n, current)).toBe(false);
        expect(buffer.canReplayFrom(firstEnd, current)).toBe(true);
        expect(buffer.canReplayFrom(current, current)).toBe(true);
    });

    it("rejects positions inside frames, impossible positions, and overflow", () => {
        const [frame, bytes] = entry(1);
        const buffer = new RSocketReplayBuffer({maxBytes: bytes.byteLength});
        const current = buffer.record(0n, frame, bytes);

        expect(buffer.canReplayFrom(1n, current)).toBe(false);
        expect(() => buffer.acknowledge(current + 1n, current)).toThrow("impossible");
        expect(() => buffer.record(current, frame, bytes)).toThrow("exceeded");
    });

    it("does not replay frames recorded reentrantly by the replay emitter", () => {
        const [first, firstBytes] = entry(1);
        const [second, secondBytes] = entry(3);
        const [third, thirdBytes] = entry(5);
        const buffer = new RSocketReplayBuffer();
        const firstEnd = buffer.record(0n, first, firstBytes);
        const current = buffer.record(firstEnd, second, secondBytes);
        let nextPosition = current;
        const replayed: Uint8Array[] = [];

        buffer.replayFrom(0n, current, (_frame, bytes) => {
            replayed.push(bytes);
            if (replayed.length === 1) nextPosition = buffer.record(current, third, thirdBytes);
        });

        expect(replayed).toEqual([firstBytes, secondBytes]);
        const later: Uint8Array[] = [];
        buffer.replayFrom(current, nextPosition, (_frame, bytes) => later.push(bytes));
        expect(later).toEqual([thirdBytes]);
    });

    it("can omit decoded frame references while retaining bytes", () => {
        const [frame, bytes] = entry(1);
        const buffer = new RSocketReplayBuffer({retainFrames: false});
        const current = buffer.record(0n, frame, bytes);
        const frames: unknown[] = [];

        buffer.replayFrom(0n, current, (value) => frames.push(value));

        expect(frames).toEqual([undefined]);
    });
});
