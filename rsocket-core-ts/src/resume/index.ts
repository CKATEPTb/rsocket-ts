/** Position-aware outbound frame retention shared by client and server Resume. */
import type {Frame} from "rsocket-frames-ts";
import {RSocketProtocolError} from "@/errors/index.js";

/** Callback writing retained bytes without assigning another implied position. */
export type RSocketReplayEmitter = (frame: Frame | undefined, bytes: Uint8Array) => void;

/** Configuration for bounded replay storage and optional diagnostic frame retention. */
export interface RSocketReplayBufferOptions {
    /** Maximum serialized bytes retained at once; defaults to unbounded. */
    readonly maxBytes?: number;
    /** Whether decoded frame objects are retained alongside serialized bytes. */
    readonly retainFrames?: boolean;
}

/** Bounded replay buffer for either direction of a resumable RSocket session. */
export class RSocketReplayBuffer {
    /** Cumulative numeric end offsets avoid one object and two bigint values per frame. */
    private readonly entries: number[] = [];
    private readonly bytes: Array<Uint8Array | undefined> = [];
    private readonly frames: Array<Frame | undefined> | undefined;
    private readonly maximumBytes: number;
    private head = 0;
    private retainedBytes = 0;
    private originPosition = 0n;
    private endPosition = 0n;
    private acknowledgedPosition = 0n;

    /** Validates storage options once for the lifetime of the logical session. */
    constructor(options: RSocketReplayBufferOptions = {}) {
        const maximum = options.maxBytes ?? Number.POSITIVE_INFINITY;
        if (maximum !== Number.POSITIVE_INFINITY && (!Number.isSafeInteger(maximum) || maximum <= 0)) {
            throw new RangeError("RSocket Resume maxBytes must be a positive safe integer");
        }
        this.maximumBytes = maximum;
        this.frames = options.retainFrames === false ? undefined : [];
    }

    /** Earliest exact local position still available for replay. */
    firstAvailablePosition(currentPosition: bigint): bigint {
        if (this.head >= this.entries.length) return currentPosition;
        return this.head === 0
            ? this.originPosition
            : this.originPosition + BigInt(this.entries[this.head - 1] as number);
    }

    /** Retains one positional frame and returns its exclusive end position. */
    record(start: bigint, frame: Frame | undefined, bytes: Uint8Array): bigint {
        if (this.retainedBytes + bytes.byteLength > this.maximumBytes) {
            throw new RSocketProtocolError(
                `RSocket Resume replay buffer exceeded ${this.maximumBytes} bytes`
            );
        }
        const previousEndOffset = this.entries.length === 0
            ? 0
            : this.entries[this.entries.length - 1] as number;
        if (this.entries.length === 0) this.originPosition = start;
        else if (this.endPosition !== start) {
            throw new RSocketProtocolError("RSocket Resume frames must have contiguous implied positions");
        }
        const endOffset = previousEndOffset + bytes.byteLength;
        if (!Number.isSafeInteger(endOffset)) {
            throw new RSocketProtocolError("RSocket Resume replay span exceeds the safe integer range");
        }
        const end = start + BigInt(bytes.byteLength);
        this.endPosition = end;
        this.entries.push(endOffset);
        this.bytes.push(bytes);
        this.frames?.push(frame);
        this.retainedBytes += bytes.byteLength;
        return end;
    }

    /** Drops frames acknowledged by a monotonic peer position. */
    acknowledge(position: bigint, currentPosition: bigint): void {
        if (position <= this.acknowledgedPosition) return;
        const nextHead = this.positionIndex(position, currentPosition);
        this.acknowledgedPosition = position;
        if (nextHead <= this.head) return;
        this.advanceHead(nextHead);
        this.compact();
    }

    /** Checks whether an exact position remains available for replay. */
    canReplayFrom(position: bigint, currentPosition: bigint): boolean {
        if (position < this.firstAvailablePosition(currentPosition)) return false;
        try {
            this.positionIndex(position, currentPosition);
            return true;
        } catch {
            return false;
        }
    }

    /** Replays the retained snapshot at or after one acknowledged position. */
    replayFrom(position: bigint, currentPosition: bigint, emit: RSocketReplayEmitter): void {
        if (position < this.firstAvailablePosition(currentPosition)) {
            throw new RSocketProtocolError("RSocket Resume requested frames that are no longer retained");
        }
        const nextHead = this.positionIndex(position, currentPosition);
        if (position > this.acknowledgedPosition) this.acknowledgedPosition = position;
        this.advanceHead(nextHead);
        this.compact();

        // Frames recorded reentrantly by `emit` belong after this replay pass.
        const replayEnd = this.entries.length;
        for (let index = this.head; index < replayEnd; index += 1) {
            const bytes = this.bytes[index];
            if (bytes !== undefined) emit(this.frames?.[index], bytes);
        }
    }

    /** Releases all retained bytes and frame references. */
    clear(): void {
        this.entries.length = 0;
        this.bytes.length = 0;
        if (this.frames !== undefined) this.frames.length = 0;
        this.head = 0;
        this.retainedBytes = 0;
        this.originPosition = 0n;
        this.endPosition = 0n;
        this.acknowledgedPosition = 0n;
    }

    /** Resolves an exact frame boundary to an entry index. */
    private positionIndex(position: bigint, currentPosition: bigint): number {
        if (position < 0n || position > currentPosition) {
            throw new RSocketProtocolError("RSocket peer reported an impossible replay position");
        }
        if (position === currentPosition) return this.entries.length;
        const firstAvailable = this.firstAvailablePosition(currentPosition);
        if (position === firstAvailable) return this.head;
        if (position < firstAvailable || position < this.originPosition) {
            throw new RSocketProtocolError("RSocket peer reported a position inside a frame");
        }
        const offsetBigInt = position - this.originPosition;
        const offset = Number(offsetBigInt);
        if (!Number.isSafeInteger(offset) || BigInt(offset) !== offsetBigInt) {
            throw new RSocketProtocolError("RSocket peer reported a position outside the retained replay span");
        }
        let low = this.head;
        let high = this.entries.length;
        while (low < high) {
            const middle = low + Math.floor((high - low) / 2);
            if ((this.entries[middle] as number) < offset) low = middle + 1;
            else high = middle;
        }
        if (this.entries[low] === offset) return low + 1;
        throw new RSocketProtocolError("RSocket peer reported a position inside a frame");
    }

    /** Releases acknowledged entries and updates retained byte accounting. */
    private advanceHead(nextHead: number): void {
        for (let index = this.head; index < nextHead; index += 1) {
            const bytes = this.bytes[index];
            if (bytes !== undefined) this.retainedBytes -= bytes.byteLength;
            this.bytes[index] = undefined;
            if (this.frames !== undefined) this.frames[index] = undefined;
        }
        this.head = nextHead;
    }

    /** Compacts sparse storage only after a meaningful consumed prefix. */
    private compact(): void {
        if (this.head === this.entries.length) {
            this.entries.length = 0;
            this.bytes.length = 0;
            if (this.frames !== undefined) this.frames.length = 0;
            this.head = 0;
            this.originPosition = this.acknowledgedPosition;
            return;
        }
        if (this.head < 256 || this.head * 2 < this.entries.length) return;
        const consumedOffset = this.entries[this.head - 1] as number;
        const remaining = this.entries.length - this.head;
        this.entries.copyWithin(0, this.head);
        this.bytes.copyWithin(0, this.head);
        this.frames?.copyWithin(0, this.head);
        for (let index = 0; index < remaining; index += 1) {
            this.entries[index] = (this.entries[index] as number) - consumedOffset;
        }
        this.entries.length = remaining;
        this.bytes.length = remaining;
        if (this.frames !== undefined) this.frames.length = remaining;
        this.originPosition += BigInt(consumedOffset);
        this.head = 0;
    }
}
