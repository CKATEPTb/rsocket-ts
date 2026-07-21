/** Position-aware outbound frame retention shared by client and server Resume. */
import type {Frame} from "rsocket-frames-ts";
import {RSocketProtocolError} from "@/errors/index.js";

/** One retained frame with half-open implied Resume positions. */
interface ReplayEntry {
    readonly start: bigint;
    readonly end: bigint;
    readonly frame?: Frame;
    readonly bytes: Uint8Array;
}

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
    private readonly entries: Array<ReplayEntry | undefined> = [];
    private readonly maximumBytes: number;
    private readonly retainFrames: boolean;
    private head = 0;
    private retainedBytes = 0;
    private acknowledgedPosition = 0n;

    /** Validates storage options once for the lifetime of the logical session. */
    constructor(options: RSocketReplayBufferOptions = {}) {
        const maximum = options.maxBytes ?? Number.POSITIVE_INFINITY;
        if (maximum !== Number.POSITIVE_INFINITY && (!Number.isSafeInteger(maximum) || maximum <= 0)) {
            throw new RangeError("RSocket Resume maxBytes must be a positive safe integer");
        }
        this.maximumBytes = maximum;
        this.retainFrames = options.retainFrames ?? true;
    }

    /** Earliest exact local position still available for replay. */
    firstAvailablePosition(currentPosition: bigint): bigint {
        return this.entries[this.head]?.start ?? currentPosition;
    }

    /** Retains one positional frame and returns its exclusive end position. */
    record(start: bigint, frame: Frame | undefined, bytes: Uint8Array): bigint {
        if (this.retainedBytes + bytes.byteLength > this.maximumBytes) {
            throw new RSocketProtocolError(
                `RSocket Resume replay buffer exceeded ${this.maximumBytes} bytes`
            );
        }
        const end = start + BigInt(bytes.byteLength);
        this.entries.push(this.retainFrames && frame !== undefined
            ? {start, end, frame, bytes}
            : {start, end, bytes});
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
            const entry = this.entries[index];
            if (entry !== undefined) emit(entry.frame, entry.bytes);
        }
    }

    /** Releases all retained bytes and frame references. */
    clear(): void {
        this.entries.length = 0;
        this.head = 0;
        this.retainedBytes = 0;
        this.acknowledgedPosition = 0n;
    }

    /** Resolves an exact frame boundary to an entry index. */
    private positionIndex(position: bigint, currentPosition: bigint): number {
        if (position < 0n || position > currentPosition) {
            throw new RSocketProtocolError("RSocket peer reported an impossible replay position");
        }
        if (position === currentPosition) return this.entries.length;
        for (let index = this.head; index < this.entries.length; index += 1) {
            const entry = this.entries[index] as ReplayEntry;
            if (entry.start === position) return index;
            if (entry.end === position) return index + 1;
            if (entry.end > position) break;
        }
        throw new RSocketProtocolError("RSocket peer reported a position inside a frame");
    }

    /** Releases acknowledged entries and updates retained byte accounting. */
    private advanceHead(nextHead: number): void {
        for (let index = this.head; index < nextHead; index += 1) {
            const entry = this.entries[index];
            if (entry !== undefined) this.retainedBytes -= entry.bytes.byteLength;
            this.entries[index] = undefined;
        }
        this.head = nextHead;
    }

    /** Compacts sparse storage only after a meaningful consumed prefix. */
    private compact(): void {
        if (this.head === this.entries.length) {
            this.entries.length = 0;
            this.head = 0;
            return;
        }
        if (this.head < 256 || this.head * 2 < this.entries.length) return;
        compactArray(this.entries, this.head);
        this.head = 0;
    }
}

/** Removes an array's consumed prefix without allocating a splice result. */
function compactArray<T>(values: T[], consumed: number): void {
    const remaining = values.length - consumed;
    for (let index = 0; index < remaining; index += 1) values[index] = values[index + consumed] as T;
    values.length = remaining;
}
