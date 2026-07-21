import type {Frame} from "@/frame/Frame";
import {FrameDeserializer} from "@/frame/FrameDeserializer";
import {byteView} from "@/binary";
import {
    FRAME_HEADER_SIZE,
    FRAME_LENGTH_PREFIX_SIZE,
    MAX_FRAME_SIZE
} from "@/frame/transport/constants";
import {assertFrameSize, readFrameSize} from "@/frame/transport/framing";
import type {MimeType} from "@/mimetype/MimeType";
import {assertInteger} from "@/utils";

const EMPTY = new Uint8Array(0);
const MAX_RETAINED_BUFFER_SIZE = 64 * 1024;

/** Configuration for a stateful length-prefixed frame decoder. */
export interface LengthPrefixedFrameDecoderOptions {
    /** Largest accepted frame, excluding the three-byte prefix. */
    readonly maxFrameSize?: number;
}

/** Raw TCP frame splitter used by protocol engines before MIME negotiation. */
export interface RawLengthPrefixedFrameDecoder {
    /** Number of unconsumed transport bytes retained between reads. */
    readonly bufferedBytes: number;
    /** Emits every complete raw RSocket frame found in one TCP chunk. */
    push(chunk: Uint8Array, emit: (frame: Uint8Array) => void): void;
    /** Verifies that the stream ended on a frame boundary. */
    finish(): void;
    /** Releases any incomplete packet. */
    reset(): void;
}

/**
 * Reassembles and decodes RSocket frames from arbitrary byte-stream chunks.
 *
 * TCP may split a prefix or frame across reads and may coalesce several frames
 * into one read. This adapter delegates packet boundaries to the same raw
 * splitter used by higher-level protocol engines.
 */
export class LengthPrefixedFrameDecoder {
    private readonly stream: RawLengthPrefixedFrameDecoder;

    /** Creates a typed TCP stream decoder using negotiated MIME codecs. */
    public constructor(
        private readonly metadataType: MimeType<any>,
        private readonly payloadType: MimeType<any>,
        options: LengthPrefixedFrameDecoderOptions = {}
    ) {
        this.stream = createRawLengthPrefixedFrameDecoder(options);
    }

    /** Number of unconsumed bytes waiting for the rest of a frame. */
    public get bufferedBytes(): number {
        return this.stream.bufferedBytes;
    }

    /** Consumes one chunk and returns all decoded frames in wire order. */
    public push(chunk: Uint8Array): Frame[] {
        const frames: Frame[] = [];
        this.stream.push(chunk, (frame) => {
            frames.push(FrameDeserializer.deserialize(frame, this.metadataType, this.payloadType));
        });
        return frames;
    }

    /** Verifies that the TCP stream ended on a frame boundary. */
    public finish(): void {
        this.stream.finish();
    }

    /** Discards buffered bytes after a transport failure. */
    public reset(): void {
        this.stream.reset();
    }
}

/** Creates the allocation-light raw splitter shared by frame and protocol codecs. */
export function createRawLengthPrefixedFrameDecoder(
    options: LengthPrefixedFrameDecoderOptions = {}
): RawLengthPrefixedFrameDecoder {
    return new LengthPrefixedByteDecoder(options.maxFrameSize ?? MAX_FRAME_SIZE);
}

/** Stateful packet-boundary decoder independent of application MIME codecs. */
class LengthPrefixedByteDecoder implements RawLengthPrefixedFrameDecoder {
    private storage = EMPTY;
    private buffered = 0;

    /** Validates the defensive frame limit once. */
    constructor(private readonly maxFrameSize: number) {
        assertInteger("maxFrameSize", maxFrameSize, FRAME_HEADER_SIZE, MAX_FRAME_SIZE);
    }

    /** Number of transport bytes retained for an incomplete packet. */
    get bufferedBytes(): number {
        return this.buffered;
    }

    /** Consumes one TCP chunk and emits complete raw frames without an array allocation. */
    push(chunk: Uint8Array, emit: (frame: Uint8Array) => void): void {
        const input = byteView(chunk);
        if (input === undefined) throw new TypeError("Frame decoder input must be a Uint8Array");
        if (input.length === 0) return;

        try {
            let offset = this.buffered === 0 ? 0 : this.completePendingFrame(input, emit);
            if (this.buffered !== 0) return;
            offset = this.decodeCompleteFrames(input, offset, emit);
            if (offset < input.length) this.append(input.subarray(offset));
        } catch (error) {
            this.reset();
            throw error;
        }
    }

    /** Rejects a stream that ends inside a prefix or frame. */
    finish(): void {
        if (this.buffered === 0) {
            this.reset();
            return;
        }
        const buffered = this.buffered;
        this.reset();
        throw new RangeError(`Length-prefixed stream ended with ${buffered} incomplete byte(s)`);
    }

    /** Releases an incomplete packet after failure or cancellation. */
    reset(): void {
        this.storage = EMPTY;
        this.buffered = 0;
    }

    /** Completes one packet retained from previous chunks. */
    private completePendingFrame(chunk: Uint8Array, emit: (frame: Uint8Array) => void): number {
        let consumed = 0;
        if (this.buffered < FRAME_LENGTH_PREFIX_SIZE) {
            const count = Math.min(FRAME_LENGTH_PREFIX_SIZE - this.buffered, chunk.length);
            this.append(chunk.subarray(0, count));
            consumed = count;
            if (this.buffered < FRAME_LENGTH_PREFIX_SIZE) return consumed;
        }

        const frameSize = readFrameSize(this.storage);
        assertFrameSize(frameSize, this.maxFrameSize);
        const packetSize = FRAME_LENGTH_PREFIX_SIZE + frameSize;
        const count = Math.min(packetSize - this.buffered, chunk.length - consumed);
        if (count > 0) {
            this.append(chunk.subarray(consumed, consumed + count), packetSize);
            consumed += count;
        }
        if (this.buffered < packetSize) return consumed;

        const frame = this.storage.length === packetSize && packetSize > MAX_RETAINED_BUFFER_SIZE
            ? this.storage.subarray(FRAME_LENGTH_PREFIX_SIZE, packetSize)
            : this.storage.slice(FRAME_LENGTH_PREFIX_SIZE, packetSize);
        this.clearPending();
        emit(frame);
        return consumed;
    }

    /** Emits complete frames directly from a fresh network chunk. */
    private decodeCompleteFrames(
        chunk: Uint8Array,
        initial: number,
        emit: (frame: Uint8Array) => void
    ): number {
        let offset = initial;
        while (chunk.length - offset >= FRAME_LENGTH_PREFIX_SIZE) {
            const frameSize = readFrameSize(chunk, offset);
            assertFrameSize(frameSize, this.maxFrameSize);
            const packetSize = FRAME_LENGTH_PREFIX_SIZE + frameSize;
            if (chunk.length - offset < packetSize) {
                this.append(chunk.subarray(offset), packetSize);
                return chunk.length;
            }

            const standalonePacket = offset === 0 &&
                packetSize === chunk.length &&
                chunk.byteOffset === 0 &&
                chunk.buffer.byteLength === chunk.byteLength;
            emit(standalonePacket
                ? chunk.subarray(FRAME_LENGTH_PREFIX_SIZE)
                : chunk.slice(offset + FRAME_LENGTH_PREFIX_SIZE, offset + packetSize));
            offset += packetSize;
        }
        return offset;
    }

    /** Appends bytes to geometrically growing storage bounded by the known packet size. */
    private append(
        chunk: Uint8Array,
        capacityLimit = FRAME_LENGTH_PREFIX_SIZE + this.maxFrameSize
    ): void {
        const required = this.buffered + chunk.length;
        if (required > capacityLimit) {
            throw new RangeError(`Length-prefixed packet exceeds the configured ${this.maxFrameSize}-byte frame limit`);
        }
        if (required > this.storage.length) {
            let capacity = Math.max(this.storage.length, Math.min(128, capacityLimit));
            while (capacity < required) capacity = Math.min(capacity * 2, capacityLimit);
            const next = new Uint8Array(capacity);
            next.set(this.storage.subarray(0, this.buffered));
            this.storage = next;
        }
        this.storage.set(chunk, this.buffered);
        this.buffered = required;
    }

    /** Reuses only modest buffers after completing a pending packet. */
    private clearPending(): void {
        this.buffered = 0;
        if (this.storage.length > MAX_RETAINED_BUFFER_SIZE) this.storage = EMPTY;
    }
}
