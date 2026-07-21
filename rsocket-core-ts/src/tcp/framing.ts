/** RSocket's 24-bit frame-length adapter for stream-oriented transports. */
import {FrameCodec} from "rsocket-frames-ts";
import {RSocketFrameSizeError, RSocketProtocolError} from "@/errors/index.js";
import {DEFAULT_MAX_FRAME_LENGTH} from "@/protocol/constants.js";
import {
    assertTcpFrameLength,
    TCP_FRAME_HEADER_LENGTH,
    TCP_FRAME_PREFIX_LENGTH,
    writeTcpFrameLength
} from "@/tcp/prefix.js";

/** Callback receiving one complete RSocket frame without its length prefix. */
export type TcpFrameConsumer = (frame: Uint8Array) => void;

/** Thin Core error adapter around the canonical frame-package TCP splitter. */
export class TcpFrameDecoder {
    private readonly decoder: ReturnType<typeof FrameCodec.createTcpStreamDecoder>;

    /** Validates the defensive raw-frame limit once. */
    constructor(private readonly maxFrameLength = DEFAULT_MAX_FRAME_LENGTH) {
        if (!Number.isInteger(maxFrameLength) ||
            maxFrameLength < TCP_FRAME_HEADER_LENGTH ||
            maxFrameLength > DEFAULT_MAX_FRAME_LENGTH) {
            throw new RSocketFrameSizeError(maxFrameLength, DEFAULT_MAX_FRAME_LENGTH);
        }
        this.decoder = FrameCodec.createTcpStreamDecoder(maxFrameLength);
    }

    /** Number of transport bytes retained for an incomplete packet. */
    get bufferedBytes(): number {
        return this.decoder.bufferedBytes;
    }

    /** Consumes a TCP chunk and emits every complete frame in order. */
    push(chunk: Uint8Array, emit: TcpFrameConsumer): void {
        try {
            this.decoder.push(chunk, emit);
        } catch (error) {
            throw this.normalizeDecoderError(error);
        }
    }

    /** Rejects a stream that ends inside a prefix or frame. */
    finish(): void {
        const buffered = this.decoder.bufferedBytes;
        try {
            this.decoder.finish();
        } catch (error) {
            throw new RSocketProtocolError(
                `TCP stream ended with ${buffered} incomplete framing byte(s)`,
                {cause: error}
            );
        }
    }

    /** Releases an incomplete packet after failure or cancellation. */
    reset(): void {
        this.decoder.reset();
    }

    /** Maps frame-package range failures to Core's transport error taxonomy. */
    private normalizeDecoderError(error: unknown): unknown {
        if (!(error instanceof RangeError)) return error;
        const match = /RSocket frame size.+received (\d+)$/.exec(error.message);
        if (match === null) return new RSocketProtocolError(error.message, {cause: error});
        const length = Number(match[1]);
        return length < TCP_FRAME_HEADER_LENGTH
            ? new RSocketProtocolError(`TCP packet declares an invalid ${length}-byte RSocket frame`, {cause: error})
            : new RSocketFrameSizeError(length, this.maxFrameLength);
    }
}

/** Prefixes one raw RSocket frame for a TCP byte stream. */
export function encodeTcpFrame(
    frame: Uint8Array,
    maxFrameLength = DEFAULT_MAX_FRAME_LENGTH
): Uint8Array {
    const length = frame.byteLength;
    assertTcpFrameLength(length, maxFrameLength);
    const packet = new Uint8Array(TCP_FRAME_PREFIX_LENGTH + length);
    writeTcpFrameLength(packet, length);
    packet.set(frame, TCP_FRAME_PREFIX_LENGTH);
    return packet;
}
