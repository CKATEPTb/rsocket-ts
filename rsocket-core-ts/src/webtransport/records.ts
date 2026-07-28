/** Ordered record framing used inside reliable WebTransport byte streams. */
import {
    RSocketFrameSizeError,
    RSocketProtocolError
} from "@/errors/index.js";
import {DEFAULT_MAX_FRAME_LENGTH} from "@/protocol/constants.js";
import {RSOCKET_WEBTRANSPORT_RECORD_HEADER_LENGTH} from "@/webtransport/constants.js";

/** Largest unsigned ordinal representable by the mapping header. */
const MAX_ORDINAL = 0xffff_ffff_ffff_ffffn;
/** Smallest complete raw RSocket frame. */
const MIN_FRAME_LENGTH = 6;

/** Receives a mapping ordinal and an optional RSocket frame. */
export type RSocketWebTransportRecordConsumer = (
    ordinal: bigint,
    frame: Uint8Array | undefined,
    skippedFireAndForgetStreamId: number | undefined
) => void;

/** Encodes one globally ordered RSocket frame record. */
export function encodeWebTransportRecord(
    ordinal: bigint,
    frame: Uint8Array,
    maxFrameLength = DEFAULT_MAX_FRAME_LENGTH
): Uint8Array {
    assertOrdinal(ordinal);
    assertFrameLength(frame.byteLength, maxFrameLength);
    const packet = new Uint8Array(RSOCKET_WEBTRANSPORT_RECORD_HEADER_LENGTH + frame.byteLength);
    writeOrdinal(packet, ordinal);
    writeLength(packet, frame.byteLength);
    packet.set(frame, RSOCKET_WEBTRANSPORT_RECORD_HEADER_LENGTH);
    return packet;
}

/** Encodes a reliable marker that advances past a lost best-effort datagram. */
export function encodeWebTransportSkipRecord(ordinal: bigint, streamId: number): Uint8Array {
    assertOrdinal(ordinal);
    if (!Number.isInteger(streamId) || streamId <= 0 || streamId > 0x7fff_ffff) {
        throw new RSocketProtocolError("Skipped WebTransport FNF requires a positive 31-bit stream ID");
    }
    const packet = new Uint8Array(RSOCKET_WEBTRANSPORT_RECORD_HEADER_LENGTH + 4);
    writeOrdinal(packet, ordinal);
    writeLength(packet, 4);
    packet[11] = streamId >>> 24;
    packet[12] = streamId >>> 16;
    packet[13] = streamId >>> 8;
    packet[14] = streamId;
    return packet;
}

/** Incrementally decodes split or coalesced reliable-stream records. */
export class RSocketWebTransportRecordDecoder {
    private readonly header = new Uint8Array(RSOCKET_WEBTRANSPORT_RECORD_HEADER_LENGTH);
    private headerOffset = 0;
    private body: Uint8Array | undefined;
    private bodyOffset = 0;
    private ordinal = 0n;
    private recordLength = 0;

    /** Configures the largest accepted raw RSocket frame. */
    constructor(private readonly maxFrameLength = DEFAULT_MAX_FRAME_LENGTH) {
        assertMaximumFrameLength(maxFrameLength);
    }

    /** Bytes retained for one incomplete record. */
    get bufferedBytes(): number {
        return this.body === undefined
            ? this.headerOffset
            : RSOCKET_WEBTRANSPORT_RECORD_HEADER_LENGTH + this.body.byteLength;
    }

    /** Consumes an arbitrary byte chunk and emits every complete record. */
    push(chunk: Uint8Array, emit: RSocketWebTransportRecordConsumer): void {
        if (chunk.byteLength === 0) return;
        let offset = 0;
        while (offset < chunk.byteLength) {
            const body = this.body;
            if (body !== undefined) {
                const copied = Math.min(body.byteLength - this.bodyOffset, chunk.byteLength - offset);
                body.set(chunk.subarray(offset, offset + copied), this.bodyOffset);
                this.bodyOffset += copied;
                offset += copied;
                if (this.bodyOffset !== body.byteLength) return;
                this.emitPendingBody(emit);
                continue;
            }

            const remaining = chunk.byteLength - offset;
            if (this.headerOffset !== 0 || remaining < RSOCKET_WEBTRANSPORT_RECORD_HEADER_LENGTH) {
                const copied = Math.min(
                    RSOCKET_WEBTRANSPORT_RECORD_HEADER_LENGTH - this.headerOffset,
                    remaining
                );
                this.header.set(chunk.subarray(offset, offset + copied), this.headerOffset);
                this.headerOffset += copied;
                offset += copied;
                if (this.headerOffset !== RSOCKET_WEBTRANSPORT_RECORD_HEADER_LENGTH) return;
                this.startPendingBody(this.header, 0);
                this.headerOffset = 0;
                continue;
            }

            const length = readLength(chunk, offset);
            if (length !== 4) assertFrameLength(length, this.maxFrameLength);
            const packetLength = RSOCKET_WEBTRANSPORT_RECORD_HEADER_LENGTH + length;
            if (remaining < packetLength) {
                this.startPendingBody(chunk, offset);
                offset += RSOCKET_WEBTRANSPORT_RECORD_HEADER_LENGTH;
                continue;
            }

            const ordinal = readOrdinal(chunk, offset);
            const bodyOffset = offset + RSOCKET_WEBTRANSPORT_RECORD_HEADER_LENGTH;
            offset += packetLength;
            emit(
                ordinal,
                length === 4 ? undefined : standaloneFrame(chunk, bodyOffset, length),
                length === 4 ? readSkippedStreamId(chunk, bodyOffset) : undefined
            );
        }
    }

    /** Rejects a reliable stream ending inside a mapping record. */
    finish(): void {
        const length = this.body === undefined
            ? this.headerOffset
            : RSOCKET_WEBTRANSPORT_RECORD_HEADER_LENGTH + this.bodyOffset;
        this.reset();
        if (length !== 0) {
            throw new RSocketProtocolError(
                `RSocket WebTransport stream ended with ${length} incomplete record byte(s)`
            );
        }
    }

    /** Releases an incomplete record after cancellation or failure. */
    reset(): void {
        this.headerOffset = 0;
        this.body = undefined;
        this.bodyOffset = 0;
        this.ordinal = 0n;
        this.recordLength = 0;
    }

    /** Validates one decoded header and allocates its body exactly once. */
    private startPendingBody(bytes: Uint8Array, offset: number): void {
        const length = readLength(bytes, offset);
        if (length !== 4) assertFrameLength(length, this.maxFrameLength);
        this.ordinal = readOrdinal(bytes, offset);
        this.recordLength = length;
        this.body = new Uint8Array(length);
        this.bodyOffset = 0;
    }

    /** Emits and releases one body assembled from multiple transport chunks. */
    private emitPendingBody(emit: RSocketWebTransportRecordConsumer): void {
        const body = this.body as Uint8Array;
        const ordinal = this.ordinal;
        const length = this.recordLength;
        this.body = undefined;
        this.bodyOffset = 0;
        this.ordinal = 0n;
        this.recordLength = 0;
        emit(
            ordinal,
            length === 4 ? undefined : body,
            length === 4 ? readSkippedStreamId(body, 0) : undefined
        );
    }
}

/** Reads the stream ID carried by a reliable best-effort FNF marker. */
function readSkippedStreamId(bytes: Uint8Array, offset: number): number {
    const streamId = ((bytes[offset] as number) << 24) |
        ((bytes[offset + 1] as number) << 16) |
        ((bytes[offset + 2] as number) << 8) |
        (bytes[offset + 3] as number);
    if (streamId <= 0) {
        throw new RSocketProtocolError("Skipped WebTransport FNF marker has an invalid stream ID");
    }
    return streamId;
}

/** Writes one unsigned 64-bit ordinal in network byte order. */
export function writeWebTransportOrdinal(bytes: Uint8Array, offset: number, ordinal: bigint): void {
    assertOrdinal(ordinal);
    for (let index = 7; index >= 0; index -= 1) {
        bytes[offset + index] = Number(ordinal & 0xffn);
        ordinal >>= 8n;
    }
}

/** Reads one unsigned 64-bit ordinal in network byte order. */
export function readWebTransportOrdinal(bytes: Uint8Array, offset: number): bigint {
    if (offset < 0 || bytes.byteLength - offset < 8) {
        throw new RSocketProtocolError("RSocket WebTransport ordinal is incomplete");
    }
    return readOrdinal(bytes, offset);
}

/** Writes an ordinal at the start of a reliable record. */
function writeOrdinal(bytes: Uint8Array, ordinal: bigint): void {
    writeWebTransportOrdinal(bytes, 0, ordinal);
}

/** Reads an ordinal without repeating public bounds checks in the decoder loop. */
function readOrdinal(bytes: Uint8Array, offset: number): bigint {
    let ordinal = 0n;
    for (let index = 0; index < 8; index += 1) {
        ordinal = (ordinal << 8n) | BigInt(bytes[offset + index] as number);
    }
    return ordinal;
}

/** Writes a 24-bit raw-frame length after the ordinal. */
function writeLength(bytes: Uint8Array, length: number): void {
    bytes[8] = length >>> 16;
    bytes[9] = length >>> 8;
    bytes[10] = length;
}

/** Reads a 24-bit raw-frame length at one record offset. */
function readLength(bytes: Uint8Array, offset: number): number {
    return ((bytes[offset + 8] as number) << 16) |
        ((bytes[offset + 9] as number) << 8) |
        (bytes[offset + 10] as number);
}

/** Validates one RSocket frame length against mapping and protocol limits. */
function assertFrameLength(length: number, maxFrameLength: number): void {
    if (length < MIN_FRAME_LENGTH) {
        throw new RSocketProtocolError(`RSocket WebTransport record declares an invalid ${length}-byte frame`);
    }
    if (length > maxFrameLength) throw new RSocketFrameSizeError(length, maxFrameLength);
}

/** Validates the configured defensive limit once. */
function assertMaximumFrameLength(maxFrameLength: number): void {
    if (!Number.isInteger(maxFrameLength) ||
        maxFrameLength < MIN_FRAME_LENGTH ||
        maxFrameLength > DEFAULT_MAX_FRAME_LENGTH) {
        throw new RSocketFrameSizeError(maxFrameLength, DEFAULT_MAX_FRAME_LENGTH);
    }
}

/** Rejects negative or overflowing mapping ordinals. */
function assertOrdinal(ordinal: bigint): void {
    if (ordinal < 0n || ordinal > MAX_ORDINAL) {
        throw new RSocketProtocolError("RSocket WebTransport ordinal exceeds unsigned 64-bit range");
    }
}

/** Avoids retaining a coalesced transport chunk for one small replay frame. */
function standaloneFrame(bytes: Uint8Array, offset: number, length: number): Uint8Array {
    return offset === RSOCKET_WEBTRANSPORT_RECORD_HEADER_LENGTH &&
        length + RSOCKET_WEBTRANSPORT_RECORD_HEADER_LENGTH === bytes.byteLength
        ? bytes.subarray(offset)
        : bytes.slice(offset, offset + length);
}
