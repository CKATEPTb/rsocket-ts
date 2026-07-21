/**
 * Outbound RSocket frame fragmentation helpers.
 *
 * Fragment sizes are calculated from raw RSocket frames. Transport framing,
 * including TCP's 24-bit length field, is deliberately excluded.
 */
import {
    type Frame,
    FrameFlag,
    FrameType,
    Metadata,
    type MimeType,
    Payload,
    PayloadFlag,
    PayloadFrame,
    RequestChannelFrame,
    RequestFireAndForgetFrame,
    RequestResponseFrame,
    RequestStreamFrame
} from "rsocket-frames-ts";
import {RSocketFrameSizeError} from "@/errors/index.js";

/** Serialized raw RSocket frame header length. */
const FRAME_HEADER_LENGTH = 6;
/** Serialized request-n field length used by stream and channel requests. */
const REQUEST_N_LENGTH = 4;
/** Serialized metadata length field emitted when metadata is present. */
const METADATA_LENGTH_FIELD = 3;
/** Continuation PAYLOAD overhead when metadata is present. */
const PAYLOAD_METADATA_OVERHEAD = FRAME_HEADER_LENGTH + METADATA_LENGTH_FIELD;
/** Continuation PAYLOAD overhead when only data is present. */
const PAYLOAD_DATA_OVERHEAD = FRAME_HEADER_LENGTH;
/** Shared empty buffer used to avoid per-request allocations. */
const EMPTY_BYTES = new Uint8Array(0);

/**
 * Raw payload pieces extracted from one outbound frame for fragmentation.
 */
interface OutboundPayloadParts {
    /** Whether the wire frame carries a metadata section, including an empty one. */
    readonly metadataPresent: boolean;
    /** MIME type carried by metadata fragments. */
    readonly metadataMimeType: MimeType<any> | undefined;
    /** Raw metadata bytes, if present. */
    readonly metadataBytes: Uint8Array;
    /** MIME type carried by data fragments. */
    readonly dataMimeType: MimeType<any> | undefined;
    /** Raw data bytes, if present. */
    readonly dataBytes: Uint8Array;
}

/**
 * Callback used to emit one outbound fragment.
 */
type FragmentEmitter = (frame: Frame) => void;

/** Callback used to write one already-serialized outbound frame. */
type SerializedFrameEmitter = (frame: Frame, bytes: Uint8Array) => void;

/**
 * Calculates serialized length for fragmentable frames without allocating an estimate object.
 */
export function outboundFrameLength(frame: Frame): number | undefined {
    const frameType = frame.type;
    if (!isFragmentableFrameType(frameType)) return undefined;
    const metadata = frame.metadata;
    return frameOverhead(frameType, frame.hasMetadata()) +
        (metadata?.toUint8Array().byteLength ?? 0) +
        (frame.payload?.toUint8Array().byteLength ?? 0);
}

/**
 * Splits oversized outbound REQUEST/PAYLOAD frames into RSocket fragments.
 */
export function emitOutboundFrameFragments(
    frame: Frame,
    frameLength: number,
    maxFrameLength: number,
    emit: FragmentEmitter
): void {
    if (frameLength <= maxFrameLength) {
        emit(frame);
        return;
    }
    const frameType = frame.type;
    if (!isFragmentableFrameType(frameType)) throw new RSocketFrameSizeError(frameLength, maxFrameLength);

    emitFragmentFrames(frame, frameLength, maxFrameLength, outboundPayloadParts(frame), emit);
}

/**
 * Serializes one logical outbound frame and fragments it only when required.
 *
 * The fast path serializes exactly once. Oversized fragmentable frames avoid
 * allocating the original full wire buffer when their length can be derived
 * from already-encoded metadata and data payloads.
 */
export function emitSerializedOutboundFrames(
    frame: Frame,
    maxFrameLength: number,
    emit: SerializedFrameEmitter
): void {
    const estimatedLength = outboundFrameLength(frame);
    if (estimatedLength !== undefined && estimatedLength > maxFrameLength) {
        emitSerializedFragments(frame, estimatedLength, maxFrameLength, emit);
        return;
    }

    const bytes = frame.toUint8Array();
    if (bytes.byteLength <= maxFrameLength) {
        emit(frame, bytes);
        return;
    }
    emitSerializedFragments(frame, bytes.byteLength, maxFrameLength, emit);
}

/** Serializes each fragment immediately so callers share one sizing policy. */
function emitSerializedFragments(
    frame: Frame,
    frameLength: number,
    maxFrameLength: number,
    emit: SerializedFrameEmitter
): void {
    emitOutboundFrameFragments(frame, frameLength, maxFrameLength, (fragment) => {
        emit(fragment, fragment.toUint8Array());
    });
}

/**
 * Emits fragments for one oversized outbound frame.
 */
function emitFragmentFrames(
    frame: Frame,
    frameLength: number,
    maxFrameLength: number,
    parts: OutboundPayloadParts,
    emit: FragmentEmitter
): void {
    if (parts.metadataBytes.length === 0 && parts.dataBytes.length === 0) {
        throw new RSocketFrameSizeError(frameLength, maxFrameLength);
    }

    let metadataOffset = 0;
    let dataOffset = 0;
    let first = true;
    const metadataTotal = parts.metadataBytes.length;
    const dataTotal = parts.dataBytes.length;
    const frameType = frame.type;
    const firstMetadataOverhead = frameOverhead(frameType, true);
    const firstDataOverhead = frameOverhead(frameType, false);

    while (metadataOffset < metadataTotal || dataOffset < dataTotal) {
        const metadataOverhead = first ? firstMetadataOverhead : PAYLOAD_METADATA_OVERHEAD;
        const dataOverhead = first ? firstDataOverhead : PAYLOAD_DATA_OVERHEAD;
        const metadataRemaining = metadataTotal - metadataOffset;
        const dataRemaining = dataTotal - dataOffset;
        const includeMetadata = metadataRemaining > 0 || (first && parts.metadataPresent);
        let metadataLength = 0;
        let dataLength = 0;
        const emitsEmptyMetadata = first && includeMetadata && metadataRemaining === 0;

        if (includeMetadata) {
            const metadataCapacity = maxFrameLength - metadataOverhead;
            if (metadataCapacity < 0 || (metadataCapacity === 0 && metadataRemaining > 0)) {
                throw new RSocketFrameSizeError(frameLength, maxFrameLength);
            }
            metadataLength = Math.min(metadataRemaining, metadataCapacity);

            if (metadataLength === metadataRemaining && dataRemaining > 0) {
                const dataCapacity = maxFrameLength - metadataOverhead - metadataLength;
                dataLength = Math.min(dataRemaining, Math.max(0, dataCapacity));
            }
        } else {
            const dataCapacity = maxFrameLength - dataOverhead;
            if (dataCapacity <= 0) throw new RSocketFrameSizeError(frameLength, maxFrameLength);
            dataLength = Math.min(dataRemaining, dataCapacity);
        }

        if (metadataLength <= 0 && dataLength <= 0 && !emitsEmptyMetadata) {
            throw new RSocketFrameSizeError(frameLength, maxFrameLength);
        }

        const follows =
            metadataOffset + metadataLength < metadataTotal ||
            dataOffset + dataLength < dataTotal;
        emit(
            first
                ? initialFragment(
                    frame,
                    parts,
                    includeMetadata ? slicePart(parts.metadataBytes, metadataOffset, metadataLength, true) : undefined,
                    slicePart(parts.dataBytes, dataOffset, dataLength),
                    follows,
                    frameLength,
                    maxFrameLength
                )
                : continuationFragment(
                    frame,
                    parts,
                    includeMetadata ? slicePart(parts.metadataBytes, metadataOffset, metadataLength, true) : undefined,
                    slicePart(parts.dataBytes, dataOffset, dataLength),
                    follows
                )
        );
        metadataOffset += metadataLength;
        dataOffset += dataLength;
        first = false;
    }
}

/**
 * Indicates whether an outbound frame can be fragmented by the protocol.
 */
function isFragmentableFrameType(type: FrameType): boolean {
    return (type >= FrameType.REQUEST_RESPONSE && type <= FrameType.REQUEST_CHANNEL) ||
        type === FrameType.PAYLOAD;
}

/**
 * Extracts raw metadata/data bytes and MIME types from a frame.
 */
function outboundPayloadParts(frame: Frame): OutboundPayloadParts {
    const metadata = frame.metadata;
    const payload = frame.payload;
    return {
        metadataPresent: frame.hasMetadata(),
        metadataMimeType: metadata?.mimeType,
        metadataBytes: metadata?.toUint8Array() ?? EMPTY_BYTES,
        dataMimeType: payload?.mimeType,
        dataBytes: payload?.toUint8Array() ?? EMPTY_BYTES
    };
}

/**
 * Builds the initial fragment preserving the original frame type.
 */
function initialFragment(
    frame: Frame,
    parts: OutboundPayloadParts,
    metadataBytes: Uint8Array | undefined,
    dataBytes: Uint8Array | undefined,
    follows: boolean,
    frameLength: number,
    maxFrameLength: number
): Frame {
    const header = frame.header;
    const frameType = frame.type;
    const streamId = header.streamId;
    const hasMetadata = metadataBytes !== undefined;
    const flags = fragmentInitialFlags(header.flags, follows, frameType, hasMetadata);
    const metadata = fragmentMetadata(parts, metadataBytes);
    const payload = fragmentPayload(parts, dataBytes);

    switch (frameType) {
        case FrameType.REQUEST_RESPONSE:
            return new RequestResponseFrame(streamId, flags, metadata, payload);
        case FrameType.REQUEST_FNF:
            return new RequestFireAndForgetFrame(streamId, flags, metadata, payload);
        case FrameType.REQUEST_STREAM:
            return new RequestStreamFrame(streamId, flags, (frame as RequestStreamFrame).request, metadata, payload);
        case FrameType.REQUEST_CHANNEL:
            return new RequestChannelFrame(streamId, flags, (frame as RequestChannelFrame).request, metadata, payload);
        case FrameType.PAYLOAD:
            return new PayloadFrame(
                streamId,
                fragmentPayloadFlags(header.flags, follows, false, hasMetadata),
                metadata,
                payload
            );
        default:
            throw new RSocketFrameSizeError(frameLength, maxFrameLength);
    }
}

/**
 * Builds continuation PAYLOAD fragments for a fragmented REQUEST/PAYLOAD.
 */
function continuationFragment(
    frame: Frame,
    parts: OutboundPayloadParts,
    metadataBytes: Uint8Array | undefined,
    dataBytes: Uint8Array | undefined,
    follows: boolean
): PayloadFrame {
    const header = frame.header;
    const frameType = frame.type;
    const hasMetadata = metadataBytes !== undefined;
    const metadata = fragmentMetadata(parts, metadataBytes);
    const payload = fragmentPayload(parts, dataBytes);
    const flags = frameType === FrameType.PAYLOAD
        ? fragmentPayloadFlags(header.flags, follows, true, hasMetadata)
        : continuationPayloadFlags(frameType, header.flags, follows, hasMetadata);
    return new PayloadFrame(header.streamId, flags, metadata, payload);
}

/**
 * Preserves original initial-frame flags while toggling the FOLLOWS bit.
 */
function fragmentInitialFlags(flags: number, follows: boolean, type: FrameType, hasMetadata: boolean): number {
    let baseFlags = flags & ~FrameFlag.METADATA & ~PayloadFlag.FOLLOWS;
    if (hasMetadata) baseFlags |= FrameFlag.METADATA;
    if (follows && type === FrameType.REQUEST_CHANNEL) baseFlags &= ~PayloadFlag.COMPLETE;
    return follows ? baseFlags | PayloadFlag.FOLLOWS : baseFlags;
}

/**
 * Builds continuation semantics for fragmented REQUEST frames.
 */
function continuationPayloadFlags(
    type: FrameType,
    originalFlags: number,
    follows: boolean,
    hasMetadata: boolean
): number {
    let flags = PayloadFlag.NEXT | (hasMetadata ? FrameFlag.METADATA : FrameFlag.NONE);
    if (follows) return flags | PayloadFlag.FOLLOWS;
    if (type === FrameType.REQUEST_CHANNEL && (originalFlags & PayloadFlag.COMPLETE) !== 0) {
        flags |= PayloadFlag.COMPLETE;
    }
    return flags;
}

/**
 * Preserves PAYLOAD semantic flags while keeping COMPLETE only on the final fragment.
 */
function fragmentPayloadFlags(
    flags: number,
    follows: boolean,
    continuation: boolean,
    hasMetadata: boolean
): number {
    const baseFlags = (flags & ~FrameFlag.METADATA & ~PayloadFlag.FOLLOWS) |
        (hasMetadata ? FrameFlag.METADATA : FrameFlag.NONE);
    return follows
        ? (baseFlags & ~PayloadFlag.COMPLETE) |
            PayloadFlag.FOLLOWS |
            (continuation ? PayloadFlag.NEXT : FrameFlag.NONE)
        : baseFlags;
}

/**
 * Returns fixed serialized overhead for one fragment shape.
 */
function frameOverhead(type: FrameType, hasMetadata: boolean): number {
    return FRAME_HEADER_LENGTH +
        (type === FrameType.REQUEST_STREAM || type === FrameType.REQUEST_CHANNEL ? REQUEST_N_LENGTH : 0) +
        (hasMetadata ? METADATA_LENGTH_FIELD : 0);
}

/**
 * Wraps a metadata byte slice without decoding it.
 */
function fragmentMetadata(parts: OutboundPayloadParts, bytes: Uint8Array | undefined): Metadata<any> | undefined {
    const mimeType = parts.metadataMimeType;
    if (bytes === undefined || mimeType === undefined) return undefined;
    return new Metadata(mimeType, bytes);
}

/**
 * Wraps a data byte slice without decoding it.
 */
function fragmentPayload(parts: OutboundPayloadParts, bytes: Uint8Array | undefined): Payload<any> | undefined {
    const mimeType = parts.dataMimeType;
    if (bytes === undefined || bytes.length === 0 || mimeType === undefined) return undefined;
    return new Payload(mimeType, bytes);
}

/**
 * Returns a byte slice, optionally preserving an empty slice with a shared buffer.
 */
function slicePart(bytes: Uint8Array, offset: number, length: number, keepEmpty = false): Uint8Array | undefined {
    if (length <= 0) return keepEmpty ? EMPTY_BYTES : undefined;
    if (offset === 0 && length === bytes.length) return bytes;
    return bytes.subarray(offset, offset + length);
}
