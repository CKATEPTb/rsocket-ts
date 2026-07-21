/** Allocation-free helpers for raw RSocket frame headers and decoding. */
import {
    type Frame,
    FrameDeserializer,
    FrameErrorCode,
    FrameFlag,
    FrameType,
    type MimeType,
    PayloadFlag
} from "rsocket-frames-ts";
import {RSocketProtocolError} from "@/errors/index.js";
import {ERROR_DATA_MIME_TYPE, KEEPALIVE_DATA_MIME_TYPE} from "@/protocol/constants.js";

/** Shared empty MIME override bag for the inbound frame hot path. */
const NO_MIME_OVERRIDES: Readonly<{
    metadataMimeType?: MimeType<any>;
    dataMimeType?: MimeType<any>;
}> = Object.freeze({});

/** Deserializes one complete raw RSocket frame. */
export function deserializeFrame(
    buffer: Uint8Array,
    metadataMimeType: MimeType<any>,
    dataMimeType: MimeType<any>,
    overrides: {metadataMimeType?: MimeType<any>; dataMimeType?: MimeType<any>} = NO_MIME_OVERRIDES,
    frameType: FrameType = readFrameTypeAndFlags(buffer) >>> 10
): Frame {
    if (buffer.byteLength < 6) throw new RSocketProtocolError("RSocket frame header is incomplete");
    const effectiveDataMimeType = overrides.dataMimeType ?? dataMimeType;
    const payloadMimeType = frameType === FrameType.KEEPALIVE
        ? KEEPALIVE_DATA_MIME_TYPE
        : frameType === FrameType.ERROR
            ? ERROR_DATA_MIME_TYPE
            : effectiveDataMimeType;
    return FrameDeserializer.deserialize(buffer, overrides.metadataMimeType ?? metadataMimeType, payloadMimeType);
}

/** Reads the 31-bit stream ID from a complete raw RSocket frame. */
export function readFrameStreamId(buffer: Uint8Array): number {
    assertHeader(buffer);
    if (((buffer[0] as number) & 0x80) !== 0) {
        throw new RSocketProtocolError("RSocket stream ID reserved bit must be zero");
    }
    return ((buffer[0] as number) << 24) |
        ((buffer[1] as number) << 16) |
        ((buffer[2] as number) << 8) |
        (buffer[3] as number);
}

/** Reads the combined 6-bit frame type and 10-bit flags word. */
export function readFrameTypeAndFlags(buffer: Uint8Array): number {
    assertHeader(buffer);
    return ((buffer[4] as number) << 8) | (buffer[5] as number);
}

/** Reads the 63-bit implied position from a KEEPALIVE frame. */
export function readKeepalivePosition(buffer: Uint8Array): bigint {
    if (buffer.byteLength < 14) throw new RSocketProtocolError("RSocket KEEPALIVE frame is incomplete");
    let position = BigInt((buffer[6] as number) & 0x7f);
    for (let index = 7; index < 14; index += 1) {
        position = (position << 8n) | BigInt(buffer[index] as number);
    }
    return position;
}

/**
 * Detects a truncated or out-of-bounds metadata section that the sender marked
 * ignorable. The protocol requires endpoints to drop this frame instead of
 * closing the connection.
 */
export function hasIgnorableInvalidMetadataLength(
    buffer: Uint8Array,
    typeAndFlags = readFrameTypeAndFlags(buffer)
): boolean {
    const flags = typeAndFlags & 0x03ff;
    if ((flags & (FrameFlag.IGNORE | FrameFlag.METADATA)) !==
        (FrameFlag.IGNORE | FrameFlag.METADATA)) {
        return false;
    }

    const metadataOffset = metadataLengthOffset((typeAndFlags >>> 10) as FrameType);
    if (metadataOffset === undefined || buffer.byteLength < metadataOffset) return false;
    if (buffer.byteLength < metadataOffset + 3) return true;

    const declaredLength = ((buffer[metadataOffset] as number) << 16) |
        ((buffer[metadataOffset + 1] as number) << 8) |
        (buffer[metadataOffset + 2] as number);
    return declaredLength > buffer.byteLength - metadataOffset - 3;
}

/** Whether raw PAYLOAD flags indicate that non-terminal continuation fragments follow. */
export function payloadHasMoreFragments(typeAndFlags: number): boolean {
    const flags = typeAndFlags & 0x03ff;
    return (flags & PayloadFlag.FOLLOWS) !== 0 && (flags & PayloadFlag.COMPLETE) === 0;
}

/** Whether a frame starts a requester interaction. */
export function isInitialRequestFrame(type: FrameType): boolean {
    return type >= FrameType.REQUEST_RESPONSE && type <= FrameType.REQUEST_CHANNEL;
}

/**
 * Whether a frame containing application metadata or data must stay
 * byte-oriented until endpoint dispatch.
 *
 * MIME interpretation is an application concern in the RSocket protocol.
 * Keeping these frame bodies raw prevents an ignored frame or one malformed
 * application payload from becoming a connection-level decoding failure.
 */
export function requiresRawPayloadDecode(type: FrameType): boolean {
    return type === FrameType.LEASE ||
        type === FrameType.METADATA_PUSH ||
        type === FrameType.PAYLOAD ||
        type === FrameType.EXT ||
        isInitialRequestFrame(type);
}

/** Whether a frame contributes bytes to an implied Resume position. */
export function isResumePositionFrame(type: FrameType): boolean {
    return type >= FrameType.REQUEST_RESPONSE && type <= FrameType.ERROR;
}

/** Whether a frame type is valid only on stream zero. */
export function isConnectionFrame(type: FrameType): boolean {
    return type === FrameType.SETUP ||
        type === FrameType.LEASE ||
        type === FrameType.KEEPALIVE ||
        type === FrameType.METADATA_PUSH ||
        type === FrameType.RESUME ||
        type === FrameType.RESUME_OK;
}

/**
 * Whether an established endpoint must silently discard this frame.
 *
 * The protocol also requires a repeated initial request to be ignored while
 * its stream ID is still active. Checking this before body deserialization
 * prevents malformed duplicate content from turning into a connection error.
 */
export function isIgnorableEstablishedFrame(
    type: FrameType,
    streamId: number,
    streamInUse = false
): boolean {
    return type === FrameType.SETUP ||
        (type === FrameType.METADATA_PUSH && streamId !== 0) ||
        (streamInUse && isInitialRequestFrame(type));
}

/** Whether a stream-scoped frame must be dropped when its stream is unknown. */
export function isIgnorableUnknownStreamFrame(type: FrameType, streamId: number): boolean {
    return type === FrameType.REQUEST_N ||
        type === FrameType.CANCEL ||
        type === FrameType.PAYLOAD ||
        (type === FrameType.ERROR && streamId !== 0);
}

/** Whether an ERROR code is valid only on stream zero. */
export function isConnectionErrorCode(code: FrameErrorCode): boolean {
    return code === FrameErrorCode.INVALID_SETUP ||
        code === FrameErrorCode.UNSUPPORTED_SETUP ||
        code === FrameErrorCode.REJECTED_SETUP ||
        code === FrameErrorCode.REJECTED_RESUME ||
        code === FrameErrorCode.CONNECTION_ERROR ||
        code === FrameErrorCode.CONNECTION_CLOSE;
}

/** Whether an ERROR code requires a non-zero stream ID. */
export function isStreamErrorCode(code: FrameErrorCode): boolean {
    return code === FrameErrorCode.APPLICATION_ERROR ||
        code === FrameErrorCode.REJECTED ||
        code === FrameErrorCode.CANCELED ||
        code === FrameErrorCode.INVALID ||
        code >= 0x00000301 && code <= 0xfffffffe;
}

/** Whether an ERROR code is reserved for SETUP or RESUME rejection. */
export function isHandshakeErrorCode(code: FrameErrorCode): boolean {
    return code === FrameErrorCode.INVALID_SETUP ||
        code === FrameErrorCode.UNSUPPORTED_SETUP ||
        code === FrameErrorCode.REJECTED_SETUP ||
        code === FrameErrorCode.REJECTED_RESUME;
}

/** Whether an ERROR code is legal for its encoded stream ID. */
export function isErrorCodeValidForStream(code: FrameErrorCode, streamId: number): boolean {
    if (isConnectionErrorCode(code)) return streamId === 0;
    if (isStreamErrorCode(code)) return streamId !== 0;
    return true;
}

/** Rejects buffers too short to contain the fixed RSocket frame header. */
function assertHeader(buffer: Uint8Array): void {
    if (Object.prototype.toString.call(buffer) !== "[object Uint8Array]" || buffer.byteLength < 6) {
        throw new RSocketProtocolError("RSocket frame header is incomplete");
    }
}

/** Returns the metadata-length field offset for frames carrying data and metadata. */
function metadataLengthOffset(type: FrameType): number | undefined {
    switch (type) {
        case FrameType.REQUEST_RESPONSE:
        case FrameType.REQUEST_FNF:
        case FrameType.PAYLOAD:
            return 6;
        case FrameType.REQUEST_STREAM:
        case FrameType.REQUEST_CHANNEL:
        case FrameType.EXT:
            return 10;
        default:
            return undefined;
    }
}
