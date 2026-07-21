/** Internal validation and writing for RSocket's TCP length prefix. */
import {RSocketFrameSizeError, RSocketProtocolError} from "@/errors/index.js";
import {DEFAULT_MAX_FRAME_LENGTH} from "@/protocol/constants.js";

/** Number of bytes in every raw RSocket frame header. */
export const TCP_FRAME_HEADER_LENGTH = 6;
/** Number of bytes in the unsigned TCP frame-length prefix. */
export const TCP_FRAME_PREFIX_LENGTH = 3;

/** Validates one raw frame length against protocol and configured bounds. */
export function assertTcpFrameLength(length: number, maxFrameLength: number): void {
    if (length < TCP_FRAME_HEADER_LENGTH) {
        throw new RSocketProtocolError("RSocket frame header is incomplete");
    }
    if (length > maxFrameLength || length > DEFAULT_MAX_FRAME_LENGTH) {
        throw new RSocketFrameSizeError(length, Math.min(maxFrameLength, DEFAULT_MAX_FRAME_LENGTH));
    }
}

/** Writes one already-validated unsigned 24-bit frame length. */
export function writeTcpFrameLength(target: Uint8Array, length: number, offset = 0): void {
    target[offset] = length >>> 16;
    target[offset + 1] = length >>> 8;
    target[offset + 2] = length;
}
