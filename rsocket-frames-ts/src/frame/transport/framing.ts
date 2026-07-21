import {
    FRAME_HEADER_SIZE,
    FRAME_LENGTH_PREFIX_SIZE,
    MAX_FRAME_SIZE
} from "@/frame/transport/constants";
import {assertInteger} from "@/utils";

/**
 * Validates an RSocket frame length.
 *
 * @param length Frame length excluding a transport prefix.
 * @param maximum Configured upper bound, never larger than the protocol limit.
 */
export function assertFrameSize(length: number, maximum = MAX_FRAME_SIZE): void {
    assertInteger("Maximum frame size", maximum, FRAME_HEADER_SIZE, MAX_FRAME_SIZE);
    assertInteger("RSocket frame size", length, FRAME_HEADER_SIZE, maximum);
}

/**
 * Reads a 24-bit big-endian TCP frame-length prefix.
 *
 * @param bytes Buffer containing the prefix.
 * @param offset Prefix offset in the buffer.
 * @returns Frame length excluding the three-byte prefix.
 */
export function readFrameSize(bytes: Uint8Array, offset = 0): number {
    if (!Number.isInteger(offset) || offset < 0 || bytes.length - offset < FRAME_LENGTH_PREFIX_SIZE) {
        throw new RangeError("A TCP frame-length prefix requires three available bytes");
    }
    return bytes[offset]! * 0x10000 + bytes[offset + 1]! * 0x100 + bytes[offset + 2]!;
}
