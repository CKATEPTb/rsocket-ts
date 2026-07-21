import {FrameErrorCode} from "@/frame/FrameErrorCode";
import {FrameType} from "@/frame/FrameType";

/**
 * Enforces stream zero for connection frames whose constructors do not carry a stream ID.
 *
 * `METADATA_PUSH` is deliberately excluded: the protocol tells peers to ignore
 * that frame on a non-zero stream, so the codec must preserve it for the caller.
 *
 * @param type Frame type being decoded or created.
 * @param streamId Unsigned 31-bit stream ID.
 */
export function assertFixedConnectionStream(type: FrameType, streamId: number): void {
    const fixed = type === FrameType.SETUP
        || type === FrameType.LEASE
        || type === FrameType.KEEPALIVE
        || type === FrameType.RESUME
        || type === FrameType.RESUME_OK;
    if (fixed && streamId !== 0) {
        throw new RangeError(`${FrameType[type]} must use stream ID 0`);
    }
}

/**
 * Enforces the stream scope assigned to standard and application error codes.
 *
 * @param code Unsigned RSocket error code.
 * @param streamId Validated frame stream ID.
 */
export function assertErrorStreamScope(code: FrameErrorCode, streamId: number): void {
    const connectionScoped = code >= FrameErrorCode.INVALID_SETUP && code <= FrameErrorCode.REJECTED_RESUME
        || code === FrameErrorCode.CONNECTION_ERROR
        || code === FrameErrorCode.CONNECTION_CLOSE;
    const streamScoped = code >= FrameErrorCode.APPLICATION_ERROR && code <= FrameErrorCode.INVALID
        || code >= 0x00000301 && code <= 0xfffffffe;
    if (connectionScoped && streamId !== 0) {
        throw new RangeError(`Error code 0x${code.toString(16).padStart(8, "0")} must use stream ID 0`);
    }
    if (streamScoped && streamId === 0) {
        throw new RangeError(`Error code 0x${code.toString(16).padStart(8, "0")} must use a non-zero stream ID`);
    }
}
