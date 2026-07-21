/** Errors that controller code can deliberately return to an RSocket requester. */
import {FrameErrorCode} from "rsocket-frames-ts";
import {isStreamErrorCode} from "rsocket-core-ts";

/**
 * Error thrown by a controller when it needs a specific stream-level RSocket
 * error code instead of the default `APPLICATION_ERROR`.
 */
export class RSocketRequestError extends Error {
    /** Creates a typed responder error sent on the affected stream. */
    constructor(
        message: string,
        readonly code: FrameErrorCode = FrameErrorCode.APPLICATION_ERROR,
        options?: ErrorOptions
    ) {
        super(message, options);
        if (!isStreamErrorCode(code)) {
            throw new RangeError("RSocketRequestError requires a stream-scoped error code");
        }
        this.name = "RSocketRequestError";
    }
}
