/**
 * Shared RSocket transport and protocol error hierarchy.
 *
 * Errors retain frame codes, stream ids, and source ERROR frames so endpoint
 * implementations can preserve useful wire-level context.
 */
import {type ErrorFrame, FrameErrorCode} from "rsocket-frames-ts";
import {errorMessage} from "@/payload/index.js";

/**
 * Base error for every library-generated failure.
 */
export class RSocketError extends Error {
    /** RSocket error code when the failure maps to a protocol ERROR frame. */
    readonly code?: FrameErrorCode;
    /** Stream id associated with the failure, when available. */
    readonly streamId?: number;
    /** Original ERROR frame received from the responder, when available. */
    readonly frame?: ErrorFrame;

    /**
     * Creates an RSocket error with optional protocol context.
     */
    constructor(message: string, options: {
        code?: FrameErrorCode;
        streamId?: number;
        frame?: ErrorFrame;
        cause?: unknown
    } = {}) {
        super(message);
        this.name = "RSocketError";
        if (options.cause !== undefined) (this as Error & { cause?: unknown }).cause = options.cause;
        if (options.code !== undefined) this.code = options.code;
        if (options.streamId !== undefined) this.streamId = options.streamId;
        if (options.frame !== undefined) this.frame = options.frame;
    }
}

/**
 * Raised when the transport or RSocket connection is closed, failed, or not ready.
 */
export class RSocketConnectionError extends RSocketError {
    /**
     * Creates a connection-level error.
     */
    constructor(message: string, cause?: unknown) {
        super(message, {cause});
        this.name = "RSocketConnectionError";
    }
}

/** Raised when incoming or outgoing frames violate RSocket protocol rules. */
export class RSocketProtocolError extends RSocketError {
    /**
     * Creates a protocol error with optional RSocket frame metadata.
     */
    constructor(message: string, options: {
        code?: FrameErrorCode;
        streamId?: number;
        frame?: ErrorFrame;
        cause?: unknown
    } = {}) {
        super(message, options);
        this.name = "RSocketProtocolError";
    }
}

/**
 * Raised when a frame is larger than the configured maximum frame length.
 */
export class RSocketFrameSizeError extends RSocketProtocolError {
    /**
     * Creates a frame-size error.
     */
    constructor(length: number, maxFrameLength: number) {
        super(`RSocket frame length ${length} exceeds configured maxFrameLength ${maxFrameLength}`);
        this.name = "RSocketFrameSizeError";
    }
}

/**
 * Converts an incoming ERROR frame to the most specific library error type.
 */
export function errorFromFrame(frame: ErrorFrame): RSocketError {
    const data = framePayload(frame);
    const message = data !== undefined ? String(data) : FrameErrorCode[frame.code] ?? "RSocket error";
    return new RSocketError(message, {
        code: frame.code,
        streamId: frame.header.streamId,
        frame
    });
}

/**
 * Normalizes any close reason into a connection error.
 */
export function connectionClosedError(reason: unknown = "RSocket connection closed"): RSocketConnectionError {
    return new RSocketConnectionError(errorMessage(reason));
}

/**
 * Extracts the application error value from an ERROR frame payload.
 */
function framePayload(frame: ErrorFrame): unknown {
    const payload = (frame as { readonly payload?: unknown }).payload;
    if (payload !== undefined && payload !== null && typeof payload === "object" && "payload" in payload) {
        return (payload as { readonly payload?: unknown }).payload;
    }
    return payload;
}
