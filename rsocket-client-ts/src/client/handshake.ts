/**
 * SETUP/RESUME handshake helpers used before the full client dispatcher starts.
 */
import {ErrorFrame, type Frame, FrameErrorCode, type MimeType, ResumeOkFrame} from "rsocket-frames-ts";
import {
    deserializeFrame,
    errorFromFrame,
    readFrameStreamId,
    RSocketFrameSizeError,
    RSocketProtocolError,
    type RSocketTransportConnection
} from "rsocket-core-ts";
import type {RSocketFrameActivityListener} from "@/client/types.js";

/**
 * Normalized options required by pre-client handshake helpers.
 */
export interface RSocketHandshakeOptions {
    /** Optional transport or RSocket handshake timeout. */
    readonly connectTimeoutMs: number | undefined;
    /** Maximum serialized RSocket frame length accepted or sent. */
    readonly maxFrameLength: number;
    /** Optional diagnostic callback for raw frame send/receive events. */
    readonly activityListener: RSocketFrameActivityListener | undefined;
    /** Optional dynamic switch for activity logging. */
    readonly activityEnabled: (() => boolean) | undefined;
    /** MIME types needed to decode the first responder frame. */
    readonly setup: {
        readonly metadataMimeType: MimeType<any>;
        readonly dataMimeType: MimeType<any>;
    };
}

/**
 * Sends one handshake frame before the normal client dispatcher is attached.
 */
export function sendHandshakeFrame(
    connection: RSocketTransportConnection,
    options: RSocketHandshakeOptions,
    frame: Frame
): void {
    const bytes = frame.toUint8Array();
    if (bytes.length > options.maxFrameLength) {
        throw new RSocketFrameSizeError(bytes.length, options.maxFrameLength);
    }

    connection.write(bytes);
    emitHandshakeActivity(options, "send", frame);
}

/**
 * Decodes and validates the responder's first frame during Resume.
 *
 * Transport subscription ownership remains with `RSocketClient`, preventing a
 * gap in which replayed frames could arrive between handshake and session
 * subscriptions.
 */
export function decodeResumeOkFrame(
    bytes: Uint8Array,
    options: RSocketHandshakeOptions
): ResumeOkFrame {
    if (bytes.length > options.maxFrameLength) {
        throw new RSocketFrameSizeError(bytes.length, options.maxFrameLength);
    }

    if (readFrameStreamId(bytes) !== 0) {
        throw new RSocketProtocolError("RSocket Resume responder sent a handshake frame on a non-zero stream");
    }
    const frame = deserializeFrame(bytes, options.setup.metadataMimeType, options.setup.dataMimeType);
    emitHandshakeActivity(options, "receive", frame);
    if (frame instanceof ResumeOkFrame) return frame;
    if (frame instanceof ErrorFrame) {
        if (frame.code !== FrameErrorCode.CONNECTION_ERROR && frame.code !== FrameErrorCode.REJECTED_RESUME) {
            throw new RSocketProtocolError("RSocket Resume responder sent an invalid handshake ERROR code");
        }
        throw errorFromFrame(frame);
    }
    throw new RSocketProtocolError("RSocket Resume expected RESUME_OK from responder");
}

/**
 * Emits diagnostic activity for frames exchanged before the client is attached.
 */
function emitHandshakeActivity(
    options: RSocketHandshakeOptions,
    direction: Parameters<RSocketFrameActivityListener>[0]["direction"],
    frame: Frame
): void {
    if (options.activityListener === undefined || options.activityEnabled?.() === false) return;
    try {
        options.activityListener({direction, frame});
    } catch {
        // Handshake activity listeners are diagnostic-only.
    }
}
