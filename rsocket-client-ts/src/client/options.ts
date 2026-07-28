/**
 * Low-level option normalization for transport-neutral RSocket sessions.
 */
import type {MimeType, RSocketResumeToken} from "rsocket-frames-ts";
import {
    DEFAULT_DATA_MIME_TYPE,
    DEFAULT_MAX_FRAME_LENGTH,
    DEFAULT_METADATA_MIME_TYPE,
    MAX_REQUEST_N,
    RSocketConnectionError,
    type RSocketPayloadInput
} from "rsocket-core-ts";
import type {
    RSocketClientConfiguration,
    RSocketFrameActivityListener
} from "@/client/types.js";

/** Default interval between requester KEEPALIVE frames. */
const DEFAULT_KEEP_ALIVE_MS = 20_000;
/** Default maximum delay between responder KEEPALIVE frames. */
const DEFAULT_LIFETIME_MS = 90_000;

/**
 * SETUP frame options after defaults have been applied.
 */
export interface NormalizedSetup<D = unknown, M = unknown> {
    /** Interval between requester keepalive frames in milliseconds. */
    readonly keepAliveMs: number;
    /** Maximum interval without a responder KEEPALIVE before the connection is dead. */
    readonly lifetimeMs: number;
    /** RSocket major protocol version sent in SETUP. */
    readonly majorVersion: number;
    /** RSocket minor protocol version sent in SETUP. */
    readonly minorVersion: number;
    /** Optional resume token included for protocol compatibility. */
    readonly resumeToken: RSocketResumeToken | undefined;
    /** Whether requester-side lease accounting must be enforced. */
    readonly honorLease: boolean;
    /** MIME type used to encode request data payloads by default. */
    readonly dataMimeType: MimeType<D>;
    /** MIME type used to encode request metadata payloads by default. */
    readonly metadataMimeType: MimeType<M>;
    /** Optional payload carried by the initial SETUP frame. */
    readonly setupPayload?: RSocketPayloadInput<D, M>;
}

/**
 * Fully normalized client options consumed by `RSocketClient`.
 */
export interface NormalizedClientOptions<D = unknown, M = unknown> {
    /** Optional transport open timeout. */
    readonly connectTimeoutMs: number | undefined;
    /** Maximum serialized RSocket frame length accepted or sent. */
    readonly maxFrameLength: number;
    /** Optional diagnostic callback for raw frame send/receive events. */
    readonly activityListener: RSocketFrameActivityListener | undefined;
    /** Optional dynamic switch for activity logging. */
    readonly activityEnabled: (() => boolean) | undefined;
    /** Optional receiver for transport-specific best-effort media payloads. */
    readonly mediaListener: ((payload: Uint8Array) => void) | undefined;
    /** Normalized SETUP frame options. */
    readonly setup: NormalizedSetup<D, M>;
}

/** Smallest useful frame length because every RSocket frame starts with a 6-byte header. */
const MIN_FRAME_LENGTH = 6;

/**
 * Applies defaults and normalizes user-facing client options.
 */
export function normalizeClientOptions<D, M>(
    options: RSocketClientConfiguration<D, M>
): NormalizedClientOptions<D, M> {
    const setup = options.setup ?? {};
    const keepAliveMs = positiveMilliseconds(setup.keepAliveMs ?? DEFAULT_KEEP_ALIVE_MS, "setup.keepAlive");
    const lifetimeMs = positiveMilliseconds(setup.lifetimeMs ?? DEFAULT_LIFETIME_MS, "setup.lifetime");
    const connectTimeoutMs = options.connectTimeoutMs === undefined
        ? 4_000
        : positiveMilliseconds(options.connectTimeoutMs, "connectTimeout");
    return {
        connectTimeoutMs,
        maxFrameLength: frameLength(options.maxFrameLength ?? DEFAULT_MAX_FRAME_LENGTH),
        activityListener: options.activityListener,
        activityEnabled: options.activityEnabled,
        mediaListener: options.mediaListener,
        setup: {
            keepAliveMs,
            lifetimeMs,
            majorVersion: unsignedInteger(setup.majorVersion ?? 1, 0xffff, "setup.majorVersion"),
            minorVersion: unsignedInteger(setup.minorVersion ?? 0, 0xffff, "setup.minorVersion"),
            resumeToken: typeof setup.resumeToken === "string"
                ? setup.resumeToken
                : setup.resumeToken?.slice(),
            honorLease: setup.honorLease ?? false,
            dataMimeType: setup.dataMimeType ?? DEFAULT_DATA_MIME_TYPE,
            metadataMimeType: setup.metadataMimeType ?? DEFAULT_METADATA_MIME_TYPE,
            ...(setup.payload === undefined ? {} : {setupPayload: setup.payload})
        }
    };
}

/**
 * Validates SETUP timing fields that the RSocket protocol requires to be positive.
 */
function positiveMilliseconds(value: number, option: string): number {
    if (Number.isInteger(value) && value > 0 && value <= MAX_REQUEST_N) return value;
    throw new RSocketConnectionError(`RSocket ${option} must be between 1 and ${MAX_REQUEST_N} milliseconds`);
}

/**
 * Validates configured frame length before the transport is opened.
 */
function frameLength(value: number): number {
    if (Number.isInteger(value) && value >= MIN_FRAME_LENGTH && value <= DEFAULT_MAX_FRAME_LENGTH) return value;
    throw new RSocketConnectionError(
        `RSocket maxFrameLength must be between ${MIN_FRAME_LENGTH} and ${DEFAULT_MAX_FRAME_LENGTH} bytes`
    );
}

/**
 * Validates an unsigned integer stored in a fixed-width RSocket frame field.
 */
function unsignedInteger(value: number, max: number, option: string): number {
    if (Number.isInteger(value) && value >= 0 && value <= max) return value;
    throw new RSocketConnectionError(`RSocket ${option} must be an integer between 0 and ${max}`);
}

