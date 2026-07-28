/** Validation and defaults for the server protocol engine. */
import {DEFAULT_MAX_FRAME_LENGTH, MAX_REQUEST_N, RSocketConnectionError} from "rsocket-core-ts";
import type {NormalizedServerOptions} from "@/session/types.js";
import type {RSocketServerOptions} from "@/server/types.js";

const FRAME_HEADER_LENGTH = 6;
const DEFAULT_RESUME_BUFFER_BYTES = 16 * 1024 * 1024;
const DEFAULT_HANDSHAKE_TIMEOUT_MS = 10_000;

/** Validates public options once before any transport is accepted. */
export function normalizeServerOptions<D, M>(options: RSocketServerOptions<D, M>): NormalizedServerOptions<D, M> {
    const resume = options.resume;
    const lease = options.lease;
    return {
        maxFrameLength: boundedInteger(
            options.maxFrameLength ?? DEFAULT_MAX_FRAME_LENGTH,
            FRAME_HEADER_LENGTH,
            DEFAULT_MAX_FRAME_LENGTH,
            "maxFrameLength"
        ),
        handshakeTimeoutMs: boundedInteger(
            options.handshakeTimeoutMs ?? DEFAULT_HANDSHAKE_TIMEOUT_MS,
            1,
            MAX_REQUEST_N,
            "handshakeTimeoutMs"
        ),
        accept: options.accept,
        metadataPush: options.metadataPush,
        media: options.media,
        activityListener: options.activityListener,
        resume: resume === undefined
            ? undefined
            : {
                ttlMs: boundedInteger(resume.ttlMs, 1, MAX_REQUEST_N, "resume.ttlMs"),
                maxBufferBytes: boundedInteger(
                    resume.maxBufferBytes ?? DEFAULT_RESUME_BUFFER_BYTES,
                    FRAME_HEADER_LENGTH,
                    Number.MAX_SAFE_INTEGER,
                    "resume.maxBufferBytes"
                )
            },
        lease: lease === undefined
            ? undefined
            : {
                ttlMs: boundedInteger(lease.ttlMs, 0, MAX_REQUEST_N, "lease.ttlMs"),
                requests: boundedInteger(lease.requests, 0, MAX_REQUEST_N, "lease.requests"),
                metadata: lease.metadata
            }
    };
}

/** Validates one finite integer option against inclusive bounds. */
function boundedInteger(value: number, minimum: number, maximum: number, name: string): number {
    if (Number.isInteger(value) && value >= minimum && value <= maximum) return value;
    throw new RSocketConnectionError(
        `RSocket server ${name} must be an integer between ${minimum} and ${maximum}`
    );
}
