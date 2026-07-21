/** Normalization for client-level protocol Resume policy. */
import {MAX_REQUEST_N, RSocketConnectionError} from "rsocket-core-ts";
import {createResumeToken} from "@/resume/token.js";

/** Normalized Resume behavior used by reconnecting client facades. */
export interface RSocketResumeOptions {
    /** Whether protocol Resume should be attempted after transport loss. */
    readonly enabled: boolean;
    /** Opaque token sent in SETUP and later RESUME frames. */
    readonly token: string | undefined;
    /** Backend resume-state lifetime in milliseconds. */
    readonly ttlMs: number;
}

/** Resume policy accepted by public `reconnect.resume`. */
export interface RSocketResumePolicyInput {
    /** Backend resume-state lifetime in milliseconds. */
    readonly ttl: number;
}

/** Public constructor options related to protocol Resume. */
export interface RSocketResumeOptionInput {
    /** Reconnect options that may enable protocol Resume. */
    readonly reconnect?: boolean | {
        /** Resume policy; omit it to reconnect with a fresh SETUP. */
        readonly resume?: RSocketResumePolicyInput;
    };
}

/** Default backend Resume window: five minutes. */
const DEFAULT_RESUME_TTL_MS = 300_000;

/** Normalizes reconnect Resume options and generates an initial token when enabled. */
export function normalizeResumeOptions(input: RSocketResumeOptionInput = {}): RSocketResumeOptions {
    const reconnect = input.reconnect;
    const policy = typeof reconnect === "object" ? reconnect.resume : undefined;
    const enabled = policy !== undefined;
    return {
        enabled,
        token: enabled ? createResumeToken() : undefined,
        ttlMs: resumeTtl(policy)
    };
}

/** Extracts and validates the backend Resume TTL from the public policy shape. */
function resumeTtl(policy: RSocketResumePolicyInput | undefined): number {
    const ttl = policy?.ttl ?? DEFAULT_RESUME_TTL_MS;
    if (Number.isInteger(ttl) && ttl > 0 && ttl <= MAX_REQUEST_N) return ttl;
    throw new RSocketConnectionError(
        `RSocket reconnect.resume.ttl must be an integer between 1 and ${MAX_REQUEST_N}`
    );
}
