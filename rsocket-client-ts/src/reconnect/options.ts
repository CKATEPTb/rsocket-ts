/** Normalized reconnect configuration. */
import type {RSocketResumePolicyInput} from "@/resume/options.js";

/** Internal reconnect policy resolved from the public switch. */
export interface RSocketReconnectOptions {
    /** Whether automatic reconnect attempts are enabled. */
    readonly enabled: boolean;
}

/** Uptime required before a successful reconnect resets the attempt counter. */
export const RECONNECT_MIN_UPTIME_MS = 5_000;

/** Nested reconnect options accepted by `new RSocket(...)`. */
export interface RSocketReconnectPolicyInput {
    /** Enables protocol Resume for the given backend state lifetime. */
    readonly resume?: RSocketResumePolicyInput;
}

/** Public reconnect options accepted by `new RSocket(...)`. */
export interface RSocketReconnectOptionInput {
    /** Resume settings object or a boolean reconnect switch. */
    readonly reconnect?: boolean | RSocketReconnectPolicyInput;
}

/** Shared immutable policies avoid one allocation per socket. */
const ENABLED_RECONNECT: RSocketReconnectOptions = Object.freeze({enabled: true});
const DISABLED_RECONNECT: RSocketReconnectOptions = Object.freeze({enabled: false});

/** Enables reconnect only when the client explicitly requests it. */
export function normalizeReconnectOptions(input: RSocketReconnectOptionInput): RSocketReconnectOptions {
    return input.reconnect === true || typeof input.reconnect === "object"
        ? ENABLED_RECONNECT
        : DISABLED_RECONNECT;
}
