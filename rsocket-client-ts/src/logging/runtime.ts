/** Stateful socket logger shared by transport facades. */
import type {RSocketFrameActivity} from "@/client/types.js";
import {emitLog, frameLogEvent, lifecycleLogEvent} from "@/logging/emit.js";
import {normalizeLogOptions, SOCKET_LOG_DEFAULTS} from "@/logging/options.js";
import type {
    NormalizedRSocketLogOptions,
    RSocketLifecycleActivity,
    RSocketLogOptions,
    RSocketLogSink
} from "@/logging/types.js";

/** Logger input restricted to the frame and lifecycle capabilities implemented by this class. */
type RSocketLoggerInput = boolean | string | RSocketLogSink | Omit<RSocketLogOptions, "interactions">;

/**
 * Normalizes socket log configuration and emits guarded frame/lifecycle events.
 * Logger failures are isolated from protocol and reconnect state.
 */
export class RSocketLogger {
    private readonly options: NormalizedRSocketLogOptions | undefined;

    /** Creates an optionally enabled socket logger. */
    constructor(input?: RSocketLoggerInput) {
        if (input === undefined) return;
        const options = normalizeLogOptions(input, SOCKET_LOG_DEFAULTS);
        this.options = options.enabled ? options : undefined;
    }

    /** Whether raw frame activity needs to be observed by the requester. */
    get framesEnabled(): boolean {
        return this.options?.frames === true;
    }

    /** Whether lifecycle events need to be materialized. */
    get lifecycleEnabled(): boolean {
        return this.options?.lifecycle === true;
    }

    /** Emits one raw frame event when frame logging is enabled. */
    frame(activity: RSocketFrameActivity): void {
        const options = this.options;
        if (options?.frames !== true) return;
        emitLog(options, frameLogEvent(activity, options.payload));
    }

    /** Emits one connection lifecycle event when lifecycle logging is enabled. */
    lifecycle(event: RSocketLifecycleActivity): void {
        const options = this.options;
        if (options?.lifecycle !== true) return;
        emitLog(options, lifecycleLogEvent(event));
    }
}
