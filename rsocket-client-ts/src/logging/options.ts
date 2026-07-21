/**
 * Logging option normalization and default presets.
 */
import type {
    NormalizedRSocketLogOptions,
    RSocketLogDefaults,
    RSocketLogInput,
    RSocketLogOptions,
    RSocketLogSink
} from "@/logging/types.js";
import {defaultLogSink} from "@/logging/sink.js";
import type {RSocketControllerKind} from "@/controllers/types.js";

/**
 * Defaults used by socket constructor logging.
 */
export const SOCKET_LOG_DEFAULTS: RSocketLogDefaults = {
    category: "RSocket",
    frames: true,
    lifecycle: true,
    interactions: false,
    payload: false
};

/**
 * Defaults used by `controller.log()`.
 */
export const CONTROLLER_LOG_DEFAULTS: RSocketLogDefaults = {
    category: "RSocketController",
    frames: false,
    lifecycle: false,
    interactions: true,
    payload: false
};

/**
 * Controller logging defaults keyed by interaction model.
 */
const CONTROLLER_LOG_DEFAULTS_BY_KIND: Record<RSocketControllerKind, RSocketLogDefaults> = {
    fireAndForget: {
        ...CONTROLLER_LOG_DEFAULTS,
        category: `${CONTROLLER_LOG_DEFAULTS.category}.fireAndForget`
    },
    requestResponse: {
        ...CONTROLLER_LOG_DEFAULTS,
        category: `${CONTROLLER_LOG_DEFAULTS.category}.requestResponse`
    },
    requestStream: {
        ...CONTROLLER_LOG_DEFAULTS,
        category: `${CONTROLLER_LOG_DEFAULTS.category}.requestStream`
    },
    requestChannel: {
        ...CONTROLLER_LOG_DEFAULTS,
        category: `${CONTROLLER_LOG_DEFAULTS.category}.requestChannel`
    }
};

/**
 * Returns controller logging defaults for one interaction model.
 */
export function controllerLogDefaults(kind: RSocketControllerKind): RSocketLogDefaults {
    return CONTROLLER_LOG_DEFAULTS_BY_KIND[kind];
}

/**
 * Converts any accepted logging input into a complete option object.
 *
 * Passing `false` disables logging while preserving previous category and
 * logger values so toggling logging back on is predictable.
 */
export function normalizeLogOptions(
    input: RSocketLogInput = true,
    defaults: RSocketLogDefaults,
    previous?: NormalizedRSocketLogOptions
): NormalizedRSocketLogOptions {
    if (input === false) {
        return {
            enabled: false,
            category: previous?.category ?? defaults.category,
            frames: previous?.frames ?? defaults.frames,
            lifecycle: previous?.lifecycle ?? defaults.lifecycle,
            interactions: previous?.interactions ?? defaults.interactions,
            payload: previous?.payload ?? defaults.payload,
            logger: previous?.logger ?? defaultLogSink
        };
    }

    const options = toOptions(input);
    return {
        enabled: options.enabled ?? true,
        category: options.category ?? previous?.category ?? defaults.category,
        frames: options.frames ?? previous?.frames ?? defaults.frames,
        lifecycle: options.lifecycle ?? previous?.lifecycle ?? defaults.lifecycle,
        interactions: options.interactions ?? previous?.interactions ?? defaults.interactions,
        payload: options.payload ?? previous?.payload ?? defaults.payload,
        logger: options.logger ?? previous?.logger ?? defaultLogSink
    };
}

/**
 * Converts shorthand logging inputs into an options object.
 */
function toOptions(input: Exclude<RSocketLogInput, false>): RSocketLogOptions {
    if (input === true) return {};
    if (typeof input === "string") return {category: input};
    if (typeof input === "function") return {logger: input as RSocketLogSink};
    return input;
}
