/**
 * Log event builders and guarded emission helpers.
 */
import {type Frame, FrameType} from "rsocket-frames-ts";
import type {RSocketFrameActivity} from "@/client/index.js";
import type {
    NormalizedRSocketLogOptions,
    RSocketFrameLogEvent,
    RSocketInteractionLogEvent,
    RSocketLifecycleActivity,
    RSocketLifecycleLogEvent,
    RSocketLogEvent
} from "@/logging/types.js";

type RawLogEvent =
    | Omit<RSocketFrameLogEvent, "category">
    | Omit<RSocketLifecycleLogEvent, "category">
    | Omit<RSocketInteractionLogEvent, "category">;

/**
 * Sends a log event to the configured sink if the event category is enabled.
 */
export function emitLog(options: NormalizedRSocketLogOptions | undefined, event: RawLogEvent): void {
    if (options === undefined || !options.enabled || !allows(options, event.type)) return;

    try {
        options.logger(logEvent(event, options));
    } catch {
        // Logging is diagnostic-only and must not affect protocol flow.
    }
}

/**
 * Creates a compact log event for a raw frame activity notification.
 */
export function frameLogEvent(
    activity: RSocketFrameActivity,
    includeFrame = true
): Omit<RSocketFrameLogEvent, "category"> {
    const event = {
        type: "frame",
        direction: activity.direction,
        frameType: frameTypeName(activity.frame),
        streamId: activity.frame.header.streamId,
        flags: activity.frame.header.flags
    } as const;
    return includeFrame ? {...event, frame: activity.frame} : event;
}

/**
 * Creates a lifecycle log event from a connection event.
 */
export function lifecycleLogEvent(event: RSocketLifecycleActivity): Omit<RSocketLifecycleLogEvent, "category"> {
    const logEvent: Omit<RSocketLifecycleLogEvent, "category"> & { delayMs?: number } = {
        type: "lifecycle",
        event: event.type,
        attempt: event.attempt,
        reconnect: event.reconnect,
        error: event.error
    };
    if (event.delayMs !== undefined) logEvent.delayMs = event.delayMs;
    return logEvent;
}

/**
 * Creates a controller interaction log event.
 */
export function interactionLogEvent(
    event: Omit<RSocketInteractionLogEvent, "category" | "type">
): Omit<RSocketInteractionLogEvent, "category"> {
    return {
        type: "interaction",
        ...event
    };
}

/**
 * Checks whether a normalized logging category enables the event family.
 */
function allows(options: NormalizedRSocketLogOptions, type: RSocketLogEvent["type"]): boolean {
    switch (type) {
        case "frame":
            return options.frames;
        case "lifecycle":
            return options.lifecycle;
        case "interaction":
            return options.interactions;
    }
}

/**
 * Adds the configured category and removes heavy fields when payload logging is disabled.
 */
function logEvent(event: RawLogEvent, options: NormalizedRSocketLogOptions): RSocketLogEvent {
    if (options.payload) return {...event, category: options.category} as RSocketLogEvent;

    switch (event.type) {
        case "frame":
            return {
                type: "frame",
                category: options.category,
                direction: event.direction,
                frameType: event.frameType,
                streamId: event.streamId,
                flags: event.flags
            };
        case "interaction": {
            const log: {
                type: "interaction";
                category: string;
                interaction: RSocketInteractionLogEvent["interaction"];
                stage: RSocketInteractionLogEvent["stage"];
                label?: string;
                error?: unknown;
            } = {
                type: "interaction",
                category: options.category,
                interaction: event.interaction,
                stage: event.stage
            };
            if (event.label !== undefined) log.label = event.label;
            if (event.error !== undefined) log.error = event.error;
            return log;
        }
        case "lifecycle":
            return {...event, category: options.category};
    }
}

/**
 * Converts numeric RSocket frame type ids into stable names for logs.
 */
function frameTypeName(frame: Frame): string {
    return FrameType[frame.type] ?? String(frame.type);
}
