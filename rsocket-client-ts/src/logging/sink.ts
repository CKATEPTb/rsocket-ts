/**
 * Default console-based logging sink.
 */
import type {RSocketLogEvent, RSocketLogSink} from "@/logging/types.js";

/**
 * Writes formatted diagnostic events to `console.debug`.
 */
export const defaultLogSink: RSocketLogSink = (event) => {
    const details = logDetails(event);
    if (details === undefined) {
        console.debug(formatLogEvent(event));
        return;
    }

    console.debug(formatLogEvent(event), details);
};

/**
 * Formats a compact one-line message for a log event.
 */
function formatLogEvent(event: RSocketLogEvent): string {
    switch (event.type) {
        case "frame":
            return `[${event.category}] ${event.direction === "send" ? "=>" : "<="} ${event.frameType} stream=${event.streamId}`;
        case "lifecycle":
            return `[${event.category}] lifecycle ${event.event}${event.reconnect ? " reconnect" : ""} attempt=${event.attempt}`;
        case "interaction":
            return `[${event.category}] ${event.interaction} ${event.stage}${event.label ? ` ${event.label}` : ""}`;
    }
}

/**
 * Extracts optional structured details printed after the log line.
 */
function logDetails(event: RSocketLogEvent): object | undefined {
    switch (event.type) {
        case "frame":
            return event.frame === undefined
                ? {flags: event.flags}
                : {flags: event.flags, frame: event.frame};
        case "lifecycle":
            return event.delayMs !== undefined || event.error !== undefined
                ? {delayMs: event.delayMs, error: event.error}
                : undefined;
        case "interaction":
            return event.payload !== undefined || event.value !== undefined || event.error !== undefined
                ? {payload: event.payload, value: event.value, error: event.error}
                : undefined;
    }
}
