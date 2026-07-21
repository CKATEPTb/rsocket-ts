/**
 * Lifecycle event hub used by the public socket.
 */
import type {RSocketLifecycleEventType} from "@/logging/types.js";

/**
 * Names of lifecycle events emitted by the high-level socket.
 */
export type RSocketConnectionEventType = RSocketLifecycleEventType;

/**
 * UI-friendly connection status derived from lifecycle events.
 */
export type RSocketConnectionStatus =
    | "connecting"
    | "connected"
    | "disconnected"
    | "reconnecting"
    | "closed";

/**
 * Common payload shared by every lifecycle event.
 */
interface BaseConnectionEvent<T extends RSocketConnectionEventType> {
    /** Lifecycle event discriminant. */
    readonly type: T;
    /** UI-friendly connection status after this event. */
    readonly status: RSocketConnectionStatus;
    /** Whether a connected low-level session is available after this event. */
    readonly connected: boolean;
    /** Whether the client is currently waiting for or performing reconnect. */
    readonly recovering: boolean;
    /** Current reconnect attempt number. */
    readonly attempt: number;
    /** Whether this event belongs to a reconnect attempt. */
    readonly reconnect: boolean;
    /** Error that caused or accompanied the transition. */
    readonly error?: unknown;
    /** Whether another reconnect attempt is expected. */
    readonly willReconnect?: boolean;
    /** Short user-facing status message suitable for UI banners. */
    readonly message: string;
}

/**
 * Lifecycle event emitted before a delayed reconnect attempt is scheduled.
 */
interface ReconnectingEvent extends BaseConnectionEvent<"reconnecting"> {
    /** Delay before the next physical transport connection attempt. */
    readonly delayMs: number;
}

/**
 * Lifecycle event emitted when connecting, reconnecting, closing, or failing.
 */
export type RSocketConnectionEvent =
    | BaseConnectionEvent<"connecting">
    | BaseConnectionEvent<"connected">
    | BaseConnectionEvent<"disconnect">
    | ReconnectingEvent
    | BaseConnectionEvent<"resumeRejected">
    | BaseConnectionEvent<"reconnectFailed">
    | BaseConnectionEvent<"closed">;

/**
 * Draft lifecycle event before UI-friendly fields are derived.
 */
export type RSocketConnectionEventDraft =
    | DraftConnectionEvent<"connecting">
    | DraftConnectionEvent<"connected">
    | DraftConnectionEvent<"disconnect">
    | (DraftConnectionEvent<"reconnecting"> & { readonly delayMs: number })
    | DraftConnectionEvent<"resumeRejected">
    | DraftConnectionEvent<"reconnectFailed">
    | DraftConnectionEvent<"closed">;

/**
 * Listener type for a specific lifecycle event.
 */
export type RSocketConnectionEventListener<T extends RSocketConnectionEventType> = (
    event: Extract<RSocketConnectionEvent, { type: T }>
) => void;

/**
 * Listener type for every lifecycle event.
 */
export type RSocketAnyConnectionEventListener = (event: RSocketConnectionEvent) => void;

/**
 * Constructor-time lifecycle handlers.
 */
export interface RSocketConnectionEventHandlers {
    /** Receives every lifecycle event. */
    readonly event?: RSocketAnyConnectionEventListener;
    /** Receives `connecting` events. */
    readonly connecting?: RSocketConnectionEventListener<"connecting">;
    /** Receives `connected` events. */
    readonly connected?: RSocketConnectionEventListener<"connected">;
    /** Receives `disconnect` events. */
    readonly disconnect?: RSocketConnectionEventListener<"disconnect">;
    /** Receives `reconnecting` events. */
    readonly reconnecting?: RSocketConnectionEventListener<"reconnecting">;
    /** Receives events emitted when a responder explicitly rejects protocol Resume. */
    readonly resumeRejected?: RSocketConnectionEventListener<"resumeRejected">;
    /** Receives `reconnectFailed` events. */
    readonly reconnectFailed?: RSocketConnectionEventListener<"reconnectFailed">;
    /** Receives `closed` events. */
    readonly closed?: RSocketConnectionEventListener<"closed">;
}

/**
 * Builds a full lifecycle event with UI-friendly derived fields.
 */
export function connectionEvent(draft: RSocketConnectionEventDraft): RSocketConnectionEvent {
    const status = connectionStatus(draft);
    const connected = draft.type === "connected";
    const recovering =
        draft.type === "reconnecting" ||
        draft.type === "resumeRejected" ||
        (draft.type === "reconnectFailed" && draft.willReconnect === true) ||
        (draft.type === "connecting" && draft.reconnect);
    return {
        ...draft,
        status,
        connected,
        recovering,
        message: connectionMessage(draft)
    } as RSocketConnectionEvent;
}

/**
 * Emits lifecycle events to registered callback listeners.
 */
export class RSocketEventHub {
    private listeners: Map<RSocketConnectionEventType, Set<(event: RSocketConnectionEvent) => void>> | undefined;

    /**
     * Emits an event to callback listeners.
     */
    emit(event: RSocketConnectionEvent): void {
        const listeners = this.listeners?.get(event.type);
        if (!listeners) return;
        for (const listener of [...listeners]) {
            try {
                listener(event);
            } catch {
                // User lifecycle handlers must not break reconnect scheduling.
            }
        }
    }

    /**
     * Reports whether a specific lifecycle event currently has listeners.
     */
    has(type: RSocketConnectionEventType): boolean {
        return (this.listeners?.get(type)?.size ?? 0) > 0;
    }

    /**
     * Registers a typed lifecycle listener and returns an unsubscribe callback.
     */
    on<T extends RSocketConnectionEventType>(
        type: T,
        listener: RSocketConnectionEventListener<T>
    ): () => void {
        const hub = this.listeners ??= new Map<RSocketConnectionEventType, Set<(event: RSocketConnectionEvent) => void>>();
        let listeners = hub.get(type);
        if (listeners === undefined) {
            listeners = new Set<(event: RSocketConnectionEvent) => void>();
            hub.set(type, listeners);
        }
        listeners.add(listener as (event: RSocketConnectionEvent) => void);
        return () => this.off(type, listener);
    }

    /**
     * Removes a previously registered lifecycle listener.
     */
    off<T extends RSocketConnectionEventType>(
        type: T,
        listener: RSocketConnectionEventListener<T>
    ): void {
        const hub = this.listeners;
        const listeners = hub?.get(type);
        if (!listeners) return;
        listeners.delete(listener as (event: RSocketConnectionEvent) => void);
        if (listeners.size === 0) {
            hub?.delete(type);
            if (hub?.size === 0) this.listeners = undefined;
        }
    }

}

/**
 * Draft fields common to every lifecycle event before enrichment.
 */
interface DraftConnectionEvent<T extends RSocketConnectionEventType> {
    /** Lifecycle event discriminant. */
    readonly type: T;
    /** Current reconnect attempt number. */
    readonly attempt: number;
    /** Whether this event belongs to a reconnect attempt. */
    readonly reconnect: boolean;
    /** Error that caused or accompanied the transition. */
    readonly error?: unknown;
    /** Whether another reconnect attempt is expected. */
    readonly willReconnect?: boolean;
}

/**
 * Maps a lifecycle event draft to a UI-friendly status.
 */
function connectionStatus(draft: RSocketConnectionEventDraft): RSocketConnectionStatus {
    switch (draft.type) {
        case "connecting":
            return draft.reconnect ? "reconnecting" : "connecting";
        case "connected":
            return "connected";
        case "disconnect":
            return "disconnected";
        case "reconnecting":
        case "resumeRejected":
            return "reconnecting";
        case "reconnectFailed":
            return draft.willReconnect === true ? "reconnecting" : "closed";
        case "closed":
            return "closed";
    }
}

/**
 * Creates a concise status message for frontend banners and logs.
 */
function connectionMessage(draft: RSocketConnectionEventDraft): string {
    switch (draft.type) {
        case "connecting":
            return draft.reconnect ? "Restoring RSocket connection" : "Opening RSocket connection";
        case "connected":
            return draft.reconnect ? "RSocket connection restored" : "RSocket connection established";
        case "disconnect":
            return draft.willReconnect === true
                ? "RSocket connection interrupted"
                : "RSocket connection disconnected";
        case "reconnecting":
            return `Restoring RSocket connection in ${draft.delayMs}ms`;
        case "resumeRejected":
            return "RSocket resume rejected; opening a fresh connection";
        case "reconnectFailed":
            return draft.willReconnect === true
                ? "RSocket reconnect attempt failed"
                : "RSocket reconnect attempts exhausted";
        case "closed":
            return "RSocket connection closed";
    }
}
