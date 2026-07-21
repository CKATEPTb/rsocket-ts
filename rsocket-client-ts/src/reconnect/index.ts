/** Internal transport-neutral reconnect support. */
export {reconnectDelay} from "@/reconnect/backoff.js";
export {deferred} from "@/reconnect/deferred.js";
export {connectionEvent, RSocketEventHub} from "@/reconnect/events.js";
export {normalizeReconnectOptions, RECONNECT_MIN_UPTIME_MS} from "@/reconnect/options.js";
export {immediateReconnectSignals} from "@/reconnect/signals.js";
export type {Deferred} from "@/reconnect/deferred.js";
export type {
    RSocketAnyConnectionEventListener,
    RSocketConnectionEvent,
    RSocketConnectionEventDraft,
    RSocketConnectionEventHandlers
} from "@/reconnect/events.js";
export type {RSocketReconnectOptions} from "@/reconnect/options.js";
export type {RSocketReconnectSignals} from "@/reconnect/signals.js";
