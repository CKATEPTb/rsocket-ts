/**
 * Diagnostic logging types for socket lifecycle, raw RSocket frames, and
 * declarative controller interactions.
 */
import type {Frame} from "rsocket-frames-ts";
import type {RSocketControllerKind} from "@/controllers/types.js";

/** Connection lifecycle names understood by the transport-neutral logger. */
export type RSocketLifecycleEventType =
    | "connecting"
    | "connected"
    | "disconnect"
    | "reconnecting"
    | "resumeRejected"
    | "reconnectFailed"
    | "closed";

/** Structural lifecycle activity accepted from any client environment. */
export interface RSocketLifecycleActivity {
    /** Lifecycle transition name. */
    readonly type: RSocketLifecycleEventType;
    /** Current reconnect attempt number. */
    readonly attempt: number;
    /** Whether this transition belongs to reconnect. */
    readonly reconnect: boolean;
    /** Delay before the next attempt when one was scheduled. */
    readonly delayMs?: number;
    /** Error associated with the transition. */
    readonly error?: unknown;
}

/**
 * Frame direction in a frame log event.
 */
export type RSocketLogDirection = "send" | "receive";
/**
 * Stage emitted by controller-level interaction logging.
 */
export type RSocketInteractionLogStage = "send" | "receive" | "complete" | "error";

/**
 * User-facing logging options accepted by socket constructor options and
 * `controller.log()`.
 */
export interface RSocketLogOptions {
    /** Enables or disables logging. */
    readonly enabled?: boolean;
    /** Log category shown in default console output and stored on events. */
    readonly category?: string;
    /** Emits raw frame send/receive events. */
    readonly frames?: boolean;
    /** Emits connection lifecycle events. */
    readonly lifecycle?: boolean;
    /** Emits declarative controller interaction events. */
    readonly interactions?: boolean;
    /** Includes raw frames, payloads, and decoded values in log events. */
    readonly payload?: boolean;
    /** Custom sink that receives normalized log events. */
    readonly logger?: RSocketLogSink;
}

/**
 * All accepted logging input forms.
 */
export type RSocketLogInput = boolean | string | RSocketLogSink | RSocketLogOptions;
/**
 * Function that receives normalized log events.
 */
export type RSocketLogSink = (event: RSocketLogEvent) => void;

/**
 * Fully normalized logging options used internally.
 */
export interface NormalizedRSocketLogOptions {
    /** Resolved logging enablement. */
    readonly enabled: boolean;
    /** Resolved event category. */
    readonly category: string;
    /** Resolved frame logging flag. */
    readonly frames: boolean;
    /** Resolved lifecycle logging flag. */
    readonly lifecycle: boolean;
    /** Resolved controller interaction logging flag. */
    readonly interactions: boolean;
    /** Resolved payload inclusion flag. */
    readonly payload: boolean;
    /** Resolved sink used to write events. */
    readonly logger: RSocketLogSink;
}

/**
 * Defaults used when a caller enables logging without specifying each flag.
 */
export interface RSocketLogDefaults {
    /** Default category. */
    readonly category: string;
    /** Default frame logging flag. */
    readonly frames: boolean;
    /** Default lifecycle logging flag. */
    readonly lifecycle: boolean;
    /** Default controller interaction logging flag. */
    readonly interactions: boolean;
    /** Default payload inclusion flag. */
    readonly payload: boolean;
}

/**
 * Shared fields present on every log event.
 */
interface BaseLogEvent<T extends string> {
    /** Discriminant identifying the event family. */
    readonly type: T;
    /** Category configured by socket constructor options or `controller.log()`. */
    readonly category: string;
}

/**
 * Log event emitted for a raw RSocket frame.
 */
export interface RSocketFrameLogEvent extends BaseLogEvent<"frame"> {
    /** Whether the frame was sent or received. */
    readonly direction: RSocketLogDirection;
    /** Human-readable RSocket frame type name. */
    readonly frameType: string;
    /** Stream id from the frame header. */
    readonly streamId: number;
    /** Raw frame flags from the frame header. */
    readonly flags: number;
    /** Raw frame, included only when payload logging is enabled. */
    readonly frame?: Frame;
}

/**
 * Log event emitted for connection lifecycle transitions.
 */
export interface RSocketLifecycleLogEvent extends BaseLogEvent<"lifecycle"> {
    /** Lifecycle event name. */
    readonly event: RSocketLifecycleEventType;
    /** Reconnect attempt number associated with the event. */
    readonly attempt: number;
    /** Whether the event belongs to a reconnect attempt. */
    readonly reconnect: boolean;
    /** Delay before the next reconnect attempt, when relevant. */
    readonly delayMs?: number;
    /** Error associated with the lifecycle transition. */
    readonly error?: unknown;
}

/**
 * Log event emitted for a declarative controller invocation.
 */
export interface RSocketInteractionLogEvent extends BaseLogEvent<"interaction"> {
    /** Controller interaction model. */
    readonly interaction: RSocketControllerKind;
    /** Interaction stage being logged. */
    readonly stage: RSocketInteractionLogStage;
    /** Optional human-readable label. */
    readonly label?: string;
    /** Outbound payload, included only when payload logging is enabled. */
    readonly payload?: unknown;
    /** Inbound value, included only when payload logging is enabled. */
    readonly value?: unknown;
    /** Error, included when the interaction fails. */
    readonly error?: unknown;
}

/**
 * Union of every event shape a log sink can receive.
 */
export type RSocketLogEvent =
    | RSocketFrameLogEvent
    | RSocketLifecycleLogEvent
    | RSocketInteractionLogEvent;
