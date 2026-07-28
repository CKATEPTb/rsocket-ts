/**
 * Public configuration and diagnostic types for a transport-neutral RSocket client session.
 */
import type {Frame, MimeType, RSocketResumeToken} from "rsocket-frames-ts";
import type {
    PublisherInput as CorePublisherInput,
    RSocketFrameDirection as CoreRSocketFrameDirection,
    RSocketPayloadInput,
    RSocketTransportConnection
} from "rsocket-core-ts";

/** Options supplied whenever a client opens a physical transport connection. */
export interface RSocketTransportOpenOptions {
    /** Maximum time allowed for the transport handshake, in milliseconds. */
    readonly timeoutMs?: number;
    /** Signal that aborts an in-progress transport handshake. */
    readonly abortSignal?: AbortSignal;
}

/** Factory that opens one physical client connection for SETUP or RESUME. */
export type RSocketTransportFactory = (
    options: RSocketTransportOpenOptions
) => RSocketTransportConnection;

/** Input accepted by request-channel publishers. */
export type PublisherInput<T> = CorePublisherInput<T>;

/** Per-request payload encoding options. */
export interface RSocketRequestOptions<D = unknown, M = unknown> {
    /** MIME override for this request's data payload. */
    readonly dataMimeType?: MimeType<D>;
    /** MIME override for this request's metadata payload. */
    readonly metadataMimeType?: MimeType<M>;
    /** Optional request-response timeout in milliseconds. */
    readonly timeout?: number;
}

/** Stream request options, named separately for interaction API readability. */
export type RSocketStreamRequestOptions<D = unknown, M = unknown> = RSocketRequestOptions<D, M>;

/** Request-channel outbound input source. */
export type RSocketChannelInput<D = unknown, M = unknown> = PublisherInput<RSocketPayloadInput<D, M>>;

/** Direction of a frame observed by client diagnostics. */
export type RSocketFrameDirection = CoreRSocketFrameDirection;

/** Diagnostic frame event emitted before a frame is written or after it is read. */
export interface RSocketFrameActivity {
    /** Whether the frame was sent or received. */
    readonly direction: RSocketFrameDirection;
    /** Raw decoded RSocket frame. */
    readonly frame: Frame;
}

/** Listener invoked for diagnostic frame activity. */
export type RSocketFrameActivityListener = (activity: RSocketFrameActivity) => void;

/** Options encoded into the initial RSocket SETUP frame. */
export interface RSocketSetupOptions<D = unknown, M = unknown> {
    /** Keepalive frame interval in milliseconds. */
    readonly keepAliveMs?: number;
    /** Maximum interval without a responder KEEPALIVE, in milliseconds. */
    readonly lifetimeMs?: number;
    /** RSocket major protocol version. */
    readonly majorVersion?: number;
    /** RSocket minor protocol version. */
    readonly minorVersion?: number;
    /** Optional resume token sent in SETUP. */
    readonly resumeToken?: RSocketResumeToken;
    /** Whether requester-side lease limits should be enforced. */
    readonly honorLease?: boolean;
    /** Default MIME type used to encode data payloads. */
    readonly dataMimeType?: MimeType<D>;
    /** Default MIME type used to encode metadata payloads. */
    readonly metadataMimeType?: MimeType<M>;
    /** Optional data/metadata payload included in SETUP. */
    readonly payload?: RSocketPayloadInput<D, M>;
}

/** Configuration shared by all RSocket client transport adapters. */
export interface RSocketClientConfiguration<D = unknown, M = unknown> {
    /** Optional raw frame activity listener. */
    readonly activityListener?: RSocketFrameActivityListener;
    /** Optional dynamic switch for frame activity delivery. */
    readonly activityEnabled?: () => boolean;
    /** Optional receiver for transport-specific best-effort media payloads. */
    readonly mediaListener?: (payload: Uint8Array) => void;
    /** Optional signal used to abort connection opening. */
    readonly abortSignal?: AbortSignal;
    /** SETUP frame options. */
    readonly setup?: RSocketSetupOptions<D, M>;
    /** Optional physical transport open timeout in milliseconds. */
    readonly connectTimeoutMs?: number;
    /** Maximum serialized frame length. */
    readonly maxFrameLength?: number;
}

/** Low-level options consumed by one transport-neutral requester session. */
export interface RSocketClientOptions<D = unknown, M = unknown>
    extends RSocketClientConfiguration<D, M> {
    /** Factory that opens a fresh physical transport connection. */
    readonly transport: RSocketTransportFactory;
}
