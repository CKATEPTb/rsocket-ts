/** Public configuration and context types for `RSocketServer`. */
import type {Frame, Metadata, MimeType, RSocketResumeToken} from "rsocket-frames-ts";
import type {
    RSocketFrameDirection,
    RSocketPayloadFrame,
    RSocketWebTransportSession
} from "rsocket-core-ts";
import type {Flux, Mono} from "reactor-core-ts";
import type {RSocketControllerRegistration, RSocketHandlerResult} from "@/controllers/types.js";
import type {RSocketServerConnection} from "@/server/connection.js";

/** Setup information negotiated for one logical client session. */
export interface RSocketSetupContext<D = unknown, M = unknown> extends RSocketPayloadFrame<D, M> {
    /** Stable connection exposed to lifecycle and controller callbacks. */
    readonly connection: RSocketServerConnection<D, M>;
    /** Client-requested keepalive interval in milliseconds. */
    readonly keepAliveMs: number;
    /** Client-declared maximum interval without a responder KEEPALIVE response. */
    readonly lifetimeMs: number;
    /** Negotiated RSocket major protocol version. */
    readonly majorVersion: number;
    /** Negotiated RSocket minor protocol version. */
    readonly minorVersion: number;
    /** Whether the client requested requester-side lease enforcement. */
    readonly honorsLease: boolean;
    /** Opaque resume token, when protocol Resume was requested. */
    readonly resumeToken?: RSocketResumeToken;
    /** Metadata MIME codec negotiated by SETUP. */
    readonly metadataMimeType: MimeType<M>;
    /** Data MIME codec negotiated by SETUP. */
    readonly dataMimeType: MimeType<D>;
}

/** Result returned by the optional synchronous SETUP authorization callback. */
export type RSocketSetupDecision = boolean | void;

/** Server-side protocol Resume retention policy. */
export interface RSocketResumeOptions {
    /** Time a disconnected logical session remains resumable, in milliseconds. */
    readonly ttlMs: number;
    /** Maximum unacknowledged outbound bytes retained for replay. */
    readonly maxBufferBytes?: number;
}

/** Lease automatically granted when a SETUP requests lease semantics. */
export interface RSocketLeaseOptions<M = unknown> {
    /** Lease validity in milliseconds. */
    readonly ttlMs: number;
    /** Number of new requests permitted during the lease. */
    readonly requests: number;
    /** Optional lease metadata encoded with the negotiated metadata MIME. */
    readonly metadata?: M | Metadata<unknown>;
}

/** Direction of one diagnostic server frame activity event. */
export type RSocketServerFrameDirection = RSocketFrameDirection;

/** Diagnostic event emitted around a decoded RSocket frame. */
export interface RSocketServerFrameActivity<D = unknown, M = unknown> {
    /** Stable logical server connection associated with the frame. */
    readonly connection: RSocketServerConnection<D, M>;
    /** Whether the frame entered or left the server. */
    readonly direction: RSocketServerFrameDirection;
    /** Decoded protocol frame. */
    readonly frame: Frame;
}

/** Listener for optional frame-level diagnostics. */
export type RSocketServerFrameActivityListener<D = unknown, M = unknown> = (
    activity: RSocketServerFrameActivity<D, M>
) => void;

/** Configuration for one server that can accept many logical sessions. */
export interface RSocketServerOptions<D = unknown, M = unknown> {
    /** Declarative controllers indexed by interaction type and exact route. */
    readonly controllers?: readonly RSocketControllerRegistration[];
    /** Optional protocol Resume retention policy. */
    readonly resume?: RSocketResumeOptions;
    /** Lease issued automatically when the client requests lease semantics. */
    readonly lease?: RSocketLeaseOptions<M>;
    /** Largest serialized frame accepted or emitted before fragmentation. */
    readonly maxFrameLength?: number;
    /** Maximum time for an accepted transport to send SETUP or RESUME. */
    readonly handshakeTimeoutMs?: number;
    /** Synchronously accepts or rejects one decoded SETUP frame. */
    readonly accept?: (setup: RSocketSetupContext<D, M>) => RSocketSetupDecision;
    /** Handles asynchronous connection-level metadata sent by a client. */
    readonly metadataPush?: (
        metadata: Metadata<M>,
        connection: RSocketServerConnection<D, M>
    ) => RSocketHandlerResult<void> | void;
    /** Handles best-effort media extension datagrams on WebTransport sessions. */
    readonly media?: (
        payload: Uint8Array,
        connection: RSocketServerConnection<D, M>
    ) => RSocketHandlerResult<void> | void;
    /** Receives optional frame-level diagnostics without changing protocol behavior. */
    readonly activityListener?: RSocketServerFrameActivityListener<D, M>;
}

/** Mapping options applied while accepting one WebTransport session. */
export interface RSocketWebTransportAcceptOptions {
    /** Defensive cap for frames waiting behind another QUIC stream. */
    readonly maxReorderBufferBytes?: number;
    /** Accepts complete REQUEST_FNF frames through best-effort datagrams. */
    readonly unreliableFireAndForget?: boolean;
}

/** Session shape accepted by `RSocketServer.acceptWebTransport`. */
export type RSocketAcceptedWebTransport = RSocketWebTransportSession;

/** Address and socket settings used by the built-in Node TCP listener. */
export interface RSocketTcpListenOptions {
    /** Local interface to bind; defaults to `127.0.0.1`. */
    readonly host?: string;
    /** TCP port, or `0` to request an ephemeral operating-system port. */
    readonly port: number;
    /** Maximum queued but not yet accepted TCP connections. */
    readonly backlog?: number;
}

/** Bound local TCP address after a listener starts. */
export interface RSocketTcpServerAddress {
    /** Bound interface or IP address. */
    readonly host: string;
    /** Bound TCP port, including an operating-system-assigned ephemeral port. */
    readonly port: number;
}

/** Running Node TCP listener returned by `RSocketServer.listenTcp`. */
export interface RSocketTcpServerListener<D = unknown, M = unknown> {
    /** Logical sessions accepted while this event stream has demand. */
    readonly connections: Flux<RSocketServerConnection<D, M>>;
    /** Listener and handshake failures that do not stop the listener. */
    readonly errors: Flux<unknown>;
    /** Current bound address. */
    readonly address: RSocketTcpServerAddress;
    /** Stops accepting sockets and completes both event streams. */
    close(): Mono<void>;
}
