/** Internal contracts shared by server session components. */
import type {Frame, Metadata} from "rsocket-frames-ts";
import type {
    EncodedPayload,
    ReactiveTransportBinding,
    RSocketPayloadInput,
    RSocketTransportHandler
} from "rsocket-core-ts";
import type {
    RSocketServerFrameActivityListener,
    RSocketSetupContext,
    RSocketSetupDecision
} from "@/server/types.js";
import type {RSocketServerConnection} from "@/server/connection.js";
import type {RSocketHandlerResult} from "@/controllers/types.js";

/** Fully normalized protocol Resume options. */
export interface NormalizedResumeOptions {
    readonly ttlMs: number;
    readonly maxBufferBytes: number;
}

/** Fully normalized automatic lease options. */
export interface NormalizedLeaseOptions<M = unknown> {
    readonly ttlMs: number;
    readonly requests: number;
    readonly metadata: M | Metadata<unknown> | undefined;
}

/** Validated server configuration retained by all accepted sessions. */
export interface NormalizedServerOptions<D = unknown, M = unknown> {
    readonly resume: NormalizedResumeOptions | undefined;
    readonly lease: NormalizedLeaseOptions<M> | undefined;
    readonly maxFrameLength: number;
    readonly handshakeTimeoutMs: number;
    readonly accept: ((setup: RSocketSetupContext<D, M>) => RSocketSetupDecision) | undefined;
    readonly metadataPush: ((
        metadata: Metadata<M>,
        connection: RSocketServerConnection<D, M>
    ) => RSocketHandlerResult<void> | void) | undefined;
    readonly activityListener: RSocketServerFrameActivityListener<D, M> | undefined;
}

/** Shared callbacks installed on one physical transport subscription. */
export type TransportBindingHandler = RSocketTransportHandler;

/** Shared physical subscription that moves from handshake to a server session. */
export type TransportBinding = ReactiveTransportBinding;

/** Session operations available to one active interaction. */
export interface ResponderSession {
    /** Stable public connection facade. */
    readonly connection: RSocketServerConnection<any, any>;
    /** Whether connection shutdown already released this logical session. */
    readonly isTerminated: boolean;
    /** Consumes admission state and returns a rejection reason when unavailable. */
    acquireRequest(): string | undefined;
    /** Encodes a controller output under negotiated MIME codecs. */
    encode(payload: RSocketPayloadInput<any, any> | undefined): EncodedPayload;
    /** Sends one logical frame with automatic fragmentation. */
    send(frame: Frame): void;
    /** Releases one terminal stream. */
    unregister(streamId: number): void;
    /** Sends and applies one stream-scoped failure. */
    streamError(streamId: number, error: unknown): void;
    /** Terminates the connection for invalid protocol state. */
    protocolError(error: unknown): void;
}

/** Active stream state receiving non-initial stream frames. */
export interface ResponderStream {
    /** Client-generated stream identifier. */
    readonly streamId: number;
    /** Checks one wire fragment before it can allocate reassembly storage. */
    acceptPayloadFragment(frame: import("rsocket-frames-ts").PayloadFrame, continuation?: boolean): boolean;
    /** Adds response credits. */
    handleRequestN(request: number): void;
    /** Handles one reassembled requester payload. */
    handlePayload(frame: import("rsocket-frames-ts").PayloadFrame): void;
    /** Handles a requester stream error. */
    handleError(error: unknown): void;
    /** Handles requester cancellation. */
    handleCancel(): void;
    /** Releases local stream resources. */
    terminate(error?: unknown): void;
}
