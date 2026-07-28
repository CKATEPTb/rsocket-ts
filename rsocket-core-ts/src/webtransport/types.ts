/** Structural WebTransport contracts shared by browser and server adapters. */
import type {RSocketTransportConnection} from "@/transport/types.js";

/** Reader returned by a WHATWG-compatible readable stream. */
export interface RSocketWebTransportReader<T> {
    /** Reads the next stream item. */
    read(): PromiseLike<ReadableStreamReadResult<T>>;
    /** Cancels pending reads and releases transport resources. */
    cancel?(reason?: unknown): PromiseLike<void> | void;
    /** Releases the reader lock when supported. */
    releaseLock?(): void;
}

/** Minimal readable stream surface required by the adapter. */
export interface RSocketWebTransportReadable<T> {
    /** Acquires one exclusive reader. */
    getReader(): RSocketWebTransportReader<T>;
}

/** Writer returned by a WHATWG-compatible writable stream. */
export interface RSocketWebTransportWriter<T> {
    /** Writes one ordered chunk. */
    write(value: T): PromiseLike<void> | void;
    /** Gracefully finishes the stream. */
    close?(): PromiseLike<void> | void;
    /** Aborts the stream after a failure. */
    abort?(reason?: unknown): PromiseLike<void> | void;
    /** Releases the writer lock when supported. */
    releaseLock?(): void;
}

/** Minimal writable stream surface required by the adapter. */
export interface RSocketWebTransportWritable<T> {
    /** Acquires one exclusive writer. */
    getWriter(): RSocketWebTransportWriter<T>;
}

/** One reliable bidirectional WebTransport byte stream. */
export interface RSocketWebTransportBidirectionalStream {
    /** Bytes received from the stream initiator or peer. */
    readonly readable: RSocketWebTransportReadable<Uint8Array>;
    /** Bytes sent back to the stream peer. */
    readonly writable: RSocketWebTransportWritable<Uint8Array>;
}

/** Datagram duplex surface exposed by a WebTransport session. */
export interface RSocketWebTransportDatagrams {
    /** Unreliable incoming datagrams; current APIs may emit `null` for a dropped packet. */
    readonly readable: RSocketWebTransportReadable<Uint8Array | null>;
    /** Earlier outgoing datagram stream retained for compatible implementations. */
    readonly writable?: RSocketWebTransportWritable<Uint8Array>;
    /** Creates an outgoing datagram stream in the current WebTransport API. */
    createWritable?(): RSocketWebTransportWritable<Uint8Array>;
    /** Current WebTransport maximum outgoing datagram size. */
    readonly maxDatagramSize?: number;
    /** Earlier name used by compatible WebTransport implementations. */
    readonly outgoingMaxDatagramSize?: number;
}

/** Transport-independent close information resolved by `WebTransport.closed`. */
export interface RSocketWebTransportCloseInfo {
    /** Application close code supplied by the peer. */
    readonly closeCode?: number;
    /** Human-readable close reason supplied by the peer. */
    readonly reason?: string;
}

/** Minimal browser or server WebTransport session consumed by the mapping. */
export interface RSocketWebTransportSession {
    /** Resolves after the CONNECT handshake establishes the session. */
    readonly ready: PromiseLike<void>;
    /** Resolves for graceful closure and rejects for transport failure. */
    readonly closed: PromiseLike<RSocketWebTransportCloseInfo | undefined>;
    /** Peer-created reliable bidirectional streams. */
    readonly incomingBidirectionalStreams: RSocketWebTransportReadable<RSocketWebTransportBidirectionalStream>;
    /** Peer-created reliable unidirectional receive streams. */
    readonly incomingUnidirectionalStreams: RSocketWebTransportReadable<RSocketWebTransportReadable<Uint8Array>>;
    /** Optional unreliable datagram channel. */
    readonly datagrams?: RSocketWebTransportDatagrams;
    /** Whether the established first hop supports unreliable delivery. */
    readonly reliability?: "pending" | "reliable-only" | "supports-unreliable";
    /** Earlier location used by compatible implementations for the outgoing datagram limit. */
    readonly maxDatagramSize?: number;

    /** Creates one locally initiated reliable bidirectional stream. */
    createBidirectionalStream(options?: {
        /** Waits for peer stream credit instead of failing while the limit is exhausted. */
        readonly waitUntilAvailable?: boolean;
    }): PromiseLike<RSocketWebTransportBidirectionalStream>;
    /** Creates one locally initiated reliable unidirectional send stream. */
    createUnidirectionalStream(options?: {
        /** Waits for peer stream credit instead of failing while the limit is exhausted. */
        readonly waitUntilAvailable?: boolean;
    }): PromiseLike<RSocketWebTransportWritable<Uint8Array>>;
    /** Closes the complete WebTransport session. */
    close(options?: {readonly closeCode?: number; readonly reason?: string}): void;
}

/** Endpoint role controlling which side owns the initial control stream. */
export type RSocketWebTransportRole = "requester" | "responder";

/** Configuration for the RSocket-over-WebTransport mapping. */
export interface RSocketWebTransportConnectionOptions {
    /** Side opening SETUP/RESUME or accepting it. */
    readonly role: RSocketWebTransportRole;
    /** Largest raw RSocket frame accepted by one mapping record. */
    readonly maxFrameLength?: number;
    /** Maximum out-of-order reliable bytes retained across QUIC streams. */
    readonly maxReorderBufferBytes?: number;
    /** Sends complete REQUEST_FNF frames as best-effort datagrams. */
    readonly unreliableFireAndForget?: boolean;
    /** Optional timeout for the WebTransport session handshake. */
    readonly timeoutMs?: number;
    /** Optional cancellation signal for the WebTransport session handshake. */
    readonly abortSignal?: AbortSignal;
}

/** RSocket transport with the WebTransport media datagram extension. */
export interface RSocketWebTransportConnection extends RSocketTransportConnection {
    /** Best-effort media payloads received outside standard RSocket positions. */
    readonly media: import("reactor-core-ts").Flux<Uint8Array>;
    /** Lost best-effort FNF stream IDs restored to global transport order. */
    readonly skippedFireAndForget: import("reactor-core-ts").Flux<number>;
    /** Sends one best-effort media payload. */
    writeMedia(payload: Uint8Array): void;
}
