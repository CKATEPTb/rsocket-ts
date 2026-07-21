/** Transport contracts consumed by requester and responder protocol engines. */
import type {Flux, Mono} from "reactor-core-ts";

/** Transport-independent close information emitted to the protocol engine. */
export interface RSocketTransportClose {
    /** Optional transport-specific close code. */
    readonly code?: number;
    /** Human-readable transport close reason. */
    readonly reason?: string;
    /** Native close event or error retained for diagnostics. */
    readonly cause?: unknown;
}

/** Options used by the protocol engine to close a physical transport. */
export interface RSocketTransportCloseOptions {
    /** Optional transport-specific close code requested by an application. */
    readonly code?: number;
    /** Human-readable close reason. */
    readonly reason?: string;
    /** Whether the close was caused by a protocol or transport failure. */
    readonly error?: boolean;
}

/**
 * One ordered, reliable, bidirectional connection carrying raw RSocket frames.
 *
 * TCP implementations add/remove the 24-bit frame-length field. WebSocket
 * implementations map one binary message to one raw RSocket frame.
 */
export interface RSocketTransportConnection {
    /** Completes after the physical transport is ready for writes. */
    readonly opened: Mono<void>;
    /** Complete inbound RSocket frames without transport framing bytes. */
    readonly frames: Flux<Uint8Array>;
    /** Non-terminal native transport errors. */
    readonly errors: Flux<unknown>;
    /** Physical transport close notifications. */
    readonly closes: Flux<RSocketTransportClose>;
    /** Whether bytes can currently be written to the transport. */
    readonly isOpen: boolean;

    /** Writes one complete raw RSocket frame immediately. */
    write(frame: Uint8Array): void;

    /** Closes the physical transport. */
    close(options?: RSocketTransportCloseOptions): void;
}
