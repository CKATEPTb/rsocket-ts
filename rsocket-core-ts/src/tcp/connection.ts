/** Shared transport contract implementation for an already-created TCP socket. */
import type {Flux, Mono} from "reactor-core-ts";
import {RSocketConnectionError} from "@/errors/index.js";
import type {
    RSocketTransportClose,
    RSocketTransportCloseOptions,
    RSocketTransportConnection
} from "@/transport/types.js";
import {
    isTcpSocketOpen,
    tcpCloseFlux,
    tcpErrorFlux,
    tcpFrameFlux,
    type RSocketTcpEventSocket
} from "@/tcp/events.js";
import {encodeTcpFrame} from "@/tcp/framing.js";
import {
    assertTcpFrameLength,
    TCP_FRAME_PREFIX_LENGTH,
    writeTcpFrameLength
} from "@/tcp/prefix.js";
import {addTcpSocketListener} from "@/tcp/listener.js";

const ZERO_COPY_TCP_FRAME_LENGTH = 4 * 1024;

/** Optional Node.js write batching surface kept outside the public socket API. */
interface RSocketTcpWriteBatch {
    /** Starts collecting writes for one native writev operation. */
    cork(): unknown;
    /** Flushes writes collected after {@link cork}. */
    uncork(): unknown;
}

/** Writable operations required in addition to the shared TCP event surface. */
export interface RSocketTcpDuplexSocket extends RSocketTcpEventSocket {
    /** Disables Nagle buffering for latency-sensitive protocol frames. */
    setNoDelay(noDelay?: boolean): unknown;
    /** Writes one byte chunk to the stream. */
    write(data: Uint8Array): unknown;
    /** Gracefully half-closes the writable stream. */
    end(): unknown;
}

/** Transport implementation shared by connecting and accepted TCP endpoints. */
export class ReactiveTcpTransportConnection<S extends RSocketTcpDuplexSocket = RSocketTcpDuplexSocket>
    implements RSocketTransportConnection {
    /** Ordered raw RSocket frames decoded from TCP chunks. */
    readonly frames: Flux<Uint8Array>;
    /** Native TCP errors. */
    readonly errors: Flux<unknown>;
    /** Native TCP close notifications. */
    readonly closes: Flux<RSocketTransportClose>;

    /** Configures one socket around its role-specific readiness signal. */
    constructor(
        readonly socket: S,
        private readonly maxFrameLength: number,
        readonly opened: Mono<void>
    ) {
        socket.setNoDelay(true);
        guardLateSocketErrors(socket);
        this.frames = tcpFrameFlux(socket, maxFrameLength);
        this.errors = tcpErrorFlux(socket);
        this.closes = tcpCloseFlux(socket);
    }

    /** Whether both sides of the TCP socket currently permit frame writes. */
    get isOpen(): boolean {
        return isTcpSocketOpen(this.socket);
    }

    /** Prefixes and writes one complete raw RSocket frame. */
    write(frame: Uint8Array): void {
        if (!this.isOpen) throw new RSocketConnectionError("TCP socket is not open");
        const writeBatch = frame.byteLength >= ZERO_COPY_TCP_FRAME_LENGTH
            ? tcpWriteBatch(this.socket)
            : undefined;
        if (writeBatch === undefined) {
            const packet = encodeTcpFrame(frame, this.maxFrameLength);
            try {
                this.socket.write(packet);
            } catch (error) {
                throw new RSocketConnectionError("TCP send failed", error);
            }
            return;
        }
        assertTcpFrameLength(frame.byteLength, this.maxFrameLength);
        try {
            this.writeLargeFrame(frame, writeBatch);
        } catch (error) {
            throw new RSocketConnectionError("TCP send failed", error);
        }
    }

    /** Gracefully ends normal sessions and destroys failed transports. */
    close(options: RSocketTransportCloseOptions = {}): void {
        if (this.socket.destroyed) return;
        if (options.error === true) this.socket.destroy();
        else this.socket.end();
    }

    /** Batches prefix and payload without allocating another payload-sized buffer. */
    private writeLargeFrame(frame: Uint8Array, writeBatch: RSocketTcpWriteBatch): void {
        const prefix = new Uint8Array(TCP_FRAME_PREFIX_LENGTH);
        writeTcpFrameLength(prefix, frame.byteLength);
        writeBatch.cork();
        try {
            this.socket.write(prefix);
            this.socket.write(frame);
        } finally {
            writeBatch.uncork();
        }
    }
}

/** Finds Node's optional write batching methods without widening the public contract. */
function tcpWriteBatch(socket: RSocketTcpDuplexSocket): RSocketTcpWriteBatch | undefined {
    const candidate = socket as RSocketTcpDuplexSocket & Partial<RSocketTcpWriteBatch>;
    return typeof candidate.cork === "function" && typeof candidate.uncork === "function"
        ? candidate as RSocketTcpDuplexSocket & RSocketTcpWriteBatch
        : undefined;
}

/**
 * Prevents Node from converting a late socket error into an uncaught process
 * exception after the reactive transport binding has released its listeners.
 */
function guardLateSocketErrors(socket: RSocketTcpEventSocket): void {
    const guard = (): void => undefined;
    const releaseError = addTcpSocketListener(socket, "error", guard);
    try {
        addTcpSocketListener(socket, "close", releaseError, true);
    } catch (error) {
        releaseError();
        throw error;
    }
}
