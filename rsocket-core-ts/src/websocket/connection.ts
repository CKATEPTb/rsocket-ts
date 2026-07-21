/** Shared transport contract implementation for an already-created WebSocket. */
import type {Flux, Mono} from "reactor-core-ts";
import {RSocketConnectionError} from "@/errors/index.js";
import type {
    RSocketTransportClose,
    RSocketTransportCloseOptions,
    RSocketTransportConnection
} from "@/transport/types.js";
import {
    webSocketCloseFlux,
    webSocketEventFlux,
    webSocketFrameFlux,
    removeWebSocketListener,
    writeWebSocketFrame
} from "@/websocket/index.js";

/** WHATWG WebSocket ready-state constants shared by browser and Node adapters. */
const OPEN = 1;
const CLOSING = 2;
const CLOSED = 3;
/** WHATWG WebSocket close reasons may occupy at most 123 UTF-8 bytes. */
const CLOSE_REASON_MAX_BYTES = 123;
/** Reused encoder and scalar-sized buffer for allocation-light reason truncation. */
const CLOSE_REASON_ENCODER = new TextEncoder();
const CLOSE_REASON_CHARACTER = new Uint8Array(4);

/** Minimal WebSocket operations required by the shared transport. */
interface RSocketWebSocketTransportSocket {
    /** Binary representation requested for incoming messages when configurable. */
    binaryType?: string;
    /** WHATWG-compatible connection state. */
    readonly readyState: number;
    /** Sends one complete binary message. */
    send(data: Uint8Array<ArrayBuffer>): unknown;
    /** Starts a close handshake. */
    close(code?: number, reason?: string): unknown;
    /** Immediately destroys a failed Node WebSocket when available. */
    terminate?(): unknown;
    /** Node EventEmitter listener registration when provided by `ws`. */
    on?(type: string, listener: (...args: any[]) => void): unknown;
    /** Node EventEmitter listener removal when provided by `ws`. */
    off?(type: string, listener: (...args: any[]) => void): unknown;
}

/** Transport implementation shared by connecting and accepted WebSocket endpoints. */
export class ReactiveWebSocketTransportConnection<
    S extends RSocketWebSocketTransportSocket = RSocketWebSocketTransportSocket
> implements RSocketTransportConnection {
    /** Ordered binary messages containing complete raw RSocket frames. */
    readonly frames: Flux<Uint8Array>;
    /** Native WebSocket errors. */
    readonly errors: Flux<unknown>;
    /** Native WebSocket close notifications. */
    readonly closes: Flux<RSocketTransportClose>;

    /** Configures one socket around its role-specific readiness and close policy. */
    constructor(
        readonly socket: S,
        readonly opened: Mono<void>,
        private readonly errorCloseCode = 1011,
        private readonly terminateOnError = false
    ) {
        if ("binaryType" in socket) socket.binaryType = "arraybuffer";
        guardLateWebSocketErrors(socket);
        this.frames = webSocketFrameFlux(socket);
        this.errors = webSocketEventFlux(socket, "error");
        this.closes = webSocketCloseFlux(socket);
    }

    /** Whether the WebSocket currently accepts binary messages. */
    get isOpen(): boolean {
        return this.socket.readyState === OPEN;
    }

    /** Writes one raw RSocket frame as one binary WebSocket message. */
    write(frame: Uint8Array): void {
        writeWebSocketFrame(this.socket, frame, OPEN);
    }

    /** Closes normally or immediately terminates a failed accepted socket. */
    close(options: RSocketTransportCloseOptions = {}): void {
        if (this.socket.readyState === CLOSING || this.socket.readyState === CLOSED) return;
        if (options.error === true && this.terminateOnError && this.socket.terminate !== undefined) {
            try {
                this.socket.terminate();
                return;
            } catch {
                if (this.socket.readyState === CLOSING || this.socket.readyState === CLOSED) return;
                // A compatible close handshake is the remaining cleanup path.
            }
        }
        try {
            this.socket.close(
                options.code ?? (options.error === true ? this.errorCloseCode : 1000),
                boundedCloseReason(options.reason)
            );
        } catch (error) {
            if (this.socket.readyState === CLOSING || this.socket.readyState === CLOSED) return;
            throw new RSocketConnectionError("WebSocket close failed", error);
        }
    }
}

/** Prevents a late Node `ws` error from becoming an uncaught EventEmitter exception. */
function guardLateWebSocketErrors(socket: RSocketWebSocketTransportSocket): void {
    if (socket.on === undefined || socket.off === undefined) return;
    let active = true;
    const guard = (): void => undefined;
    const release = (): void => {
        if (!active) return;
        active = false;
        removeWebSocketListener(socket, "error", guard);
        removeWebSocketListener(socket, "close", release);
    };
    try {
        socket.on("error", guard);
        socket.on("close", release);
    } catch (error) {
        release();
        throw error;
    }
}

/** Truncates a close reason at a Unicode boundary accepted by WHATWG WebSocket. */
function boundedCloseReason(reason: string | undefined): string | undefined {
    if (reason === undefined) return undefined;
    let result = "";
    let length = 0;
    for (const character of reason) {
        const written = CLOSE_REASON_ENCODER.encodeInto(character, CLOSE_REASON_CHARACTER).written;
        if (length + written > CLOSE_REASON_MAX_BYTES) return result;
        length += written;
        result += character;
    }
    return reason;
}
