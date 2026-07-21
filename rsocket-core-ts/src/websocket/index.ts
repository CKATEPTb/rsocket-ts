/** Binary WebSocket message helpers shared by connecting and accepted adapters. */
import {Flux} from "reactor-core-ts";
import {RSocketConnectionError, RSocketProtocolError} from "@/errors/index.js";
import type {RSocketTransportClose} from "@/transport/types.js";
import {SerialSignalDispatcher} from "@/transport/signals.js";

/** Shared resolved tail used before an asynchronous binary message is queued. */
const RESOLVED_MESSAGE = Promise.resolve();
/** Shared UTF-8 decoder for Node `ws` close-reason buffers. */
const CLOSE_REASON_DECODER = new TextDecoder();

/** Minimal send surface shared by WHATWG WebSocket and Node `ws`. */
interface WritableWebSocket {
    readonly readyState: number;
    send(data: Uint8Array<ArrayBuffer>): unknown;
}

/** Converts one WebSocket event into a hot Reactor stream with deterministic cleanup. */
export function webSocketEventFlux<T = unknown>(socket: unknown, type: string): Flux<T> {
    return Flux.create<T>((sink) => {
        const listener = (...args: unknown[]): void => {
            if (!sink.isCancelled()) sink.next((args.length === 1 ? args[0] : args) as T);
        };
        const cleanup = (): void => removeWebSocketListener(socket, type, listener);
        sink.onCancel(cleanup);
        try {
            addWebSocketListener(socket, type, listener);
            if (sink.isCancelled()) cleanup();
        } catch (error) {
            if (!sink.isCancelled()) sink.error(error);
        }
    });
}

/** Emits binary WebSocket messages in wire order, including asynchronous Blob values. */
export function webSocketFrameFlux(socket: unknown, copy = false): Flux<Uint8Array> {
    return Flux.create<Uint8Array>((sink) => {
        let queue = RESOLVED_MESSAGE;
        let queuedTasks = 0;
        let terminated = false;
        const synchronous = new SerialSignalDispatcher<Uint8Array>(
            (bytes) => {
                if (!terminated && !sink.isCancelled()) sink.next(bytes);
            },
            (bytes) => new Uint8Array(bytes)
        );
        const cleanup = (): void => removeWebSocketListener(socket, "message", message);
        const fail = (error: unknown): void => {
            if (terminated) return;
            terminated = true;
            cleanup();
            synchronous.clear();
            if (!sink.isCancelled()) sink.error(error);
        };
        const message = (...args: unknown[]): void => {
            if (terminated || sink.isCancelled()) return;
            let decoded: Uint8Array | Promise<Uint8Array>;
            try {
                decoded = decodeWebSocketMessage(args, copy);
            } catch (error) {
                fail(error);
                return;
            }
            if (decoded instanceof Uint8Array && queuedTasks === 0) {
                try {
                    synchronous.dispatch(decoded);
                } catch (error) {
                    fail(error);
                }
                return;
            }
            if (!(decoded instanceof Uint8Array)) {
                // The ordered queue may not observe this Promise until an earlier Blob settles.
                void decoded.catch(ignoreEarlyMessageRejection);
            }
            queuedTasks += 1;
            queue = queue.then(() => {
                if (terminated || sink.isCancelled()) return;
                if (decoded instanceof Uint8Array) sink.next(decoded);
                else return decoded.then((bytes) => {
                    if (!terminated && !sink.isCancelled()) sink.next(bytes);
                });
            }).then(() => {
                queuedTasks -= 1;
            }, (error) => {
                queuedTasks -= 1;
                fail(error);
            });
        };
        sink.onCancel(() => {
            terminated = true;
            cleanup();
            synchronous.clear();
        });
        try {
            addWebSocketListener(socket, "message", message);
            if (terminated || sink.isCancelled()) cleanup();
        } catch (error) {
            fail(error);
        }
    });
}

/** Marks an eagerly started Blob conversion as handled until the ordered queue observes it. */
function ignoreEarlyMessageRejection(): void {
}

/** Emits one normalized close event from WHATWG WebSocket or Node `ws`. */
export function webSocketCloseFlux(socket: unknown): Flux<RSocketTransportClose> {
    return Flux.create<RSocketTransportClose>((sink) => {
        let terminated = false;
        const close = (...args: unknown[]): void => {
            if (terminated || sink.isCancelled()) return;
            terminated = true;
            removeWebSocketListener(socket, "close", close);
            const event = args[0];
            const normalized = webSocketClose(event, args[1]);
            sink.next(normalized);
            if (!sink.isCancelled()) sink.complete();
        };
        sink.onCancel(() => {
            terminated = true;
            removeWebSocketListener(socket, "close", close);
        });
        try {
            addWebSocketListener(socket, "close", close);
            if (terminated || sink.isCancelled()) removeWebSocketListener(socket, "close", close);
            else if ((socket as {readonly readyState?: unknown}).readyState === 3) close();
        } catch (error) {
            if (!terminated && !sink.isCancelled()) {
                terminated = true;
                sink.error(error);
            }
        }
    });
}

/** Converts WHATWG or Node WebSocket callback arguments into ordered frame bytes. */
export function decodeWebSocketMessage(
    args: readonly unknown[],
    copy = false
): Uint8Array | Promise<Uint8Array> {
    if (typeof args[1] === "boolean" && !args[1]) {
        throw new RSocketProtocolError("RSocket WebSocket transport expects binary messages");
    }
    const first = args[0];
    const data = typeof first === "object" && first !== null && "data" in first
        ? (first as {readonly data: unknown}).data
        : first;
    return webSocketBinaryData(data, copy);
}

/** Converts one standard WebSocket binary payload into standalone bytes. */
export function webSocketBinaryData(
    data: unknown,
    copy = false
): Uint8Array | Promise<Uint8Array> {
    if (data instanceof Uint8Array) return standaloneBytes(data, copy);
    if (objectTag(data) === "[object ArrayBuffer]") {
        const buffer = data as ArrayBuffer;
        return copy ? new Uint8Array(buffer.slice(0)) : new Uint8Array(buffer);
    }
    if (ArrayBuffer.isView(data)) {
        return standaloneBytes(new Uint8Array(data.buffer, data.byteOffset, data.byteLength), copy);
    }
    if (Array.isArray(data) && data.every((part) => ArrayBuffer.isView(part))) {
        return concatenateViews(data as ArrayBufferView[]);
    }
    if (objectTag(data) === "[object Blob]" && typeof (data as BlobLike).arrayBuffer === "function") {
        return Promise.resolve((data as BlobLike).arrayBuffer()).then((buffer) => new Uint8Array(buffer));
    }
    throw new RSocketProtocolError("RSocket WebSocket transport expects binary messages");
}

/** Returns an ArrayBuffer-backed view accepted by WHATWG WebSocket `send`. */
export function webSocketSendData(frame: Uint8Array): Uint8Array<ArrayBuffer> {
    return frame.buffer instanceof ArrayBuffer
        ? new Uint8Array(frame.buffer, frame.byteOffset, frame.byteLength)
        : Uint8Array.from(frame);
}

/** Normalizes the incompatible WHATWG and Node `ws` close callback shapes. */
function webSocketClose(event: unknown, reasonValue: unknown): RSocketTransportClose {
    if (isWebSocketCloseEvent(event)) {
        return {code: event.code, reason: webSocketCloseDescription(event.code, event.reason), cause: event};
    }
    const code = typeof event === "number" ? event : undefined;
    const reason = webSocketCloseDescription(code, webSocketCloseReason(reasonValue));
    return {
        ...(code === undefined ? {} : {code}),
        reason,
        ...((event === undefined && reasonValue === undefined) ? {} : {cause: [event, reasonValue]})
    };
}

/** Preserves transport context while retaining the peer-provided close detail. */
function webSocketCloseDescription(code: number | undefined, detail: string | undefined): string {
    const status = code === undefined ? "" : ` (${code})`;
    return detail === undefined || detail.length === 0
        ? `WebSocket closed${status}`
        : `WebSocket closed${status}: ${detail}`;
}

/** Detects a WHATWG-compatible close event without relying on DOM globals. */
function isWebSocketCloseEvent(value: unknown): value is {readonly code: number; readonly reason: string} {
    return typeof value === "object" && value !== null &&
        typeof (value as {code?: unknown}).code === "number" &&
        typeof (value as {reason?: unknown}).reason === "string";
}

/** Decodes the close reason accepted by Node `ws` and compatible adapters. */
function webSocketCloseReason(value: unknown): string | undefined {
    if (typeof value === "string") return value;
    if (ArrayBuffer.isView(value) && objectTag(value) === "[object Uint8Array]") {
        return CLOSE_REASON_DECODER.decode(new Uint8Array(value.buffer, value.byteOffset, value.byteLength));
    }
    return undefined;
}

/** Writes one binary message after checking and normalizing WebSocket failures. */
export function writeWebSocketFrame(socket: WritableWebSocket, frame: Uint8Array, openState = 1): void {
    if (socket.readyState !== openState) throw new RSocketConnectionError("WebSocket is not open");
    try {
        socket.send(webSocketSendData(frame));
    } catch (error) {
        throw new RSocketConnectionError("WebSocket send failed", error);
    }
}

/** Registers a listener through WHATWG or EventEmitter APIs. */
export function addWebSocketListener(
    socket: unknown,
    type: string,
    listener: (...args: any[]) => void
): void {
    const source = socket as WebSocketEventSource;
    try {
        if (source.addEventListener !== undefined) source.addEventListener(type, listener);
        else if (source.on !== undefined) source.on(type, listener);
        else throw new TypeError("WebSocket must provide addEventListener(...) or on(...)");
    } catch (error) {
        removeWebSocketListener(socket, type, listener);
        throw error;
    }
}

/** Removes a listener while isolating custom transport cleanup failures. */
export function removeWebSocketListener(
    socket: unknown,
    type: string,
    listener: (...args: any[]) => void
): void {
    const source = socket as WebSocketEventSource;
    try {
        if (source.removeEventListener !== undefined) source.removeEventListener(type, listener);
        else source.off?.(type, listener);
    } catch {
        // Listener cleanup cannot change a signal that was already delivered.
    }
}

/** Structural event APIs shared by WHATWG and Node WebSocket implementations. */
interface WebSocketEventSource {
    addEventListener?(type: string, listener: (...args: any[]) => void): void;
    removeEventListener?(type: string, listener: (...args: any[]) => void): void;
    on?(type: string, listener: (...args: any[]) => void): void;
    off?(type: string, listener: (...args: any[]) => void): void;
}

/** Cross-realm Blob operation used by browser WebSocket message events. */
interface BlobLike {
    arrayBuffer(): PromiseLike<ArrayBuffer>;
}

/** Reads an intrinsic object tag without relying on realm-local constructors. */
function objectTag(value: unknown): string {
    return Object.prototype.toString.call(value);
}

/** Detaches views from oversized buffers and optionally copies standalone bytes. */
function standaloneBytes(bytes: Uint8Array, copy: boolean): Uint8Array {
    return copy || bytes.byteOffset !== 0 || bytes.byteLength !== bytes.buffer.byteLength
        ? new Uint8Array(bytes)
        : bytes;
}

/** Concatenates Node `ws` fragmented `Buffer[]` payloads. */
function concatenateViews(parts: readonly ArrayBufferView[]): Uint8Array {
    let length = 0;
    for (const part of parts) length += part.byteLength;
    const bytes = new Uint8Array(length);
    let offset = 0;
    for (const part of parts) {
        const view = new Uint8Array(part.buffer, part.byteOffset, part.byteLength);
        bytes.set(view, offset);
        offset += view.byteLength;
    }
    return bytes;
}
