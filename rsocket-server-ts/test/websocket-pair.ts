import type {RSocketAcceptedWebSocket} from "@/index.js";

type Listener = (...args: any[]) => void;

/** Minimal in-process WHATWG WebSocket used by both published transport adapters. */
export class TestWebSocket implements RSocketAcceptedWebSocket {
    private readonly listeners = new Map<string, Set<Listener>>();
    private peer: TestWebSocket | undefined;
    binaryType: BinaryType = "blob";
    readyState = 1;
    lastCloseReason: string | undefined;

    /** Connects this endpoint to its opposite direction. */
    link(peer: TestWebSocket): void {
        this.peer = peer;
    }

    /** Copies and asynchronously delivers one binary WebSocket message. */
    send(data: BufferSource): void {
        if (this.readyState !== 1 || this.peer?.readyState !== 1) throw new Error("WebSocket is closed");
        const bytes = data instanceof ArrayBuffer
            ? new Uint8Array(data.slice(0))
            : new Uint8Array(data.buffer.slice(data.byteOffset, data.byteOffset + data.byteLength));
        queueMicrotask(() => this.peer?.emit("message", {data: bytes}));
    }

    /** Completes a bidirectional close handshake once. */
    close(code = 1000, reason = ""): void {
        this.finishClose(code, reason);
        this.peer?.finishClose(code, reason);
    }

    /** Immediately closes this test socket as a failed Node-compatible peer. */
    terminate(): void {
        this.close(1011, "terminated");
    }

    /** Registers a WHATWG-style event listener. */
    addEventListener(type: string, listener: Listener): void {
        const listeners = this.listeners.get(type) ?? new Set<Listener>();
        listeners.add(listener);
        this.listeners.set(type, listeners);
    }

    /** Removes a WHATWG-style event listener. */
    removeEventListener(type: string, listener: Listener): void {
        this.listeners.get(type)?.delete(listener);
    }

    /** Emits an intentionally invalid text message for protocol-boundary tests. */
    sendText(value: string): void {
        if (this.readyState !== 1 || this.peer?.readyState !== 1) throw new Error("WebSocket is closed");
        queueMicrotask(() => this.peer?.emit("message", {data: value}));
    }

    /** Publishes one event to a stable listener snapshot. */
    private emit(type: string, event: unknown): void {
        for (const listener of [...this.listeners.get(type) ?? []]) listener(event);
    }

    /** Transitions one side to CLOSED and reports a close event. */
    private finishClose(code: number, reason: string): void {
        if (this.readyState >= 2) return;
        this.readyState = 3;
        this.lastCloseReason = reason;
        queueMicrotask(() => this.emit("close", {code, reason}));
    }
}

/** Creates two already-open linked WebSocket endpoints. */
export function webSocketPair(): {client: TestWebSocket; server: TestWebSocket} {
    const client = new TestWebSocket();
    const server = new TestWebSocket();
    client.link(server);
    server.link(client);
    return {client, server};
}

/** Minimal EventEmitter-style socket matching the callback shape of Node `ws`. */
export class TestNodeWebSocket implements RSocketAcceptedWebSocket {
    private readonly listeners = new Map<string, Set<Listener>>();
    readyState = 1;

    /** Records no outbound bytes; adapter unit tests only exercise inbound events. */
    send(_data: BufferSource): void {
    }

    /** Closes this socket and emits the Node `close(code, reason)` shape. */
    close(code = 1000, reason = ""): void {
        if (this.readyState >= 2) return;
        this.readyState = 3;
        this.emit("close", code, new TextEncoder().encode(reason));
    }

    /** Registers an EventEmitter-style listener. */
    on(type: string, listener: Listener): void {
        const listeners = this.listeners.get(type) ?? new Set<Listener>();
        listeners.add(listener);
        this.listeners.set(type, listeners);
    }

    /** Removes an EventEmitter-style listener. */
    off(type: string, listener: Listener): void {
        this.listeners.get(type)?.delete(listener);
    }

    /** Emits the Node `message(data, isBinary)` callback shape. */
    emitMessage(data: Uint8Array, isBinary: boolean): void {
        this.emit("message", data, isBinary);
    }

    /** Invokes a stable snapshot of EventEmitter listeners. */
    private emit(type: string, ...args: unknown[]): void {
        for (const listener of [...this.listeners.get(type) ?? []]) listener(...args);
    }
}
