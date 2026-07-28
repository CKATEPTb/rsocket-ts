/** Shared WebSocket binary decoding and ordered Flux tests. */
import {Mono} from "reactor-core-ts";
import {runInNewContext} from "node:vm";
import {describe, expect, it} from "vitest";
import {
    addWebSocketListener,
    ReactiveWebSocketTransportConnection,
    webSocketBinaryData,
    webSocketCloseFlux,
    webSocketFrameFlux,
    webSocketSendData
} from "@";

describe("Core WebSocket adapters", () => {
    it("detaches views from larger buffers and preserves standalone bytes", () => {
        const standalone = Uint8Array.of(1, 2);
        const backing = new Uint8Array(10);
        backing.set([3, 4], 4);
        const view = backing.subarray(4, 6);

        expect(webSocketBinaryData(standalone)).toBe(standalone);
        const detached = webSocketBinaryData(view) as Uint8Array;
        expect(detached).toEqual(Uint8Array.of(3, 4));
        expect(detached.buffer).not.toBe(backing.buffer);
        expect(webSocketBinaryData(standalone, true)).not.toBe(standalone);
    });

    it("supports Node ws Buffer arrays and rejects text messages", () => {
        expect(webSocketBinaryData([Uint8Array.of(1), Uint8Array.of(2, 3)]))
            .toEqual(Uint8Array.of(1, 2, 3));
        expect(() => webSocketBinaryData("text")).toThrow("binary messages");
    });

    it("accepts ArrayBuffer and Blob payloads created in another realm", async () => {
        const buffer = runInNewContext("new Uint8Array([4, 5]).buffer") as ArrayBuffer;
        const blob = {
            [Symbol.toStringTag]: "Blob",
            arrayBuffer: () => Promise.resolve(buffer)
        };

        expect(buffer).not.toBeInstanceOf(ArrayBuffer);
        expect(webSocketBinaryData(buffer)).toEqual(Uint8Array.of(4, 5));
        await expect(webSocketBinaryData(blob)).resolves.toEqual(Uint8Array.of(4, 5));
    });

    it("normalizes outgoing shared-buffer views to WHATWG-compatible ArrayBuffer bytes", () => {
        const regular = Uint8Array.of(1, 2);
        const regularResult = webSocketSendData(regular);
        expect(regularResult).toBe(regular);
        expect(regularResult).toEqual(regular);
        expect(regularResult.buffer).toBe(regular.buffer);

        const shared = new Uint8Array(new SharedArrayBuffer(2));
        shared.set([3, 4]);
        const sharedResult = webSocketSendData(shared);
        expect(sharedResult).toEqual(Uint8Array.of(3, 4));
        expect(sharedResult.buffer).toBeInstanceOf(ArrayBuffer);
    });

    it("bounds close reasons without splitting a UTF-8 character", () => {
        const socket = new FakeWebSocket();
        const opened = Mono.create<void>((sink) => sink.success());
        const connection = new ReactiveWebSocketTransportConnection(socket, opened);
        const reason = `${"x".repeat(120)}😀tail`;
        connection.close({reason});

        expect(socket.closeReason).toBe("x".repeat(120));
        expect(new TextEncoder().encode(socket.closeReason).byteLength).toBeLessThanOrEqual(123);
    });

    it("keeps asynchronous Blob and synchronous messages in wire order", async () => {
        const socket = new FakeWebSocket();
        const received: Uint8Array[] = [];
        const subscription = webSocketFrameFlux(socket).subscribe((bytes) => received.push(bytes));

        socket.emit("message", {data: new Blob([Uint8Array.of(1)])});
        socket.emit("message", {data: Uint8Array.of(2)});
        await waitFor(() => received.length === 2);

        expect(received).toEqual([Uint8Array.of(1), Uint8Array.of(2)]);
        subscription.dispose();
        expect(socket.listenerCount("message")).toBe(0);
    });

    it("handles a queued Blob failure before an earlier Blob settles", async () => {
        const socket = new FakeWebSocket();
        const failure = new Error("Blob decode failed");
        let resolveFirst!: (buffer: ArrayBuffer) => void;
        const first = blobLike(() => new Promise<ArrayBuffer>((resolve) => {
            resolveFirst = resolve;
        }));
        const second = blobLike(() => Promise.reject(failure));
        const receivedError = new Promise<unknown>((resolve) => {
            webSocketFrameFlux(socket).subscribe(() => undefined, resolve);
        });

        socket.emit("message", {data: first});
        socket.emit("message", {data: second});
        await new Promise((resolve) => setTimeout(resolve, 0));
        resolveFirst(Uint8Array.of(1).buffer);

        await expect(receivedError).resolves.toBe(failure);
        expect(socket.listenerCount("message")).toBe(0);
    });

    it("serializes synchronously reentrant WebSocket messages", async () => {
        const socket = new FakeWebSocket();
        const received: number[] = [];
        let depth = 0;
        let maximumDepth = 0;
        const subscription = webSocketFrameFlux(socket).subscribe((bytes) => {
            depth += 1;
            maximumDepth = Math.max(maximumDepth, depth);
            received.push(bytes[0] as number);
            if (bytes[0] === 1) socket.emit("message", {data: Uint8Array.of(2)});
            depth -= 1;
        });

        socket.emit("message", {data: Uint8Array.of(1)});
        await waitFor(() => received.length === 2);

        expect(received).toEqual([1, 2]);
        expect(maximumDepth).toBe(1);
        subscription.dispose();
    });

    it("releases a message listener when registration emits into immediate cancellation", async () => {
        const socket = new ImmediateMessageWebSocket();
        let subscription: {cancel(): void} | undefined;

        webSocketFrameFlux(socket).subscribe({
            onSubscribe(value) {
                subscription = value;
                value.request(1);
            },
            onNext() {
                subscription?.cancel();
            },
            onError() {
            },
            onComplete() {
            }
        });

        await waitFor(() => socket.listenerCount("message") === 0);
        expect(socket.listenerCount("message")).toBe(0);
    });

    it("rejects Node ws text callbacks and removes the message listener", async () => {
        const socket = new FakeWebSocket();
        const failure = new Promise<unknown>((resolve) => {
            webSocketFrameFlux(socket).subscribe(() => undefined, resolve);
        });

        socket.emit("message", Uint8Array.of(1), false);

        await expect(failure).resolves.toEqual(expect.objectContaining({message: expect.stringContaining("binary")}));
        expect(socket.listenerCount("message")).toBe(0);
    });

    it("rolls back a listener retained by a failing custom WebSocket", () => {
        const socket = new RetainingThrowWebSocket();

        expect(() => addWebSocketListener(socket, "message", () => undefined))
            .toThrow("listener registration failed");
        expect(socket.listenerCount("message")).toBe(0);
    });

    it("guards late Node WebSocket errors until the native close event", () => {
        const socket = new NodeStyleWebSocket();
        const connection = new ReactiveWebSocketTransportConnection(socket, Mono.empty());
        const errors = connection.errors.subscribe();

        errors.dispose();

        expect(socket.listenerCount("error")).toBe(1);
        expect(() => socket.emit("error", new Error("late failure"))).not.toThrow();
        socket.emit("close", 1000, Uint8Array.of());
        expect(socket.listenerCount("error")).toBe(0);
        expect(socket.listenerCount("close")).toBe(0);
    });

    it("attempts every late-error guard cleanup when one listener removal fails", () => {
        const socket = new ThrowingOffWebSocket();
        new ReactiveWebSocketTransportConnection(socket, Mono.empty());

        socket.emit("close", 1000, Uint8Array.of());

        expect(socket.removedTypes).toEqual(["error", "close"]);
        expect(socket.listenerCount("close")).toBe(0);
    });

    it("normalizes WHATWG and Node ws close callback shapes", async () => {
        const whatwg = new FakeWebSocket();
        const node = new FakeWebSocket();
        const closes: unknown[] = [];
        webSocketCloseFlux(whatwg).subscribe((close) => closes.push(close));
        webSocketCloseFlux(node).subscribe((close) => closes.push(close));

        const event = {code: 1000, reason: "normal"};
        whatwg.emit("close", event);
        node.emit("close", 1011, new TextEncoder().encode("failed"));
        await waitFor(() => closes.length === 2);

        expect(closes).toEqual([
            {code: 1000, reason: "WebSocket closed (1000): normal", cause: event},
            {code: 1011, reason: "WebSocket closed (1011): failed", cause: [1011, expect.any(Uint8Array)]}
        ]);
        expect(whatwg.listenerCount("close")).toBe(0);
        expect(node.listenerCount("close")).toBe(0);
    });

    it("decodes Node ws close reasons created in another realm", async () => {
        const socket = new FakeWebSocket();
        const closes: Array<{reason?: string}> = [];
        const reason = runInNewContext("new Uint8Array([102, 97, 105, 108, 101, 100])");
        webSocketCloseFlux(socket).subscribe((close) => closes.push(close));

        socket.emit("close", 1011, reason);
        await waitFor(() => closes.length === 1);

        expect(closes[0]?.reason).toBe("WebSocket closed (1011): failed");
    });

    it("emits closure for an already closed WebSocket", async () => {
        const socket = new FakeWebSocket();
        socket.readyState = 3;
        const closes: unknown[] = [];

        webSocketCloseFlux(socket).subscribe({
            onSubscribe: (subscription) => subscription.request(1),
            onNext: (close) => closes.push(close),
            onError: () => undefined,
            onComplete: () => undefined
        });
        await waitFor(() => closes.length === 1);

        expect(closes).toEqual([{reason: "WebSocket closed"}]);
        expect(socket.listenerCount("close")).toBe(0);
    });

    it("emits one closure when registration reports close synchronously", async () => {
        const socket = new ImmediateCloseWebSocket();
        const closes: unknown[] = [];
        let completions = 0;

        webSocketCloseFlux(socket).subscribe({
            onSubscribe: (subscription) => subscription.request(2),
            onNext: (close) => closes.push(close),
            onError: () => undefined,
            onComplete: () => {
                completions += 1;
            }
        });
        await waitFor(() => completions === 1);

        expect(closes).toEqual([{reason: "WebSocket closed"}]);
        expect(socket.listenerCount("close")).toBe(0);
    });
});

class FakeWebSocket {
    private readonly listeners = new Map<string, Set<(...args: any[]) => void>>();
    readyState = 1;
    binaryType = "blob";
    closeReason = "";

    send(): void {
    }

    close(_code?: number, reason = ""): void {
        this.closeReason = reason;
        this.readyState = 2;
    }

    addEventListener(type: string, listener: (...args: any[]) => void): void {
        (this.listeners.get(type) ?? this.addType(type)).add(listener);
    }

    removeEventListener(type: string, listener: (...args: any[]) => void): void {
        this.listeners.get(type)?.delete(listener);
    }

    emit(type: string, ...args: unknown[]): void {
        for (const listener of [...this.listeners.get(type) ?? []]) listener(...args);
    }

    listenerCount(type: string): number {
        return this.listeners.get(type)?.size ?? 0;
    }

    private addType(type: string): Set<(...args: any[]) => void> {
        const listeners = new Set<(...args: any[]) => void>();
        this.listeners.set(type, listeners);
        return listeners;
    }
}

/** WebSocket double that retains a listener before reporting registration failure. */
class RetainingThrowWebSocket extends FakeWebSocket {
    override addEventListener(type: string, listener: (...args: any[]) => void): void {
        super.addEventListener(type, listener);
        throw new Error("listener registration failed");
    }
}

/** WebSocket that emits one message before listener registration returns. */
class ImmediateMessageWebSocket extends FakeWebSocket {
    override addEventListener(type: string, listener: (...args: any[]) => void): void {
        super.addEventListener(type, listener);
        if (type === "message") listener({data: Uint8Array.of(1)});
    }
}

/** WebSocket that closes before listener registration returns. */
class ImmediateCloseWebSocket extends FakeWebSocket {
    override addEventListener(type: string, listener: (...args: any[]) => void): void {
        super.addEventListener(type, listener);
        if (type === "close") listener();
    }
}

/** EventEmitter-shaped WebSocket that reproduces Node's unhandled `error` behavior. */
class NodeStyleWebSocket {
    private readonly listeners = new Map<string, Set<(...args: any[]) => void>>();
    readyState = 1;
    binaryType = "nodebuffer";

    send(): void {
    }

    close(): void {
        this.readyState = 2;
    }

    on(type: string, listener: (...args: any[]) => void): void {
        (this.listeners.get(type) ?? this.addType(type)).add(listener);
    }

    off(type: string, listener: (...args: any[]) => void): void {
        this.listeners.get(type)?.delete(listener);
    }

    emit(type: string, ...args: unknown[]): void {
        const listeners = [...this.listeners.get(type) ?? []];
        if (type === "error" && listeners.length === 0) throw args[0];
        for (const listener of listeners) listener(...args);
    }

    listenerCount(type: string): number {
        return this.listeners.get(type)?.size ?? 0;
    }

    private addType(type: string): Set<(...args: any[]) => void> {
        const listeners = new Set<(...args: any[]) => void>();
        this.listeners.set(type, listeners);
        return listeners;
    }
}

/** Node-style socket that fails one cleanup to verify independent removal attempts. */
class ThrowingOffWebSocket extends NodeStyleWebSocket {
    readonly removedTypes: string[] = [];

    override off(type: string, listener: (...args: any[]) => void): void {
        this.removedTypes.push(type);
        if (type === "error") throw new Error("error-listener cleanup failed");
        super.off(type, listener);
    }
}

async function waitFor(condition: () => boolean): Promise<void> {
    const deadline = Date.now() + 1_000;
    while (!condition()) {
        if (Date.now() >= deadline) throw new Error("Timed out waiting for WebSocket messages");
        await new Promise((resolve) => setTimeout(resolve, 0));
    }
}

/** Creates a structural Blob test value with controllable asynchronous decoding. */
function blobLike(arrayBuffer: () => Promise<ArrayBuffer>): unknown {
    return {
        [Symbol.toStringTag]: "Blob",
        arrayBuffer
    };
}
