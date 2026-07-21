/** TCP framing and connected-socket adapter tests. */
import {EventEmitter} from "node:events";
import {runInNewContext} from "node:vm";
import {Mono} from "reactor-core-ts";
import {RequestResponseFrame, WellKnownMimeType} from "rsocket-frames-ts";
import {describe, expect, it} from "vitest";
import {
    encodeTcpFrame,
    isTcpSocketOpen,
    ReactiveTcpTransportConnection,
    TcpFrameDecoder,
    tcpCloseFlux,
    tcpFrameFlux,
    type RSocketTcpEventSocket
} from "@";

const frame = new RequestResponseFrame(
    1,
    0,
    undefined,
    WellKnownMimeType.APPLICATION_JSON.toPayload({id: 1})
).toUint8Array();

describe("Core TCP framing", () => {
    it("round-trips a frame split at every byte boundary", () => {
        const packet = encodeTcpFrame(frame);
        for (let split = 1; split < packet.byteLength; split += 1) {
            const decoder = new TcpFrameDecoder();
            const received: Uint8Array[] = [];
            decoder.push(packet.subarray(0, split), (value) => received.push(value));
            decoder.push(packet.subarray(split), (value) => received.push(value));
            decoder.finish();
            expect(received).toEqual([frame]);
        }
    });

    it("decodes coalesced packets without retaining an oversized source chunk", () => {
        const first = encodeTcpFrame(frame);
        const secondFrame = frame.slice();
        secondFrame[3] = 3;
        const second = encodeTcpFrame(secondFrame);
        const chunk = new Uint8Array(first.byteLength + second.byteLength);
        chunk.set(first);
        chunk.set(second, first.byteLength);
        const received: Uint8Array[] = [];

        new TcpFrameDecoder().push(chunk, (value) => received.push(value));
        chunk.fill(0);

        expect(received).toEqual([frame, secondFrame]);
        expect(received.every((value) => value.buffer !== chunk.buffer)).toBe(true);
    });

    it("uses zero-copy only for an isolated standalone packet", () => {
        const packet = encodeTcpFrame(frame);
        const direct: Uint8Array[] = [];
        new TcpFrameDecoder().push(packet, (value) => direct.push(value));
        expect(direct[0]?.buffer).toBe(packet.buffer);

        const backing = new Uint8Array(packet.byteLength + 64);
        backing.set(packet, 32);
        const detached: Uint8Array[] = [];
        new TcpFrameDecoder().push(
            backing.subarray(32, 32 + packet.byteLength),
            (value) => detached.push(value)
        );
        expect(detached[0]?.buffer).not.toBe(backing.buffer);
    });

    it("accepts TCP chunks created in another JavaScript realm", () => {
        const packet = encodeTcpFrame(frame);
        const foreign = runInNewContext("Uint8Array.from(bytes)", {bytes: Array.from(packet)}) as Uint8Array;
        const received: Uint8Array[] = [];

        new TcpFrameDecoder().push(foreign, (value) => received.push(value));

        expect(received).toEqual([frame]);
    });

    it("rejects invalid limits, frame lengths, chunks, and truncated endings", () => {
        expect(() => new TcpFrameDecoder(5)).toThrow();
        expect(() => encodeTcpFrame(new Uint8Array(5), 64)).toThrow("header");
        expect(() => encodeTcpFrame(new Uint8Array(65), 64)).toThrow("maxFrameLength");
        expect(() => new TcpFrameDecoder().push([] as unknown as Uint8Array, () => undefined))
            .toThrow("Uint8Array");
        expect(() => new TcpFrameDecoder().push(
            new Uint8Array([0, 0, 5, 0, 0, 0, 0, 0]),
            () => undefined
        )).toThrow("invalid 5-byte");

        const decoder = new TcpFrameDecoder();
        decoder.push(encodeTcpFrame(frame).subarray(0, 4), () => undefined);
        expect(() => decoder.finish()).toThrow("incomplete framing");
        expect(decoder.bufferedBytes).toBe(0);
    });

    it("reports decoder and socket-destroy failures through the frame Flux", async () => {
        const cause = new Error("destroy failed");
        const socket = new ThrowingDestroySocket(cause);
        const reported = new Promise<unknown>((resolve) => {
            tcpFrameFlux(socket as unknown as RSocketTcpEventSocket, 1_024)
                .subscribe(() => undefined, resolve);
        });

        socket.emit("end");

        await expect(reported).resolves.toEqual(expect.objectContaining({
            name: "RSocketConnectionError",
            message: expect.stringContaining("shutdown"),
            cause
        }));
        expect(socket.listenerCount("data")).toBe(0);
        expect(socket.listenerCount("end")).toBe(0);
    });

    it("completes the frame Flux and releases listeners after a clean TCP half-close", async () => {
        const socket = new DuplexSocket();
        let completed = false;
        tcpFrameFlux(socket, 1_024).subscribe({
            onSubscribe: (subscription) => subscription.request(1),
            onNext: () => undefined,
            onError: () => undefined,
            onComplete: () => {
                completed = true;
            }
        });

        socket.emit("end");
        await new Promise((resolve) => setTimeout(resolve, 0));

        expect(completed).toBe(true);
        expect(socket.destroyed).toBe(true);
        expect(socket.listenerCount("data")).toBe(0);
        expect(socket.listenerCount("end")).toBe(0);
    });

    it("classifies a truncated TCP packet as resumable transport loss", async () => {
        const socket = new DuplexSocket();
        const reported = new Promise<unknown>((resolve) => {
            tcpFrameFlux(socket, 1_024).subscribe(() => undefined, resolve);
        });
        socket.emit("data", encodeTcpFrame(frame).subarray(0, 4));

        socket.emit("end");

        await expect(reported).resolves.toEqual(expect.objectContaining({
            name: "RSocketConnectionError",
            message: expect.stringContaining("complete RSocket frame")
        }));
    });

    it("emits closure when listener registration sees an already destroyed socket", async () => {
        const socket = new DuplexSocket();
        socket.destroyed = true;
        const closes: unknown[] = [];

        tcpCloseFlux(socket).subscribe({
            onSubscribe: (subscription) => subscription.request(1),
            onNext: (close) => closes.push(close),
            onError: () => undefined,
            onComplete: () => undefined
        });
        await new Promise((resolve) => setTimeout(resolve, 0));

        expect(closes).toEqual([{reason: "TCP socket closed"}]);
        expect(socket.listenerCount("close")).toBe(0);
    });

    it("emits one closure and releases a listener registered into a synchronous close", async () => {
        const socket = new ImmediateCloseSocket();
        const closes: unknown[] = [];
        let completions = 0;

        tcpCloseFlux(socket).subscribe({
            onSubscribe: (subscription) => subscription.request(2),
            onNext: (close) => closes.push(close),
            onError: () => undefined,
            onComplete: () => {
                completions += 1;
            }
        });
        await new Promise((resolve) => setTimeout(resolve, 0));

        expect(closes).toEqual([{reason: "TCP socket closed"}]);
        expect(completions).toBe(1);
        expect(socket.listenerCount("close")).toBe(0);
    });

    it("preserves coalesced TCP order across synchronous reentrant data events", async () => {
        const socket = new DuplexSocket();
        const second = new RequestResponseFrame(3, 0).toUint8Array();
        const third = new RequestResponseFrame(5, 0).toUint8Array();
        const firstPacket = encodeTcpFrame(frame);
        const secondPacket = encodeTcpFrame(second);
        const coalesced = new Uint8Array(firstPacket.byteLength + secondPacket.byteLength);
        coalesced.set(firstPacket);
        coalesced.set(secondPacket, firstPacket.byteLength);
        const received: Uint8Array[] = [];
        let depth = 0;
        let maximumDepth = 0;
        const subscription = tcpFrameFlux(socket, 1_024).subscribe((value) => {
            depth += 1;
            maximumDepth = Math.max(maximumDepth, depth);
            received.push(value);
            if (received.length === 1) socket.emit("data", encodeTcpFrame(third));
            depth -= 1;
        });

        socket.emit("data", coalesced);
        await new Promise((resolve) => setTimeout(resolve, 0));

        expect(received).toEqual([frame, second, third]);
        expect(maximumDepth).toBe(1);
        subscription.dispose();
    });

    it("rolls back every frame listener when registration fails", async () => {
        const socket = new ThrowingListenerSocket();
        const reported = new Promise<unknown>((resolve) => {
            tcpFrameFlux(socket as unknown as RSocketTcpEventSocket, 1_024)
                .subscribe(() => undefined, resolve);
        });

        await expect(reported).resolves.toEqual(expect.objectContaining({
            message: expect.stringContaining("listener registration")
        }));
        expect(socket.listenerCount("data")).toBe(0);
        expect(socket.listenerCount("end")).toBe(0);
    });

    it("releases frame listeners when registration emits into immediate cancellation", async () => {
        const socket = new ImmediateDataSocket(encodeTcpFrame(frame));
        let subscription: {cancel(): void} | undefined;

        tcpFrameFlux(socket, 1_024).subscribe({
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

        await new Promise((resolve) => setTimeout(resolve, 0));
        expect(socket.listenerCount("data")).toBe(0);
        expect(socket.listenerCount("end")).toBe(0);
    });

    it("shares write, close, and framing behavior across endpoint roles", () => {
        const socket = new DuplexSocket();
        const connection = new ReactiveTcpTransportConnection(socket, 1_024, Mono.empty());

        connection.write(frame);
        expect(socket.writes).toEqual([encodeTcpFrame(frame, 1_024)]);
        expect(connection.isOpen).toBe(true);
        expect(() => connection.write(new Uint8Array(5))).toThrow("header");

        connection.close();
        expect(socket.ended).toBe(true);
        connection.close({error: true});
        expect(socket.destroyed).toBe(true);
    });

    it("batches large TCP frames without copying their payload bytes", () => {
        const socket = new CorkingDuplexSocket();
        const connection = new ReactiveTcpTransportConnection(socket, 8_192, Mono.empty());
        const largeFrame = new Uint8Array(4_096);

        connection.write(largeFrame);

        expect(socket.corkCalls).toBe(1);
        expect(socket.uncorkCalls).toBe(1);
        expect(socket.writes).toHaveLength(2);
        expect(socket.writes[0]).toEqual(Uint8Array.of(0, 16, 0));
        expect(socket.writes[1]).toBe(largeFrame);
    });

    it("transfers exact large pending storage and releases decoder ownership", () => {
        const largeFrame = new Uint8Array(128 * 1024);
        const packet = encodeTcpFrame(largeFrame);
        const decoder = new TcpFrameDecoder();
        const received: Uint8Array[] = [];

        decoder.push(packet.subarray(0, packet.byteLength - 1), (value) => received.push(value));
        decoder.push(packet.subarray(packet.byteLength - 1), (value) => received.push(value));

        expect(received).toHaveLength(1);
        expect(received[0]).toEqual(largeFrame);
        expect(received[0]?.buffer.byteLength).toBe(packet.byteLength);
        expect(decoder.bufferedBytes).toBe(0);
        expect((decoder as unknown as {decoder: {storage: Uint8Array}}).decoder.storage.byteLength).toBe(0);
    });

    it("guards late native errors until the TCP socket closes", () => {
        const socket = new DuplexSocket();
        new ReactiveTcpTransportConnection(socket, 1_024, Mono.empty());

        expect(() => socket.emit("error", new Error("late reset"))).not.toThrow();
        expect(socket.listenerCount("error")).toBe(1);

        socket.emit("close", true);
        expect(socket.listenerCount("error")).toBe(0);
    });

    it("accepts compatible open sockets without the optional readyState hint", () => {
        const socket = new DuplexSocket() as DuplexSocket & {readyState?: string};
        Object.defineProperty(socket, "readyState", {value: undefined});

        expect(isTcpSocketOpen(socket)).toBe(true);
    });

    it("rolls back the late-error guard when close registration fails", () => {
        const socket = new ThrowingCloseListenerSocket();

        expect(() => new ReactiveTcpTransportConnection(socket, 1_024, Mono.empty()))
            .toThrow("close listener failed");
        expect(socket.listenerCount("error")).toBe(0);
        expect(socket.listenerCount("close")).toBe(0);
    });
});

/** Minimal socket double whose half-close cleanup fails synchronously. */
class ThrowingDestroySocket extends EventEmitter {
    readonly destroyed = false;
    readonly connecting = false;
    readonly writable = true;
    readonly readyState = "open";

    constructor(private readonly failure: Error) {
        super();
    }

    destroy(): never {
        throw this.failure;
    }
}

/** EventEmitter that retains its second listener before throwing. */
class ThrowingListenerSocket extends ThrowingDestroySocket {
    constructor() {
        super(new Error("destroy should not run"));
    }

    override on(event: string, listener: (...args: any[]) => void): this {
        super.on(event, listener);
        if (event === "end") throw new Error("listener registration failed");
        return this;
    }
}

class DuplexSocket extends EventEmitter {
    destroyed = false;
    readonly connecting = false;
    readonly writable = true;
    readonly readyState = "open";
    readonly writes: Uint8Array[] = [];
    ended = false;

    setNoDelay(): this {
        return this;
    }

    write(data: Uint8Array): boolean {
        this.writes.push(data);
        return true;
    }

    end(): this {
        this.ended = true;
        return this;
    }

    destroy(): this {
        this.destroyed = true;
        return this;
    }
}

/** EventEmitter that publishes one data chunk before `on(...)` returns. */
class ImmediateDataSocket extends DuplexSocket {
    constructor(private readonly chunk: Uint8Array) {
        super();
    }

    override on(event: string, listener: (...args: any[]) => void): this {
        super.on(event, listener);
        if (event === "data") listener(this.chunk);
        return this;
    }
}

/** EventEmitter that reports closure before `once(...)` returns. */
class ImmediateCloseSocket extends DuplexSocket {
    override once(event: string, listener: (...args: any[]) => void): this {
        super.once(event, listener);
        if (event === "close") listener(false);
        return this;
    }
}

/** Duplex socket that retains a close listener before registration fails. */
class ThrowingCloseListenerSocket extends DuplexSocket {
    override once(event: string, listener: (...args: any[]) => void): this {
        super.once(event, listener);
        if (event === "close") throw new Error("close listener failed");
        return this;
    }
}

/** Duplex socket exposing writable batching for large-frame zero-copy tests. */
class CorkingDuplexSocket extends DuplexSocket {
    corkCalls = 0;
    uncorkCalls = 0;

    /** Records one writable cork operation. */
    cork(): void {
        this.corkCalls += 1;
    }

    /** Records one writable uncork operation. */
    uncork(): void {
        this.uncorkCalls += 1;
    }
}
