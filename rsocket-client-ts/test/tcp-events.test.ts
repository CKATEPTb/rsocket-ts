/** TCP opening adapter tests for synchronous and unusual socket implementations. */
import {EventEmitter} from "node:events";
import type {Socket} from "node:net";
import {describe, expect, it} from "vitest";
import {openTcpSocket} from "@/tcp/events.js";

describe("TCP connection opening", () => {
    it("does not retain listeners when a compatible socket connects during registration", async () => {
        const socket = new EagerConnectSocket();

        await expect(openTcpSocket(socket as unknown as Socket, undefined).block()).resolves.toBeUndefined();

        expect(socket.listenerCount("connect")).toBe(0);
        expect(socket.listenerCount("error")).toBe(0);
        expect(socket.listenerCount("close")).toBe(0);
    });

    it("continues removing sibling listeners when one custom off method throws", async () => {
        const socket = new ThrowingCleanupSocket();
        const abort = new AbortController();
        const opened = openTcpSocket(socket as unknown as Socket, undefined, abort.signal).block();

        abort.abort();
        await expect(opened).rejects.toThrow("aborted");

        expect(socket.listenerCount("error")).toBe(0);
        expect(socket.listenerCount("close")).toBe(0);
    });
});

/** Socket double that reproduces a synchronous connect notification from a custom factory. */
class EagerConnectSocket extends EventEmitter {
    readonly destroyed = false;
    readonly connecting = true;
    readonly writable = false;
    readonly readyState = "opening";

    /** Registers normally, then reports connection before registration returns. */
    override once(event: string, listener: (...args: any[]) => void): this {
        super.once(event, listener);
        if (event === "connect") this.emit("connect");
        return this;
    }
}

/** Socket double whose first listener removal cannot block later cleanup. */
class ThrowingCleanupSocket extends EventEmitter {
    readonly destroyed = false;
    readonly connecting = true;
    readonly writable = false;
    readonly readyState = "opening";

    /** Simulates one faulty compatibility adapter during terminal cleanup. */
    override off(event: string, listener: (...args: any[]) => void): this {
        if (event === "connect") throw new Error("connect listener cleanup failed");
        return super.off(event, listener);
    }
}
