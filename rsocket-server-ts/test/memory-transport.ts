import {type Flux, Mono, Sinks} from "reactor-core-ts";
import type {
    RSocketTransportClose,
    RSocketTransportCloseOptions,
    RSocketTransportConnection
} from "rsocket-core-ts";

/** In-memory ordered duplex transport used for client/server protocol tests. */
export class MemoryTransport implements RSocketTransportConnection {
    private readonly frameSink = Sinks.many().unicast().onBackpressureBuffer<Uint8Array>();
    private readonly errorSink = Sinks.many().unicast().onBackpressureBuffer<unknown>();
    private readonly closeSink = Sinks.many().replay().limit<RSocketTransportClose>(1);
    private peer: MemoryTransport | undefined;
    private open = true;
    private writesBeforeDrop: number | undefined;
    readonly sent: Uint8Array[] = [];
    readonly opened = Mono.empty<void>();
    readonly frames: Flux<Uint8Array> = this.frameSink.asFlux();
    readonly errors: Flux<unknown> = this.errorSink.asFlux();
    readonly closes: Flux<RSocketTransportClose> = this.closeSink.asFlux();

    /** Links this endpoint to its opposite duplex direction. */
    link(peer: MemoryTransport): void {
        this.peer = peer;
    }

    /** Whether this endpoint currently accepts writes. */
    get isOpen(): boolean {
        return this.open;
    }

    /** Delivers one cloned frame synchronously to the peer. */
    write(frame: Uint8Array): void {
        if (!this.open || this.peer?.open !== true) throw new Error("Memory transport is closed");
        const bytes = frame.slice();
        this.sent.push(bytes);
        if (this.writesBeforeDrop === 0) {
            this.writesBeforeDrop = undefined;
            this.shutdown({reason: "Write delivery lost", cause: new Error("Write delivery lost")});
            throw new Error("Write delivery outcome is unknown");
        }
        this.peer.frameSink.tryEmitNext(bytes);
        if (this.writesBeforeDrop !== undefined) this.writesBeforeDrop -= 1;
    }

    /** Drops immediately before a future write after delivering `count` preceding writes. */
    dropBeforeDeliveryAfter(count: number): void {
        if (!Number.isInteger(count) || count < 0) throw new RangeError("count must be a non-negative integer");
        this.writesBeforeDrop = count;
    }

    /** Closes both directions of the physical transport. */
    close(options: RSocketTransportCloseOptions = {}): void {
        this.shutdown({
            ...(options.code === undefined ? {} : {code: options.code}),
            ...(options.reason === undefined ? {} : {reason: options.reason}),
            ...(options.error === true ? {cause: new Error(options.reason ?? "Transport failed")} : {})
        });
    }

    /** Simulates an abrupt bidirectional network loss. */
    drop(reason = "Network lost"): void {
        this.shutdown({reason, cause: new Error(reason)});
    }

    /** Terminates only the local frame decoder stream with a supplied failure. */
    failFrames(error: unknown): void {
        if (!this.open) return;
        this.frameSink.tryEmitError(error);
    }

    /** Emits one close signal per endpoint without recursive close calls. */
    private shutdown(close: RSocketTransportClose): void {
        if (!this.open) return;
        this.open = false;
        this.closeSink.tryEmitNext(close);
        this.closeSink.tryEmitComplete();
        this.frameSink.tryEmitComplete();
        this.errorSink.tryEmitComplete();
        this.peer?.shutdown(close);
    }
}

/** Creates two linked raw-frame endpoints. */
export function memoryTransportPair(): {client: MemoryTransport; server: MemoryTransport} {
    const client = new MemoryTransport();
    const server = new MemoryTransport();
    client.link(server);
    server.link(client);
    return {client, server};
}
