/** In-memory implementation of the transport contract used by client tests. */
import {Flux, Mono} from "reactor-core-ts";
import type {
    RSocketTransportClose,
    RSocketTransportCloseOptions,
    RSocketTransportConnection
} from "rsocket-core-ts";

/** Ordered in-memory transport with explicit peer frame and close controls. */
export class FakeTransportConnection implements RSocketTransportConnection {
    readonly opened = Mono.create<void>((sink) => sink.success());
    readonly frames: Flux<Uint8Array>;
    readonly errors: Flux<unknown>;
    readonly closes: Flux<RSocketTransportClose>;
    readonly sent: Uint8Array[] = [];
    private readonly frameListeners = new Set<(frame: Uint8Array) => void>();
    private readonly errorListeners = new Set<(error: unknown) => void>();
    private readonly closeListeners = new Set<(close: RSocketTransportClose) => void>();
    private open = true;

    /** Creates hot Reactor streams backed by listener sets. */
    constructor() {
        this.frames = listenerFlux(this.frameListeners);
        this.errors = listenerFlux(this.errorListeners);
        this.closes = listenerFlux(this.closeListeners);
    }

    /** Whether this fake currently accepts writes. */
    get isOpen(): boolean {
        return this.open;
    }

    /** Number of transport callbacks retained by active reactive subscriptions. */
    get listenerCount(): number {
        return this.frameListeners.size + this.errorListeners.size + this.closeListeners.size;
    }

    /** Records an immutable copy of one outbound raw frame. */
    write(frame: Uint8Array): void {
        if (!this.open) throw new Error("Fake transport is closed");
        this.sent.push(frame.slice());
    }

    /** Closes the fake and emits one close signal. */
    close(options: RSocketTransportCloseOptions = {}): void {
        if (!this.open) return;
        this.open = false;
        emit(this.closeListeners, {
            ...(options.code === undefined ? {} : {code: options.code}),
            ...(options.reason === undefined ? {} : {reason: options.reason})
        });
    }

    /** Delivers one responder frame to the requester. */
    receive(frame: Uint8Array): void {
        if (this.open) emit(this.frameListeners, frame);
    }

    /** Simulates an unexpected physical transport loss. */
    disconnect(reason = "connection lost"): void {
        if (!this.open) return;
        this.open = false;
        emit(this.closeListeners, {reason});
    }
}

/** Creates a hot Flux from a mutable listener set. */
function listenerFlux<T>(listeners: Set<(value: T) => void>): Flux<T> {
    return Flux.create<T>((sink) => {
        const listener = (value: T): void => {
            if (!sink.isCancelled()) sink.next(value);
        };
        listeners.add(listener);
        sink.onCancel(() => listeners.delete(listener));
    });
}

/** Emits a value to a stable snapshot of current listeners. */
function emit<T>(listeners: Set<(value: T) => void>, value: T): void {
    for (const listener of [...listeners]) listener(value);
}
