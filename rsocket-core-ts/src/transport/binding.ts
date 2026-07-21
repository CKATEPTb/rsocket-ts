/** Stable reactive subscriptions around one physical RSocket transport. */
import type {Disposable} from "reactor-core-ts";
import {RSocketConnectionError} from "@/errors/index.js";
import type {RSocketTransportConnection} from "@/transport/types.js";

/** Signals emitted by one subscribed physical transport. */
export interface RSocketTransportHandler {
    /** Handles one complete ordered raw RSocket frame. */
    frame(bytes: Uint8Array): void;
    /** Handles failure of the frame stream itself. */
    frameError(error: unknown): void;
    /** Handles a native transport error signal. */
    error(error: unknown): void;
    /** Handles physical transport termination. */
    close(error: unknown): void;
}

/** Subscribes once and permits atomic protocol-handler replacement. */
export class ReactiveTransportBinding {
    private readonly disposables: Disposable[] = [];
    private started = false;
    private disposed = false;

    /** Retains the transport and its initial handshake or session handler. */
    constructor(
        readonly connection: RSocketTransportConnection,
        private handler: RSocketTransportHandler
    ) {
    }

    /** Subscribes once while handling synchronous signal reentrancy safely. */
    start(): void {
        if (this.started || this.disposed) return;
        this.started = true;
        try {
            this.track(this.connection.frames.subscribe(
                (bytes) => this.handler.frame(bytes),
                (error) => this.frameFailed(error),
                () => this.transportClosed(new RSocketConnectionError("RSocket transport frame stream completed"))
            ));
            if (this.disposed) return;
            this.track(this.connection.errors.subscribe(
                (error) => this.handler.error(error),
                (error) => this.handler.error(error)
            ));
            if (this.disposed) return;
            this.track(this.connection.closes.subscribe(
                (close) => this.transportClosed(
                    close.cause instanceof Error
                        ? close.cause
                        : new RSocketConnectionError(close.reason ?? "RSocket transport closed", close.cause)
                ),
                (error) => this.transportClosed(error)
            ));
        } catch (error) {
            this.frameFailed(error);
        }
    }

    /** Redirects subsequent signals without resubscribing or creating a receive gap. */
    setHandler(handler: RSocketTransportHandler): void {
        this.handler = handler;
    }

    /** Writes one raw frame after checking physical transport state. */
    write(bytes: Uint8Array): void {
        if (!this.connection.isOpen) throw new RSocketConnectionError("RSocket transport is not open");
        this.connection.write(bytes);
    }

    /** Closes the physical connection and detaches every listener. */
    close(reason: string, error = false): void {
        this.dispose();
        this.connection.close({reason, error});
    }

    /** Releases every transport subscription exactly once. */
    dispose(): void {
        if (this.disposed) return;
        this.disposed = true;
        let disposable: Disposable | undefined;
        while ((disposable = this.disposables.pop()) !== undefined) {
            try {
                disposable.dispose();
            } catch {
                // One custom cleanup must not retain the remaining transport listeners.
            }
        }
    }

    /** Retains a subscription unless synchronous handling already disposed the binding. */
    private track(disposable: Disposable): void {
        if (!this.disposed) {
            this.disposables.push(disposable);
            return;
        }
        try {
            disposable.dispose();
        } catch {
            // Synchronous terminal delivery already detached the logical binding.
        }
    }

    /** Detaches sibling streams before reporting terminal frame decoding failure. */
    private frameFailed(error: unknown): void {
        if (this.disposed) return;
        const handler = this.handler;
        this.dispose();
        handler.frameError(error);
    }

    /** Detaches every physical listener before reporting one terminal close. */
    private transportClosed(error: unknown): void {
        if (this.disposed) return;
        const handler = this.handler;
        this.dispose();
        handler.close(error);
    }
}
