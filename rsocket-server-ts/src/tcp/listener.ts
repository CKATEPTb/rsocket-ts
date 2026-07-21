/** Built-in Node TCP listener for the transport-neutral responder. */
import {createServer, type Server, type Socket} from "node:net";
import {type Flux, Mono, Sinks} from "reactor-core-ts";
import {RSocketConnectionError} from "rsocket-core-ts";
import type {RSocketServerConnection} from "@/server/connection.js";
import type {RSocketServer} from "@/server/server.js";
import type {
    RSocketTcpListenOptions,
    RSocketTcpServerAddress,
    RSocketTcpServerListener
} from "@/server/types.js";
import {ReactiveAcceptedTcpConnection} from "@/tcp/connection.js";

/** Narrow sink operations used without publishing Reactor implementation types. */
interface DirectSink<T> {
    /** Exposes this sink as a read-only Flux. */
    asFlux(): Flux<T>;
    /** Emits one event without retaining it for absent subscribers. */
    tryEmitNext(value: T): unknown;
    /** Completes current subscribers. */
    tryEmitComplete(): unknown;
}

/** Running Node TCP listener and its accepted logical connection stream. */
class DefaultRSocketTcpServerListener<D, M> implements RSocketTcpServerListener<D, M> {
    readonly connections: Flux<RSocketServerConnection<D, M>>;
    readonly errors: Flux<unknown>;
    private closing: Promise<void> | undefined;
    private completeSignals: (() => void) | undefined;
    private releaseNativeListeners: (() => void) | undefined;
    private onClosed: ((listener: RSocketTcpServerListener<D, M>) => void) | undefined;

    /** Wraps one listening native server and direct non-buffering event sinks. */
    constructor(
        private readonly native: Server,
        connections: DirectSink<RSocketServerConnection<D, M>>,
        errors: DirectSink<unknown>,
        onClosed: (listener: RSocketTcpServerListener<D, M>) => void,
        releaseNativeListeners: () => void
    ) {
        this.connections = connections.asFlux();
        this.errors = errors.asFlux();
        this.onClosed = onClosed;
        this.releaseNativeListeners = releaseNativeListeners;
        this.completeSignals = () => {
            connections.tryEmitComplete();
            errors.tryEmitComplete();
        };
    }

    /** Local interface and port currently bound by the listener. */
    get address(): RSocketTcpServerAddress {
        const address = this.native.address();
        if (address === null || typeof address === "string") {
            throw new RSocketConnectionError("RSocket TCP server is not listening on an IP socket");
        }
        return {host: address.address, port: address.port};
    }

    /** Stops accepting new sockets and completes after Node releases the listener. */
    close(): Mono<void> {
        return Mono.defer(() => Mono.fromPromise(this.closePromise()));
    }

    /** Shares one idempotent native close operation across subscribers. */
    private closePromise(): Promise<void> {
        if (this.closing !== undefined) return this.closing;
        this.closing = Promise.resolve().then(() => new Promise<void>((resolve, reject) => {
            if (!this.native.listening) {
                this.finishClose();
                resolve();
                return;
            }
            try {
                this.native.close((error) => {
                    this.finishClose();
                    if (error === undefined) resolve();
                    else reject(error);
                });
            } catch (error) {
                this.finishClose();
                reject(error);
            }
        }));
        return this.closing;
    }

    /** Completes public signals and releases the owning server callback once. */
    private finishClose(): void {
        const releaseNativeListeners = this.releaseNativeListeners;
        const onClosed = this.onClosed;
        const completeSignals = this.completeSignals;
        this.releaseNativeListeners = undefined;
        this.onClosed = undefined;
        this.completeSignals = undefined;
        try {
            releaseNativeListeners?.();
        } catch {
            // Native listener cleanup must not retain the owning RSocket server.
        }
        try {
            onClosed?.(this);
        } catch {
            // Owner bookkeeping cannot prevent completion of public streams.
        }
        try {
            completeSignals?.();
        } catch {
            // A custom sink cannot retain native listener state after close.
        }
    }
}

/** Starts a native listener lazily and registers it with the owning server. */
export function createTcpServerListener<D, M>(
    rsocket: RSocketServer<D, M>,
    options: RSocketTcpListenOptions,
    maxFrameLength: number,
    onListening: (listener: RSocketTcpServerListener<D, M>) => boolean,
    onClosed: (listener: RSocketTcpServerListener<D, M>) => void
): Mono<RSocketTcpServerListener<D, M>> {
    return Mono.create<RSocketTcpServerListener<D, M>>((sink) => {
        const host = options.host ?? "127.0.0.1";
        validateListenOptions(host, options.port, options.backlog);
        const accepted = connectionSink<D, M>();
        const failures = errorSink();
        let settled = false;
        let listener: DefaultRSocketTcpServerListener<D, M>;
        const acceptSocket = (socket: Socket): void => {
            try {
                rsocket.accept(new ReactiveAcceptedTcpConnection(socket, maxFrameLength)).subscribe(
                    (connection) => accepted.tryEmitNext(connection),
                    (error) => failures.tryEmitNext(error)
                );
            } catch (error) {
                try {
                    socket.destroy();
                } catch {
                    // The initialization failure remains the emitted listener error.
                }
                failures.tryEmitNext(error instanceof RSocketConnectionError
                    ? error
                    : new RSocketConnectionError("Accepted TCP connection initialization failed", error));
            }
        };
        const native = createServer(acceptSocket);
        const runtimeError = (error: Error): void => {
            failures.tryEmitNext(error);
        };
        const releaseNativeListeners = (): void => {
            native.off("connection", acceptSocket);
            native.off("error", runtimeError);
        };
        listener = new DefaultRSocketTcpServerListener<D, M>(
            native,
            accepted,
            failures,
            onClosed,
            releaseNativeListeners
        );
        const startupError = (error: unknown): void => {
            if (settled) return;
            settled = true;
            native.off("listening", listening);
            releaseNativeListeners();
            sink.error(error instanceof Error
                ? error
                : new RSocketConnectionError("TCP listener startup failed", error));
        };
        const listening = (): void => {
            if (settled) return;
            settled = true;
            native.off("error", startupError);
            native.on("error", runtimeError);
            if (!onListening(listener)) {
                void listener.close().block().then(
                    () => sink.error(new RSocketConnectionError("RSocket server is closed")),
                    (error) => sink.error(error)
                );
                return;
            }
            sink.success(listener);
        };
        sink.onCancel(() => {
            if (settled) return;
            settled = true;
            native.off("error", startupError);
            native.off("listening", listening);
            releaseNativeListeners();
            try {
                native.close();
            } catch {
                // The native listener may not have entered its listening state yet.
            }
        });
        if (settled) return;
        try {
            native.once("error", startupError);
            native.once("listening", listening);
            native.listen({host, port: options.port, ...(options.backlog === undefined ? {} : {backlog: options.backlog})});
        } catch (error) {
            startupError(error);
        }
    });
}

/** Produces a direct sink so unobserved lifecycle events are never retained. */
function connectionSink<D, M>(): DirectSink<RSocketServerConnection<D, M>> {
    return Sinks.many().multicast().directBestEffort<RSocketServerConnection<D, M>>();
}

/** Produces a direct sink for listener and accepted-session failures. */
function errorSink(): DirectSink<unknown> {
    return Sinks.many().multicast().directBestEffort<unknown>();
}

/** Validates TCP bind options before allocating a native server. */
function validateListenOptions(host: string, port: number, backlog: number | undefined): void {
    if (typeof host !== "string" || host.trim().length === 0) {
        throw new RSocketConnectionError("TCP listen host must be a non-empty string");
    }
    if (!Number.isInteger(port) || port < 0 || port > 65_535) {
        throw new RSocketConnectionError("TCP listen port must be an integer between 0 and 65535");
    }
    if (backlog !== undefined && (!Number.isInteger(backlog) || backlog <= 0)) {
        throw new RSocketConnectionError("TCP listen backlog must be a positive integer");
    }
}
