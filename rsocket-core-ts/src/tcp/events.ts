/** Shared Reactor adapters around connected Node TCP socket events. */
import {Flux} from "reactor-core-ts";
import {RSocketConnectionError} from "@/errors/index.js";
import type {RSocketTransportClose} from "@/transport/types.js";
import {TcpFrameDecoder} from "@/tcp/framing.js";
import {addTcpSocketListener} from "@/tcp/listener.js";
import {SerialSignalDispatcher} from "@/transport/signals.js";

/** Minimal event surface required from an already-connected stream socket. */
export interface RSocketTcpEventSocket {
    /** Whether the socket was permanently destroyed. */
    readonly destroyed: boolean;
    /** Whether the socket is still establishing its physical connection. */
    readonly connecting: boolean;
    /** Whether the writable side accepts bytes. */
    readonly writable: boolean;
    /** Runtime connection state exposed by Node-compatible stream sockets. */
    readonly readyState?: string;

    /** Registers a persistent native event listener. */
    on(event: string, listener: (...args: any[]) => void): unknown;
    /** Registers a single-use native event listener. */
    once(event: string, listener: (...args: any[]) => void): unknown;
    /** Removes a previously registered native event listener. */
    off(event: string, listener: (...args: any[]) => void): unknown;
    /** Tears down the stream socket. */
    destroy(): unknown;
}

/** Emits complete raw RSocket frames extracted from TCP data chunks. */
export function tcpFrameFlux(socket: RSocketTcpEventSocket, maxFrameLength: number): Flux<Uint8Array> {
    return Flux.create<Uint8Array>((sink) => {
        const decoder = new TcpFrameDecoder(maxFrameLength);
        let terminated = false;
        let endPending = false;
        let releaseData: (() => void) | undefined;
        let releaseEnd: (() => void) | undefined;
        const chunks = new SerialSignalDispatcher<Uint8Array>(
            (chunk) => decoder.push(chunk, (frame) => {
                if (!terminated && !sink.isCancelled()) sink.next(frame);
            }),
            (chunk) => new Uint8Array(chunk)
        );
        const cleanup = (): void => {
            const data = releaseData;
            const end = releaseEnd;
            releaseData = undefined;
            releaseEnd = undefined;
            data?.();
            end?.();
            chunks.clear();
        };
        const fail = (error: unknown): void => {
            if (terminated) return;
            terminated = true;
            cleanup();
            decoder.reset();
            if (!sink.isCancelled()) sink.error(error);
        };
        const onData = (chunk: Uint8Array): void => {
            if (terminated || endPending || sink.isCancelled()) return;
            try {
                chunks.dispatch(chunk);
                if (endPending && !terminated) finishEnd();
            } catch (error) {
                fail(error);
            }
        };
        const finishEnd = (): void => {
            if (terminated) return;
            endPending = false;
            try {
                decoder.finish();
            } catch (error) {
                fail(new RSocketConnectionError("TCP stream ended before a complete RSocket frame", error));
                return;
            }
            try {
                socket.destroy();
            } catch (error) {
                fail(new RSocketConnectionError("TCP socket shutdown failed", error));
                return;
            }
            if (terminated) return;
            terminated = true;
            cleanup();
            decoder.reset();
            if (!sink.isCancelled()) sink.complete();
        };
        const onEnd = (): void => {
            if (terminated || endPending) return;
            endPending = true;
            if (!chunks.isDispatching) finishEnd();
        };
        sink.onCancel(() => {
            terminated = true;
            cleanup();
            decoder.reset();
        });
        try {
            const data = addTcpSocketListener(socket, "data", onData);
            if (terminated || sink.isCancelled()) {
                data();
                return;
            }
            releaseData = data;
            const end = addTcpSocketListener(socket, "end", onEnd);
            if (terminated || sink.isCancelled()) {
                end();
                cleanup();
                return;
            }
            releaseEnd = end;
        } catch (error) {
            fail(new RSocketConnectionError("TCP frame listener registration failed", error));
        }
    });
}

/** Emits native TCP socket errors without retaining absent subscribers. */
export function tcpErrorFlux(socket: RSocketTcpEventSocket): Flux<unknown> {
    return Flux.create<unknown>((sink) => {
        const listener = (error: Error): void => {
            if (!sink.isCancelled()) sink.next(error);
        };
        let release: (() => void) | undefined;
        sink.onCancel(() => release?.());
        try {
            const cleanup = addTcpSocketListener(socket, "error", listener);
            if (sink.isCancelled()) cleanup();
            else release = cleanup;
        } catch (error) {
            sink.error(new RSocketConnectionError("TCP error listener registration failed", error));
        }
    });
}

/** Emits one normalized native TCP close notification. */
export function tcpCloseFlux(socket: RSocketTcpEventSocket): Flux<RSocketTransportClose> {
    return Flux.create<RSocketTransportClose>((sink) => {
        let terminated = false;
        let release: (() => void) | undefined;
        const cleanup = (): void => {
            const current = release;
            release = undefined;
            current?.();
        };
        const listener = (hadError: boolean): void => {
            if (terminated || sink.isCancelled()) return;
            terminated = true;
            const reason = hadError ? "TCP socket closed after an error" : "TCP socket closed";
            cleanup();
            sink.next({reason, ...(hadError ? {cause: new RSocketConnectionError(reason)} : {})});
            if (!sink.isCancelled()) sink.complete();
        };
        sink.onCancel(() => {
            terminated = true;
            cleanup();
        });
        try {
            const registered = addTcpSocketListener(socket, "close", listener, true);
            if (terminated || sink.isCancelled()) registered();
            else {
                release = registered;
                if (socket.destroyed) listener(false);
            }
        } catch (error) {
            cleanup();
            if (!terminated && !sink.isCancelled()) {
                terminated = true;
                sink.error(new RSocketConnectionError("TCP close listener registration failed", error));
            }
        }
    });
}

/** Whether a connected Node TCP socket currently permits writes. */
export function isTcpSocketOpen(socket: RSocketTcpEventSocket): boolean {
    return !socket.destroyed &&
        !socket.connecting &&
        socket.writable &&
        (socket.readyState === undefined || socket.readyState === "open");
}
