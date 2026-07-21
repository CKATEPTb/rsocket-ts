/** Reactor wrappers around Node TCP socket events. */
import type {Socket} from "node:net";
import {Mono} from "reactor-core-ts";
import {isTcpSocketOpen, observeAbort, RSocketConnectionError, unrefTimer} from "rsocket-core-ts";

/** Completes when a connecting TCP socket becomes writable. */
export function openTcpSocket(
    socket: Socket,
    timeoutMs: number | undefined,
    abortSignal?: AbortSignal
): Mono<void> {
    return Mono.create<void>((sink) => {
        if (abortSignal?.aborted) {
            sink.error(new RSocketConnectionError("TCP connection aborted"));
            return;
        }
        if (isTcpSocketOpen(socket)) {
            sink.success();
            return;
        }
        if (socket.destroyed) {
            sink.error(new RSocketConnectionError("TCP socket is already closed"));
            return;
        }

        let timeout: ReturnType<typeof setTimeout> | undefined;
        let releaseAbort: (() => void) | undefined;
        let settled = false;
        const cleanup = (): void => {
            if (timeout !== undefined) clearTimeout(timeout);
            timeout = undefined;
            removeSocketListener(socket, "connect", onConnect);
            removeSocketListener(socket, "error", onError);
            removeSocketListener(socket, "close", onClose);
            const release = releaseAbort;
            releaseAbort = undefined;
            release?.();
        };
        const finish = (callback?: () => void): void => {
            if (settled) return;
            settled = true;
            cleanup();
            callback?.();
        };
        const onConnect = (): void => finish(() => sink.success());
        const onError = (error: Error): void => finish(() => sink.error(new RSocketConnectionError("TCP connection failed", error)));
        const onClose = (): void => finish(() => sink.error(new RSocketConnectionError("TCP connection closed before it opened")));
        const onAbort = (): void => finish(() => sink.error(new RSocketConnectionError("TCP connection aborted")));

        try {
            socket.once("connect", onConnect);
            if (!settled) socket.once("error", onError);
            if (!settled) socket.once("close", onClose);
            if (!settled) {
                const release = observeAbort(abortSignal, onAbort);
                if (settled) release();
                else releaseAbort = release;
            }
        } catch (error) {
            finish(() => sink.error(new RSocketConnectionError("TCP listener registration failed", error)));
            return;
        }
        sink.onCancel(() => finish());

        if (isTcpSocketOpen(socket)) onConnect();
        else if (socket.destroyed) onClose();
        if (!settled && timeoutMs !== undefined && timeoutMs > 0) {
            timeout = setTimeout(() => finish(() => sink.error(
                new RSocketConnectionError(`TCP connection timed out after ${timeoutMs}ms`)
            )), timeoutMs);
            unrefTimer(timeout);
        }
    });
}

/** Removes one custom socket listener without blocking sibling cleanup. */
function removeSocketListener(socket: Socket, event: string, listener: (...args: any[]) => void): void {
    try {
        socket.off(event, listener);
    } catch {
        // A custom socket cleanup failure cannot alter the terminal signal.
    }
}
