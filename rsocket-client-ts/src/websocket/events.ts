/**
 * Reactor wrappers around browser WebSocket events.
 */
import {Mono} from "reactor-core-ts";
import {
    addWebSocketListener,
    observeAbort,
    RSocketConnectionError,
    unrefTimer,
    removeWebSocketListener
} from "rsocket-core-ts";
import type {RSocketWebSocket} from "@/websocket/types.js";
import {WS_CLOSED, WS_CLOSING, WS_OPEN} from "@/websocket/constants.js";

/**
 * Returns a `Mono` that completes when the WebSocket opens.
 */
export function openWebSocket(
    socket: RSocketWebSocket,
    timeoutMs: number | undefined,
    abortSignal?: AbortSignal
): Mono<void> {
    return Mono.create<void>((sink) => {
        if (abortSignal?.aborted) {
            sink.error(new RSocketConnectionError("WebSocket connection aborted"));
            return;
        }

        if (socket.readyState === WS_OPEN) {
            sink.success();
            return;
        }
        if (socket.readyState === WS_CLOSING || socket.readyState === WS_CLOSED) {
            sink.error(new RSocketConnectionError("WebSocket is already closed"));
            return;
        }

        let timeout: ReturnType<typeof setTimeout> | undefined;
        let releaseAbort: (() => void) | undefined;
        let settled = false;

        const cleanup = (): void => {
            if (timeout !== undefined) clearTimeout(timeout);
            timeout = undefined;
            removeWebSocketListener(socket, "open", onOpen);
            removeWebSocketListener(socket, "error", onError);
            removeWebSocketListener(socket, "close", onClose);
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

        const onOpen = (): void => {
            finish(() => sink.success());
        };

        const onError = (event: Event): void => {
            finish(() => sink.error(new RSocketConnectionError("WebSocket connection failed", event)));
        };

        const onClose = (): void => {
            finish(() => sink.error(new RSocketConnectionError("WebSocket closed before it opened")));
        };

        const onAbort = (): void => {
            finish(() => sink.error(new RSocketConnectionError("WebSocket connection aborted")));
        };

        const listen = (type: "open" | "error" | "close", listener: (event: any) => void): void => {
            if (settled) return;
            addWebSocketListener(socket, type, listener);
            if (settled) removeWebSocketListener(socket, type, listener);
        };

        try {
            listen("open", onOpen);
            listen("error", onError);
            listen("close", onClose);
            const release = observeAbort(abortSignal, onAbort);
            if (settled) release();
            else releaseAbort = release;
        } catch (error) {
            finish(() => sink.error(new RSocketConnectionError("WebSocket listener registration failed", error)));
            return;
        }
        sink.onCancel(() => finish());

        if (settled) return;
        if (socket.readyState === WS_OPEN) onOpen();
        else if (socket.readyState === WS_CLOSING || socket.readyState === WS_CLOSED) onClose();
        if (settled) return;

        if (timeoutMs !== undefined && timeoutMs > 0) {
            timeout = setTimeout(() => {
                finish(() => sink.error(new RSocketConnectionError(`WebSocket connection timed out after ${timeoutMs}ms`)));
            }, timeoutMs);
            unrefTimer(timeout);
        }
    });
}

