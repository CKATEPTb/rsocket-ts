/** Failure-safe listener registration for Node-compatible TCP sockets. */
import type {RSocketTcpEventSocket} from "@/tcp/events.js";

/** Callback accepted by the minimal TCP event-emitter surface. */
export type TcpSocketListener = (...args: any[]) => void;

/**
 * Registers one listener and returns an idempotent cleanup callback.
 * A partially registered listener is removed when a custom emitter throws.
 */
export function addTcpSocketListener(
    socket: RSocketTcpEventSocket,
    event: string,
    listener: TcpSocketListener,
    once = false
): () => void {
    let active = true;
    try {
        if (once) socket.once(event, listener);
        else socket.on(event, listener);
    } catch (error) {
        active = false;
        removeTcpSocketListener(socket, event, listener);
        throw error;
    }
    return () => {
        if (!active) return;
        active = false;
        removeTcpSocketListener(socket, event, listener);
    };
}

/** Contains cleanup failures so one emitter cannot retain sibling listeners. */
function removeTcpSocketListener(
    socket: RSocketTcpEventSocket,
    event: string,
    listener: TcpSocketListener
): void {
    try {
        socket.off(event, listener);
    } catch {
        // Cleanup is best-effort for third-party Node-compatible emitters.
    }
}
