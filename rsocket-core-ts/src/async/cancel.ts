/** Reactive Streams cleanup shared by requester and responder runtimes. */
import type {Subscription} from "reactor-core-ts";

/** Shared cleanup for absent or already-fired abort listeners. */
const NOOP = (): void => undefined;

/** Cancels an optional subscription without allowing cleanup code to escape. */
export function cancelSubscription(subscription: Subscription | undefined): void {
    try {
        subscription?.cancel();
    } catch {
        // Cancellation is terminal cleanup; the original signal remains authoritative.
    }
}

/**
 * Observes one abort signal exactly once and returns idempotent cleanup.
 *
 * The listener is invoked synchronously when the signal is already aborted.
 * Registration and removal failures from custom signal implementations are
 * contained so partial listener state cannot leak into endpoint lifecycles.
 */
export function observeAbort(signal: AbortSignal | undefined, listener: () => void): () => void {
    if (signal === undefined) return NOOP;
    let active = true;
    const cleanup = (): void => {
        if (!active) return;
        active = false;
        try {
            signal.removeEventListener("abort", onAbort);
        } catch {
            // Abort cleanup cannot alter the operation's terminal state.
        }
    };
    const onAbort = (): void => {
        if (!active) return;
        cleanup();
        listener();
    };

    if (signal.aborted) {
        active = false;
        listener();
        return NOOP;
    }
    try {
        signal.addEventListener("abort", onAbort, {once: true});
    } catch (error) {
        cleanup();
        throw error;
    }
    if (signal.aborted) onAbort();
    return cleanup;
}
