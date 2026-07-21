/**
 * Browser lifecycle signals used to make reconnect more resilient on mobile.
 */
import {observeAbort} from "rsocket-core-ts";

/**
 * Listener invoked when the browser may have regained execution or network.
 */
export type RSocketBrowserWakeListener = () => void;

/**
 * Browser signal surface consumed by the high-level reconnect loop.
 */
export interface RSocketBrowserReconnectSignals {
    /** Returns `false` only when the browser explicitly reports offline state. */
    isAvailable(): boolean;

    /** Resolves immediately when online, otherwise waits for a browser wake signal. */
    waitUntilAvailable(signal?: AbortSignal): Promise<void>;

    /** Subscribes to browser wake/network events that should re-check the socket. */
    onWake(listener: RSocketBrowserWakeListener): () => void;
}

/**
 * Shared browser signal source.
 */
export const browserReconnectSignals: RSocketBrowserReconnectSignals = {
    isAvailable,
    waitUntilAvailable,
    onWake
};

/** Shared cleanup used when no browser wake targets exist. */
const NOOP = (): void => undefined;
/** Shared resolved promise for the common already-online reconnect path. */
const ONLINE_PROMISE = Promise.resolve();

/**
 * Checks the browser network hint without treating missing APIs as offline.
 */
function isAvailable(): boolean {
    const navigatorLike = globalThis.navigator as ({ readonly onLine?: boolean } | undefined);
    return navigatorLike?.onLine !== false;
}

/**
 * Waits until browser events suggest the network can be tried again.
 */
function waitUntilAvailable(signal?: AbortSignal): Promise<void> {
    if (signal?.aborted) return Promise.reject(reconnectAbortError());
    if (isAvailable()) return ONLINE_PROMISE;
    return new Promise<void>((resolve, reject) => {
        let settled = false;
        let unsubscribe: (() => void) | undefined;
        let releaseAbort: (() => void) | undefined;

        const cleanup = (): void => {
            try {
                unsubscribe?.();
            } catch {
                // A host cleanup failure must not retain the abort listener or block settlement.
            }
            const release = releaseAbort;
            releaseAbort = undefined;
            release?.();
        };
        const finish = (callback: () => void): void => {
            if (settled) return;
            settled = true;
            cleanup();
            callback();
        };
        const tryResolve = (): void => {
            if (isAvailable()) finish(resolve);
        };
        const onAbort = (): void => {
            finish(() => reject(reconnectAbortError()));
        };

        try {
            const releaseWake = onWake(tryResolve);
            if (settled) releaseWake();
            else unsubscribe = releaseWake;
            const release = observeAbort(signal, onAbort);
            if (settled) release();
            else releaseAbort = release;
        } catch (error) {
            finish(() => reject(error));
            return;
        }
        if (!settled) tryResolve();
    });
}

/**
 * Creates a standard abort error for reconnect waits.
 */
function reconnectAbortError(): DOMException {
    return new DOMException("Reconnect wait aborted", "AbortError");
}

/**
 * Registers wake listeners on the browser globals that exist in this runtime.
 */
function onWake(listener: RSocketBrowserWakeListener): () => void {
    const globalTarget = eventTarget(globalThis);
    const documentTarget = eventTarget(globalThis.document);
    if (globalTarget === undefined && documentTarget === undefined) return NOOP;

    const cleanups: Array<() => void> = [];
    try {
        for (const type of ["online", "pageshow", "focus"] as const) {
            if (globalTarget !== undefined) cleanups.push(addListener(globalTarget, type, listener));
        }

        if (documentTarget !== undefined) {
            cleanups.push(addListener(documentTarget, "visibilitychange", () => {
                if (globalThis.document?.visibilityState !== "hidden") listener();
            }));
        }
    } catch (error) {
        releaseWakeListeners(cleanups);
        throw error;
    }

    return () => releaseWakeListeners(cleanups);
}

/** Attempts every registered wake-listener cleanup exactly once. */
function releaseWakeListeners(cleanups: Array<() => void>): void {
    let cleanup: (() => void) | undefined;
    while ((cleanup = cleanups.pop()) !== undefined) {
        try {
            cleanup();
        } catch {
            // Every remaining browser listener still needs a cleanup attempt.
        }
    }
}

/**
 * Narrows browser-like globals to the event target operations we need.
 */
function eventTarget(value: unknown): EventTargetLike | undefined {
    if (
        value !== undefined
        && value !== null
        && typeof (value as EventTargetLike).addEventListener === "function"
        && typeof (value as EventTargetLike).removeEventListener === "function"
    ) {
        return value as EventTargetLike;
    }
    return undefined;
}

/**
 * Adds one listener and returns its cleanup callback.
 */
function addListener(
    target: EventTargetLike,
    type: string,
    listener: EventListener
): () => void {
    try {
        target.addEventListener(type, listener);
    } catch (error) {
        try {
            target.removeEventListener(type, listener);
        } catch {
            // The original registration error remains the useful failure.
        }
        throw error;
    }
    return () => target.removeEventListener(type, listener);
}

/**
 * Minimal event target shape used to avoid DOM assumptions in tests and SSR.
 */
interface EventTargetLike {
    addEventListener(type: string, listener: EventListener): void;

    removeEventListener(type: string, listener: EventListener): void;
}
