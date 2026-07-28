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

/** Shared resolved promise for the common already-online reconnect path. */
const ONLINE_PROMISE = Promise.resolve();
/** Logical sockets multiplexed over one browser-global listener set. */
const wakeListeners = new Set<WakeSubscription>();
/** Native listener cleanups retained only while at least one socket is interested. */
let wakeCleanups: Array<() => void> | undefined;
/** Host methods used by the active native listener set. */
let wakeEnvironment: WakeEnvironment | undefined;

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
    const environment = currentWakeEnvironment();
    if (wakeEnvironment !== undefined && !sameWakeEnvironment(wakeEnvironment, environment)) {
        const staleCleanups = wakeCleanups;
        wakeCleanups = undefined;
        wakeEnvironment = undefined;
        wakeListeners.clear();
        if (staleCleanups !== undefined) releaseWakeListeners(staleCleanups);
    }
    const subscription = {listener};
    wakeListeners.add(subscription);
    if (wakeListeners.size === 1) {
        try {
            wakeCleanups = installWakeListeners(environment);
            wakeEnvironment = environment;
        } catch (error) {
            wakeListeners.delete(subscription);
            wakeCleanups = undefined;
            wakeEnvironment = undefined;
            throw error;
        }
    }

    let active = true;
    return () => {
        if (!active) return;
        active = false;
        wakeListeners.delete(subscription);
        if (wakeListeners.size !== 0) return;
        const cleanups = wakeCleanups;
        wakeCleanups = undefined;
        wakeEnvironment = undefined;
        if (cleanups !== undefined) releaseWakeListeners(cleanups);
    };
}

/** Installs the single native listener set shared by every browser RSocket. */
function installWakeListeners(environment: WakeEnvironment): Array<() => void> {
    const {globalTarget, documentTarget} = environment;
    if (globalTarget === undefined && documentTarget === undefined) return [];

    const cleanups: Array<() => void> = [];
    try {
        for (const type of ["online", "pageshow", "focus"] as const) {
            if (globalTarget !== undefined) cleanups.push(addListener(globalTarget, type, dispatchWake));
        }

        if (documentTarget !== undefined) {
            cleanups.push(addListener(documentTarget, "visibilitychange", () => {
                if (globalThis.document?.visibilityState !== "hidden") dispatchWake();
            }));
        }
    } catch (error) {
        releaseWakeListeners(cleanups);
        throw error;
    }

    return cleanups;
}

/** Captures enough host identity to detect browser realm or test-environment replacement. */
function currentWakeEnvironment(): WakeEnvironment {
    const globalTarget = eventTarget(globalThis);
    const documentTarget = eventTarget(globalThis.document);
    return {
        globalTarget,
        documentTarget,
        globalAdd: globalTarget?.addEventListener,
        globalRemove: globalTarget?.removeEventListener,
        documentAdd: documentTarget?.addEventListener,
        documentRemove: documentTarget?.removeEventListener
    };
}

/** Compares native listener operations without allocating an environment key. */
function sameWakeEnvironment(left: WakeEnvironment, right: WakeEnvironment): boolean {
    return left.globalTarget === right.globalTarget &&
        left.documentTarget === right.documentTarget &&
        left.globalAdd === right.globalAdd &&
        left.globalRemove === right.globalRemove &&
        left.documentAdd === right.documentAdd &&
        left.documentRemove === right.documentRemove;
}

/** Notifies the current logical listeners while isolating application callbacks. */
function dispatchWake(): void {
    const listeners = [...wakeListeners];
    for (const subscription of listeners) {
        try {
            subscription.listener();
        } catch {
            // One socket's lifecycle callback cannot starve the remaining sockets.
        }
    }
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

/** Browser listener surfaces captured when the shared wake hub is installed. */
interface WakeEnvironment {
    readonly globalTarget: EventTargetLike | undefined;
    readonly documentTarget: EventTargetLike | undefined;
    readonly globalAdd: EventTargetLike["addEventListener"] | undefined;
    readonly globalRemove: EventTargetLike["removeEventListener"] | undefined;
    readonly documentAdd: EventTargetLike["addEventListener"] | undefined;
    readonly documentRemove: EventTargetLike["removeEventListener"] | undefined;
}

/** One independently removable logical wake subscription. */
interface WakeSubscription {
    readonly listener: RSocketBrowserWakeListener;
}
