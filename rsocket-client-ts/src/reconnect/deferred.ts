/**
 * Cancellation-aware deferred signal used by the requester facade.
 */
import {Mono} from "reactor-core-ts";
import {observeAbort} from "rsocket-core-ts";

/** One pending signal observer. */
interface DeferredObserver<T> {
    /** Receives the resolved value. */
    readonly resolve: (value: T) => void;
    /** Receives the rejection reason. */
    readonly reject: (error: unknown) => void;
}

/** State of a deferred signal. */
type DeferredState = "pending" | "resolved" | "rejected";

/**
 * A one-shot readiness signal whose cancelled waiters are removed immediately.
 */
export interface Deferred<T> {
    /** Whether the signal has already resolved or rejected. */
    readonly settled: boolean;

    /** Returns a cold Mono that unregisters its observer on cancellation. */
    mono(): Mono<T>;

    /** Waits for the signal and unregisters when `signal` is aborted. */
    wait(signal?: AbortSignal): Promise<T>;

    /** Resolves the signal exactly once. */
    resolve(value: T): void;

    /** Rejects the signal exactly once. */
    reject(error: unknown): void;
}

/**
 * Creates a cancellation-aware, one-shot deferred signal.
 */
export function deferred<T>(): Deferred<T> {
    let state: DeferredState = "pending";
    let result: T | undefined;
    let failure: unknown;
    const observers = new Set<DeferredObserver<T>>();

    const subscribe = (observer: DeferredObserver<T>): (() => void) => {
        if (state === "resolved") {
            observer.resolve(result as T);
            return NOOP;
        }
        if (state === "rejected") {
            observer.reject(failure);
            return NOOP;
        }
        observers.add(observer);
        return () => observers.delete(observer);
    };

    const settle = (nextState: Exclude<DeferredState, "pending">, value: T | unknown): void => {
        if (state !== "pending") return;
        state = nextState;
        if (nextState === "resolved") result = value as T;
        else failure = value;

        const pending = [...observers];
        observers.clear();
        for (const observer of pending) {
            try {
                if (nextState === "resolved") observer.resolve(result as T);
                else observer.reject(failure);
            } catch {
                // One consumer callback cannot retain or starve the remaining waiters.
            }
        }
    };

    return {
        /** Reports whether this signal is terminal. */
        get settled() {
            return state !== "pending";
        },
        /** Adapts this signal to a cancellation-aware Reactor Mono. */
        mono() {
            return Mono.create<T>((sink) => {
                const unsubscribe = subscribe({
                    resolve: (value) => sink.success(value),
                    reject: (error) => sink.error(error)
                });
                sink.onCancel(unsubscribe);
            });
        },
        /** Adapts this signal to an abortable Promise for deferred stream setup. */
        wait(signal) {
            if (signal?.aborted) return Promise.reject(abortReason(signal));
            return new Promise<T>((resolve, reject) => {
                let unsubscribe = NOOP;
                let releaseAbort = NOOP;
                let completed = false;
                const cleanup = (): void => {
                    unsubscribe();
                    releaseAbort();
                };
                const finish = (callback: () => void): void => {
                    if (completed) return;
                    completed = true;
                    cleanup();
                    callback();
                };
                unsubscribe = subscribe({
                    resolve: (value) => finish(() => resolve(value)),
                    reject: (error) => finish(() => reject(error))
                });
                if (completed) return;
                try {
                    const release = observeAbort(signal, () => {
                        finish(() => reject(abortReason(signal!)));
                    });
                    if (completed) release();
                    else releaseAbort = release;
                } catch (error) {
                    finish(() => reject(error));
                }
            });
        },
        /** Resolves this signal unless it is already terminal. */
        resolve(value) {
            settle("resolved", value);
        },
        /** Rejects this signal unless it is already terminal. */
        reject(error) {
            settle("rejected", error);
        }
    };
}

/** Shared no-op unsubscriber for already-settled signals. */
function NOOP(): void {
}

/** Returns a stable error for an aborted readiness wait. */
function abortReason(signal: AbortSignal): unknown {
    return signal.reason ?? new Error("RSocket readiness wait cancelled");
}
