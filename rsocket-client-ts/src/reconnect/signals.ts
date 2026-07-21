/** Runtime availability signals used to accelerate reconnect attempts. */
export interface RSocketReconnectSignals {
    /** Reports whether opening a physical transport is currently meaningful. */
    isAvailable(): boolean;
    /** Waits for a signal that another connection attempt may succeed. */
    waitUntilAvailable(signal?: AbortSignal): Promise<void>;
    /** Observes runtime wake or availability transitions. */
    onWake(listener: () => void): () => void;
}

/** Default policy for runtimes without explicit connectivity signals. */
export const immediateReconnectSignals: RSocketReconnectSignals = Object.freeze({
    isAvailable: () => true,
    waitUntilAvailable: () => Promise.resolve(),
    onWake: () => () => undefined
});
