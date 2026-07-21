/** Transport-neutral reconnect backoff calculation. */

/** Delay used after the immediate first reconnect attempt. */
const MIN_RECONNECT_DELAY_MS = 3_000;
/** Upper bound for repeated reconnect attempts. */
const MAX_RECONNECT_DELAY_MS = 10_000;
/** Exponential growth applied between repeated reconnect attempts. */
const RECONNECT_DELAY_GROWTH = 1.3;

/** Calculates the delay before a reconnect attempt. */
export function reconnectDelay(attempt: number): number {
    if (attempt <= 1) return 0;
    return Math.min(
        MAX_RECONNECT_DELAY_MS,
        Math.round(MIN_RECONNECT_DELAY_MS * RECONNECT_DELAY_GROWTH ** Math.max(0, attempt - 2))
    );
}
