/** Low-churn endpoint inactivity tracking shared by client and server sessions. */

/**
 * Prevents a Node.js timeout from keeping the process alive while remaining a
 * no-op for browser timeout handles.
 */
export function unrefTimer(timer: ReturnType<typeof setTimeout>): void {
    (timer as ReturnType<typeof setTimeout> & {unref?: () => void}).unref?.();
}

/** One reschedulable timeout that expires after a full interval without activity. */
export class RSocketInactivityTimer {
    private lastActivityAt = Date.now();
    private timer: ReturnType<typeof setTimeout> | undefined;

    /** Records peer activity without allocating or rescheduling a timer. */
    touch(now = Date.now()): void {
        this.lastActivityAt = now;
    }

    /** Returns whether the peer remains inside the configured lifetime. */
    isAlive(lifetimeMs: number, now = Date.now()): boolean {
        return this.elapsed(now) < lifetimeMs;
    }

    /** Starts an accurate inactivity deadline and replaces any previous one. */
    start(lifetimeMs: number, expire: () => void): void {
        this.stop();
        const schedule = (delay: number): void => {
            this.timer = setTimeout(() => {
                this.timer = undefined;
                const remaining = lifetimeMs - this.elapsed(Date.now());
                if (remaining > 0) schedule(remaining);
                else expire();
            }, delay);
            unrefTimer(this.timer);
        };
        schedule(lifetimeMs);
    }

    /** Cancels the active inactivity deadline. */
    stop(): void {
        if (this.timer !== undefined) clearTimeout(this.timer);
        this.timer = undefined;
    }

    /** Treats a backwards wall-clock adjustment as fresh activity for one bounded interval. */
    private elapsed(now: number): number {
        if (now >= this.lastActivityAt) return now - this.lastActivityAt;
        this.lastActivityAt = now;
        return 0;
    }
}
