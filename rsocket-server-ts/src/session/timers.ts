/** Low-allocation lifetime and Resume expiry timers for one logical session. */
import {RSocketInactivityTimer, unrefTimer} from "rsocket-core-ts";

/** Owns mutually coordinated connection lifetime and suspended-session timers. */
export class ServerSessionTimers {
    private readonly lifetime = new RSocketInactivityTimer();
    private resumeTimer: ReturnType<typeof setTimeout> | undefined;

    /** Records SETUP, RESUME, or KEEPALIVE proof that the peer is alive. */
    received(): void {
        this.lifetime.touch();
    }

    /** Starts a low-churn inactivity check for the negotiated lifetime. */
    startLifetime(lifetimeMs: number, expire: () => void): void {
        this.lifetime.start(lifetimeMs, expire);
    }

    /** Stops active lifetime checks while no physical transport exists. */
    stopLifetime(): void {
        this.lifetime.stop();
    }

    /** Starts the retention deadline once for the current suspension window. */
    startResumeExpiry(ttlMs: number, expire: () => void): void {
        if (this.resumeTimer !== undefined) return;
        const timer = setTimeout(() => {
            this.resumeTimer = undefined;
            expire();
        }, ttlMs);
        this.resumeTimer = timer;
        unrefTimer(timer);
    }

    /** Cancels a suspended-session deadline after Resume succeeds. */
    resume(): void {
        this.clearResumeExpiry();
    }

    /** Releases every timer owned by the logical session. */
    close(): void {
        this.lifetime.stop();
        this.clearResumeExpiry();
    }

    /** Cancels the current Resume retention timeout. */
    private clearResumeExpiry(): void {
        if (this.resumeTimer !== undefined) clearTimeout(this.resumeTimer);
        this.resumeTimer = undefined;
    }
}
