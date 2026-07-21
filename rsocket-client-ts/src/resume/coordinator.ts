/** Transport-neutral state machine coordinating Resume and fresh SETUP fallback. */
import {FrameErrorCode} from "rsocket-frames-ts";
import {RSocketConnectionError, unrefTimer} from "rsocket-core-ts";
import {
    normalizeResumeOptions,
    type RSocketResumeOptionInput,
    type RSocketResumeOptions
} from "@/resume/options.js";
import {createResumeToken} from "@/resume/token.js";

/** Logical session operations needed by the Resume coordinator. */
export interface RSocketResumableSession {
    /** Whether the logical session survived physical transport loss. */
    readonly isSuspended: boolean;
    /** Permanently fails streams retained for a Resume attempt. */
    abandonResume(error: unknown): void;
}

/** Information reported before falling back from RESUME to a fresh SETUP. */
export interface RSocketResumeFallback {
    /** Error that made the Resume attempt unusable. */
    readonly error: unknown;
    /** Whether the responder explicitly returned `REJECTED_RESUME`. */
    readonly rejected: boolean;
}

/** Environment and transport operations used for one connection attempt. */
export interface RSocketResumeOpenOperations<Client extends RSocketResumableSession> {
    /** Opens a fresh logical session with the supplied token. */
    connect(token: string | undefined): Promise<Client>;
    /** Reattaches the retained logical session with the supplied token. */
    resume(client: Client, token: string): Promise<Client>;
    /** Reports whether the surrounding connection attempt was cancelled. */
    isAborted?(): boolean;
    /** Observes a non-retryable Resume failure before fresh SETUP starts. */
    onFallback?(fallback: RSocketResumeFallback): void;
}

/**
 * Owns the retained logical session, Resume deadline, token rotation, and
 * RESUME-to-SETUP fallback independently of WebSocket, TCP, DOM, or Node APIs.
 */
export class RSocketResumeCoordinator<Client extends RSocketResumableSession> {
    private readonly options: Omit<RSocketResumeOptions, "token">;
    private currentToken: string | undefined;
    private retainedClient: Client | undefined;
    private deadlineAt = 0;
    private expiryTimer: ReturnType<typeof setTimeout> | undefined;

    /** Creates a coordinator from public reconnect options. */
    constructor(input: RSocketResumeOptionInput = {}) {
        const options = normalizeResumeOptions(input);
        this.options = options;
        this.currentToken = options.token;
    }

    /** Token to place in the next SETUP or RESUME handshake. */
    get token(): string | undefined {
        return this.currentToken;
    }

    /** Whether a logical session is currently retained for Resume. */
    get hasRetainedSession(): boolean {
        return this.retainedClient !== undefined;
    }

    /** Retains a suspended session and starts its backend TTL window. */
    retain(client: Client, now = Date.now()): void {
        this.clearExpiryTimer();
        if (client.isSuspended && this.options.enabled && this.currentToken !== undefined) {
            const previous = this.retainedClient;
            if (previous !== undefined && previous !== client) {
                try {
                    previous.abandonResume(new RSocketConnectionError("RSocket Resume session was replaced"));
                } catch {
                    // Replacing retained state must still release the coordinator reference.
                }
            }
            this.retainedClient = client;
            this.deadlineAt = now + this.options.ttlMs;
            this.scheduleExpiry(client, this.options.ttlMs);
            return;
        }
        const previous = this.retainedClient;
        this.retainedClient = undefined;
        this.deadlineAt = 0;
        if (previous !== undefined && previous !== client) {
            this.abandonClient(previous, new RSocketConnectionError("RSocket Resume session was replaced"));
        }
        this.rotateToken();
    }

    /** Whether a new transport can still attempt protocol Resume. */
    canResume(now = Date.now()): boolean {
        return this.options.enabled &&
            this.currentToken !== undefined &&
            this.retainedClient?.isSuspended === true &&
            this.deadlineAt > now;
    }

    /** Caps a reconnect delay so the next attempt starts before Resume expires. */
    limitDelay(delayMs: number, now = Date.now()): number {
        return this.canResume(now)
            ? Math.min(delayMs, Math.max(0, this.deadlineAt - now - 1))
            : delayMs;
    }

    /** Fails and releases the retained logical session without rotating its token. */
    abandon(error: unknown): void {
        const client = this.takeRetained();
        if (client !== undefined) this.abandonClient(client, error);
    }

    /** Abandons retained state and prepares a token for a new logical session. */
    reset(error: unknown): void {
        this.abandon(error);
        this.rotateToken();
    }

    /** Releases retained state during explicit disconnect and rotates the token. */
    dispose(close: (client: Client) => void): void {
        const client = this.takeRetained();
        try {
            if (client !== undefined) close(client);
        } finally {
            this.rotateToken();
        }
    }

    /** Performs RESUME when possible and otherwise opens a fresh logical session. */
    async open(
        reconnect: boolean,
        previousError: unknown,
        operations: RSocketResumeOpenOperations<Client>
    ): Promise<Client> {
        if (!reconnect || !this.canResume()) {
            const hadRetainedSession = this.retainedClient !== undefined;
            this.abandon(previousError);
            if (hadRetainedSession) this.rotateToken();
            if (operations.isAborted?.() === true) throw abortedConnectionError(previousError);
            return this.connectFresh(operations);
        }

        const client = this.retainedClient as Client;
        const token = this.currentToken as string;
        if (operations.isAborted?.() === true) throw abortedConnectionError(previousError);
        try {
            const resumed = await operations.resume(client, token);
            if (operations.isAborted?.() === true) {
                const error = abortedConnectionError(previousError);
                if (this.retainedClient === client) this.takeRetained();
                this.abandonClient(resumed, error);
                throw error;
            }
            if (this.retainedClient === client) {
                this.takeRetained();
                return resumed;
            }
            this.abandonClient(
                resumed,
                new RSocketConnectionError("RSocket Resume completed after its retained session was released")
            );
            this.reset(new RSocketConnectionError(
                "RSocket retained session was superseded before fresh SETUP"
            ));
            return this.connectFresh(operations);
        } catch (error) {
            if (operations.isAborted?.() === true || isRetryableResumeError(error)) throw error;

            const fallback = {error, rejected: hasErrorCode(error, FrameErrorCode.REJECTED_RESUME)};
            this.reset(error);
            operations.onFallback?.(fallback);
            if (operations.isAborted?.() === true) throw error;
            return this.connectFresh(operations);
        }
    }

    /** Opens SETUP and rotates a rejected logical-session token before the next retry. */
    private async connectFresh(operations: RSocketResumeOpenOperations<Client>): Promise<Client> {
        try {
            return await operations.connect(this.currentToken);
        } catch (error) {
            if (isSetupRejection(error)) this.rotateToken();
            throw error;
        }
    }

    /** Removes retained state and clears its deadline. */
    private takeRetained(): Client | undefined {
        this.clearExpiryTimer();
        const client = this.retainedClient;
        this.retainedClient = undefined;
        this.deadlineAt = 0;
        return client;
    }

    /** Schedules terminal cleanup when no transport attempt arrives before backend TTL. */
    private scheduleExpiry(client: Client, delayMs: number): void {
        const timer = setTimeout(() => {
            if (this.expiryTimer !== timer || this.retainedClient !== client) return;
            this.expiryTimer = undefined;
            this.retainedClient = undefined;
            this.deadlineAt = 0;
            try {
                client.abandonResume(new RSocketConnectionError(
                    `RSocket Resume TTL expired after ${this.options.ttlMs}ms`
                ));
            } catch {
                // The coordinator reference and token still have to be released.
            }
            this.rotateToken();
        }, Math.max(1, delayMs));
        this.expiryTimer = timer;
        unrefTimer(timer);
    }

    /** Cancels the pending Resume expiry deadline without touching retained state. */
    private clearExpiryTimer(): void {
        if (this.expiryTimer !== undefined) clearTimeout(this.expiryTimer);
        this.expiryTimer = undefined;
    }

    /** Contains logical-session cleanup failures after coordinator ownership is released. */
    private abandonClient(client: Client, error: unknown): void {
        try {
            client.abandonResume(error);
        } catch {
            // Coordinator state and future SETUP attempts must not retain a failed session.
        }
    }

    /** Creates a distinct token for the next logical session when Resume is enabled. */
    private rotateToken(): void {
        if (this.options.enabled) this.currentToken = createResumeToken();
    }
}

/** Whether a failed RESUME should be retried on another physical connection. */
function isRetryableResumeError(error: unknown): boolean {
    return error instanceof RSocketConnectionError || hasErrorCode(error, FrameErrorCode.CONNECTION_ERROR);
}

/** Whether a server definitively rejected the token-bearing SETUP attempt. */
function isSetupRejection(error: unknown): boolean {
    return hasErrorCode(error, FrameErrorCode.INVALID_SETUP) ||
        hasErrorCode(error, FrameErrorCode.UNSUPPORTED_SETUP) ||
        hasErrorCode(error, FrameErrorCode.REJECTED_SETUP);
}

/** Reads a protocol error code without coupling to one concrete error class. */
function hasErrorCode(error: unknown, code: FrameErrorCode): boolean {
    return typeof error === "object" &&
        error !== null &&
        (error as {readonly code?: unknown}).code === code;
}

/** Preserves the triggering failure when cancellation wins before fresh SETUP. */
function abortedConnectionError(error: unknown): unknown {
    return error ?? new RSocketConnectionError("RSocket connection attempt aborted");
}
