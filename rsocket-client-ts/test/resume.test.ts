import {describe, expect, it, vi} from "vitest";
import {FrameErrorCode} from "rsocket-frames-ts";
import {MAX_REQUEST_N, RSocketConnectionError} from "rsocket-core-ts";
import {RSocketResumeCoordinator} from "@/resume/index.js";
import {normalizeResumeOptions} from "@/resume/options.js";
import {createResumeToken} from "@/resume/token.js";

/** Minimal logical client used to verify transport-neutral Resume orchestration. */
class TestSession {
    readonly abandoned: unknown[] = [];

    /** Creates a suspended or terminal logical session. */
    constructor(readonly isSuspended = true) {}

    /** Records terminal Resume abandonment. */
    abandonResume(error: unknown): void {
        this.abandoned.push(error);
    }
}

describe("client Resume options", () => {
    it("keeps Resume disabled unless a policy is configured", () => {
        expect(normalizeResumeOptions({})).toEqual({
            enabled: false,
            token: undefined,
            ttlMs: 300_000
        });
        expect(normalizeResumeOptions({reconnect: true}).enabled).toBe(false);
    });

    it("creates an opaque token and preserves a valid backend TTL", () => {
        const options = normalizeResumeOptions({reconnect: {resume: {ttl: 45_000}}});

        expect(options.enabled).toBe(true);
        expect(options.token).toEqual(expect.any(String));
        expect(options.token?.length).toBeGreaterThan(0);
        expect(options.ttlMs).toBe(45_000);
    });

    it.each([0, 0.5, MAX_REQUEST_N + 1, Number.NaN, Number.POSITIVE_INFINITY])(
        "rejects invalid backend TTL %s before creating reconnect state",
        (ttl) => {
            expect(() => normalizeResumeOptions({reconnect: {resume: {ttl}}}))
                .toThrow("reconnect.resume.ttl");
        }
    );

    it("accepts the largest TTL representable by protocol and JavaScript timers", () => {
        expect(normalizeResumeOptions({reconnect: {resume: {ttl: MAX_REQUEST_N}}}).ttlMs)
            .toBe(MAX_REQUEST_N);
    });

    it("generates non-empty tokens independently of reconnect policy parsing", () => {
        expect(createResumeToken()).toEqual(expect.any(String));
    });

    it("refuses to generate predictable Resume tokens without Web Crypto", () => {
        vi.stubGlobal("crypto", undefined);
        try {
            expect(() => createResumeToken()).toThrow("cryptographically secure Web Crypto");
        } finally {
            vi.unstubAllGlobals();
        }
    });

});

describe("RSocketResumeCoordinator", () => {
    it("retains a suspended session only for the configured backend TTL", () => {
        const coordinator = resumeCoordinator(100);
        const client = new TestSession();

        coordinator.retain(client, 1_000);

        expect(coordinator.hasRetainedSession).toBe(true);
        expect(coordinator.canResume(1_099)).toBe(true);
        expect(coordinator.canResume(1_100)).toBe(false);
        expect(coordinator.limitDelay(500, 1_050)).toBe(49);
        coordinator.dispose(() => undefined);
    });

    it("schedules reconnect strictly before the retained session expires", () => {
        const coordinator = resumeCoordinator(100);
        coordinator.retain(new TestSession(), 1_000);

        expect(coordinator.limitDelay(100, 1_000)).toBe(99);
        expect(coordinator.limitDelay(100, 1_099)).toBe(0);
        coordinator.dispose(() => undefined);
    });

    it("releases an older retained session when a later session cannot be resumed", () => {
        const coordinator = resumeCoordinator();
        const retained = new TestSession();
        coordinator.retain(retained);

        coordinator.retain(new TestSession(false));

        expect(coordinator.hasRetainedSession).toBe(false);
        expect(retained.abandoned).toEqual([
            expect.objectContaining({message: "RSocket Resume session was replaced"})
        ]);
    });

    it("releases a suspended session when TTL expires without another connection attempt", () => {
        vi.useFakeTimers();
        try {
            const coordinator = resumeCoordinator(10);
            const client = new TestSession();
            const previousToken = coordinator.token;
            coordinator.retain(client);

            vi.advanceTimersByTime(10);

            expect(coordinator.hasRetainedSession).toBe(false);
            expect(client.abandoned).toEqual([
                expect.objectContaining({message: "RSocket Resume TTL expired after 10ms"})
            ]);
            expect(coordinator.token).not.toBe(previousToken);
            expect(vi.getTimerCount()).toBe(0);
        } finally {
            vi.useRealTimers();
        }
    });

    it("resumes the retained logical session instead of opening SETUP", async () => {
        const coordinator = resumeCoordinator();
        const client = new TestSession();
        coordinator.retain(client);
        const connect = vi.fn(async () => new TestSession());
        const resume = vi.fn(async () => client);

        const result = await coordinator.open(true, undefined, {connect, resume});

        expect(result).toBe(client);
        expect(resume).toHaveBeenCalledWith(client, coordinator.token);
        expect(connect).not.toHaveBeenCalled();
        expect(coordinator.hasRetainedSession).toBe(false);
    });

    it("keeps resumable state after a retryable transport failure", async () => {
        const coordinator = resumeCoordinator();
        const client = new TestSession();
        const error = new RSocketConnectionError("offline");
        coordinator.retain(client);

        await expect(coordinator.open(true, undefined, {
            connect: vi.fn(),
            resume: async () => Promise.reject(error)
        })).rejects.toBe(error);

        expect(coordinator.hasRetainedSession).toBe(true);
        expect(client.abandoned).toEqual([]);
        coordinator.dispose(() => undefined);
    });

    it("falls back to fresh SETUP and reports an explicit Resume rejection", async () => {
        const coordinator = resumeCoordinator();
        const retained = new TestSession();
        const fresh = new TestSession(false);
        const previousToken = coordinator.token;
        const rejection = {code: FrameErrorCode.REJECTED_RESUME};
        const onFallback = vi.fn();
        const connect = vi.fn(async () => fresh);
        coordinator.retain(retained);

        const result = await coordinator.open(true, undefined, {
            connect,
            resume: async () => Promise.reject(rejection),
            onFallback
        });

        expect(result).toBe(fresh);
        expect(retained.abandoned).toEqual([rejection]);
        expect(coordinator.token).not.toBe(previousToken);
        expect(connect).toHaveBeenCalledWith(coordinator.token);
        expect(onFallback).toHaveBeenCalledWith({error: rejection, rejected: true});
    });

    it("rotates a rejected SETUP token before the next fresh connection attempt", async () => {
        const coordinator = resumeCoordinator();
        const rejectedToken = coordinator.token;
        const rejection = {code: FrameErrorCode.REJECTED_SETUP};

        await expect(coordinator.open(false, undefined, {
            connect: async () => Promise.reject(rejection),
            resume: vi.fn()
        })).rejects.toBe(rejection);

        expect(coordinator.token).not.toBe(rejectedToken);
        const connect = vi.fn(async () => new TestSession(false));
        await coordinator.open(true, rejection, {connect, resume: vi.fn()});
        expect(connect).toHaveBeenCalledWith(coordinator.token);
    });

    it("opens a new logical session after the Resume window expires", async () => {
        const coordinator = resumeCoordinator(10);
        const retained = new TestSession();
        const fresh = new TestSession(false);
        const previousToken = coordinator.token;
        const previousError = new Error("connection lost");
        const connect = vi.fn(async () => fresh);
        coordinator.retain(retained, Date.now() - 11);

        const result = await coordinator.open(true, previousError, {
            connect,
            resume: vi.fn()
        });

        expect(result).toBe(fresh);
        expect(retained.abandoned).toEqual([previousError]);
        expect(coordinator.token).not.toBe(previousToken);
        expect(connect).toHaveBeenCalledWith(coordinator.token);
    });

    it("does not fall back when its surrounding connection attempt is aborted", async () => {
        const coordinator = resumeCoordinator();
        const retained = new TestSession();
        const error = {code: FrameErrorCode.REJECTED_RESUME};
        const connect = vi.fn();
        let aborted = false;
        coordinator.retain(retained);

        await expect(coordinator.open(true, undefined, {
            connect,
            resume: async () => {
                aborted = true;
                return Promise.reject(error);
            },
            isAborted: () => aborted
        })).rejects.toBe(error);

        expect(connect).not.toHaveBeenCalled();
        expect(coordinator.hasRetainedSession).toBe(true);
        expect(retained.abandoned).toEqual([]);
        coordinator.dispose(() => undefined);
    });

    it("does not start Resume after its surrounding connection attempt is aborted", async () => {
        const coordinator = resumeCoordinator();
        const retained = new TestSession();
        const previousError = new RSocketConnectionError("offline");
        const resume = vi.fn();
        coordinator.retain(retained);

        await expect(coordinator.open(true, previousError, {
            connect: vi.fn(),
            resume,
            isAborted: () => true
        })).rejects.toBe(previousError);

        expect(resume).not.toHaveBeenCalled();
        expect(coordinator.hasRetainedSession).toBe(true);
        coordinator.dispose(() => undefined);
    });

    it("does not open fallback SETUP when cancellation wins after successful Resume", async () => {
        const coordinator = resumeCoordinator();
        const retained = new TestSession();
        const previousError = new RSocketConnectionError("offline");
        const connect = vi.fn();
        let aborted = false;
        coordinator.retain(retained);

        await expect(coordinator.open(true, previousError, {
            connect,
            resume: async () => {
                aborted = true;
                return retained;
            },
            isAborted: () => aborted
        })).rejects.toBe(previousError);

        expect(connect).not.toHaveBeenCalled();
        expect(coordinator.hasRetainedSession).toBe(false);
        expect(retained.abandoned).toEqual([previousError]);
    });

    it("releases a stale successful Resume before opening a fresh session", async () => {
        const coordinator = resumeCoordinator();
        const retained = new TestSession();
        const replacement = new TestSession();
        const fresh = new TestSession(false);
        let resolveResume: ((client: TestSession) => void) | undefined;
        const resumed = new Promise<TestSession>((resolve) => {
            resolveResume = resolve;
        });
        const connect = vi.fn(async () => fresh);
        coordinator.retain(retained);

        const opening = coordinator.open(true, undefined, {
            connect,
            resume: async () => resumed
        });
        coordinator.retain(replacement);
        resolveResume?.(retained);

        await expect(opening).resolves.toBe(fresh);
        expect(retained.abandoned).toEqual([
            expect.objectContaining({message: "RSocket Resume session was replaced"}),
            expect.objectContaining({message: "RSocket Resume completed after its retained session was released"})
        ]);
        expect(replacement.abandoned).toEqual([
            expect.objectContaining({message: "RSocket retained session was superseded before fresh SETUP"})
        ]);
        expect(coordinator.hasRetainedSession).toBe(false);
    });

    it("does not open fallback SETUP when resumeRejected handling aborts the attempt", async () => {
        const coordinator = resumeCoordinator();
        const retained = new TestSession();
        const rejection = {code: FrameErrorCode.REJECTED_RESUME};
        const connect = vi.fn();
        let aborted = false;
        coordinator.retain(retained);

        await expect(coordinator.open(true, undefined, {
            connect,
            resume: async () => Promise.reject(rejection),
            isAborted: () => aborted,
            onFallback: () => {
                aborted = true;
            }
        })).rejects.toBe(rejection);

        expect(connect).not.toHaveBeenCalled();
        expect(retained.abandoned).toEqual([rejection]);
    });

    it("closes retained state and rotates the logical-session token on dispose", () => {
        const coordinator = resumeCoordinator();
        const retained = new TestSession();
        const previousToken = coordinator.token;
        const close = vi.fn();
        coordinator.retain(retained);

        coordinator.dispose(close);

        expect(close).toHaveBeenCalledWith(retained);
        expect(coordinator.hasRetainedSession).toBe(false);
        expect(coordinator.token).not.toBe(previousToken);
    });
});

/** Creates an enabled coordinator with a configurable backend Resume window. */
function resumeCoordinator(ttl = 60_000): RSocketResumeCoordinator<TestSession> {
    return new RSocketResumeCoordinator({reconnect: {resume: {ttl}}});
}
