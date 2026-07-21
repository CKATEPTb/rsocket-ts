/** Inactivity timer behavior under deterministic virtual time. */
import {afterEach, describe, expect, it, vi} from "vitest";
import {RSocketInactivityTimer, unrefTimer} from "@";

afterEach(() => vi.useRealTimers());

describe("Core inactivity timer", () => {
    it("unrefs Node-compatible timers without requiring Node timer types", () => {
        const unref = vi.fn();
        unrefTimer({unref} as unknown as ReturnType<typeof setTimeout>);
        expect(unref).toHaveBeenCalledOnce();

        expect(() => unrefTimer(1 as unknown as ReturnType<typeof setTimeout>)).not.toThrow();
    });

    it("tracks activity without reallocating the scheduled timeout", () => {
        vi.useFakeTimers();
        vi.setSystemTime(1_000);
        const timer = new RSocketInactivityTimer();
        const expire = vi.fn();
        timer.touch();
        timer.start(100, expire);

        vi.advanceTimersByTime(80);
        timer.touch();
        vi.advanceTimersByTime(99);
        expect(expire).not.toHaveBeenCalled();
        vi.advanceTimersByTime(1);
        expect(expire).toHaveBeenCalledOnce();
    });

    it("reports exact lifetime boundaries and cancels deadlines", () => {
        const timer = new RSocketInactivityTimer();
        timer.touch(10);
        expect(timer.isAlive(50, 59)).toBe(true);
        expect(timer.isAlive(50, 60)).toBe(false);

        vi.useFakeTimers();
        const expire = vi.fn();
        timer.touch(Date.now());
        timer.start(10, expire);
        timer.stop();
        vi.advanceTimersByTime(20);
        expect(expire).not.toHaveBeenCalled();
    });

    it("bounds lifetime extension when the wall clock moves backwards", () => {
        vi.useFakeTimers();
        vi.setSystemTime(1_000);
        const timer = new RSocketInactivityTimer();
        const expire = vi.fn();
        timer.touch();
        timer.start(100, expire);

        vi.setSystemTime(500);
        vi.advanceTimersByTime(100);
        expect(expire).not.toHaveBeenCalled();
        vi.advanceTimersByTime(99);
        expect(expire).not.toHaveBeenCalled();
        vi.advanceTimersByTime(1);
        expect(expire).toHaveBeenCalledOnce();
    });
});
