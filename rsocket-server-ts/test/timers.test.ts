import {afterEach, describe, expect, it, vi} from "vitest";
import {ServerSessionTimers} from "@/session/timers.js";

describe("ServerSessionTimers", () => {
    afterEach(() => vi.restoreAllMocks());

    it("does not retain the Node event loop while Resume state is suspended", () => {
        const unref = vi.fn();
        const timer = {unref} as unknown as ReturnType<typeof setTimeout>;
        const schedule = vi.spyOn(globalThis, "setTimeout").mockReturnValue(timer);
        const clear = vi.spyOn(globalThis, "clearTimeout").mockImplementation(() => undefined);
        const timers = new ServerSessionTimers();

        timers.startResumeExpiry(1_000, vi.fn());
        timers.startResumeExpiry(2_000, vi.fn());

        expect(schedule).toHaveBeenCalledOnce();
        expect(unref).toHaveBeenCalledOnce();
        timers.resume();
        expect(clear).toHaveBeenCalledWith(timer);
    });
});
