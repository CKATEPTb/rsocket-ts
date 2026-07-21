import {describe, expect, it} from "vitest";
import {
    RSocketLogger,
    type RSocketLogEvent
} from "@/logging/index.js";

describe("client logging", () => {
    it("normalizes socket logging without environment-specific dependencies", () => {
        const events: RSocketLogEvent[] = [];
        const logging = new RSocketLogger({
            category: "api",
            logger: (event) => events.push(event)
        });

        logging.lifecycle({type: "connected", attempt: 0, reconnect: false});

        expect(logging.framesEnabled).toBe(true);
        expect(logging.lifecycleEnabled).toBe(true);
        expect(events[0]).toMatchObject({
            category: "api",
            type: "lifecycle"
        });
    });

    it("accepts structural lifecycle events from a browser or another client environment", () => {
        const events: RSocketLogEvent[] = [];
        const logging = new RSocketLogger({logger: (event) => events.push(event)});

        logging.lifecycle({
            type: "reconnecting",
            attempt: 2,
            reconnect: true,
            delayMs: 500
        });

        expect(events).toEqual([{
            type: "lifecycle",
            category: "RSocket",
            event: "reconnecting",
            attempt: 2,
            reconnect: true,
            delayMs: 500,
            error: undefined
        }]);
    });

    it("isolates protocol flow from logger failures", () => {
        const logging = new RSocketLogger({
            logger: () => {
                throw new Error("diagnostic failure");
            }
        });

        expect(() => logging.lifecycle({
            type: "connected",
            attempt: 0,
            reconnect: false
        })).not.toThrow();
    });
});
