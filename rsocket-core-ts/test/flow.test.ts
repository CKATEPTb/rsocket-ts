/** Reactive Streams demand validation and saturation tests. */
import {describe, expect, it} from "vitest";
import {
    addReactiveDemand,
    normalizeFiniteReactiveDemand,
    normalizeReactiveDemand
} from "@";

describe("Core demand arithmetic", () => {
    it("accepts finite and unbounded legal demand", () => {
        expect(normalizeFiniteReactiveDemand(Number.MAX_SAFE_INTEGER)).toBe(Number.MAX_SAFE_INTEGER);
        expect(normalizeReactiveDemand(Number.POSITIVE_INFINITY)).toBe(Number.POSITIVE_INFINITY);
    });

    it.each([0, -1, 1.5, Number.NEGATIVE_INFINITY, Number.MAX_SAFE_INTEGER + 1])(
        "rejects invalid demand %s",
        (value) => expect(() => normalizeReactiveDemand(value)).toThrow("positive safe integer")
    );

    it("adds exact demand and saturates before precision is lost", () => {
        expect(addReactiveDemand(10, 20)).toBe(30);
        expect(addReactiveDemand(Number.MAX_SAFE_INTEGER - 1, 10)).toBe(Number.MAX_SAFE_INTEGER);
        expect(addReactiveDemand(1, Number.POSITIVE_INFINITY)).toBe(Number.POSITIVE_INFINITY);
    });
});
