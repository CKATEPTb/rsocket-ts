/** Routing metadata extraction and validation tests. */
import {Metadata, WellKnownMimeType} from "rsocket-frames-ts";
import {describe, expect, it} from "vitest";
import {
    compositeMetadata,
    normalizeRSocketRoute,
    route,
    routingTags
} from "@";

describe("Core routing helpers", () => {
    it("extracts direct and composite routing metadata", () => {
        expect(routingTags(route("account", "find"))).toEqual(["account", "find"]);
        expect(routingTags(compositeMetadata(
            WellKnownMimeType.TEXT_PLAIN.toMetadata("context"),
            route("secured")
        ))).toEqual(["secured"]);
        expect(routingTags(undefined)).toEqual([]);
    });

    it("ignores malformed and unrelated decoded metadata", () => {
        const malformed = new Metadata(
            WellKnownMimeType.MESSAGE_RSOCKET_ROUTING,
            "not-tags" as unknown as string[]
        );
        expect(routingTags(malformed)).toEqual([]);
        expect(routingTags(WellKnownMimeType.TEXT_PLAIN.toMetadata("value"))).toEqual([]);
    });

    it("normalizes immutable routes and enforces the one-byte tag length", () => {
        const input = ["account", "find"];
        const normalized = normalizeRSocketRoute(input);
        input[0] = "changed";

        expect(normalized).toEqual(["account", "find"]);
        expect(Object.isFrozen(normalized)).toBe(true);
        expect(() => normalizeRSocketRoute([], false)).toThrow("non-empty");
        expect(() => normalizeRSocketRoute([""])).toThrow("non-empty");
        expect(() => normalizeRSocketRoute("x".repeat(256))).toThrow("255 UTF-8 bytes");
    });
});
