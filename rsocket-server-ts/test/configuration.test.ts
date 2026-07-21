import {describe, expect, it} from "vitest";
import {DEFAULT_MAX_FRAME_LENGTH, MAX_REQUEST_N} from "rsocket-core-ts";
import {RSocketServer} from "@/index.js";

describe("RSocketServer configuration", () => {
    it("supports an empty server with no options", () => {
        expect(() => new RSocketServer()).not.toThrow();
    });

    it.each([5, DEFAULT_MAX_FRAME_LENGTH + 1, 10.5, Number.NaN])(
        "rejects invalid maxFrameLength %s",
        (maxFrameLength) => {
            expect(() => new RSocketServer({maxFrameLength})).toThrow("maxFrameLength");
        }
    );

    it.each([0, MAX_REQUEST_N + 1, 1.5, Number.POSITIVE_INFINITY])(
        "rejects invalid Resume TTL %s",
        (ttlMs) => {
            expect(() => new RSocketServer({resume: {ttlMs}})).toThrow("resume.ttlMs");
        }
    );

    it.each([0, MAX_REQUEST_N + 1, 1.5, Number.NaN])(
        "rejects invalid handshake timeout %s",
        (handshakeTimeoutMs) => {
            expect(() => new RSocketServer({handshakeTimeoutMs})).toThrow("handshakeTimeoutMs");
        }
    );

    it.each([5, 1.5, Number.NaN])("rejects invalid Resume buffer size %s", (maxBufferBytes) => {
        expect(() => new RSocketServer({resume: {ttlMs: 1_000, maxBufferBytes}}))
            .toThrow("resume.maxBufferBytes");
    });

    it.each([
        {ttlMs: -1, requests: 1},
        {ttlMs: 1, requests: -1},
        {ttlMs: 1.5, requests: 1},
        {ttlMs: 1, requests: 1.5}
    ])("rejects an invalid lease %#", (lease) => {
        expect(() => new RSocketServer({lease})).toThrow("RSocket server lease");
    });
});
