/** Resume handshake validation independent of a concrete transport adapter. */
import {describe, expect, it} from "vitest";
import {
    ErrorFrame,
    FrameErrorCode,
    ResumeOkFrame,
    WellKnownMimeType
} from "rsocket-frames-ts";
import {RSocketError} from "rsocket-core-ts";
import {
    decodeResumeOkFrame,
    type RSocketHandshakeOptions
} from "@/client/handshake.js";

/** Minimal negotiated options required to decode the first Resume response. */
const OPTIONS: RSocketHandshakeOptions = {
    connectTimeoutMs: undefined,
    maxFrameLength: 0xffffff,
    activityListener: undefined,
    activityEnabled: undefined,
    setup: {
        dataMimeType: WellKnownMimeType.APPLICATION_JSON,
        metadataMimeType: WellKnownMimeType.MESSAGE_RSOCKET_COMPOSITE_METADATA
    }
};

describe("Resume handshake", () => {
    it("rejects RESUME_OK on a non-zero stream", () => {
        const bytes = new ResumeOkFrame(0n).toUint8Array().slice();
        bytes[3] = 1;

        expect(() => decodeResumeOkFrame(bytes, OPTIONS)).toThrow("non-zero stream");
    });

    it("rejects connection ERROR codes that are invalid during Resume", () => {
        const bytes = new ErrorFrame(
            0,
            FrameErrorCode.REJECTED_SETUP,
            WellKnownMimeType.TEXT_PLAIN.toPayload("wrong handshake")
        ).toUint8Array();

        expect(() => decodeResumeOkFrame(bytes, OPTIONS)).toThrow("invalid handshake ERROR code");
    });

    it("preserves an explicit responder REJECTED_RESUME code", () => {
        const bytes = new ErrorFrame(
            0,
            FrameErrorCode.REJECTED_RESUME,
            WellKnownMimeType.TEXT_PLAIN.toPayload("session expired")
        ).toUint8Array();

        try {
            decodeResumeOkFrame(bytes, OPTIONS);
            throw new Error("Expected Resume rejection");
        } catch (error) {
            expect(error).toBeInstanceOf(RSocketError);
            expect((error as RSocketError).code).toBe(FrameErrorCode.REJECTED_RESUME);
        }
    });
});
