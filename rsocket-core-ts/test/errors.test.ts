/** Independent protocol and transport error mapping tests for Core. */
import {describe, expect, it} from "vitest";
import {
    ErrorFrame,
    FrameErrorCode,
    WellKnownMimeType
} from "rsocket-frames-ts";
import {
    connectionClosedError,
    deserializeFrame,
    errorFromFrame,
    RSocketConnectionError,
    RSocketError,
    RSocketFrameSizeError,
    RSocketProtocolError
} from "@";

describe("Core error hierarchy", () => {
    it("preserves names, causes, and protocol context", () => {
        const cause = new Error("native failure");
        const connection = new RSocketConnectionError("connection failed", cause);
        const protocol = new RSocketProtocolError("invalid frame", {
            code: FrameErrorCode.INVALID,
            streamId: 3,
            cause
        });
        const frameSize = new RSocketFrameSizeError(65, 64);

        expect(connection).toBeInstanceOf(RSocketError);
        expect(connection.name).toBe("RSocketConnectionError");
        expect(connection.cause).toBe(cause);
        expect(protocol.name).toBe("RSocketProtocolError");
        expect(protocol).toMatchObject({code: FrameErrorCode.INVALID, streamId: 3, cause});
        expect(frameSize).toBeInstanceOf(RSocketProtocolError);
        expect(frameSize.message).toContain("65 exceeds configured maxFrameLength 64");
    });

    it("maps an ERROR frame without losing its code, stream, frame, or message", () => {
        const encoded = new ErrorFrame(
            7,
            FrameErrorCode.APPLICATION_ERROR,
            WellKnownMimeType.TEXT_PLAIN.toPayload("not allowed")
        );
        const frame = deserializeFrame(
            encoded.toUint8Array(),
            WellKnownMimeType.APPLICATION_OCTET_STREAM,
            WellKnownMimeType.APPLICATION_JSON
        ) as ErrorFrame;
        const error = errorFromFrame(frame);

        expect(error).toBeInstanceOf(RSocketError);
        expect(error).toMatchObject({
            message: "not allowed",
            code: FrameErrorCode.APPLICATION_ERROR,
            streamId: 7,
            frame
        });
    });

    it("uses the protocol code name when an ERROR frame has no payload", () => {
        const frame = new ErrorFrame(0, FrameErrorCode.CONNECTION_ERROR);

        expect(errorFromFrame(frame).message).toBe("CONNECTION_ERROR");
    });

    it("normalizes arbitrary close reasons into connection errors", () => {
        const cause = new Error("socket closed");

        expect(connectionClosedError(cause)).toMatchObject({
            name: "RSocketConnectionError",
            message: "socket closed"
        });
        expect(connectionClosedError({code: 1006}).message).toBe('{"code":1006}');
        expect(connectionClosedError().message).toBe("RSocket connection closed");
    });
});
