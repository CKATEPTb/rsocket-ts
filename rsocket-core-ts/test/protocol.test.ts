/** Independent raw frame reader and deserialization tests for Core. */
import {runInNewContext} from "node:vm";
import {describe, expect, it} from "vitest";
import {
    ErrorFrame,
    FrameErrorCode,
    FrameFlag,
    FrameType,
    ExtensionFlag,
    ExtensionFrame,
    KeepaliveFlag,
    KeepaliveFrame,
    PayloadFlag,
    PayloadFrame,
    RequestResponseFrame,
    RequestStreamFrame,
    WellKnownMimeType
} from "rsocket-frames-ts";
import {
    deserializeFrame,
    hasIgnorableInvalidMetadataLength,
    isConnectionErrorCode,
    isConnectionFrame,
    isIgnorableEstablishedFrame,
    isIgnorableUnknownStreamFrame,
    isErrorCodeValidForStream,
    isHandshakeErrorCode,
    isInitialRequestFrame,
    isResumePositionFrame,
    isStreamErrorCode,
    nextRSocketStreamId,
    payloadHasMoreFragments,
    readFrameStreamId,
    readFrameTypeAndFlags,
    readKeepalivePosition,
    requiresRawPayloadDecode,
    RSocketProtocolError
} from "@";

const metadataMimeType = WellKnownMimeType.MESSAGE_RSOCKET_COMPOSITE_METADATA;
const dataMimeType = WellKnownMimeType.APPLICATION_JSON;

describe("Core raw frame protocol helpers", () => {
    it("reads stream ID, frame type, and flags without deserializing the frame", () => {
        const source = new RequestStreamFrame(
            0x7fffffff,
            FrameFlag.IGNORE,
            17,
            undefined,
            dataMimeType.toPayload({query: true})
        );
        const bytes = source.toUint8Array();
        const typeAndFlags = readFrameTypeAndFlags(bytes);

        expect(readFrameStreamId(bytes)).toBe(0x7fffffff);
        expect(typeAndFlags >>> 10).toBe(FrameType.REQUEST_STREAM);
        expect(typeAndFlags & 0x03ff).toBe(source.header.flags);
    });

    it("reads frame headers created in another JavaScript realm", () => {
        const local = new RequestResponseFrame(7, FrameFlag.NONE).toUint8Array();
        const foreign = runInNewContext("Uint8Array.from(bytes)", {bytes: Array.from(local)}) as Uint8Array;

        expect(readFrameStreamId(foreign)).toBe(7);
        expect(readFrameTypeAndFlags(foreign) >>> 10).toBe(FrameType.REQUEST_RESPONSE);
    });

    it("rejects incomplete or non-byte frame headers", () => {
        expect(() => readFrameStreamId(new Uint8Array(5))).toThrow(RSocketProtocolError);
        expect(() => readFrameTypeAndFlags([] as unknown as Uint8Array)).toThrow("header is incomplete");
        expect(() => deserializeFrame(
            new Uint8Array(5),
            metadataMimeType,
            dataMimeType
        )).toThrow("header is incomplete");
    });

    it("rejects the reserved high bit of a raw stream ID", () => {
        const bytes = new RequestResponseFrame(1, FrameFlag.NONE).toUint8Array().slice();
        bytes[0] = (bytes[0] as number) | 0x80;

        expect(() => readFrameStreamId(bytes)).toThrow("reserved bit must be zero");
    });

    it("uses protocol MIME types for ERROR and KEEPALIVE payloads", () => {
        const errorBytes = new ErrorFrame(
            1,
            FrameErrorCode.APPLICATION_ERROR,
            WellKnownMimeType.TEXT_PLAIN.toPayload("denied")
        ).toUint8Array();
        const keepaliveBytes = new KeepaliveFrame(
            KeepaliveFlag.NONE,
            123n,
            WellKnownMimeType.APPLICATION_OCTET_STREAM.toPayload(Uint8Array.of(1, 2, 3))
        ).toUint8Array();

        const error = deserializeFrame(errorBytes, metadataMimeType, dataMimeType) as ErrorFrame;
        const keepalive = deserializeFrame(keepaliveBytes, metadataMimeType, dataMimeType) as KeepaliveFrame;

        expect(error.payload?.payload).toBe("denied");
        expect(keepalive.payload?.payload).toEqual(Uint8Array.of(1, 2, 3));
        expect(readKeepalivePosition(keepaliveBytes)).toBe(123n);
    });

    it("applies explicit payload MIME overrides to fragmented bytes", () => {
        const bytes = new RequestStreamFrame(
            1,
            FrameFlag.NONE,
            1,
            undefined,
            WellKnownMimeType.APPLICATION_OCTET_STREAM.toPayload(Uint8Array.of(0x7b))
        ).toUint8Array();
        const rawMimeType = WellKnownMimeType.APPLICATION_OCTET_STREAM;
        const frame = deserializeFrame(
            bytes,
            metadataMimeType,
            dataMimeType,
            {dataMimeType: rawMimeType}
        ) as RequestStreamFrame;

        expect(frame.payload?.mimeType).toBe(rawMimeType);
        expect(frame.payload?.payload).toEqual(Uint8Array.of(0x7b));
    });

    it("rejects an incomplete KEEPALIVE position", () => {
        expect(() => readKeepalivePosition(new Uint8Array(13)))
            .toThrow("KEEPALIVE frame is incomplete");
    });

    it("detects only ignorable out-of-bounds metadata lengths", () => {
        const metadata = WellKnownMimeType.APPLICATION_OCTET_STREAM.toMetadata(Uint8Array.of(1), false);
        const frames = [
            new RequestResponseFrame(1, FrameFlag.IGNORE, metadata),
            new RequestStreamFrame(1, FrameFlag.IGNORE, 1, metadata),
            new ExtensionFrame(0, ExtensionFlag.IGNORE, 1, metadata)
        ];

        for (const frame of frames) {
            const bytes = frame.toUint8Array().slice();
            const metadataOffset = frame instanceof RequestResponseFrame ? 6 : 10;
            bytes[metadataOffset] = 0x7f;
            expect(hasIgnorableInvalidMetadataLength(bytes)).toBe(true);

            bytes[4] = (bytes[4] as number) & ~(FrameFlag.IGNORE >>> 8);
            expect(hasIgnorableInvalidMetadataLength(bytes)).toBe(false);
        }
    });

    it("recognizes non-terminal PAYLOAD fragment sequences", () => {
        const follows = new PayloadFrame(1, PayloadFlag.NEXT | PayloadFlag.FOLLOWS).header.flags;
        const final = new PayloadFrame(1, PayloadFlag.NEXT).header.flags;
        const completeWins = new PayloadFrame(
            1,
            PayloadFlag.NEXT | PayloadFlag.FOLLOWS | PayloadFlag.COMPLETE
        ).header.flags;

        expect(payloadHasMoreFragments(FrameType.PAYLOAD << 10 | follows)).toBe(true);
        expect(payloadHasMoreFragments(FrameType.PAYLOAD << 10 | final)).toBe(false);
        expect(payloadHasMoreFragments(FrameType.PAYLOAD << 10 | completeWins)).toBe(false);
    });

    it("classifies shared frame scopes and Resume positions", () => {
        expect(isInitialRequestFrame(FrameType.REQUEST_RESPONSE)).toBe(true);
        expect(isInitialRequestFrame(FrameType.REQUEST_CHANNEL)).toBe(true);
        expect(isInitialRequestFrame(FrameType.REQUEST_N)).toBe(false);
        expect(isResumePositionFrame(FrameType.REQUEST_N)).toBe(true);
        expect(isResumePositionFrame(FrameType.KEEPALIVE)).toBe(false);
        expect(isConnectionFrame(FrameType.METADATA_PUSH)).toBe(true);
        expect(isConnectionFrame(FrameType.PAYLOAD)).toBe(false);
        expect(isIgnorableEstablishedFrame(FrameType.SETUP, 0)).toBe(true);
        expect(isIgnorableEstablishedFrame(FrameType.METADATA_PUSH, 1)).toBe(true);
        expect(isIgnorableEstablishedFrame(FrameType.METADATA_PUSH, 0)).toBe(false);
        expect(isIgnorableEstablishedFrame(FrameType.REQUEST_RESPONSE, 1, true)).toBe(true);
        expect(isIgnorableEstablishedFrame(FrameType.REQUEST_RESPONSE, 1, false)).toBe(false);
        expect(isIgnorableUnknownStreamFrame(FrameType.REQUEST_N, 0)).toBe(true);
        expect(isIgnorableUnknownStreamFrame(FrameType.CANCEL, 0)).toBe(true);
        expect(isIgnorableUnknownStreamFrame(FrameType.PAYLOAD, 0)).toBe(true);
        expect(isIgnorableUnknownStreamFrame(FrameType.ERROR, 1)).toBe(true);
        expect(isIgnorableUnknownStreamFrame(FrameType.ERROR, 0)).toBe(false);
    });

    it("advances odd and even requester stream IDs with parity-safe wraparound", () => {
        expect(nextRSocketStreamId(1, 1)).toBe(3);
        expect(nextRSocketStreamId(0x7fffffff, 1)).toBe(1);
        expect(nextRSocketStreamId(2, 2)).toBe(4);
        expect(nextRSocketStreamId(0x7ffffffe, 2)).toBe(2);
    });

    it("keeps application frame bodies byte-oriented until endpoint dispatch", () => {
        expect(requiresRawPayloadDecode(FrameType.LEASE)).toBe(true);
        expect(requiresRawPayloadDecode(FrameType.EXT)).toBe(true);
        expect(requiresRawPayloadDecode(FrameType.REQUEST_RESPONSE)).toBe(true);
        expect(requiresRawPayloadDecode(FrameType.REQUEST_CHANNEL)).toBe(true);
        expect(requiresRawPayloadDecode(FrameType.PAYLOAD)).toBe(true);
        expect(requiresRawPayloadDecode(FrameType.METADATA_PUSH)).toBe(true);
        expect(requiresRawPayloadDecode(FrameType.KEEPALIVE)).toBe(false);
        expect(requiresRawPayloadDecode(FrameType.ERROR)).toBe(false);
    });

    it("enforces standard ERROR code stream scopes", () => {
        expect(isConnectionErrorCode(FrameErrorCode.CONNECTION_ERROR)).toBe(true);
        expect(isStreamErrorCode(FrameErrorCode.APPLICATION_ERROR)).toBe(true);
        expect(isHandshakeErrorCode(FrameErrorCode.REJECTED_RESUME)).toBe(true);
        expect(isErrorCodeValidForStream(FrameErrorCode.CONNECTION_ERROR, 0)).toBe(true);
        expect(isErrorCodeValidForStream(FrameErrorCode.CONNECTION_ERROR, 1)).toBe(false);
        expect(isErrorCodeValidForStream(FrameErrorCode.APPLICATION_ERROR, 1)).toBe(true);
        expect(isErrorCodeValidForStream(FrameErrorCode.APPLICATION_ERROR, 0)).toBe(false);
        const applicationCode = FrameErrorCode.fromByte(0x00000301);
        expect(isStreamErrorCode(applicationCode)).toBe(true);
        expect(isErrorCodeValidForStream(applicationCode, 1)).toBe(true);
        expect(isErrorCodeValidForStream(applicationCode, 0)).toBe(false);
    });
});
