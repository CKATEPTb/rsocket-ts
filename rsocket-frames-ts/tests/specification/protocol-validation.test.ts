import {
    ErrorFrame,
    FrameDeserializer,
    FrameErrorCode,
    FrameFlag,
    FrameType,
    MetadataPushFrame,
    UnknownFrame
} from "@/index";
import {binaryMimeType, metadata, payload, wire} from "@test/specification/fixtures";

describe("protocol-level frame validation", () => {
    test("keeps strict assigned-type lookup separate from wire parsing", () => {
        expect(() => FrameType.fromByte(0x0f)).toThrow("Unknown RSocket frame type");
        expect(FrameType.fromWireByte(0x0f)).toBe(0x0f);
    });

    test("rejects the reserved high bit in every 31-bit stream ID", () => {
        expect(() => FrameDeserializer.deserialize(
            wire("80000001 2400"),
            binaryMimeType,
            binaryMimeType
        )).toThrow("Stream ID");
    });

    test("preserves an unassigned frame only when IGNORE is set", () => {
        const encoded = wire("00000000 3e00 aabb");
        const frame = FrameDeserializer.deserialize(encoded, binaryMimeType, binaryMimeType);

        expect(frame).toBeInstanceOf(UnknownFrame);
        expect(frame.type).toBe(0x0f);
        expect(frame.header.flags).toBe(FrameFlag.IGNORE);
        expect((frame as UnknownFrame).body).toEqual(wire("aabb"));
        expect(frame.canBeIgnored()).toBe(true);
        expect(frame.toUint8Array()).toEqual(encoded);

        expect(() => FrameDeserializer.deserialize(
            wire("00000000 3c00"),
            binaryMimeType,
            binaryMimeType
        )).toThrow("Unknown RSocket frame type: 0x0f");
    });

    test("treats RESERVED as opaque only when it is explicitly ignorable", () => {
        expect(FrameDeserializer.deserialize(
            wire("00000000 0200 01"),
            binaryMimeType,
            binaryMimeType
        )).toBeInstanceOf(UnknownFrame);

        expect(() => FrameDeserializer.deserialize(
            wire("00000000 0000"),
            binaryMimeType,
            binaryMimeType
        )).toThrow("Unknown RSocket frame type: 0x00");
    });

    test("requires METADATA_PUSH to carry metadata and set the M flag", () => {
        expect(() => new MetadataPushFrame(undefined as never)).toThrow("requires metadata");
        expect(() => FrameDeserializer.deserialize(
            wire("00000000 3000 aa"),
            binaryMimeType,
            binaryMimeType
        )).toThrow("must set the METADATA flag");
        expect(new MetadataPushFrame(metadata(0xaa)).type).toBe(FrameType.METADATA_PUSH);
    });

    test("enforces connection and stream scope for standard error codes", () => {
        expect(() => new ErrorFrame(1, FrameErrorCode.REJECTED_SETUP)).toThrow("stream ID 0");
        expect(() => new ErrorFrame(0, FrameErrorCode.APPLICATION_ERROR)).toThrow("non-zero stream ID");
        expect(() => new ErrorFrame(0, 0x00000301 as FrameErrorCode)).toThrow("non-zero stream ID");
        expect(() => new ErrorFrame(0, FrameErrorCode.CONNECTION_ERROR, payload(1))).not.toThrow();
        expect(() => new ErrorFrame(1, FrameErrorCode.INVALID, payload(1))).not.toThrow();
    });

    test("validates error-code width before serialization", () => {
        expect(() => new ErrorFrame(1, -1 as FrameErrorCode)).toThrow("unsigned 32-bit");
        expect(() => new ErrorFrame(1, 0x1_0000_0000 as FrameErrorCode)).toThrow("unsigned 32-bit");
    });
});
