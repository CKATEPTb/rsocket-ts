import {
    CancelFrame,
    ErrorFrame,
    ExtensionFlag,
    ExtensionFrame,
    Frame,
    FrameDeserializer,
    FrameErrorCode,
    FrameFlag,
    KeepaliveFlag,
    KeepaliveFrame,
    LeaseFrame,
    MetadataPushFrame,
    MimeType,
    PayloadFlag,
    PayloadFrame,
    RequestChannelFlag,
    RequestChannelFrame,
    RequestFireAndForgetFrame,
    RequestNFrame,
    RequestResponseFrame,
    RequestStreamFlag,
    RequestStreamFrame,
    ResumeFrame,
    ResumeOkFrame,
    SetupFlag,
    SetupFrame
} from "@/index";
import {binaryMimeType, metadata, payload, wire} from "@test/specification/fixtures";

describe("official RSocket frame wire layout", () => {
    const setupMetadataType = new MimeType("m");
    const setupDataType = new MimeType("d");
    const cases: ReadonlyArray<readonly [string, Frame, Uint8Array]> = [
        [
            "SETUP",
            new SetupFrame(
                500,
                1_000,
                setupMetadataType,
                setupDataType,
                "id",
                1,
                0,
                SetupFlag.LEASE,
                metadata(0xaa),
                payload(0xbb)
            ),
            wire("00000000 05c0 0001 0000 000001f4 000003e8 0002 6964 01 6d 01 64 000001 aa bb")
        ],
        ["LEASE", new LeaseFrame(1, 2, metadata(0xaa, 0xbb)), wire("00000000 0900 00000001 00000002 aabb")],
        [
            "KEEPALIVE",
            new KeepaliveFrame(KeepaliveFlag.RESPOND, 0x0001020304050607n, payload(0xaa)),
            wire("00000000 0c80 0001020304050607 aa")
        ],
        [
            "REQUEST_RESPONSE",
            new RequestResponseFrame(1, FrameFlag.NONE, metadata(0xaa, 0xbb), payload(0xcc)),
            wire("00000001 1100 000002 aabb cc")
        ],
        [
            "REQUEST_FNF",
            new RequestFireAndForgetFrame(3, RequestStreamFlag.FOLLOWS, undefined, payload(0xdd)),
            wire("00000003 1480 dd")
        ],
        [
            "REQUEST_STREAM",
            new RequestStreamFrame(5, RequestStreamFlag.FOLLOWS, 0x7fffffff, metadata(0xaa), payload(0xbb)),
            wire("00000005 1980 7fffffff 000001 aa bb")
        ],
        [
            "REQUEST_CHANNEL",
            new RequestChannelFrame(
                7,
                RequestChannelFlag.FOLLOWS | RequestChannelFlag.COMPLETE,
                2,
                metadata(0xaa),
                payload(0xbb)
            ),
            wire("00000007 1dc0 00000002 000001 aa bb")
        ],
        ["REQUEST_N", new RequestNFrame(9, 3), wire("00000009 2000 00000003")],
        ["CANCEL", new CancelFrame(11), wire("0000000b 2400")],
        [
            "PAYLOAD",
            new PayloadFrame(
                13,
                PayloadFlag.FOLLOWS | PayloadFlag.COMPLETE | PayloadFlag.NEXT,
                metadata(0xaa),
                payload(0xbb)
            ),
            wire("0000000d 29e0 000001 aa bb")
        ],
        [
            "ERROR",
            new ErrorFrame(15, FrameErrorCode.APPLICATION_ERROR, payload(0x65)),
            wire("0000000f 2c00 00000201 65")
        ],
        ["METADATA_PUSH", new MetadataPushFrame(metadata(0xaa, 0xbb)), wire("00000000 3100 aabb")],
        [
            "RESUME",
            new ResumeFrame("t", 1n, 2n),
            wire("00000000 3400 0001 0000 0001 74 0000000000000001 0000000000000002")
        ],
        ["RESUME_OK", new ResumeOkFrame(3n), wire("00000000 3800 0000000000000003")],
        [
            "EXT",
            new ExtensionFrame(
                17,
                ExtensionFlag.IGNORE | ExtensionFlag.EXT_1,
                4,
                metadata(0xaa),
                payload(0xbb)
            ),
            wire("00000011 ff80 00000004 000001 aa bb")
        ]
    ];

    test.each(cases)("encodes %s fields in big-endian protocol order", (_, frame, expected) => {
        expect(frame.toUint8Array()).toEqual(expected);
    });

    test.each(cases)("decodes an independent %s wire vector", (_, frame, encoded) => {
        const decoded = FrameDeserializer.deserialize(encoded, binaryMimeType, binaryMimeType);

        expect(decoded).toBeInstanceOf(frame.constructor);
        expect(decoded.type).toBe(frame.type);
        expect(decoded.header.streamId).toBe(frame.header.streamId);
        expect(decoded.header.flags).toBe(frame.header.flags);
        expect(decoded.toUint8Array()).toEqual(encoded);
    });

    test("uses a 31-bit stream ID field and a 6-bit type plus 10-bit flags field", () => {
        expect(new PayloadFrame(0x7fffffff, PayloadFlag.NEXT).toUint8Array().subarray(0, 6))
            .toEqual(wire("7fffffff 2820"));
    });
});
