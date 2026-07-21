import {
    CancelFrame,
    ErrorFrame,
    ExtensionFrame,
    Frame,
    FrameCodec,
    FrameDeserializer,
    FrameErrorCode,
    FrameFlag,
    FrameType,
    KeepaliveFlag,
    KeepaliveFrame,
    LeaseFrame,
    Metadata,
    MetadataPushFrame,
    Payload,
    PayloadFlag,
    PayloadFrame,
    RequestChannelFlag,
    RequestChannelFrame,
    RequestFireAndForgetFrame,
    RequestNFrame,
    RequestResponseFrame,
    RequestStreamFrame,
    ResumeFrame,
    ResumeOkFrame,
    SetupFlag,
    SetupFrame,
    WellKnownMimeType
} from "@/index";
import type {ByteWriter} from "bebyte";

const metadataType = WellKnownMimeType.APPLICATION_OCTET_STREAM;
const payloadType = WellKnownMimeType.APPLICATION_OCTET_STREAM;
const metadata = (bytes: number[]) => metadataType.toMetadata(
    new Uint8Array([0, 0, bytes.length, ...bytes])
);
const payload = (bytes: number[]) => payloadType.toPayload(new Uint8Array(bytes));

describe("standard RSocket frames", () => {
    const cases: Array<[string, Frame, new (...args: any[]) => Frame]> = [
        ["SETUP", new SetupFrame(30_000, 90_000, metadataType, payloadType, "resume", 1, 0, SetupFlag.LEASE, undefined, payload([1])), SetupFrame],
        ["LEASE", new LeaseFrame(1_000, 3, metadata([2])), LeaseFrame],
        ["KEEPALIVE", new KeepaliveFrame(KeepaliveFlag.RESPOND, 42n, payload([3])), KeepaliveFrame],
        ["REQUEST_RESPONSE", new RequestResponseFrame(1, FrameFlag.NONE, metadata([4]), payload([5])), RequestResponseFrame],
        ["REQUEST_FNF", new RequestFireAndForgetFrame(3, FrameFlag.NONE, metadata([6]), payload([7])), RequestFireAndForgetFrame],
        ["REQUEST_STREAM", new RequestStreamFrame(5, FrameFlag.NONE, 11, metadata([8]), payload([9])), RequestStreamFrame],
        ["REQUEST_CHANNEL", new RequestChannelFrame(7, RequestChannelFlag.COMPLETE, 13, metadata([10]), payload([11])), RequestChannelFrame],
        ["REQUEST_N", new RequestNFrame(9, 17), RequestNFrame],
        ["CANCEL", new CancelFrame(11), CancelFrame],
        ["PAYLOAD", new PayloadFrame(13, PayloadFlag.NEXT | PayloadFlag.COMPLETE, metadata([12]), payload([13])), PayloadFrame],
        ["ERROR", new ErrorFrame(15, FrameErrorCode.APPLICATION_ERROR, payload([14])), ErrorFrame],
        ["METADATA_PUSH", new MetadataPushFrame(metadata([15])), MetadataPushFrame],
        ["RESUME", new ResumeFrame("resume", 19n, 7n), ResumeFrame],
        ["RESUME_OK", new ResumeOkFrame(23n), ResumeOkFrame],
        ["EXT", new ExtensionFrame(17, FrameFlag.IGNORE, 29, metadata([16]), payload([17])), ExtensionFrame]
    ];

    test.each(cases)("round-trips %s over raw and TCP framing", (_, frame, FrameClass) => {
        const websocket = new FrameCodec({
            transport: "websocket",
            mimetype: {metadata: metadataType, data: payloadType}
        });
        const tcp = new FrameCodec({
            transport: "tcp",
            mimetype: {metadata: metadataType, data: payloadType}
        });
        const raw = websocket.serialize(frame);
        const [decodedRaw] = websocket.deserialize(raw);
        const [decodedTcp] = tcp.deserialize(tcp.serialize(frame));

        for (const decoded of [decodedRaw!, decodedTcp!]) {
            expect(decoded).toBeInstanceOf(FrameClass);
            expect(decoded.type).toBe(frame.type);
            expect(decoded.header.streamId).toBe(frame.header.streamId);
            expect(decoded.header.flags).toBe(frame.header.flags);
            expect(decoded.toUint8Array()).toEqual(raw);
        }
    });

    test("preserves application-defined error codes exactly", () => {
        const applicationCode = 0x0000abcd as FrameErrorCode;
        const decoded = FrameDeserializer.deserialize(
            new ErrorFrame(1, applicationCode, payload([1])).toUint8Array(),
            metadataType,
            payloadType
        ) as ErrorFrame;

        expect(decoded.code).toBe(applicationCode);
    });

    test("preserves arbitrary binary Resume tokens without UTF-8 coercion", () => {
        const expected = Uint8Array.of(0xff, 0x00, 0x80, 0x61);
        const mutable = expected.slice();
        const setup = new SetupFrame(1, 1, metadataType, payloadType, mutable);
        const resume = new ResumeFrame(mutable, 9n, 3n);
        mutable.fill(0);
        const setupBytes = setup.toUint8Array();
        const resumeBytes = resume.toUint8Array();

        const decodedSetup = FrameDeserializer.deserialize(setupBytes, metadataType, payloadType) as SetupFrame;
        const decodedResume = FrameDeserializer.deserialize(resumeBytes, metadataType, payloadType) as ResumeFrame;

        expect(decodedSetup.resumeToken).toBeInstanceOf(Uint8Array);
        expect(decodedSetup.resumeToken).toEqual(expected);
        expect(decodedResume.resumeToken).toBeInstanceOf(Uint8Array);
        expect(decodedResume.resumeToken).toEqual(expected);
        expect(decodedSetup.toUint8Array()).toEqual(setupBytes);
        expect(decodedResume.toUint8Array()).toEqual(resumeBytes);
    });

    test("round-trips an explicitly present zero-length metadata field", () => {
        const frame = new RequestResponseFrame(
            1,
            FrameFlag.METADATA,
            undefined,
            payload([1])
        );
        const decoded = FrameDeserializer.deserialize(
            frame.toUint8Array(),
            metadataType,
            payloadType
        );

        expect(decoded.hasMetadata()).toBe(true);
        expect(decoded.metadata).toBeInstanceOf(Metadata);
        expect((decoded.metadata as Metadata).toUint8Array()).toHaveLength(0);
        expect(decoded.payload).toBeInstanceOf(Payload);
    });

    test("rejects unknown frame types, truncated bodies, and trailing bytes", () => {
        expect(() => FrameDeserializer.deserialize(
            new Uint8Array([0, 0, 0, 1, 0x3c, 0]),
            metadataType,
            payloadType
        )).toThrow("Unknown RSocket frame type");

        expect(() => FrameDeserializer.deserialize(
            new Uint8Array([0, 0, 0, 1, FrameType.REQUEST_N << 2, 0]),
            metadataType,
            payloadType
        )).toThrow(RangeError);

        const cancel = new CancelFrame(1).toUint8Array();
        const withTrailingByte = new Uint8Array(cancel.length + 1);
        withTrailingByte.set(cancel);
        expect(() => FrameDeserializer.deserialize(withTrailingByte, metadataType, payloadType))
            .toThrow("unexpected trailing byte");

        const metadataPush = new MetadataPushFrame(metadata([1])).toUint8Array();
        metadataPush[3] = 5;
        const ignoredByProtocol = FrameDeserializer.deserialize(metadataPush, metadataType, payloadType);
        expect(ignoredByProtocol.header.streamId).toBe(5);
    });

    test("serializes multi-megabyte payloads without changing their bytes", () => {
        const bytes = new Uint8Array(2 * 1024 * 1024);
        for (let index = 0; index < bytes.length; index += 4096) bytes[index] = index / 4096 & 0xff;

        const serialized = new RequestResponseFrame(
            1,
            FrameFlag.NONE,
            undefined,
            payloadType.toPayload(bytes)
        ).toUint8Array();

        expect(serialized.length).toBe(6 + bytes.length);
        let matches = true;
        for (let index = 0; index < bytes.length; index++) {
            if (serialized[index + 6] !== bytes[index]) {
                matches = false;
                break;
            }
        }
        expect(matches).toBe(true);
    });

    test("rejects an oversized payload before writing it into a frame buffer", () => {
        class OversizedPayload extends Payload<Uint8Array> {
            public override toUint8Array(): Uint8Array {
                return {length: 0x1000000} as Uint8Array;
            }
        }

        const frame = new RequestResponseFrame(
            1,
            FrameFlag.NONE,
            undefined,
            new OversizedPayload(payloadType, new Uint8Array(0))
        );

        expect(() => frame.toUint8Array()).toThrow("RSocket frame size");
    });

    test("supports every bebyte integer width in custom frame writers", () => {
        class WriterProbeFrame extends Frame {
            public constructor() {
                super(FrameType.ERROR, 1);
            }

            /** Writes uncommon widths used by third-party frame extensions. */
            protected override write(writer: ByteWriter): void {
                writer.i1(1);
                writer.i40(0x0102030405n);
                writer.i56(0x01020304050607n);
            }
        }

        expect(new WriterProbeFrame().toUint8Array().subarray(6)).toEqual(new Uint8Array([
            0x01,
            0x01, 0x02, 0x03, 0x04, 0x05,
            0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07
        ]));
    });
});
