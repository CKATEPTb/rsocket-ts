// @ts-expect-error Binary primitives belong to bebyte and are not re-exported here.
import type {ByteReader as LeakedByteReader} from "@/index";
import * as api from "@/index";
import {Frame, FrameDeserializer} from "@/index";

/** Compile-time sentinel ensuring the expected-error import remains part of this file. */
type PublicApiMustNotExposeByteReader = LeakedByteReader;

describe("public package surface", () => {
    test("exports only protocol frames, flags, MIME codecs, and one transport codec", () => {
        expect(Object.keys(api).sort()).toEqual([
            "AuthType",
            "CancelFrame",
            "ErrorFrame",
            "ExtensionFlag",
            "ExtensionFrame",
            "FireAndForgetFlag",
            "Frame",
            "FrameCodec",
            "FrameDeserializer",
            "FrameErrorCode",
            "FrameFlag",
            "FrameType",
            "Header",
            "KeepaliveFlag",
            "KeepaliveFrame",
            "LeaseFrame",
            "MAX_FRAME_SIZE",
            "Metadata",
            "MetadataPushFrame",
            "MimeType",
            "Payload",
            "PayloadFlag",
            "PayloadFrame",
            "RequestChannelFlag",
            "RequestChannelFrame",
            "RequestFireAndForgetFrame",
            "RequestNFrame",
            "RequestResponseFlag",
            "RequestResponseFrame",
            "RequestStreamFlag",
            "RequestStreamFrame",
            "ResumeFrame",
            "ResumeOkFrame",
            "SetupFlag",
            "SetupFrame",
            "UnknownFrame",
            "WellKnownAuthType",
            "WellKnownMimeType"
        ]);
        expect(Frame.prototype).not.toHaveProperty("toTcpUint8Array");
        expect(FrameDeserializer).not.toHaveProperty("deserializeTcp");
    });

    test("does not proxy bebyte runtime exports", () => {
        expect(api).not.toHaveProperty("ByteReader");
        expect(api).not.toHaveProperty("ByteWriter");
    });
});

void (0 as unknown as PublicApiMustNotExposeByteReader);
