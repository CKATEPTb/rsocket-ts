import {
    MimeType,
    WellKnownAuthType,
    WellKnownMimeType
} from "@/index";
import {wire} from "@test/specification/fixtures";

describe("official RSocket metadata extension wire layouts", () => {
    test("matches every well-known MIME identifier assigned by the extension specification", () => {
        const mappings: ReadonlyArray<readonly [number, string]> = [
            [0x00, "application/avro"], [0x01, "application/cbor"], [0x02, "application/graphql"],
            [0x03, "application/gzip"], [0x04, "application/javascript"], [0x05, "application/json"],
            [0x06, "application/octet-stream"], [0x07, "application/pdf"],
            [0x08, "application/vnd.apache.thrift.binary"], [0x09, "application/vnd.google.protobuf"],
            [0x0a, "application/xml"], [0x0b, "application/zip"], [0x0c, "audio/aac"],
            [0x0d, "audio/mp3"], [0x0e, "audio/mp4"], [0x0f, "audio/mpeg3"],
            [0x10, "audio/mpeg"], [0x11, "audio/ogg"], [0x12, "audio/opus"],
            [0x13, "audio/vorbis"], [0x14, "image/bmp"], [0x15, "image/gif"],
            [0x16, "image/heic-sequence"], [0x17, "image/heic"], [0x18, "image/heif-sequence"],
            [0x19, "image/heif"], [0x1a, "image/jpeg"], [0x1b, "image/png"],
            [0x1c, "image/tiff"], [0x1d, "multipart/mixed"], [0x1e, "text/css"],
            [0x1f, "text/csv"], [0x20, "text/html"], [0x21, "text/plain"],
            [0x22, "text/xml"], [0x23, "video/H264"], [0x24, "video/H265"],
            [0x25, "video/VP8"], [0x26, "application/x-hessian"], [0x27, "application/x-java-object"],
            [0x28, "application/cloudevents+json"], [0x29, "application/x-capnp"],
            [0x2a, "application/x-flatbuffers"], [0x7a, "message/x.rsocket.mime-type.v0"],
            [0x7b, "message/x.rsocket.accept-mime-types.v0"],
            [0x7c, "message/x.rsocket.authentication.v0"],
            [0x7d, "message/x.rsocket.tracing-zipkin.v0"], [0x7e, "message/x.rsocket.routing.v0"],
            [0x7f, "message/x.rsocket.composite-metadata.v0"]
        ];

        for (const [identifier, name] of mappings) {
            expect(MimeType.valueOf(identifier).mimeType).toBe(name);
            expect(MimeType.valueOf(name).identifier).toBe(identifier);
        }
    });

    test("encodes routing, bearer, and simple authentication fields exactly", () => {
        expect(WellKnownMimeType.MESSAGE_RSOCKET_ROUTING.toMetadata(["a"]).toUint8Array())
            .toEqual(wire("01 61"));
        expect(WellKnownMimeType.MESSAGE_RSOCKET_AUTHENTICATION
            .toMetadata(WellKnownAuthType.BEARER.auth("t")).toUint8Array())
            .toEqual(wire("81 74"));
        expect(WellKnownMimeType.MESSAGE_RSOCKET_AUTHENTICATION.toMetadata(
            WellKnownAuthType.SIMPLE.auth({username: "u", password: "p"})
        ).toUint8Array()).toEqual(wire("80 0001 75 70"));
    });

    test("encodes composite entry type, length, and content independently", () => {
        const route = WellKnownMimeType.MESSAGE_RSOCKET_ROUTING.toMetadata(["a"]);
        const auth = WellKnownMimeType.MESSAGE_RSOCKET_AUTHENTICATION
            .toMetadata(WellKnownAuthType.BEARER.auth("t"));

        expect(WellKnownMimeType.MESSAGE_RSOCKET_COMPOSITE_METADATA
            .toMetadata([route, auth]).toUint8Array())
            .toEqual(wire("fe 000002 0161 fc 000002 8174"));
    });

    test("encodes well-known and custom MIME metadata with length-minus-one semantics", () => {
        expect(WellKnownMimeType.MESSAGE_RSOCKET_MIMETYPE
            .toMetadata(WellKnownMimeType.APPLICATION_JSON).toUint8Array())
            .toEqual(wire("85"));
        expect(WellKnownMimeType.MESSAGE_RSOCKET_ACCEPT_MIMETYPES.toMetadata([
            WellKnownMimeType.APPLICATION_JSON,
            WellKnownMimeType.TEXT_PLAIN
        ]).toUint8Array()).toEqual(wire("85 a1"));
        expect(WellKnownMimeType.MESSAGE_RSOCKET_MIMETYPE
            .toMetadata(new MimeType("abc")).toUint8Array())
            .toEqual(wire("02 616263"));
    });

    test("rejects trailing bytes in single MIME metadata", () => {
        expect(() => WellKnownMimeType.MESSAGE_RSOCKET_MIMETYPE
            .toMetadata(wire("85 00"), false))
            .toThrow("unexpected trailing byte");
    });
});
