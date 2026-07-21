import {
    AuthType,
    FrameDeserializer,
    FrameFlag,
    Metadata,
    MetadataPushFrame,
    MimeType,
    RequestResponseFrame,
    WellKnownAuthType,
    WellKnownMimeType
} from "@/index";
import type {ByteReader} from "bebyte";
import {runInNewContext} from "node:vm";
import {Payload} from "@/frame/context/Payload";

describe("RSocket MIME extensions", () => {
    test.each(["x", "x".repeat(128)])("encodes custom MIME length as length minus one", name => {
        const custom = new MimeType(name);
        const encoded = WellKnownMimeType.MESSAGE_RSOCKET_MIMETYPE
            .toMetadata(custom)
            .toUint8Array();
        const decoded = WellKnownMimeType.MESSAGE_RSOCKET_MIMETYPE
            .toMetadata(encoded, false);

        expect(encoded[0]).toBe(name.length - 1);
        expect(decoded.payload.mimeType).toBe(name);
    });

    test("encodes composite routing and bearer authentication metadata", () => {
        const route = WellKnownMimeType.MESSAGE_RSOCKET_ROUTING.toMetadata(["account.sign-in"]);
        const auth = WellKnownMimeType.MESSAGE_RSOCKET_AUTHENTICATION.toMetadata(
            WellKnownAuthType.BEARER.auth("access-token")
        );
        const composite = WellKnownMimeType.MESSAGE_RSOCKET_COMPOSITE_METADATA.toMetadata([route, auth]);
        const frame = new RequestResponseFrame(1, FrameFlag.NONE, composite);
        const decoded = FrameDeserializer.deserialize(
            frame.toUint8Array(),
            WellKnownMimeType.MESSAGE_RSOCKET_COMPOSITE_METADATA,
            WellKnownMimeType.APPLICATION_OCTET_STREAM
        ) as RequestResponseFrame;
        const entries = (decoded.metadata as Metadata<Metadata<any>[]>).payload;

        expect(entries).toHaveLength(2);
        expect(entries[0]!.mimeType).toBe(WellKnownMimeType.MESSAGE_RSOCKET_ROUTING);
        expect(entries[0]!.payload).toEqual(["account.sign-in"]);
        expect(entries[1]!.mimeType).toBe(WellKnownMimeType.MESSAGE_RSOCKET_AUTHENTICATION);
        expect(entries[1]!.payload).toEqual({
            authType: WellKnownAuthType.BEARER,
            data: "access-token"
        });
    });

    test("round-trips a custom composite MIME type using the Java-compatible 1..128 range", () => {
        const custom = new MimeType("application/example");
        const entry = new Metadata(custom, new Uint8Array([1, 2, 3]));
        const encoded = WellKnownMimeType.MESSAGE_RSOCKET_COMPOSITE_METADATA
            .toMetadata([entry])
            .toUint8Array();
        const decoded = WellKnownMimeType.MESSAGE_RSOCKET_COMPOSITE_METADATA
            .toMetadata(encoded, false);

        expect(encoded[0]).toBe(custom.mimeType.length - 1);
        expect(decoded.payload[0]!.mimeType.mimeType).toBe(custom.mimeType);
        expect(decoded.payload[0]!.payload).toEqual(new Uint8Array([1, 2, 3]));
    });

    test("preserves MIME wrappers for decoded text and JSON composite entries", () => {
        const text = WellKnownMimeType.TEXT_PLAIN.toMetadata("message");
        const json = WellKnownMimeType.APPLICATION_JSON.toMetadata({id: 7});
        const encoded = WellKnownMimeType.MESSAGE_RSOCKET_COMPOSITE_METADATA
            .toMetadata([text, json])
            .toUint8Array();
        const decoded = WellKnownMimeType.MESSAGE_RSOCKET_COMPOSITE_METADATA
            .toMetadata(encoded, false);

        expect(decoded.payload).toHaveLength(2);
        expect(decoded.payload[0]).toBeInstanceOf(Metadata);
        expect(decoded.payload[0]!.mimeType).toBe(WellKnownMimeType.TEXT_PLAIN);
        expect(decoded.payload[0]!.payload).toBe("message");
        expect(decoded.payload[1]).toBeInstanceOf(Metadata);
        expect(decoded.payload[1]!.mimeType).toBe(WellKnownMimeType.APPLICATION_JSON);
        expect(decoded.payload[1]!.payload).toEqual({id: 7});
    });

    test("rejects oversized routing, MIME, and simple-auth fields", () => {
        expect(() => WellKnownMimeType.MESSAGE_RSOCKET_ROUTING
            .toMetadata(["x".repeat(256)])
            .toUint8Array()).toThrow("Routing tag");
        expect(() => WellKnownMimeType.MESSAGE_RSOCKET_MIMETYPE
            .toMetadata(new MimeType("x".repeat(129)))
            .toUint8Array()).toThrow("between 1 and 128");
        expect(() => WellKnownMimeType.MESSAGE_RSOCKET_AUTHENTICATION
            .toMetadata(WellKnownAuthType.SIMPLE.auth({
                username: "x".repeat(65_536),
                password: "secret"
            }))
            .toUint8Array()).toThrow("Username");
        expect(() => WellKnownMimeType.MESSAGE_RSOCKET_MIMETYPE
            .toMetadata(new Uint8Array([0, 0xff]), false)).toThrow("non-ASCII");
        expect(() => new MimeType("application/x-\u0442\u0435\u0441\u0442")).toThrow("ASCII");
    });

    test("keeps registered lookups stable without retaining unknown wire values", () => {
        expect(MimeType.valueOf("application/json")).toBe(WellKnownMimeType.APPLICATION_JSON);
        expect(MimeType.valueOf(0x05)).toBe(WellKnownMimeType.APPLICATION_JSON);
        expect(AuthType.valueOf(1)).toBe(WellKnownAuthType.BEARER);

        const unknownMime = MimeType.valueOf(0x70);
        const unknownAuth = AuthType.valueOf(0x70);
        expect(MimeType.valueOf(0x70)).toBe(unknownMime);
        expect(AuthType.valueOf(0x70)).toBe(unknownAuth);
        expect(unknownMime.identifier).toBe(0x70);
        expect(unknownAuth.identifier).toBe(0x70);

        const custom = new MimeType("application/x-registered-codec");
        expect(MimeType.valueOf(custom.mimeType)).toBe(custom);
    });

    test("does not grow registries for untrusted unknown names", () => {
        const mimeRegistry = MimeType as unknown as {
            valuesByName: ReadonlyMap<string, MimeType<any>>
        };
        const authRegistry = AuthType as unknown as {
            valuesByName: ReadonlyMap<string, AuthType<any>>
        };
        const mimeSize = mimeRegistry.valuesByName.size;
        const authSize = authRegistry.valuesByName.size;

        for (let index = 0; index < 1_000; index++) {
            MimeType.valueOf(`application/x-untrusted-${index}`);
            AuthType.valueOf(`untrusted-${index}`);
        }

        expect(mimeRegistry.valuesByName.size).toBe(mimeSize);
        expect(authRegistry.valuesByName.size).toBe(authSize);
    });

    test("omits Zipkin IDs when only sampling flags are present", () => {
        const tracing = WellKnownMimeType.MESSAGE_RSOCKET_TRACING_ZIPKIN.toMetadata({
            flags: {
                idsSet: false,
                debug: false,
                sampled: false,
                notSampled: true,
                traceId128: false,
                hasParent: false
            }
        });
        const encoded = tracing.toUint8Array();
        const decoded = WellKnownMimeType.MESSAGE_RSOCKET_TRACING_ZIPKIN
            .toMetadata(encoded, false);

        expect(encoded).toEqual(new Uint8Array([0x10]));
        expect(decoded.payload.traceId).toBeUndefined();
        expect(decoded.payload.spanId).toBeUndefined();
        expect(decoded.payload.flags.notSampled).toBe(true);
    });

    test("requires complete Zipkin IDs when the IDs flag is set", () => {
        expect(() => WellKnownMimeType.MESSAGE_RSOCKET_TRACING_ZIPKIN.toMetadata({
            flags: {
                idsSet: true,
                debug: false,
                sampled: true,
                notSampled: false,
                traceId128: false,
                hasParent: false
            },
            traceId: 1n
        }).toUint8Array()).toThrow("spanId is required");
    });

    test("rejects values that the selected text codec cannot encode", () => {
        expect(() => WellKnownMimeType.APPLICATION_JSON.toPayload(undefined))
            .toThrow("JSON payload is not serializable");
        expect(() => WellKnownMimeType.TEXT_PLAIN.toPayload(42 as never))
            .toThrow("Text payload must be a string");
    });

    test("preserves typed wrappers for empty text data and metadata", () => {
        const payload = WellKnownMimeType.TEXT_PLAIN.toPayload(new Uint8Array(0));
        const metadata = WellKnownMimeType.TEXT_PLAIN.toMetadata(new Uint8Array(0), false);

        expect(payload).toBeInstanceOf(Payload);
        expect(payload.payload).toBe("");
        expect(metadata).toBeInstanceOf(Metadata);
        expect(metadata.payload).toBe("");
    });

    test("decodes byte views created in another JavaScript realm", () => {
        const bytes = runInNewContext("new Uint8Array([49, 50, 51])") as Uint8Array;

        expect(bytes).not.toBeInstanceOf(Uint8Array);
        expect(WellKnownMimeType.TEXT_PLAIN.toPayload(bytes).payload).toBe("123");
        expect(new Payload(WellKnownMimeType.APPLICATION_OCTET_STREAM, bytes).toUint8Array())
            .toEqual(Uint8Array.of(49, 50, 51));
        expect(new Metadata(WellKnownMimeType.APPLICATION_OCTET_STREAM, bytes).toUint8Array())
            .toEqual(Uint8Array.of(49, 50, 51));
    });

    test("encodes JSON null as data rather than treating it as no payload", () => {
        const encoded = WellKnownMimeType.APPLICATION_JSON.toPayload(null).toUint8Array();
        expect(new TextDecoder().decode(encoded)).toBe("null");
        const decoded = WellKnownMimeType.APPLICATION_JSON.toPayload(encoded);
        expect(decoded).toBeInstanceOf(Payload);
        expect(decoded.payload).toBeNull();
    });

    test("does not retain the source object after JSON serialization", () => {
        const source = {id: 1};
        const encoded = WellKnownMimeType.APPLICATION_JSON.toPayload(source);

        expect(encoded.payload).toBeInstanceOf(Uint8Array);
        expect(encoded.payload).not.toBe(source);
    });

    test("does not mistake reader-like application objects for byte readers", () => {
        const source = {
            i8: () => 1,
            readRemaining: () => new Uint8Array(0),
            value: "application data"
        };

        expect(() => WellKnownMimeType.APPLICATION_JSON.toPayload(source)).not.toThrow();
    });

    test("preserves JSON null in METADATA_PUSH frames", () => {
        const metadataType = WellKnownMimeType.APPLICATION_JSON;
        const frame = new MetadataPushFrame(metadataType.toMetadata(null));
        const decoded = FrameDeserializer.deserialize(
            frame.toUint8Array(),
            metadataType,
            WellKnownMimeType.APPLICATION_OCTET_STREAM
        );

        expect(decoded.metadata).toBeInstanceOf(Metadata);
        expect(decoded.metadata?.payload).toBeNull();
        expect(decoded.hasMetadata()).toBe(true);
    });

    test.each([
        ["null", null],
        ["empty", ""]
    ])("preserves %s JSON metadata presence on request frames", (_, value) => {
        const metadataType = WellKnownMimeType.APPLICATION_JSON;
        const frame = new RequestResponseFrame(
            1,
            FrameFlag.NONE,
            metadataType.toMetadata(value)
        );
        const decoded = FrameDeserializer.deserialize(
            frame.toUint8Array(),
            metadataType,
            WellKnownMimeType.APPLICATION_OCTET_STREAM
        );

        expect(decoded.metadata).toBeInstanceOf(Metadata);
        expect(decoded.metadata?.payload).toBe(value);
        expect(decoded.hasMetadata()).toBe(true);
    });

    test("decodes an empty JSON wire value as absent", () => {
        const decoded = WellKnownMimeType.APPLICATION_JSON.toPayload(new Uint8Array(0));
        const metadata = WellKnownMimeType.APPLICATION_JSON.toMetadata(new Uint8Array([0, 0, 0]));

        expect(decoded.payload).toBeUndefined();
        expect(metadata.payload).toBe("");
    });

    test("preserves incomplete JSON text until fragmented payloads are reassembled", () => {
        expect(WellKnownMimeType.APPLICATION_JSON.toPayload(
            new TextEncoder().encode('{"partial"')
        ).payload).toBe('{"partial"');
    });

    test("supports every bebyte reader width in custom MIME codecs", () => {
        class ReaderProbeMimeType extends MimeType<readonly bigint[]> {
            /** Reads the uncommon integer widths exposed by bebyte. */
            protected override deserializePayload(reader: ByteReader): Payload<readonly bigint[]> {
                const encoded = reader.toUint8Array();
                expect(reader.viewBytes(0)).toHaveLength(0);
                return new Payload(this, [reader.i40(), reader.i48(), reader.i56()], encoded);
            }
        }

        const decoded = new ReaderProbeMimeType("application/x-reader-probe").toPayload(new Uint8Array([
            1, 2, 3, 4, 5,
            1, 2, 3, 4, 5, 6,
            1, 2, 3, 4, 5, 6, 7
        ]));

        expect(decoded.payload).toEqual([
            0x0102030405n,
            0x010203040506n,
            0x01020304050607n
        ]);
    });
});
