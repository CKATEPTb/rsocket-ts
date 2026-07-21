/** Independent payload, metadata, and decoding tests for Core. */
import {describe, expect, it} from "vitest";
import {
    FrameFlag,
    Metadata,
    MimeType,
    Payload,
    RequestResponseFrame,
    WellKnownMimeType
} from "rsocket-frames-ts";
import {
    compositeMetadata,
    compositeMetadataEntries,
    data,
    decodeFramePayload,
    deserializeFrame,
    encodeMetadataInput,
    encodePayloadInput,
    errorMessage,
    errorPayload,
    metadata,
    route
} from "@";

const dataMimeType = WellKnownMimeType.APPLICATION_JSON;
const compositeMimeType = WellKnownMimeType.MESSAGE_RSOCKET_COMPOSITE_METADATA;
const routingMimeType = WellKnownMimeType.MESSAGE_RSOCKET_ROUTING;

/** Codec whose public methods make dispatch through the supported API observable. */
class PublicNullCodec extends MimeType<null> {
    payloadCalls = 0;
    metadataCalls = 0;

    /** Encodes null data with a recognizable byte. */
    override toPayload(): Payload<null> {
        this.payloadCalls += 1;
        return new Payload(this, null, Uint8Array.of(0x11));
    }

    /** Encodes null metadata with a different recognizable byte. */
    override toMetadata(): Metadata<null> {
        this.metadataCalls += 1;
        return new Metadata(this, null, Uint8Array.of(0x22));
    }
}

describe("Core payload helpers", () => {
    it("encodes data, metadata, routes, and composite entries with their selected MIME types", () => {
        const body = data({id: 7}, dataMimeType);
        const trace = metadata("trace-1", WellKnownMimeType.TEXT_PLAIN);
        const routing = route("account.find", "fallback");
        const composite = compositeMetadata(routing, trace);

        expect(body).toBeInstanceOf(Payload);
        expect(body.mimeType).toBe(dataMimeType);
        expect(new TextDecoder().decode(body.toUint8Array())).toBe('{"id":7}');
        expect(trace).toBeInstanceOf(Metadata);
        expect(trace.mimeType).toBe(WellKnownMimeType.TEXT_PLAIN);
        expect(new TextDecoder().decode(trace.toUint8Array())).toBe("trace-1");
        expect(routing.mimeType).toBe(routingMimeType);
        expect(routing.payload).toEqual(["account.find", "fallback"]);
        expect(composite.mimeType).toBe(compositeMimeType);
        expect(composite.payload).toEqual([routing, trace]);
        expect(compositeMetadataEntries([routing, trace]).toUint8Array())
            .toEqual(composite.toUint8Array());
    });

    it("normalizes every supported payload input shape", () => {
        const encodedBody = dataMimeType.toPayload({encoded: true});
        const routing = route("account.find");

        expect(encodePayloadInput(undefined, dataMimeType, compositeMimeType)).toEqual({});
        expect(new TextDecoder().decode(
            encodePayloadInput({plain: true}, dataMimeType, compositeMimeType).payload?.toUint8Array()
        )).toBe('{"plain":true}');
        expect(new TextDecoder().decode(
            encodePayloadInput(encodedBody, dataMimeType, compositeMimeType).payload?.toUint8Array()
        )).toBe('{"encoded":true}');

        const metadataOnly = encodePayloadInput(routing, dataMimeType, compositeMimeType);
        expect(metadataOnly.payload).toBeUndefined();
        expect(metadataOnly.metadata?.mimeType).toBe(compositeMimeType);
        expect(metadataOnly.metadata?.payload).toEqual([expect.objectContaining({mimeType: routingMimeType})]);

        const envelope = encodePayloadInput({
            data: "hello",
            metadata: ["account.find"],
            dataMimeType: WellKnownMimeType.TEXT_PLAIN,
            metadataMimeType: routingMimeType
        }, dataMimeType, compositeMimeType);
        expect(envelope.payload?.mimeType).toBe(WellKnownMimeType.TEXT_PLAIN);
        expect(new TextDecoder().decode(envelope.payload?.toUint8Array())).toBe("hello");
        expect(envelope.metadata?.mimeType).toBe(routingMimeType);
        expect(envelope.metadata?.payload).toEqual(["account.find"]);
    });

    it("preserves JSON null as data in both raw and envelope inputs", () => {
        const raw = encodePayloadInput(null, dataMimeType, compositeMimeType);
        const envelope = encodePayloadInput({data: null}, dataMimeType, compositeMimeType);

        expect(new TextDecoder().decode(raw.payload?.toUint8Array())).toBe("null");
        expect(new TextDecoder().decode(envelope.payload?.toUint8Array())).toBe("null");
    });

    it("uses public MIME codec methods for null data and metadata", () => {
        const codec = new PublicNullCodec("application/x-public-null");
        const encoded = encodePayloadInput(
            {data: null, metadata: null},
            codec,
            codec
        );

        expect(encoded.payload?.toUint8Array()).toEqual(Uint8Array.of(0x11));
        expect(encoded.metadata?.toUint8Array()).toEqual(Uint8Array.of(0x22));
        expect(codec.payloadCalls).toBe(1);
        expect(codec.metadataCalls).toBe(1);
    });

    it("accepts matching direct metadata and rejects incompatible MIME entries", () => {
        const routing = route("account.find");
        const direct = encodeMetadataInput(routing, routingMimeType);

        expect(direct.mimeType).toBe(routingMimeType);
        expect(direct.payload).toEqual(["account.find"]);
        expect(() => encodeMetadataInput(routing, WellKnownMimeType.TEXT_PLAIN))
            .toThrow("cannot be sent with negotiated metadata MIME");
    });

    it("decodes application values while retaining raw frame and codec objects", () => {
        const routing = route("account.find");
        const encoded = new RequestResponseFrame(
            11,
            FrameFlag.NONE,
            routing,
            dataMimeType.toPayload({id: 11})
        );
        const frame = deserializeFrame(
            encoded.toUint8Array(),
            routingMimeType,
            dataMimeType
        ) as RequestResponseFrame;
        const decoded = decodeFramePayload<{id: number}, string[]>(frame);

        expect(decoded.frame).toBe(frame);
        expect(decoded.data).toEqual({id: 11});
        expect(decoded.metadata).toEqual(["account.find"]);
        expect(decoded.dataPayload).toBe(frame.payload);
        expect(decoded.metadataPayload).toBe(frame.metadata);
    });

    it("decodes compatible frame-like values without requiring concrete codec classes", () => {
        const frame = {
            payload: {raw: true},
            metadata: "metadata"
        } as unknown as RequestResponseFrame;
        const decoded = decodeFramePayload<{raw: boolean}, string>(frame);

        expect(decoded.data).toEqual({raw: true});
        expect(decoded.metadata).toBe("metadata");
        expect(decoded.dataPayload).toBeUndefined();
        expect(decoded.metadataPayload).toBeUndefined();
    });
});

describe("Core error payload helpers", () => {
    it("normalizes common thrown values without throwing while formatting", () => {
        const circular: {self?: unknown} = {};
        circular.self = circular;
        const unprintable = Object.create(null) as Record<string, unknown>;
        unprintable.self = unprintable;

        expect(errorMessage(new Error("failed"))).toBe("failed");
        expect(errorMessage("failed")).toBe("failed");
        expect(errorMessage({code: 7})).toBe('{"code":7}');
        expect(errorMessage(42n)).toBe("42");
        expect(errorMessage(circular)).toBe("[object Object]");
        expect(errorMessage(unprintable)).toBe("Unprintable error");
    });

    it("encodes normalized errors as text payloads", () => {
        const payload = errorPayload(new Error("request failed"));

        expect(payload.mimeType).toBe(WellKnownMimeType.TEXT_PLAIN);
        expect(new TextDecoder().decode(payload.toUint8Array())).toBe("request failed");
    });
});
