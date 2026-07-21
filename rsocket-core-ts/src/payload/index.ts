/** Payload and metadata helpers built around `rsocket-frames-ts` codecs. */
import {type Frame, Metadata, MimeType, Payload, WellKnownMimeType} from "rsocket-frames-ts";
import {DEFAULT_DATA_MIME_TYPE, DEFAULT_METADATA_MIME_TYPE, ERROR_DATA_MIME_TYPE} from "@/protocol/index.js";
import type {RSocketPayload, RSocketPayloadFrame, RSocketPayloadInput} from "@/types/index.js";

/** Shared encoded-empty payload result for metadata-only or empty frames. */
const EMPTY_ENCODED_PAYLOAD: EncodedPayload = Object.freeze({});
/** Stable MIME name used to recognize negotiated composite metadata. */
const COMPOSITE_METADATA_MIME_TYPE = WellKnownMimeType.MESSAGE_RSOCKET_COMPOSITE_METADATA.mimeType;
/** Reuses one-entry composite wrappers for stable MIME-typed metadata. */
const COMPOSITE_METADATA_CACHE = new WeakMap<Metadata<any>, Metadata<any>>();

/**
 * Encoded payload pieces ready to be inserted into an RSocket frame.
 */
export interface EncodedPayload {
    payload?: Payload<any>;
    metadata?: Metadata<any>;
}

/**
 * Encodes an application value into an RSocket data payload.
 *
 * @param payload - Application value to encode.
 * @param mimeType - Mime type codec used to encode the value.
 */
export function data<D>(payload: D, mimeType: MimeType<D> = DEFAULT_DATA_MIME_TYPE as MimeType<D>): Payload<D> {
    return encodePayloadValue(payload, mimeType);
}

/**
 * Encodes an application value into RSocket metadata.
 *
 * @param payload - Metadata value to encode.
 * @param mimeType - Mime type codec used to encode the value.
 */
export function metadata<M>(
    payload: M,
    mimeType: MimeType<M> = DEFAULT_METADATA_MIME_TYPE as unknown as MimeType<M>
): Metadata<M> {
    return encodeMetadataValue(payload, mimeType);
}

/**
 * Creates routing metadata for one or more Spring/RSocket route segments.
 */
export function route(...routes: string[]): Metadata<string[]> {
    return cachedMetadata(WellKnownMimeType.MESSAGE_RSOCKET_ROUTING.toMetadata(routes));
}

/**
 * Combines metadata entries into RSocket composite metadata.
 */
export function compositeMetadata(...entries: Array<Metadata<any>>): Metadata<Array<Metadata<any>>> {
    return compositeMetadataEntries(entries) as Metadata<Array<Metadata<any>>>;
}

/**
 * Combines an existing metadata entry list without rest-parameter allocation.
 */
export function compositeMetadataEntries(entries: readonly Metadata<any>[]): Metadata<readonly Metadata<any>[]> {
    return cachedMetadata(WellKnownMimeType.MESSAGE_RSOCKET_COMPOSITE_METADATA.toMetadata(entries as Metadata<any>[]));
}

/**
 * Normalizes user payload input into frame-ready payload and metadata objects.
 *
 * Accepts raw values, `Payload`, `Metadata`, or `{ data, metadata }` envelopes.
 */
export function encodePayloadInput(
    input: RSocketPayloadInput<any, any> | undefined,
    dataMimeType: MimeType<any>,
    metadataMimeType: MimeType<any>
): EncodedPayload {
    if (input === undefined) return EMPTY_ENCODED_PAYLOAD;
    if (input instanceof Payload) return {payload: cachedPayload(input)};
    if (input instanceof Metadata) return {metadata: encodeMetadataInput(input, metadataMimeType)};

    if (isPayloadEnvelope(input)) {
        const payload =
            "data" in input && input.data !== undefined
                ? encodeData(input.data, input.dataMimeType ?? dataMimeType)
                : undefined;
        const frameMetadata =
            "metadata" in input && input.metadata !== undefined
                ? encodeMetadataInput(input.metadata, input.metadataMimeType ?? metadataMimeType)
                : undefined;

        return optionalEncoded(payload, frameMetadata);
    }

    return {payload: encodePayloadValue(input, dataMimeType)};
}

/**
 * Encodes raw metadata or adapts a MIME-typed entry to the negotiated metadata MIME.
 *
 * A non-composite entry is wrapped automatically when an endpoint uses
 * RSocket composite metadata. With a matching direct MIME, including RSocket
 * routing metadata, the entry is sent without a container.
 */
export function encodeMetadataInput(value: unknown, mimeType: MimeType<any>): Metadata<any> {
    return value instanceof Metadata
        ? metadataForMimeType(value, mimeType)
        : encodeMetadataValue(value, mimeType);
}

/**
 * Decodes data and metadata from a received frame into a friendly payload shape.
 */
export function decodeFramePayload<D = unknown, M = unknown>(frame: Frame): RSocketPayloadFrame<D, M> {
    const rawDataPayload = (frame as { payload?: unknown }).payload;
    const rawMetadataPayload = (frame as { metadata?: unknown }).metadata;
    const decoded: {
        frame: Frame;
        data?: D;
        metadata?: M;
        dataPayload?: Payload<D>;
        metadataPayload?: Metadata<M>;
    } = {frame};

    if (rawDataPayload !== undefined) {
        if (isPayloadObject(rawDataPayload)) {
            decoded.data = rawDataPayload.payload as D;
            decoded.dataPayload = rawDataPayload as Payload<D>;
        } else {
            decoded.data = rawDataPayload as D;
        }
    }

    if (rawMetadataPayload !== undefined) {
        if (isMetadataObject(rawMetadataPayload)) {
            decoded.metadata = rawMetadataPayload.payload as M;
            decoded.metadataPayload = rawMetadataPayload as Metadata<M>;
        } else {
            decoded.metadata = rawMetadataPayload as M;
        }
    }

    return decoded as RSocketPayloadFrame<D, M>;
}

/**
 * Encodes an arbitrary error into a text payload suitable for ERROR frames.
 */
export function errorPayload(error: unknown): Payload<string> {
    return cachedPayload(ERROR_DATA_MIME_TYPE.toPayload(errorMessage(error)));
}

/**
 * Converts an arbitrary thrown value into a readable message.
 */
export function errorMessage(error: unknown): string {
    if (error instanceof Error && error.message.length > 0) return error.message;
    if (typeof error === "string") return error;
    if (error === null || typeof error !== "object") return String(error);
    try {
        const json = JSON.stringify(error);
        if (typeof json === "string") return json;
    } catch {
        // Fall through to String(...) for values like BigInt that JSON cannot encode.
    }
    try {
        return String(error);
    } catch {
        return "Unprintable error";
    }
}

/**
 * Encodes a data value unless it is already a `Payload` instance.
 */
function encodeData(value: unknown, mimeType: MimeType<any>): Payload<any> {
    if (value instanceof Payload) return cachedPayload(value);
    return encodePayloadValue(value, mimeType);
}

/** Serializes outbound data through the MIME type's public codec contract. */
function encodePayloadValue<T>(value: T, mimeType: MimeType<T>): Payload<T> {
    return cachedPayload(mimeType.toPayload(value));
}

/** Serializes outbound metadata through the MIME type's public codec contract. */
function encodeMetadataValue<T>(value: T, mimeType: MimeType<T>): Metadata<T> {
    return cachedMetadata(mimeType.toMetadata(value));
}

/**
 * Converts one already typed metadata entry to the connection metadata format.
 */
function metadataForMimeType(value: Metadata<any>, target: MimeType<any>): Metadata<any> {
    const sourceMimeType = value.mimeType.mimeType;
    const targetMimeType = target.mimeType;
    if (targetMimeType === COMPOSITE_METADATA_MIME_TYPE) {
        if (sourceMimeType === targetMimeType) return cachedMetadata(value);
        const cached = COMPOSITE_METADATA_CACHE.get(value);
        if (cached !== undefined) return cached;
        const composite = compositeMetadataEntries([cachedMetadata(value)]);
        COMPOSITE_METADATA_CACHE.set(value, composite);
        return composite;
    }
    if (sourceMimeType === targetMimeType) return cachedMetadata(value);
    throw new TypeError(
        `Metadata MIME "${sourceMimeType}" cannot be sent with negotiated metadata MIME "${targetMimeType}". ` +
        "Use RSocket composite metadata or the matching direct MIME type."
    );
}

/**
 * Builds an encoded payload object without explicit undefined fields.
 */
function optionalEncoded(payload: Payload<any> | undefined, frameMetadata: Metadata<any> | undefined): EncodedPayload {
    if (payload === undefined) {
        return frameMetadata === undefined ? EMPTY_ENCODED_PAYLOAD : {metadata: frameMetadata};
    }
    return frameMetadata === undefined ? {payload} : {payload, metadata: frameMetadata};
}

/**
 * Detects the friendly `{ data, metadata }` payload envelope.
 */
function isPayloadEnvelope(value: unknown): value is RSocketPayload<any, any> {
    if (typeof value !== "object" || value === null) return false;
    return "data" in value || "metadata" in value || "dataMimeType" in value || "metadataMimeType" in value;
}

/**
 * Detects `Payload` instances and compatible codec payload shapes.
 */
function isPayloadObject(value: unknown): value is Payload<any> {
    return value instanceof Payload || isCodecPayloadShape(value);
}

/**
 * Detects `Metadata` instances and compatible codec payload shapes.
 */
function isMetadataObject(value: unknown): value is Metadata<any> {
    return value instanceof Metadata || isCodecPayloadShape(value);
}

/**
 * Detects codec objects produced by `rsocket-frames-ts`.
 */
function isCodecPayloadShape(value: unknown): value is {
    payload: unknown;
    mimeType: MimeType<any>;
    toUint8Array(): Uint8Array
} {
    if (typeof value !== "object" || value === null) return false;
    const candidate = value as { readonly toUint8Array?: unknown };
    return "payload" in candidate &&
        "mimeType" in candidate &&
        typeof candidate.toUint8Array === "function";
}

/**
 * Wraps a freshly encoded payload so repeated size checks reuse the same bytes.
 */
function cachedPayload<T>(source: Payload<T>): Payload<T> {
    return source instanceof CachedPayload || source.toUint8Array === Payload.prototype.toUint8Array
        ? source
        : new CachedPayload(source);
}

/**
 * Wraps freshly encoded metadata so repeated size checks reuse the same bytes.
 */
function cachedMetadata<T>(source: Metadata<T>): Metadata<T> {
    return source instanceof CachedMetadata || source.toUint8Array === Metadata.prototype.toUint8Array
        ? source
        : new CachedMetadata(source);
}

/**
 * Payload wrapper that computes serialized bytes once for repeated frame writes.
 */
class CachedPayload<T> extends Payload<T> {
    private bytes: Uint8Array | undefined;

    /**
     * Creates a payload value that delegates encoding to the original codec.
     */
    constructor(private readonly source: Payload<T>) {
        super(source.mimeType, source.payload);
    }

    /**
     * Returns cached wire bytes for stable payload values.
     */
    override toUint8Array(): Uint8Array {
        return this.bytes ??= this.source.toUint8Array();
    }
}

/**
 * Metadata wrapper that computes serialized bytes once for repeated frame writes.
 */
class CachedMetadata<T> extends Metadata<T> {
    private bytes: Uint8Array | undefined;

    /**
     * Creates a metadata value that delegates encoding to the original codec.
     */
    constructor(private readonly source: Metadata<T>) {
        super(source.mimeType, source.payload);
    }

    /**
     * Returns cached wire bytes for stable metadata payloads.
     */
    override toUint8Array(): Uint8Array {
        return this.bytes ??= this.source.toUint8Array();
    }
}
