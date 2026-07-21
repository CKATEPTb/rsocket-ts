import type {ByteReader} from "bebyte";
import {Metadata} from "@/frame/context/Metadata";
import {Payload} from "@/frame/context/Payload";
import {byteView, createReader} from "@/binary";
import {assertInteger} from "@/utils";
import {assertAscii} from "@/mimetype/encoding";

/** Returns whether a value implements the reader operations used by codecs. */
function isByteReader(value: unknown): value is ByteReader {
    return typeof value === "object"
        && value !== null
        && typeof (value as ByteReader).i8 === "function"
        && typeof (value as ByteReader).i24 === "function"
        && typeof (value as ByteReader).viewBytes === "function"
        && typeof (value as ByteReader).viewRemaining === "function";
}

/**
 * Represents a MIME type and provides serialization/deserialization
 * logic for metadata and payloads associated with that type.
 *
 * @template T The payload type, defaults to `Uint8Array`.
 */
export class MimeType<T = Uint8Array> {
    /**
     * Internal registry of all known MIME types.
     * Maps MIME type strings to their corresponding `MimeType` instances.
     * @private
     */
    private static readonly valuesByName = new Map<string, MimeType<any>>();
    private static readonly valuesByIdentifier = new Map<number, MimeType<any>>();
    /** Bounded fallback cache for the 128 possible unregistered wire identifiers. */
    private static readonly unknownByIdentifier: Array<MimeType<any> | undefined> = [];

    /**
     * Creates a new `MimeType` instance and registers it in the internal map.
     *
     * @param {string} mimeType - The MIME type string (e.g. "application/json").
     * @param {number} [identifier] - Optional numeric identifier for well-known types.
     * @param register Whether this codec should be available to global wire lookups.
     */
    public constructor(
        public readonly mimeType: string,
        public readonly identifier?: number,
        register = true
    ) {
        if (typeof mimeType !== "string" || mimeType.length === 0) {
            throw new TypeError("MIME type must be a non-empty string");
        }
        assertAscii(mimeType, "MIME type");
        if (identifier !== undefined) {
            assertInteger("MIME type identifier", identifier, 0, 0x7f)
            if (register) MimeType.valuesByIdentifier.set(identifier, this)
        }
        if (register) MimeType.valuesByName.set(mimeType, this)
    }

    /**
     * Indicates whether the MIME type is well-known (has an associated identifier).
     *
     * @returns {boolean} `true` if the type has an identifier, otherwise `false`.
     */
    public get isWellKnown() {
        return this.identifier !== undefined
    }

    /**
     * Serializes the given payload into a `Metadata` object.
     *
     * @param {T} payload - The payload to wrap.
     * @returns {Metadata<T>} The resulting `Metadata` instance.
     * @protected
     */
    protected serializeMetadata(payload: T): Metadata<T> {
        return new Metadata(this, payload)
    }

    /**
     * Deserializes a `Metadata` object from a `ByteReader` stream.
     *
     * @param {ByteReader} payload - The byte stream to read from.
     * @param {boolean} [hasPayload=true] - Whether a length-prefixed payload is expected.
     * @returns {Metadata<T>} The deserialized metadata.
     * @protected
     */
    protected deserializeMetadata(payload: ByteReader, hasPayload: boolean = true): Metadata<T> {
        const encoded = hasPayload ? payload.viewBytes(payload.i24()) : payload.viewRemaining();
        return new Metadata(this, encoded as T)
    }

    /**
     * Converts a payload or byte stream into a `Metadata` object.
     * Automatically chooses between serialization and deserialization based on input type.
     *
     * @param {Uint8Array | ByteReader | T} payload Encoded bytes, a reader, or a value to encode.
     * @param {boolean} [hasPayload=true] - Indicates if the reader contains a length-prefixed payload.
     * @returns {Metadata<T>} A `Metadata` instance.
     */
    public toMetadata(payload: Uint8Array | ByteReader | T, hasPayload: boolean = true): Metadata<T> {
        const bytes = byteView(payload);
        if (bytes !== undefined) return this.deserializeMetadata(createReader(bytes), hasPayload)
        if (isByteReader(payload)) return this.deserializeMetadata(payload, hasPayload)
        return this.serializeMetadata(payload as T)
    }

    /**
     * Serializes the given payload into a `Payload` object.
     *
     * @param {T} payload - The payload to wrap.
     * @returns {Payload<T>} The resulting `Payload` instance.
     * @protected
     */
    protected serializePayload(payload: T): Payload<T> {
        return new Payload(this, payload)
    }

    /**
     * Deserializes a `Payload` object from a `ByteReader` stream.
     *
     * @param {ByteReader} payload - The byte stream to read from.
     * @returns {Payload<T>} The deserialized payload.
     * @protected
     */
    protected deserializePayload(payload: ByteReader): Payload<T> {
        const encoded = payload.viewRemaining();
        return new Payload(this, encoded as T)
    }

    /**
     * Converts a payload or byte stream into a `Payload` object.
     * Automatically chooses between serialization and deserialization based on input type.
     *
     * @param {Uint8Array | ByteReader | T} payload Encoded bytes, a reader, or a value to encode.
     * @returns {Payload<T>} A `Payload` instance.
     */
    public toPayload(payload: Uint8Array | ByteReader | T): Payload<T> {
        const bytes = byteView(payload);
        if (bytes !== undefined) return this.deserializePayload(createReader(bytes))
        if (isByteReader(payload)) return this.deserializePayload(payload)
        return this.serializePayload(payload as T)
    }

    /**
     * Retrieves a registered `MimeType` by string or identifier.
     * Unknown names are represented by transient generic codecs so arbitrary
     * peer values cannot grow a registry. Unknown identifiers use a bounded
     * 128-slot cache and retain their identifier for lossless re-encoding.
     *
     * @param {string | number} mimeType - MIME type string or numeric identifier.
     * @returns {MimeType} A matching or unknown `MimeType` instance.
     */
    public static valueOf<T = Uint8Array>(mimeType: string | number): MimeType<T> {
        if (typeof mimeType === "number") {
            assertInteger("MIME type identifier", mimeType, 0, 0x7f)
            return (MimeType.valuesByIdentifier.get(mimeType)
                ?? (MimeType.unknownByIdentifier[mimeType]
                    ??= new MimeType(String(mimeType), mimeType, false))) as MimeType<T>
        }
        return (MimeType.valuesByName.get(mimeType) ?? new MimeType(mimeType, undefined, false)) as MimeType<T>
    }
}
