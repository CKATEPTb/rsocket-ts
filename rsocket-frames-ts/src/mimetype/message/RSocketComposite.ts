import type {ByteReader} from "bebyte";
import {MimeType} from "@/mimetype/MimeType";
import {Metadata} from "@/frame/context/Metadata";
import {createReader, createWriter} from "@/binary";
import {decodeCustomType, encodeCustomType} from "@/mimetype/encoding";

/**
 * # Composite Metadata Extension
 *
 * _This extension specification is currently incubating.  While incubating the version is 0._
 *
 * ## Introduction
 * There are a number of situations where an arbitrary collection of discrete metadata types should be attached to frame.  For example, a request frame may want to include both routing metadata as well as tracing metadata.  This extension specification provides an interoperable structure for metadadata payloads to contain multiple discrete metadata types.  It is designed such that if a consumer of the metadata is unaware of a particular type, it can be safely skipped and the next one read.
 *
 * ## Metadata Payload
 * This metadata type is intended to be used per stream, and not per connection nor individual payloads and as such it **MUST** only be used in frame types used to initiate interactions.
 * This includes [`REQUEST_FNF`]{@link RequestFireAndForgetFrame}, [`REQUEST_RESPONSE`]{@link RequestResponseFrame}, [`REQUEST_STREAM`]{@link RequestStreamFrame}, and [`REQUEST_CHANNEL`]{@link RequestChannelFrame}.
 * Multiple metadata payloads with the same MIME type are allowed. The order of metadata payloads MUST be preserved when presented to responders.
 * The [`SETUP` Frame]{@link SetupFrame} Metadata MIME Type is `message/x.rsocket.composite-metadata.v0`.
 *
 * ### Metadata Contents
 * ```
 *      0                   1                   2                   3
 *      0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
 *     +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 *     |M| MIME ID/Len |   Metadata Encoding MIME Type                ...
 *     +---------------+---------------+---------------+---------------+
 *     |              Metadata Length                  |
 *     +-----------------------------------------------+---------------+
 *     |                     Metadata Payload                         ...
 *     +---------------+-----------------------------------------------+
 *     |M| MIME ID/Len |   Metadata Encoding MIME Type                ...
 *     +---------------+-------------------------------+---------------+
 *     |              Metadata Length                  |
 *     +-----------------------------------------------+---------------+
 *     |                     Metadata Payload                         ...
 *     +---------------------------------------------------------------+
 *                                    ...
 * ```
 *
 * * **Metadata Payload**: Any number of complete metadata payloads.
 *   * (**M**)etadata Type: Metadata type is a well known value represented by a unique integer.
 *   * **MIME ID/Length**: (7 bits = max value 2^7 = 128) Unsigned 7-bit integer.  If M flag is set, indicates a [Well-known MIME Type ID][wk].  If M flag is not set, indicates the encoding MIME Type Length in bytes.
 *   * **Metadata Encoding MIME Type**: MIME Type for encoding of Metadata. This SHOULD be a US-ASCII string that includes the [Internet media type](https://en.wikipedia.org/wiki/Internet_media_type) specified in [RFC 2045][rf].  Many are registered with [IANA][ia] and others such as [Routing][r] and [Tracing (Zipkin)][tz] are not.  [Suffix][s] rules MAY be used for handling layout.  The string MUST NOT be null terminated.  (Not present if M flag is set)
 *   * **Metadata Length**: (24 bits = max value 16,777,215) Unsigned 24-bit integer of Metadata Length in bytes.
 *   * **Metadata Payload**: User configured metadata encoded as defined by the Metadata Encoding MIME Type.
 */
export class RSocketComposite extends MimeType<Array<Metadata<any>>> {
    /**
     * Serializes an array of `Metadata<T>` entries into composite metadata format.
     *
     * @param {Array<Metadata<any>>} payloads - Metadata entries to encode.
     * @returns {Metadata<Array<Metadata<any>>>} Composite metadata wrapper.
     */
    protected override serializeMetadata(payloads: Array<Metadata<any>>): Metadata<Array<Metadata<any>>> {
        let capacity = 0;
        const encodedPayloads = new Array<Uint8Array>(payloads.length);
        for (let index = 0; index < payloads.length; index += 1) {
            const payload = payloads[index] as Metadata<any>;
            const bytes = payload.toUint8Array();
            encodedPayloads[index] = bytes;
            capacity += 1 + 3 + bytes.byteLength;
            if (!payload.mimeType.isWellKnown) {
                const length = payload.mimeType.mimeType.length;
                if (length < 1 || length > 128) {
                    throw new RangeError("Composite metadata MIME type must contain between 1 and 128 ASCII bytes");
                }
                capacity += length;
            }
        }

        const writer = createWriter(capacity)
        for (let index = 0; index < payloads.length; index += 1) {
            const payload = payloads[index] as Metadata<any>;
            if (payload.mimeType.isWellKnown) writer.i8(128 | payload.mimeType.identifier!)
            else {
                const type = encodeCustomType(payload.mimeType.mimeType, "Composite metadata MIME type")
                writer.i7(type.length - 1)
                writer.write(type)
            }
            const bytes = encodedPayloads[index] as Uint8Array;
            writer.i24(bytes.byteLength);
            writer.write(bytes)
        }
        return new Metadata(this, payloads, writer.toUint8Array())
    }

    /**
     * Deserializes a composite metadata block into individual `Metadata<T>` instances.
     * Each metadata entry is parsed according to its MIME type.
     *
     * @param {ByteReader} payloads - Reader containing composite metadata.
     * @param {boolean} [hasPayload=true] - Whether the payload is prefixed with a length (i24).
     * @returns {Metadata<Array<Metadata<any>>>} The reconstructed composite metadata object.
     */
    protected override deserializeMetadata(payloads: ByteReader, hasPayload: boolean = true): Metadata<Array<Metadata<any>>> {
        const array = hasPayload ? payloads.viewBytes(payloads.i24()) : payloads.viewRemaining();
        const buffer = createReader(array);
        const deserialized: Array<Metadata<any>> = []
        while (buffer.offset < array.length) {
            const i8 = buffer.i8()
            const i7 = i8 & 0x7F
            const mimeType = i8 >> 7
                ? MimeType.valueOf(i7)
                : MimeType.valueOf(decodeCustomType(i7, length => buffer.viewBytes(length)))
            const encoded = buffer.viewBytes(buffer.i24())
            const decoded = mimeType.toMetadata(encoded, false)
            deserialized.push(decoded instanceof Metadata
                ? decoded
                : new Metadata(mimeType, decoded, encoded))
        }
        return new Metadata(this, deserialized, array)
    }
}
