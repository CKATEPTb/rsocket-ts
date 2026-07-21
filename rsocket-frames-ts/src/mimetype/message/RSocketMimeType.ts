import type {ByteReader} from "bebyte";
import {MimeType} from "@/mimetype/MimeType";
import {Metadata} from "@/frame/context/Metadata";
import {createReader, createWriter} from "@/binary";
import {decodeCustomType, encodeCustomType} from "@/mimetype/encoding";

/**
 * ## Metadata Payload for data MIME Type
 * This metadata type is intended to be used per stream, and not per connection nor individual payloads and as such it **MUST** only be used in frame types used to initiate interactions.
 * This includes [`REQUEST_FNF`]{@link RequestFireAndForgetFrame}, [`REQUEST_RESPONSE`]{@link RequestResponseFrame}, [`REQUEST_STREAM`]{@link RequestStreamFrame}, and [`REQUEST_CHANNEL`]{@link RequestChannelFrame}.
 * Multiple metadata payloads with the same MIME type are allowed.
 * The order of metadata payloads MUST be preserved when presented to responders.  The Metadata MIME Type is `message/x.rsocket.mime-type.v0`.
 *
 * ### Metadata Contents
 * ```
 *      0                   1                   2                   3
 *      0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
 *     +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 *     |M| MIME ID/Len |   Data Encoding MIME Type                    ...
 *     +---------------+-----------------------------------------------+
 * ```
 * * (**M**)etadata Type: Metadata type is a well known value represented by a unique integer.
 * * **MIME ID/Length**: (7 bits = max value 2^7 = 128) Unsigned 7-bit integer.  If M flag is set, indicates a [Well-known MIME Type ID]{@link WellKnownMimeType}.  If M flag is not set, indicates the encoding MIME Type Length in bytes.
 * * **Metadata Encoding MIME Type**: MIME Type for encoding of Metadata. This SHOULD be a US-ASCII string that includes the [Internet media type]{@link https://en.wikipedia.org/wiki/Internet_media_type} specified in [RFC 2045]{@link https://github.com/rsocket/rsocket/blob/master/Protocol.md#frame-fnf}.
 * Many are registered with [IANA]{@link https://www.iana.org/assignments/media-types/media-types.xhtml} and others such as [Routing]{@link RSocketRouting} and [Tracing (Zipkin)]{@link} are not.
 * [Suffix]{@link http://www.iana.org/assignments/media-type-structured-suffix/media-type-structured-suffix.xml} rules MAY be used for handling layout.  The string MUST NOT be null terminated.  (Not present if M flag is set)
 */
export class RSocketMimeType extends MimeType<MimeType<any>> {
    /**
     * Serializes a single `MimeType` to binary metadata format.
     *
     * @param {MimeType} payload - The MIME type to serialize.
     * @returns {Metadata<MimeType>} The metadata object wrapping the MIME type.
     */
    protected override serializeMetadata(payload: MimeType<any>): Metadata<MimeType<any>> {
        const writer = createWriter()
        if (payload.isWellKnown) writer.i8(128 | payload.identifier!)
        else {
            const type = encodeCustomType(payload.mimeType, "MIME type")
            writer.i7(type.length - 1)
            writer.write(type)
        }
        return new Metadata(this, payload, writer.toUint8Array())
    }

    /**
     * Deserializes metadata into a `MimeType` object.
     *
     * @param {ByteReader} payload - The binary reader.
     * @param {boolean} [hasPayload=true] - Whether the payload has a length prefix.
     * @returns {Metadata<MimeType>} The resulting metadata.
     */
    protected override deserializeMetadata(payload: ByteReader, hasPayload: boolean = true): Metadata<MimeType<any>> {
        const array = hasPayload ? payload.viewBytes(payload.i24()) : payload.viewRemaining();
        const buffer = createReader(array);
        const i8 = buffer.i8()
        const i7 = i8 & 0x7F
        const mimeType = i8 >> 7
            ? MimeType.valueOf(i7)
            : MimeType.valueOf(decodeCustomType(i7, length => buffer.viewBytes(length)))
        if (buffer.offset !== array.length) {
            throw new RangeError(`MIME type metadata contains ${array.length - buffer.offset} unexpected trailing byte(s)`);
        }
        return new Metadata(this, mimeType, array)
    }

}

/**
 * ## Metadata Payload for acceptable data MIME Types
 * This metadata type is intended to be used per stream, and not per connection nor individual payloads and as such it **MUST** only be used in frame types used to initiate interactions.
 * This includes [`REQUEST_FNF`]{@link RequestFireAndForgetFrame}, [`REQUEST_RESPONSE`]{@link RequestResponseFrame}, [`REQUEST_STREAM`]{@link RequestStreamFrame}, and [`REQUEST_CHANNEL`]{@link RequestChannelFrame}.
 * Multiple metadata payloads with the same MIME type are allowed.  The order of metadata payloads MUST be preserved when presented to responders.  The Metadata MIME Type is `message/x.rsocket.accept-mime-types.v0`.
 *
 * ### Metadata Contents
 * ```
 *      0                   1                   2                   3
 *      0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
 *     +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 *     |M| MIME ID/Len |   Data Encoding MIME Type                    ...
 *     +---------------+-----------------------------------------------+
 *     |M| MIME ID/Len |   Data Encoding MIME Type                    ...
 *     +---------------+-----------------------------------------------+
 *                                    ...
 * ```
 * * (**M**)etadata Type: Metadata type is a well known value represented by a unique integer.
 * * **MIME ID/Length**: (7 bits = max value 2^7 = 128) Unsigned 7-bit integer.  If M flag is set, indicates a [Well-known MIME Type ID]{@link WellKnownMimeType}.  If M flag is not set, indicates the encoding MIME Type Length in bytes.
 * * **Metadata Encoding MIME Type**: MIME Type for encoding of Metadata. This SHOULD be a US-ASCII string that includes the [Internet media type]{@link https://en.wikipedia.org/wiki/Internet_media_type} specified in [RFC 2045]{@link https://github.com/rsocket/rsocket/blob/master/Protocol.md#frame-fnf}.
 * Many are registered with [IANA]{@link https://www.iana.org/assignments/media-types/media-types.xhtml} and others such as [Routing]{@link RSocketRouting} and [Tracing (Zipkin)]{@link} are not.
 * [Suffix]{@link http://www.iana.org/assignments/media-type-structured-suffix/media-type-structured-suffix.xml} rules MAY be used for handling layout.  The string MUST NOT be null terminated.  (Not present if M flag is set)
 */
export class RSocketMimeTypes extends MimeType<Array<MimeType<any>>> {
    /**
     * Serializes a list of MIME types into binary metadata format.
     *
     * @param {Array<MimeType>} payloads - MIME types to encode.
     * @returns {Metadata<Array<MimeType>>} The resulting metadata.
     */
    protected override serializeMetadata(payloads: Array<MimeType<any>>): Metadata<Array<MimeType<any>>> {
        const writer = createWriter()
        for (const payload of payloads) {
            if (payload.isWellKnown) writer.i8(128 | payload.identifier!)
            else {
                const type = encodeCustomType(payload.mimeType, "MIME type")
                writer.i7(type.length - 1)
                writer.write(type)
            }
        }
        return new Metadata(this, payloads, writer.toUint8Array())
    }

    /**
     * Deserializes a byte stream into a list of `MimeType` objects.
     *
     * @param {ByteReader} reader - Source to read metadata from.
     * @param {boolean} [hasPayload=true] - Whether to read a length-prefixed block.
     * @returns {Metadata<Array<MimeType>>} Parsed metadata with MIME types.
     */
    protected override deserializeMetadata(reader: ByteReader, hasPayload: boolean = true): Metadata<Array<MimeType<any>>> {
        const array = hasPayload ? reader.viewBytes(reader.i24()) : reader.viewRemaining();
        const buffer = createReader(array);
        const payloads: Array<MimeType<any>> = []
        while (buffer.offset < array.length) {
            const i8 = buffer.i8()
            const i7 = i8 & 0x7F
            payloads.push(i8 >> 7
                ? MimeType.valueOf(i7)
                : MimeType.valueOf(decodeCustomType(i7, length => buffer.viewBytes(length))))
        }
        return new Metadata(this, payloads, array)
    }
}
