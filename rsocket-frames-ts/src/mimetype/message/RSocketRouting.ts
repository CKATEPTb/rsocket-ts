import type {ByteReader} from "bebyte";
import {decode, encode} from "@/utils";
import {MimeType} from "@/mimetype/MimeType";
import {Metadata} from "@/frame/context/Metadata";
import {createReader, createWriter} from "@/binary";
import {assertByteLength} from "@/utils";

/**
 * # Routing Metadata Extension
 *
 * _This extension specification is currently incubating.  While incubating the version is 0._
 *
 * ## Introduction
 * When two system are communicating via RSocket, there are often logical divisions in the messages that are sent from the requester to the responder.  These logical divisions can often be implemented by the responder as "routes" for messages to be sent to.  This extension specification provides an interoperable structure for metadata payloads to contain routing information.  It is designed such that an arbitrary collection of tags (strings) can be used by the responder to route messages and any individual tag (or all included tags) can be ignored.
 *
 * ## Metadata Payload
 * This metadata type is intended to be used per stream, and not per connection nor individual payloads and as such it **MUST** only be used in frame types used to initiate interactions.
 * This includes [`REQUEST_FNF`]{@link RequestFireAndForgetFrame}, [`REQUEST_RESPONSE`]{@link RequestResponseFrame}, [`REQUEST_STREAM`]{@link RequestStreamFrame}, and [`REQUEST_CHANNEL`]{@link RequestChannelFrame}.
 * The Metadata MIME Type is `message/x.rsocket.routing.v0`.
 *
 * ### Metadata Contents
 * ```
 *      0                   1                   2                   3
 *      0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
 *     +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 *     |  Tag Length   |              Tag                             ...
 *     +---------------+-----------------------------------------------+
 *     |  Tag Length   |              Tag                             ...
 *     +---------------+-----------------------------------------------+
 *                                    ...
 * ```
 *
 * * **Tag Payload**: Any number of complete tag payloads.
 *   * **Tag Length**: (8 bits = max value 2^8-1 = 255) Unsigned 8-bit integer of Tag Length in bytes.
 *   * **Tag**:  UTF-8 encoded Token used for routing.  The string MUST NOT be null terminated.  Examples include URI path-style routes (`/person/1`, `/address`), dot-separated convention ("person.1"), or any other format (`ios-client`, `android-client`).
 */
export class RSocketRouting extends MimeType<Array<string>> {
    /**
     * Serializes an array of routing tags (UTF-8 strings) into metadata.
     * Each tag is prefixed by its byte length as an 8-bit unsigned integer.
     *
     * @param {Array<string>} payloads - Array of routing tags to serialize.
     * @returns {Metadata<Array<string>>} Serialized metadata instance.
     */
    protected override serializeMetadata(payloads: Array<string>): Metadata<Array<string>> {
        const writer = createWriter()
        for (const payload of payloads) {
            const tag = encode(payload)
            assertByteLength("Routing tag", tag.length, 0, 0xff)
            writer.i8(tag.length)
            writer.write(tag)
        }
        return new Metadata(this, payloads, writer.toUint8Array())
    }

    /**
     * Converts the routing tag array into a `Uint8Array` binary format.
     *
     * Format:
     * [tag_length: u8][tag: UTF-8 bytes] repeated for each tag.
     *
     * @returns {Uint8Array} Serialized routing metadata.
     */
    protected override deserializeMetadata(payloads: ByteReader, hasPayload: boolean = true): Metadata<Array<string>> {
        const array = hasPayload ? payloads.viewBytes(payloads.i24()) : payloads.viewRemaining();
        const buffer = createReader(array)
        const deserialized: Array<string> = []
        while (buffer.offset < array.length) {
            deserialized.push(decode(buffer.viewBytes(buffer.i8())))
        }
        return new Metadata(this, deserialized, array)
    }
}
