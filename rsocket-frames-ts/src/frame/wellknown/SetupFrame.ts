import type {ByteReader, ByteWriter} from "bebyte";
import {decode, encode} from "@/utils";
import {Frame} from "@/frame/Frame";
import {Payload} from "@/frame/context/Payload";
import {FrameType} from "@/frame/FrameType";
import {MimeType} from "@/mimetype/MimeType";
import {SetupFlag} from "@/frame/FrameFlag";
import {Header} from "@/frame/context/Header";
import {Metadata} from "@/frame/context/Metadata";
import {assertByteLength, assertInteger, MAX_UINT_31} from "@/utils";
import {
    decodeResumeToken,
    type RSocketResumeToken,
    resumeTokenBytes,
    snapshotResumeToken
} from "@/frame/ResumeToken";

/**
 * ### SETUP Frame (0x01)
 *
 * Setup frames MUST always use Stream ID 0 as they pertain to the connection.
 *
 * The SETUP frame is sent by the client to inform the server of the parameters under which it desires
 * to operate. The usage and message sequence used is shown in [Connection Establishment]{@link https://github.com/rsocket/rsocket/blob/master/Protocol.md#connection-establishment}.
 *
 * One of the important parameters for a connection is the format, layout, and any schema of the data and metadata for
 * frames. This is, for lack of a better term, referred to here as "MIME Type". An implementation MAY use typical MIME type
 * values or MAY decide to use specific non-MIME type values to indicate format, layout, and any schema
 * for data and metadata. The protocol implementation MUST NOT interpret the MIME type itself. This is an application
 * concern only.
 *
 * The encoding format for Data and Metadata are included separately in the SETUP.
 *
 * Frame Contents
 *
 * ```
 *      0                   1                   2                   3
 *      0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
 *     +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 *     |0|                       Stream ID = 0                         |
 *     +-----------+-+-+-+-+-----------+-------------------------------+
 *     |Frame Type |0|M|R|L|  Flags    |
 *     +-----------+-+-+-+-+-----------+-------------------------------+
 *     |         Major Version         |        Minor Version          |
 *     +-------------------------------+-------------------------------+
 *     |0|                 Time Between KEEPALIVE Frames               |
 *     +---------------------------------------------------------------+
 *     |0|                       Max Lifetime                          |
 *     +---------------------------------------------------------------+
 *     |         Token Length          | Resume Identification Token  ...
 *     +---------------+-----------------------------------------------+
 *     |  MIME Length  |   Metadata Encoding MIME Type                ...
 *     +---------------+-----------------------------------------------+
 *     |  MIME Length  |     Data Encoding MIME Type                  ...
 *     +---------------+-----------------------------------------------+
 *                        Metadata & Setup Payload
 * ```
 *
 * * [__Frame Type__: (6 bits) 0x01]{@link FrameType#SETUP}
 * * [__Flags__: (10 bits)]{@link SetupFlag}
 *      * (__M__)etadata: Metadata present
 *      * (__R__)esume Enable: Client requests resume capability if possible. Resume Identification Token present.
 *      * (__L__)ease: Will honor LEASE (or not).
 * * __Major Version__: (16 bits = max value 65,535) Unsigned 16-bit integer of Major version number of the protocol.
 * * __Minor Version__: (16 bits = max value 65,535) Unsigned 16-bit integer of Minor version number of the protocol.
 * * __Time Between KEEPALIVE Frames__: (31 bits = max value 2^31-1 = 2,147,483,647) Unsigned 31-bit integer of Time (in milliseconds) between KEEPALIVE frames that the client will send. Value MUST be > 0.
 *    * For server-to-server connections, a reasonable time interval between client KEEPALIVE frames is 500ms.
 *    * For mobile-to-server connections, the time interval between client KEEPALIVE frames is often > 30,000ms.
 * * __Max Lifetime__: (31 bits = max value 2^31-1 = 2,147,483,647) Unsigned 31-bit integer of Time (in milliseconds) that a client will allow a server to not respond to a KEEPALIVE before it is assumed to be dead. Value MUST be > 0.
 * * __Resume Identification Token Length__: (16 bits = max value 65,535) Unsigned 16-bit integer of Resume Identification Token Length in bytes. (Not present if R flag is not set)
 * * __Resume Identification Token__: Token used for client resume identification (Not present if R flag is not set)
 * * __MIME Length__: Encoding MIME Type Length in bytes.
 * * __Encoding MIME Type__: MIME Type for encoding of Data and Metadata. This SHOULD be a US-ASCII string
 * that includes the [Internet media type](https://en.wikipedia.org/wiki/Internet_media_type) specified
 * in [RFC 2045](https://tools.ietf.org/html/rfc2045). Many are registered with
 * [IANA](https://www.iana.org/assignments/media-types/media-types.xhtml) such as
 * [CBOR](https://www.iana.org/assignments/media-types/application/cbor).
 * [Suffix](http://www.iana.org/assignments/media-type-structured-suffix/media-type-structured-suffix.xml)
 * rules MAY be used for handling layout. For example, `application/x.netflix+cbor` or
 * `application/x.reactivesocket+cbor` or `application/x.netflix+json`. The string MUST NOT be null terminated.
 * * __Setup Data__: includes payload describing connection capabilities of the endpoint sending the
 * Setup header.
 *
 * __NOTE__: A server that receives a SETUP frame that has (__R__)esume Enabled set, but does not support resuming operation, MUST reject the SETUP with an ERROR[REJECTED_SETUP].
 *
 * @description Sent by client to initiate protocol processing.
 * @see [Official documentation]{@link https://github.com/rsocket/rsocket/blob/master/Protocol.md#frame-setup}
 */
export class SetupFrame extends Frame {
    /** Opaque Resume token snapshot, when Resume is enabled. */
    public readonly resumeToken: RSocketResumeToken | undefined;
    /**
     * Constructs a `SetupFrame` instance.
     *
     * @param {number} keepalive - Interval (ms) between client KEEPALIVE frames.
     * @param {number} lifetime - Max time (ms) the client allows without a server KEEPALIVE response.
     * @param {MimeType<any>} metadataType - MIME type for metadata encoding.
     * @param {MimeType<any>} dataType - MIME type for data encoding.
     * @param {RSocketResumeToken} [resumeToken] - Optional opaque resume token (if `RESUME` is enabled).
     * @param {number} [majorVersion=1] - Major protocol version.
     * @param {number} [minorVersion=0] - Minor protocol version.
     * @param {SetupFlag} [flags=SetupFlag.NONE] - Initial flags (LEASE, METADATA, etc).
     * @param {Metadata<any>} [metadata] - Optional metadata block.
     * @param {Payload<any>} [payload] - Optional setup payload (application-specific).
     */
    public constructor(
        public readonly keepalive: number,
        public readonly lifetime: number,
        public readonly metadataType: MimeType<any>,
        public readonly dataType: MimeType<any>,
        resumeToken?: RSocketResumeToken,
        public readonly majorVersion: number = 1,
        public readonly minorVersion: number = 0,
        flags: SetupFlag = SetupFlag.NONE,
        metadata?: Metadata<any>,
        payload?: Payload<any>
    ) {
        const token = resumeToken === undefined ? undefined : snapshotResumeToken(resumeToken)
        flags = SetupFlag.combine(flags, token !== undefined ? SetupFlag.RESUME : SetupFlag.NONE)
        super(FrameType.SETUP, 0, flags, metadata, payload)
        this.resumeToken = token
        assertInteger("Keepalive interval", keepalive, 1, MAX_UINT_31)
        assertInteger("Maximum lifetime", lifetime, 1, MAX_UINT_31)
        assertInteger("Major version", majorVersion, 0, 0xffff)
        assertInteger("Minor version", minorVersion, 0, 0xffff)
        assertByteLength("Metadata MIME type", encode(metadataType.mimeType).length, 1, 0xff)
        assertByteLength("Data MIME type", encode(dataType.mimeType).length, 1, 0xff)
        if (this.hasResume()) {
            if (token === undefined) throw new TypeError("Resume flag requires a resume token")
            assertByteLength("Resume token", resumeTokenBytes(token).length, 0, 0xffff)
        }
    }

    /**
     * Deserializes a `SetupFrame` from binary.
     *
     * @param {Header} header - Frame header containing flags and type.
     * @param {ByteReader} reader - Reader instance pointing to frame body.
     * @param {MimeType<any>} _ - Ignored metadata type from deserialization context.
     * @param {MimeType<any>} __ - Ignored payload type from deserialization context.
     * @returns {SetupFrame} Parsed setup frame.
     */
    public static from(header: Header, reader: ByteReader, _: MimeType, __: MimeType): SetupFrame {
        const major = reader.i16()
        const minor = reader.i16()
        const keepalive = reader.i32()
        const lifetime = reader.i32()
        const resumeToken = header.isFlagSet(SetupFlag.RESUME)
            ? decodeResumeToken(reader.viewBytes(reader.i16()))
            : undefined
        const metadataType = MimeType.valueOf(decode(reader.viewBytes(reader.i8())))
        const dataType = MimeType.valueOf(decode(reader.viewBytes(reader.i8())))
        const metadata = header.isFlagSet(SetupFlag.METADATA) ? metadataType.toMetadata(reader) : undefined
        const payload = dataType.toPayload(reader)
        return new SetupFrame(keepalive, lifetime, metadataType, dataType, resumeToken, major, minor, header.flags, metadata, payload)
    }

    /**
     * Writes the frame-specific portion of the `SetupFrame` to the output.
     *
     * This includes protocol version, keepalive, lifetime, resume token (if any),
     * and MIME types for metadata and data.
     *
     * @param {ByteWriter} writer - Writer to output binary data.
     */
    protected write(writer: ByteWriter) {
        writer.i16(this.majorVersion)
        writer.i16(this.minorVersion)
        writer.i31(this.keepalive)
        writer.i31(this.lifetime)
        if (this.hasResume()) {
            const resumeToken = resumeTokenBytes(this.resumeToken!)
            writer.i16(resumeToken.length)
            writer.write(resumeToken)
        }
        const metadataType = encode(this.metadataType.mimeType)
        writer.i8(metadataType.length)
        writer.write(metadataType)
        const dataType = encode(this.dataType.mimeType)
        writer.i8(dataType.length)
        writer.write(dataType)
    }

    /**
     * SETUP frames must never be ignored, regardless of the IGNORE flag.
     *
     * @returns {false}
     */
    public override canBeIgnored(): boolean {
        return false
    }

    /**
     * Indicates whether this frame has resume support enabled.
     * Based on the `RESUME` flag.
     *
     * @returns {boolean} `true` if resume token is present and `RESUME` flag is set.
     */
    public hasResume(): boolean {
        return this.isFlagSet(SetupFlag.RESUME)
    }

    /**
     * Indicates whether this frame honors LEASE semantics.
     * Based on the `LEASE` flag.
     *
     * @returns {boolean} `true` if the LEASE flag is set.
     */
    public isRespectLease(): boolean {
        return this.isFlagSet(SetupFlag.LEASE)
    }
}
