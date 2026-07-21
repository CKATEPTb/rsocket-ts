import type {ByteReader, ByteWriter} from "bebyte";
import {Frame} from "@/frame/Frame";
import {FrameType} from "@/frame/FrameType";
import {Header} from "@/frame/context/Header";
import {MimeType} from "@/mimetype/MimeType";
import {assertBigInt, assertByteLength, assertInteger, MAX_UINT_63} from "@/utils";
import {
    decodeResumeToken,
    type RSocketResumeToken,
    resumeTokenBytes,
    snapshotResumeToken
} from "@/frame/ResumeToken";

/**
 * #### RESUME Frame (0x0D)
 *
 * The general format for a Resume frame is given below.
 *
 * RESUME frames MUST always use Stream ID 0 as they pertain to the connection.
 *
 * ```
 *      0                   1                   2                   3
 *      0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
 *     +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 *     |0|                       Stream ID = 0                         |
 *     +-----------+-+-+---------------+-------------------------------+
 *     |Frame Type |0|0|    Flags      |
 *     +-------------------------------+-------------------------------+
 *     |        Major Version          |         Minor Version         |
 *     +-------------------------------+-------------------------------+
 *     |         Token Length          | Resume Identification Token  ...
 *     +---------------------------------------------------------------+
 *     |0|                                                             |
 *     +                 Last Received Server Position                 +
 *     |                                                               |
 *     +---------------------------------------------------------------+
 *     |0|                                                             |
 *     +                First Available Client Position                +
 *     |                                                               |
 *     +---------------------------------------------------------------+
 * ```
 *
 * * [__Frame Type__: (6 bits) 0x0D]{@link FrameType#RESUME}
 * * __Major Version__: (16 bits = max value 65,535) Unsigned 16-bit integer of Major version number of the protocol.
 * * __Minor Version__: (16 bits = max value 65,535) Unsigned 16-bit integer of Minor version number of the protocol.
 * * __Resume Identification Token Length__: (16 bits = max value 65,535) Unsigned 16-bit integer of Resume Identification Token Length in bytes.
 * * __Resume Identification Token__: Token used for client resume identification. Same Resume Identification used in the initial SETUP by the client.
 * * __Last Received Server Position__: (63 bits = max value 2^63-1) Unsigned 63-bit long of the last implied position the client received from the server.
 * * __First Available Client Position__: (63 bits = max value 2^63-1) Unsigned 63-bit long of the earliest position that the client can rewind back to prior to resending frames.
 *
 * @description Resume: Replaces [SETUP]{@link SetupFrame} for Resuming Operation (optional)
 * @see [Official documentation]{@link https://github.com/rsocket/rsocket/blob/master/Protocol.md#frame-resume}
 */
export class ResumeFrame extends Frame {
    /** Immutable opaque token matching the original SETUP frame. */
    public readonly resumeToken: RSocketResumeToken;
    /**
     * Creates a new `ResumeFrame`.
     *
     * @param {RSocketResumeToken} resumeToken - Opaque identifier that must match the token from `SETUP` byte-for-byte.
     * @param {bigint} lastReceivedServerPosition - The last byte position received from the server before disconnection.
     * @param {bigint} firstAvailableClientPosition - The earliest byte position from which the client can replay messages.
     * @param {number} [majorVersion=1] - Protocol major version.
     * @param {number} [minorVersion=0] - Protocol minor version.
     */
    public constructor(
        resumeToken: RSocketResumeToken,
        public readonly lastReceivedServerPosition: bigint,
        public readonly firstAvailableClientPosition: bigint,
        public readonly majorVersion: number = 1,
        public readonly minorVersion: number = 0
    ) {
        super(FrameType.RESUME, 0)
        this.resumeToken = snapshotResumeToken(resumeToken)
        assertByteLength("Resume token", resumeTokenBytes(this.resumeToken).length, 0, 0xffff)
        assertBigInt("Last received server position", lastReceivedServerPosition, 0n, MAX_UINT_63)
        assertBigInt("First available client position", firstAvailableClientPosition, 0n, MAX_UINT_63)
        assertInteger("Major version", majorVersion, 0, 0xffff)
        assertInteger("Minor version", minorVersion, 0, 0xffff)
    }

    /**
     * Deserializes a `ResumeFrame` from the binary stream.
     *
     * @param {Header} _ - The frame header (must be type `RESUME`).
     * @param {ByteReader} reader - Binary reader positioned at the body.
     * @param {MimeType} __ - Ignored metadata type (no metadata in RESUME).
     * @param {MimeType} ___ - Ignored payload type (no payload in RESUME).
     * @returns {ResumeFrame} A parsed `ResumeFrame` instance.
     */
    public static from(_: Header, reader: ByteReader, __: MimeType, ___: MimeType): ResumeFrame {
        const major = reader.i16()
        const minor = reader.i16()
        const resumeToken = decodeResumeToken(reader.viewBytes(reader.i16()))
        const lastReceivedServerPosition = reader.i64()
        const firstAvailableClientPosition = reader.i64()
        return new ResumeFrame(resumeToken, lastReceivedServerPosition, firstAvailableClientPosition, major, minor)
    }

    /**
     * Writes the `ResumeFrame` to the binary stream.
     *
     * Format includes protocol version, resume token, and position fields.
     *
     * @param {ByteWriter} writer - Writer used to serialize the frame body.
     */
    protected write(writer: ByteWriter) {
        writer.i16(this.majorVersion)
        writer.i16(this.minorVersion)
        const resumeToken = resumeTokenBytes(this.resumeToken)
        writer.i16(resumeToken.length)
        writer.write(resumeToken)
        writer.i63(this.lastReceivedServerPosition)
        writer.i63(this.firstAvailableClientPosition)
    }

    /**
     * Resume frames must never be ignored.
     *
     * @returns {false}
     */
    public override canBeIgnored(): boolean {
        return false;
    }

    /**
     * Resume frames never contain metadata.
     *
     * @returns {false}
     */
    public override hasMetadata(): boolean {
        return false;
    }
}
