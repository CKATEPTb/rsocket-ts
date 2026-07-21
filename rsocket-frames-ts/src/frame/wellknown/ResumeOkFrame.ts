import type {ByteReader, ByteWriter} from "bebyte";
import {Frame} from "@/frame/Frame";
import {FrameType} from "@/frame/FrameType";
import {Header} from "@/frame/context/Header";
import {MimeType} from "@/mimetype/MimeType";
import {assertBigInt, MAX_UINT_63} from "@/utils";

/**
 * #### RESUME_OK Frame (0x0E)
 *
 * The general format for a Resume OK frame is given below.
 *
 * RESUME OK frames MUST always use Stream ID 0 as they pertain to the connection.
 *
 * ```
 *      0                   1                   2                   3
 *      0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
 *     +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 *     |0|                       Stream ID = 0                         |
 *     +-----------+-+-+---------------+-------------------------------+
 *     |Frame Type |0|0|    Flags      |
 *     +-------------------------------+-------------------------------+
 *     |0|                                                             |
 *     +               Last Received Client Position                   +
 *     |                                                               |
 *     +---------------------------------------------------------------+
 * ```
 *
 * * [__Frame Type__: (6 bits) 0x0E]{@link FrameType#RESUME_OK}
 * * __Last Received Client Position__: (63 bits = max value 2^63-1) Unsigned 63-bit long of the last implied position the server received from the client.
 *
 * @description Sent in response to a RESUME if resuming operation possible (optional)
 * @see [Official documentation]{@link https://github.com/rsocket/rsocket/blob/master/Protocol.md#frame-resume-ok}
 */
export class ResumeOkFrame extends Frame {
    /**
     * @param {bigint} lastReceivedClientPosition - The last position (in bytes) the server received from the client before disconnection.
     */
    public constructor(
        public readonly lastReceivedClientPosition: bigint
    ) {
        super(FrameType.RESUME_OK, 0);
        assertBigInt("Last received client position", lastReceivedClientPosition, 0n, MAX_UINT_63)
    }

    /**
     * Creates a `ResumeOkFrame` instance from a binary stream.
     *
     * @param {Header} _ - Parsed header (unused, must be type RESUME_OK).
     * @param {ByteReader} reader - Binary reader positioned at frame body.
     * @param {MimeType} __ - Metadata MIME type (ignored for this frame).
     * @param {MimeType} ___ - Payload MIME type (ignored for this frame).
     * @returns {ResumeOkFrame} Parsed frame instance.
     */
    public static from(_: Header, reader: ByteReader, __: MimeType, ___: MimeType): ResumeOkFrame {
        return new ResumeOkFrame(reader.i64())
    }

    /**
     * Serializes the frame body into a binary writer.
     *
     * Writes a single 63-bit unsigned integer representing the client's last acknowledged position.
     *
     * @param {ByteWriter} writer - Writer to output binary data.
     */
    protected write(writer: ByteWriter) {
        writer.i63(this.lastReceivedClientPosition)
    }

    /**
     * `RESUME_OK` frames must never be ignored.
     *
     * @returns {false}
     */
    public override canBeIgnored(): boolean {
        return false;
    }

    /**
     * `RESUME_OK` frames do not contain metadata.
     *
     * @returns {false}
     */
    public override hasMetadata(): boolean {
        return false;
    }
}
