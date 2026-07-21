import {FrameType} from "@/frame/FrameType";
import type {ByteReader, ByteWriter} from "bebyte";
import {FrameFlag} from "@/frame/FrameFlag";
import {FrameWriter} from "@/frame/FrameWriter";
import {assertInteger, MAX_UINT_31} from "@/utils";
import {assertFixedConnectionStream} from "@/frame/validation";

/**
 * ### Frame Header Format
 *
 * RSocket frames begin with a RSocket Frame Header. The general layout is given below.
 *
 * ```
 *      0                   1                   2                   3
 *      0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
 *     +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 *     |0|                         Stream ID                           |
 *     +-----------+-+-+---------------+-------------------------------+
 *     |Frame Type |I|M|     Flags     |     Depends on Frame Type    ...
 *     +-------------------------------+
 * ```
 *
 * * __Stream ID__: (31 bits = max value 2^31-1 = 2,147,483,647) Unsigned 31-bit integer representing the stream Identifier for this frame or 0 to indicate the entire connection.
 *   * Transport protocols that include demultiplexing, such as HTTP/2, MAY omit the Stream ID field if all parties agree. The means of negotiation and agreement is left to the transport protocol.
 * * __Frame Type__: (6 bits = max value 63) Type of Frame.
 * * [__Flags__: (10 bits)]{@link FrameFlag} Any Flag bit not specifically indicated in the frame type should be set to 0 when sent and not interpreted on
 * reception. Flags generally depend on Frame Type, but all frame types MUST provide space for the following flags:
 *      * (__I__)gnore: Ignore frame if not understood
 *      * (__M__)etadata: Metadata present
 *
 * @see [Official documentation]{@link https://github.com/rsocket/rsocket/blob/master/Protocol.md#frame-header-format}
 */
export class Header extends FrameWriter {
    /**
     * Creates a new RSocket `Header` instance.
     *
     * @param {FrameType} frameType - Type of the frame (6 bits).
     * @param {number} streamId - Stream ID this frame is associated with (must be a 31-bit unsigned int).
     * @param {FrameFlag} flags - Bitmask of frame flags (10 bits max).
     */
    constructor(
        public readonly frameType: FrameType,
        public readonly streamId: number,
        public readonly flags: FrameFlag
    ) {
        super()
        FrameType.fromWireByte(frameType)
        assertInteger("Stream ID", streamId, 0, MAX_UINT_31)
        assertInteger("Frame flags", flags, 0, 0x03ff)
        assertFixedConnectionStream(frameType, streamId)
    }

    /**
     * Deserializes a `Header` from the binary reader.
     *
     * @param {ByteReader} reader - Byte stream reader positioned at the start of the frame.
     * @returns {Header} Parsed frame header.
     */
    public static from(reader: ByteReader): Header {
        const streamId = reader.i32()
        const frameTypeAndFlagsByte = reader.i16()
        const frameType = FrameType.fromWireByte(frameTypeAndFlagsByte >> 10)
        const flags = frameTypeAndFlagsByte & 0x03FF
        return new Header(frameType, streamId, flags)
    }

    /**
     * Checks if a specific flag is set.
     *
     * @param {FrameFlag} flag - Flag to check.
     * @returns {boolean} True if the flag is set.
     */
    public isFlagSet(flag: FrameFlag): boolean {
        return (this.flags & flag) === flag
    }

    /**
     * Serializes this header to the binary writer.
     *
     * @param {ByteWriter} writer - Writer to serialize to.
     */
    public write(writer: ByteWriter): void {
        writer.i31(this.streamId) // streamId
        writer.i16(
            this.frameType << 10 |
            this.flags
        )
    }
}
