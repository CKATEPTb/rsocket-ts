import type {ByteReader, ByteWriter} from "bebyte";
import {Frame} from "@/frame/Frame";
import {FrameType} from "@/frame/FrameType";
import {Header} from "@/frame/context/Header";
import {MimeType} from "@/mimetype/MimeType";
import {assertInteger, MAX_UINT_31} from "@/utils";

/**
 * ### REQUEST_N Frame (0x08)
 *
 * Frame Contents
 *
 * ```
 *      0                   1                   2                   3
 *      0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
 *     +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 *     |0|                         Stream ID                           |
 *     +-----------+-+-+---------------+-------------------------------+
 *     |Frame Type |0|0|     Flags     |
 *     +-------------------------------+-------------------------------+
 *     |0|                         Request N                           |
 *     +---------------------------------------------------------------+
 * ```
 *
 * * [__Frame Type__: (6 bits) 0x08]{@link FrameType#REQUEST_N}
 * * __Request N__: (31 bits = max value 2^31-1 = 2,147,483,647) Unsigned 31-bit integer representing the number of items to request. Value MUST be > 0.
 *
 * See Flow Control: Reactive Streams Semantics for more information on RequestN behavior.
 *
 * @description Request N: Request N more items with Reactive Streams semantics.
 * @see [Official documentation]{@link https://github.com/rsocket/rsocket/blob/master/Protocol.md#frame-request-n}
 */
export class RequestNFrame extends Frame {
    /**
     * Constructs a `RequestNFrame` instance.
     *
     * @param {number} streamId - The ID of the stream to request more items on.
     * @param {number} request - The number of additional items to request (must be > 0).
     */
    public constructor(
        streamId: number,
        public readonly request: number,
    ) {
        super(FrameType.REQUEST_N, streamId);
        assertInteger("Request N", request, 1, MAX_UINT_31)
    }

    /**
     * Parses a `RequestNFrame` from a binary stream.
     *
     * @param {Header} header - Frame header (should be of type `REQUEST_N`).
     * @param {ByteReader} reader - Reader positioned at the body.
     * @param {MimeType} _ - Ignored metadata type.
     * @param {MimeType} __ - Ignored payload type.
     * @returns {RequestNFrame} Parsed frame instance.
     */
    public static from(header: Header, reader: ByteReader, _: MimeType, __: MimeType): RequestNFrame {
        return new RequestNFrame(
            header.streamId,
            reader.i32()
        )
    }

    /**
     * Writes the body of the `RequestNFrame` (a single 31-bit integer).
     *
     * @param {ByteWriter} writer - Binary writer to serialize the frame.
     */
    protected write(writer: ByteWriter) {
        writer.i31(this.request)
    }

    /**
     * Indicates that this frame must never be ignored.
     *
     * @returns {false}
     */
    public override canBeIgnored(): boolean {
        return false;
    }

    /**
     * Indicates that this frame does not contain metadata.
     *
     * @returns {false}
     */
    public override hasMetadata(): boolean {
        return false;
    }
}
