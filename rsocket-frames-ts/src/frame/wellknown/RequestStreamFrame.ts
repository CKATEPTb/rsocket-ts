import type {ByteReader, ByteWriter} from "bebyte";
import {Frame} from "@/frame/Frame";
import {FrameType} from "@/frame/FrameType";
import {RequestStreamFlag} from "@/frame/FrameFlag";
import {Payload} from "@/frame/context/Payload";
import {Header} from "@/frame/context/Header";
import {MimeType} from "@/mimetype/MimeType";
import {Metadata} from "@/frame/context/Metadata";
import {assertInteger, MAX_UINT_31} from "@/utils";

/**
 *
 * ### REQUEST_STREAM Frame (0x06)
 *
 * Frame Contents
 *
 * ```
 *      0                   1                   2                   3
 *      0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
 *     +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 *     |0|                         Stream ID                           |
 *     +-----------+-+-+-+-------------+-------------------------------+
 *     |Frame Type |0|M|F|    Flags    |
 *     +-------------------------------+-------------------------------+
 *     |0|                    Initial Request N                        |
 *     +---------------------------------------------------------------+
 *                           Metadata & Request Data
 * ```
 *
 * * [__Frame Type__: (6 bits) 0x06]{@link FrameType#REQUEST_STREAM}
 * * [__Flags__: (10 bits)]{@link RequestStreamFlag}
 *     * (__M__)etadata: Metadata present
 *     * (__F__)ollows: More fragments follow this fragment.
 * * __Initial Request N__: (31 bits = max value 2^31-1 = 2,147,483,647) Unsigned 31-bit integer representing the initial number of items to request. Value MUST be > 0.
 * * __Request Data__: identification of the service being requested along with parameters for the request.
 *
 * See [Flow Control: Reactive Streams Semantics](#flow-control-reactive-streams) for more information on RequestN behavior.
 *
 * @description Request a completable stream.
 * @see [Official documentation]{@link https://github.com/rsocket/rsocket/blob/master/Protocol.md#frame-request-stream}
 */
export class RequestStreamFrame extends Frame {
    /**
     * Creates a `REQUEST_STREAM` frame.
     *
     * @param {number} streamId - The unique stream identifier.
     * @param {RequestStreamFlag} flags - Frame flags (e.g., METADATA, FOLLOWS).
     * @param {number} request - Initial number of items requested (must be > 0).
     * @param {Metadata<any>} [metadata] - Optional metadata block.
     * @param {Payload<any>} [payload] - Optional payload block.
     */
    public constructor(
        streamId: number,
        flags: RequestStreamFlag,
        public readonly request: number,
        metadata?: Metadata<any>,
        payload?: Payload<any>
    ) {
        super(FrameType.REQUEST_STREAM, streamId, flags, metadata, payload);
        assertInteger("Initial request N", request, 1, MAX_UINT_31)
    }

    /**
     * Parses a `REQUEST_STREAM` frame from binary.
     *
     * @param {Header} header - Frame header (must be type `REQUEST_STREAM`).
     * @param {ByteReader} reader - Reader positioned at frame body.
     * @param {MimeType<any>} metadataType - MIME type for decoding metadata.
     * @param {MimeType<any>} payloadType - MIME type for decoding payload.
     * @returns {RequestStreamFrame} The parsed frame instance.
     */
    public static from(header: Header, reader: ByteReader, metadataType: MimeType, payloadType: MimeType): RequestStreamFrame {
        return new RequestStreamFrame(
            header.streamId,
            header.flags,
            reader.i32(),
            header.isFlagSet(RequestStreamFlag.METADATA) ? metadataType.toMetadata(reader) : undefined,
            payloadType.toPayload(reader)
        )
    }

    /**
     * Serializes the initial request count.
     *
     * @param {ByteWriter} writer - Writer to output the frame body.
     */
    protected write(writer: ByteWriter) {
        writer.i31(this.request)
    }

    /**
     * Checks whether a specific flag is set.
     *
     * @param {RequestStreamFlag} flag - The flag to test.
     * @returns {boolean} True if the flag is set.
     */
    public override isFlagSet(flag: RequestStreamFlag): boolean {
        return super.isFlagSet(flag)
    }

    /**
     * Indicates whether this frame may be safely ignored.
     * Always returns `false`.
     *
     * @returns {false}
     */
    public override canBeIgnored(): boolean {
        return false;
    }

    /**
     * Returns `true` if the frame has the `FOLLOWS` flag set, indicating
     * that additional fragments follow this one.
     *
     * @returns {boolean} `true` if the frame is fragmented.
     */
    public hasFollows(): boolean {
        return this.isFlagSet(RequestStreamFlag.FOLLOWS)
    }
}
