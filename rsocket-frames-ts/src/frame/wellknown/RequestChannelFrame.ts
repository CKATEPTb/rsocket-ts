import type {ByteReader, ByteWriter} from "bebyte";
import {Frame} from "@/frame/Frame";
import {FrameType} from "@/frame/FrameType";
import {RequestChannelFlag} from "@/frame/FrameFlag";
import {Payload} from "@/frame/context/Payload";
import {Header} from "@/frame/context/Header";
import {MimeType} from "@/mimetype/MimeType";
import {Metadata} from "@/frame/context/Metadata";
import {assertInteger, MAX_UINT_31} from "@/utils";

/**
 * ### REQUEST_CHANNEL Frame (0x07)
 *
 * Frame Contents
 *
 * ```
 *      0                   1                   2                   3
 *      0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
 *     +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 *     |0|                         Stream ID                           |
 *     +-----------+-+-+-+-+-----------+-------------------------------+
 *     |Frame Type |0|M|F|C|  Flags    |
 *     +-------------------------------+-------------------------------+
 *     |0|                    Initial Request N                        |
 *     +---------------------------------------------------------------+
 *                            Metadata & Request Data
 * ```
 *
 * * [__Frame Type__: (6 bits) 0x07]{@link FrameType#REQUEST_CHANNEL}
 * * [__Flags__: (10 bits)]{@link RequestChannelFlag}
 *     * (__M__)etadata: Metadata present
 *     * (__F__)ollows: More fragments follow this fragment.
 *     * (__C__)omplete: bit to indicate stream completion.
 *       * If set, `onComplete()` or equivalent will be invoked on Subscriber/Observer.
 * * __Initial Request N__: (31 bits = max value 2^31-1 = 2,147,483,647) Unsigned 31-bit integer representing the initial request N value for channel. Value MUST be > 0.
 * * __Request Data__: identification of the service being requested along with parameters for the request.
 *
 * @description Request Channel: Request a completable stream in both directions.
 * @see [Official documentation]{@link https://github.com/rsocket/rsocket/blob/master/Protocol.md#frame-request-channel}
 */
export class RequestChannelFrame extends Frame {
    /**
     * Constructs a new `RequestChannelFrame` instance.
     *
     * @param {number} streamId - Unique stream ID.
     * @param {RequestChannelFlag} flags - Flags (e.g. METADATA, FOLLOWS, COMPLETE).
     * @param {number} request - Initial number of items requested (must be > 0).
     * @param {Metadata<any>} [metadata] - Optional metadata block.
     * @param {Payload<any>} [payload] - Optional payload block.
     */
    public constructor(
        streamId: number,
        flags: RequestChannelFlag,
        public readonly request: number,
        metadata?: Metadata<any>,
        payload?: Payload<any>
    ) {
        super(FrameType.REQUEST_CHANNEL, streamId, flags, metadata, payload);
        assertInteger("Initial request N", request, 1, MAX_UINT_31)
    }

    /**
     * Parses a `RequestChannelFrame` from a binary stream.
     *
     * @param {Header} header - Parsed frame header.
     * @param {ByteReader} reader - Stream reader positioned at frame body.
     * @param {MimeType} metadataType - MIME type for decoding metadata.
     * @param {MimeType} payloadType - MIME type for decoding payload.
     * @returns {RequestChannelFrame} Parsed frame instance.
     */
    public static from(header: Header, reader: ByteReader, metadataType: MimeType, payloadType: MimeType): RequestChannelFrame {
        return new RequestChannelFrame(
            header.streamId,
            header.flags,
            reader.i32(),
            header.isFlagSet(RequestChannelFlag.METADATA) ? metadataType.toMetadata(reader) : undefined,
            payloadType.toPayload(reader)
        )
    }

    /**
     * Serializes the initial `requestN` value.
     *
     * @param {ByteWriter} writer - Writer for binary serialization.
     */
    protected write(writer: ByteWriter) {
        writer.i31(this.request)
    }


    /**
     * Checks if a specific flag is set on this frame.
     *
     * @param {RequestChannelFlag} flag - The flag to check.
     * @returns {boolean} `true` if set.
     */
    public override isFlagSet(flag: RequestChannelFlag): boolean {
        return super.isFlagSet(flag)
    }

    /**
     * `REQUEST_CHANNEL` frames must never be ignored.
     *
     * @returns {false}
     */
    public override canBeIgnored(): boolean {
        return false;
    }

    /**
     * Returns `true` if the frame is fragmented (i.e. `FOLLOWS` flag is set).
     */
    public hasFollows() {
        return this.isFlagSet(RequestChannelFlag.FOLLOWS)
    }

    /**
     * Returns `true` if the stream should be marked as complete after the initial payload.
     */
    public isComplete() {
        return this.isFlagSet(RequestChannelFlag.COMPLETE)
    }
}
