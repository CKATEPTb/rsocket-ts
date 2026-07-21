import type {ByteReader, ByteWriter} from "bebyte";
import {Frame} from "@/frame/Frame";
import {FrameType} from "@/frame/FrameType";
import {RequestResponseFlag} from "@/frame/FrameFlag";
import {Payload} from "@/frame/context/Payload";
import {Header} from "@/frame/context/Header";
import {MimeType} from "@/mimetype/MimeType";
import {Metadata} from "@/frame/context/Metadata";

/**
 * ### REQUEST_RESPONSE Frame (0x04)
 *
 * Frame Contents
 *
 * ```
 *      0                   1                   2                   3
 *      0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
 *     +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 *     |0|                         Stream ID                           |
 *     +-----------+-+-+-+-------------+-------------------------------+
 *     |Frame Type |0|M|F|     Flags   |
 *     +-------------------------------+
 *                          Metadata & Request Data
 * ```
 *
 * * [__Frame Type__: (6 bits) 0x04]{@link FrameType#REQUEST_RESPONSE}
 * * [__Flags__: (10 bits)]{@link RequestResponseFlag}
 *     * (__M__)etadata: Metadata present
 *     * (__F__)ollows: More fragments follow this fragment.
 * * __Request Data__: identification of the service being requested along with parameters for the request.
 *
 * @description Request single response.
 * @see [Official documentation]{@link https://github.com/rsocket/rsocket/blob/master/Protocol.md#frame-request-response}
 */
export class RequestResponseFrame extends Frame {
    /**
     * Constructs a new `RequestResponseFrame`.
     *
     * @param {number} streamId - The stream ID assigned to this request.
     * @param {RequestResponseFlag} flags - Flags indicating metadata and fragmentation.
     * @param {Metadata<any>} [metadata] - Optional metadata block.
     * @param {Payload<any>} [payload] - Optional payload block.
     */
    public constructor(
        streamId: number,
        flags: RequestResponseFlag,
        metadata?: Metadata<any>,
        payload?: Payload<any>
    ) {
        super(FrameType.REQUEST_RESPONSE, streamId, flags, metadata, payload);
    }

    /**
     * Parses a `RequestResponseFrame` from a byte stream.
     *
     * @param {Header} header - Frame header (should be of type `REQUEST_RESPONSE`).
     * @param {ByteReader} reader - Byte stream positioned at frame body.
     * @param {MimeType} metadataType - MIME type used for metadata decoding.
     * @param {MimeType} payloadType - MIME type used for payload decoding.
     * @returns {RequestResponseFrame} The parsed frame.
     */
    public static from(header: Header, reader: ByteReader, metadataType: MimeType, payloadType: MimeType): RequestResponseFrame {
        return new RequestResponseFrame(
            header.streamId,
            header.flags,
            header.isFlagSet(RequestResponseFlag.METADATA) ? metadataType.toMetadata(reader) : undefined,
            payloadType.toPayload(reader)
        )
    }

    /**
     * Serializes the frame body.
     *
     * This frame has no fixed header body fields, so this method is a no-op.
     *
     * @param {ByteWriter} _ - Unused writer.
     */
    protected write(_: ByteWriter) {
        // No fixed fields in body; metadata and payload handled separately.
    }

    /**
     * Checks if a specific flag is set.
     *
     * @param {RequestResponseFlag} flag - The flag to check.
     * @returns {boolean} `true` if the flag is present.
     */
    public override isFlagSet(flag: RequestResponseFlag): boolean {
        return super.isFlagSet(flag)
    }

    /**
     * `REQUEST_RESPONSE` frames must not be ignored.
     *
     * @returns {false}
     */
    public override canBeIgnored(): boolean {
        return false;
    }

    /**
     * Indicates whether this frame is fragmented.
     *
     * @returns {boolean} `true` if the `FOLLOWS` flag is set.
     */
    public hasFollows(): boolean {
        return this.isFlagSet(RequestResponseFlag.FOLLOWS)
    }
}
