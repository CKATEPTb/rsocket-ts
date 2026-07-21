import type {ByteReader, ByteWriter} from "bebyte";
import {Frame} from "@/frame/Frame";
import {FrameType} from "@/frame/FrameType";
import {FireAndForgetFlag} from "@/frame/FrameFlag";
import {Payload} from "@/frame/context/Payload";
import {Header} from "@/frame/context/Header";
import {MimeType} from "@/mimetype/MimeType";
import {Metadata} from "@/frame/context/Metadata";

/**
 * ### REQUEST_FNF (Fire-n-Forget) Frame (0x05)
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
 *     +-------------------------------+
 *                           Metadata & Request Data
 * ```
 *
 * * [__Frame Type__: (6 bits) 0x05]{@link FrameType#REQUEST_FNF}
 * * [__Flags__: (10 bits)]{@link FireAndForgetFlag}
 *     * (__M__)etadata: Metadata present
 *     * (__F__)ollows: More fragments follow this fragment.
 * * __Request Data__: identification of the service being requested along with parameters for the request.
 *
 * @description A single one-way message.
 * @see [Official documentation]{@link https://github.com/rsocket/rsocket/blob/master/Protocol.md#frame-fnf}
 */
export class RequestFireAndForgetFrame extends Frame {
    /**
     * Constructs a `REQUEST_FNF` frame instance.
     *
     * @param {number} streamId - The ID of the stream.
     * @param {FireAndForgetFlag} flags - Flags (e.g., METADATA, FOLLOWS).
     * @param {Metadata<any>} [metadata] - Optional metadata.
     * @param {Payload<any>} [payload] - Optional payload.
     */
    public constructor(
        streamId: number,
        flags: FireAndForgetFlag,
        metadata?: Metadata<any>,
        payload?: Payload<any>
    ) {
        super(FrameType.REQUEST_FNF, streamId, flags, metadata, payload);
    }


    /**
     * Deserializes a `REQUEST_FNF` frame from the byte stream.
     *
     * @param {Header} header - Frame header.
     * @param {ByteReader} reader - Byte reader positioned at frame body.
     * @param {MimeType<any>} metadataType - Metadata MIME type for decoding.
     * @param {MimeType<any>} payloadType - Payload MIME type for decoding.
     * @returns {RequestFireAndForgetFrame} Parsed frame.
     */
    public static from(header: Header, reader: ByteReader, metadataType: MimeType, payloadType: MimeType): RequestFireAndForgetFrame {
        return new RequestFireAndForgetFrame(
            header.streamId,
            header.flags,
            header.isFlagSet(FireAndForgetFlag.METADATA) ? metadataType.toMetadata(reader) : undefined,
            payloadType.toPayload(reader)
        )
    }

    /**
     * Serializes the frame-specific body.
     * This frame has no fixed fields; metadata and payload are written separately.
     *
     * @param {ByteWriter} _ - Writer (unused in this method).
     */
    protected write(_: ByteWriter) {
    }

    /**
     * Serializes the frame-specific body.
     * This frame has no fixed fields; metadata and payload are written separately.
     *
     * @param {ByteWriter} _ - Writer (unused in this method).
     */
    public override isFlagSet(flag: FireAndForgetFlag): boolean {
        return super.isFlagSet(flag)
    }

    /**
     * Serializes the frame-specific body.
     * This frame has no fixed fields; metadata and payload are written separately.
     *
     * @param {ByteWriter} _ - Writer (unused in this method).
     */
    public override canBeIgnored(): boolean {
        return false;
    }

    /**
     * Returns `true` if the frame is fragmented (i.e. `FOLLOWS` flag is set).
     *
     * @returns {boolean} `true` if additional fragments follow.
     */
    public hasFollows(): boolean {
        return this.isFlagSet(FireAndForgetFlag.FOLLOWS)
    }
}
