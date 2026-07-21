import type {ByteReader, ByteWriter} from "bebyte";
import {Frame} from "@/frame/Frame";
import {FrameType} from "@/frame/FrameType";
import {Header} from "@/frame/context/Header";
import {MimeType} from "@/mimetype/MimeType";

/**
 * ### CANCEL Frame (0x09)
 * Frame Contents
 * ```
 *      0                   1                   2                   3
 *      0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
 *     +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 *     |0|                         Stream ID                           |
 *     +-----------+-+-+---------------+-------------------------------+
 *     |Frame Type |0|0|    Flags      |
 *     +-------------------------------+-------------------------------+
 * ```
 * * [__Frame Type__: (6 bits) 0x09]{@link FrameType#CANCEL}
 *
 * @description  Cancel outstanding request.
 * @see [Official documentation]{@link https://github.com/rsocket/rsocket/blob/master/Protocol.md#frame-cancel}
 */
export class CancelFrame extends Frame {
    /**
     * Constructs a `CancelFrame` for the given stream.
     *
     * @param {number} streamId - Stream ID to cancel (must be > 0).
     */
    public constructor(streamId: number) {
        super(FrameType.CANCEL, streamId);
    }

    /**
     * Parses a `CancelFrame` from the header.
     *
     * @param {Header} header - Frame header (must have type `CANCEL`).
     * @param {ByteReader} _ - Reader (ignored, as this frame has no body).
     * @param {MimeType} __ - Metadata type (unused).
     * @param {MimeType} ___ - Payload type (unused).
     * @returns {CancelFrame} Parsed frame.
     */
    public static from(header: Header, _: ByteReader, __: MimeType, ___: MimeType): CancelFrame {
        return new CancelFrame(header.streamId)
    }

    /**
     * Serializes the frame — does nothing as `CANCEL` has no body.
     *
     * @param {ByteWriter} _ - Writer (unused).
     */
    protected write(_: ByteWriter) {
        // Nobody to write.
    }

    /**
     * `CANCEL` frames must never be ignored.
     *
     * @returns {false}
     */
    public override canBeIgnored(): boolean {
        return false
    }

    /**
     * `CANCEL` frames never carry metadata.
     *
     * @returns {false}
     */
    public override hasMetadata(): boolean {
        return false
    }
}
