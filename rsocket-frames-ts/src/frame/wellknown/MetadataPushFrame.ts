import type {ByteReader, ByteWriter} from "bebyte";
import {Frame} from "@/frame/Frame";
import {FrameType} from "@/frame/FrameType";
import {FrameFlag} from "@/frame/FrameFlag";
import {Header} from "@/frame/context/Header";
import {MimeType} from "@/mimetype/MimeType";
import {Metadata} from "@/frame/context/Metadata";

/**
 * ### METADATA_PUSH Frame (0x0C)
 *
 * A Metadata Push frame can be used to send asynchronous metadata notifications from a Requester or
 * Responder to its peer.
 *
 * METADATA_PUSH frames MUST always use Stream ID 0 as they pertain to the Connection.
 *
 * Metadata tied to a particular stream uses the individual Payload frame Metadata flag.
 *
 * Frame Contents
 *
 * ```
 *      0                   1                   2                   3
 *      0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
 *     +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 *     |0|                       Stream ID = 0                         |
 *     +-----------+-+-+---------------+-------------------------------+
 *     |Frame Type |0|1|     Flags     |
 *     +-------------------------------+-------------------------------+
 *                                 Metadata
 * ```
 *
 * * [__Frame Type__: (6 bits) 0x0C]{@link FrameType#METADATA_PUSH}
 *
 * This frame only supports Metadata, so the Metadata Length header MUST NOT be included.
 *
 * @description  Metadata: Asynchronous Metadata frame
 * @see [Official documentation]{@link https://github.com/rsocket/rsocket/blob/master/Protocol.md#frame-metadata-push}
 */
export class MetadataPushFrame extends Frame {
    /**
     * Constructs a new `MetadataPushFrame` with given metadata.
     *
     * @param {Metadata<any>} metadata - The metadata to push.
     * @param streamId Wire stream ID. Outgoing frames should keep the default `0`.
     */
    public constructor(metadata: Metadata<any>, streamId = 0) {
        super(FrameType.METADATA_PUSH, streamId, FrameFlag.METADATA, metadata, undefined, false);
        if (metadata === undefined) {
            throw new TypeError("METADATA_PUSH requires metadata");
        }
    }


    /**
     * Deserializes a `MetadataPushFrame` from a byte stream.
     *
     * @param {Header} _ - Frame header (unused, must be `FrameType.METADATA_PUSH`).
     * @param {ByteReader} reader - Reader positioned at metadata body.
     * @param {MimeType} metadataType - MIME type for decoding metadata.
     * @param {MimeType} __ - Payload type (ignored).
     * @returns {MetadataPushFrame} Parsed frame instance.
     */
    public static from(header: Header, reader: ByteReader, metadataType: MimeType, __: MimeType): MetadataPushFrame {
        if (!header.isFlagSet(FrameFlag.METADATA)) {
            throw new RangeError("METADATA_PUSH must set the METADATA flag");
        }
        return new MetadataPushFrame(metadataType.toMetadata(reader, false), header.streamId)
    }

    /**
     * Overrides the `write` method to disable metadata length encoding.
     * This frame must write raw metadata only, without a 24-bit length prefix.
     *
     * @param {ByteWriter} _ - Writer for serialization (not used here).
     */
    protected write(_: ByteWriter) {
    }

    /**
     * `METADATA_PUSH` frames must never be ignored.
     *
     * @returns {false}
     */
    public override canBeIgnored(): boolean {
        return false
    }

    /**
     * Always returns `true`, since this frame only carries metadata.
     *
     * @returns {true}
     */
    public override hasMetadata(): boolean {
        return true
    }
}
