import type {ByteReader, ByteWriter} from "bebyte";
import {Frame} from "@/frame/Frame";
import {FrameType} from "@/frame/FrameType";
import {Header} from "@/frame/context/Header";
import {ExtensionFlag} from "@/frame/FrameFlag";
import {MimeType} from "@/mimetype/MimeType";
import {Metadata} from "@/frame/context/Metadata";
import {Payload} from "@/frame/context/Payload";
import {assertInteger, MAX_UINT_31} from "@/utils";

/**
 * ### EXT (Extension) Frame (0x3F)
 *
 * The general format for an extension frame is given below.
 *
 * ```
 *      0                   1                   2                   3
 *      0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
 *     +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 *     |0|                         Stream ID                           |
 *     +-----------+-+-+---------------+-------------------------------+
 *     |Frame Type |I|M|1|2|3|4|5|6|7|8|
 *     +-------------------------------+-------------------------------+
 *     |0|                      Extended Type                          |
 *     +---------------------------------------------------------------+
 *                         Depends on Extended Type...
 * ```
 *
 * * [__Frame Type__: (6 bits) 0x3F]{@link FrameType#EXT}
 * * [__Flags__: (10 bits)]{@link ExtensionFlag}
 *     * (__I__)gnore: Can the frame be ignored if not understood?
 *     * (__M__)etadata: Ext  Present.
 *     * EXT_(__1__): Flag 1 Present.
 *     * EXT_(__2__): Flag 2 Present.
 *     * EXT_(__3__): Flag 3 Present.
 *     * EXT_(__4__): Flag 4 Present.
 *     * EXT_(__5__): Flag 5 Present.
 *     * EXT_(__6__): Flag 6 Present.
 *     * EXT_(__7__): Flag 7 Present.
 *     * EXT_(__8__): Flag 8 Present.
 * * __Extended Type__: (31 bits = max value 2^31-1 = 2,147,483,647) Unsigned 31-bit integer of Extended type information. Value MUST be > 0.
 *
 * @description Used To Extend more frame types as well as extensions.
 * @see [Official documentation]{@link https://github.com/rsocket/rsocket/blob/master/Protocol.md#frame-ext}
 */
export class ExtensionFrame extends Frame {
    /**
     * Constructs an `ExtensionFrame` instance.
     *
     * @param {number} streamId - Associated stream ID (`0` for connection-level extensions).
     * @param {ExtensionFlag} flags - Extension and metadata flags.
     * @param {number} extendedType - Custom extension type (must be > 0).
     * @param {Metadata<any>} [metadata] - Optional metadata block.
     * @param {Payload<any>} [payload] - Optional payload block.
     */
    public constructor(
        streamId: number,
        flags: ExtensionFlag,
        public readonly extendedType: number,
        metadata?: Metadata<any>,
        payload?: Payload<any>
    ) {
        super(FrameType.EXT, streamId, flags, metadata, payload);
        assertInteger("Extended type", extendedType, 1, MAX_UINT_31)
    }

    /**
     * Parses an `ExtensionFrame` from binary data.
     *
     * @param {Header} header - Frame header (must be `FrameType.EXT`).
     * @param {ByteReader} reader - Byte stream reader.
     * @param {MimeType} metadataType - Metadata MIME type for decoding.
     * @param {MimeType} payloadType - Payload MIME type for decoding.
     * @returns {ExtensionFrame} Parsed extension frame instance.
     */
    public static from(header: Header, reader: ByteReader, metadataType: MimeType, payloadType: MimeType): ExtensionFrame {
        return new ExtensionFrame(
            header.streamId,
            header.flags,
            reader.i32(),
            header.isFlagSet(ExtensionFlag.METADATA) ? metadataType.toMetadata(reader) : undefined,
            payloadType.toPayload(reader)
        )
    }

    /**
     * Checks whether a specific flag is set on this extension frame.
     *
     * @param {ExtensionFlag} flag - The flag to check.
     * @returns {boolean} `true` if the flag is present.
     */
    public override isFlagSet(flag: ExtensionFlag): boolean {
        return super.isFlagSet(flag)
    }

    /**
     * Serializes the extension frame (writes extended type and optional metadata/payload).
     *
     * @param {ByteWriter} writer - Writer for binary serialization.
     */
    protected write(writer: ByteWriter) {
        writer.i31(this.extendedType)
    }
}
