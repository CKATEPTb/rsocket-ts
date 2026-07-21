import type {ByteReader, ByteWriter} from "bebyte";
import {Frame} from "@/frame/Frame";
import {FrameType} from "@/frame/FrameType";
import {Payload} from "@/frame/context/Payload";
import {Header} from "@/frame/context/Header";
import {KeepaliveFlag} from "@/frame/FrameFlag";
import {MimeType} from "@/mimetype/MimeType";
import {assertBigInt, MAX_UINT_63} from "@/utils";

/**
 * ### KEEPALIVE Frame (0x03)
 *
 * KEEPALIVE frames MUST always use Stream ID 0 as they pertain to the Connection.
 *
 * KEEPALIVE frames MUST be initiated by the client and sent periodically with the (__R__)espond flag set.
 *
 * KEEPALIVE frames MAY be initiated by the server and sent upon application request with the (__R__)espond flag set.
 *
 * Reception of a KEEPALIVE frame with the (__R__)espond flag set MUST cause a client or server to send
 * back a KEEPALIVE with the (__R__)espond flag __NOT__ set. The data in the received KEEPALIVE MUST be
 * echoed back in the generated KEEPALIVE.
 *
 * Reception of a KEEPALIVE by a server indicates to the server that the client is alive.
 *
 * Reception of a KEEPALIVE by a client indicates to the client that the server is alive.
 *
 * Frame Contents
 *
 * ```
 *      0                   1                   2                   3
 *      0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
 *     +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 *     |0|                       Stream ID = 0                         |
 *     +-----------+-+-+-+-------------+-------------------------------+
 *     |Frame Type |0|0|R|    Flags    |
 *     +-----------+-+-+-+-------------+-------------------------------+
 *     |0|                  Last Received Position                     |
 *     +                                                               +
 *     |                                                               |
 *     +---------------------------------------------------------------+
 *                                   Data
 * ```
 *
 * * [__Frame Type__: (6 bits) 0x03]{@link FrameType#KEEPALIVE}
 * * [__Flags__: (10 bits)]{@link KeepaliveFlag}
 *      * (__R__)espond with KEEPALIVE or not
 * * __Last Received Position__: (63 bits = max value 2^63-1) Unsigned 63-bit long of Resume Last Received Position. Value MUST be > 0. (optional. Set to all 0s when not supported.)
 * * __Data__: Data attached to a KEEPALIVE.
 *
 * @description Keepalive: Connection keepalive.
 * @see [Official documentation]{@link https://github.com/rsocket/rsocket/blob/master/Protocol.md#frame-keepalive}
 */
export class KeepaliveFrame extends Frame {
    /**
     * Constructs a new `KeepaliveFrame` instance.
     *
     * @param {KeepaliveFlag} flags - Indicates if a response is required (`RESPOND`).
     * @param {bigint} lastReceivedPosition - Resume position received (or `0n` if not used).
     * @param {Payload<any>} [payload] - Optional payload to echo (must be echoed back if RESPOND is set).
     */
    constructor(
        flags: KeepaliveFlag = KeepaliveFlag.NONE,
        public readonly lastReceivedPosition: bigint = 0n,
        payload?: Payload<any>
    ) {
        super(FrameType.KEEPALIVE, 0, flags, undefined, payload);
        assertBigInt("Last received position", lastReceivedPosition, 0n, MAX_UINT_63)
    }

    /**
     * Parses a `KeepaliveFrame` from a binary stream.
     *
     * @param {Header} header - Frame header (must be type `KEEPALIVE`).
     * @param {ByteReader} reader - Reader positioned at frame body.
     * @param {MimeType} _ - Metadata type (ignored).
     * @param {MimeType} payloadType - Used to deserialize payload.
     * @returns {KeepaliveFrame} Parsed frame.
     */
    public static from(header: Header, reader: ByteReader, _: MimeType, payloadType: MimeType): KeepaliveFrame {
        return new KeepaliveFrame(
            header.flags,
            reader.i64(),
            payloadType.toPayload(reader)
        )
    }

    /**
     * Parses a `KeepaliveFrame` from a binary stream.
     *
     * @param {Header} header - Frame header (must be type `KEEPALIVE`).
     * @param {ByteReader} reader - Reader positioned at frame body.
     * @param {MimeType} _ - Metadata type (ignored).
     * @param {MimeType} payloadType - Used to deserialize payload.
     * @returns {KeepaliveFrame} Parsed frame.
     */
    public override isFlagSet(flag: KeepaliveFlag): boolean {
        return super.isFlagSet(flag)
    }

    /**
     * Serializes the resume position into the stream.
     *
     * @param {ByteWriter} writer - Binary writer.
     */
    protected write(writer: ByteWriter): void {
        writer.i63(this.lastReceivedPosition)
    }

    /**
     * Indicates that this frame cannot be ignored.
     *
     * @returns {false}
     */
    public override canBeIgnored(): boolean {
        return false
    }

    /**
     * `KEEPALIVE` never contains metadata.
     *
     * @returns {false}
     */
    public override hasMetadata(): boolean {
        return false
    }

    /**
     * Returns `true` if the `RESPOND` flag is set, meaning the peer must reply.
     *
     * @returns {boolean}
     */
    public isRequireRespond() {
        return this.isFlagSet(KeepaliveFlag.RESPOND)
    }
}
