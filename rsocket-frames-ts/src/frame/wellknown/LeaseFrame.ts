import type {ByteReader, ByteWriter} from "bebyte";
import {Frame} from "@/frame/Frame";
import {FrameType} from "@/frame/FrameType";
import {FrameFlag} from "@/frame/FrameFlag";
import {Header} from "@/frame/context/Header";
import {MimeType} from "@/mimetype/MimeType";
import {Metadata} from "@/frame/context/Metadata";
import {assertInteger, MAX_UINT_31} from "@/utils";

/**
 * ### LEASE Frame (0x02)
 *
 * Lease frames MAY be sent by the client-side or server-side Responders and inform the
 * Requester that it may send Requests for a period of time and how many it may send during that duration.
 * See [Lease Semantics](#lease-semantics) for more information.
 *
 * The last received LEASE frame overrides all previous LEASE frame values.
 *
 * Lease frames MUST always use Stream ID 0 as they pertain to the Connection.
 *
 * Frame Contents
 *
 * ```
 *      0                   1                   2                   3
 *      0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
 *     +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 *     |0|                       Stream ID = 0                         |
 *     +-----------+-+-+---------------+-------------------------------+
 *     |Frame Type |0|M|     Flags     |
 *     +-----------+-+-+---------------+-------------------------------+
 *     |0|                       Time-To-Live                          |
 *     +---------------------------------------------------------------+
 *     |0|                     Number of Requests                      |
 *     +---------------------------------------------------------------+
 *                                 Metadata
 * ```
 *
 * * [__Frame Type__: (6 bits) 0x02]{@link FrameType#LEASE}
 * * [__Flags__: (10 bits)]{@link FrameFlag}
 *      * (__M__)etadata: Metadata present
 * * __Time-To-Live (TTL)__: (31 bits = max value 2^31-1 = 2,147,483,647) Unsigned 31-bit integer of Time (in milliseconds) for validity of LEASE from time of reception. Value MUST be > 0.
 * * __Number of Requests__: (31 bits = max value 2^31-1 = 2,147,483,647) Unsigned 31-bit integer of Number of Requests that may be sent until next LEASE. Value MUST be > 0.
 *
 * A Responder implementation MAY stop all further requests by sending a LEASE with a value of 0 for __Number of Requests__ or __Time-To-Live__.
 *
 * When a LEASE expires due to time, the value of the __Number of Requests__ that a Requester may make is implicitly 0.
 *
 * This frame only supports Metadata, so the Metadata Length header MUST NOT be included, even if the (M)etadata flag is set true.
 *
 * @description Sent by Responder to grant the ability to send requests.
 * @see [Official documentation]{@link https://github.com/rsocket/rsocket/blob/master/Protocol.md#frame-lease}
 */
export class LeaseFrame extends Frame {
    /**
     * Constructs a `LeaseFrame` instance.
     *
     * @param {number} ttl - Time-To-Live in milliseconds. Zero immediately revokes the current lease.
     * @param {number} requestLimit - Maximum request count. Zero prevents new requests until another lease arrives.
     * @param {Metadata<any>} [metadata] - Optional metadata (without length prefix).
     */
    public constructor(
        public readonly ttl: number,
        public readonly requestLimit: number,
        metadata?: Metadata<any>
    ) {
        super(FrameType.LEASE, 0, FrameFlag.NONE, metadata, undefined);
        assertInteger("Lease TTL", ttl, 0, MAX_UINT_31)
        assertInteger("Lease request limit", requestLimit, 0, MAX_UINT_31)
    }

    /**
     * Parses a `LeaseFrame` from a byte stream.
     *
     * @param {Header} header - Frame header (must have `Stream ID = 0`).
     * @param {ByteReader} reader - Reader positioned at TTL.
     * @param {MimeType} metadataType - MIME type to decode metadata.
     * @param {MimeType} _ - Payload MIME type (ignored).
     * @returns {LeaseFrame} Parsed instance.
     */
    public static from(header: Header, reader: ByteReader, metadataType: MimeType, _: MimeType): LeaseFrame {
        return new LeaseFrame(
            reader.i32(),
            reader.i32(),
            header.isFlagSet(FrameFlag.METADATA) ? metadataType.toMetadata(reader, false) : undefined
        )
    }

    /**
     * Writes the TTL and request limit, followed by optional metadata (without metadata length prefix).
     *
     * @param {ByteWriter} writer - Writer for binary serialization.
     */
    protected write(writer: ByteWriter) {
        writer.i31(this.ttl)
        writer.i31(this.requestLimit)
    }

    /**
     * `LEASE` frames must never be ignored, as they control permission to send requests.
     *
     * @returns {false}
     */
    public override canBeIgnored(): boolean {
        return false
    }
}
