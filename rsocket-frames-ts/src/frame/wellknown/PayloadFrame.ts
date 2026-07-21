import type {ByteReader, ByteWriter} from "bebyte";
import {Frame} from "@/frame/Frame";
import {FrameType} from "@/frame/FrameType";
import {PayloadFlag} from "@/frame/FrameFlag";
import {Header} from "@/frame/context/Header";
import {MimeType} from "@/mimetype/MimeType";
import {Metadata} from "@/frame/context/Metadata";
import {Payload} from "@/frame/context/Payload";

/**
 * ### PAYLOAD Frame (0x0A)
 *
 * Frame Contents
 *
 * ```
 *      0                   1                   2                   3
 *      0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
 *     +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 *     |0|                         Stream ID                           |
 *     +-----------+-+-+-+-+-+---------+-------------------------------+
 *     |Frame Type |0|M|F|C|N|  Flags  |
 *     +-------------------------------+-------------------------------+
 *                             Metadata & Data
 * ```
 *
 * * [__Frame Type__: (6 bits) 0x0A]{@link FrameType#PAYLOAD}
 * [__Flags__: (10 bits)]{@link PayloadFlag}
 *     * (__M__)etadata: Metadata Present.
 *     * (__F__)ollows: More fragments follow this fragment.
 *     * (__C__)omplete: bit to indicate stream completion.
 *        * If set, `onComplete()` or equivalent will be invoked on Subscriber/Observer.
 *     * (__N__)ext: bit to indicate Next (Payload Data and/or Metadata present).
 *        * If set, `onNext(Payload)` or equivalent will be invoked on Subscriber/Observer.
 * * __Payload Data__: payload for Reactive Streams onNext.
 *
 * Valid combinations of (C)omplete and (N)ext flags are:
 *
 * - Both (C)omplete and (N)ext set meaning PAYLOAD contains data and signals stream completion.
 *   - For example: An Observable stream receiving `onNext(payload)` followed by `onComplete()`.
 * - Just (C)omplete set meaning PAYLOAD contains no data and only signals stream completion.
 *   - For example: An Observable stream receiving `onComplete()`.
 * - Just (N)ext set meaning PAYLOAD contains data stream is NOT completed.
 *   - For example: An Observable stream receiving `onNext(payload)`.
 *
 * A PAYLOAD MUST NOT have both (C)complete and (N)ext empty (false).
 *
 * The reason for the (N)ext flag instead of just deriving from Data length being > 0 is that 0 length data can be considered a valid PAYLOAD resulting in a delivery to the application layer with a PAYLOAD containing 0 bytes of data.
 *
 * For example: An Observable stream receiving data via `onNext(payload)` where payload contains 0 bytes of data.
 *
 * @description Payload on a stream. For example, response to a request, or message on a channel.
 * @see [Official documentation]{@link https://github.com/rsocket/rsocket/blob/master/Protocol.md#frame-payload}
 */
export class PayloadFrame extends Frame {
    /**
     * Constructs a new `PayloadFrame`.
     *
     * @param {number} streamId - The stream ID this frame belongs to.
     * @param {PayloadFlag} flags - Flags (NEXT, COMPLETE, METADATA, FOLLOWS).
     * @param {Metadata<any>} [metadata] - Optional metadata.
     * @param {Payload<any>} [payload] - Optional payload.
     */
    public constructor(
        streamId: number,
        flags: PayloadFlag,
        metadata?: Metadata<any>,
        payload?: Payload<any>
    ) {
        super(FrameType.PAYLOAD, streamId, flags, metadata, payload);
    }

    /**
     * Parses a `PayloadFrame` from binary data.
     *
     * @param {Header} header - The frame header.
     * @param {ByteReader} reader - The byte reader positioned at the payload.
     * @param {MimeType} metadataType - Metadata MIME type.
     * @param {MimeType} payloadType - Payload MIME type.
     * @returns {PayloadFrame} The parsed payload frame.
     */
    public static from(header: Header, reader: ByteReader, metadataType: MimeType, payloadType: MimeType): PayloadFrame {
        return new PayloadFrame(
            header.streamId,
            header.flags,
            header.isFlagSet(PayloadFlag.METADATA) ? metadataType.toMetadata(reader) : undefined,
            payloadType.toPayload(reader)
        )
    }

    /**
     * Writes the frame-specific body to the writer.
     * PAYLOAD has no fixed header fields beyond metadata/payload.
     *
     * @param {ByteWriter} _ - Writer (unused here).
     */
    protected write(_: ByteWriter) {
    }

    /**
     * Checks if the specified flag is set.
     *
     * @param {PayloadFlag} flag - The flag to check.
     * @returns {boolean} `true` if the flag is set.
     */
    public override isFlagSet(flag: PayloadFlag): boolean {
        return super.isFlagSet(flag)
    }

    /**
     * Indicates that PAYLOAD frames must never be ignored.
     *
     * @returns {false}
     */
    public override canBeIgnored(): boolean {
        return false;
    }

    /**
     * Returns `true` if the `FOLLOWS` flag is set,
     * meaning more fragments are expected.
     */
    public hasFollows() {
        return this.isFlagSet(PayloadFlag.FOLLOWS)
    }

    /**
     * Returns `true` if this frame signals stream completion.
     */
    public isComplete() {
        return this.isFlagSet(PayloadFlag.COMPLETE)
    }

    /**
     * Returns `true` if this frame contains an application payload.
     */
    public isNext() {
        return this.isFlagSet(PayloadFlag.NEXT)
    }
}
