import type {ByteReader, ByteWriter} from "bebyte";
import {Frame} from "@/frame/Frame";
import {FrameType} from "@/frame/FrameType";
import {FrameErrorCode} from "@/frame/FrameErrorCode";
import {Header} from "@/frame/context/Header";
import {FrameFlag} from "@/frame/FrameFlag";
import {MimeType} from "@/mimetype/MimeType";
import {Payload} from "@/frame/context/Payload";
import {assertErrorStreamScope} from "@/frame/validation";

/**
 * ### ERROR Frame (0x0B)
 *
 * Error frames are used for errors on individual requests/streams as well as connection errors and in response to SETUP frames.
 *
 * Frame Contents
 *
 * ```
 *      0                   1                   2                   3
 *      0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
 *     +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 *     |0|                         Stream ID                           |
 *     +-----------+-+-+---------------+-------------------------------+
 *     |Frame Type |0|0|      Flags    |
 *     +-----------+-+-+---------------+-------------------------------+
 *     |                          Error Code                           |
 *     +---------------------------------------------------------------+
 *                                Error Data
 * ```
 *
 * * [__Frame Type__: (6 bits) 0x0B]{@link FrameType#ERROR}
 * * __Error Code__: (32 bits = max value 2^31-1 = 2,147,483,647) Type of Error.
 *      * See list of valid Error Codes below.
 * * __Error Data__: includes Payload describing error information. Error Data SHOULD be a UTF-8 encoded string. The string MUST NOT be null terminated.
 *
 * A Stream ID of 0 means the error pertains to the connection., including connection establishment. A Stream ID > 0 means the error pertains to a given stream.
 *
 * The Error Data is typically an Exception message, but could include stringified stacktrace information if appropriate.
 *
 * #### [Error Codes]{@link FrameErrorCode}
 *
 * |  Type                          | Value      | Description |
 * |:-------------------------------|:-----------|:------------|
 * | __RESERVED__                   | 0x00000000 | __Reserved__ |
 * | __INVALID_SETUP__              | 0x00000001 | The Setup frame is invalid for the server (it could be that the client is too recent for the old server). Stream ID MUST be 0. |
 * | __UNSUPPORTED_SETUP__          | 0x00000002 | Some (or all) of the parameters specified by the client are unsupported by the server. Stream ID MUST be 0. |
 * | __REJECTED_SETUP__             | 0x00000003 | The server rejected the setup, it can specify the reason in the payload. Stream ID MUST be 0. |
 * | __REJECTED_RESUME__            | 0x00000004 | The server rejected the resume, it can specify the reason in the payload. Stream ID MUST be 0. |
 * | __CONNECTION_ERROR__           | 0x00000101 | The connection is being terminated. Stream ID MUST be 0. Sender or Receiver of this frame MAY close the connection immediately without waiting for outstanding streams to terminate.|
 * | __CONNECTION_CLOSE__           | 0x00000102 | The connection is being terminated. Stream ID MUST be 0. Sender or Receiver of this frame MUST wait for outstanding streams to terminate before closing the connection. New requests MAY not be accepted.|
 * | __APPLICATION_ERROR__          | 0x00000201 | Application layer logic generating a Reactive Streams _onError_ event. Stream ID MUST be > 0. |
 * | __REJECTED__                   | 0x00000202 | Despite being a valid request, the Responder decided to reject it. The Responder guarantees that it didn't process the request. The reason for the rejection is explained in the Error Data section. Stream ID MUST be > 0. |
 * | __CANCELED__                   | 0x00000203 | The Responder canceled the request but may have started processing it (similar to REJECTED but doesn't guarantee lack of side-effects). Stream ID MUST be > 0. |
 * | __INVALID__                    | 0x00000204 | The request is invalid. Stream ID MUST be > 0. |
 * | __RESERVED__                   | 0xFFFFFFFF | __Reserved for Extension Use__ |
 *
 * __NOTE__: Unsed values in the range of 0x0001 to 0x00300 are reserved for future protocol use. Values in the range of 0x00301 to 0xFFFFFFFE are reserved for application layer errors.
 *
 * When this document refers to a specific Error Code as a frame, it uses this pattern: ERROR[error_code] or ERROR[error_code|error_code]
 *
 * For example:
 *
 * - ERROR[INVALID_SETUP] means the ERROR frame with the INVALID_SETUP code
 * - ERROR[REJECTED] means the ERROR frame with the REJECTED code
 * - ERROR[CONNECTION_ERROR|REJECTED_RESUME] means the ERROR frame with either the CONNECTION_ERROR or REJECTED_RESUME code
 *
 * @description Error at connection or application level.
 * @see [Official documentation]{@link https://github.com/rsocket/rsocket/blob/master/Protocol.md#frame-error}
 */
export class ErrorFrame extends Frame {
    /**
     * Creates an `ErrorFrame` instance.
     *
     * @param {number} streamId - The stream ID (0 for connection-level errors).
     * @param {FrameErrorCode} code - The specific error code.
     * @param {Payload<any>} [payload] - Optional error payload (usually UTF-8 message).
     */
    constructor(
        streamId: number,
        public readonly code: FrameErrorCode,
        payload?: Payload<any>
    ) {
        super(FrameType.ERROR, streamId, FrameFlag.NONE, undefined, payload);
        FrameErrorCode.fromByte(code);
        assertErrorStreamScope(code, streamId);
    }

    /**
     * Parses an `ErrorFrame` from binary data.
     *
     * @param {Header} header - The frame header.
     * @param {ByteReader} reader - Reader positioned at error code.
     * @param {MimeType} _ - Metadata MIME type (ignored).
     * @param {MimeType} payloadType - Used to decode the error payload.
     * @returns {ErrorFrame} The parsed error frame.
     */
    public static from(header: Header, reader: ByteReader, _: MimeType, payloadType: MimeType): ErrorFrame {
        return new ErrorFrame(
            header.streamId,
            FrameErrorCode.fromByte(reader.i32()),
            payloadType.toPayload(reader)
        )
    }

    /**
     * Serializes the frame body (error code + optional payload).
     *
     * @param {ByteWriter} writer - Writer for binary serialization.
     */
    protected write(writer: ByteWriter): void {
        writer.i32(this.code)
    }

    /**
     * `ERROR` frames must not be ignored.
     *
     * @returns {false}
     */
    public override canBeIgnored(): boolean {
        return false
    }

    /**
     * `ERROR` frames never carry metadata.
     *
     * @returns {false}
     */
    public override hasMetadata(): boolean {
        return false
    }
}
