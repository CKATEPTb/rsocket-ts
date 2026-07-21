import type {ByteWriter} from "bebyte";
import type {MimeType} from "@/mimetype/MimeType";
import {FrameWriter} from "@/frame/FrameWriter";
import {byteView} from "@/binary";

/**
 * Represents a payload frame in the RSocket protocol.
 *
 * @template T - The type of the payload, defaults to `Uint8Array`.
 */
export class Payload<T = Uint8Array> extends FrameWriter {
    /**
     * Creates a new [Payload]{@link Payload} instance.
     *
     * @param mimeType - The MIME type indicating the format of the payload data.
     * @param payload Payload value or already encoded bytes.
     * @param encoded Existing wire bytes for an already decoded or eagerly encoded value.
     */
    public constructor(
        public readonly mimeType: MimeType<T>,
        public readonly payload: T,
        private readonly encoded?: Uint8Array
    ) {
        super()
    }

    /**
     * Converts the payload to a [Uint8Array]{@link Uint8Array}.
     *
     * @returns The payload data as a [Uint8Array]{@link Uint8Array}.
     */
    public toUint8Array(): Uint8Array {
        if (this.encoded !== undefined) return this.encoded;
        const bytes = byteView(this.payload);
        if (bytes === undefined) {
            throw new TypeError("Payload is not encoded; create it with its MIME type's toPayload() method")
        }
        return bytes
    }

    /**
     * Serializes the payload and writes it to the given [ByteWriter]{@link ByteWriter}.
     *
     * @param writer - The byte writer to which the payload will be written.
     */
    public write(writer: ByteWriter) {
        writer.write(this.toUint8Array())
    }
}
