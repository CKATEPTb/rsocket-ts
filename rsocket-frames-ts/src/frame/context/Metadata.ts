import type {ByteWriter} from "bebyte";
import type {MimeType} from "@/mimetype/MimeType";
import {FrameWriter} from "@/frame/FrameWriter";
import {byteView} from "@/binary";

/**
 * Represents a metadata frame in RSocket protocol.
 *
 * This class encapsulates metadata information, including its MIME type and payload,
 * and provides methods to serialize it into binary format for transmission.
 *
 * @template T - The type of the metadata payload, defaults to `Uint8Array`.
 */
export class Metadata<T = Uint8Array> extends FrameWriter {
    /**
     * Creates a new [Metadata]{@link Metadata} instance.
     *
     * @param mimeType - The MIME type describing the format of the metadata.
     * @param payload Metadata value or already encoded bytes.
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
     * Converts the metadata payload to a [Uint8Array]{@link Uint8Array}.
     *
     * @returns The metadata payload as a [Uint8Array]{@link Uint8Array}.
     */
    public toUint8Array(): Uint8Array {
        if (this.encoded !== undefined) return this.encoded;
        const bytes = byteView(this.payload);
        if (bytes === undefined) {
            throw new TypeError("Metadata is not encoded; create it with its MIME type's toMetadata() method")
        }
        return bytes
    }

    /**
     * Serializes the metadata and writes it to the given [ByteWriter]{@link ByteWriter}.
     *
     * @param writer - The byte writer to which the metadata will be written.
     * @param hasPayload - Indicates whether to write the payload length prefix (defaults to `true`).
     */
    public write(writer: ByteWriter, hasPayload: boolean = true) {
        const array = this.toUint8Array()
        if (hasPayload) writer.i24(array.length)
        writer.write(array)
    }
}
