import type {ByteWriter} from "bebyte";

/**
 * Abstract base class for writing frame content to a binary stream.
 *
 * Subclasses must implement the `write` method to serialize frame-specific
 * data using the provided `ByteWriter`.
 *
 * ### Example Usage:
 * ```ts
 * class SetupFrameWriter extends FrameWriter {
 *   protected write(writer: ByteWriter): void {
 *     writer.i32(this.version);
 *     writer.write(this.payload);
 *   }
 * }
 * ```
 */
export abstract class FrameWriter {
    /**
     * Writes the frame content to the provided `ByteWriter`.
     * This method must be implemented by subclasses to define
     * the serialization logic for the frame.
     *
     * @param {ByteWriter} writer - The writer used to output binary data.
     */
    protected abstract write(writer: ByteWriter): void;
}
