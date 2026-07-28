import bebyte, {type ByteReader, type ByteWriter} from "bebyte";

/** Intrinsic tag shared by Uint8Array instances from every JavaScript realm. */
const UINT8_ARRAY_TAG = "[object Uint8Array]";

/**
 * Creates a zero-copy big-endian reader over an existing byte view.
 *
 * @param bytes Bytes to read.
 * @returns A complete `bebyte` reader, including every integer width.
 */
export function createReader(bytes: Uint8Array): ByteReader {
    return bebyte.reader(bytes);
}

/**
 * Creates a growable big-endian writer.
 *
 * @param initialCapacity Optional number of bytes to reserve.
 * @returns A complete `bebyte` writer.
 */
export function createWriter(initialCapacity = 0): ByteWriter {
    return bebyte.writer(initialCapacity);
}

/** Returns a local byte view for a Uint8Array created in any JavaScript realm. */
export function byteView(value: unknown): Uint8Array | undefined {
    if (value instanceof Uint8Array) return value;
    if (typeof value !== "object" || value === null || !ArrayBuffer.isView(value)) return undefined;
    if (Object.prototype.toString.call(value) !== UINT8_ARRAY_TAG) return undefined;
    const bytes = value as Uint8Array;
    return new Uint8Array(bytes.buffer, bytes.byteOffset, bytes.byteLength);
}
