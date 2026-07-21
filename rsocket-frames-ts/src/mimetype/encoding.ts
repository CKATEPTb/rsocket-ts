import {decode, encode} from "@/utils";

/**
 * Validates an ASCII protocol type name without allocating encoded bytes.
 *
 * @param value Type name to validate.
 * @param label Human-readable field name used in errors.
 */
export function assertAscii(value: string, label: string): void {
    for (let index = 0; index < value.length; index++) {
        if (value.charCodeAt(index) > 0x7f) {
            throw new TypeError(`${label} must contain ASCII characters only`);
        }
    }
}

/**
 * Encodes a custom MIME or authentication type for a seven-bit length field.
 *
 * RSocket stores `actualLength - 1`, allowing ASCII names from 1 through 128
 * bytes to fit in seven bits.
 *
 * @param value Type name to encode.
 * @param label Human-readable field name used in errors.
 */
export function encodeCustomType(value: string, label: string): Uint8Array {
    assertAscii(value, label);
    const bytes = encode(value);
    if (bytes.length < 1 || bytes.length > 128) {
        throw new RangeError(`${label} must contain between 1 and 128 ASCII bytes`);
    }
    return bytes;
}

/**
 * Decodes a custom type name from its seven-bit stored length.
 *
 * @param encodedLength Wire value containing `actualLength - 1`.
 * @param read Function that returns exactly the requested number of bytes.
 */
export function decodeCustomType(encodedLength: number, read: (length: number) => Uint8Array): string {
    const bytes = read(encodedLength + 1);
    for (const byte of bytes) if (byte > 0x7f) throw new TypeError("Custom type contains non-ASCII bytes");
    return decode(bytes);
}
