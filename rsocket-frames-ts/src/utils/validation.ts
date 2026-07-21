/** Largest unsigned 31-bit value used by RSocket stream and request fields. */
export const MAX_UINT_31 = 0x7fffffff;

/** Largest unsigned 63-bit value used by RSocket resume positions. */
export const MAX_UINT_63 = (1n << 63n) - 1n;

/**
 * Validates an integer field against an inclusive range.
 *
 * @param name Field name used in the error message.
 * @param value Value to validate.
 * @param min Smallest accepted value.
 * @param max Largest accepted value.
 */
export function assertInteger(name: string, value: number, min: number, max: number): void {
    if (!Number.isInteger(value) || value < min || value > max) {
        throw new RangeError(`${name} must be an integer between ${min} and ${max}; received ${value}`);
    }
}

/**
 * Validates a bigint field against an inclusive range.
 *
 * @param name Field name used in the error message.
 * @param value Value to validate.
 * @param min Smallest accepted value.
 * @param max Largest accepted value.
 */
export function assertBigInt(name: string, value: bigint, min: bigint, max: bigint): void {
    if (typeof value !== "bigint" || value < min || value > max) {
        throw new RangeError(`${name} must be a bigint between ${min} and ${max}; received ${String(value)}`);
    }
}

/**
 * Validates the encoded byte length of a variable-width field.
 *
 * @param name Field name used in the error message.
 * @param length Encoded byte length.
 * @param min Smallest accepted length.
 * @param max Largest accepted length.
 */
export function assertByteLength(name: string, length: number, min: number, max: number): void {
    assertInteger(`${name} byte length`, length, min, max);
}
