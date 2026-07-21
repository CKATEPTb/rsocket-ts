/**
 * Enum representing possible frame error codes.
 * Used in protocol-level error handling for setup, connection, or stream errors.
 */
export enum FrameErrorCode {
    /** Reserved. */
    RESERVED_ZERO = 0x00000000,
    /** The Setup frame is invalid for the server. Stream ID MUST be 0. */
    INVALID_SETUP = 0x00000001,
    /** Some parameters specified by the client are unsupported. Stream ID MUST be 0. */
    UNSUPPORTED_SETUP = 0x00000002,
    /** Server rejected the setup, reason may be specified in payload. Stream ID MUST be 0. */
    REJECTED_SETUP = 0x00000003,
    /** Server rejected resume attempt. Reason may be specified. Stream ID MUST be 0. */
    REJECTED_RESUME = 0x00000004,
    /** Connection is being terminated immediately. Stream ID MUST be 0. */
    CONNECTION_ERROR = 0x00000101,
    /** Connection is being terminated gracefully. Stream ID MUST be 0. */
    CONNECTION_CLOSE = 0x00000102,
    /** Application layer raised an error (onError). Stream ID MUST be > 0. */
    APPLICATION_ERROR = 0x00000201,
    /** Valid request rejected by responder. No processing guaranteed. Stream ID MUST be > 0. */
    REJECTED = 0x00000202,
    /** Responder canceled the request. Side-effects may exist. Stream ID MUST be > 0. */
    CANCELED = 0x00000203,
    /** Request was invalid. Stream ID MUST be > 0. */
    INVALID = 0x00000204,
    /** Reserved for extension use. */
    RESERVED_ONE = 0xFFFFFFFF
}

/** Lookup helpers for standard and application-defined error codes. */
export namespace FrameErrorCode {
    /**
     * Resolves a FrameErrorCode from a numeric byte value.
     * Matches the code exactly (no bitmask logic).
     *
     * Application-defined codes are preserved because the protocol reserves
     * `0x00000301..0xFFFFFFFE` for that purpose.
     *
     * @param {number} byte - The unsigned 32-bit error code.
     * @returns {FrameErrorCode} A standard or application-defined error code.
     * @throws {RangeError} If the value does not fit in 32 bits.
     */
    export function fromByte(byte: number): FrameErrorCode {
        if (!Number.isInteger(byte) || byte < 0 || byte > 0xffffffff) {
            throw new RangeError(`Error code must be an unsigned 32-bit integer; received ${byte}`)
        }
        return byte as FrameErrorCode
    }
}
