/**
 * Enumeration of all standard RSocket frame types.
 *
 * These frame types define the kind of protocol message being sent
 * over the RSocket connection. Each frame has a unique identifier byte
 * used for decoding and routing.
 */
export enum FrameType {
    /** `0x00` - Reserved for future use. */
    RESERVED = 0x00,
    /** `0x01` - Sent by the client to initiate the connection and negotiate setup parameters. */
    SETUP = 0x01,
    /** `0x02` - Sent by the responder to grant the requester permission to send requests. */
    LEASE = 0x02,
    /** `0x03` - Used to maintain liveness of the connection. */
    KEEPALIVE = 0x03,
    /** `0x04` - Request-Response interaction model (1 request, 1 response). */
    REQUEST_RESPONSE = 0x04,
    /** `0x05` - Fire-and-Forget: A one-way message with no response. */
    REQUEST_FNF = 0x05,
    /** `0x06` - Request a stream of responses (possibly infinite). */
    REQUEST_STREAM = 0x06,
    /** `0x07` - Bi-directional stream of messages between requester and responder. */
    REQUEST_CHANNEL = 0x07,
    /** `0x08` - Request N more items in a stream (backpressure mechanism). */
    REQUEST_N = 0x08,
    /** `0x09` - Cancel an ongoing request. */
    CANCEL = 0x09,
    /** `0x0A` - Used to transmit a payload on a stream. */
    PAYLOAD = 0x0A,
    /** `0x0B` - Represents an application or connection-level error. */
    ERROR = 0x0B,
    /** `0x0C` - Pushes metadata out-of-band to the peer. */
    METADATA_PUSH = 0x0C,
    /** `0x0D` - Sent to resume a connection (if supported). */
    RESUME = 0x0D,
    /** `0x0E` - Acknowledges a successful resume. */
    RESUME_OK = 0x0E,
    /** `0x3F` - Reserved for protocol extensions. */
    EXT = 0x3F
}

/** Parsing helpers for the six-bit frame type field. */
export namespace FrameType {
    /**
     * Attempts to determine the `FrameType` based on the given byte value.
     *
     * @param {number} byte - The raw frame type byte value.
     * @returns {FrameType} The corresponding assigned frame type.
     * @throws {RangeError} If the six-bit value is unassigned by the protocol.
     */
    export function fromByte(byte: number): FrameType {
        const value = fromWireByte(byte);
        if (value <= FrameType.RESUME_OK || value === FrameType.EXT) return value;
        throw new RangeError(`Unknown RSocket frame type: 0x${byte.toString(16).padStart(2, "0")}`)
    }

    /**
     * Parses any six-bit wire value, including unassigned ignorable types.
     *
     * @param byte Raw six-bit frame-type field.
     * @returns Validated wire value represented as `FrameType`.
     * @throws {RangeError} If the value does not fit in six bits.
     */
    export function fromWireByte(byte: number): FrameType {
        if (!Number.isInteger(byte) || byte < 0 || byte > 0x3f) {
            throw new RangeError(`Frame type must be an unsigned 6-bit integer; received ${byte}`)
        }
        return byte as FrameType;
    }
}
