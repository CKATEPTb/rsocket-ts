import {Header} from "@/frame/context/Header";
import type {Payload} from "@/frame/context/Payload";
import {FrameType} from "@/frame/FrameType";
import {FrameFlag} from "@/frame/FrameFlag";
import {FrameWriter} from "@/frame/FrameWriter";
import type {Metadata} from "@/frame/context/Metadata";
import {createWriter} from "@/binary";
import {MAX_FRAME_SIZE} from "@/frame/transport/constants";
import {assertFrameSize} from "@/frame/transport/framing";

const serializedFrame = Symbol("serializedFrame");
const serializeBytes = Symbol("serializeBytes");
const FRAME_HEADER_LENGTH = 6;

/** Internal symbol-backed storage used without global weak-map lookups. */
interface SerializedFrameOwner {
    [serializedFrame]: Uint8Array | undefined;
}

/**
 * Serializes a frame with optional transport-prefix capacity in one allocation.
 * This helper is internal and intentionally omitted from the package barrel.
 */
export function serializeFrame(
    frame: Frame,
    prefixLength: 0 | 3,
    maximum = MAX_FRAME_SIZE
): Uint8Array {
    const serialized = serializedBytes(frame);
    if (serialized !== undefined || frame.toUint8Array !== Frame.prototype.toUint8Array) {
        const raw = frame.toUint8Array();
        assertFrameSize(raw.length, maximum);
        if (prefixLength === 0) return raw;
        const result = new Uint8Array(raw.length + 3);
        result[0] = raw.length >>> 16;
        result[1] = raw.length >>> 8;
        result[2] = raw.length;
        result.set(raw, 3);
        return result;
    }
    return frame[serializeBytes](prefixLength, maximum);
}

/** Associates a decoded frame with its original wire view. */
export function rememberSerializedFrame(frame: Frame, bytes: Uint8Array): Frame {
    (frame as unknown as SerializedFrameOwner)[serializedFrame] = bytes;
    return frame;
}

/** Reads the original wire bytes retained directly by a decoded frame. */
function serializedBytes(frame: Frame): Uint8Array | undefined {
    return (frame as unknown as SerializedFrameOwner)[serializedFrame];
}

/**
 * Abstract base class representing an RSocket frame.
 *
 * All specific frame types (e.g., `SetupFrame`, `RequestFrame`, etc.)
 * must extend this class to implement their frame-specific logic.
 *
 * Each frame has:
 * - A header that includes frame type, stream ID, and flags.
 * - Optional metadata and payload sections.
 *
 * This class also implements encoding logic to convert a frame into a binary buffer.
 */
export abstract class Frame extends FrameWriter {
    /** Original wire bytes retained only for frames produced by the deserializer. */
    declare private [serializedFrame]: Uint8Array | undefined;

    /**
     * The internal frame header (type, flags, stream ID).
     * @protected
     */
    public readonly header: Header

    /**
     * Constructs a new frame instance.
     *
     * @param {FrameType} type - The RSocket frame type (e.g. SETUP, REQUEST_RESPONSE).
     * @param {number} streamId - The stream identifier associated with the frame.
     * @param {FrameFlag} [flags=FrameFlag.NONE] - Initial frame flags.
     * @param {Metadata<any>} [metadata] - Optional metadata section.
     * @param {Payload<any>} [payload] - Optional payload section.
     * @param normalizeMetadataFlag Whether metadata should add the metadata flag.
     * Internal unknown-frame decoding disables normalization to preserve flags verbatim.
     */
    protected constructor(
        type: FrameType,
        streamId: number,
        flags: FrameFlag = FrameFlag.NONE,
        public readonly metadata?: Metadata<any>,
        public readonly payload?: Payload<any>,
        normalizeMetadataFlag = true
    ) {
        super()
        const hasMetadata = this.metadata !== undefined
        const normalizedFlags = normalizeMetadataFlag && hasMetadata
            ? flags | FrameFlag.METADATA
            : flags
        this.header = new Header(
            type,
            streamId,
            normalizedFlags
        )
    }

    /**
     * Gets the frame type.
     *
     * @returns {FrameType} The frame type value.
     */
    public get type() {
        return this.header.frameType
    }

    /**
     * Checks if a specific flag is set on the frame.
     *
     * @param {FrameFlag} flag - The flag to check.
     * @returns {boolean} `true` if the flag is set, otherwise `false`.
     */
    public isFlagSet(flag: FrameFlag): boolean {
        return this.header.isFlagSet(flag)
    }

    /**
     * Indicates whether the frame can be safely ignored by the peer.
     * Relies on the `IGNORE` flag being set.
     *
     * @returns {boolean} `true` if frame has IGNORE flag, otherwise `false`.
     */
    public canBeIgnored(): boolean {
        return this.isFlagSet(FrameFlag.IGNORE)
    }

    /**
     * Indicates whether the frame contains metadata.
     * Relies on the `METADATA` flag being set.
     *
     * @returns {boolean} `true` if the METADATA flag is set, otherwise `false`.
     */
    public hasMetadata(): boolean {
        return this.isFlagSet(FrameFlag.METADATA)
    }
    /**
     * Serializes the frame into a `Uint8Array` for transmission.
     *
     * Frame is written as:
     *  - Header
     *  - Frame-specific body (`write()` method implemented in subclass)
     *  - Metadata (if present)
     *  - Payload (if present)
     *
     * @returns {Uint8Array} Serialized binary representation of the frame.
     *
     * This is the raw RSocket representation. Use `FrameCodec` when transport
     * framing must be applied for WebSocket or TCP.
     *
     * @throws {RangeError} If the frame exceeds the protocol maximum.
     */
    public toUint8Array(): Uint8Array {
        const serialized = this[serializedFrame];
        if (serialized !== undefined) return serialized;
        return this[serializeBytes](0, MAX_FRAME_SIZE);
    }

    /** Serializes this frame with optional space reserved for a transport prefix. */
    private [serializeBytes](prefixLength: 0 | 3, maximum: number): Uint8Array {
        const metadataHasLength = this.type !== FrameType.LEASE && this.type !== FrameType.METADATA_PUSH;
        const metadata = this.metadata !== undefined && this.metadata !== null
            ? this.metadata.toUint8Array()
            : undefined;
        const payload = this.payload?.toUint8Array();
        const emptyMetadataLength = metadata === undefined && this.hasMetadata() && metadataHasLength ? 3 : 0;
        const variableLength = (metadata?.length ?? 0)
            + (metadata !== undefined && metadataHasLength ? 3 : emptyMetadataLength)
            + (payload?.length ?? 0);
        assertFrameSize(FRAME_HEADER_LENGTH + variableLength, maximum)

        const writer = createWriter(prefixLength + FRAME_HEADER_LENGTH + frameBodyCapacity(this.type) + variableLength)
        if (prefixLength !== 0) writer.offset = prefixLength;
        this.header.write(writer)
        this.write(writer)
        const frameLength = writer.length - prefixLength
            + variableLength;
        assertFrameSize(frameLength, maximum)

        if (metadata !== undefined) {
            if (metadataHasLength) writer.i24(metadata.length)
            writer.write(metadata)
        } else if (this.hasMetadata() && metadataHasLength) {
            writer.i24(0)
        }
        if (payload !== undefined) writer.write(payload)
        const result = writer.toUint8Array()
        if (prefixLength !== 0) {
            result[0] = frameLength >>> 16;
            result[1] = frameLength >>> 8;
            result[2] = frameLength;
        }
        return result
    }
}

/** Returns fixed body capacity for common frames and a bounded handshake estimate. */
function frameBodyCapacity(type: FrameType): number {
    switch (type) {
        case FrameType.LEASE:
        case FrameType.KEEPALIVE:
        case FrameType.RESUME_OK:
            return 8;
        case FrameType.REQUEST_STREAM:
        case FrameType.REQUEST_CHANNEL:
        case FrameType.ERROR:
        case FrameType.EXT:
            return 4;
        case FrameType.SETUP:
        case FrameType.RESUME:
            return 64;
        default:
            return 0;
    }
}
