/** Byte-part accumulation shared by PAYLOAD and initial request reassembly. */
import {Metadata, Payload, type Frame} from "rsocket-frames-ts";
import {RSocketProtocolError} from "@/errors/index.js";
import {KEEPALIVE_DATA_MIME_TYPE} from "@/protocol/index.js";

const EMPTY_BYTES = new Uint8Array(0);
/** Minimum wasted backing storage worth replacing with one compact copy. */
const MAX_RETAINED_UNUSED_BYTES = 64 * 1024;

/** Mutable byte totals retained for one fragmented logical payload. */
export interface FragmentParts {
    metadataChunks?: Uint8Array[];
    dataChunks?: Uint8Array[];
    hasMetadata: boolean;
    metadataLength: number;
    dataLength: number;
}

/** Appends metadata before data and rejects an invalid metadata-after-data sequence. */
export function appendFragmentParts(state: FragmentParts, frame: Frame): void {
    if (state.dataLength > 0 && frame.hasMetadata()) {
        throw new RSocketProtocolError("RSocket fragment sequence contains metadata after data", {
            streamId: frame.header.streamId
        });
    }
    const metadata = fragmentPartBytes(frame.metadata);
    const data = fragmentPartBytes(frame.payload);
    if (metadata !== undefined && metadata.byteLength > 0) {
        (state.metadataChunks ??= []).push(metadata);
        state.metadataLength += metadata.byteLength;
    }
    if (data !== undefined && data.byteLength > 0) {
        (state.dataChunks ??= []).push(data);
        state.dataLength += data.byteLength;
    }
    if (frame.hasMetadata()) state.hasMetadata = true;
}

/** Concatenates retained views in one allocation, reusing a sole complete view. */
export function concatFragmentBytes(
    chunks: readonly Uint8Array[] | undefined,
    length: number
): Uint8Array {
    if (length === 0) return EMPTY_BYTES;
    const values = chunks as readonly Uint8Array[];
    if (values.length === 1) return compactFragmentView(values[0] as Uint8Array);
    const bytes = new Uint8Array(length);
    let offset = 0;
    for (let index = 0; index < values.length; index += 1) {
        const value = values[index] as Uint8Array;
        bytes.set(value, offset);
        offset += value.byteLength;
    }
    return bytes;
}

/** Detaches a small fragment view only when the copy releases more memory than it allocates. */
function compactFragmentView(bytes: Uint8Array): Uint8Array {
    const unused = bytes.buffer.byteLength - bytes.byteLength;
    return unused > MAX_RETAINED_UNUSED_BYTES && bytes.byteLength < unused
        ? bytes.slice()
        : bytes;
}

/** Extracts a raw view from frame payload wrappers decoded with byte MIME codecs. */
function fragmentPartBytes(part: unknown): Uint8Array | undefined {
    if (part === undefined || part === null) return undefined;
    if (part instanceof Uint8Array) return part;
    if (part instanceof Payload || part instanceof Metadata) {
        return part.mimeType === KEEPALIVE_DATA_MIME_TYPE && part.payload instanceof Uint8Array
            ? part.payload
            : part.toUint8Array();
    }
    if (typeof part !== "object") return undefined;
    const toUint8Array = (part as {readonly toUint8Array?: unknown}).toUint8Array;
    if (typeof toUint8Array !== "function") return undefined;
    const bytes = toUint8Array.call(part);
    return bytes instanceof Uint8Array ? bytes : undefined;
}
