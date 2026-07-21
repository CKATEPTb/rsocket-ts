/**
 * Incoming RSocket PAYLOAD fragment reassembly helpers.
 *
 * An endpoint decodes fragmented payload pieces as raw bytes; this module joins
 * them into one logical PAYLOAD frame before interaction handlers see it.
 */
import {
    FrameErrorCode,
    FrameFlag,
    type MimeType,
    type Payload,
    PayloadFlag,
    PayloadFrame
} from "rsocket-frames-ts";
import {RSocketProtocolError} from "@/errors/index.js";
import {
    appendFragmentParts,
    concatFragmentBytes,
    type FragmentParts
} from "@/reassembly/parts.js";
import {decodeRawMetadata, decodeRawPayload} from "@/reassembly/decode.js";

export {
    decodeInitialRequestFrame,
    RequestFragmentAssembler,
    type InitialRequestFrame
} from "@/reassembly/request.js";
export {IgnoredPayloadFragments} from "@/reassembly/ignored.js";

/**
 * Buffered fragments for one incoming fragmented PAYLOAD sequence.
 */
interface PayloadFragmentState extends FragmentParts {
    /** Combined payload flags with FOLLOWS and METADATA removed. */
    flags: number;
}

/**
 * Fragment state keyed by stream id.
 */
export type PayloadFragmentMap = Map<number, PayloadFragmentState>;

/**
 * Reassembles a PAYLOAD fragment sequence into one logical frame.
 */
export function reassemblePayloadFrame(
    frame: PayloadFrame,
    fragments: PayloadFragmentMap,
    metadataMimeType: MimeType<any>,
    dataMimeType: MimeType<any>
): PayloadFrame | undefined {
    const streamId = frame.header.streamId;
    if (!frame.isNext() && !frame.isComplete() && !frame.hasFollows()) {
        fragments.delete(streamId);
        throw new RSocketProtocolError("RSocket PAYLOAD must set FOLLOWS, NEXT, COMPLETE, or a valid combination", {
            code: FrameErrorCode.INVALID,
            streamId
        });
    }
    // COMPLETE wins when a peer sends the otherwise contradictory F|C pair.
    const follows = frame.hasFollows() && !frame.isComplete();
    let state = fragments.get(streamId);
    if (!follows && state === undefined && !frame.hasFollows()) {
        if (!frame.isNext()) return frame;
        const metadata = decodeRawMetadata(frame.metadata, metadataMimeType);
        const payload = decodeRawPayload(frame.payload, dataMimeType);
        return metadata === frame.metadata && payload === frame.payload
            ? frame
            : new PayloadFrame(streamId, frame.header.flags, metadata, payload);
    }

    state ??= newPayloadFragmentState();
    // The final fragment carries the logical NEXT/COMPLETE semantics. The
    // initial fragment of a terminal-only PAYLOAD can legally carry only FOLLOWS.
    state.flags = frame.header.flags & ~PayloadFlag.FOLLOWS & ~FrameFlag.METADATA;
    try {
        appendFragmentParts(state, frame);
    } catch (error) {
        fragments.delete(streamId);
        throw error;
    }

    if (follows) {
        fragments.set(streamId, state);
        return undefined;
    }

    fragments.delete(streamId);
    const metadataBytes = concatFragmentBytes(state.metadataChunks, state.metadataLength);
    const dataBytes = concatFragmentBytes(state.dataChunks, state.dataLength);
    const metadata = state.hasMetadata
        ? metadataMimeType.toMetadata(metadataBytes, false)
        : undefined;
    const payload = state.dataLength > 0 || (state.flags & PayloadFlag.NEXT) === PayloadFlag.NEXT
        ? dataMimeType.toPayload(dataBytes)
        : undefined;

    return new PayloadFrame(streamId, state.flags, metadata, payload as Payload<any> | undefined);
}

/**
 * Creates an empty fragment state for one stream.
 */
function newPayloadFragmentState(): PayloadFragmentState {
    return {
        hasMetadata: false,
        metadataLength: 0,
        dataLength: 0,
        flags: FrameFlag.NONE
    };
}

