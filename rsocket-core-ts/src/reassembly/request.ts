/** Reassembly for fragmented initial REQUEST_* frames shared by responders. */
import {
    FrameFlag,
    type Metadata,
    type MimeType,
    type Payload,
    PayloadFlag,
    PayloadFrame,
    RequestChannelFlag,
    RequestChannelFrame,
    RequestFireAndForgetFrame,
    RequestResponseFrame,
    RequestStreamFrame,
    type FrameType
} from "rsocket-frames-ts";
import {RSocketProtocolError} from "@/errors/index.js";
import {decodeRawMetadata, decodeRawPayload} from "@/reassembly/decode.js";
import {
    appendFragmentParts,
    concatFragmentBytes,
    type FragmentParts
} from "@/reassembly/parts.js";

/** Initial request frame types that support RSocket fragmentation. */
export type InitialRequestFrame =
    | RequestFireAndForgetFrame
    | RequestResponseFrame
    | RequestStreamFrame
    | RequestChannelFrame;

/** Decodes one complete initial request retained as application wire bytes. */
export function decodeInitialRequestFrame(
    frame: InitialRequestFrame,
    metadataMimeType: MimeType<any>,
    dataMimeType: MimeType<any>
): InitialRequestFrame {
    const metadata = decodeRawMetadata(frame.metadata, metadataMimeType);
    const payload = decodeRawPayload(frame.payload, dataMimeType);
    if (metadata === frame.metadata && payload === frame.payload) return frame;
    return createRequestFrame(frame, frame.header.flags, metadata, payload);
}

/** Buffered bytes and original request semantics for one stream. */
interface RequestFragmentState extends FragmentParts {
    readonly initial: InitialRequestFrame;
}

/** Tracks fragmented initial requests independently by stream identifier. */
export class RequestFragmentAssembler {
    private readonly fragments = new Map<number, RequestFragmentState>();

    /** Starts buffering an initial frame whose FOLLOWS flag is set. */
    start(frame: InitialRequestFrame): void {
        const streamId = frame.header.streamId;
        if (this.fragments.has(streamId)) {
            throw new RSocketProtocolError("RSocket stream already has an incomplete initial request", {streamId});
        }
        const state: RequestFragmentState = {
            initial: frame,
            hasMetadata: false,
            metadataLength: 0,
            dataLength: 0
        };
        appendFragmentParts(state, frame);
        this.fragments.set(streamId, state);
    }

    /** Whether a PAYLOAD belongs to a fragmented initial request. */
    has(streamId: number): boolean {
        return this.fragments.has(streamId);
    }

    /** Whether no incomplete initial request is retained. */
    get isEmpty(): boolean {
        return this.fragments.size === 0;
    }

    /** Returns the initial request type retained for one fragmented sequence. */
    frameType(streamId: number): FrameType | undefined {
        return this.fragments.get(streamId)?.initial.type;
    }

    /** Adds a continuation and returns the complete logical request when final. */
    continue(
        frame: PayloadFrame,
        metadataMimeType: MimeType<any>,
        dataMimeType: MimeType<any>
    ): InitialRequestFrame | undefined {
        const streamId = frame.header.streamId;
        const state = this.fragments.get(streamId);
        if (state === undefined) {
            throw new RSocketProtocolError("RSocket request fragment has no initial REQUEST frame", {streamId});
        }
        if (!frame.isNext() && !frame.isComplete() && !frame.hasFollows()) {
            this.fragments.delete(streamId);
            throw new RSocketProtocolError(
                "RSocket request continuation must set FOLLOWS, NEXT, COMPLETE, or a valid combination",
                {streamId}
            );
        }
        try {
            appendFragmentParts(state, frame);
        } catch (error) {
            this.fragments.delete(streamId);
            throw error;
        }
        if (frame.hasFollows() && !frame.isComplete()) return undefined;

        this.fragments.delete(streamId);
        return rebuildRequest(state, frame, metadataMimeType, dataMimeType);
    }

    /** Drops one incomplete request after CANCEL or stream failure. */
    delete(streamId: number): void {
        this.fragments.delete(streamId);
    }

    /** Releases every retained fragment on session termination. */
    clear(): void {
        this.fragments.clear();
    }
}

/** Restores the original request type after all continuation bytes arrive. */
function rebuildRequest(
    state: RequestFragmentState,
    finalFrame: PayloadFrame,
    metadataMimeType: MimeType<any>,
    dataMimeType: MimeType<any>
): InitialRequestFrame {
    const initial = state.initial;
    let flags = initial.header.flags & ~PayloadFlag.FOLLOWS & ~FrameFlag.METADATA;
    if (initial instanceof RequestChannelFrame && finalFrame.isComplete()) flags |= RequestChannelFlag.COMPLETE;
    const metadata = state.hasMetadata
        ? metadataMimeType.toMetadata(concatFragmentBytes(state.metadataChunks, state.metadataLength), false)
        : undefined;
    const payload = dataMimeType.toPayload(
        concatFragmentBytes(state.dataChunks, state.dataLength)
    ) as Payload<any>;

    return createRequestFrame(initial, flags, metadata, payload);
}

/** Recreates an initial request while preserving its frame-specific fields. */
function createRequestFrame(
    initial: InitialRequestFrame,
    flags: number,
    metadata: Metadata<any> | undefined,
    payload: Payload<any> | undefined
): InitialRequestFrame {
    const streamId = initial.header.streamId;
    if (initial instanceof RequestFireAndForgetFrame) {
        return new RequestFireAndForgetFrame(streamId, flags, metadata, payload);
    }
    if (initial instanceof RequestResponseFrame) {
        return new RequestResponseFrame(streamId, flags, metadata, payload);
    }
    if (initial instanceof RequestStreamFrame) {
        return new RequestStreamFrame(streamId, flags, initial.request, metadata, payload);
    }
    return new RequestChannelFrame(streamId, flags, initial.request, metadata, payload);
}
