/** Tracking for fragmented payload sequences that protocol leniency discards. */
import {payloadHasMoreFragments} from "@/protocol/frames.js";

/**
 * Retains only stream IDs whose ignored PAYLOAD sequence still has continuations.
 *
 * Both endpoint roles use this state to ignore duplicate requests while a
 * malformed-but-ignorable fragmented message still occupies its stream ID.
 */
export class IgnoredPayloadFragments {
    private streamIds: Set<number> | undefined;

    /** Whether an ignored fragmented sequence currently owns the stream ID. */
    has(streamId: number): boolean {
        return this.streamIds?.has(streamId) === true;
    }

    /** Starts or finishes ignored-sequence tracking from one raw frame header. */
    update(streamId: number, typeAndFlags: number): void {
        if (payloadHasMoreFragments(typeAndFlags)) {
            (this.streamIds ??= new Set()).add(streamId);
        } else {
            this.delete(streamId);
        }
    }

    /** Consumes one continuation and releases the ID after its final fragment. */
    consume(streamId: number, typeAndFlags: number): boolean {
        if (!this.has(streamId)) return false;
        if (!payloadHasMoreFragments(typeAndFlags)) this.delete(streamId);
        return true;
    }

    /** Releases one ignored sequence and its empty backing set. */
    delete(streamId: number): void {
        const streamIds = this.streamIds;
        if (streamIds?.delete(streamId) === true && streamIds.size === 0) this.streamIds = undefined;
    }

    /** Releases every ignored stream after logical session termination. */
    clear(): void {
        this.streamIds?.clear();
        this.streamIds = undefined;
    }
}
