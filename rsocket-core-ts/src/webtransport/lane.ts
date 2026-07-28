/** Allocation-conscious serial writes to one WebTransport stream or datagram writer. */
import {RSocketConnectionError} from "@/errors/index.js";
import type {
    RSocketWebTransportWritable,
    RSocketWebTransportWriter
} from "@/webtransport/types.js";

/** Callback invoked after one queued packet succeeds or is discarded. */
export type RSocketWebTransportWriteSettled = () => void;

/** Serializes asynchronous WHATWG writes while keeping enqueue synchronous. */
export class RSocketWebTransportWriteLane {
    private packets: Uint8Array[] | undefined;
    private settled: Map<Uint8Array, RSocketWebTransportWriteSettled> | undefined;
    private head = 0;
    private writer: RSocketWebTransportWriter<Uint8Array> | undefined;
    private opening = true;
    private draining = false;
    private finishing = false;
    private closed = false;

    /** Acquires one writer and optionally places a stream preface first. */
    constructor(
        writable: PromiseLike<RSocketWebTransportWritable<Uint8Array>> | RSocketWebTransportWritable<Uint8Array>,
        preface: Uint8Array | undefined,
        private readonly onFailure: (error: unknown) => void
    ) {
        if (preface !== undefined) this.enqueue(preface);
        void Promise.resolve(writable).then(
            (value) => this.writerReady(value),
            (error) => this.fail(error)
        );
    }

    /** Whether this lane rejects further packets. */
    get isClosed(): boolean {
        return this.closed || this.finishing;
    }

    /** Queues one immutable packet without waiting for native backpressure. */
    enqueue(packet: Uint8Array, settled?: RSocketWebTransportWriteSettled): void {
        if (this.closed || this.finishing) {
            throw new RSocketConnectionError("RSocket WebTransport write lane is closed");
        }
        (this.packets ??= []).push(packet);
        if (settled !== undefined) (this.settled ??= new Map()).set(packet, settled);
        if (!this.opening && !this.draining) void this.drain();
    }

    /** Closes the writer after every packet already accepted by `enqueue`. */
    finish(): void {
        if (this.closed || this.finishing) return;
        this.finishing = true;
        if (!this.opening && !this.draining) void this.drain();
    }

    /** Immediately discards queued packets and aborts the underlying writer. */
    abort(reason?: unknown): void {
        if (this.closed) return;
        this.closed = true;
        this.finishCallbacks();
        this.clearQueue();
        const writer = this.writer;
        this.writer = undefined;
        if (writer === undefined) return;
        try {
            const aborted = writer.abort?.(reason);
            if (aborted !== undefined) void Promise.resolve(aborted).catch(ignoreRejection);
        } catch {
            // Session termination remains the authoritative close signal.
        }
        releaseWriter(writer);
    }

    /** Starts draining after the writer lock has been acquired. */
    private writerReady(writable: RSocketWebTransportWritable<Uint8Array>): void {
        if (this.closed) return;
        try {
            this.writer = writable.getWriter();
            this.opening = false;
            void this.drain();
        } catch (error) {
            this.fail(error);
        }
    }

    /** Performs at most one native write at a time and compacts long-lived queues. */
    private async drain(): Promise<void> {
        if (this.draining || this.opening || this.closed) return;
        this.draining = true;
        try {
            const writer = this.writer;
            if (writer === undefined) return;
            let packets = this.packets;
            while (packets !== undefined && this.head < packets.length) {
                const index = this.head;
                const packet = packets[index] as Uint8Array;
                await writer.write(packet);
                this.head = index + 1;
                const settled = this.settled?.get(packet);
                if (settled !== undefined) {
                    this.settled?.delete(packet);
                    settled();
                }
                packets = this.packets;
                if (this.head >= 64 && this.head * 2 >= (packets?.length ?? 0)) {
                    this.compactQueue();
                    packets = this.packets;
                }
            }
            this.clearQueue();
            if (this.finishing) await this.closeWriter(writer);
        } catch (error) {
            this.fail(error);
        } finally {
            this.draining = false;
            if (!this.closed && !this.opening &&
                ((this.packets?.length ?? 0) > this.head || this.finishing)) {
                void this.drain();
            }
        }
    }

    /** Gracefully closes and releases one fully drained writer. */
    private async closeWriter(writer: RSocketWebTransportWriter<Uint8Array>): Promise<void> {
        this.closed = true;
        this.writer = undefined;
        try {
            await writer.close?.();
        } finally {
            releaseWriter(writer);
        }
    }

    /** Terminates the lane and reports exactly one asynchronous write failure. */
    private fail(error: unknown): void {
        if (this.closed) return;
        const writer = this.writer;
        this.writer = undefined;
        this.closed = true;
        this.opening = false;
        this.finishCallbacks();
        this.clearQueue();
        if (writer !== undefined) releaseWriter(writer);
        this.onFailure(error);
    }

    /** Runs completion callbacks for packets skipped after a best-effort failure. */
    private finishCallbacks(): void {
        const callbacks = this.settled;
        if (callbacks === undefined) return;
        this.settled = undefined;
        for (const callback of callbacks.values()) callback();
    }

    /** Drops consumed array prefixes without shifting on every packet. */
    private compactQueue(): void {
        this.packets = this.packets?.slice(this.head);
        this.head = 0;
    }

    /** Releases queue storage once all pending writes settle. */
    private clearQueue(): void {
        this.packets = undefined;
        this.settled = undefined;
        this.head = 0;
    }
}

/** Releases a WHATWG writer lock without changing the primary outcome. */
function releaseWriter(writer: RSocketWebTransportWriter<Uint8Array>): void {
    try {
        writer.releaseLock?.();
    } catch {
        // A custom writer may already have released its lock while closing.
    }
}

/** Handles an optional async abort rejection during terminal cleanup. */
function ignoreRejection(): void {
}
