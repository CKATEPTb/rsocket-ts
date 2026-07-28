/** Bounded global reordering across independent reliable WebTransport streams. */
import {RSocketProtocolError} from "@/errors/index.js";

/** One record waiting for an earlier transport ordinal. */
interface PendingRecord<T> {
    /** Value emitted after every preceding ordinal arrives or is skipped. */
    readonly value: T;
    /** Bytes charged against the defensive reorder limit. */
    readonly bytes: number;
}

/** Restores the synchronous write order hidden by QUIC stream multiplexing. */
export class RSocketWebTransportOrderedReceiver<T> {
    private readonly pending = new Map<bigint, PendingRecord<T>>();
    private nextOrdinal = 0n;
    private retainedBytes = 0;

    /** Configures a hard cap for records waiting behind an ordinal gap. */
    constructor(private readonly maxRetainedBytes: number) {
        if (!Number.isSafeInteger(maxRetainedBytes) || maxRetainedBytes < 0) {
            throw new RSocketProtocolError("RSocket WebTransport reorder limit must be a non-negative safe integer");
        }
    }

    /** Bytes currently retained behind missing earlier records. */
    get bufferedBytes(): number {
        return this.retainedBytes;
    }

    /** Delivers one record now, buffers it, or drops a late datagram/marker duplicate. */
    accept(ordinal: bigint, value: T, bytes: number, emit: (value: T) => void): void {
        if (ordinal < this.nextOrdinal || this.pending.has(ordinal)) return;
        if (ordinal === this.nextOrdinal) {
            this.nextOrdinal += 1n;
            emit(value);
            this.drain(emit);
            return;
        }
        const retained = this.retainedBytes + bytes;
        if (retained > this.maxRetainedBytes) {
            throw new RSocketProtocolError(
                `RSocket WebTransport reorder buffer exceeds ${this.maxRetainedBytes} bytes`
            );
        }
        this.pending.set(ordinal, {value, bytes});
        this.retainedBytes = retained;
    }

    /** Releases every value retained by an incomplete physical session. */
    clear(): void {
        this.pending.clear();
        this.retainedBytes = 0;
    }

    /** Emits the longest contiguous suffix after a gap closes. */
    private drain(emit: (value: T) => void): void {
        while (true) {
            const record = this.pending.get(this.nextOrdinal);
            if (record === undefined) return;
            this.pending.delete(this.nextOrdinal);
            this.retainedBytes -= record.bytes;
            this.nextOrdinal += 1n;
            emit(record.value);
        }
    }
}
