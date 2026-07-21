/** Client throughput and lifecycle-retention regressions over an in-memory transport. */
import type {Subscription} from "reactor-core-ts";
import {describe, expect, it, vi} from "vitest";
import {
    PayloadFlag,
    PayloadFrame,
    RequestResponseFrame
} from "rsocket-frames-ts";
import type {RSocketPayloadFrame} from "rsocket-core-ts";
import {
    capturedFrame,
    connectClient,
    dataMimeType,
    tick
} from "./client-test-helpers.js";
import {FakeTransportConnection} from "./fake-transport.js";
import {RSocketFlux} from "@/stream/session.js";

const REQUESTS = 5_000;
const MAX_TEST_DURATION_MS = 10_000;

/** Internal bounded collections whose emptiness proves interaction cleanup. */
interface ClientRetainedState {
    readonly streams: Map<number, unknown>;
    readonly fragments: Map<number, unknown>;
    readonly pendingFrames?: unknown[];
    readonly pendingIncomingFrames?: Uint8Array[];
}

describe("client performance and retention", () => {
    it("does not register an abort listener after synchronous stream completion", async () => {
        const addEventListener = vi.fn();
        const removeEventListener = vi.fn();
        const signal = {
            aborted: false,
            addEventListener,
            removeEventListener
        } as unknown as AbortSignal;
        const flux = new RSocketFlux((subscriber) => ({
            request() {},
            cancel() {},
            afterSubscribe() {
                subscriber.onComplete();
            }
        }));
        const iterable = flux.iterate(signal) as AsyncIterable<RSocketPayloadFrame>;
        const iterator = iterable[Symbol.asyncIterator]();

        await expect(iterator.next()).resolves.toEqual({done: true, value: undefined});
        expect(addEventListener).not.toHaveBeenCalled();
        expect(removeEventListener).not.toHaveBeenCalled();
    });

    it("completes sustained request-response traffic without retaining stream state", async () => {
        const transport = new FakeTransportConnection();
        const client = await connectClient(transport);
        transport.sent.length = 0;
        const started = performance.now();
        let checksum = 0;

        for (let index = 0; index < REQUESTS; index += 1) {
            const response = client.requestResponse({data: index}).block();
            const request = capturedFrame(transport.sent.pop()) as RequestResponseFrame;
            transport.receive(new PayloadFrame(
                request.header.streamId,
                PayloadFlag.NEXT | PayloadFlag.COMPLETE,
                undefined,
                dataMimeType.toPayload(index)
            ).toUint8Array());
            checksum += (await response)?.data as number;
        }

        const state = client as unknown as ClientRetainedState;
        expect(checksum).toBe(REQUESTS * (REQUESTS - 1) / 2);
        expect(state.streams.size).toBe(0);
        expect(state.fragments.size).toBe(0);
        expect(state.pendingFrames).toBeUndefined();
        expect(state.pendingIncomingFrames).toBeUndefined();
        expect(performance.now() - started).toBeLessThan(MAX_TEST_DURATION_MS);

        client.close();
        expect(transport.listenerCount).toBe(0);
    });

    it("does not retain a cancelled channel while iterator cleanup remains pending", async () => {
        const transport = new FakeTransportConnection();
        const client = await connectClient(transport);
        const never = new Promise<IteratorResult<{data: number}>>(() => undefined);
        let first = true;
        let cleanupCalls = 0;
        const input: AsyncIterable<{data: number}> = {
            [Symbol.asyncIterator]() {
                return {
                    next() {
                        if (first) {
                            first = false;
                            return Promise.resolve({done: false as const, value: {data: 1}});
                        }
                        return never;
                    },
                    return() {
                        cleanupCalls += 1;
                        return never;
                    }
                };
            }
        };
        let subscription: Subscription | undefined;

        client.requestChannel(input).subscribe({
            onSubscribe(value) {
                subscription = value;
                value.request(1);
            },
            onNext(_value: RSocketPayloadFrame) {},
            onError() {},
            onComplete() {}
        });
        await tick();
        expect((client as unknown as ClientRetainedState).streams.size).toBe(1);

        subscription?.cancel();
        await tick();

        expect(cleanupCalls).toBe(1);
        expect((client as unknown as ClientRetainedState).streams.size).toBe(0);
        expect((client as unknown as ClientRetainedState).fragments.size).toBe(0);
        client.close();
        expect(transport.listenerCount).toBe(0);
    });

    it("bounds unacknowledged Resume storage and releases it on terminal close", async () => {
        const transport = new FakeTransportConnection();
        const client = await connectClient(transport, "bounded-resume");
        const replay = (client as unknown as {
            replayBuffer: {
                maximumBytes: number;
                entries: unknown[];
                retainedBytes: number;
            };
        }).replayBuffer;
        await client.fireAndForget({data: 1}).block();

        expect(replay.maximumBytes).toBe(16 * 1024 * 1024);
        expect(replay.entries.length).toBeGreaterThan(0);
        expect(replay.retainedBytes).toBeGreaterThan(0);

        client.close();

        expect(replay.entries).toHaveLength(0);
        expect(replay.retainedBytes).toBe(0);
        expect(transport.listenerCount).toBe(0);
    });

    it("terminates instead of retrying a locally exhausted Resume buffer", async () => {
        const transport = new FakeTransportConnection();
        const client = await connectClient(transport, "bounded-resume");
        const replay = (client as unknown as {
            replayBuffer: {maximumBytes: number};
        }).replayBuffer;
        (replay as {maximumBytes: number}).maximumBytes = 6;

        await expect(client.fireAndForget({data: 1}).block())
            .rejects.toThrow("Resume replay buffer exceeded 6 bytes");

        expect(client.isClosed).toBe(true);
        expect(client.isSuspended).toBe(false);
        expect(transport.listenerCount).toBe(0);
        expect(transport.sent).toHaveLength(1);
    });
});
