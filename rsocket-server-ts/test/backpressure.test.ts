import {afterEach, describe, expect, it} from "vitest";
import type {Subscriber, Subscription} from "reactor-core-ts";
import {readFrameTypeAndFlags, route, type RSocketPayloadFrame} from "rsocket-core-ts";
import {FrameType} from "rsocket-frames-ts";
import {prependChannelPayload} from "./client-engine.js";
import {RequestChannelController, RequestStreamController} from "@/index.js";
import {connectTestPair, type ConnectedTestPair} from "./helpers.js";
import {ManualPublisher} from "./manual-publisher.js";
import {waitFor} from "./wait.js";

class ManualStreamController extends RequestStreamController<void, number> {
    protected readonly route = "manual-stream";

    constructor(readonly source: ManualPublisher<number>) {
        super();
    }

    override handle(): ManualPublisher<number> {
        return this.source;
    }
}

class DemandChannelController extends RequestChannelController<number, number> {
    protected readonly route = "demand-channel";

    override handle(requests: import("reactor-core-ts").Flux<RSocketPayloadFrame<number>>) {
        return requests.map(({data}) => data as number);
    }
}

describe("network backpressure", () => {
    let pair: ConnectedTestPair | undefined;

    afterEach(async () => {
        pair?.client.close();
        await pair?.server.close().block();
        pair = undefined;
    });

    it("does not start request-stream before subscriber demand", async () => {
        const source = new ManualPublisher<number>();
        pair = await connectTestPair([new ManualStreamController(source)]);
        let subscription: Subscription | undefined;
        const values: number[] = [];
        pair.client.requestStream({metadata: route("manual-stream")}).subscribe({
            onSubscribe(value) {
                subscription = value;
            },
            onNext(value) {
                values.push(value.data as number);
            },
            onError(error) {
                throw error;
            },
            onComplete() {
            }
        } satisfies Subscriber<RSocketPayloadFrame>);

        expect(frameCount(pair.clientTransport.sent, FrameType.REQUEST_STREAM)).toBe(0);
        subscription?.request(2);
        await waitFor(() => source.requested === 2);
        source.next(10);
        source.next(20);
        await waitFor(() => values.length === 2);
        expect(values).toEqual([10, 20]);
        expect(frameCount(pair.serverTransport.sent, FrameType.PAYLOAD)).toBe(2);

        subscription?.request(1);
        await waitFor(() => source.requested === 3);
        source.next(30);
        await waitFor(() => values.length === 3);
        subscription?.cancel();
        await waitFor(() => source.cancelled);
        expect(frameCount(pair.clientTransport.sent, FrameType.CANCEL)).toBe(1);
    });

    it("couples request-channel input credits to response demand", async () => {
        const outbound = new ManualPublisher<{data: number}>();
        pair = await connectTestPair([DemandChannelController]);
        let subscription: Subscription | undefined;
        const values: number[] = [];
        pair.client.requestChannel(prependChannelPayload(
            {metadata: route("demand-channel")},
            outbound
        )).subscribe({
            onSubscribe(value) {
                subscription = value;
                value.request(1);
            },
            onNext(value) {
                values.push(value.data as number);
            },
            onError(error) {
                throw error;
            },
            onComplete() {
            }
        } satisfies Subscriber<RSocketPayloadFrame>);

        await waitFor(() => outbound.requested === 1);
        outbound.next({data: 1});
        await waitFor(() => values.length === 1);
        expect(values).toEqual([1]);
        await waitFor(() => outbound.requested === 2);
        outbound.next({data: 2});
        await new Promise((resolve) => setTimeout(resolve, 0));
        expect(frameCount(pair.clientTransport.sent, FrameType.PAYLOAD)).toBe(1);
        expect(values).toEqual([1]);

        subscription?.request(2);
        await waitFor(() => outbound.requested === 3);
        outbound.next({data: 3});
        await waitFor(() => values.length === 3);
        expect(values).toEqual([1, 2, 3]);
        outbound.complete();
        subscription?.cancel();
    });
});

/** Counts one frame type in a raw transport write log. */
function frameCount(frames: readonly Uint8Array[], type: FrameType): number {
    return frames.filter((frame) => readFrameTypeAndFlags(frame) >>> 10 === type).length;
}
