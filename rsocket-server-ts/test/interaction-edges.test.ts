import {afterEach, beforeEach, describe, expect, it} from "vitest";
import type {Flux, Publisher, Subscriber} from "reactor-core-ts";
import {readFrameTypeAndFlags, route, type RSocketPayloadFrame} from "rsocket-core-ts";
import {FrameType} from "rsocket-frames-ts";
import {
    FireAndForgetController,
    RequestChannelController,
    RequestResponseController,
    RequestStreamController,
    type RSocketRequestContext
} from "@/index.js";
import {prependChannelPayload} from "./client-engine.js";
import {connectTestPair, type ConnectedTestPair} from "./helpers.js";
import {ManualPublisher} from "./manual-publisher.js";

let fireAndForgetCalls = 0;
let primaryResponseSubscriptionCancelled = false;
let duplicateResponseSubscriptionCancelled = false;
let detachedSourceSubscriptions = 0;
let detachedChannelResponseSubscriptions = 0;

class EmptyFireAndForgetController extends FireAndForgetController<void> {
    protected readonly route = [] as const;

    override handle(data: void): void {
        expect(data).toBeUndefined();
        fireAndForgetCalls += 1;
    }
}

class ThrowingFireAndForgetController extends FireAndForgetController<void> {
    protected readonly route = "throwing-fnf";

    override handle(): never {
        throw new Error("fire-and-forget failed");
    }
}

class EmptyResponseController extends RequestResponseController<void, void> {
    protected readonly route = [] as const;

    override handle(): undefined {
        return undefined;
    }
}

class PromiseResponseController extends RequestResponseController<number, number> {
    protected readonly route = [] as const;

    override handle(data: number): Promise<number> {
        return Promise.resolve(data * 2);
    }
}

class NullResponseController extends RequestResponseController<void, null> {
    protected readonly route = "null-response";

    override handle(): null {
        return null;
    }
}

class DuplicateSubscriptionResponseController extends RequestResponseController<void, number> {
    protected readonly route = "duplicate-subscription";

    override handle(): Publisher<number> {
        return {
            subscribe(subscriber: Subscriber<number>): void {
                let requested = false;
                subscriber.onSubscribe({
                    request: () => {
                        requested = true;
                    },
                    cancel: () => {
                        primaryResponseSubscriptionCancelled = true;
                    }
                });
                subscriber.onSubscribe({
                    request: () => undefined,
                    cancel: () => {
                        duplicateResponseSubscriptionCancelled = true;
                    }
                });
                if (requested) subscriber.onNext(42);
            }
        };
    }
}

class EmptyStreamController extends RequestStreamController<void, never> {
    protected readonly route = [] as const;

    override handle(): readonly never[] {
        return [];
    }
}

class EmptyChannelController extends RequestChannelController<void, never> {
    protected readonly route = [] as const;

    override handle(requests: Flux<RSocketPayloadFrame<void>>): Flux<never> {
        return requests.flatMap(() => [] as never[]);
    }
}

/** Closes its session reentrantly before returning an otherwise retained source. */
class DisconnectingStreamController extends RequestStreamController<void, never> {
    protected readonly route = "disconnect-stream";

    override handle(_data: void, context: RSocketRequestContext): Publisher<never> {
        context.connection.disconnect("controller closed connection").subscribe();
        return {
            subscribe(subscriber: Subscriber<never>): void {
                detachedSourceSubscriptions += 1;
                subscriber.onSubscribe({request() {}, cancel() {}});
            }
        };
    }
}

/** Cancels request input before returning a response publisher. */
class CancellingChannelController extends RequestChannelController<void, never> {
    protected readonly route = "cancel-channel-input";

    override handle(requests: Flux<RSocketPayloadFrame<void>>): Publisher<never> {
        requests.subscribe({
            onSubscribe(subscription) {
                subscription.cancel();
            },
            onNext() {},
            onError() {},
            onComplete() {}
        });
        return {
            subscribe(subscriber: Subscriber<never>): void {
                detachedChannelResponseSubscriptions += 1;
                subscriber.onSubscribe({request() {}, cancel() {}});
            }
        };
    }
}

describe("interaction terminal edge cases", () => {
    let pair: ConnectedTestPair | undefined;

    beforeEach(() => {
        fireAndForgetCalls = 0;
        primaryResponseSubscriptionCancelled = false;
        duplicateResponseSubscriptionCancelled = false;
        detachedSourceSubscriptions = 0;
        detachedChannelResponseSubscriptions = 0;
    });

    afterEach(async () => {
        pair?.client.close();
        await pair?.server.close().block();
        pair = undefined;
    });

    it("handles fire-and-forget with neither data nor metadata", async () => {
        pair = await connectTestPair([EmptyFireAndForgetController]);

        await pair.client.fireAndForget(undefined).block();

        expect(fireAndForgetCalls).toBe(1);
    });

    it("does not create an error response for failed or unregistered fire-and-forget", async () => {
        pair = await connectTestPair([ThrowingFireAndForgetController]);
        pair.serverTransport.sent.length = 0;

        await pair.client.fireAndForget({metadata: route("throwing-fnf")}).block();
        await pair.client.fireAndForget({metadata: route("missing-fnf")}).block();
        await new Promise((resolve) => setTimeout(resolve, 0));

        expect(pair.serverTransport.sent.some((bytes) =>
            readFrameTypeAndFlags(bytes) >>> 10 === FrameType.ERROR
        )).toBe(false);
        expect(pair.client.isClosed).toBe(false);
    });

    it("completes request-response without a NEXT payload for an empty result", async () => {
        pair = await connectTestPair([EmptyResponseController]);

        await expect(pair.client.requestResponse(undefined).block()).resolves.toBeUndefined();
    });

    it("adapts promise request-response results", async () => {
        pair = await connectTestPair([PromiseResponseController]);

        const response = await pair.client.requestResponse(6).block();

        expect(response?.data).toBe(12);
    });

    it("encodes a synchronous JSON null response without a Reactor null signal", async () => {
        pair = await connectTestPair([NullResponseController]);

        const response = await pair.client.requestResponse({metadata: route("null-response")}).block();

        expect(response?.data).toBeNull();
    });

    it("cancels duplicate and terminal request-response subscriptions", async () => {
        pair = await connectTestPair([DuplicateSubscriptionResponseController]);

        const response = await pair.client.requestResponse({
            metadata: route("duplicate-subscription")
        }).block();

        expect(response?.data).toBe(42);
        expect(primaryResponseSubscriptionCancelled).toBe(true);
        expect(duplicateResponseSubscriptionCancelled).toBe(true);
    });

    it("completes an empty request-stream without requiring a payload", async () => {
        pair = await connectTestPair([EmptyStreamController]);

        await expect(pair.client.requestStream(undefined).toArray()).resolves.toEqual([]);
    });

    it("supports an initially complete request-channel with no payload", async () => {
        pair = await connectTestPair([EmptyChannelController]);

        await expect(pair.client.requestChannel([]).toArray()).resolves.toEqual([]);
    });

    it("does not subscribe controller output after reentrant session shutdown", async () => {
        pair = await connectTestPair([DisconnectingStreamController]);

        await expect(pair.client.requestStream({
            metadata: route("disconnect-stream")
        }).toArray()).rejects.toThrow("controller closed connection");

        expect(detachedSourceSubscriptions).toBe(0);
    });

    it("does not subscribe channel output after controller cancels request input", async () => {
        const outbound = new ManualPublisher<{data: void}>();
        pair = await connectTestPair([CancellingChannelController]);

        await expect(pair.client.requestChannel(prependChannelPayload(
            {metadata: route("cancel-channel-input")},
            outbound
        )).toArray()).rejects.toThrow("cancelled by controller code");

        expect(detachedChannelResponseSubscriptions).toBe(0);
    });
});
