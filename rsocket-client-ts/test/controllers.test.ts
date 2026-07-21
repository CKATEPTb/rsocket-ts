import {Flux, Mono, type Subscriber, type Subscription} from "reactor-core-ts";
import {describe, expect, it, vi} from "vitest";
import {
    RequestChannelController,
    RequestStreamController
} from "@/controllers/index.js";
import {processController} from "@/controllers/process.js";
import type {RSocketControllerConnection} from "@/controllers/types.js";
import type {RSocketPayloadFrame} from "rsocket-core-ts";
import {RSocketFlux} from "@/stream/index.js";

/** Request-stream controller used to exercise decoded response demand. */
class NumberStreamController extends RequestStreamController<void, number> {
    /** Route is only materialized into the ignored fake request payload. */
    protected readonly route = "numbers";

    /** Decodes a numeric test payload. */
    protected override response(payload: RSocketPayloadFrame): number {
        return payload.data as number;
    }
}

/** Request-channel controller used to exercise decoded response demand. */
class NumberChannelController extends RequestChannelController<number, number> {
    /** Route is only materialized into the ignored fake channel input. */
    protected readonly route = "number-channel";

    /** Decodes a numeric test payload. */
    protected override response(payload: RSocketPayloadFrame): number {
        return payload.data as number;
    }
}

describe("controller Flux transformations", () => {
    it("preserves request-stream initial request batching", () => {
        const controlled = controlledFlux();
        const result = processController(
            controllerConnection(controlled.source),
            new NumberStreamController(),
            []
        );
        const subscription = subscribe(result);

        subscription.request(30);

        expect(controlled.requests).toEqual([30]);
    });

    it("preserves request-channel response batching", () => {
        const controlled = controlledFlux();
        const result = processController(
            controllerConnection(controlled.source),
            new NumberChannelController(),
            [Flux.just({data: 1})]
        );
        const subscription = subscribe(result);

        subscription.request(30);

        expect(controlled.requests).toEqual([30]);
    });

    it("preserves request batching when controller interaction logging is enabled", () => {
        const controlled = controlledFlux();
        const events: unknown[] = [];
        const controller = new NumberStreamController().log({
            interactions: true,
            logger: (event) => events.push(event)
        });
        const result = processController(controllerConnection(controlled.source), controller, []);
        const subscription = subscribe(result);

        subscription.request(30);

        expect(controlled.requests).toEqual([30]);
        expect(events).toEqual([
            expect.objectContaining({interaction: "requestStream", stage: "send"})
        ]);
    });

    it("restores the previous sink and category after logging is toggled", () => {
        const controlled = controlledFlux();
        const events: Array<{category?: string; stage?: string}> = [];
        const controller = new NumberStreamController()
            .log({category: "numbers", logger: (event) => events.push(event)})
            .log(false)
            .log(true);

        subscribe(processController(controllerConnection(controlled.source), controller, []));

        expect(events).toEqual([
            expect.objectContaining({category: "numbers", stage: "send"})
        ]);
    });

    it("forwards cancellation exactly once and suppresses later demand", () => {
        const controlled = controlledFlux();
        const result = processController(
            controllerConnection(controlled.source),
            new NumberStreamController(),
            []
        );
        const subscription = subscribe(result);

        subscription.request(30);
        subscription.cancel();
        subscription.cancel();
        subscription.request(10);

        expect(controlled.requests).toEqual([30]);
        expect(controlled.cancel).toHaveBeenCalledTimes(1);
    });

    it("releases a transformed client subscription when upstream cancel throws", () => {
        const failure = new Error("cancel failed");
        const cancel = vi.fn(() => {
            throw failure;
        });
        const source = new RSocketFlux(() => ({request: () => undefined, cancel}));
        const result = processController(
            controllerConnection(source),
            new NumberStreamController(),
            []
        );
        const subscription = subscribe(result);

        expect(() => subscription.cancel()).not.toThrow();
        expect(() => subscription.cancel()).not.toThrow();
        expect(cancel).toHaveBeenCalledTimes(1);
    });

    it("relays an upstream error exactly once", () => {
        const controlled = controlledFlux();
        const errors: unknown[] = [];
        const result = processController(
            controllerConnection(controlled.source),
            new NumberStreamController(),
            []
        );
        subscribe(result, errors);
        const failure = new Error("responder failed");

        controlled.error(failure);
        controlled.error(new Error("late error"));

        expect(errors).toEqual([failure]);
        expect(controlled.cancel).not.toHaveBeenCalled();
    });

    it("cancels upstream and fails once when response decoding throws", () => {
        const controlled = controlledFlux();
        const failure = new Error("decode failed");
        class FailingController extends NumberStreamController {
            /** Fails synchronously while decoding one responder payload. */
            protected override response(): number {
                throw failure;
            }
        }
        const errors: unknown[] = [];
        const values: number[] = [];
        const result = processController(
            controllerConnection(controlled.source),
            new FailingController(),
            []
        );
        const subscription = subscribe(result, errors, values);
        subscription.request(1);

        controlled.next(payload(1));
        controlled.next(payload(2));
        controlled.error(new Error("late error"));

        expect(values).toEqual([]);
        expect(errors).toEqual([failure]);
        expect(controlled.cancel).toHaveBeenCalledTimes(1);
    });
});

/** Creates a manually controlled RSocket source and records raw demand signals. */
function controlledFlux(): {
    readonly source: RSocketFlux;
    readonly requests: number[];
    readonly cancel: ReturnType<typeof vi.fn>;
    next(value: RSocketPayloadFrame): void;
    error(error: unknown): void;
} {
    const requests: number[] = [];
    const cancel = vi.fn();
    let subscriber: Subscriber<RSocketPayloadFrame> | undefined;
    const source = new RSocketFlux((nextSubscriber) => {
        subscriber = nextSubscriber;
        return {
            request: (n) => requests.push(n),
            cancel
        };
    });
    return {
        source,
        requests,
        cancel,
        next: (value) => subscriber?.onNext(value),
        error: (error) => subscriber?.onError(error)
    };
}

/** Builds the minimal controller connection around one controlled stream. */
function controllerConnection(source: RSocketFlux): RSocketControllerConnection {
    return {
        fireAndForget: () => Mono.empty(),
        requestResponse: () => Mono.error(new Error("requestResponse is not used by this test")),
        requestStream: () => source,
        requestChannel: () => source
    };
}

/** Subscribes without implicit unbounded demand and returns the captured subscription. */
function subscribe<T>(source: Flux<T>, errors: unknown[] = [], values: T[] = []): Subscription {
    let subscription: Subscription | undefined;
    source.subscribe({
        onSubscribe: (nextSubscription) => {
            subscription = nextSubscription;
        },
        onNext: (value) => values.push(value),
        onError: (error) => errors.push(error),
        onComplete: () => undefined
    });
    if (subscription === undefined) throw new Error("Expected a synchronous controller subscription");
    return subscription;
}

/** Creates a structural response payload sufficient for controller decoding. */
function payload(data: unknown): RSocketPayloadFrame {
    return {data} as RSocketPayloadFrame;
}
