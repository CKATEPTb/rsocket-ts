/** Independent class-controller dispatch tests for the client package. */
import {Mono, type Subscriber, type Subscription} from "reactor-core-ts";
import {describe, expect, it} from "vitest";
import {Metadata, WellKnownMimeType} from "rsocket-frames-ts";
import {
    type RSocketPayload,
    type RSocketPayloadFrame,
    type RSocketPayloadInput
} from "rsocket-core-ts";
import {
    FireAndForgetController,
    RequestChannelController,
    RequestResponseController,
    RequestStreamController,
    RSocketControllerProcessor
} from "@/controllers/index.js";
import type {RSocketControllerConnection} from "@/controllers/types.js";
import type {
    RSocketChannelInput,
    RSocketRequestOptions,
    RSocketStreamRequestOptions
} from "@/client/types.js";
import {RSocketFlux} from "@/stream/index.js";

/** Fire-and-forget declaration used by processor dispatch tests. */
class AuditController extends FireAndForgetController<{id: number}> {
    protected readonly route = "audit.write";
}

/** Request-response declaration used by processor dispatch tests. */
class UserController extends RequestResponseController<{id: number}, {name: string}> {
    protected readonly route = "user.find";
}

/** Request-stream declaration used by processor dispatch tests. */
class NumberController extends RequestStreamController<void, number> {
    protected readonly route = "number.watch";
}

/** Request-channel declaration used by processor dispatch tests. */
class ChannelController extends RequestChannelController<number, number> {
    protected readonly route = "number.channel";
}

describe("RSocketControllerProcessor", () => {
    it("dispatches one-shot controllers with route metadata, data, and request options", async () => {
        const connection = new RecordingConnection();
        const processor = new RSocketControllerProcessor(connection);
        const options = {dataMimeType: WellKnownMimeType.TEXT_PLAIN};

        await processor.process(new AuditController(options), {id: 7}).block();
        expect(connection.kind).toBe("fireAndForget");
        expect(payloadData(connection.payload)).toEqual({id: 7});
        expect(payloadRoutes(connection.payload)).toEqual(["audit.write"]);
        expect(connection.options).toBe(options);

        const response = await processor.process(UserController, {id: 9}).block();
        expect(connection.kind).toBe("requestResponse");
        expect(payloadData(connection.payload)).toEqual({id: 9});
        expect(payloadRoutes(connection.payload)).toEqual(["user.find"]);
        expect(response).toEqual({name: "Ada"});
    });

    it("decodes request-stream values while preserving subscriber demand", () => {
        const connection = new RecordingConnection();
        const processor = new RSocketControllerProcessor(connection);
        const values: number[] = [];
        let subscription: Subscription | undefined;

        processor.process(NumberController).subscribe({
            onSubscribe: (value) => {
                subscription = value;
            },
            onNext: (value) => values.push(value),
            onError: () => undefined,
            onComplete: () => undefined
        });
        subscription?.request(2);

        expect(connection.kind).toBe("requestStream");
        expect(payloadRoutes(connection.payload)).toEqual(["number.watch"]);
        expect(connection.requested).toEqual([2]);
        expect(values).toEqual([1, 2]);
    });

    it("prepends channel route metadata without consuming the caller input early", () => {
        const connection = new RecordingConnection();
        const processor = new RSocketControllerProcessor(connection);
        let iterations = 0;
        const input: Iterable<RSocketPayloadInput<number, unknown>> = {
            *[Symbol.iterator]() {
                iterations += 1;
                yield {data: 1};
                yield {data: 2};
            }
        };

        processor.process(ChannelController, input);
        expect(iterations).toBe(0);
        const values = synchronousInput(connection.channelInput);

        expect(payloadRoutes(values[0])).toEqual(["number.channel"]);
        expect(payloadData(values[1])).toBe(1);
        expect(payloadData(values[2])).toBe(2);
        expect(iterations).toBe(1);

        expect(synchronousInput(connection.channelInput)).toHaveLength(3);
        expect(iterations).toBe(2);
    });

    it("creates one controller instance per processor and class", async () => {
        let constructions = 0;
        class CachedController extends RequestResponseController<void, {name: string}> {
            protected readonly route = "cached";

            /** Counts materialization by the processor cache. */
            constructor() {
                super();
                constructions += 1;
            }
        }
        const processor = new RSocketControllerProcessor(new RecordingConnection());

        await processor.process(CachedController).block();
        await processor.process(CachedController).block();

        expect(constructions).toBe(1);
    });

    it("rejects empty and oversized route segments before calling the connection", () => {
        class EmptyRouteController extends RequestResponseController<void, unknown> {
            protected readonly route = "";
        }
        class OversizedRouteController extends RequestResponseController<void, unknown> {
            protected readonly route = "x".repeat(256);
        }
        const connection = new RecordingConnection();
        const processor = new RSocketControllerProcessor(connection);

        expect(() => processor.process(EmptyRouteController)).toThrow("non-empty");
        expect(() => processor.process(OversizedRouteController)).toThrow("at most 255 UTF-8 bytes");
        expect(connection.kind).toBeUndefined();
    });
});

/** Minimal connection that records dispatch and supplies deterministic responses. */
class RecordingConnection implements RSocketControllerConnection {
    kind: "fireAndForget" | "requestResponse" | "requestStream" | "requestChannel" | undefined;
    payload: RSocketPayloadInput<any, any> | undefined;
    channelInput: RSocketChannelInput<any, any> | undefined;
    options: RSocketRequestOptions | undefined;
    readonly requested: number[] = [];

    /** Records a fire-and-forget request. */
    fireAndForget(payload: RSocketPayloadInput<any, any>, options?: RSocketRequestOptions): Mono<void> {
        this.record("fireAndForget", payload, options);
        return Mono.empty();
    }

    /** Records a request-response request and returns one body. */
    requestResponse(
        payload: RSocketPayloadInput<any, any>,
        options?: RSocketRequestOptions
    ): Mono<RSocketPayloadFrame> {
        this.record("requestResponse", payload, options);
        return Mono.just({data: {name: "Ada"}} as RSocketPayloadFrame);
    }

    /** Records a request-stream request and returns two values. */
    requestStream(
        payload: RSocketPayloadInput<any, any>,
        options?: RSocketStreamRequestOptions
    ): RSocketFlux {
        this.record("requestStream", payload, options);
        return payloadFlux([1, 2], this.requested);
    }

    /** Records request-channel input and returns two values. */
    requestChannel(
        payloads: RSocketChannelInput<any, any>,
        options?: RSocketStreamRequestOptions
    ): RSocketFlux {
        this.kind = "requestChannel";
        this.channelInput = payloads;
        this.options = options;
        return payloadFlux([1, 2], this.requested);
    }

    /** Stores one single-payload interaction call. */
    private record(
        kind: Exclude<RecordingConnection["kind"], "requestChannel" | undefined>,
        payload: RSocketPayloadInput<any, any>,
        options?: RSocketRequestOptions
    ): void {
        this.kind = kind;
        this.payload = payload;
        this.options = options;
    }
}

/** Creates a demand-controlled payload source for controller result mapping. */
function payloadFlux(values: readonly unknown[], requested: number[]): RSocketFlux {
    return new RSocketFlux((subscriber: Subscriber<RSocketPayloadFrame>) => {
        let index = 0;
        let cancelled = false;
        return {
            request: (n) => {
                requested.push(n);
                const limit = Math.min(values.length, index + n);
                while (!cancelled && index < limit) {
                    subscriber.onNext({data: values[index]} as RSocketPayloadFrame);
                    index += 1;
                }
                if (!cancelled && index === values.length) subscriber.onComplete();
            },
            cancel: () => {
                cancelled = true;
            }
        };
    });
}

/** Reads a payload envelope's application data. */
function payloadData(input: RSocketPayloadInput<any, any> | undefined): unknown {
    return (input as RSocketPayload<any, any> | undefined)?.data;
}

/** Reads routing tags from a controller-generated payload. */
function payloadRoutes(input: RSocketPayloadInput<any, any> | undefined): readonly string[] | undefined {
    const value = (input as RSocketPayload<any, any> | undefined)?.metadata;
    return value instanceof Metadata ? value.payload as readonly string[] : undefined;
}

/** Materializes the synchronous channel input produced for an iterable source. */
function synchronousInput(input: RSocketChannelInput<any, any> | undefined): RSocketPayloadInput<any, any>[] {
    if (input === undefined || typeof (input as Partial<Iterable<unknown>>)[Symbol.iterator] !== "function") {
        throw new Error("Expected a synchronous controller channel input");
    }
    return Array.from(input as Iterable<RSocketPayloadInput<any, any>>);
}
