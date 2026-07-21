/** Reactive physical-transport subscription lifecycle tests. */
import {Flux, Mono, type Subscriber, type Subscription} from "reactor-core-ts";
import {describe, expect, it} from "vitest";
import {
    ReactiveTransportBinding,
    type RSocketTransportClose,
    type RSocketTransportConnection
} from "@";

describe("Core reactive transport binding", () => {
    it("releases a synchronous frame subscription before attaching later listeners", () => {
        let frameCancelled = 0;
        let errorSubscriptions = 0;
        let closeSubscriptions = 0;
        let binding: ReactiveTransportBinding;
        const transport: RSocketTransportConnection = {
            opened: Mono.empty(),
            frames: new ImmediateFlux(new Uint8Array(6), () => frameCancelled += 1),
            errors: new CountingNeverFlux(() => errorSubscriptions += 1),
            closes: new CountingNeverFlux<RSocketTransportClose>(() => closeSubscriptions += 1),
            isOpen: true,
            write() {},
            close() {}
        };
        binding = new ReactiveTransportBinding(transport, {
            frame: () => binding.dispose(),
            frameError: () => undefined,
            error: () => undefined,
            close: () => undefined
        });

        binding.start();

        expect(frameCancelled).toBe(1);
        expect(errorSubscriptions).toBe(0);
        expect(closeSubscriptions).toBe(0);
    });

    it("routes frame-stream errors separately and replaces handlers atomically", () => {
        const frames = new ManualFlux<Uint8Array>();
        const transport: RSocketTransportConnection = {
            opened: Mono.empty(),
            frames,
            errors: Flux.never(),
            closes: Flux.never(),
            isOpen: true,
            write() {},
            close() {}
        };
        const calls: string[] = [];
        const binding = new ReactiveTransportBinding(transport, handler("first", calls));
        binding.start();
        binding.setHandler(handler("second", calls));

        frames.emitValue(new Uint8Array(6));
        frames.emitFailure(new Error("decode"));

        expect(calls).toEqual(["second:frame", "second:frameError"]);
    });

    it("releases every subscription when one custom cancellation throws", () => {
        const cancelled: string[] = [];
        const transport: RSocketTransportConnection = {
            opened: Mono.empty(),
            frames: new CancellationFlux(() => cancelled.push("frames")),
            errors: new CancellationFlux(() => {
                cancelled.push("errors");
                throw new Error("cleanup failed");
            }),
            closes: new CancellationFlux<RSocketTransportClose>(() => cancelled.push("closes")),
            isOpen: true,
            write() {},
            close() {}
        };
        const binding = new ReactiveTransportBinding(transport, handler("unused", []));
        binding.start();

        expect(() => binding.dispose()).not.toThrow();
        expect(cancelled).toEqual(["closes", "errors", "frames"]);
    });

    it("releases earlier subscriptions when a later transport subscription throws", () => {
        const cancelled: string[] = [];
        const failure = new Error("error stream subscribe failed");
        const calls: string[] = [];
        const transport: RSocketTransportConnection = {
            opened: Mono.empty(),
            frames: new CancellationFlux(() => cancelled.push("frames")),
            errors: new ThrowingFlux(failure),
            closes: new CountingNeverFlux<RSocketTransportClose>(() => calls.push("closes:subscribed")),
            isOpen: true,
            write() {},
            close() {}
        };
        const binding = new ReactiveTransportBinding(transport, {
            ...handler("transport", calls),
            frameError: (error) => calls.push(error === failure ? "transport:failure" : "transport:other")
        });

        expect(() => binding.start()).not.toThrow();

        expect(cancelled).toEqual(["frames"]);
        expect(calls).toEqual(["transport:failure"]);
    });
});

function handler(prefix: string, calls: string[]) {
    return {
        frame: () => calls.push(`${prefix}:frame`),
        frameError: () => calls.push(`${prefix}:frameError`),
        error: () => calls.push(`${prefix}:error`),
        close: () => calls.push(`${prefix}:close`)
    };
}

class ImmediateFlux<T> extends Flux<T> {
    constructor(private readonly value: T, private readonly cancelled: () => void) {
        super(() => []);
    }

    protected override subscribeActual(subscriber: Subscriber<T>): void {
        let active = true;
        subscriber.onSubscribe({
            request: () => {
                if (active) subscriber.onNext(this.value);
            },
            cancel: () => {
                if (!active) return;
                active = false;
                this.cancelled();
            }
        });
    }
}

class CountingNeverFlux<T> extends Flux<T> {
    constructor(private readonly subscribed: () => void) {
        super(() => []);
    }

    protected override subscribeActual(subscriber: Subscriber<T>): void {
        this.subscribed();
        subscriber.onSubscribe({request() {}, cancel() {}});
    }
}

class CancellationFlux<T> extends Flux<T> {
    constructor(private readonly cancelled: () => void) {
        super(() => []);
    }

    protected override subscribeActual(subscriber: Subscriber<T>): void {
        subscriber.onSubscribe({request() {}, cancel: this.cancelled});
    }
}

class ThrowingFlux<T> extends Flux<T> {
    constructor(private readonly failure: unknown) {
        super(() => []);
    }

    protected override subscribeActual(): void {
        throw this.failure;
    }
}

class ManualFlux<T> extends Flux<T> {
    private subscriber: Subscriber<T> | undefined;

    constructor() {
        super(() => []);
    }

    protected override subscribeActual(subscriber: Subscriber<T>): void {
        this.subscriber = subscriber;
        const subscription: Subscription = {request() {}, cancel: () => this.subscriber = undefined};
        subscriber.onSubscribe(subscription);
    }

    emitValue(value: T): void {
        this.subscriber?.onNext(value);
    }

    emitFailure(error: unknown): void {
        this.subscriber?.onError(error);
    }
}
