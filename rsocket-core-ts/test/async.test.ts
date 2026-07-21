/** Async adaptation tests shared by requester and responder runtimes. */
import {Flux, type Subscriber} from "reactor-core-ts";
import {describe, expect, it, vi} from "vitest";
import {
    AsyncQueue,
    createPullAsyncIterable,
    isPromiseLike,
    isPublisher,
    observeAbort
} from "@";

describe("Core async primitives", () => {
    it("preserves queued undefined values and insertion order at volume", async () => {
        const queue = new AsyncQueue<number | undefined>();
        queue.push(undefined);
        for (let index = 0; index < 5_000; index += 1) queue.push(index);

        await expect(queue.next()).resolves.toEqual({done: false, value: undefined});
        for (let index = 0; index < 5_000; index += 1) {
            await expect(queue.next()).resolves.toEqual({done: false, value: index});
        }
    });

    it("resolves pending readers in order and completes all remaining readers", async () => {
        const queue = new AsyncQueue<number>();
        const reads = Array.from({length: 2_000}, () => queue.next());
        for (let index = 0; index < 1_999; index += 1) queue.push(index);
        queue.complete();

        const results = await Promise.all(reads);
        expect(results.slice(0, -1).map(({value}) => value))
            .toEqual(Array.from({length: 1_999}, (_value, index) => index));
        expect(results.at(-1)).toEqual({done: true, value: undefined});
        await expect(queue.next()).resolves.toEqual({done: true, value: undefined});
    });

    it("drains buffered values before a terminal error", async () => {
        const queue = new AsyncQueue<number>();
        const failure = new Error("boom");
        queue.push(1);
        queue.error(failure);

        await expect(queue.next()).resolves.toEqual({done: false, value: 1});
        await expect(queue.next()).rejects.toBe(failure);
    });

    it("releases queued values and pending readers on consumer return", async () => {
        const queue = new AsyncQueue<number>();
        const pending = queue.next();

        await expect(queue.return()).resolves.toEqual({done: true, value: undefined});
        await expect(pending).resolves.toEqual({done: true, value: undefined});
        await expect(queue.next()).resolves.toEqual({done: true, value: undefined});
    });

    it("grants one publisher credit per iterator pull and cancels on abort", async () => {
        const controller = new AbortController();
        const requests: number[] = [];
        const cancel = vi.fn();
        let subscriber: Subscriber<number> | undefined;
        const iterable = createPullAsyncIterable(controller.signal, (value) => {
            subscriber = value;
            value.onSubscribe({request: (n) => requests.push(n), cancel});
        });
        const iterator = iterable[Symbol.asyncIterator]();

        const first = iterator.next();
        expect(requests).toEqual([1]);
        subscriber?.onNext(7);
        await expect(first).resolves.toEqual({done: false, value: 7});

        controller.abort();
        expect(cancel).toHaveBeenCalledOnce();
        await expect(iterator.next()).resolves.toEqual({done: true, value: undefined});
    });

    it("does not subscribe when the pull signal is already aborted", async () => {
        const controller = new AbortController();
        const subscribe = vi.fn();
        controller.abort();

        const iterator = createPullAsyncIterable(controller.signal, subscribe)[Symbol.asyncIterator]();

        await expect(iterator.next()).resolves.toEqual({done: true, value: undefined});
        expect(subscribe).not.toHaveBeenCalled();
    });

    it("replays pull demand when onSubscribe arrives late", () => {
        const requests: number[] = [];
        let subscriber: Subscriber<number> | undefined;
        const iterator = createPullAsyncIterable(new AbortController().signal, (value) => {
            subscriber = value;
        })[Symbol.asyncIterator]();

        void iterator.next();
        void iterator.next();
        subscriber?.onSubscribe({request: (n) => requests.push(n), cancel: () => undefined});

        expect(requests).toEqual([2]);
        void iterator.return?.();
    });

    it("rejects publisher emissions beyond granted iterator demand", async () => {
        const cancel = vi.fn();
        const iterator = createPullAsyncIterable(new AbortController().signal, (subscriber) => {
            subscriber.onSubscribe({
                request() {
                    subscriber.onNext(1);
                    subscriber.onNext(2);
                },
                cancel
            });
        })[Symbol.asyncIterator]();

        await expect(iterator.next()).resolves.toEqual({done: false, value: 1});
        await expect(iterator.next()).rejects.toThrow("emitted without demand");
        expect(cancel).toHaveBeenCalledOnce();
    });

    it("rejects iterator reads and cancels when upstream demand throws", async () => {
        const failure = new Error("request failed");
        const cancel = vi.fn();
        const iterator = createPullAsyncIterable(new AbortController().signal, (subscriber) => {
            subscriber.onSubscribe({
                request: () => {
                    throw failure;
                },
                cancel
            });
        })[Symbol.asyncIterator]();

        await expect(iterator.next()).rejects.toBe(failure);
        expect(cancel).toHaveBeenCalledOnce();
    });

    it("recognizes publishers and thenables without invoking them", () => {
        const publisher = Flux.just(1);
        const thenable = {then: () => undefined};

        expect(isPublisher(publisher)).toBe(true);
        expect(isPublisher({subscribe: 1})).toBe(false);
        expect(isPromiseLike(Promise.resolve())).toBe(true);
        expect(isPromiseLike(thenable)).toBe(true);
        expect(isPromiseLike(null)).toBe(false);
    });

    it("observes an already-aborted signal without retaining a listener", () => {
        const listener = vi.fn();
        const addEventListener = vi.fn();
        const signal = {
            aborted: true,
            addEventListener,
            removeEventListener: vi.fn()
        } as unknown as AbortSignal;

        const cleanup = observeAbort(signal, listener);
        cleanup();

        expect(listener).toHaveBeenCalledOnce();
        expect(addEventListener).not.toHaveBeenCalled();
    });

    it("rolls back an abort listener when custom registration retains it before throwing", () => {
        const failure = new Error("registration failed");
        let registered: EventListenerOrEventListenerObject | undefined;
        const signal = {
            aborted: false,
            addEventListener(_type: string, listener: EventListenerOrEventListenerObject) {
                registered = listener;
                throw failure;
            },
            removeEventListener(_type: string, listener: EventListenerOrEventListenerObject) {
                if (registered === listener) registered = undefined;
            }
        } as unknown as AbortSignal;

        expect(() => observeAbort(signal, () => undefined)).toThrow(failure);
        expect(registered).toBeUndefined();
    });

    it("delivers abort once and makes explicit cleanup idempotent", () => {
        const listeners = new Set<EventListenerOrEventListenerObject>();
        const listener = vi.fn();
        const signal = {
            aborted: false,
            addEventListener(_type: string, next: EventListenerOrEventListenerObject) {
                listeners.add(next);
            },
            removeEventListener(_type: string, next: EventListenerOrEventListenerObject) {
                listeners.delete(next);
            }
        } as unknown as AbortSignal;
        const cleanup = observeAbort(signal, listener);
        const registered = [...listeners][0] as EventListener;

        registered(new Event("abort"));
        registered(new Event("abort"));
        cleanup();
        cleanup();

        expect(listener).toHaveBeenCalledOnce();
        expect(listeners).toHaveLength(0);
    });
});
