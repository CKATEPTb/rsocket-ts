/** Pull-based adaptation shared by requester and responder publishers. */
import type {Subscriber, Subscription} from "reactor-core-ts";
import {cancelSubscription, observeAbort} from "@/async/cancel.js";
import {AsyncQueue} from "@/async/queue.js";

/** Subscription setup invoked once for each async iterator. */
export type PullSubscription<T> = (subscriber: Subscriber<T>) => void;

/**
 * Adapts callback-based Reactive Streams signals to one-credit-per-pull async iteration.
 *
 * Every `next()` grants exactly one item of upstream demand. Cancellation,
 * setup failures, duplicate subscriptions, and throwing `request()` methods are
 * contained and release both the subscription and abort listener.
 */
export function createPullAsyncIterable<T>(
    signal: AbortSignal,
    subscribe: PullSubscription<T>
): AsyncIterable<T> {
    return {
        [Symbol.asyncIterator](): AsyncIterator<T> {
            const queue = new AsyncQueue<T>();
            let subscription: Subscription | undefined;
            let releaseAbort: (() => void) | undefined;
            let pendingDemand = 0;
            let outstandingDemand = 0;
            let terminated = false;

            const cleanup = (): void => {
                const release = releaseAbort;
                releaseAbort = undefined;
                release?.();
            };
            const cancel = (): Promise<IteratorResult<T>> => {
                if (!terminated) {
                    terminated = true;
                    pendingDemand = 0;
                    outstandingDemand = 0;
                    cleanup();
                    const upstream = subscription;
                    subscription = undefined;
                    cancelSubscription(upstream);
                }
                return queue.return();
            };
            const fail = (error: unknown, cancelUpstream: boolean): void => {
                if (terminated) return;
                terminated = true;
                pendingDemand = 0;
                outstandingDemand = 0;
                cleanup();
                const upstream = subscription;
                subscription = undefined;
                if (cancelUpstream) cancelSubscription(upstream);
                queue.error(error);
            };
            const complete = (): void => {
                if (terminated) return;
                terminated = true;
                pendingDemand = 0;
                outstandingDemand = 0;
                cleanup();
                subscription = undefined;
                queue.complete();
            };
            const request = (value: number): void => {
                if (terminated) return;
                outstandingDemand = Math.min(Number.MAX_SAFE_INTEGER, outstandingDemand + value);
                if (subscription === undefined) {
                    pendingDemand = Math.min(Number.MAX_SAFE_INTEGER, pendingDemand + value);
                    return;
                }
                try {
                    subscription.request(value);
                } catch (error) {
                    fail(error, true);
                }
            };

            const subscriber: Subscriber<T> = {
                onSubscribe(value) {
                    if (terminated || subscription !== undefined) {
                        cancelSubscription(value);
                        return;
                    }
                    subscription = value;
                    const demand = pendingDemand;
                    pendingDemand = 0;
                    if (demand > 0) {
                        try {
                            value.request(demand);
                        } catch (error) {
                            fail(error, true);
                        }
                    }
                },
                onNext(value) {
                    if (terminated) return;
                    if (subscription === undefined || outstandingDemand <= 0) {
                        fail(new Error("Reactive Streams publisher emitted without demand"), true);
                        return;
                    }
                    outstandingDemand -= 1;
                    queue.push(value);
                },
                onError(error) {
                    fail(error, false);
                },
                onComplete() {
                    complete();
                }
            };

            if (signal.aborted) {
                terminated = true;
                queue.complete();
            } else {
                try {
                    subscribe(subscriber);
                    if (!terminated) {
                        const release = observeAbort(signal, () => {
                            void cancel();
                        });
                        if (terminated) release();
                        else releaseAbort = release;
                    }
                } catch (error) {
                    fail(error, true);
                }
            }

            return {
                next() {
                    const result = queue.next();
                    request(1);
                    return result;
                },
                return: cancel,
                throw(error: unknown) {
                    fail(error, true);
                    return Promise.reject(error);
                }
            };
        }
    };
}
