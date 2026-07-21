/** Resource tracking for asynchronous fire-and-forget controller work. */
import type {Subscription} from "reactor-core-ts";
import {cancelSubscription, isPromiseLike, isPublisher, MAX_REQUEST_N} from "rsocket-core-ts";

/** Retains cancellable FNF publishers until they terminate or the session closes. */
export class ServerBackgroundWork {
    private readonly subscriptions = new Set<Subscription>();
    private closed = false;

    /** Starts an optional publisher or promise returned by an FNF controller. */
    consume(result: unknown): void {
        if (result === undefined || result === null) return;
        if (isPublisher(result)) {
            let subscription: Subscription | undefined;
            let terminated = false;
            const terminate = (): void => {
                if (terminated) return;
                terminated = true;
                this.release(subscription);
            };
            try {
                result.subscribe({
                    onSubscribe: (value) => {
                        if (terminated || subscription !== undefined) {
                            cancelSubscription(value);
                            return;
                        }
                        subscription = value;
                        if (this.closed) {
                            terminated = true;
                            cancelSubscription(value);
                            return;
                        }
                        this.subscriptions.add(value);
                        try {
                            value.request(MAX_REQUEST_N);
                        } catch {
                            terminate();
                            cancelSubscription(value);
                        }
                    },
                    onNext: () => {
                        if (subscription === undefined) terminate();
                    },
                    onError: terminate,
                    onComplete: terminate
                });
            } catch {
                if (!terminated) {
                    terminated = true;
                    this.release(subscription);
                    if (subscription !== undefined) {
                        cancelSubscription(subscription);
                    }
                }
            }
            return;
        }
        if (isPromiseLike(result)) void Promise.resolve(result).catch(() => undefined);
    }

    /** Cancels every retained publisher exactly once. */
    close(): void {
        if (this.closed) return;
        this.closed = true;
        for (const subscription of this.subscriptions) cancelSubscription(subscription);
        this.subscriptions.clear();
    }

    /** Removes one terminal publisher subscription. */
    private release(subscription: Subscription | undefined): void {
        if (subscription !== undefined) this.subscriptions.delete(subscription);
    }
}
