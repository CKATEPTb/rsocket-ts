import type {Publisher, Subscriber, Subscription} from "reactor-core-ts";

/** Deterministic demand-aware publisher used by backpressure and Resume tests. */
export class ManualPublisher<T> implements Publisher<T> {
    private subscriber: Subscriber<T> | undefined;
    private demand = 0;
    requested = 0;
    cancelled = false;

    /** Attaches one subscriber and exposes demand/cancellation counters. */
    subscribe(subscriber: Subscriber<T>): void {
        if (this.subscriber !== undefined) throw new Error("ManualPublisher supports one subscriber");
        this.subscriber = subscriber;
        const subscription: Subscription = {
            request: (n) => {
                this.demand += n;
                this.requested += n;
            },
            cancel: () => {
                this.cancelled = true;
                this.subscriber = undefined;
            }
        };
        subscriber.onSubscribe(subscription);
    }

    /** Emits one value only when downstream demand is available. */
    next(value: T): void {
        if (this.cancelled || this.subscriber === undefined) return;
        if (this.demand <= 0) throw new Error("ManualPublisher emitted without demand");
        this.demand -= 1;
        this.subscriber.onNext(value);
    }

    /** Completes the attached subscriber. */
    complete(): void {
        const subscriber = this.subscriber;
        this.subscriber = undefined;
        subscriber?.onComplete();
    }

    /** Fails the attached subscriber. */
    error(error: unknown): void {
        const subscriber = this.subscriber;
        this.subscriber = undefined;
        subscriber?.onError(error);
    }
}
