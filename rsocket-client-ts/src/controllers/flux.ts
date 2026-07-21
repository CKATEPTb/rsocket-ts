/**
 * Demand-preserving Flux transformations used by declarative controllers.
 *
 * RSocket response publishers have a meaningful Reactive Streams subscription:
 * the first downstream request becomes `initialRequestN` on the wire. Adapting
 * them through an async iterator would replace every request with `request(1)`,
 * so controller transforms proxy subscriber signals directly instead.
 */
import {Context, Flux, type Subscriber, type Subscription} from "reactor-core-ts";
import {cancelSubscription} from "rsocket-core-ts";

/** Side effects attached to transformed Flux signals. */
export interface FluxTransformHooks<T> {
    /** Runs when the upstream subscription is attached. */
    readonly onSubscribe?: () => void;
    /** Runs before a transformed value is delivered downstream. */
    readonly onNext?: (value: T) => void;
    /** Runs before an error is delivered downstream. */
    readonly onError?: (error: unknown) => void;
    /** Runs before successful completion is delivered downstream. */
    readonly onComplete?: () => void;
}

/** Subscriber extension understood by Reactor context-aware publishers. */
type ContextSubscriber<T> = Subscriber<T> & {
    /** Returns the downstream Reactor context. */
    currentContext?(): Context;
};

/**
 * Maps a Flux while forwarding every downstream demand and cancellation signal
 * unchanged to the original publisher.
 */
export function transformFluxPreservingDemand<Input, Output>(
    source: Flux<Input>,
    transform: (value: Input) => Output,
    hooks?: FluxTransformHooks<Output>
): Flux<Output> {
    return new DemandPreservingTransformFlux(source, transform, hooks);
}

/** Flux implementation with a direct Reactive Streams subscription path. */
class DemandPreservingTransformFlux<Input, Output> extends Flux<Output> {
    /** Creates a demand-preserving transformed view of the source. */
    constructor(
        private readonly source: Flux<Input>,
        private readonly mapper: (value: Input) => Output,
        private readonly hooks?: FluxTransformHooks<Output>
    ) {
        super((signal, context) => transformIterable(source, mapper, hooks, signal, context));
    }

    /** Proxies signals without adapting the source to an async iterator. */
    protected override subscribeActual(subscriber: Subscriber<Output>): void {
        const bridge = new TransformSubscriber(subscriber, this.mapper, this.hooks);
        this.source.subscribe(bridge);
    }
}

/** Subscriber bridge that owns terminal-state and subscription forwarding. */
class TransformSubscriber<Input, Output> implements Subscriber<Input> {
    private upstream: Subscription | undefined;
    private terminated = false;

    /** Subscription exposed downstream while preserving request batching. */
    private readonly downstreamSubscription: Subscription = {
        request: (n) => {
            if (!this.terminated) this.upstream?.request(n);
        },
        cancel: () => {
            if (this.terminated) return;
            this.terminated = true;
            const upstream = this.upstream;
            this.upstream = undefined;
            cancelSubscription(upstream);
        }
    };

    /** Creates a bridge for one downstream subscription. */
    constructor(
        private readonly downstream: Subscriber<Output>,
        private readonly mapper: (value: Input) => Output,
        private readonly hooks?: FluxTransformHooks<Output>
    ) {
    }

    /** Preserves the downstream Reactor context for context-aware sources. */
    currentContext(): Context {
        return (this.downstream as ContextSubscriber<Output>).currentContext?.() ?? Context.empty();
    }

    /** Attaches upstream before exposing the forwarding subscription downstream. */
    onSubscribe(subscription: Subscription): void {
        if (this.upstream !== undefined || this.terminated) {
            cancelSubscription(subscription);
            return;
        }
        this.upstream = subscription;
        try {
            this.hooks?.onSubscribe?.();
        } catch (error) {
            this.failBeforeDownstreamSubscription(error);
            return;
        }
        try {
            this.downstream.onSubscribe(this.downstreamSubscription);
        } catch {
            this.terminated = true;
            this.upstream = undefined;
            cancelSubscription(subscription);
        }
    }

    /** Transforms one value and terminates the stream when decoding fails. */
    onNext(value: Input): void {
        if (this.terminated) return;
        try {
            const transformed = this.mapper(value);
            this.hooks?.onNext?.(transformed);
            this.downstream.onNext(transformed);
        } catch (error) {
            this.fail(error, true);
        }
    }

    /** Relays an upstream failure exactly once. */
    onError(error: unknown): void {
        this.fail(error, false);
    }

    /** Relays successful completion exactly once. */
    onComplete(): void {
        if (this.terminated) return;
        try {
            this.hooks?.onComplete?.();
        } catch (error) {
            this.fail(error, false);
            return;
        }
        this.terminated = true;
        this.upstream = undefined;
        try {
            this.downstream.onComplete();
        } catch {
            // Terminal subscriber callbacks cannot affect an already completed source.
        }
    }

    /** Cancels when needed and reports a failure exactly once. */
    private fail(error: unknown, cancelUpstream: boolean): void {
        if (this.terminated) return;
        this.terminated = true;
        const upstream = this.upstream;
        this.upstream = undefined;
        if (cancelUpstream) {
            cancelSubscription(upstream);
        }
        try {
            this.hooks?.onError?.(error);
        } catch {
            // Diagnostic hooks cannot replace the original stream failure.
        }
        try {
            this.downstream.onError(error);
        } catch {
            // The stream is already terminal and user callbacks must not escape.
        }
    }

    /** Preserves onSubscribe-before-terminal ordering when setup hooks fail. */
    private failBeforeDownstreamSubscription(error: unknown): void {
        this.terminated = true;
        const upstream = this.upstream;
        this.upstream = undefined;
        cancelSubscription(upstream);
        try {
            this.downstream.onSubscribe(this.downstreamSubscription);
        } catch {
            return;
        }
        try {
            this.hooks?.onError?.(error);
        } catch {
            // Diagnostic hooks cannot replace the original setup failure.
        }
        try {
            this.downstream.onError(error);
        } catch {
            // The stream is already terminal and user callbacks must not escape.
        }
    }
}

/** Async-iteration fallback for consumers that intentionally pull one value at a time. */
async function* transformIterable<Input, Output>(
    source: Flux<Input>,
    transform: (value: Input) => Output,
    hooks: FluxTransformHooks<Output> | undefined,
    signal: AbortSignal,
    context: Context
): AsyncGenerator<Output> {
    try {
        hooks?.onSubscribe?.();
        for await (const value of source.iterate(signal, context)) {
            const transformed = transform(value);
            hooks?.onNext?.(transformed);
            yield transformed;
        }
        hooks?.onComplete?.();
    } catch (error) {
        try {
            hooks?.onError?.(error);
        } catch {
            // Diagnostic hooks cannot replace the original stream failure.
        }
        throw error;
    }
}
