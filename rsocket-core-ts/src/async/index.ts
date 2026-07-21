/** Shared asynchronous adaptation primitives for RSocket endpoints. */
import type {Publisher} from "reactor-core-ts";
export {cancelSubscription, observeAbort} from "@/async/cancel.js";
export {AsyncQueue} from "@/async/queue.js";
export {createPullAsyncIterable, type PullSubscription} from "@/async/pull.js";

/** Source forms accepted by Reactor endpoint adapters. */
export type PublisherInput<T> = Publisher<T> | Iterable<T> | AsyncIterable<T> | PromiseLike<T>;

/** Detects a Reactive Streams publisher through its required operation. */
export function isPublisher<T = unknown>(value: unknown): value is Publisher<T> {
    return typeof value === "object" && value !== null &&
        typeof (value as {subscribe?: unknown}).subscribe === "function";
}

/** Detects a promise or compatible thenable without invoking it. */
export function isPromiseLike<T = unknown>(value: unknown): value is PromiseLike<T> {
    return (typeof value === "object" && value !== null || typeof value === "function") &&
        typeof (value as {then?: unknown}).then === "function";
}
