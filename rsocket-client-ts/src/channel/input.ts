/**
 * Request-channel input adaptation helpers.
 */
import {Flux} from "reactor-core-ts";
import type {RSocketPayloadInput} from "rsocket-core-ts";
import type {RSocketChannelInput} from "@/client/types.js";

/** Shared terminal iterator result for synchronous iterable adapters. */
const DONE_RESULT = Object.freeze({done: true, value: undefined as never});
/** Shared resolved terminal result to avoid allocating on every adapter return. */
const DONE_PROMISE = Promise.resolve(DONE_RESULT);

/**
 * Iterator returned for request-channel sources.
 */
export type RSocketChannelInputIterator<D = unknown, M = unknown> =
    | Iterator<RSocketPayloadInput<D, M>>
    | AsyncIterator<RSocketPayloadInput<D, M>>;

/**
 * Returns an async iterable for any supported request-channel input.
 */
export function channelInputIterable<D, M>(
    input: RSocketChannelInput<D, M>
): AsyncIterable<RSocketPayloadInput<D, M>> {
    if (isChannelInputAsyncIterable<RSocketPayloadInput<D, M>>(input)) return input;
    if (isChannelInputIterable<RSocketPayloadInput<D, M>>(input)) return iterableAsAsync(input);
    return Flux.from(input) as AsyncIterable<RSocketPayloadInput<D, M>>;
}

/**
 * Returns the cheapest iterator for a request-channel input source.
 */
export function channelInputIterator<D, M>(
    input: RSocketChannelInput<D, M>
): RSocketChannelInputIterator<D, M> {
    if (isChannelInputAsyncIterable<RSocketPayloadInput<D, M>>(input)) return input[Symbol.asyncIterator]();
    if (isChannelInputIterable<RSocketPayloadInput<D, M>>(input)) return input[Symbol.iterator]();
    return (Flux.from(input) as AsyncIterable<RSocketPayloadInput<D, M>>)[Symbol.asyncIterator]();
}

/**
 * Detects inputs that already expose async iteration.
 */
export function isChannelInputAsyncIterable<T>(input: unknown): input is AsyncIterable<T> {
    return typeof input === "object"
        && input !== null
        && typeof (input as Partial<AsyncIterable<T>>)[Symbol.asyncIterator] === "function";
}

/**
 * Detects synchronous iterable inputs such as arrays.
 */
export function isChannelInputIterable<T>(input: unknown): input is Iterable<T> {
    return typeof input === "object"
        && input !== null
        && typeof (input as Partial<Iterable<T>>)[Symbol.iterator] === "function";
}

/**
 * Prepends one initial payload while preserving the source's cheapest iteration path.
 */
export function prependChannelPayload<D, M>(
    payload: RSocketPayloadInput<D, M>,
    input: RSocketChannelInput<D, M>
): RSocketChannelInput<D, M> {
    if (!isChannelInputAsyncIterable(input) && isChannelInputIterable<RSocketPayloadInput<D, M>>(input)) {
        return prependChannelPayloadSync(payload, input);
    }
    return prependChannelPayloadAsync(payload, input);
}

/** Prepends one payload to every iteration of a reusable synchronous source. */
function prependChannelPayloadSync<D, M>(
    payload: RSocketPayloadInput<D, M>,
    input: Iterable<RSocketPayloadInput<D, M>>
): Iterable<RSocketPayloadInput<D, M>> {
    return {
        /** Creates an independent iterator for each request-channel subscription. */
        *[Symbol.iterator]() {
            yield payload;
            yield* input;
        }
    };
}

/** Prepends one payload to every iteration of an asynchronous or publisher-backed source. */
function prependChannelPayloadAsync<D, M>(
    payload: RSocketPayloadInput<D, M>,
    input: RSocketChannelInput<D, M>
): AsyncIterable<RSocketPayloadInput<D, M>> {
    return {
        /** Creates an independent async iterator for each request-channel subscription. */
        async *[Symbol.asyncIterator]() {
            yield payload;
            for await (const item of channelInputIterable(input)) yield item;
        }
    };
}

/**
 * Wraps a synchronous iterator in the async iterator contract.
 */
function iterableAsAsync<T>(input: Iterable<T>): AsyncIterable<T> {
    return {
        [Symbol.asyncIterator](): AsyncIterator<T> {
            const iterator = input[Symbol.iterator]();
            return {
                next: () => Promise.resolve(iterator.next()),
                return: () => {
                    try {
                        iterator.return?.();
                        return DONE_PROMISE as Promise<IteratorReturnResult<T>>;
                    } catch (error) {
                        return Promise.reject(error);
                    }
                }
            };
        }
    };
}
