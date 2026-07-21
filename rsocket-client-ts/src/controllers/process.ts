/**
 * Runtime executor for declarative controllers.
 *
 * It converts a typed controller object into the matching low-level RSocket
 * interaction and wires optional controller-local Reactor-style logging.
 */
import {type Flux, Mono} from "reactor-core-ts";
import {emitLog, interactionLogEvent} from "@/logging/emit.js";
import type {NormalizedRSocketLogOptions} from "@/logging/types.js";
import {
    controllerChannelInput,
    controllerDecoder,
    controllerPayload,
    controllerRuntime,
    type AnyClassController
} from "@/controllers/classes.js";
import type {
    ControllerArgs,
    ControllerReturn,
    RSocketControllerConnection,
    RSocketControllerKind
} from "@/controllers/types.js";
import type {RSocketPayloadFrame} from "rsocket-core-ts";
import type {RSocketChannelInput} from "@/client/types.js";
import {transformFluxPreservingDemand} from "@/controllers/flux.js";

/**
 * Executes a declarative controller against an RSocket connection.
 *
 * The return type is inferred from the controller kind: fire-and-forget returns
 * `Mono<void>`, request-response returns `Mono<Result>`, and streaming
 * interactions return `Flux<Result>`.
 */
export function processController<C extends AnyClassController>(
    connection: RSocketControllerConnection,
    controller: C,
    args: ControllerArgs<C>
): ControllerReturn<C> {
    const runtime = controllerRuntime(controller);
    switch (runtime.kind) {
        case "fireAndForget": {
            const payload = controllerPayload(controller, runtime, args);
            return logMono(
                connection.fireAndForget(payload, runtime.options),
                runtime.logging,
                runtime.kind,
                payload
            ) as ControllerReturn<C>;
        }
        case "requestResponse": {
            const payload = controllerPayload(controller, runtime, args);
            return logMono(
                decodeMono(connection.requestResponse(payload, runtime.options), controllerDecoder(controller, runtime)),
                runtime.logging,
                runtime.kind,
                payload
            ) as ControllerReturn<C>;
        }
        case "requestStream": {
            const payload = controllerPayload(controller, runtime, args);
            return logFlux(
                decodeFlux(connection.requestStream(payload, runtime.options), controllerDecoder(controller, runtime)),
                runtime.logging,
                runtime.kind,
                payload
            ) as ControllerReturn<C>;
        }
        case "requestChannel": {
            const input = controllerChannelInput(
                controller,
                runtime,
                args[0] as RSocketChannelInput<any, any>
            );
            return logFlux(
                decodeFlux(connection.requestChannel(input, runtime.options), controllerDecoder(controller, runtime)),
                runtime.logging,
                runtime.kind,
                input
            ) as ControllerReturn<C>;
        }
    }
}

/**
 * Applies a request-response controller decoder.
 */
function decodeMono<Result>(
    source: Mono<RSocketPayloadFrame>,
    decode: (payload: RSocketPayloadFrame) => Result
): Mono<Result> {
    return source.map(decode);
}

/**
 * Applies a streaming controller decoder.
 */
function decodeFlux<Result>(
    source: Flux<RSocketPayloadFrame>,
    decode: (payload: RSocketPayloadFrame) => Result
): Flux<Result> {
    return transformFluxPreservingDemand(source, decode);
}

/**
 * Adds controller interaction logs around a `Mono` without subscribing early.
 */
function logMono<T>(
    source: Mono<T>,
    logging: NormalizedRSocketLogOptions | undefined,
    interaction: RSocketControllerKind,
    payload: unknown
): Mono<T> {
    if (logging === undefined || !logging.enabled || !logging.interactions) return source;
    const loggedPayload = logging.payload ? payload : undefined;
    return source
        .doOnSubscribe(() => logInteraction(logging, interaction, "send", loggedPayload))
        .doOnNext((value) => logInteraction(logging, interaction, "receive", undefined, value))
        .doOnError((error) => logInteraction(logging, interaction, "error", undefined, undefined, error))
        .doFinally((signal) => {
            if (signal === "complete") logInteraction(logging, interaction, "complete");
        });
}

/**
 * Adds controller interaction logs to a `Flux` while preserving demand-driven
 * subscription behavior.
 */
function logFlux<T>(
    source: Flux<T>,
    logging: NormalizedRSocketLogOptions | undefined,
    interaction: RSocketControllerKind,
    payload: unknown
): Flux<T> {
    if (logging === undefined || !logging.enabled || !logging.interactions) return source;
    const loggedPayload = logging.payload ? payload : undefined;
    return transformFluxPreservingDemand(source, identity, {
        onSubscribe: () => logInteraction(logging, interaction, "send", loggedPayload),
        onNext: (value) => logInteraction(logging, interaction, "receive", undefined, value),
        onComplete: () => logInteraction(logging, interaction, "complete"),
        onError: (error) => logInteraction(logging, interaction, "error", undefined, undefined, error)
    });
}

/** Returns a value unchanged without allocating a mapper per logged stream. */
function identity<T>(value: T): T {
    return value;
}

/**
 * Emits one normalized controller interaction log event.
 */
function logInteraction(
    logging: NormalizedRSocketLogOptions | undefined,
    interaction: RSocketControllerKind,
    stage: "send" | "receive" | "complete" | "error",
    payload?: unknown,
    value?: unknown,
    error?: unknown
): void {
    emitLog(
        logging,
        interactionLogEvent({
            interaction,
            stage,
            payload,
            value,
            error
        })
    );
}
