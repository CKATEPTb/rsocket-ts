/**
 * Type contracts for declarative controllers executed by `RSocket.process(...)`.
 */
import type {Flux, Mono} from "reactor-core-ts";
import type {
    RSocketRoute,
    RSocketPayloadFrame,
    RSocketPayloadInput
} from "rsocket-core-ts";
import type {
    RSocketChannelInput,
    RSocketRequestOptions,
    RSocketStreamRequestOptions
} from "@/client/types.js";
import type {RSocketFlux} from "@/stream/index.js";
import type {
    AnyClassController,
    ControllerChannelArgs,
    ControllerPayloadArgs,
    FireAndForgetController,
    RequestChannelController,
    RequestResponseController,
    RequestStreamController
} from "@/controllers/classes.js";

/**
 * Interaction models that can be represented by a declarative controller.
 */
export type RSocketControllerKind =
    | "fireAndForget"
    | "requestResponse"
    | "requestStream"
    | "requestChannel";

/**
 * RSocket routing metadata accepted by class-based controllers.
 *
 * A string creates one `message/x.rsocket.routing.v0` route entry, while an
 * array creates a route from multiple path segments.
 */
export type RSocketControllerRoute = RSocketRoute;

/**
 * Zero-argument controller class accepted by controller-aware request methods.
 */
export type RSocketControllerConstructor<C extends AnyClassController = AnyClassController> = new () => C;

/**
 * Extracts the positional argument tuple expected by a controller.
 */
export type ControllerArgs<C> =
    C extends FireAndForgetController<infer Request>
        ? ControllerPayloadArgs<Request>
        : C extends RequestResponseController<infer Request, unknown>
            ? ControllerPayloadArgs<Request>
            : C extends RequestStreamController<infer Request, unknown>
                ? ControllerPayloadArgs<Request>
                : C extends RequestChannelController<infer Outbound, unknown>
                    ? ControllerChannelArgs<Outbound>
                    : never;

/**
 * Resolves the exact Reactor return type produced by a declarative controller.
 */
export type ControllerReturn<C> =
    C extends FireAndForgetController<any>
        ? Mono<void>
        : C extends RequestResponseController<any, infer Result>
            ? Mono<Result>
            : C extends RequestStreamController<any, infer Result>
                ? Flux<Result>
                : C extends RequestChannelController<any, infer Result>
                    ? Flux<Result>
                    : never;

/**
 * Minimal connection surface required to execute a declarative controller.
 */
export interface RSocketControllerConnection {
    /** Starts a fire-and-forget interaction. */
    fireAndForget(payload: RSocketPayloadInput<any, any>, options?: RSocketRequestOptions): Mono<void>;

    /** Starts a request-response interaction. */
    requestResponse(payload: RSocketPayloadInput<any, any>, options?: RSocketRequestOptions): Mono<RSocketPayloadFrame>;

    /** Starts a request-stream interaction. */
    requestStream(payload: RSocketPayloadInput<any, any>, options?: RSocketStreamRequestOptions): RSocketFlux;

    /** Starts a request-channel interaction. */
    requestChannel(payloads: RSocketChannelInput<any, any>, options?: RSocketStreamRequestOptions): RSocketFlux;
}
