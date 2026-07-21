/** Public types shared by declarative responder controllers. */
import type {Flux, Publisher} from "reactor-core-ts";
import type {RSocketPayloadFrame, RSocketPayloadInput, RSocketRoute} from "rsocket-core-ts";
import type {RSocketServerConnection} from "@/server/connection.js";
import type {
    FireAndForgetController,
    RequestChannelController,
    RequestResponseController,
    RequestStreamController
} from "@/controllers/classes.js";

/** One routing tag or an exact ordered routing-tag sequence. */
export type RSocketControllerRoute = RSocketRoute;

/** Synchronous, promise-based, or Reactive Streams controller result. */
export type RSocketHandlerResult<T> = T | PromiseLike<T> | Publisher<T>;

/** Request information available to every controller invocation. */
export interface RSocketRequestContext<D = unknown, M = unknown> extends RSocketPayloadFrame<D, M> {
    /** Stable logical server connection handling this request. */
    readonly connection: RSocketServerConnection;
    /** Client-generated odd stream identifier. */
    readonly streamId: number;
    /** Ordered routing tags decoded from direct or composite metadata. */
    readonly route: readonly string[];
}

/** Any declarative responder controller instance. */
export type RSocketController =
    | FireAndForgetController<any, any>
    | RequestResponseController<any, any, any, any>
    | RequestStreamController<any, any, any, any>
    | RequestChannelController<any, any, any, any>;

/** Constructor for a dependency-free declarative controller. */
export type RSocketControllerConstructor = new () => RSocketController;

/** Controller registration accepted by `RSocketServer`. */
export type RSocketControllerRegistration = RSocketController | RSocketControllerConstructor;

/** Output accepted from request-response and streaming controllers. */
export type RSocketControllerOutput<D, M> = RSocketPayloadInput<D, M>;

/** Input stream delivered to a request-channel controller. */
export type RSocketChannelRequests<D, M> = Flux<RSocketPayloadFrame<D, M>>;
