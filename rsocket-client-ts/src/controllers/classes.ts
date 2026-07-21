/**
 * Class-based declarative controller definitions.
 *
 * These classes support a Spring-like declaration style where an application
 * creates a small class, extends the interaction model, and assigns a protected
 * readonly `route` field with the server route name.
 */
import {controllerLogDefaults, normalizeLogOptions} from "@/logging/options.js";
import type {NormalizedRSocketLogOptions, RSocketLogInput} from "@/logging/types.js";
import {
    routeChannelInputFactory,
    routePayloadFactory,
    type RSocketRouteChannelInputFactory,
    type RSocketRoutePayloadFactory
} from "@/controllers/route.js";
import {
    normalizeRSocketRoute,
    type RSocketPayloadFrame,
    type RSocketPayloadInput
} from "rsocket-core-ts";
import type {
    RSocketControllerKind,
    RSocketControllerRoute
} from "@/controllers/types.js";
import type {
    RSocketChannelInput,
    RSocketRequestOptions,
    RSocketStreamRequestOptions
} from "@/client/types.js";

/** Package-private instance state hidden behind an unexported symbol. */
const CONTROLLER_STATE = Symbol("RSocketController.state");
/** Type-only channel anchor hidden from controller subclasses. */
const CHANNEL_OUTBOUND = Symbol("RequestChannelController.outbound");

/** Mutable runtime state shared by a controller and its cached processor view. */
export interface ControllerRuntime {
    readonly kind: RSocketControllerKind;
    readonly options: RSocketRequestOptions | undefined;
    logging: NormalizedRSocketLogOptions | undefined;
    channelInput: RSocketRouteChannelInputFactory | undefined;
    decoder: ((payload: RSocketPayloadFrame) => unknown) | undefined;
    payload: RSocketRoutePayloadFactory | undefined;
}

/** Internal structural view used only after a public controller was materialized. */
interface InternalController {
    readonly [CONTROLLER_STATE]: ControllerRuntime;
    readonly route: RSocketControllerRoute;
    data?(request: unknown): unknown;
    response?(payload: RSocketPayloadFrame): unknown;
}

/**
 * Positional arguments expected by single-payload class controllers.
 */
export type ControllerPayloadArgs<Request> = [Request] extends [void]
    ? [] | [request: Request]
    : [request: Request];

/**
 * Positional arguments expected by request-channel class controllers.
 */
export type ControllerChannelArgs<Outbound> = [payloads: RSocketChannelInput<Outbound, any>];

/**
 * Union of class-based controller instances.
 */
export type AnyClassController =
    | FireAndForgetController<any>
    | RequestResponseController<any, any>
    | RequestStreamController<any, any>
    | RequestChannelController<any, any>;

/**
 * Shared runtime behavior for class-based controllers.
 */
abstract class RSocketRouteController<Options extends RSocketRequestOptions = RSocketRequestOptions> {
    /** Immutable route metadata declared by each concrete controller class. */
    protected abstract readonly route: RSocketControllerRoute;

    private readonly [CONTROLLER_STATE]: ControllerRuntime;

    /**
     * Creates a route controller with optional per-request encoding options.
     */
    constructor(kind: RSocketControllerKind, requestOptions?: Options) {
        this[CONTROLLER_STATE] = {
            kind,
            options: requestOptions,
            logging: undefined,
            channelInput: undefined,
            decoder: undefined,
            payload: undefined,
        };
    }

    /**
     * Enables, updates, or disables logging for this controller instance.
     */
    log(options: RSocketLogInput = true): this {
        const state = this[CONTROLLER_STATE];
        const logging = normalizeLogOptions(
            options,
            controllerLogDefaults(state.kind),
            state.logging
        );
        state.logging = logging;
        return this;
    }
}

/**
 * Base class for fire-and-forget controllers.
 *
 * Extend it and set `protected readonly route = "routeName"` in the subclass.
 */
export abstract class FireAndForgetController<Request = void>
    extends RSocketRouteController {
    /** Creates a fire-and-forget declaration with optional request settings. */
    constructor(options?: RSocketRequestOptions) {
        super("fireAndForget", options);
    }

    /**
     * Converts a typed request value into the data sent to the declared route.
     */
    protected data(request: Request): unknown {
        return request;
    }
}

/**
 * Base class for request-response controllers.
 *
 * The first generic is the request body type and the second generic is the
 * decoded response body type.
 */
export abstract class RequestResponseController<Request = void, Response = unknown>
    extends RSocketRouteController {
    /** Creates a request-response declaration with optional request settings. */
    constructor(options?: RSocketRequestOptions) {
        super("requestResponse", options);
    }

    /**
     * Converts a typed request value into the data sent to the declared route.
     */
    protected data(request: Request): unknown {
        return request;
    }

    /**
     * Converts a response payload frame into the controller response type.
     */
    protected response(payload: RSocketPayloadFrame): Response {
        return payload.data as Response;
    }
}

/**
 * Base class for request-stream controllers.
 *
 * The first generic is the request body type and the second generic is the type
 * emitted for every response payload.
 */
export abstract class RequestStreamController<Request = void, Response = unknown>
    extends RSocketRouteController<RSocketStreamRequestOptions> {
    /** Creates a request-stream declaration with optional request settings. */
    constructor(options?: RSocketStreamRequestOptions) {
        super("requestStream", options);
    }

    /**
     * Converts a typed request value into the data sent to the declared route.
     */
    protected data(request: Request): unknown {
        return request;
    }

    /**
     * Converts a response payload frame into the controller response type.
     */
    protected response(payload: RSocketPayloadFrame): Response {
        return payload.data as Response;
    }
}

/**
 * Base class for request-channel controllers.
 *
 * The first generic is the outbound item type and the second generic is the
 * typed response item emitted by the responder.
 */
export abstract class RequestChannelController<Outbound = unknown, Response = unknown>
    extends RSocketRouteController<RSocketStreamRequestOptions> {
    declare private readonly [CHANNEL_OUTBOUND]: Outbound;

    /** Creates a request-channel declaration with optional request settings. */
    constructor(options?: RSocketStreamRequestOptions) {
        super("requestChannel", options);
    }

    /**
     * Converts a response payload frame into the controller response type.
     */
    protected response(payload: RSocketPayloadFrame): Response {
        return payload.data as Response;
    }
}

/** Returns package-private controller state to the socket-scoped processor. */
export function controllerRuntime(controller: AnyClassController): ControllerRuntime {
    return (controller as unknown as InternalController)[CONTROLLER_STATE];
}

/** Encodes a controller argument and adds cached routing metadata. */
export function controllerPayload(
    controller: AnyClassController,
    state: ControllerRuntime,
    args: readonly unknown[]
): RSocketPayloadInput<any, any> {
    const internal = controller as unknown as InternalController;
    const request = args.length === 0
        ? undefined
        : internal.data?.call(controller, args[0]);
    return payloadFactory(internal, state)(request);
}

/** Adds cached routing metadata to a request-channel input source. */
export function controllerChannelInput(
    controller: AnyClassController,
    state: ControllerRuntime,
    input: RSocketChannelInput<any, any>
): RSocketChannelInput<any, any> {
    return channelInputFactory(controller as unknown as InternalController, state)(input);
}

/** Returns one cached decoder backed by the subclass's response extension point. */
export function controllerDecoder(
    controller: AnyClassController,
    state: ControllerRuntime
): (payload: RSocketPayloadFrame) => unknown {
    const internal = controller as unknown as InternalController;
    return state.decoder ??= (payload) => internal.response?.call(controller, payload);
}

/** Returns the route payload builder cached for one controller instance. */
function payloadFactory(
    controller: InternalController,
    state: ControllerRuntime
): RSocketRoutePayloadFactory {
    return state.payload ??= routePayloadFactory(resolveRoute(controller));
}

/** Returns the routed channel builder cached for one controller instance. */
function channelInputFactory(
    controller: InternalController,
    state: ControllerRuntime
): RSocketRouteChannelInputFactory {
    return state.channelInput ??= routeChannelInputFactory(resolveRoute(controller));
}

/** Validates a subclass's route declaration before its first request. */
function resolveRoute(controller: InternalController): RSocketControllerRoute {
    return normalizeRSocketRoute(controller.route, false);
}
