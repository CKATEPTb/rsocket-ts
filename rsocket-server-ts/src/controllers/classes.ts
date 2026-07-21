/** Declarative controller base classes for every RSocket interaction model. */
import type {PublisherInput} from "rsocket-core-ts";
import type {
    RSocketChannelRequests,
    RSocketControllerOutput,
    RSocketControllerRoute,
    RSocketHandlerResult,
    RSocketRequestContext
} from "@/controllers/types.js";

/** Handles one fire-and-forget request selected by routing metadata. */
export abstract class FireAndForgetController<D = void, M = unknown> {
    /** Route tags that select this controller. */
    protected abstract readonly route: RSocketControllerRoute;

    /** Processes one request without producing a protocol response. */
    abstract handle(data: D, context: RSocketRequestContext<D, M>): RSocketHandlerResult<void> | void;
}

/** Handles one request and emits zero or one response payload. */
export abstract class RequestResponseController<D = void, R = void, M = unknown, RM = unknown> {
    /** Route tags that select this controller. */
    protected abstract readonly route: RSocketControllerRoute;

    /** Produces the response value, an encoded payload, or an empty result. */
    abstract handle(
        data: D,
        context: RSocketRequestContext<D, M>
    ): RSocketHandlerResult<RSocketControllerOutput<R, RM> | undefined>;
}

/** Handles one request and emits a demand-controlled response stream. */
export abstract class RequestStreamController<D = void, R = unknown, M = unknown, RM = unknown> {
    /** Route tags that select this controller. */
    protected abstract readonly route: RSocketControllerRoute;

    /** Produces a Reactive Streams, iterable, async iterable, or promise source. */
    abstract handle(
        data: D,
        context: RSocketRequestContext<D, M>
    ): PublisherInput<RSocketControllerOutput<R, RM>>;
}

/** Handles bidirectional request and response streams with independent demand. */
export abstract class RequestChannelController<D = unknown, R = unknown, M = unknown, RM = unknown> {
    /** Route tags that select this controller. */
    protected abstract readonly route: RSocketControllerRoute;

    /** Produces responses from the full payload stream sent by the requester. */
    abstract handle(
        requests: RSocketChannelRequests<D, M>,
        context: RSocketRequestContext<D, M>
    ): PublisherInput<RSocketControllerOutput<R, RM>>;
}
