/**
 * Sink-style request-channel helper used by the high-level RSocket facade.
 */
import {type Disposable, Sinks, type Subscriber} from "reactor-core-ts";
import {
    RSocketConnectionError,
    type RSocketPayloadFrame,
    type RSocketPayloadInput
} from "rsocket-core-ts";
import type {RSocketFlux} from "@/stream/index.js";
import type {RSocketChannelInput} from "@/client/types.js";

/**
 * Factory used by sink-style request-channel helpers to create response Fluxes.
 */
type ChannelResponseFactory<OD, OM, RD, RM> = (
    payloads: RSocketChannelInput<OD, OM>
) => RSocketFlux<RSocketPayloadFrame<RD, RM>>;

/**
 * Minimal sink operations used by the non-replayable channel helper.
 */
interface UnicastChannelSink<D, M> {
    /** Emits one payload to the underlying Reactor sink. */
    tryEmitNext(payload: RSocketPayloadInput<D, M>): unknown;

    /** Completes the underlying Reactor sink. */
    tryEmitComplete(): unknown;

    /** Fails the underlying Reactor sink. */
    tryEmitError(error: unknown): unknown;
}

/**
 * Imperative request-channel helper returned by `socket.requestChannel()`.
 *
 * `OD` and `OM` describe values sent through the sink. `RD` and `RM` describe
 * responses decoded with the connection's SETUP codecs.
 */
export class RSocketChannel<OD = unknown, OM = unknown, RD = OD, RM = OM>
    implements AsyncIterable<RSocketPayloadFrame<RD, RM>> {
    private outboundTerminated = false;
    /** Flux of SETUP-decoded response payloads produced by the responder side. */
    readonly responses: RSocketFlux<RSocketPayloadFrame<RD, RM>>;
    /** Reactor-style sink facade for pushing outbound channel payloads. */
    readonly sink: {
        next: (payload: RSocketPayloadInput<OD, OM>) => RSocketChannel<OD, OM, RD, RM>;
        complete: () => void;
        error: (error: unknown) => void;
    };

    /**
     * Creates a channel helper around a low-level request-channel interaction.
     */
    constructor(responseFactory: ChannelResponseFactory<OD, OM, RD, RM>) {
        const payloads = Sinks.many().unicast().onBackpressureBuffer<RSocketPayloadInput<OD, OM>>();
        this.responses = responseFactory(payloads.asFlux().doFinally(() => {
            this.outboundTerminated = true;
        }));
        this.sink = {
            next: (payload) => this.nextUnicast(payloads, payload),
            complete: () => {
                this.assertOutboundOpen();
                assertEmission(payloads.tryEmitComplete());
                this.outboundTerminated = true;
            },
            error: (error) => {
                this.assertOutboundOpen();
                assertEmission(payloads.tryEmitError(error));
                this.outboundTerminated = true;
            }
        };
    }

    /**
     * Pushes one outbound payload into the request channel.
     */
    next(payload: RSocketPayloadInput<OD, OM>): RSocketChannel<OD, OM, RD, RM> {
        return this.sink.next(payload);
    }

    /**
     * Completes the outbound side of the request channel.
     */
    complete(): void {
        this.sink.complete();
    }

    /**
     * Fails the outbound side of the request channel.
     */
    error(error: unknown): void {
        this.sink.error(error);
    }

    /**
     * Returns the response `Flux` for advanced Reactor-style composition.
     */
    asFlux(): RSocketFlux<RSocketPayloadFrame<RD, RM>> {
        return this.responses;
    }

    /** Subscribes to channel responses with a full Reactor subscriber. */
    subscribe(subscriber: Subscriber<RSocketPayloadFrame<RD, RM>>): void;
    /** Subscribes to channel responses with callback functions. */
    subscribe(
        onNext?: (value: RSocketPayloadFrame<RD, RM>) => void,
        onError?: (error: unknown) => void,
        onComplete?: () => void
    ): Disposable;
    /**
     * Subscribes to responder payloads emitted by the request channel.
     */
    subscribe(
        subscriberOrNext?: Subscriber<RSocketPayloadFrame<RD, RM>> | ((value: RSocketPayloadFrame<RD, RM>) => void),
        onError?: (error: unknown) => void,
        onComplete?: () => void
    ): Disposable | void {
        return this.responses.subscribe(subscriberOrNext as any, onError, onComplete);
    }

    /**
     * Allows `for await ... of` consumption of responder channel payloads.
     */
    [Symbol.asyncIterator](): AsyncIterator<RSocketPayloadFrame<RD, RM>> {
        return this.responses[Symbol.asyncIterator]();
    }

    /**
     * Pushes one payload into a single-subscription Reactor sink.
     */
    private nextUnicast(
        payloads: UnicastChannelSink<OD, OM>,
        payload: RSocketPayloadInput<OD, OM>
    ): RSocketChannel<OD, OM, RD, RM> {
        this.assertOutboundOpen();
        assertEmission(payloads.tryEmitNext(payload));
        return this;
    }

    /** Rejects writes after cancellation or terminal consumption released the source. */
    private assertOutboundOpen(): void {
        if (this.outboundTerminated) {
            throw new RSocketConnectionError("RSocket channel outbound side is closed");
        }
    }
}

/**
 * Converts Reactor sink emission failures into RSocket connection errors.
 */
function assertEmission(result: unknown): void {
    if (result !== "OK") {
        throw new RSocketConnectionError(`RSocket channel sink emission failed: ${String(result)}`);
    }
}
