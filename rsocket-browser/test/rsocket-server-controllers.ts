import {Flux} from "reactor-core-ts";
import type {RSocketPayloadFrame} from "rsocket-core-ts";
import {
  FireAndForgetController,
  RequestChannelController,
  RequestResponseController,
  RequestStreamController,
  type RSocketControllerRegistration
} from "rsocket-server-ts";

/** Mutable observations owned by one browser/server integration fixture. */
export interface TestServerObservations {
  /** Data accepted by the fire-and-forget controller. */
  readonly fireAndForget: number[];
}

/** Request and response shape used by the echo route. */
export interface EchoValue {
  /** Arbitrary text echoed by the responder. */
  readonly value: string;
}

/** Request accepted by the finite number stream. */
export interface NumbersRequest {
  /** Number of logical response items. */
  readonly count: number;
  /** Optional large value copied into every response. */
  readonly value?: string;
}

/** One logical number stream response. */
export interface NumberResponse {
  /** One-based sequence number. */
  readonly n: number;
  /** Optional value copied from the request. */
  readonly value?: string;
}

/** Request accepted by the delayed response route. */
export interface DelayedRequest extends EchoValue {
  /** Delay before completing the response. */
  readonly delayMs: number;
}

/** One request-channel item. */
export interface ChannelValue extends EchoValue {
}

/** One response-channel item. */
export interface ChannelResponse extends EchoValue {
  /** Stable discriminator proving that the server controller handled the item. */
  readonly kind: "channel";
}

/** Creates all declarative controllers used by browser/server integration tests. */
export function testServerControllers(
  observations: TestServerObservations
): readonly RSocketControllerRegistration[] {
  return [
    new RecordController(observations),
    EchoController,
    DelayedController,
    NumbersController,
    FailingStreamController,
    ChannelController
  ];
}

/** Records routed fire-and-forget requests. */
class RecordController extends FireAndForgetController<number> {
  protected readonly route = "record";

  /** Creates a controller bound to one fixture observation object. */
  constructor(private readonly observations: TestServerObservations) {
    super();
  }

  /** Records one request without producing a response. */
  override handle(data: number): void {
    this.observations.fireAndForget.push(data);
  }
}

/** Echoes routed request-response data. */
class EchoController extends RequestResponseController<EchoValue, EchoValue> {
  protected readonly route = "echo";

  /** Returns the decoded request unchanged. */
  override handle(data: EchoValue): EchoValue {
    return data;
  }
}

/** Completes one routed request-response after a deterministic delay. */
class DelayedController extends RequestResponseController<DelayedRequest, EchoValue> {
  protected readonly route = "delay";

  /** Delays the response so client timeout and cancellation behavior can be observed. */
  override handle(data: DelayedRequest): Promise<EchoValue> {
    return new Promise((resolve) => setTimeout(() => resolve({value: data.value}), data.delayMs));
  }
}

/** Produces a finite, demand-controlled sequence. */
class NumbersController extends RequestStreamController<NumbersRequest, NumberResponse> {
  protected readonly route = "numbers";

  /** Emits exactly the requested number of logical values. */
  override handle(data: NumbersRequest): Flux<NumberResponse> {
    return Flux.range(1, data.count).map((n) => data.value === undefined
      ? {n}
      : {n, value: data.value});
  }
}

/** Emits one item and then fails the stream. */
class FailingStreamController extends RequestStreamController<void, NumberResponse> {
  protected readonly route = "stream-error";

  /** Returns the failing async sequence used to verify ERROR propagation. */
  override handle(): AsyncIterable<NumberResponse> {
    return failingNumbers();
  }
}

/** Maps channel requests to channel responses without buffering ahead of demand. */
class ChannelController extends RequestChannelController<ChannelValue, ChannelResponse> {
  protected readonly route = "channel";

  /** Echoes each decoded request item through the response stream. */
  override handle(requests: Flux<RSocketPayloadFrame<ChannelValue>>): Flux<ChannelResponse> {
    return requests.map((payload) => ({
      kind: "channel",
      value: (payload.data as ChannelValue).value
    }));
  }
}

/** Generates one value followed by an application failure. */
async function* failingNumbers(): AsyncGenerator<NumberResponse> {
  yield {n: 1};
  throw new Error("stream boom");
}
