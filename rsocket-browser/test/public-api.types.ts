/**
 * Compile-time assertions for MIME-driven positional interaction arguments.
 *
 * This file is checked by `tsc` and intentionally excluded from Vitest runtime
 * discovery because none of these requests should be subscribed.
 */
import type {Flux, Mono} from "reactor-core-ts";
import { MimeType } from "rsocket-frames-ts";
import type {RSocketPayloadFrame} from "rsocket-core-ts";
import { RequestResponseController, RSocket } from "@";
// @ts-expect-error Browser constructor options are inferred, not root exports.
import type {RSocketOptions as LeakedRSocketOptions} from "@";
// @ts-expect-error Browser reconnect signals are adapter internals.
import type {RSocketBrowserReconnectSignals as LeakedReconnectSignals} from "@";

/** Compile-time sentinel retaining forbidden root-import checks. */
type ForbiddenBrowserExports = LeakedRSocketOptions | LeakedReconnectSignals;

/** Data encoded by the connection-level test codec. */
interface TypedData {
  /** Numeric request identifier. */
  readonly id: number;
}

/** Metadata encoded by the connection-level test codec. */
interface TypedMetadata {
  /** Tenant associated with the request. */
  readonly tenant: string;
}

const dataMimeType = new MimeType<TypedData>("application/vnd.example.data");
const metadataMimeType = new MimeType<TypedMetadata>("application/vnd.example.metadata");
const textMimeType = new MimeType<string>("application/vnd.example.text");
const socket = new RSocket("ws://localhost/rsocket", {
  setup: {
    mimetype: {
      data: dataMimeType,
      metadata: metadataMimeType
    }
  }
});

new RSocket("ws://localhost/rsocket", {
  log: {
    // @ts-expect-error Interaction logging is configured through controller.log().
    interactions: true
  }
});

/** Class controller proving that declarative calls stay isolated behind process. */
class TypedController extends RequestResponseController<TypedData, string> {
  /** Route consumed by the typed test controller. */
  protected readonly route = "typed.request";
}

/** Subclass sentinel proving browser lifecycle wiring remains package-private. */
class BrowserSocketSubclassProbe extends RSocket {
  /** Attempts to access the intentionally hidden adapter hook. */
  protected verifySocketSurface(): void {
    // @ts-expect-error Browser reconnect-signal injection is not public API.
    void this.useReconnectSignals;
  }
}

socket.fireAndForget({ id: 1 }, { tenant: "acme" });
socket.requestResponse({ id: 1 }, { tenant: "acme" }).map((payload) => {
  const data: TypedData | undefined = payload.data;
  const metadata: TypedMetadata | undefined = payload.metadata;
  return { data, metadata };
});
socket.requestStream({ id: 1 }, { tenant: "acme" });
socket.requestChannel([{ id: 1 }], { tenant: "acme" });
socket.requestChannel().next({ id: 1 });
socket.process(TypedController, { id: 1 });
socket.process(new TypedController({ timeout: 1_000 }), { id: 1 });

socket.fireAndForget("text", { tenant: "acme" }, { data: textMimeType });
const overriddenResponse: Mono<RSocketPayloadFrame<TypedData, TypedMetadata>> = socket.requestResponse(
  "text",
  {tenant: "acme"},
  {data: textMimeType}
);
const overriddenChannel: Flux<RSocketPayloadFrame<TypedData, TypedMetadata>> = socket.requestChannel(
  ["text"],
  { tenant: "acme" },
  { data: textMimeType }
);
const channel = socket.requestChannel(undefined, undefined, {data: textMimeType});
channel.next("text");
const channelResponses: Flux<RSocketPayloadFrame<TypedData, TypedMetadata>> = channel.responses;

// @ts-expect-error SETUP data MIME accepts TypedData, not string.
socket.fireAndForget("invalid", { tenant: "acme" });
// @ts-expect-error SETUP metadata MIME accepts TypedMetadata, not string.
socket.requestResponse({ id: 1 }, "invalid");
// @ts-expect-error Per-request text MIME accepts string data, not number.
socket.requestStream(42, { tenant: "acme" }, { data: textMimeType });
// @ts-expect-error Controllers are executed through process, not requestResponse.
socket.requestResponse(TypedController, { id: 1 });
// @ts-expect-error Outbound MIME overrides do not change SETUP-decoded response types.
const invalidOverriddenResponse: Mono<RSocketPayloadFrame<string, TypedMetadata>> = overriddenResponse;
// @ts-expect-error The imperative channel sink uses its outbound MIME override.
channel.next({id: 1});

socket.connect().map((connected) => {
  connected.fireAndForget({ id: 2 }, { tenant: "connected" });
  // @ts-expect-error Connected facade preserves the SETUP data type.
  connected.fireAndForget("invalid", { tenant: "connected" });
  return connected;
});

type SocketMethods = keyof typeof socket;
type ExpectedSocketMethods =
  | "connect"
  | "metadataPush"
  | "metadataUpdate"
  | "fireAndForget"
  | "requestResponse"
  | "requestStream"
  | "requestChannel"
  | "process";
const socketSurface: Assert<Equal<SocketMethods, ExpectedSocketMethods>> = true;

type Connected = NonNullable<Awaited<ReturnType<ReturnType<typeof socket.connect>["block"]>>>;
type ExpectedConnectedMethods = Exclude<ExpectedSocketMethods, "connect"> | "disconnect";
const connectedSurface: Assert<Equal<keyof Connected, ExpectedConnectedMethods>> = true;
const controllerSurface: Assert<Equal<keyof TypedController, "log">> = true;
void socketSurface;
void connectedSurface;
void controllerSurface;
void BrowserSocketSubclassProbe;
void overriddenChannel;
void channelResponses;
void invalidOverriddenResponse;
void (0 as unknown as ForbiddenBrowserExports);

/** Compile-time equality predicate. */
type Equal<A, B> = (<T>() => T extends A ? 1 : 2) extends (<T>() => T extends B ? 1 : 2)
  ? (<T>() => T extends B ? 1 : 2) extends (<T>() => T extends A ? 1 : 2)
    ? true
    : false
  : false;

/** Fails compilation when the facade gains or loses a public member. */
type Assert<T extends true> = T;
