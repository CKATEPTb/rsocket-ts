/** Compile-time contract for root imports, transport inference, and socket states. */
import type {Flux, Mono} from "reactor-core-ts";
import {MimeType} from "rsocket-frames-ts";
import type {RSocketPayloadFrame} from "rsocket-core-ts";
import {
    FireAndForgetController,
    RequestChannelController,
    RequestResponseController,
    RequestStreamController,
    RSocket
} from "@";
// @ts-expect-error Constructor options are inferred and are not a root export.
import type {RSocketOptions as LeakedRSocketOptions} from "@";
// @ts-expect-error Low-level requester sessions are not public API.
import type {RSocketClient as LeakedRSocketClient} from "@";
// @ts-expect-error Controller processor state is package-private.
import type {ControllerRuntime as LeakedControllerRuntime} from "@";

/** Compile-time sentinel retaining forbidden root-import checks. */
type ForbiddenClientExports = LeakedRSocketOptions | LeakedRSocketClient | LeakedControllerRuntime;

/** Data inferred from the configured SETUP codec. */
interface Data {
    readonly id: number;
}

/** Metadata inferred from the configured SETUP codec. */
interface MetadataValue {
    readonly tenant: string;
}

const dataMimeType = new MimeType<Data>("application/vnd.example.data");
const metadataMimeType = new MimeType<MetadataValue>("application/vnd.example.metadata");
const textMimeType = new MimeType<string>("text/plain");
const textMetadataMimeType = new MimeType<string>("application/vnd.example.text-metadata");

const websocket = new RSocket({
    transport: {type: "websocket", url: "ws://localhost/rsocket"},
    setup: {lease: true, mimetype: {data: dataMimeType, metadata: metadataMimeType}},
    reconnect: {resume: {ttl: 30_000}},
    log: {frames: true, lifecycle: true},
    events: {connected: (event) => event.connected}
});

const tcp = new RSocket({
    transport: {type: "tcp", host: "127.0.0.1", port: 7000},
    reconnect: false
});

const webtransport = new RSocket({
    transport: {
        type: "webtransport",
        url: "https://example.com/rsocket",
        media: (payload) => {
            const bytes: Uint8Array = payload;
            void bytes;
        }
    },
    setup: {mimetype: {data: dataMimeType, metadata: metadataMimeType}}
});

/** Typed controller proving that root exports retain generic inference. */
class FindController extends RequestResponseController<Data, string> {
    protected readonly route = "find";

    /** Verifies that controller subclasses see extension points, not dispatch internals. */
    protected verifyControllerSurface(): void {
        void this.data({id: 1});
        void this.response({data: "value"} as never);
        // @ts-expect-error Interaction kind is internal processor state.
        void this.kind;
        // @ts-expect-error Payload assembly is internal processor state.
        void this.payload;
        // @ts-expect-error Decoder wiring is internal processor state.
        void this.decode;
        // @ts-expect-error Request options are constructor input, not a subclass hook.
        void this.options;
        // @ts-expect-error Normalized logging state is private.
        void this.logging;
        // @ts-expect-error Route assembly is private.
        void this.routedPayload;
        // @ts-expect-error Route validation is private.
        void this.resolveRoute;
    }
}

class FireController extends FireAndForgetController<Data> {
    protected readonly route = "fire";
}

class StreamController extends RequestStreamController<Data, string> {
    protected readonly route = "stream";
}

class ChannelController extends RequestChannelController<Data, string> {
    protected readonly route = "channel";

    /** Verifies the channel subclass has no single-request or dispatch hooks. */
    protected verifyControllerSurface(): void {
        void this.response({data: "value"} as never);
        // @ts-expect-error Channel payload routing belongs to the processor.
        void this.input;
        // @ts-expect-error Request-channel has no single-request data hook.
        void this.data;
    }
}

/** Subclass sentinel proving runtime reconnect adapters are not client API. */
class RSocketSubclassProbe extends RSocket {
    /** Attempts to access the intentionally hidden environment integration hook. */
    protected verifySocketSurface(): void {
        // @ts-expect-error Runtime reconnect-signal injection is package-private.
        void this.useReconnectSignals;
    }
}

websocket.fireAndForget({id: 1}, {tenant: "acme"});
websocket.fireAndForget(textMimeType.toPayload("already encoded"));
const response: Mono<RSocketPayloadFrame<Data, MetadataValue>> = websocket.requestResponse(
    {id: 1},
    {tenant: "acme"}
);
const stream: Flux<RSocketPayloadFrame<Data, MetadataValue>> = websocket.requestStream(
    {id: 1},
    {tenant: "acme"}
);
const channelResponses: Flux<RSocketPayloadFrame<Data, MetadataValue>> = websocket.requestChannel(
    [{id: 1}],
    {tenant: "acme"}
);
websocket.requestChannel([textMimeType.toPayload("already encoded")]);
const textResponse: Mono<RSocketPayloadFrame<Data, MetadataValue>> = websocket.requestResponse(
    "outbound text",
    "outbound metadata",
    {data: textMimeType, metadata: textMetadataMimeType}
);
const textStream: Flux<RSocketPayloadFrame<Data, MetadataValue>> = websocket.requestStream(
    "outbound text",
    undefined,
    {data: textMimeType}
);
const textChannelResponses: Flux<RSocketPayloadFrame<Data, MetadataValue>> = websocket.requestChannel(
    ["outbound text"],
    undefined,
    {data: textMimeType}
);
const channel = websocket.requestChannel(undefined, undefined, {data: textMimeType});
channel.next("outbound text");
const sinkResponses: Flux<RSocketPayloadFrame<Data, MetadataValue>> = channel.responses;
websocket.requestChannel().next(textMimeType.toPayload("already encoded"));
websocket.metadataPush({tenant: "acme"});
websocket.metadataPush("typed override", {metadataMimeType: textMetadataMimeType});
websocket.metadataPush(textMetadataMimeType.toMetadata("already encoded"));
websocket.metadataUpdate((metadata) => {
    metadata.set(metadataMimeType, {tenant: "acme"});
    const stored: MetadataValue | undefined = metadata.get(metadataMimeType)?.payload;
    void stored;
    // @ts-expect-error The metadata value must match MimeType<MetadataValue>.
    metadata.set(metadataMimeType, {tenant: 42});
    metadata.remove(metadataMimeType);
});
const controllerResponse: Mono<string> = websocket.process(FindController, {id: 1});
const controllerFire: Mono<void> = websocket.process(FireController, {id: 1});
const controllerStream: Flux<string> = websocket.process(StreamController, {id: 1});
const controllerChannel: Flux<string> = websocket.process(ChannelController, [{id: 1}]);
tcp.connect();
webtransport.media(Uint8Array.of(1));

// @ts-expect-error SETUP data MIME accepts Data, not string.
websocket.fireAndForget("invalid");
// @ts-expect-error Raw METADATA_PUSH uses the SETUP metadata codec unless an override is supplied.
websocket.metadataPush("invalid");
// @ts-expect-error Sink input uses the per-call outbound text codec.
channel.next({id: 1});
// @ts-expect-error A request MIME override does not change the SETUP-decoded response type.
const invalidResponse: Mono<RSocketPayloadFrame<string, string>> = textResponse;
// @ts-expect-error Controller request type is inferred from its class declaration.
websocket.process(FindController, "invalid");
// @ts-expect-error Channel controller items use the declared outbound type.
websocket.process(ChannelController, ["invalid"]);

type SocketMethods = keyof typeof websocket;
type ExpectedSocketMethods =
    | "connect"
    | "metadataPush"
    | "media"
    | "metadataUpdate"
    | "fireAndForget"
    | "requestResponse"
    | "requestStream"
    | "requestChannel"
    | "process";
const socketSurface: Assert<Equal<SocketMethods, ExpectedSocketMethods>> = true;

type Connected = NonNullable<Awaited<ReturnType<ReturnType<typeof websocket.connect>["block"]>>>;
type ExpectedConnectedMethods = Exclude<ExpectedSocketMethods, "connect"> | "disconnect";
const connectedSurface: Assert<Equal<keyof Connected, ExpectedConnectedMethods>> = true;
const controllerSurface: Assert<Equal<keyof FindController, "log">> = true;
void socketSurface;
void connectedSurface;
void controllerSurface;
void response;
void stream;
void channelResponses;
void textStream;
void textChannelResponses;
void sinkResponses;
void controllerResponse;
void controllerFire;
void controllerStream;
void controllerChannel;
void invalidResponse;
void RSocketSubclassProbe;
void (0 as unknown as ForbiddenClientExports);

/** Compile-time equality predicate. */
type Equal<A, B> = (<T>() => T extends A ? 1 : 2) extends (<T>() => T extends B ? 1 : 2)
    ? (<T>() => T extends B ? 1 : 2) extends (<T>() => T extends A ? 1 : 2)
        ? true
        : false
    : false;

/** Fails compilation when a public surface gains or loses a member. */
type Assert<T extends true> = T;
