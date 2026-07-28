# rsocket-browser

`rsocket-browser` is a browser-first TypeScript RSocket requester for backends
that expose RSocket over WebSocket or the project's WebTransport mapping. It
provides all four RSocket interaction models, Reactive Streams backpressure,
typed metadata codecs, reconnect, Resume, fragmentation, and class-based route
controllers.

```bash
npm install rsocket-browser
```

`rsocket-client-ts`, `rsocket-core-ts`, `reactor-core-ts`,
`rsocket-frames-ts`, and `bebyte` are installed automatically. Import the client
and controllers from `rsocket-browser`; import MIME codecs and protocol metadata
helpers from `rsocket-frames-ts`.

The client is not tied to Spring Boot or any other server framework.

The URL selects the transport: `ws:` and `wss:` use WebSocket, while `https:`
uses WebTransport. Reconnect is enabled by default, with browser online, focus,
page restore, and sleep/wake signals helping restart interrupted connections.

## Quick Start

```ts
import { RSocket } from "rsocket-browser";
import { WellKnownMimeType } from "rsocket-frames-ts";

const routing = WellKnownMimeType.MESSAGE_RSOCKET_ROUTING;
const route = (name: string) => routing.toMetadata([name]);

const socket = new RSocket("wss://api.example.com/rsocket", {
  setup: {
    keepAlive: 20_000,
    lifetime: 90_000,
    mimetype: {
      data: WellKnownMimeType.APPLICATION_JSON,
      metadata: WellKnownMimeType.MESSAGE_RSOCKET_COMPOSITE_METADATA
    }
  }
});

const connection = await socket.connect().block();
if (!connection) throw new Error("RSocket connection did not open");

const response = await connection
  .requestResponse({ id: 42 }, route("user.find"))
  .block();

console.log(response?.data);
```

The routing entry is automatically wrapped in composite metadata because the
SETUP metadata MIME is composite. You do not need to construct a composite
container for a single route.

## API At A Glance

Direct interactions use one compact positional shape:

```text
connection.fireAndForget(data?, metadata?, mimetype?);
connection.requestResponse(data?, metadata?, mimetype?);
connection.requestStream(data?, metadata?, mimetype?);
connection.requestChannel(source?, metadata?, mimetype?);
```

- `data` is a value encoded by the data MIME, or an already encoded `Payload`.
- `metadata` is a value encoded by the metadata MIME, or a typed `Metadata`.
- `mimetype` optionally overrides codecs as `{ data, metadata }` for that call.
- `source` is a Publisher, `Flux`, iterable, or async iterable of channel items.
- Calling `requestChannel()` without a source returns an imperative channel sink.

| Interaction | Result | Purpose |
| --- | --- | --- |
| `fireAndForget` | `Mono<void>` | Send one message without a response. |
| `requestResponse` | `Mono<PayloadFrame>` | Send one request and receive at most one response. |
| `requestStream` | `Flux<PayloadFrame>` | Receive a demand-controlled response stream. |
| `requestChannel` | `Flux<PayloadFrame>` or channel sink | Exchange demand-controlled streams in both directions. |

`connect()` and every interaction are lazy. A frame is sent only after the
returned `Mono` or `Flux` is subscribed, blocked, or consumed with
`for await ... of`.

## Direct Interactions

### Fire And Forget

```ts
await connection
  .fireAndForget(
    { page: "/pricing", at: Date.now() },
    route("analytics.page-view")
  )
  .block();
```

Completion means the frame was written to the active transport. It does not
mean the server processed the message.

### Request Response

```ts
const response = await connection
  .requestResponse({ id: 42 }, route("user.find"))
  .block();

console.log(response?.data);
console.log(response?.metadata);
```

The result also exposes the decoded `frame`, `dataPayload`, and
`metadataPayload` when lower-level protocol access is needed.

### Request Stream

```ts
const users = connection.requestStream(
  { teamId: 7 },
  route("users.watch")
);

users.subscribe({
  onSubscribe(subscription) {
    subscription.request(20);
  },
  onNext(payload) {
    console.log(payload.data);
  },
  onError(error) {
    console.error(error);
  },
  onComplete() {
    console.log("stream complete");
  }
});
```

The initial and later `request(n)` calls become RSocket demand. The responder
cannot deliver more items than the client has requested. Cancelling the
subscription sends `CANCEL`.

### Request Channel

Use the sink form when UI actions produce outbound messages over time:

```ts
const channel = connection.requestChannel(
  undefined,
  route("chat.messages")
);

channel.subscribe({
  onSubscribe(subscription) {
    subscription.request(50);
  },
  onNext(payload) {
    console.log("received", payload.data);
  },
  onError(error) {
    console.error(error);
  },
  onComplete() {
    console.log("channel complete");
  }
});

channel.next({ room: "general", text: "Hello" });
channel.next({ room: "general", text: "World" });
channel.complete();
```

Outbound items are buffered and sent only when the responder requests them.
Inbound items are delivered only against local demand.

Use a publisher when the outbound source already exists:

```ts
async function* messages() {
  yield { room: "general", text: "Hello" };
  yield { room: "general", text: "World" };
}

const replies = connection.requestChannel(
  messages(),
  route("chat.messages")
);

for await (const reply of replies) {
  console.log(reply.data);
}
```

## Setup And Types

The two constructor forms are equivalent:

```ts
new RSocket("wss://api.example.com/rsocket", options);
new RSocket({ url: "wss://api.example.com/rsocket", ...options });
```

The public options are intentionally small:

| Option | Meaning |
| --- | --- |
| `setup.keepAlive` | Milliseconds between requester KEEPALIVE frames. |
| `setup.lifetime` | Maximum time without a responder KEEPALIVE before the connection is dead. |
| `setup.lease` | Requests and enforces responder-issued LEASE credit. |
| `setup.fragmentSize` | Maximum raw RSocket frame size before automatic fragmentation. |
| `setup.mimetype.data` | Codec used for data payloads. |
| `setup.mimetype.metadata` | Codec used for metadata payloads. |
| `setup.payload` | Optional payload included in the SETUP frame. |
| `setup.protocols` | Optional WebSocket subprotocol or ordered preference list. |
| `setup.transport` | Optional `(url) => WebSocketLike` factory for WebSocket URLs. |
| `webTransport` | Optional WebTransport factory, mapping limits, datagram mode, and media receiver. |
| `reconnect` | Enables/disables reconnect and optionally configures Resume. |
| `events` | Constructor-time connection lifecycle handlers. |
| `log` | Frame and connection lifecycle logging. |

The browser `WebSocket` constructor is used by default. A compatible WebSocket
wrapper can be supplied when the runtime needs one:

```ts
const socket = new RSocket("wss://api.example.com/rsocket", {
  setup: {
    transport: (url) => new WebSocket(url)
  }
});
```

## WebTransport

Use an HTTPS endpoint to select the browser's global `WebTransport`
constructor:

```ts
const socket = new RSocket("https://api.example.com/rsocket", {
  webTransport: {
    media(payload) {
      playMediaChunk(payload);
    }
  }
});

const connection = await socket.connect().block();
await connection?.media(encodedMediaChunk).block();
```

The versioned `RSWT/1` mapping uses one bidirectional stream for connection
control, one bidirectional stream per request-response, request-stream, or
request-channel interaction, and a reliable unidirectional stream for
fire-and-forget and metadata-push frames. Frame ordering, fragmentation,
backpressure, and Resume remain automatic.

This mapping is supplied by the project; RSocket 1.0 does not define a standard
WebTransport binding. The responder must use the same mapping, for example
`rsocket-server-ts.acceptWebTransport(session)`.

Complete fire-and-forget requests may opt into best-effort datagrams:

```ts
const socket = new RSocket("https://api.example.com/rsocket", {
  webTransport: {
    unreliableFireAndForget: true
  }
});
```

Oversized or fragmented fire-and-forget requests automatically stay on the
reliable stream. A reliable marker preserves the RSocket stream sequence when a
datagram is lost. Best-effort fire-and-forget cannot be combined with Resume;
media datagrams are always outside RSocket Resume positions. The native
constructor requests unreliable capability automatically in this mode.

Set `setup.lease: true` only when the responder sends `LEASE` frames. New
interactions wait for valid credit and consume one request from the current
lease.

The generic data and metadata types are inferred from typed SETUP MIME codecs.
For example, `TEXT_PLAIN` makes direct data arguments and response data
`string`-typed. `APPLICATION_JSON` intentionally supports any JSON shape, so
use explicit socket generics or controllers when strict application types are
required:

```ts
type ApiData =
  | { id: number }
  | { id: number; name: string }
  | { login: string; password: string }
  | { token: string };

const typedSocket = new RSocket<ApiData>("wss://api.example.com/rsocket", {
  setup: {
    mimetype: {
      data: WellKnownMimeType.APPLICATION_JSON,
      metadata: WellKnownMimeType.MESSAGE_RSOCKET_COMPOSITE_METADATA
    }
  }
});
```

A per-call codec override gives the outbound value its own type:

```ts
connection.fireAndForget<string>(
  "ready",
  route("status.update"),
  { data: WellKnownMimeType.TEXT_PLAIN }
);
```

The responder must already expect that data codec for the route. RSocket does
not attach a separate data MIME declaration to every request frame. Responses
remain typed and decoded by the SETUP codecs. An already encoded `Payload<T>`
can also be passed directly without changing the socket's response type.

See [rsocket-frames-ts](https://github.com/CKATEPTb/rsocket-ts/tree/production/rsocket-frames-ts) for the
complete MIME, frame, payload, routing, composite metadata, and authentication
codec API.

## Metadata And Authentication

### Routing

Create protocol metadata with a MIME codec, not with untyped strings:

```ts
import { WellKnownMimeType } from "rsocket-frames-ts";

const route = (name: string) =>
  WellKnownMimeType.MESSAGE_RSOCKET_ROUTING.toMetadata([name]);

connection.requestResponse({ id: 42 }, route("user.find"));
```

Use composite metadata when one request may contain more than one metadata
type, such as routing plus authentication. A fully manual composite is also
accepted:

```ts
import {
  WellKnownAuthType,
  WellKnownMimeType
} from "rsocket-frames-ts";

const authMime = WellKnownMimeType.MESSAGE_RSOCKET_AUTHENTICATION;
const compositeMime = WellKnownMimeType.MESSAGE_RSOCKET_COMPOSITE_METADATA;

const metadata = compositeMime.toMetadata([
  route("user.find"),
  authMime.toMetadata(WellKnownAuthType.BEARER.auth("access-token"))
]);

connection.requestResponse({ id: 42 }, metadata);
```

### Persistent Metadata

`metadataUpdate` changes client-side defaults merged into all later
interactions. This is the simplest way to attach an access token:

```ts
const authMime = WellKnownMimeType.MESSAGE_RSOCKET_AUTHENTICATION;

socket.metadataUpdate((metadata) => {
  metadata.set(
    authMime,
    WellKnownAuthType.BEARER.auth("access-token")
  );
});
```

Use the RSocket simple authentication type when needed:

```ts
socket.metadataUpdate((metadata) => {
  metadata.set(
    authMime,
    WellKnownAuthType.SIMPLE.auth({
      username: "daniel",
      password: "secret"
    })
  );
});
```

Remove authentication without changing other defaults:

```ts
socket.metadataUpdate((metadata) => {
  metadata.remove(authMime);
});
```

Metadata is merged by MIME type:

- Persistent authentication and a controller/request route are both retained.
- Request metadata replaces only a persistent entry with the same MIME type.
- A controller route therefore overrides a persistent route for that request.
- Unrelated persistent metadata is never removed by a request override.

The SETUP metadata MIME controls what can be stored. Composite metadata accepts
any MIME-keyed entries. A direct routing MIME accepts only routing entries; a
direct authentication MIME accepts only authentication entries. Unsupported
updates throw immediately instead of creating an invalid wire payload.

### Metadata Push

`metadataPush` sends an RSocket `METADATA_PUSH` frame to the server. It does not
replace the persistent client defaults managed by `metadataUpdate`:

```ts
await connection
  .metadataPush(
    authMime.toMetadata(
      WellKnownAuthType.BEARER.auth("refreshed-access-token")
    )
  )
  .block();
```

Server support for `METADATA_PUSH` is optional. Use `metadataUpdate` when the
goal is only to attach metadata to subsequent client requests.

## Declarative Controllers

Controllers provide endpoint-specific request and response types while keeping
the direct interaction methods available. Declare a class, select its
interaction model through `extends`, and assign a protected route.

```ts
import { RequestResponseController } from "rsocket-browser";

interface SignInRequest {
  login: string;
  password: string;
}

interface TokenResponse {
  token: string;
}

export default class SignInController extends RequestResponseController<
  SignInRequest,
  TokenResponse
> {
  protected readonly route = "account.sign-in";
}
```

`process` infers its arguments and return type from the controller:

```ts
const token = await connection
  .process(SignInController, {
    login: "login@example.com",
    password: "password"
  })
  .map((response) => response.token)
  .block();
```

The other interaction models follow the same declaration style:

```ts
import {
  FireAndForgetController,
  RequestChannelController,
  RequestStreamController
} from "rsocket-browser";

class DeactivateAccountController extends FireAndForgetController<{
  reason: string;
}> {
  protected readonly route = "account.deactivate";
}

class OnlineController extends RequestStreamController<
  { accountId: number },
  { accountId: number; lastOnlineAt: number; online: boolean }
> {
  protected readonly route = "account.online";
}

class ChatController extends RequestChannelController<
  { room: string; text: string },
  { id: string; room: string; text: string }
> {
  protected readonly route = "chat.messages";
}

connection.process(DeactivateAccountController, { reason: "privacy" });
connection.process(OnlineController, { accountId: 42 });
connection.process(ChatController, [
  { room: "general", text: "Hello" }
]);
```

Class declarations are instantiated once per socket, which reuses encoded route
metadata. Pass an instance when request options, logging, or instance state are
needed:

```ts
const signIn = new SignInController({ timeout: 5_000 }).log({
  payload: false
});

connection.process(signIn, {
  login: "login@example.com",
  password: "password"
});
```

Override `data` or `response` only when the wire shape differs from the public
controller type:

```ts
class FindUserController extends RequestResponseController<number, string> {
  protected readonly route = "user.find";

  protected override data(id: number) {
    return { id };
  }

  protected override response(payload: { data?: unknown }) {
    const user = payload.data as { name: string };
    return user.name;
  }
}
```

Those overrides, `route`, and public `log(...)` are the complete controller
extension surface. Transport, reconnect, and dispatch hooks stay internal.

Controller routes work with direct RSocket routing metadata or composite
metadata. With composite metadata, a route is merged with persistent entries
such as authentication.

## Reconnect And Resume

Automatic reconnect is enabled by default for both browser transports.
Unexpected disconnects, browser offline/online transitions, tab wakeups, and
mobile sleep/wake cycles trigger a new connection attempt. Calling
`disconnect()` stops that process.

Without Resume, reconnect opens a fresh RSocket session with `SETUP`. Enable
protocol Resume by specifying how long the backend retains resumable state:

```ts
const socket = new RSocket("wss://api.example.com/rsocket", {
  setup: {
    keepAlive: 20_000,
    lifetime: 90_000,
    mimetype: {
      data: WellKnownMimeType.APPLICATION_JSON,
      metadata: WellKnownMimeType.MESSAGE_RSOCKET_COMPOSITE_METADATA
    }
  },
  reconnect: {
    resume: {
      ttl: 5 * 60_000
    }
  }
});
```

The client creates and manages the Resume token and byte positions. While the
TTL is valid, it waits for network/server availability and attempts `RESUME`.
If the responder rejects Resume, or the TTL expires, the client automatically
falls back to a fresh `SETUP` connection.

Existing streams and channels survive only when protocol Resume succeeds. A
fresh session cannot recover server-side stream state, so affected interactions
fail and the application must create them again. The client does not guess at
application-level replay or deduplication rules.

Use lifecycle events to update frontend connection state:

```ts
const socket = new RSocket("wss://api.example.com/rsocket", {
  events: {
    disconnect(event) {
      showConnectionBanner(event.message);
    },
    reconnecting(event) {
      showConnectionBanner(event.message);
    },
    resumeRejected(event) {
      console.warn(event.message, event.error);
    },
    connected(event) {
      hideConnectionBanner();
      console.log(event.message);
    }
  }
});
```

Every event includes `type`, `status`, `message`, `connected`, `recovering`,
`attempt`, and relevant error/retry fields.

Disable automatic reconnect explicitly when the application owns retry policy:

```ts
new RSocket("wss://api.example.com/rsocket", {
  reconnect: false
});
```

## Connection Lifecycle

The state-specific surface prevents invalid lifecycle calls:

```ts
const socket = new RSocket("wss://api.example.com/rsocket");
const connected = await socket.connect().block();

if (connected) {
  const disconnected = connected.disconnect(1000, "Signed out");
  await disconnected.connect().block();
}
```

- A disconnected socket exposes interactions, `metadataPush`, `media`,
  `metadataUpdate`, `process`, and `connect`.
- A connected socket exposes the same request API plus `disconnect`, but no
  second `connect` method.
- Requests subscribed before a connection is available wait for the current
  connection attempt. Calling `connect()` explicitly during application startup
  usually makes failure handling clearer.

## Logging And Errors

Enable only the diagnostics needed for the current problem:

```ts
const socket = new RSocket("wss://api.example.com/rsocket", {
  log: {
    enabled: true,
    frames: true,
    lifecycle: true,
    payload: false
  }
});
```

Frame logging covers direct socket interactions. Higher-level interaction
logging is local to one declarative controller:

```ts
const signIn = new SignInController().log({
  interactions: true,
  payload: false
});

connection.process(signIn, {
  login: "login@example.com",
  password: "password"
});
```

Payload logging is disabled by default because payloads may contain credentials
or personal data.

RSocket `ERROR` frames, timeouts, transport failures, invalid demand, and codec
errors are propagated through Reactor error signals. `block()` rejects;
streaming subscribers receive `onError`:

```ts
try {
  await connection
    .requestResponse({ id: 42 }, route("user.find"))
    .block();
} catch (error) {
  console.error("request failed", error);
}
```

## Server Compatibility

The responder must expose either an RSocket WebSocket endpoint or the matching
`RSWT/1` WebTransport endpoint and agree on the SETUP data and metadata MIME
types. For Spring applications, composite metadata plus
`message/x.rsocket.routing.v0` is the usual choice for `@MessageMapping` routes.
The controller `route` field creates that standard routing metadata.

Resume and `METADATA_PUSH` require server-side support. If Resume is rejected,
the client reports `resumeRejected` and opens a fresh session. The package is a
requester only and does not expose a responder/server API.

Large RSocket payloads are fragmented and incoming fragments are reassembled by
the client. Backpressure, cancellation, KEEPALIVE, stream IDs, protocol errors,
and Resume positions are handled at the frame layer.

## Public Exports

The package root intentionally has one small runtime surface:

```ts
import {
  RSocket,
  FireAndForgetController,
  RequestResponseController,
  RequestStreamController,
  RequestChannelController
} from "rsocket-browser";
```

The controller implementations are shared by `rsocket-client-ts`, and the root
re-export keeps application imports concise. Frames, MIME types, metadata, and
authentication codecs remain available from the automatically installed
`rsocket-frames-ts` package. There is no default export and no secondary package
entry point.

## Development

```bash
npm test
npm run build
```

`npm test` type-checks the public API and runs unit, protocol, transport, and
full WebSocket and WebTransport integration tests against `rsocket-server-ts`.
`npm run build` creates the ESM bundle and declarations, then rewrites internal
aliases.
