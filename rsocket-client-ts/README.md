# rsocket-client-ts

TypeScript RSocket requester with WebSocket and Node TCP transports. It includes
all four interaction models, Reactive Streams backpressure, automatic
fragmentation, reconnect, protocol Resume, lifecycle events, logging, persistent
metadata, and declarative controllers.

```bash
npm install rsocket-client-ts
```

`reactor-core-ts`, `rsocket-core-ts`, `rsocket-frames-ts`, and `bebyte` are
installed automatically.

## Quick start

```ts
import {RSocket} from "rsocket-client-ts";
import {WellKnownMimeType} from "rsocket-frames-ts";

const route = (name: string) =>
  WellKnownMimeType.MESSAGE_RSOCKET_ROUTING.toMetadata([name]);

const socket = new RSocket({
  transport: {
    type: "websocket",
    url: "wss://api.example.com/rsocket"
  },
  setup: {
    keepAlive: 20_000,
    lifetime: 90_000,
    mimetype: {
      data: WellKnownMimeType.APPLICATION_JSON,
      metadata: WellKnownMimeType.MESSAGE_RSOCKET_COMPOSITE_METADATA
    }
  }
});

const connected = await socket.connect().block();

const response = await socket
  .requestResponse({id: 42}, route("account.find"))
  .block();

console.log(response?.data);
connected?.disconnect();
```

`connect()` and every interaction are lazy. The connection or request starts
only when its `Mono` or `Flux` is subscribed, blocked, or iterated.

## Public API

The package root exports only one client and four abstract controller classes:

```ts
import {
  RSocket,
  FireAndForgetController,
  RequestChannelController,
  RequestResponseController,
  RequestStreamController
} from "rsocket-client-ts";
```

A disconnected `RSocket` exposes only:

```text
connect
metadataPush
metadataUpdate
fireAndForget
requestResponse
requestStream
requestChannel
process
```

The value emitted by `connect()` exposes the same request methods and
`disconnect`, but not `connect`. There are no transport constructors, internal
session classes, default export, or technical package subpaths.

## Transports

Select WebSocket with a URL:

```ts
const socket = new RSocket({
  transport: {
    type: "websocket",
    url: "wss://api.example.com/rsocket",
    protocols: ["rsocket"]
  }
});
```

The global WHATWG `WebSocket` constructor is used by default. A compatible
factory can be supplied for another runtime:

```ts
const socket = new RSocket({
  transport: {
    type: "websocket",
    url: "wss://api.example.com/rsocket",
    factory: (url, protocols) => new WebSocket(url, protocols)
  }
});
```

Select Node TCP with a host and port:

```ts
const socket = new RSocket({
  transport: {
    type: "tcp",
    host: "127.0.0.1",
    port: 7000
  }
});
```

TCP framing is automatic. The client writes and removes the RSocket 24-bit
length prefix and handles partial or combined socket reads.

Use [`rsocket-browser`](https://www.npmjs.com/package/rsocket-browser) when a
browser application also needs online, focus, page restore, and mobile
sleep/wake recovery signals.

## Interactions

All direct interactions use the same positional shape:

```text
socket.fireAndForget(data?, metadata?, mimetype?);
socket.requestResponse(data?, metadata?, mimetype?);
socket.requestStream(data?, metadata?, mimetype?);
socket.requestChannel(source?, metadata?, mimetype?);
```

The SETUP codecs determine default argument types and every decoded response
type. The optional third argument changes only the outbound value and encoding
for that request:

```ts
await socket
  .fireAndForget(
    "ready",
    route("status.update"),
    {data: WellKnownMimeType.TEXT_PLAIN}
  )
  .block();
```

An already encoded `Payload<T>` can be sent directly regardless of the default
SETUP data type. A per-request MIME override does not renegotiate the MIME of
the response; responses still use the SETUP codecs.

Fire-and-forget completes after the frame is written. Request-response emits at
most one decoded payload. Protocol and transport failures are Reactor error
signals, so `block()` rejects and streams call `onError`.

Request-stream sends demand only when the subscriber requests values:

```ts
socket.requestStream({teamId: 7}, route("users.watch")).subscribe({
  onSubscribe(subscription) {
    subscription.request(20);
  },
  onNext(payload) {
    console.log(payload.data);
  },
  onError(error) {
    console.error(error);
  }
});
```

Request-channel applies backpressure in both directions. Pass an iterable,
async iterable, promise, or Reactive Streams publisher:

```ts
const replies = socket.requestChannel(
  [
    {room: "general", text: "Hello"},
    {room: "general", text: "World"}
  ],
  route("chat.messages")
);

for await (const reply of replies) {
  console.log(reply.data);
}
```

Call it without a source for an imperative channel:

```ts
const channel = socket.requestChannel(undefined, route("chat.messages"));

channel.subscribe({
  onSubscribe(subscription) {
    subscription.request(50);
  },
  onNext(payload) {
    console.log(payload.data);
  }
});

channel.next({room: "general", text: "Hello"});
channel.complete();
```

Outbound channel items stay queued until the responder sends `REQUEST_N`.

## Metadata and authentication

Use codecs from `rsocket-frames-ts` to create typed protocol metadata:

```ts
import {
  WellKnownAuthType,
  WellKnownMimeType
} from "rsocket-frames-ts";

const routingMime = WellKnownMimeType.MESSAGE_RSOCKET_ROUTING;
const authMime = WellKnownMimeType.MESSAGE_RSOCKET_AUTHENTICATION;

socket.requestResponse(
  {id: 42},
  routingMime.toMetadata(["account.find"])
);
```

`metadataUpdate` stores MIME-keyed defaults for later requests:

```ts
socket.metadataUpdate((metadata) => {
  metadata.set(
    authMime,
    WellKnownAuthType.BEARER.auth("access-token")
  );
});

socket.metadataUpdate((metadata) => {
  metadata.remove(authMime);
});
```

Persistent authentication and request routing are both retained when SETUP uses
composite metadata. Request metadata overrides only a persistent entry with the
same MIME type. A direct SETUP metadata MIME accepts updates only for that MIME;
unsupported updates throw immediately.

`metadataPush` sends a protocol `METADATA_PUSH` frame and does not modify the
persistent defaults:

```ts
await socket
  .metadataPush(
    authMime.toMetadata(
      WellKnownAuthType.BEARER.auth("refreshed-token")
    )
  )
  .block();
```

The responder must support `METADATA_PUSH`.

## Controllers

Controllers give a route a typed request and response contract:

```ts
import {RequestResponseController} from "rsocket-client-ts";

interface SignInRequest {
  login: string;
  password: string;
}

interface TokenResponse {
  token: string;
}

class SignInController extends RequestResponseController<
  SignInRequest,
  TokenResponse
> {
  protected readonly route = "account.sign-in";
}

const token = await socket
  .process(SignInController, {
    login: "user@example.com",
    password: "secret"
  })
  .map((response) => response.token)
  .block();
```

The same pattern works with `FireAndForgetController`,
`RequestStreamController`, and `RequestChannelController`. Pass an instance to
enable diagnostics or request options for one controller:

```ts
const signIn = new SignInController({timeout: 5_000}).log({
  interactions: true,
  payload: false
});

socket.process(signIn, {
  login: "user@example.com",
  password: "secret"
});
```

Controller subclasses expose only the required `route`, the applicable `data`
or `response` override, and public `log(...)`. Dispatch and reconnect machinery
remain private.

Controller routes are merged with persistent authentication when SETUP uses
composite metadata.

## Reconnect and Resume

Reconnect is opt-in in `rsocket-client-ts`:

```ts
new RSocket({
  transport: {type: "tcp", host: "127.0.0.1", port: 7000},
  reconnect: true
});
```

This opens a fresh SETUP session after transport loss. Enable protocol Resume
by specifying how long the responder retains resumable state:

```ts
new RSocket({
  transport: {type: "websocket", url: "wss://api.example.com/rsocket"},
  reconnect: {
    resume: {
      ttl: 5 * 60_000
    }
  },
  events: {
    reconnecting(event) {
      console.log(event.message);
    },
    resumeRejected(event) {
      console.warn(event.message, event.error);
    },
    connected(event) {
      console.log(event.message);
    }
  }
});
```

The client creates the token, tracks byte positions, replays unacknowledged
frames, and attempts `RESUME` while the TTL remains valid. If the responder
rejects Resume or the TTL expires, the client reports the event and falls back
to a fresh SETUP session. Calling `disconnect()` stops reconnect attempts.

## Setup and logging

```ts
const socket = new RSocket({
  transport: {type: "websocket", url: "wss://api.example.com/rsocket"},
  setup: {
    keepAlive: 20_000,
    lifetime: 90_000,
    lease: true,
    fragmentSize: 16 * 1024,
    mimetype: {
      data: WellKnownMimeType.APPLICATION_JSON,
      metadata: WellKnownMimeType.MESSAGE_RSOCKET_COMPOSITE_METADATA
    },
    payload: {data: {client: "dashboard"}}
  },
  log: {
    enabled: true,
    frames: true,
    lifecycle: true,
    payload: false
  }
});
```

`fragmentSize` is the largest raw RSocket frame. Oversized outgoing payloads
are fragmented and incoming fragments are reassembled automatically. Payload
logging is disabled by default because payloads may contain credentials. Set
`lease: true` only when the responder issues `LEASE` frames; new interactions
then consume responder-granted credit and fail locally when none is available.

## Development

```bash
npm test
npm run build
```

`npm test` runs the transport-neutral protocol contract plus independent real
TCP and WebSocket requester integration suites.
