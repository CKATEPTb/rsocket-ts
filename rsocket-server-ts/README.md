# rsocket-server-ts

RSocket 1.0 responder for TypeScript servers over WebTransport, WebSocket, or
Node TCP.

## Install

```bash
npm install rsocket-server-ts
```

`reactor-core-ts`, `rsocket-core-ts`, and `rsocket-frames-ts` are installed
automatically. The package is ESM-only.

This package contains responder behavior only. Shared frame, fragmentation,
demand, routing, transport-binding, and Resume-buffer mechanics come from
`rsocket-core-ts`; no requester runtime is installed with the server. The
monorepo uses `rsocket-client-ts` only as a development dependency for end-to-end
server tests.

## Define controllers

A controller declares its interaction model, route, request type, and response
type. Register the class when it has no constructor dependencies, or register
an instance when it does.

```ts
import {
  FireAndForgetController,
  RequestChannelController,
  RequestResponseController,
  RequestStreamController,
  RSocketServer
} from "rsocket-server-ts";
import {Flux} from "reactor-core-ts";
import type {RSocketPayloadFrame} from "rsocket-core-ts";

class FindAccount extends RequestResponseController<
  {id: number},
  {id: number; name: string}
> {
  protected readonly route = "account.find";

  handle(request: {id: number}) {
    return {id: request.id, name: "Ada"};
  }
}

class RecordAudit extends FireAndForgetController<{event: string}> {
  protected readonly route = "audit.record";

  handle(request: {event: string}) {
    console.log(request.event);
  }
}

class AccountEvents extends RequestStreamController<
  {accountId: number},
  {event: string}
> {
  protected readonly route = "account.events";

  handle() {
    return Flux.fromArray([{event: "created"}, {event: "updated"}]);
  }
}

class Chat extends RequestChannelController<
  {text: string},
  {accepted: string}
> {
  protected readonly route = "chat.send";

  handle(requests: Flux<RSocketPayloadFrame<{text: string}>>) {
    return requests.map(({data}) => ({accepted: data!.text}));
  }
}

const server = new RSocketServer({
  controllers: [FindAccount, RecordAudit, AccountEvents, Chat]
});
```

Routes are read from `message/x.rsocket.routing.v0` metadata, either directly
or inside `message/x.rsocket.composite-metadata.v0`. A route can also be an
ordered tag list:

```ts
protected readonly route = ["account", "find"] as const;
```

Use `protected readonly route = [] as const` for requests without routing
metadata.

## TCP server

The built-in listener handles the required 24-bit TCP frame-length prefix,
partial reads, and multiple frames in one read.

```ts
const listener = await server.listenTcp({
  host: "127.0.0.1",
  port: 7000
}).block();

if (!listener) throw new Error("RSocket listener did not start");

listener.connections.subscribe(connection => {
  console.log(connection.setup.data);
});

listener.errors.subscribe(error => {
  console.error(error);
});
```

Call `await listener.close().block()` to stop accepting TCP connections. Call
`await server.close().block()` to stop every listener and active session.

## WebSocket server

Pass each WebSocket accepted by your HTTP/WebSocket framework to
`acceptWebSocket`. The socket may use WHATWG events or Node EventEmitter events.
Each binary WebSocket message must contain exactly one raw RSocket frame.

```ts
import {WebSocketServer} from "ws";

const webSockets = new WebSocketServer({port: 8080});

webSockets.on("connection", socket => {
  server.acceptWebSocket(socket).subscribe(
    connection => console.log(connection.setup.data),
    error => console.error(error)
  );
});
```

The `ws` package is only an example. `rsocket-server-ts` does not force an HTTP
or WebSocket framework on the application.

## WebTransport server

Pass each session accepted by your HTTP/3 or WebTransport framework to
`acceptWebTransport`. The package handles RSocket streams and datagrams, but it
does not create the HTTPS/HTTP/3 listener or terminate TLS.

```ts
import {
  RSocketServer,
  type RSocketAcceptedWebTransport
} from "rsocket-server-ts";

async function onWebTransport(session: RSocketAcceptedWebTransport) {
  const connection = await server.acceptWebTransport(session).block();
  console.log(connection?.setup.data);
}
```

The versioned `RSWT/1` mapping assigns one bidirectional stream to connection
control, one bidirectional stream to each request-response, request-stream, or
request-channel interaction, and a reliable unidirectional stream to
fire-and-forget and metadata-push frames. A global record ordinal preserves
RSocket frame order across independently scheduled QUIC streams.
Because RSocket 1.0 does not define a WebTransport binding, peers must use the
same `RSWT/1` mapping.

Datagrams are optional extensions for media payloads and complete
fire-and-forget requests that may be lost:

```ts
const server = new RSocketServer({
  media(payload, connection) {
    console.log("media bytes", payload.byteLength, connection.setup);
  }
});

const connection = await server.acceptWebTransport(session, {
  unreliableFireAndForget: true
}).block();

await connection?.media(new Uint8Array([1, 2, 3])).block();
```

Fragmented fire-and-forget requests automatically use the reliable stream.
When a complete request is sent as a datagram, a reliable marker advances the
RSocket stream sequence even if that datagram is lost. The best-effort mode is
incompatible with Resume because datagrams are intentionally outside Resume
positions and replay buffers.

## Request context

Every controller receives the decoded data and a context containing metadata,
the logical connection, stream ID, and matched route.

```ts
import {
  RequestResponseController,
  type RSocketRequestContext
} from "rsocket-server-ts";
import {Metadata, WellKnownMimeType} from "rsocket-frames-ts";

class CurrentAccount extends RequestResponseController<void, {token: string}> {
  protected readonly route = "account.current";

  handle(_data: void, context: RSocketRequestContext) {
    const entries = context.metadata as Metadata<unknown>[];
    const auth = entries.find(entry =>
      entry.mimeType === WellKnownMimeType.MESSAGE_RSOCKET_AUTHENTICATION
    );
    const authentication = auth?.payload as {data?: unknown} | undefined;

    return {token: String(authentication?.data ?? "")};
  }
}
```

For composite metadata, `context.metadata` is the ordered `Metadata[]`. For a
direct metadata MIME type it is that codec's decoded value. The matching route
is also available as `context.route`.

## Metadata push

Handle client `METADATA_PUSH` frames in server options:

```ts
const server = new RSocketServer({
  controllers: [FindAccount],
  metadataPush(metadata, connection) {
    console.log(metadata.mimeType.mimeType, metadata.payload);
  }
});
```

Send metadata to a client through the accepted connection. A typed entry is
automatically wrapped when SETUP negotiated composite metadata.

```ts
import {WellKnownMimeType} from "rsocket-frames-ts";

await connection
  .metadataPush("maintenance", WellKnownMimeType.TEXT_PLAIN)
  .block();
```

## SETUP policy

Use `accept` for synchronous setup validation or authentication. Returning
`false` sends `ERROR[REJECTED_SETUP]` and closes the transport.

```ts
interface SetupData {
  client: string;
}

interface SetupMetadata {
  tenant: string;
}

const server = new RSocketServer<SetupData, SetupMetadata>({
  controllers: [FindAccount],
  accept(setup) {
    console.log(setup.data, setup.metadata);
    console.log(setup.keepAliveMs, setup.lifetimeMs);
    return setup.majorVersion === 1 && setup.minorVersion === 0;
  }
});
```

The optional `D` and `M` generics are retained by `accept`, `metadataPush`,
`activityListener`, accepted connections, and TCP listener connection streams.
They state the SETUP contract expected by the application; validate MIME names
or authentication in `accept` when clients are not fully trusted.

The server enforces the lifetime requested by the client from client KEEPALIVE
frames; ordinary request traffic does not hide a missing heartbeat. KEEPALIVE
frames are echoed automatically. Application-initiated server KEEPALIVE is
available as `connection.keepAlive(data)`.

## Resume

Enable protocol Resume by setting how long disconnected logical sessions are
retained. The server validates positions, keeps active streams and demand,
replays unacknowledged frames in order, and rejects unknown or expired tokens.

```ts
const server = new RSocketServer({
  controllers: [AccountEvents, Chat],
  resume: {
    ttlMs: 5 * 60_000,
    maxBufferBytes: 16 * 1024 * 1024
  }
});
```

The client chooses the opaque token in its SETUP frame. On transport loss it
must reconnect with a RESUME frame before `ttlMs` expires. If the process
restarts or the retained state is gone, the server sends
`ERROR[REJECTED_RESUME]`; the client can then open a fresh SETUP session.

Resume is protocol state recovery, not a server-side reconnect scheduler. The
`RSocket` facade from `rsocket-client-ts` performs reconnect and Resume for
WebTransport, WebSocket, or TCP. `rsocket-browser` uses that same facade with
additional browser availability and wake signals.

## Fragmentation and backpressure

Fragmentation and reassembly are automatic in both directions. Set the same
practical frame limit on client and server when a deployment needs smaller
frames:

```ts
const server = new RSocketServer({
  controllers: [FindAccount],
  maxFrameLength: 64 * 1024
});
```

Metadata fragments are always reassembled before data fragments. No controller
code is required for large payloads.

Request-stream responses are emitted only under client `REQUEST_N` demand.
Request-channel applies independent demand in both directions: the server asks
for client payloads and sends responses only when the client asks for them.

## Lease and errors

When a client SETUP enables lease support, the server sends the configured
lease:

```ts
const server = new RSocketServer({
  controllers: [FindAccount],
  lease: {ttlMs: 30_000, requests: 100}
});
```

The server also enforces the lease TTL and request count. A requester that
bypasses its own lease checks receives `ERROR[REJECTED]` and its controller is
not called. Calling `connection.lease(...)` replaces the current grant. A SETUP
that requests leasing is rejected with `ERROR[UNSUPPORTED_SETUP]` when the
server has no `lease` policy, instead of accepting a connection that can never
open an interaction.

Throw a normal error for `APPLICATION_ERROR`, or choose another legal stream
code explicitly:

```ts
import {FrameErrorCode} from "rsocket-frames-ts";
import {RSocketRequestError} from "rsocket-server-ts";

throw new RSocketRequestError("Request rejected", FrameErrorCode.REJECTED);
```

## Options

| Option | Purpose | Default |
| --- | --- | --- |
| `controllers` | Controller classes or dependency-injected instances | `[]` |
| `resume.ttlMs` | Retention time for a suspended logical session | Resume disabled |
| `resume.maxBufferBytes` | Maximum unacknowledged server bytes retained for replay | `16 MiB` |
| `lease` | Lease sent when the client requests lease semantics | Disabled; lease-enabled SETUP is rejected |
| `maxFrameLength` | Largest raw frame before fragmentation | Protocol maximum |
| `handshakeTimeoutMs` | Maximum delay before an accepted transport sends SETUP or RESUME | `10_000` |
| `accept` | Synchronous SETUP policy | Accept |
| `metadataPush` | Client metadata callback | Ignore |
| `media` | Best-effort WebTransport media-datagram callback | Ignore |
| `activityListener` | Observes decoded incoming and outgoing frames | Disabled |

`RSocketServer.accept(transport)` accepts any ordered
`RSocketTransportConnection` from `rsocket-core-ts`. Most applications only
need `listenTcp`, `acceptWebSocket`, or `acceptWebTransport`.

An accepted connection also exposes `metadataPush(...)`, `media(...)`,
`lease(...)`, `keepAlive(...)`, and `disconnect(...)`. Every command is a cold
`Mono`: it is sent only after `subscribe()` or `block()`.

The wire behavior follows the
[RSocket protocol specification](https://github.com/rsocket/rsocket/blob/master/Protocol.md).

## Public API

The package root has six runtime exports:

```ts
import {
  RSocketServer,
  RSocketRequestError,
  FireAndForgetController,
  RequestChannelController,
  RequestResponseController,
  RequestStreamController
} from "rsocket-server-ts";
```

`RSocketServer` exposes only `accept`, `acceptWebSocket`,
`acceptWebTransport`, `listenTcp`, and `close`. An accepted connection exposes
only its immutable `setup` information and `metadataPush`, `media`, `lease`,
`keepAlive`, and `disconnect` commands. Named TypeScript types for options,
contexts, transports, listeners, and connections are also available from the
root without adding runtime exports. There is no default export or technical
package subpath.

## Development

```bash
npm test
npm run build
```

`npm test` runs the responder protocol contract and the same interaction,
backpressure, fragmentation, error, and Resume conformance suite independently
over TCP, WebSocket, and WebTransport.
