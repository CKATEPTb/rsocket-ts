# rsocket-core-ts

Transport-neutral RSocket protocol helpers for TypeScript endpoint authors.

## Install

```bash
npm install rsocket-core-ts
```

Use this package when building an RSocket requester, responder, or transport
adapter. Application code normally starts with
[`rsocket-browser`](https://www.npmjs.com/package/rsocket-browser) or
[`rsocket-client-ts`](https://www.npmjs.com/package/rsocket-client-ts), while
TypeScript responders normally use
[`rsocket-server-ts`](https://www.npmjs.com/package/rsocket-server-ts).

Core does not create or listen on sockets. It does not contain requester or
responder sessions, controllers, reconnect scheduling, or Resume policy. It
only operates on supplied frames and already-created transport objects.

## Shared endpoint primitives

Both requester and responder implementations use the same core behavior:

| API | Purpose |
| --- | --- |
| `AsyncQueue` | Ordered callback-to-async-iterator bridge |
| `addReactiveDemand` and demand validators | Exact, saturating Reactive Streams demand |
| `RSocketReplayBuffer` | Bounded implied-position storage for protocol Resume |
| `normalizeRSocketRoute`, `routingTags` | Route validation and metadata extraction |
| `ReactiveTransportBinding` | One disposable subscription around a physical transport |
| `RSocketInactivityTimer` | Low-churn lifetime tracking |
| `TcpFrameDecoder`, `encodeTcpFrame` | RSocket's 24-bit TCP packet boundary |
| `webSocketFrameFlux` | Ordered binary WebSocket messages, including `Blob` |
| `createWebTransportConnection` | Versioned RSocket mapping over WebTransport streams and datagrams |

These are endpoint-building primitives, not a client or server API. Resume
retry timing and the requester/responder stream state machines remain in their
respective packages.

## Payloads

RSocket data and metadata are encoded by MIME types from `rsocket-frames-ts`.
Core accepts four payload forms:

- a plain data value;
- an encoded `Payload`;
- an encoded `Metadata`;
- `{data, metadata, dataMimeType?, metadataMimeType?}`.

```ts
import {
  compositeMetadata,
  data,
  metadata,
  route
} from "rsocket-core-ts";
import {WellKnownMimeType} from "rsocket-frames-ts";

const body = data(
  {id: 42},
  WellKnownMimeType.APPLICATION_JSON
);

const routing = route("account", "find");

const trace = metadata(
  "8f3a1c",
  WellKnownMimeType.TEXT_PLAIN
);

const requestMetadata = compositeMetadata(routing, trace);
```

`route(...segments)` creates RSocket routing metadata. Each argument is one
routing tag, not a dot-separated parser.

## Routing and authentication

Composite metadata lets one request carry routing, authentication, tracing,
and other independent entries.

```ts
import {compositeMetadata, route} from "rsocket-core-ts";
import {
  WellKnownAuthType,
  WellKnownMimeType
} from "rsocket-frames-ts";

const authorization =
  WellKnownMimeType.MESSAGE_RSOCKET_AUTHENTICATION.toMetadata(
    WellKnownAuthType.BEARER.auth("access-token")
  );

const requestMetadata = compositeMetadata(
  route("account.find"),
  authorization
);
```

Use `WellKnownAuthType.SIMPLE.auth({username, password})` for RSocket Simple
Authentication. Send credentials only over an encrypted transport.

## Encode a request

`encodePayloadInput` converts application input into frame-ready `Payload` and
`Metadata` objects.

```ts
import {encodePayloadInput, route} from "rsocket-core-ts";
import {
  FrameFlag,
  RequestResponseFrame,
  WellKnownMimeType
} from "rsocket-frames-ts";

const dataMimeType = WellKnownMimeType.APPLICATION_JSON;
const metadataMimeType =
  WellKnownMimeType.MESSAGE_RSOCKET_COMPOSITE_METADATA;

const encoded = encodePayloadInput(
  {
    data: {id: 42},
    metadata: route("account.find")
  },
  dataMimeType,
  metadataMimeType
);

const frame = new RequestResponseFrame(
  1,
  FrameFlag.NONE,
  encoded.metadata,
  encoded.payload
);

const bytes = frame.toUint8Array();
```

When the negotiated metadata MIME is composite, a single typed metadata entry
is wrapped in a composite container automatically. With a direct metadata MIME,
the entry MIME must match it.

## Decode a frame

`deserializeFrame` decodes one complete raw RSocket frame. The buffer must not
contain the TCP 24-bit length prefix.

```ts
import {
  decodeFramePayload,
  deserializeFrame
} from "rsocket-core-ts";
import {WellKnownMimeType} from "rsocket-frames-ts";

const frame = deserializeFrame(
  bytes,
  WellKnownMimeType.MESSAGE_RSOCKET_COMPOSITE_METADATA,
  WellKnownMimeType.APPLICATION_JSON
);

const payload = decodeFramePayload<{id: number}>(frame);
console.log(payload.data?.id);
console.log(payload.frame.header.streamId);
```

The decoded value contains:

- `data` and `metadata` application values;
- `dataPayload` and `metadataPayload` codec objects;
- `frame`, the original decoded RSocket frame.

ERROR and KEEPALIVE payloads are decoded with their protocol MIME types
automatically.

## Inspect a frame header

The header readers avoid deserializing a complete frame when routing bytes on a
hot path.

```ts
import {
  readFrameStreamId,
  readFrameTypeAndFlags
} from "rsocket-core-ts";

const streamId = readFrameStreamId(bytes);
const typeAndFlags = readFrameTypeAndFlags(bytes);
const frameType = typeAndFlags >>> 10;
const flags = typeAndFlags & 0x03ff;
```

`readKeepalivePosition(bytes)` reads the implied Resume position directly from
a KEEPALIVE frame.

## Fragmentation

RSocket fragmentation applies to raw RSocket frames. A TCP length prefix is
added after fragmentation; WebSocket sends every fragment as its own binary
message; WebTransport puts every fragment in its own ordered mapping record.

```ts
import {
  emitOutboundFrameFragments,
  outboundFrameLength
} from "rsocket-core-ts";
import type {Frame} from "rsocket-frames-ts";

const maxFrameLength = 16_384;

function writeFrame(
  frame: Frame,
  write: (bytes: Uint8Array) => void
) {
  const frameLength = outboundFrameLength(frame);

  if (frameLength === undefined) {
    write(frame.toUint8Array());
    return;
  }

  emitOutboundFrameFragments(
    frame,
    frameLength,
    maxFrameLength,
    (fragment) => write(fragment.toUint8Array())
  );
}
```

The callback receives the original frame once when it already fits. Oversized
REQUEST_FNF, REQUEST_RESPONSE, REQUEST_STREAM, REQUEST_CHANNEL, and PAYLOAD
frames are emitted as one initial frame followed by PAYLOAD fragments.

## Reassembly

Decode fragmented PAYLOAD frames as bytes, then reassemble and decode the
complete logical payload:

```ts
import {
  decodeFramePayload,
  deserializeFrame,
  readFrameStreamId,
  readFrameTypeAndFlags,
  reassemblePayloadFrame,
  type PayloadFragmentMap
} from "rsocket-core-ts";
import {
  PayloadFlag,
  PayloadFrame,
  WellKnownMimeType
} from "rsocket-frames-ts";

const dataMimeType = WellKnownMimeType.APPLICATION_JSON;
const metadataMimeType =
  WellKnownMimeType.MESSAGE_RSOCKET_COMPOSITE_METADATA;
const rawMimeType = WellKnownMimeType.APPLICATION_OCTET_STREAM;
const fragments: PayloadFragmentMap = new Map();

function receive(bytes: Uint8Array) {
  const streamId = readFrameStreamId(bytes);
  const flags = readFrameTypeAndFlags(bytes) & 0x03ff;
  const fragmented =
    fragments.has(streamId) ||
    (flags & PayloadFlag.FOLLOWS) !== 0;

  const frame = deserializeFrame(
    bytes,
    metadataMimeType,
    dataMimeType,
    fragmented
      ? {
          metadataMimeType: rawMimeType,
          dataMimeType: rawMimeType
        }
      : undefined
  );

  if (!(frame instanceof PayloadFrame) || !fragmented) return frame;

  const complete = reassemblePayloadFrame(
    frame,
    fragments,
    metadataMimeType,
    dataMimeType
  );

  return complete === undefined
    ? undefined
    : decodeFramePayload(complete);
}
```

The map is keyed by stream ID. Remove stream state when the stream is cancelled
or terminated if your endpoint owns additional fragment lifecycle around this
helper.

## Transport contract

Implement `RSocketTransportConnection` to connect a protocol endpoint to a new
transport:

| Member | Contract |
| --- | --- |
| `opened: Mono<void>` | Completes when writes are possible |
| `frames: Flux<Uint8Array>` | Emits complete raw RSocket frames in order |
| `errors: Flux<unknown>` | Emits native transport errors |
| `closes: Flux<RSocketTransportClose>` | Emits physical close information |
| `media?: Flux<Uint8Array>` | Emits optional best-effort media payloads outside RSocket positions |
| `skippedFireAndForget?: Flux<number>` | Reports stream IDs reserved by lost best-effort FNF datagrams |
| `isOpen` | Reports whether `write` can be used |
| `write(frame)` | Writes one complete raw RSocket frame |
| `writeMedia?(payload)` | Writes optional best-effort media outside RSocket positions |
| `close(options?)` | Closes the physical transport |

WebSocket adapters normally map one binary message to one `frames` item. TCP
adapters must remove the 24-bit frame-length prefix and handle partial or
combined reads before emitting an item. Multiplexed adapters must restore the
sender's frame order before emitting across independently scheduled streams.

For an already-created stream socket, `ReactiveTcpTransportConnection` provides
the shared framing, write, close, and event behavior. Supply a role-specific
`Mono<void>` that represents readiness: a client waits for `connect`, while a
server can immediately accept an open socket. `webSocketFrameFlux(socket)`
provides the equivalent ordered receive side for WHATWG and Node-compatible
WebSockets.

## WebTransport mapping

`createWebTransportConnection` wraps an already-created browser or server
WebTransport session. Its structural types follow the
[W3C WebTransport API](https://www.w3.org/TR/webtransport/) and depend only on
WHATWG-style stream methods, so core does not install an HTTP/3 server or a
browser runtime.
The adapter uses the current `datagrams.createWritable()` API and also accepts
the earlier `datagrams.writable` shape. Reliable stream creation waits for QUIC
stream credit instead of failing transiently at the peer's concurrency limit.

```ts
import {createWebTransportConnection} from "rsocket-core-ts";

const requesterTransport = createWebTransportConnection(session, {
  role: "requester",
  maxFrameLength: 64 * 1024,
  maxReorderBufferBytes: 16 * 1024 * 1024
});

await requesterTransport.opened.block();
```

Use `role: "responder"` around the peer session. Applications normally let
`rsocket-client-ts` or `rsocket-server-ts` create this transport instead.

The versioned `RSWT/1` mapping is:

| WebTransport lane | RSocket traffic |
| --- | --- |
| Connection-control bidirectional stream | `SETUP`, `KEEPALIVE`, `LEASE`, `RESUME`, `RESUME_OK`, connection `ERROR`, and stream-zero `EXT` |
| One bidirectional stream per interaction | `REQUEST_RESPONSE`, `REQUEST_STREAM`, `REQUEST_CHANNEL`, and their follow-up frames |
| Reliable unidirectional stream | `REQUEST_FNF`, `METADATA_PUSH`, and fragmented FNF continuations |
| Datagram extension | Optional complete best-effort FNF and media payloads |

Every reliable record carries a global ordinal. The receiver bounds and
reorders records by that ordinal before exposing `frames`, preserving the
synchronous RSocket write order even when QUIC schedules streams differently.

Complete fire-and-forget frames may use datagrams when
`unreliableFireAndForget` is enabled and the packet fits. A reliable skip marker
reserves the same stream ID if the datagram is lost. Fragmented FNF always uses
the reliable stream. Best-effort FNF cannot be combined with protocol Resume;
media payloads are never counted in RSocket Resume positions.

RSocket 1.0 does not specify a WebTransport binding, so both peers must use this
project's mapping version. Stream prefaces reject incompatible versions before
their records enter the protocol engine.

## Errors

| Error | Meaning |
| --- | --- |
| `RSocketConnectionError` | Transport is closed, failed, or unavailable |
| `RSocketProtocolError` | Incoming or outgoing data violates the protocol |
| `RSocketFrameSizeError` | A frame cannot fit the configured maximum |

`errorFromFrame(frame)` preserves an ERROR frame's code, stream ID, and payload
message. `connectionClosedError(reason)` normalizes native close values.

## Protocol reference

See the [RSocket Protocol 1.0 specification](https://github.com/rsocket/rsocket/blob/master/Protocol.md)
for frame fields, flags, stream rules, fragmentation, and Resume semantics.

## Development

```bash
npm test
npm run build
```
