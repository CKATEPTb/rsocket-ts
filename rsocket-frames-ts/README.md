# rsocket-frames-ts

Encode and decode [RSocket Protocol 1.0 frames](https://github.com/rsocket/rsocket/blob/master/Protocol.md)
and metadata over WebSocket or TCP.

## Install

```bash
npm install rsocket-frames-ts
```

## WebSocket

```ts
import {
  FrameCodec,
  FrameFlag,
  RequestResponseFrame,
  WellKnownMimeType
} from "rsocket-frames-ts";

const metadataMime = WellKnownMimeType.MESSAGE_RSOCKET_COMPOSITE_METADATA;
const dataMime = WellKnownMimeType.APPLICATION_JSON;
const codec = new FrameCodec({
  transport: "websocket",
  mimetype: {
    metadata: metadataMime,
    data: dataMime
  }
});

const metadata = metadataMime.toMetadata([
  WellKnownMimeType.MESSAGE_RSOCKET_ROUTING.toMetadata(["account.find"])
]);

const request = new RequestResponseFrame(
  1,
  FrameFlag.NONE,
  metadata,
  dataMime.toPayload({accountId: 42})
);

webSocket.send(codec.serialize(request));

const [response] = codec.deserialize(bytesFromWebSocket);
if (response !== undefined) handleFrame(response);
```

Client-initiated streams use odd IDs. Server-initiated streams use even IDs.

## TCP

The same codec handles TCP's three-byte length prefix, partial reads and multiple
frames received in one chunk.

```ts
const codec = new FrameCodec({
  transport: "tcp",
  mimetype: {
    metadata: WellKnownMimeType.MESSAGE_RSOCKET_COMPOSITE_METADATA,
    data: WellKnownMimeType.APPLICATION_JSON
  }
});

socket.write(codec.serialize(request));

socket.on("data", chunk => {
  for (const frame of codec.deserialize(chunk)) handleFrame(frame);
});

socket.on("end", () => codec.finish());
socket.on("error", () => codec.reset());
```

## MIME types

Built-in codecs are available through `WellKnownMimeType`.

| Data | Constants |
| --- | --- |
| JSON | `APPLICATION_JSON`, `APPLICATION_CLOUDEVENTS_JSON` |
| Text | `APPLICATION_GRAPHQL`, `APPLICATION_JAVASCRIPT`, `APPLICATION_XML`, `TEXT_CSS`, `TEXT_CSV`, `TEXT_HTML`, `TEXT_PLAIN`, `TEXT_XML` |
| Binary | `APPLICATION_AVRO`, `APPLICATION_CBOR`, `APPLICATION_GZIP`, `APPLICATION_OCTET_STREAM`, `APPLICATION_PDF`, `APPLICATION_PROTOBUF`, `APPLICATION_THRIFT`, `APPLICATION_ZIP`, `MULTIPART_MIXED`, `AUDIO_*`, `IMAGE_*`, `VIDEO_*` |
| RSocket metadata | `MESSAGE_RSOCKET_MIMETYPE`, `MESSAGE_RSOCKET_ACCEPT_MIMETYPES`, `MESSAGE_RSOCKET_AUTHENTICATION`, `MESSAGE_RSOCKET_TRACING_ZIPKIN`, `MESSAGE_RSOCKET_ROUTING`, `MESSAGE_RSOCKET_COMPOSITE_METADATA` |

Use `toPayload` for frame data and `toMetadata` for metadata:

```ts
const payload = WellKnownMimeType.APPLICATION_JSON.toPayload({id: 1});
const decoded = WellKnownMimeType.APPLICATION_JSON.toPayload(
  payload.toUint8Array()
);
```

Use a generic codec when values are already encoded:

```ts
import {MimeType} from "rsocket-frames-ts";

const msgpack = new MimeType<Uint8Array>("application/msgpack");
const payload = msgpack.toPayload(encodedMsgpack);
```

## Routing and authentication

Routing and authentication are entries in composite metadata.

```ts
import {
  WellKnownAuthType,
  WellKnownMimeType
} from "rsocket-frames-ts";

const route = WellKnownMimeType.MESSAGE_RSOCKET_ROUTING.toMetadata([
  "account.sign-in"
]);

const auth = WellKnownMimeType.MESSAGE_RSOCKET_AUTHENTICATION.toMetadata(
  WellKnownAuthType.BEARER.auth("access-token")
);

const metadata =
  WellKnownMimeType.MESSAGE_RSOCKET_COMPOSITE_METADATA.toMetadata([
    route,
    auth
  ]);
```

Simple authentication uses the same metadata type:

```ts
const auth = WellKnownMimeType.MESSAGE_RSOCKET_AUTHENTICATION.toMetadata(
  WellKnownAuthType.SIMPLE.auth({
    username: "user@example.com",
    password: "secret"
  })
);
```

Use `wss` or TLS-wrapped TCP when sending credentials.

## Custom MIME types

Every MIME codec extends `MimeType<T>` and implements four methods: serialization
and deserialization for data, then serialization and deserialization for metadata.

### Override `application/json`

The following class implements the same behavior as the built-in JSON codec:

```ts
import type {ByteReader} from "bebyte";
import type {Metadata, Payload} from "rsocket-frames-ts";
import {MimeType} from "rsocket-frames-ts";

const encoder = new TextEncoder();
const decoder = new TextDecoder();

function encodeJson(value: unknown): Uint8Array {
  const json = JSON.stringify(value);
  if (json === undefined) {
    throw new TypeError("JSON value is not serializable");
  }
  return encoder.encode(json);
}

function decodeJson<T>(bytes: Uint8Array): T | string | undefined {
  if (bytes.length === 0) return undefined;
  const text = decoder.decode(bytes);
  try {
    return JSON.parse(text) as T;
  } catch {
    return text;
  }
}

class ApplicationJson<T = unknown> extends MimeType<T> {
  constructor() {
    super("application/json", 0x05);
  }

  protected override serializePayload(value: T): Payload<T> {
    return super.serializePayload(encodeJson(value) as unknown as T);
  }

  protected override deserializePayload(reader: ByteReader): Payload<T> {
    const bytes = reader.viewRemaining();
    return new Payload(this, decodeJson<T>(bytes), bytes);
  }

  protected override serializeMetadata(value: T): Metadata<T> {
    return super.serializeMetadata(encodeJson(value) as unknown as T);
  }

  protected override deserializeMetadata(
    reader: ByteReader,
    hasPayload = true
  ): Metadata<T> {
    const bytes = hasPayload
      ? reader.viewBytes(reader.i24())
      : reader.viewRemaining();
    return new Metadata(
      this,
      bytes.length === 0 ? "" as T : decodeJson<T>(bytes),
      bytes
    );
  }
}

export const APPLICATION_JSON = new ApplicationJson();
```

Well-known names and identifiers are assigned by the
[RSocket well-known MIME type registry](https://github.com/rsocket/rsocket/blob/master/Extensions/WellKnownMimeTypes.md).
`application/json` has identifier `0x05`, so both values are passed to `super`.
Creating the instance replaces registry lookups by name and identifier. Never
invent an identifier: only MIME types listed in the registry may have one.

### Register a custom MIME type

A custom MIME type implements the same four methods, but passes only its name to
`super`. Its full name is written to composite metadata.

```ts
type Event = {
  name: string;
  createdAt: string;
};

class EventJson extends MimeType<Event> {
  constructor() {
    super("application/vnd.example.event+json");
  }

  protected override serializePayload(value: Event): Payload<Event> {
    return super.serializePayload(encodeJson(value) as unknown as Event);
  }

  protected override deserializePayload(reader: ByteReader): Payload<Event> {
    const bytes = reader.viewRemaining();
    return new Payload(this, decodeJson<Event>(bytes), bytes);
  }

  protected override serializeMetadata(value: Event): Metadata<Event> {
    return super.serializeMetadata(encodeJson(value) as unknown as Event);
  }

  protected override deserializeMetadata(
    reader: ByteReader,
    hasPayload = true
  ): Metadata<Event> {
    const bytes = hasPayload
      ? reader.viewBytes(reader.i24())
      : reader.viewRemaining();
    if (bytes.length === 0) return "" as unknown as Metadata<Event>;
    return new Metadata(this, decodeJson<Event>(bytes), bytes);
  }
}

export const EVENT_JSON = new EventJson();
```

## References

- [RSocket metadata extensions](https://github.com/rsocket/rsocket/tree/master/Extensions)
- [Well-known MIME type registry](https://github.com/rsocket/rsocket/blob/master/Extensions/WellKnownMimeTypes.md)
