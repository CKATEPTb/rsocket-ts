# rsocket-ts

TypeScript packages for RSocket frames, protocol primitives, clients, servers,
and browser recovery.

## Choose a package

| Package | Install | Use it for |
| --- | --- | --- |
| [`rsocket-browser`](./rsocket-browser/README.md) | `npm install rsocket-browser` | Browser-first WebSocket or WebTransport applications with reconnect, Resume, lifecycle events, logging, and typed controllers |
| [`rsocket-client-ts`](./rsocket-client-ts/README.md) | `npm install rsocket-client-ts` | One requester API for WebSocket, WebTransport, or Node TCP, with reconnect, Resume, logging, events, metadata, and controllers |
| [`rsocket-server-ts`](./rsocket-server-ts/README.md) | `npm install rsocket-server-ts` | RSocket responders over WebSocket, accepted WebTransport sessions, or Node TCP |
| [`rsocket-core-ts`](./rsocket-core-ts/README.md) | `npm install rsocket-core-ts` | Building requester, responder, or transport implementations |
| [`rsocket-frames-ts`](./rsocket-frames-ts/README.md) | `npm install rsocket-frames-ts` | Encoding and decoding frames, MIME types, metadata, and TCP frame prefixes |

Start with `rsocket-browser` for a browser application. Use `rsocket-client-ts`
when the application must select WebSocket, WebTransport, or Node TCP directly.
The browser package uses the same client facade and adds browser-specific
availability and wake signals. Use `rsocket-server-ts` to accept those clients
in a TypeScript backend.

## Browser example

```ts
import {RSocket} from "rsocket-browser";
import {WellKnownMimeType} from "rsocket-frames-ts";

const route = (name: string) =>
  WellKnownMimeType.MESSAGE_RSOCKET_ROUTING.toMetadata([name]);

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

const connected = await socket.connect().block();
if (!connected) throw new Error("RSocket connection did not open");

const response = await socket
  .requestResponse({id: 42}, route("account.find"))
  .block();

console.log(response?.data);
connected.disconnect();
```

See each package README for its complete public API and focused examples.

## Package layers

```text
rsocket-browser   -> rsocket-client-ts + rsocket-core-ts
rsocket-client-ts -> rsocket-core-ts + rsocket-frames-ts
rsocket-server-ts -> rsocket-core-ts + rsocket-frames-ts
rsocket-core-ts   -> rsocket-frames-ts -> bebyte
```

Higher layers install their lower-level dependencies automatically. Importing a
low-level package directly is only necessary when its API is used in application
code.

`rsocket-core-ts` owns every role-independent algorithm: payload encoding,
fragmentation/reassembly, demand arithmetic, routing, Resume replay storage,
lifetime timers, transport subscriptions, TCP packet boundaries, ordered
WebSocket message decoding, and the multiplexed WebTransport mapping.
`rsocket-client-ts` owns requester state, all connecting transports, reconnect,
Resume coordination, metadata, logging, and client controllers.
`rsocket-server-ts` owns responder state, listeners, and server controllers.
Neither endpoint package has a runtime dependency on the other.

## Development

```bash
npm install
npm test
npm run build
```

The root commands run all workspaces in dependency order:

```text
rsocket-frames-ts -> rsocket-core-ts -> rsocket-client-ts -> rsocket-server-ts -> rsocket-browser
```

Packages are ESM-only and licensed under MIT.

## Release

Only a push to `production` starts the release workflow. It runs the complete
test and build suite, compares each changed package's reachable public
TypeScript API with its published snapshot, and selects a SemVer increment:

- breaking API change: major;
- additive API change: minor;
- implementation or documentation change: patch.

Affected dependants receive a patch release. Packages are then published with
npm provenance in dependency order, and the workflow waits for each version to
become available before publishing the next package. The repository needs an
`NPM_TOKEN` Actions secret with publish access.
