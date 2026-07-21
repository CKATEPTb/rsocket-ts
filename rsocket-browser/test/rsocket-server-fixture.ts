import {WellKnownMimeType} from "rsocket-frames-ts";
import {RSocketServer, type RSocketServerOptions} from "rsocket-server-ts";
import {RSocket} from "@";
import type {RSocketOptions} from "@/rsocket/options.js";
import {serverWebSocketPair, type ServerTestWebSocket} from "./server-websocket.js";

/** One physical in-process WebSocket accepted by the production server adapter. */
export interface BrowserServerSocketPair {
  /** Browser requester endpoint. */
  readonly client: ServerTestWebSocket;
  /** Server responder endpoint. */
  readonly server: ServerTestWebSocket;
}

/** Publicly nameable state returned by the browser/server test fixture. */
export interface RSocketServerFixture {
  /** Browser requester under test. */
  readonly socket: RSocket;
  /** Request API used after the initial connection succeeds. */
  readonly connection: RSocket;
  /** TypeScript responder under test. */
  readonly server: RSocketServer;
  /** Physical WebSocket pairs opened by SETUP and reconnect attempts. */
  readonly pairs: BrowserServerSocketPair[];
  /** Server handshake operations in physical connection order. */
  readonly accepts: Promise<unknown>[];
  /** Unexpected server handshake failures. */
  readonly acceptFailures: unknown[];
  /** Disconnects both endpoints and settles all handshakes. */
  close(): Promise<void>;
}

/** Opens the public browser requester against the public TypeScript responder. */
export async function openRSocketServerFixture(
  serverOptions: RSocketServerOptions,
  clientOptions: RSocketOptions = {}
): Promise<RSocketServerFixture> {
  const server = new RSocketServer(serverOptions);
  const pairs: BrowserServerSocketPair[] = [];
  const accepts: Promise<unknown>[] = [];
  const acceptFailures: unknown[] = [];
  const {setup, ...options} = clientOptions;
  const transport = (): ServerTestWebSocket => {
    const pair = serverWebSocketPair();
    pairs.push(pair);
    const accepted = server.acceptWebSocket(pair.server).block();
    accepts.push(accepted);
    void accepted.catch((error) => {
      acceptFailures.push(error);
    });
    return pair.client;
  };
  const socket = new RSocket("ws://rsocket.test", {
    reconnect: false,
    ...options,
    setup: {
      keepAlive: 60_000,
      lifetime: 60_000,
      mimetype: {
        data: WellKnownMimeType.APPLICATION_JSON,
        metadata: WellKnownMimeType.MESSAGE_RSOCKET_COMPOSITE_METADATA
      },
      ...setup,
      transport
    }
  });

  try {
    const connection = await socket.connect().block();
    if (connection === undefined) throw new Error("Browser RSocket did not connect");
    const initialAccept = accepts[0];
    if (initialAccept === undefined) throw new Error("RSocket server did not receive the transport");
    await initialAccept;
    let closed = false;
    return {
      socket,
      connection: socket,
      server,
      pairs,
      accepts,
      acceptFailures,
      /** Disconnects both endpoints and settles every started server accept operation. */
      async close(): Promise<void> {
        if (closed) return;
        closed = true;
        connection.disconnect();
        await server.close().block();
        await Promise.allSettled(accepts);
      }
    };
  } catch (error) {
    await server.close().block();
    throw error;
  }
}
