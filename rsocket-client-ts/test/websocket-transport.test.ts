/**
 * WebSocket transport tests for binary message ordering and open timeouts.
 */
import { describe, expect, it } from "vitest";
import { WellKnownMimeType } from "rsocket-frames-ts";
import {RSocketConnectionError, webSocketFrameFlux} from "rsocket-core-ts";
import {RSocket} from "@/rsocket/socket.js";
import {RSocketClient} from "@/client/session.js";
import {
  createReactiveWebSocketConnection,
  createWebSocketTransport,
  ReactiveWebSocketConnection
} from "@/websocket/connection.js";
import {WS_CLOSED} from "@/websocket/constants.js";
import {normalizeWebSocketEndpoint} from "@/websocket/spec.js";
import type { RSocketWebSocketFactory } from "@/websocket/types.js";
import { FakeWebSocket, fakeWebSocketFactory } from "./websocket-fake.js";

const dataMimeType = WellKnownMimeType.APPLICATION_JSON;
const metadataMimeType = WellKnownMimeType.MESSAGE_RSOCKET_COMPOSITE_METADATA;

describe("WebSocket transport", () => {
  it("does not create a WebSocket before the connect Mono is subscribed", async () => {
    const socket = new FakeWebSocket();
    let factoryCalls = 0;
    const connector = new RSocket({
      transport: {
        type: "websocket",
        url: "ws://localhost/rsocket",
        factory: () => {
          factoryCalls += 1;
          return socket;
        }
      },
      setup: {mimetype: {data: dataMimeType, metadata: metadataMimeType}}
    });

    const connection = connector.connect();
    expect(factoryCalls).toBe(0);

    const ready = connection.block();
    expect(factoryCalls).toBe(1);
    socket.open();
    const connected = await ready;
    expect(connected).toBeDefined();
    connected?.disconnect();
  });

  it("emits asynchronously decoded WebSocket messages in arrival order", async () => {
    const socket = new FakeWebSocket();
    const received: number[] = [];

    webSocketFrameFlux(socket).subscribe((bytes) => {
      received.push(bytes[0] ?? -1);
    });

    socket.dispatchRawMessage(new DelayedBlob([new Uint8Array([1])], 20));
    socket.dispatchRawMessage(new DelayedBlob([new Uint8Array([2])], 0));

    await waitFor(() => received.length === 2);
    expect(received).toEqual([1, 2]);
  });

  it("does not retain a pooled buffer around a WebSocket message view", async () => {
    const socket = new FakeWebSocket();
    const pooled = new Uint8Array(1024);
    const view = pooled.subarray(100, 102);
    view.set([1, 2]);
    let received: Uint8Array | undefined;
    webSocketFrameFlux(socket).subscribe((bytes) => {
      received = bytes;
    });

    socket.dispatchRawMessage(view);
    await waitFor(() => received !== undefined);

    expect(received).toEqual(new Uint8Array([1, 2]));
    expect(received?.buffer).not.toBe(pooled.buffer);
  });

  it("closes a still-connecting WebSocket when the open timeout expires", async () => {
    const socket = new FakeWebSocket();

    await expect(
      RSocketClient.connect({
        transport: createWebSocketTransport({
          url: "ws://localhost/rsocket",
          webSocketFactory: fakeWebSocketFactory(socket)
        }),
        connectTimeoutMs: 1,
        setup: {dataMimeType, metadataMimeType}
      })
    ).rejects.toThrow("timed out");

    expect(socket.readyState).toBe(WS_CLOSED);
  });

  it("preserves the original open failure when cleanup close throws", async () => {
    const socket = new CloseThrowingWebSocket();

    await expect(
      RSocketClient.connect({
        transport: createWebSocketTransport({
          url: "ws://localhost/rsocket",
          webSocketFactory: fakeWebSocketFactory(socket)
        }),
        connectTimeoutMs: 1,
        setup: {dataMimeType, metadataMimeType}
      })
    ).rejects.toThrow("timed out");
  });

  it("settles opened when a custom transport throws during listener cleanup", async () => {
    const socket = new RemoveListenerThrowingWebSocket();
    const connection = new ReactiveWebSocketConnection(socket, undefined);
    const opened = connection.opened.block();

    expect(() => socket.open()).not.toThrow();
    await expect(opened).resolves.toBeUndefined();
  });

  it("normalizes http and https URLs before passing them to custom factories", () => {
    let receivedUrl: string | URL | undefined;
    let receivedProtocols: string | string[] | undefined;
    const factory: RSocketWebSocketFactory = (url, protocols) => {
      receivedUrl = url;
      receivedProtocols = protocols;
      return new FakeWebSocket();
    };

    createTestConnection(
      factory,
      "https://example.com/rsocket",
      ["v1.rsocket", "v2.rsocket"],
      undefined
    );

    expect(receivedUrl).toBe("wss://example.com/rsocket");
    expect(receivedProtocols).toEqual(["v1.rsocket", "v2.rsocket"]);
  });

  it("closes a socket that fails during connection wrapper initialization", () => {
    const socket = new FakeWebSocket();
    const broken = new Proxy(socket, {
      set(target, property, value) {
        if (property === "binaryType") throw new Error("binary mode rejected");
        return Reflect.set(target, property, value);
      }
    });

    expect(() => createTestConnection(
      () => broken,
      "ws://example.com/rsocket",
      undefined,
      undefined
    )).toThrow(RSocketConnectionError);
    expect(socket.readyState).toBe(WS_CLOSED);
  });

  it("snapshots validated WebSocket subprotocol arrays", () => {
    let receivedProtocols: string | string[] | undefined;
    const protocols = ["v1.rsocket"];
    const endpoint = createTestConnection(
      (_url, nextProtocols) => {
        receivedProtocols = nextProtocols;
        return new FakeWebSocket();
      },
      "ws://example.com/rsocket",
      protocols,
      undefined
    );

    protocols.push("mutated-after-validation");
    endpoint.close();

    expect(receivedProtocols).toEqual(["v1.rsocket"]);
  });

  it("rejects WebSocket URLs that browser WebSocket constructors reject", () => {
    expect(() =>
      createTestConnection(
        fakeWebSocketFactory(new FakeWebSocket()),
        "ftp://example.com/rsocket",
        undefined,
        undefined
      )
    ).toThrow("Invalid WebSocket URL scheme");

    expect(() =>
      createTestConnection(
        fakeWebSocketFactory(new FakeWebSocket()),
        "ws://example.com/rsocket#fragment",
        undefined,
        undefined
      )
    ).toThrow("Fragments are not allowed");

    expect(() =>
      createTestConnection(
        fakeWebSocketFactory(new FakeWebSocket()),
        "ws://example.com/rsocket#",
        undefined,
        undefined
      )
    ).toThrow("Fragments are not allowed");

    expect(() =>
      createTestConnection(
        fakeWebSocketFactory(new FakeWebSocket()),
        "wss://user:secret@example.com/rsocket",
        undefined,
        undefined
      )
    ).toThrow("Credentials are not allowed");
  });

  it("rejects invalid or duplicate WebSocket subprotocol values", () => {
    expect(() =>
      createTestConnection(
        fakeWebSocketFactory(new FakeWebSocket()),
        "ws://example.com/rsocket",
        "bad protocol",
        undefined
      )
    ).toThrow("Invalid WebSocket protocol");

    expect(() =>
      createTestConnection(
        fakeWebSocketFactory(new FakeWebSocket()),
        "ws://example.com/rsocket",
        ["rsocket", "rsocket"],
        undefined
      )
    ).toThrow("Duplicate WebSocket protocol");

    expect(() =>
      createTestConnection(
        fakeWebSocketFactory(new FakeWebSocket()),
        "ws://example.com/rsocket",
        ["rsocket", 42] as any,
        undefined
      )
    ).toThrow("Invalid WebSocket protocol");
  });

  it("validates close codes and safely bounds oversized close reasons", () => {
    expect(() => new ReactiveWebSocketConnection(new FakeWebSocket(), undefined).close({code: 1002})).toThrow(
      "Invalid WebSocket close code"
    );
    expect(() => new ReactiveWebSocketConnection(new FakeWebSocket(), undefined).close({code: 2999})).toThrow(
      "Invalid WebSocket close code"
    );
    const oversizedSocket = new FakeWebSocket();
    expect(() => new ReactiveWebSocketConnection(oversizedSocket, undefined).close({
      code: 3000,
      reason: `${"x".repeat(120)}😀tail`
    })).not.toThrow();
    expect(new TextEncoder().encode(oversizedSocket.closeReason ?? "").byteLength).toBeLessThanOrEqual(123);

    const socket = new FakeWebSocket();
    const connection = new ReactiveWebSocketConnection(socket, undefined);

    expect(() => connection.close({code: 3000, reason: "application close"})).not.toThrow();
    expect(socket.readyState).toBe(WS_CLOSED);
  });

  it("does not fail close when the socket becomes closed during close", () => {
    const connection = new ReactiveWebSocketConnection(new AlreadyClosedDuringCloseWebSocket(), undefined);

    expect(() => connection.close({code: 3000, reason: "application close"})).not.toThrow();
  });
});

/**
 * Blob test double that resolves `arrayBuffer()` after a configurable delay.
 */
class DelayedBlob extends Blob {
  /**
   * Creates a delayed Blob from the provided parts.
   */
  constructor(
    parts: BlobPart[],
    private readonly delayMs: number
  ) {
    super(parts);
  }

  /**
   * Delays before returning the real Blob bytes.
   */
  override async arrayBuffer(): Promise<ArrayBuffer> {
    await new Promise((resolve) => setTimeout(resolve, this.delayMs));
    return super.arrayBuffer();
  }
}

/**
 * Fake socket that rejects cleanup close calls after an open failure.
 */
class CloseThrowingWebSocket extends FakeWebSocket {
  /**
   * Simulates a custom transport whose close implementation can fail.
   */
  override close(_code?: number, _reason?: string): void {
    throw new Error("close failed");
  }
}

/**
 * Fake socket whose listener cleanup operation is faulty.
 */
class RemoveListenerThrowingWebSocket extends FakeWebSocket {
  /**
   * Simulates cleanup failure after a transport event was already delivered.
   */
  override removeEventListener(
    _type: "open" | "message" | "error" | "close",
    _listener: (event: any) => void
  ): void {
    throw new Error("removeEventListener failed");
  }
}

/**
 * Fake socket that reports CLOSED before throwing from close.
 */
class AlreadyClosedDuringCloseWebSocket extends FakeWebSocket {
  /**
   * Simulates a custom transport that closes concurrently with the wrapper call.
   */
  override close(_code?: number, _reason?: string): void {
    this.readyState = WS_CLOSED;
    throw new Error("already closed");
  }
}

/**
 * Polls until an asynchronous test condition becomes true.
 */
async function waitFor(predicate: () => boolean): Promise<void> {
  for (let attempt = 0; attempt < 20; attempt += 1) {
    if (predicate()) return;
    await new Promise((resolve) => setTimeout(resolve, 5));
  }
  throw new Error("Timed out waiting for condition");
}

/** Creates a physical connection through the adapter's normalized endpoint boundary. */
function createTestConnection(
  factory: RSocketWebSocketFactory,
  url: string | URL,
  protocols: string | string[] | undefined,
  timeoutMs: number | undefined
): ReactiveWebSocketConnection {
  return createReactiveWebSocketConnection(
    factory,
    normalizeWebSocketEndpoint(url, protocols),
    timeoutMs
  );
}
