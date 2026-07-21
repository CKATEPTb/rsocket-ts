import type {RSocketAcceptedWebSocket} from "rsocket-server-ts";

type Listener = (...args: any[]) => void;

/** Already-open in-process WebSocket used between browser and server adapters. */
export class ServerTestWebSocket implements RSocketAcceptedWebSocket {
  /** Binary mode selected by the browser transport. */
  binaryType: BinaryType = "blob";
  /** WHATWG OPEN/CLOSED state. */
  readyState = 1;
  /** Stable copies of every binary message sent by this endpoint. */
  readonly sent: Uint8Array[] = [];
  private readonly listeners = new Map<string, Set<Listener>>();
  private peer: ServerTestWebSocket | undefined;

  /** Links this endpoint to the opposite side of the test connection. */
  link(peer: ServerTestWebSocket): void {
    this.peer = peer;
  }

  /** Copies and asynchronously delivers one binary WebSocket message. */
  send(data: BufferSource): void {
    if (this.readyState !== 1 || this.peer?.readyState !== 1) {
      throw new Error("Test WebSocket is closed");
    }
    const bytes = copyBufferSource(data);
    this.sent.push(bytes);
    const peer = this.peer;
    queueMicrotask(() => {
      if (peer?.readyState === 1) peer.emit("message", {data: bytes});
    });
  }

  /** Performs a normal bidirectional WebSocket close handshake. */
  close(code = 1000, reason = ""): void {
    this.finishClose(code, reason);
    this.peer?.finishClose(code, reason);
  }

  /** Simulates an abnormal physical transport loss without a close handshake. */
  drop(reason = "network lost"): void {
    this.finishClose(1006, reason);
    this.peer?.finishClose(1006, reason);
  }

  /** Registers a WHATWG-style event listener. */
  addEventListener(type: string, listener: Listener): void {
    const listeners = this.listeners.get(type) ?? new Set<Listener>();
    listeners.add(listener);
    this.listeners.set(type, listeners);
  }

  /** Removes a WHATWG-style event listener. */
  removeEventListener(type: string, listener: Listener): void {
    this.listeners.get(type)?.delete(listener);
  }

  /** Emits one event to a stable listener snapshot. */
  private emit(type: string, event: unknown): void {
    for (const listener of [...this.listeners.get(type) ?? []]) listener(event);
  }

  /** Closes one endpoint and reports the native close event once. */
  private finishClose(code: number, reason: string): void {
    if (this.readyState >= 2) return;
    this.readyState = 3;
    queueMicrotask(() => this.emit("close", {code, reason}));
  }
}

/** Creates linked client and accepted-server WebSocket endpoints. */
export function serverWebSocketPair(): {
  readonly client: ServerTestWebSocket;
  readonly server: ServerTestWebSocket;
} {
  const client = new ServerTestWebSocket();
  const server = new ServerTestWebSocket();
  client.link(server);
  server.link(client);
  return {client, server};
}

/** Copies a WHATWG binary send argument into stable standalone bytes. */
function copyBufferSource(data: BufferSource): Uint8Array {
  return data instanceof ArrayBuffer
    ? new Uint8Array(data.slice(0))
    : new Uint8Array(data.buffer.slice(data.byteOffset, data.byteOffset + data.byteLength));
}
