/**
 * In-memory WebSocket implementation used by protocol and reconnect tests.
 */
import type { Frame, MimeType } from "rsocket-frames-ts";
import { FrameDeserializer, FrameType, WellKnownMimeType } from "rsocket-frames-ts";
/** WHATWG WebSocket ready-state constants used by the test double. */
export const WS_CONNECTING = 0;
export const WS_OPEN = 1;
export const WS_CLOSED = 3;

/**
 * Generic DOM-style event listener used by the fake socket.
 */
type Listener = (event: any) => void;

/**
 * Minimal controllable WebSocket test double.
 *
 * The fake records binary sends, exposes helper methods for server-side events,
 * and keeps enough DOM WebSocket behavior to exercise the production transport.
 */
export class FakeWebSocket {
  /** Binary mode requested by the production connection wrapper. */
  binaryType: BinaryType = "blob";
  /** Current fake WebSocket ready state. */
  readyState = WS_CONNECTING;
  /** Last close code supplied by the client. */
  closeCode: number | undefined;
  /** Last close reason supplied by the client. */
  closeReason: string | undefined;
  /** Raw frames sent by the RSocket requester. */
  readonly sent: Uint8Array[] = [];
  /** Optional hook invoked after every successful send. */
  onSend?: (bytes: Uint8Array) => void;
  private readonly listeners = new Map<string, Set<Listener>>();

  /**
   * Records a sent message and notifies the optional send hook.
   */
  send(data: BufferSource): void {
    if (this.readyState !== WS_OPEN) throw new Error("FakeWebSocket is not open");
    const bytes = copyBufferSource(data);
    this.sent.push(bytes);
    this.onSend?.(bytes);
  }

  /**
   * Closes the fake socket and dispatches a close event.
   */
  close(code?: number, reason?: string): void {
    if (this.readyState === WS_CLOSED) return;
    this.closeCode = code;
    this.closeReason = reason;
    this.readyState = WS_CLOSED;
    this.dispatch("close", { code, reason });
  }

  /**
   * Registers an event listener for a WebSocket event type.
   */
  addEventListener(type: "open" | "message" | "error" | "close", listener: Listener): void {
    let listeners = this.listeners.get(type);
    if (!listeners) {
      listeners = new Set();
      this.listeners.set(type, listeners);
    }
    listeners.add(listener);
  }

  /**
   * Removes a previously registered event listener.
   */
  removeEventListener(type: "open" | "message" | "error" | "close", listener: Listener): void {
    this.listeners.get(type)?.delete(listener);
  }

  /**
   * Returns the current listener count for transport cleanup assertions.
   */
  listenerCount(type: "open" | "message" | "error" | "close"): number {
    return this.listeners.get(type)?.size ?? 0;
  }

  /**
   * Moves the fake socket to OPEN and dispatches the open event.
   */
  open(): void {
    this.readyState = WS_OPEN;
    this.dispatch("open", {});
  }

  /**
   * Serializes an RSocket frame and dispatches it as a server message.
   */
  serverSend(frame: Frame): void {
    const bytes = frame.toUint8Array();
    this.dispatchMessage(bytes);
  }

  /**
   * Dispatches raw binary bytes as a WebSocket message.
   */
  dispatchMessage(bytes: Uint8Array): void {
    this.dispatch("message", { data: bytes.buffer.slice(bytes.byteOffset, bytes.byteOffset + bytes.byteLength) });
  }

  /**
   * Dispatches an arbitrary message payload for transport edge-case tests.
   */
  dispatchRawMessage(data: unknown): void {
    this.dispatch("message", { data });
  }

  /**
   * Decodes one recorded outbound RSocket frame for assertions.
   */
  decodeSent(index: number, metadataMimeType: MimeType<any>, dataMimeType: MimeType<any>): Frame {
    const bytes = this.sent[index];
    if (!bytes) throw new Error(`No sent frame at index ${index}`);
    if (bytes.byteLength < 6) throw new RangeError("An RSocket frame header requires six bytes");
    const frameType = FrameType.fromByte((bytes[4]! << 8 | bytes[5]!) >>> 10);
    const payloadMimeType =
      frameType === FrameType.KEEPALIVE
        ? WellKnownMimeType.APPLICATION_OCTET_STREAM
        : frameType === FrameType.ERROR
          ? WellKnownMimeType.TEXT_PLAIN
          : dataMimeType;
    return FrameDeserializer.deserialize(bytes, metadataMimeType, payloadMimeType);
  }

  /**
   * Dispatches one fake DOM event to registered listeners.
   */
  private dispatch(type: string, event: any): void {
    for (const listener of this.listeners.get(type) ?? []) listener(event);
  }
}

/** Copies a WHATWG binary send argument into stable test bytes. */
function copyBufferSource(data: BufferSource): Uint8Array {
  return data instanceof ArrayBuffer
    ? new Uint8Array(data.slice(0))
    : new Uint8Array(data.buffer.slice(data.byteOffset, data.byteOffset + data.byteLength));
}

/**
 * Creates a WebSocket factory that always returns the provided fake socket.
 */
export function fakeWebSocketFactory(socket: FakeWebSocket) {
  return () => socket;
}

