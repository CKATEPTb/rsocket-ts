/**
 * End-to-end WebSocket requester tests for the public browser `RSocket`
 * facade and protocol behavior.
 */
import { describe, expect, it, vi } from "vitest";
import { Flux, Mono, type Subscription } from "reactor-core-ts";
import {
  CancelFrame,
  ErrorFrame,
  ExtensionFlag,
  ExtensionFrame,
  FrameErrorCode,
  FrameFlag,
  Header,
  KeepaliveFlag,
  KeepaliveFrame,
  LeaseFrame,
  Metadata,
  MimeType,
  MetadataPushFrame,
  Payload,
  PayloadFlag,
  PayloadFrame,
  RequestChannelFrame,
  RequestChannelFlag,
  RequestFireAndForgetFrame,
  RequestNFrame,
  RequestResponseFrame,
  RequestStreamFrame,
  ResumeFrame,
  ResumeOkFrame,
  SetupFrame,
  WellKnownAuthType,
  WellKnownMimeType,
  type Frame
} from "rsocket-frames-ts";
import {
  FireAndForgetController,
  RequestChannelController,
  RequestResponseController,
  RequestStreamController,
  RSocket
} from "@";
import { browserReconnectSignals } from "@/reconnect/index.js";
import { browserClientOptions } from "@/rsocket/options.js";
import {
  emitOutboundFrameFragments,
  errorMessage,
  outboundFrameLength
} from "rsocket-core-ts";
import { installBrowserSignals } from "./browser-signals.js";
import { FakeWebSocket, fakeWebSocketFactory, WS_CLOSED } from "./fake-websocket.js";

const dataMimeType = WellKnownMimeType.APPLICATION_JSON;
const metadataMimeType = WellKnownMimeType.MESSAGE_RSOCKET_COMPOSITE_METADATA;

/** Reusable typed request-response controller for facade and logging tests. */
class UserLookupController extends RequestResponseController<{ id: number }, { name: string }> {
  /** Route consumed by the user lookup responder. */
  protected readonly route = "user.find";
}

/** Request-response controller with a configurable protocol timeout for tests. */
class TimedRequestController extends RequestResponseController<Record<string, unknown>, unknown> {
  /** Route used only to exercise controller-level request options. */
  protected readonly route = "test.timeout";

  /** Creates a timed controller instance. */
  constructor(timeout: number) {
    super({ timeout });
  }
}

/**
 * Creates nested SETUP options used by public socket constructor tests.
 */
function setupOptions(
  keepAlive = 20_000,
  lifetime = 90_000,
  transport?: (url: string | URL, protocols?: string | string[]) => FakeWebSocket,
  setupMetadataMimeType: MimeType<any> = metadataMimeType
) {
  const setup = {
    keepAlive,
    lifetime,
    mimetype: {
      data: dataMimeType,
      metadata: setupMetadataMimeType
    }
  };
  return transport === undefined ? setup : { ...setup, transport };
}

describe("RSocket", () => {
  it("rejects browser online waits when the caller already aborted", async () => {
    const browser = installBrowserSignals(true);
    const abort = new AbortController();

    try {
      abort.abort();
      await expect(browserReconnectSignals.waitUntilAvailable(abort.signal)).rejects.toMatchObject({
        name: "AbortError"
      });
    } finally {
      browser.restore();
    }
  });

  it("does not wait for an online event before the initial connection attempt", async () => {
    const browser = installBrowserSignals(false);
    const sockets: FakeWebSocket[] = [];

    try {
      const client = new RSocket("ws://localhost/rsocket", {
        setup: setupOptions(20_000, 90_000, () => {
          const socket = new FakeWebSocket();
          sockets.push(socket);
          return socket;
        }),
        reconnect: false
      });
      const ready = client.connect().block();

      expect(sockets).toHaveLength(1);
      sockets[0]?.close(1006, "offline");
      await expect(ready).rejects.toThrow("closed before it opened");
    } finally {
      browser.restore();
    }
  });

  it("sends SETUP as a raw WebSocket binary message", async () => {
    const { socket } = await connect();

    expect(socket.sent).toHaveLength(1);
    const setup = socket.decodeSent(0, metadataMimeType, dataMimeType);

    expect(setup).toBeInstanceOf(SetupFrame);
    expect(setup.header.streamId).toBe(0);
    expect((setup as SetupFrame).dataType.mimeType).toBe("application/json");
    expect((setup as SetupFrame).metadataType.mimeType).toBe("message/x.rsocket.composite-metadata.v0");
  });

  it("rejects invalid SETUP timing values before opening a WebSocket", async () => {
    const sockets: FakeWebSocket[] = [];
    const client = new RSocket("ws://localhost/rsocket", {
      setup: setupOptions(0, 90_000, () => {
        const socket = new FakeWebSocket();
        sockets.push(socket);
        return socket;
      })
    });

    await expect(client.connect().block()).rejects.toThrow("setup.keepAlive");
    expect(sockets).toHaveLength(0);
  });

  it("rejects SETUP timing values that round outside the 31-bit wire field", async () => {
    const subMillisecond = new RSocket("ws://localhost/rsocket", {
      setup: setupOptions(0.5, 90_000, () => new FakeWebSocket())
    });
    const overflowing = new RSocket("ws://localhost/rsocket", {
      setup: setupOptions(0x80000000, 90_000, () => new FakeWebSocket())
    });

    await expect(subMillisecond.connect().block()).rejects.toThrow("setup.keepAlive");
    await expect(overflowing.connect().block()).rejects.toThrow("setup.keepAlive");
  });

  it("does not report connected when the WebSocket closes during SETUP send", async () => {
    const socket = new FakeWebSocket();
    const lifecycle: string[] = [];
    socket.onSend = () => socket.close(1006, "closed during setup");
    const client = new RSocket("ws://localhost/rsocket", {
      setup: setupOptions(20_000, 90_000, fakeWebSocketFactory(socket)),
      reconnect: false,
      events: {
        event: (event) => lifecycle.push(event.type)
      }
    });

    const ready = client.connect().block();
    socket.open();

    await expect(ready).rejects.toThrow("Transport closed");
    expect(lifecycle).not.toContain("connected");
  });

  it("rejects invalid WebSocket URLs without entering reconnect", async () => {
    const client = new RSocket("ftp://localhost/rsocket");

    await expect(client.connect().block()).rejects.toThrow("Invalid WebSocket URL scheme");
  });

  it("accepts a URL object created by another browser realm", () => {
    const foreignUrl = {
      href: "ws://localhost/rsocket",
      toString: () => "ws://localhost/rsocket",
      [Symbol.toStringTag]: "URL"
    } as unknown as URL;

    expect(browserClientOptions(foreignUrl, {reconnect: false}).transport)
      .toEqual({type: "websocket", url: foreignUrl});
  });

  it("performs request-response with decoded JSON payloads", async () => {
    const { client, socket } = await connect();

    const response = client.requestResponse({ hello: "world" }).block();
    expect(socket.sent).toHaveLength(2);

    const request = socket.decodeSent(1, metadataMimeType, dataMimeType);
    expect(request.header.streamId).toBe(1);
    expect(decodedPayload(request)).toEqual({ hello: "world" });

    socket.serverSend(
      new PayloadFrame(
        request.header.streamId,
        PayloadFlag.combine(PayloadFlag.NEXT, PayloadFlag.COMPLETE),
        undefined,
        dataMimeType.toPayload({ ok: true })
      )
    );

    await expect(response).resolves.toMatchObject({ data: { ok: true } });
  });

  it("encodes a raw null value as JSON data instead of dropping the payload", async () => {
    const { client, socket } = await connect();
    const response = client.requestResponse(null).block();
    const request = socket.decodeSent(1, metadataMimeType, dataMimeType) as RequestResponseFrame;

    expect(decodedPayload(request)).toBeNull();
    socket.serverSend(
      new PayloadFrame(
        request.header.streamId,
        PayloadFlag.combine(PayloadFlag.NEXT, PayloadFlag.COMPLETE),
        undefined,
        dataMimeType.toPayload({ ok: true })
      )
    );
    await expect(response).resolves.toMatchObject({ data: { ok: true } });
  });

  it("keeps application payloads with controller-like kind fields as data", async () => {
    const { client, socket } = await connect();
    const data = { kind: "requestResponse", id: 7 } as const;
    const response = client.requestResponse(data).block();
    const request = socket.decodeSent(1, metadataMimeType, dataMimeType) as RequestResponseFrame;

    expect(decodedPayload(request)).toEqual(data);
    socket.serverSend(
      new PayloadFrame(
        request.header.streamId,
        PayloadFlag.combine(PayloadFlag.NEXT, PayloadFlag.COMPLETE),
        undefined,
        dataMimeType.toPayload({ ok: true })
      )
    );
    await expect(response).resolves.toMatchObject({ data: { ok: true } });
  });

  it("assumes request-response COMPLETE when the responder omits it", async () => {
    const { client, socket } = await connect();
    const response = client.requestResponse({ splitTerminal: true }).block();
    const request = socket.decodeSent(1, metadataMimeType, dataMimeType) as RequestResponseFrame;

    socket.serverSend(
      new PayloadFrame(
        request.header.streamId,
        PayloadFlag.NEXT,
        undefined,
        dataMimeType.toPayload({ ok: true })
      )
    );
    await expect(response).resolves.toMatchObject({ data: { ok: true } });
  });

  it("completes request-response empty when COMPLETE has no NEXT payload", async () => {
    const { client, socket } = await connect();
    const response = client.requestResponse({ empty: true }).block();
    const request = socket.decodeSent(1, metadataMimeType, dataMimeType) as RequestResponseFrame;

    socket.serverSend(new PayloadFrame(request.header.streamId, PayloadFlag.COMPLETE));

    await expect(response).resolves.toBeUndefined();
  });

  it("ignores later payloads after an implicit request-response completion", async () => {
    const { client, socket } = await connect({ reconnect: false });
    const response = client.requestResponse({ duplicate: true }).block();
    const request = socket.decodeSent(1, metadataMimeType, dataMimeType) as RequestResponseFrame;

    socket.serverSend(
      new PayloadFrame(
        request.header.streamId,
        PayloadFlag.NEXT,
        undefined,
        dataMimeType.toPayload({ value: 1 })
      )
    );
    await expect(response).resolves.toMatchObject({ data: { value: 1 } });

    socket.serverSend(
      new PayloadFrame(
        request.header.streamId,
        PayloadFlag.combine(PayloadFlag.NEXT, PayloadFlag.COMPLETE),
        undefined,
        dataMimeType.toPayload({ value: 2 })
      )
    );
    await flush();

    expect(socket.readyState).not.toBe(WS_CLOSED);
    expect(socket.sent).toHaveLength(2);
  });

  it("encodes a small request payload once while checking frame size", async () => {
    const { client, socket } = await connect();
    const payloadEncoding = vi.spyOn(dataMimeType, "toPayload");

    try {
      const response = client.requestResponse({ hello: "cache" }).block();
      expect(payloadEncoding).toHaveBeenCalledTimes(1);
      const request = socket.decodeSent(1, metadataMimeType, dataMimeType) as RequestResponseFrame;

      expect(request).toBeInstanceOf(RequestResponseFrame);

      socket.serverSend(
        new PayloadFrame(
          request.header.streamId,
          PayloadFlag.combine(PayloadFlag.NEXT, PayloadFlag.COMPLETE),
          undefined,
          dataMimeType.toPayload({ ok: true })
        )
      );

      await expect(response).resolves.toMatchObject({ data: { ok: true } });
    } finally {
      payloadEncoding.mockRestore();
    }
  });

  it("settles an online wait even when browser listener cleanup throws", async () => {
    const browser = installBrowserSignals(false);
    const originalRemove = globalThis.removeEventListener;
    let cleanupAttempts = 0;

    try {
      const available = browserReconnectSignals.waitUntilAvailable();
      (globalThis as any).removeEventListener = () => {
        cleanupAttempts += 1;
        throw new Error("listener cleanup failed");
      };
      browser.setOnline(true);
      browser.dispatch("online");

      await expect(available).resolves.toBeUndefined();
      expect(cleanupAttempts).toBe(3);
    } finally {
      (globalThis as any).removeEventListener = originalRemove;
      browser.restore();
    }
  });

  it("removes wake listeners when availability changes during registration", async () => {
    const browser = installBrowserSignals(false);
    const originalAdd = globalThis.addEventListener;
    const originalRemove = globalThis.removeEventListener;
    let removals = 0;

    try {
      (globalThis as any).addEventListener = (type: string, listener: EventListener): void => {
        originalAdd(type, listener);
        browser.setOnline(true);
        listener(new Event(type));
      };
      (globalThis as any).removeEventListener = (type: string, listener: EventListener): void => {
        removals += 1;
        originalRemove(type, listener);
      };

      await expect(browserReconnectSignals.waitUntilAvailable()).resolves.toBeUndefined();
      expect(removals).toBe(3);
    } finally {
      browser.restore();
    }
  });

  it("removes partially registered browser listeners when registration fails", () => {
    const browser = installBrowserSignals(false);
    const registeredAdd = globalThis.addEventListener;
    const registeredRemove = globalThis.removeEventListener;
    const listener = vi.fn();
    let registrations = 0;
    let removals = 0;

    try {
      (globalThis as any).addEventListener = (type: string, callback: EventListener): void => {
        registeredAdd(type, callback);
        registrations += 1;
        if (registrations === 2) throw new Error("listener registration failed");
      };
      (globalThis as any).removeEventListener = (type: string, callback: EventListener): void => {
        removals += 1;
        registeredRemove(type, callback);
      };

      expect(() => browserReconnectSignals.onWake(listener)).toThrow("listener registration failed");
      browser.dispatch("online");
      browser.dispatch("pageshow");

      expect(removals).toBe(2);
      expect(listener).not.toHaveBeenCalled();
    } finally {
      browser.restore();
    }
  });

  it("shares one native wake-listener set across logical sockets", () => {
    const browser = installBrowserSignals(true);
    const registeredAdd = globalThis.addEventListener;
    const registeredRemove = globalThis.removeEventListener;
    const first = vi.fn();
    const second = vi.fn();
    let registrations = 0;
    let removals = 0;
    let releaseFirst: (() => void) | undefined;
    let releaseSecond: (() => void) | undefined;

    try {
      (globalThis as any).addEventListener = (type: string, callback: EventListener): void => {
        registrations += 1;
        registeredAdd(type, callback);
      };
      (globalThis as any).removeEventListener = (type: string, callback: EventListener): void => {
        removals += 1;
        registeredRemove(type, callback);
      };

      releaseFirst = browserReconnectSignals.onWake(first);
      releaseSecond = browserReconnectSignals.onWake(second);
      expect(registrations).toBe(3);

      browser.dispatch("focus");
      expect(first).toHaveBeenCalledTimes(1);
      expect(second).toHaveBeenCalledTimes(1);

      releaseFirst();
      expect(removals).toBe(0);
      browser.dispatch("pageshow");
      expect(first).toHaveBeenCalledTimes(1);
      expect(second).toHaveBeenCalledTimes(2);

      releaseSecond();
      releaseSecond();
      expect(removals).toBe(3);

      registrations = 0;
      removals = 0;
      const duplicate = vi.fn();
      releaseFirst = browserReconnectSignals.onWake(duplicate);
      releaseSecond = browserReconnectSignals.onWake(duplicate);
      browser.dispatch("online");
      expect(duplicate).toHaveBeenCalledTimes(2);
      releaseFirst();
      browser.dispatch("online");
      expect(duplicate).toHaveBeenCalledTimes(3);
      releaseSecond();
      expect(registrations).toBe(3);
      expect(removals).toBe(3);
    } finally {
      releaseFirst?.();
      releaseSecond?.();
      browser.restore();
    }
  });

  it("parses each inbound frame header only once", async () => {
    const { client, socket } = await connect();
    const response = client.requestResponse({ singleHeaderParse: true }).block();
    const request = socket.decodeSent(1, metadataMimeType, dataMimeType) as RequestResponseFrame;
    const header = vi.spyOn(Header, "from");

    try {
      socket.serverSend(
        new PayloadFrame(
          request.header.streamId,
          PayloadFlag.combine(PayloadFlag.NEXT, PayloadFlag.COMPLETE),
          undefined,
          dataMimeType.toPayload({ ok: true })
        )
      );

      await response;
      expect(header).toHaveBeenCalledTimes(1);
    } finally {
      header.mockRestore();
    }
  });

  it("serializes caller-provided payload and metadata objects once", async () => {
    const { client, socket } = await connect();
    const bytes = new Uint8Array([10, 20, 30]);
    const rawPayload = WellKnownMimeType.APPLICATION_OCTET_STREAM.toPayload(bytes);
    const rawMetadata = WellKnownMimeType.TEXT_PLAIN.toMetadata("caller-metadata");
    const payloadBytes = vi.spyOn(rawPayload, "toUint8Array");
    const metadataBytes = vi.spyOn(rawMetadata, "toUint8Array");

    try {
      const payloadResponse = client.requestResponse(rawPayload).block();
      const payloadRequest = socket.decodeSent(
        1,
        metadataMimeType,
        WellKnownMimeType.APPLICATION_OCTET_STREAM
      ) as RequestResponseFrame;

      expect(payloadBytesOf((payloadRequest as any).payload)).toEqual(bytes);
      expect(payloadBytes).toHaveBeenCalledTimes(1);
      socket.serverSend(
        new PayloadFrame(
          payloadRequest.header.streamId,
          PayloadFlag.combine(PayloadFlag.NEXT, PayloadFlag.COMPLETE),
          undefined,
          dataMimeType.toPayload({ ok: true })
        )
      );
      await expect(payloadResponse).resolves.toMatchObject({ data: { ok: true } });

      const metadataResponse = client
        .requestResponse(undefined, rawMetadata, { metadata: WellKnownMimeType.TEXT_PLAIN })
        .block();
      const metadataRequest = socket.decodeSent(
        2,
        metadataMimeType,
        dataMimeType
      ) as RequestResponseFrame;

      expect(compositePayloads((metadataRequest as any).metadata)).toEqual(["caller-metadata"]);
      expect(metadataBytes).toHaveBeenCalledTimes(1);
      socket.serverSend(
        new PayloadFrame(
          metadataRequest.header.streamId,
          PayloadFlag.combine(PayloadFlag.NEXT, PayloadFlag.COMPLETE),
          undefined,
          dataMimeType.toPayload({ ok: true })
        )
      );
      await expect(metadataResponse).resolves.toMatchObject({ data: { ok: true } });
    } finally {
      payloadBytes.mockRestore();
      metadataBytes.mockRestore();
    }
  });

  it("rejects request-response when the responder sends an ERROR frame", async () => {
    const { client, socket } = await connect();

    const response = client.requestResponse({ fail: true }).block();
    const request = socket.decodeSent(1, metadataMimeType, dataMimeType) as RequestResponseFrame;

    socket.serverSend(
      new ErrorFrame(
        request.header.streamId,
        FrameErrorCode.APPLICATION_ERROR,
        WellKnownMimeType.TEXT_PLAIN.toPayload("boom")
      )
    );

    await expect(response).rejects.toThrow("boom");
  });

  it("formats non-json error values as readable strings", () => {
    expect(errorMessage(undefined)).toBe("undefined");
    expect(errorMessage(Symbol.for("rsocket-test"))).toBe("Symbol(rsocket-test)");
  });

  it("treats a flagless request-response PAYLOAD as implicit empty completion", async () => {
    const { client, socket } = await connect({ reconnect: false });

    const response = client.requestResponse({ invalid: true }).block();
    const request = socket.decodeSent(1, metadataMimeType, dataMimeType) as RequestResponseFrame;

    socket.serverSend(new PayloadFrame(request.header.streamId, 0));
    await expect(response).resolves.toBeUndefined();
    expect(socket.readyState).not.toBe(WS_CLOSED);
  });

  it("ignores an unknown PAYLOAD on stream zero", async () => {
    const { socket } = await connect({ reconnect: false });

    socket.serverSend(
      new PayloadFrame(
        0,
        PayloadFlag.combine(PayloadFlag.NEXT, PayloadFlag.COMPLETE),
        undefined,
        dataMimeType.toPayload({ invalid: true })
      )
    );
    await flush();

    expect(socket.readyState).not.toBe(WS_CLOSED);
    expect(socket.sent).toHaveLength(1);
  });

  it("ignores a server request that reuses an active client stream ID", async () => {
    const { client, socket } = await connect({ reconnect: false });
    const response = client.requestResponse({ active: true }).block();
    const request = socket.decodeSent(1, metadataMimeType, dataMimeType) as RequestResponseFrame;

    socket.serverSend(
      new RequestResponseFrame(
        request.header.streamId,
        FrameFlag.NONE,
        undefined,
        dataMimeType.toPayload({ invalidServerRequest: true })
      )
    );
    await flush();

    expect(socket.sent).toHaveLength(2);
    socket.serverSend(
      new PayloadFrame(
        request.header.streamId,
        PayloadFlag.combine(PayloadFlag.NEXT, PayloadFlag.COMPLETE),
        undefined,
        dataMimeType.toPayload({ ok: true })
      )
    );

    await expect(response).resolves.toMatchObject({ data: { ok: true } });
    expect(socket.readyState).not.toBe(WS_CLOSED);
  });

  it("ignores REQUEST_N for request-response", async () => {
    const { client, socket } = await connect({ reconnect: false });

    const response = client.requestResponse({ invalid: "request-n" }).block();
    const request = socket.decodeSent(1, metadataMimeType, dataMimeType) as RequestResponseFrame;

    socket.serverSend(new RequestNFrame(request.header.streamId, 1));
    await flush();
    socket.serverSend(
      new PayloadFrame(
        request.header.streamId,
        PayloadFlag.combine(PayloadFlag.NEXT, PayloadFlag.COMPLETE),
        undefined,
        dataMimeType.toPayload({ ok: true })
      )
    );

    await expect(response).resolves.toMatchObject({ data: { ok: true } });
    expect(socket.readyState).not.toBe(WS_CLOSED);
  });

  it("keeps JSON objects with payload-like keys as application data", async () => {
    const { client, socket } = await connect();
    const response = client.requestResponse({ model: "codec-shaped" }).block();
    const request = socket.decodeSent(1, metadataMimeType, dataMimeType) as RequestResponseFrame;
    const data = { payload: { id: 42 }, mimeType: "domain/chat" };

    socket.serverSend(
      new PayloadFrame(
        request.header.streamId,
        PayloadFlag.combine(PayloadFlag.NEXT, PayloadFlag.COMPLETE),
        undefined,
        dataMimeType.toPayload(data)
      )
    );

    await expect(response).resolves.toMatchObject({ data });
  });

  it("times out request-response and cancels the stream", async () => {
    const { client, socket } = await connect();

    const response = client
      .process(new TimedRequestController(5), { slow: true })
      .block();
    const request = socket.decodeSent(1, metadataMimeType, dataMimeType) as RequestResponseFrame;

    await expect(response).rejects.toThrow("timed out after 5ms");
    const cancel = socket.decodeSent(2, metadataMimeType, dataMimeType) as CancelFrame;
    expect(cancel).toBeInstanceOf(CancelFrame);
    expect(cancel.header.streamId).toBe(request.header.streamId);
  });

  it("clears request-response timeout when the response arrives during send", async () => {
    vi.useFakeTimers();
    try {
      const { client, socket } = await connect();
      let responded = false;

      socket.onSend = () => {
        const frame = socket.decodeSent(socket.sent.length - 1, metadataMimeType, dataMimeType);
        if (responded || !(frame instanceof RequestResponseFrame)) return;
        responded = true;
        socket.serverSend(
          new PayloadFrame(
            frame.header.streamId,
            PayloadFlag.combine(PayloadFlag.NEXT, PayloadFlag.COMPLETE),
            undefined,
            dataMimeType.toPayload({ ok: true })
          )
        );
      };

      const response = client
        .process(new TimedRequestController(5), { immediate: true })
        .block();

      await expect(response).resolves.toMatchObject({ ok: true });
      await vi.advanceTimersByTimeAsync(5);

      const sentFrames = socket.sent.map((_bytes, index) => socket.decodeSent(index, metadataMimeType, dataMimeType));
      expect(sentFrames.some((frame) => frame instanceof CancelFrame)).toBe(false);
    } finally {
      vi.useRealTimers();
    }
  });

  it("allocates odd client stream IDs sequentially across interaction models", async () => {
    const { client, socket } = await connect();

    const response = client.requestResponse({ interaction: "rr" }).block();
    const requestResponse = socket.decodeSent(1, metadataMimeType, dataMimeType) as RequestResponseFrame;
    socket.serverSend(
      new PayloadFrame(
        requestResponse.header.streamId,
        PayloadFlag.combine(PayloadFlag.NEXT, PayloadFlag.COMPLETE),
        undefined,
        dataMimeType.toPayload({ ok: true })
      )
    );
    await response;

    await client.fireAndForget({ interaction: "fnf" }).block();

    let streamSubscription: Subscription | undefined;
    client.requestStream({ interaction: "stream" }).subscribe({
      onSubscribe(subscription) {
        streamSubscription = subscription;
      },
      onNext() {},
      onError(error) {
        throw error;
      },
      onComplete() {}
    });
    streamSubscription?.request(1);

    let channelSubscription: Subscription | undefined;
    client.requestChannel(Flux.fromArray([{ data: { interaction: "channel" } }])).subscribe({
      onSubscribe(subscription) {
        channelSubscription = subscription;
      },
      onNext() {},
      onError(error) {
        throw error;
      },
      onComplete() {}
    });
    channelSubscription?.request(1);
    await waitFor(() => socket.sent.length >= 5);

    expect((socket.decodeSent(1, metadataMimeType, dataMimeType) as RequestResponseFrame).header.streamId).toBe(1);
    expect((socket.decodeSent(2, metadataMimeType, dataMimeType) as RequestFireAndForgetFrame).header.streamId).toBe(3);
    expect((socket.decodeSent(3, metadataMimeType, dataMimeType) as RequestStreamFrame).header.streamId).toBe(5);
    expect((socket.decodeSent(4, metadataMimeType, dataMimeType) as RequestChannelFrame).header.streamId).toBe(7);
  });

  it("does not consume a stream ID when a stream is cancelled before demand", async () => {
    const { client, socket } = await connect();
    let subscription: Subscription | undefined;

    client.requestStream({ route: "never-started" }).subscribe({
      onSubscribe(nextSubscription) {
        subscription = nextSubscription;
      },
      onNext() {},
      onError(error) {
        throw error;
      },
      onComplete() {}
    });
    subscription?.cancel();

    await client.fireAndForget({ firstWireInteraction: true }).block();
    const request = socket.decodeSent(1, metadataMimeType, dataMimeType) as RequestFireAndForgetFrame;

    expect(request).toBeInstanceOf(RequestFireAndForgetFrame);
    expect(request.header.streamId).toBe(1);
  });

  it("waits for a connection when a request starts before connect", async () => {
    const socket = new FakeWebSocket();
    const client = new RSocket("ws://localhost/rsocket", {
      setup: setupOptions(20_000, 90_000, fakeWebSocketFactory(socket))
    });

    const response = client.requestResponse({ hello: "queued" }).block();
    expect(socket.sent).toHaveLength(0);

    socket.open();
    await waitFor(() => socket.sent.length === 2);

    const request = socket.decodeSent(1, metadataMimeType, dataMimeType) as RequestResponseFrame;
    expect(decodedPayload(request)).toEqual({ hello: "queued" });

    socket.serverSend(
      new PayloadFrame(
        request.header.streamId,
        PayloadFlag.combine(PayloadFlag.NEXT, PayloadFlag.COMPLETE),
        undefined,
        dataMimeType.toPayload({ ok: true })
      )
    );

    await expect(response).resolves.toMatchObject({ data: { ok: true } });
  });

  it("does not send a queued Mono request after its subscription is cancelled", async () => {
    const socket = new FakeWebSocket();
    const client = new RSocket("ws://localhost/rsocket", {
      setup: setupOptions(20_000, 90_000, fakeWebSocketFactory(socket))
    });
    let subscription: Subscription | undefined;

    client.requestResponse({ cancelled: true }).subscribe({
      onSubscribe(nextSubscription) {
        subscription = nextSubscription;
      },
      onNext() {},
      onError() {},
      onComplete() {}
    });
    subscription?.request(1);
    subscription?.cancel();
    socket.open();
    await waitFor(() => socket.sent.length === 1);
    await flush();

    expect(socket.decodeSent(0, metadataMimeType, dataMimeType)).toBeInstanceOf(SetupFrame);
    expect(socket.sent).toHaveLength(1);
  });

  it("releases a queued Flux request when it is cancelled before the WebSocket opens", async () => {
    const socket = new FakeWebSocket();
    const client = new RSocket("ws://localhost/rsocket", {
      setup: setupOptions(20_000, 90_000, fakeWebSocketFactory(socket))
    });
    let subscription: Subscription | undefined;

    client.requestStream({ cancelled: true }).subscribe({
      onSubscribe(nextSubscription) {
        subscription = nextSubscription;
      },
      onNext() {},
      onError() {},
      onComplete() {}
    });
    subscription?.request(1);
    subscription?.cancel();
    socket.open();
    await waitFor(() => socket.sent.length === 1);
    await flush();

    expect(socket.decodeSent(0, metadataMimeType, dataMimeType)).toBeInstanceOf(SetupFrame);
    expect(socket.sent).toHaveLength(1);
  });

  it("merges metadataUpdate entries into outgoing composite metadata", async () => {
    const { client, socket } = await connect();
    const authMetadata = WellKnownMimeType.TEXT_PLAIN.toMetadata("Bearer test-token");

    const snapshot = client.metadataUpdate(authMetadata);
    expect(snapshot.get(WellKnownMimeType.TEXT_PLAIN)).toBe(authMetadata);

    const response = client.requestResponse({ hello: "metadata" }).block();
    const request = socket.decodeSent(1, metadataMimeType, dataMimeType) as RequestResponseFrame;
    const metadata = (request as any).metadata;

    expect(metadata.mimeType).toBe(WellKnownMimeType.MESSAGE_RSOCKET_COMPOSITE_METADATA);
    expect(compositePayloads(metadata)).toEqual(["Bearer test-token"]);

    socket.serverSend(
      new PayloadFrame(
        request.header.streamId,
        PayloadFlag.combine(PayloadFlag.NEXT, PayloadFlag.COMPLETE),
        undefined,
        dataMimeType.toPayload({ ok: true })
      )
    );

    await expect(response).resolves.toMatchObject({ data: { ok: true } });
    expect(client.metadataUpdate((metadata) => {
      metadata.remove(WellKnownMimeType.TEXT_PLAIN);
    }).has(WellKnownMimeType.TEXT_PLAIN)).toBe(false);
  });

  it("adds and removes Bearer authentication through the metadata editor", async () => {
    const { client, socket } = await connect();
    const authenticationMimeType = WellKnownMimeType.MESSAGE_RSOCKET_AUTHENTICATION;
    const token = "editor-bearer-token";

    const authenticated = client.metadataUpdate((metadata) => {
      expect(metadata.size).toBe(0);
      metadata.set(authenticationMimeType, WellKnownAuthType.BEARER.auth(token));
      expect(metadata.has(authenticationMimeType)).toBe(true);
    });
    expect(authenticated.has(authenticationMimeType)).toBe(true);

    await client.fireAndForget({ authenticated: true }).block();
    const authenticatedRequest = socket.decodeSent(1, metadataMimeType, dataMimeType) as RequestFireAndForgetFrame;
    const composite = (authenticatedRequest as unknown as { metadata: Metadata<Metadata<any>[]> }).metadata;
    const authentication = composite.payload.find(
      (entry) => entry.mimeType.mimeType === authenticationMimeType.mimeType
    );
    expect(authentication?.payload).toMatchObject({ data: token });

    const anonymous = client.metadataUpdate((metadata) => {
      expect(metadata.get(authenticationMimeType)).toBeDefined();
      metadata.remove(authenticationMimeType);
    });
    expect(anonymous.has(authenticationMimeType)).toBe(false);

    await client.fireAndForget({ authenticated: false }).block();
    const anonymousRequest = socket.decodeSent(2, metadataMimeType, dataMimeType) as RequestFireAndForgetFrame;
    expect((anonymousRequest as any).metadata).toBeUndefined();
  });

  it("preserves unrelated defaults and overrides matching MIME metadata per interaction", async () => {
    const { client, socket } = await connect();
    const routingMimeType = WellKnownMimeType.MESSAGE_RSOCKET_ROUTING;
    const authenticationMimeType = WellKnownMimeType.MESSAGE_RSOCKET_AUTHENTICATION;
    const token = "default-authentication";
    client.metadataUpdate((metadata) => {
      metadata.set(routingMimeType, ["default.route"]);
      metadata.set(authenticationMimeType, WellKnownAuthType.BEARER.auth(token));
    });

    await client.fireAndForget({ interaction: true }, routingMimeType.toMetadata(["interaction.route"])).block();
    const request = socket.decodeSent(1, metadataMimeType, dataMimeType) as RequestFireAndForgetFrame;
    const requestEntries = (request as unknown as { metadata: Metadata<Metadata<any>[]> }).metadata.payload;
    const requestRoutes = requestEntries.filter((entry) => entry.mimeType.mimeType === routingMimeType.mimeType);
    expect(requestRoutes).toHaveLength(1);
    expect(requestRoutes[0]?.payload).toEqual(["interaction.route"]);
    expect(requestEntries.find((entry) => entry.mimeType.mimeType === authenticationMimeType.mimeType)?.payload)
      .toMatchObject({ data: token });

    await client.metadataPush(routingMimeType.toMetadata(["push.route"])).block();
    const push = socket.decodeSent(2, metadataMimeType, dataMimeType) as MetadataPushFrame;
    const pushEntries = (push as unknown as { metadata: Metadata<Metadata<any>[]> }).metadata.payload;
    const pushRoutes = pushEntries.filter((entry) => entry.mimeType.mimeType === routingMimeType.mimeType);
    expect(pushRoutes).toHaveLength(1);
    expect(pushRoutes[0]?.payload).toEqual(["push.route"]);
    expect(pushEntries.find((entry) => entry.mimeType.mimeType === authenticationMimeType.mimeType)?.payload)
      .toMatchObject({ data: token });
  });

  it("accepts MimeType-keyed metadataUpdate tuples and updater snapshots", async () => {
    const { client } = await connect();
    const authMetadata = WellKnownMimeType.TEXT_PLAIN.toMetadata("Bearer tuple-token");
    const tenantMetadata = WellKnownMimeType.APPLICATION_JSON.toMetadata({ tenant: "acme" });

    const snapshot = client.metadataUpdate([
      authMetadata,
      [WellKnownMimeType.APPLICATION_JSON, tenantMetadata] as const
    ]);

    expect(snapshot.get(WellKnownMimeType.TEXT_PLAIN)).toBe(authMetadata);
    expect(snapshot.get(WellKnownMimeType.APPLICATION_JSON)).toBe(tenantMetadata);
    expect(
      client.metadataUpdate((current) => {
        expect(current.get(WellKnownMimeType.TEXT_PLAIN)).toBe(authMetadata);
        current.remove(WellKnownMimeType.TEXT_PLAIN);
      }).has(WellKnownMimeType.TEXT_PLAIN)
    ).toBe(false);
  });

  it("rejects metadataUpdate entries whose Metadata MIME does not match the map key", async () => {
    const { client } = await connect();
    const json = WellKnownMimeType.APPLICATION_JSON.toMetadata({ tenant: "acme" });

    expect(() => client.metadataUpdate(new Map([
      [WellKnownMimeType.TEXT_PLAIN, json]
    ]))).toThrow("must match its MimeType map key");
  });

  it("applies iterable metadataUpdate patches transactionally", async () => {
    const { client } = await connect();
    const auth = WellKnownMimeType.TEXT_PLAIN.toMetadata("Bearer stable-token");
    client.metadataUpdate(auth);

    expect(() => client.metadataUpdate([
      [WellKnownMimeType.APPLICATION_JSON, { tenant: "must-not-commit" }],
      ["invalid-mime", "invalid-value"] as never
    ])).toThrow("iterable entries");

    client.metadataUpdate((current) => {
      expect(current.get(WellKnownMimeType.TEXT_PLAIN)).toBe(auth);
      expect(current.has(WellKnownMimeType.APPLICATION_JSON)).toBe(false);
      return [];
    });
  });

  it("encodes null metadataPush values with the selected codec inside negotiated composite metadata", async () => {
    const { client, socket } = await connect();

    await client.metadataPush(null, {
      metadataMimeType: WellKnownMimeType.APPLICATION_JSON
    }).block();

    const frame = socket.decodeSent(
      1,
      metadataMimeType,
      dataMimeType
    ) as MetadataPushFrame;
    expect(frame).toBeInstanceOf(MetadataPushFrame);
    expect(compositePayloads((frame as any).metadata)).toEqual([null]);
  });

  it("wraps a per-interaction metadata codec in the MIME negotiated by SETUP", async () => {
    const { client, socket } = await connect();

    await client.fireAndForget(
      { id: 1 },
      ["users.find"],
      { metadata: WellKnownMimeType.MESSAGE_RSOCKET_ROUTING }
    ).block();

    const request = socket.decodeSent(1, metadataMimeType, dataMimeType) as RequestFireAndForgetFrame;
    const entries = (request as unknown as { metadata: Metadata<Metadata<any>[]> }).metadata.payload;
    expect(entries).toHaveLength(1);
    expect(entries[0]?.mimeType).toBe(WellKnownMimeType.MESSAGE_RSOCKET_ROUTING);
    expect(entries[0]?.payload).toEqual(["users.find"]);
  });

  it("rejects string-keyed metadataUpdate object patches", async () => {
    const { client } = await connect();

    expect(() => client.metadataUpdate({ "text/plain": "Bearer string-key" } as never)).toThrow(
      /Metadata|MimeType/
    );
  });

  it("preserves data MIME overrides while request metadata replaces the matching client default", async () => {
    const { client, socket } = await connect();
    const payloadBytes = new Uint8Array([7, 8, 9]);
    client.metadataUpdate(WellKnownMimeType.TEXT_PLAIN.toMetadata("Bearer envelope-token"));

    const response = client.requestResponse(
      payloadBytes,
      "request-metadata",
      {
        data: WellKnownMimeType.APPLICATION_OCTET_STREAM,
        metadata: WellKnownMimeType.TEXT_PLAIN
      }
    ).block();
    const request = socket.decodeSent(
      1,
      metadataMimeType,
      WellKnownMimeType.APPLICATION_OCTET_STREAM
    ) as RequestResponseFrame;

    expect(payloadBytesOf((request as any).payload)).toEqual(payloadBytes);
    expect((request as any).metadata.mimeType).toBe(WellKnownMimeType.MESSAGE_RSOCKET_COMPOSITE_METADATA);
    expect(compositePayloads((request as any).metadata)).toEqual(["request-metadata"]);

    socket.serverSend(
      new PayloadFrame(
        request.header.streamId,
        PayloadFlag.combine(PayloadFlag.NEXT, PayloadFlag.COMPLETE),
        undefined,
        dataMimeType.toPayload({ ok: true })
      )
    );

    await expect(response).resolves.toMatchObject({ data: { ok: true } });
  });

  it("keeps metadataUpdate request-channel iterable input on the synchronous first-frame path", async () => {
    const { client, socket } = await connect();
    client.metadataUpdate(WellKnownMimeType.TEXT_PLAIN.toMetadata("Bearer channel-token"));
    let subscription: Subscription | undefined;
    let nextCalls = 0;
    const values = [{ data: { n: 1 } }, { data: { n: 2 } }];
    const input: Iterable<{ data: { n: number } }> = {
      [Symbol.iterator]() {
        return {
          next() {
            nextCalls += 1;
            const value = values.shift();
            return value === undefined
              ? { done: true, value: undefined as never }
              : { done: false, value };
          }
        };
      }
    };

    client.requestChannel(input).subscribe({
      onSubscribe(nextSubscription) {
        subscription = nextSubscription;
      },
      onNext() {},
      onError(error) {
        throw error;
      },
      onComplete() {}
    });

    subscription?.request(1);

    expect(socket.sent).toHaveLength(2);
    expect(nextCalls).toBe(1);

    const request = socket.decodeSent(1, metadataMimeType, dataMimeType) as RequestChannelFrame;
    expect(request).toBeInstanceOf(RequestChannelFrame);
    expect(decodedPayload(request)).toEqual({ n: 1 });
    expect(compositePayloads((request as any).metadata)).toEqual(["Bearer channel-token"]);
  });

  it("binds a deferred Mono request to the current client at subscription time", async () => {
    const sockets: FakeWebSocket[] = [];
    const client = new RSocket("ws://localhost/rsocket", {
      setup: setupOptions(20_000, 90_000, () => {
        const socket = new FakeWebSocket();
        sockets.push(socket);
        return socket;
      }),
      reconnect: true
    });
    const ready = client.connect().block();
    sockets[0]?.open();
    await ready;

    const mono = client.requestResponse({ after: "reconnect" });
    sockets[0]?.close(1006, "network lost");
    await waitFor(() => sockets.length === 2);
    sockets[1]?.open();
    await client.connect().block();

    const response = mono.block();
    await waitFor(() => (sockets[1]?.sent.length ?? 0) > 1);

    const request = sockets[1]?.decodeSent(1, metadataMimeType, dataMimeType) as RequestResponseFrame;
    expect(decodedPayload(request)).toEqual({ after: "reconnect" });

    sockets[1]?.serverSend(
      new PayloadFrame(
        request.header.streamId,
        PayloadFlag.combine(PayloadFlag.NEXT, PayloadFlag.COMPLETE),
        undefined,
        dataMimeType.toPayload({ ok: true })
      )
    );

    await expect(response).resolves.toMatchObject({ data: { ok: true } });
  });

  it("binds a deferred Flux request to the current client at subscription time", async () => {
    const sockets: FakeWebSocket[] = [];
    const client = new RSocket("ws://localhost/rsocket", {
      setup: setupOptions(20_000, 90_000, () => {
        const socket = new FakeWebSocket();
        sockets.push(socket);
        return socket;
      }),
      reconnect: true
    });
    const ready = client.connect().block();
    sockets[0]?.open();
    await ready;

    const stream = client.requestStream({ after: "reconnect-stream" });
    sockets[0]?.close(1006, "network lost");
    await waitFor(() => sockets.length === 2);
    sockets[1]?.open();
    await client.connect().block();

    let subscription: Subscription | undefined;
    const errors: unknown[] = [];
    stream.subscribe({
      onSubscribe(nextSubscription) {
        subscription = nextSubscription;
      },
      onNext() {},
      onError(error) {
        errors.push(error);
      },
      onComplete() {}
    });
    subscription?.request(1);
    await waitFor(() => (sockets[1]?.sent.length ?? 0) > 1);

    const request = sockets[1]?.decodeSent(1, metadataMimeType, dataMimeType) as RequestStreamFrame;
    expect(request).toBeInstanceOf(RequestStreamFrame);
    expect(decodedPayload(request)).toEqual({ after: "reconnect-stream" });
    expect(errors).toEqual([]);
  });

  it("uses one Reactor subscription layer for streams on an active client", async () => {
    const { client } = await connect();
    const prototype = Object.getPrototypeOf(client.requestStream());
    const subscribe = vi.spyOn(prototype, "subscribe");
    let subscription: Subscription | undefined;

    try {
      client.requestStream({ route: "direct-subscription" }).subscribe({
        onSubscribe(nextSubscription) {
          subscription = nextSubscription;
        },
        onNext() {},
        onError(error) {
          throw error;
        },
        onComplete() {}
      });

      expect(subscribe).toHaveBeenCalledTimes(1);
    } finally {
      subscription?.cancel();
      subscribe.mockRestore();
    }
  });

  it("aggregates deferred Flux demand before the WebSocket opens", async () => {
    const socket = new FakeWebSocket();
    const client = new RSocket("ws://localhost/rsocket", {
      setup: setupOptions(20_000, 90_000, fakeWebSocketFactory(socket))
    });
    const ready = client.connect().block();
    let subscription: Subscription | undefined;

    client.requestStream({ route: "queued-stream" }).subscribe({
      onSubscribe(nextSubscription) {
        subscription = nextSubscription;
      },
      onNext() {},
      onError(error) {
        throw error;
      },
      onComplete() {}
    });

    subscription?.request(1);
    subscription?.request(2);
    socket.open();
    await ready;
    await waitFor(() => socket.sent.length === 2);

    const request = socket.decodeSent(1, metadataMimeType, dataMimeType) as RequestStreamFrame;
    expect(request).toBeInstanceOf(RequestStreamFrame);
    expect(request.request).toBe(3);
    expect(decodedPayload(request)).toEqual({ route: "queued-stream" });
  });

  it("logs socket frame activity when enabled", async () => {
    const logs: Array<Record<string, unknown>> = [];
    const { client, socket } = await connect({
      log: {
        payload: true,
        logger: (event: any) => logs.push(event)
      }
    });

    expect(logs).toContainEqual(expect.objectContaining({
      type: "frame",
      direction: "send",
      frameType: "SETUP",
      streamId: 0,
      frame: expect.any(SetupFrame)
    }));

    logs.length = 0;
    const response = client.requestResponse({ hello: "logged" }).block();
    const request = socket.decodeSent(1, metadataMimeType, dataMimeType);

    socket.serverSend(
      new PayloadFrame(
        request.header.streamId,
        PayloadFlag.combine(PayloadFlag.NEXT, PayloadFlag.COMPLETE),
        undefined,
        dataMimeType.toPayload({ ok: true })
      )
    );

    await expect(response).resolves.toMatchObject({ data: { ok: true } });
    expect(logs).toContainEqual(expect.objectContaining({
      type: "frame",
      direction: "send",
      frameType: "REQUEST_RESPONSE"
    }));
    expect(logs).toContainEqual(expect.objectContaining({
      type: "frame",
      direction: "receive",
      frameType: "PAYLOAD"
    }));
  });

  it("omits raw frame payloads from socket logs by default", async () => {
    const logs: Array<Record<string, unknown>> = [];
    await connect({
      log: (event: any) => logs.push(event)
    });

    expect(logs).toContainEqual(expect.objectContaining({
      type: "frame",
      direction: "send",
      frameType: "SETUP"
    }));
    expect(logs.find((event) => event.type === "frame")).not.toHaveProperty("frame");
  });

  it("does not emit frame logs when only lifecycle logging is enabled", async () => {
    const logs: Array<Record<string, unknown>> = [];
    const { client, socket } = await connect({
      log: {
        frames: false,
        lifecycle: true,
        logger: (event: any) => logs.push(event)
      }
    });

    logs.length = 0;
    const response = client.requestResponse({ hello: "quiet-frame" }).block();
    const request = socket.decodeSent(1, metadataMimeType, dataMimeType);
    socket.serverSend(
      new PayloadFrame(
        request.header.streamId,
        PayloadFlag.combine(PayloadFlag.NEXT, PayloadFlag.COMPLETE),
        undefined,
        dataMimeType.toPayload({ ok: true })
      )
    );

    await expect(response).resolves.toMatchObject({ data: { ok: true } });
    expect(logs.filter((event) => event.type === "frame")).toHaveLength(0);
  });

  it("keeps constructor logging disabled when enabled is false", async () => {
    const logs: Array<Record<string, unknown>> = [];

    await connect({
      log: {
        enabled: false,
        logger: (event: any) => logs.push(event)
      }
    });

    expect(logs).toHaveLength(0);
  });

  it("processes declarative class controllers with inferred arguments and results", async () => {
    /** Fire-and-forget controller used to verify process return inference. */
    class PublishEventController extends FireAndForgetController<{ name: string }> {
      /** Route consumed by the event responder. */
      protected readonly route = "event.opened";
    }
    /** Request-stream controller used to verify process return inference. */
    class StreamNumbersController extends RequestStreamController<{ limit: number }, { n: number }> {
      /** Route consumed by the number responder. */
      protected readonly route = "numbers";
    }
    /** Request-channel controller used to verify process return inference. */
    class ChatController extends RequestChannelController<
      { room: string; text: string },
      { accepted: boolean }
    > {
      /** Route consumed by the chat responder. */
      protected readonly route = "chat";
    }

    const { client, connected, socket } = await connect();

    const responseMono: Mono<{ name: string }> = connected.process(UserLookupController, { id: 7 });
    const eventMono: Mono<void> = client.process(PublishEventController, { name: "menu" });
    const numberFlux: Flux<{ n: number }> = client.process(StreamNumbersController, { limit: 3 });
    const chatFlux: Flux<{ accepted: boolean }> = client.process(
      ChatController,
      Flux.fromArray([{ data: { room: "general", text: "hello" } }])
    );

    expect(eventMono).toBeInstanceOf(Mono);
    expect(numberFlux).toBeInstanceOf(Flux);
    expect(chatFlux).toBeInstanceOf(Flux);

    const response = responseMono.block();
    const request = socket.decodeSent(1, metadataMimeType, dataMimeType) as RequestResponseFrame;

    expect(request).toBeInstanceOf(RequestResponseFrame);
    expect(decodedPayload(request)).toEqual({ id: 7 });

    socket.serverSend(
      new PayloadFrame(
        request.header.streamId,
        PayloadFlag.combine(PayloadFlag.NEXT, PayloadFlag.COMPLETE),
        undefined,
        dataMimeType.toPayload({ name: "Ada" })
      )
    );

    await expect(response).resolves.toEqual({ name: "Ada" });
  });

  it("keeps controller request-stream initial demand batched with logging enabled", async () => {
    /** Controller whose decoded Flux must preserve the first request amount. */
    class BatchedStreamController extends RequestStreamController<void, number> {
      /** Route consumed by the fake responder. */
      protected readonly route = "numbers.batched";
    }

    const events: Array<Record<string, unknown>> = [];
    const controller = new BatchedStreamController().log({
      interactions: true,
      logger: (event) => events.push(event as unknown as Record<string, unknown>)
    });
    const { client, socket } = await connect();
    const values: number[] = [];
    let subscription: Subscription | undefined;

    client.process(controller).subscribe({
      onSubscribe(nextSubscription) {
        subscription = nextSubscription;
      },
      onNext(value) {
        values.push(value);
      },
      onError(error) {
        throw error;
      },
      onComplete() {}
    });
    subscription?.request(30);

    const request = socket.decodeSent(1, metadataMimeType, dataMimeType) as RequestStreamFrame;
    expect(request).toBeInstanceOf(RequestStreamFrame);
    expect(request.request).toBe(30);
    expect(events).toEqual([
      expect.objectContaining({interaction: "requestStream", stage: "send"})
    ]);

    socket.serverSend(
      new PayloadFrame(
        request.header.streamId,
        PayloadFlag.NEXT,
        undefined,
        dataMimeType.toPayload(1)
      )
    );
    await flush();

    expect(values).toEqual([1]);
    expect(socket.sent).toHaveLength(2);

    subscription?.request(7);
    const requestN = socket.decodeSent(2, metadataMimeType, dataMimeType) as RequestNFrame;
    expect(requestN).toBeInstanceOf(RequestNFrame);
    expect(requestN.request).toBe(7);

    subscription?.cancel();
  });

  it("keeps controller request-channel response demand batched", async () => {
    /** Controller whose decoded response Flux must preserve request batching. */
    class BatchedChannelController extends RequestChannelController<number, number> {
      /** Route consumed by the fake responder. */
      protected readonly route = "numbers.channel.batched";
    }

    const { client, socket } = await connect();
    let subscription: Subscription | undefined;

    client.process(BatchedChannelController, Flux.just({data: 1})).subscribe({
      onSubscribe(nextSubscription) {
        subscription = nextSubscription;
      },
      onNext() {},
      onError(error) {
        throw error;
      },
      onComplete() {}
    });
    subscription?.request(30);
    await waitFor(() => socket.sent.length >= 2);

    const request = socket.decodeSent(1, metadataMimeType, dataMimeType) as RequestChannelFrame;
    expect(request).toBeInstanceOf(RequestChannelFrame);
    expect(request.request).toBe(30);

    subscription?.cancel();
  });

  it("rejects empty or oversized controller routing tags before sending a request", async () => {
    class EmptyRouteController extends FireAndForgetController<void> {
      /** Invalid empty routing tag used by this regression test. */
      protected readonly route = "";
    }
    class OversizedRouteController extends FireAndForgetController<void> {
      /** Two-byte UTF-8 characters make this tag exceed the one-byte routing length. */
      protected readonly route = "é".repeat(128);
    }
    const { client, socket } = await connect();

    expect(() => client.process(EmptyRouteController)).toThrow("non-empty");
    expect(() => client.process(OversizedRouteController)).toThrow("255 UTF-8 bytes");
    expect(socket.sent).toHaveLength(1);
  });

  it("processes Spring-style class controllers with typed request and response bodies", async () => {
    /**
     * Request body accepted by the password-change route.
     */
    type ChangePasswordRequest = {
      currentPassword: string;
      newPassword: string;
    };
    /**
     * Response body emitted by authentication routes.
     */
    type TokenResponse = {
      token: string;
    };
    /**
     * Example request-response controller declared as an application class.
     */
    class ChangePasswordController extends RequestResponseController<
      ChangePasswordRequest,
      TokenResponse
    > {
      /** Route consumed by the Spring RSocket backend. */
      protected readonly route = "changePassword";
    }

    const { client, socket } = await connect();
    const authenticationMimeType = WellKnownMimeType.MESSAGE_RSOCKET_AUTHENTICATION;
    client.metadataUpdate((metadata) => {
      metadata.set(authenticationMimeType, WellKnownAuthType.BEARER.auth("controller-authentication"));
    });
    const responseMono: Mono<TokenResponse> = client.process(new ChangePasswordController(), {
      currentPassword: "old",
      newPassword: "new"
    });

    const response = responseMono.block();
    const request = socket.decodeSent(1, metadataMimeType, dataMimeType) as RequestResponseFrame;

    expect(request).toBeInstanceOf(RequestResponseFrame);
    expect(decodedPayload(request)).toEqual({
      currentPassword: "old",
      newPassword: "new"
    });
    const requestMetadata = (request as unknown as { metadata: Metadata<Metadata<any>[]> }).metadata;
    const routingEntry = requestMetadata.payload.find(
      (entry) => entry.mimeType.mimeType === WellKnownMimeType.MESSAGE_RSOCKET_ROUTING.mimeType
    );
    const authenticationEntry = requestMetadata.payload.find(
      (entry) => entry.mimeType.mimeType === authenticationMimeType.mimeType
    );
    expect(requestMetadata.mimeType.mimeType).toBe(metadataMimeType.mimeType);
    expect(routingEntry?.mimeType.mimeType).toBe(WellKnownMimeType.MESSAGE_RSOCKET_ROUTING.mimeType);
    expect(routingEntry?.payload).toEqual(["changePassword"]);
    expect(authenticationEntry?.payload).toMatchObject({ data: "controller-authentication" });

    socket.serverSend(
      new PayloadFrame(
        request.header.streamId,
        PayloadFlag.combine(PayloadFlag.NEXT, PayloadFlag.COMPLETE),
        undefined,
        dataMimeType.toPayload({ token: "jwt" })
      )
    );

    await expect(response).resolves.toEqual({ token: "jwt" });
  });

  it("sends controller routes directly when the connection uses routing metadata", async () => {
    /** Sign-in controller used to verify direct routing metadata. */
    class SignInController extends RequestResponseController<
      { login: string; password: string },
      { token: string }
    > {
      /** Route consumed by the account sign-in responder. */
      protected readonly route = "account.sign-in";
    }

    const routingMimeType = WellKnownMimeType.MESSAGE_RSOCKET_ROUTING;
    const socket = new FakeWebSocket();
    const client = new RSocket("ws://localhost/rsocket", {
      setup: setupOptions(20_000, 90_000, fakeWebSocketFactory(socket), routingMimeType)
    });
    const ready = client.connect().block();
    socket.open();
    await ready;

    client.metadataUpdate((metadata) => {
      metadata.set(routingMimeType, ["account.default"]);
    });
    expect(() => client.metadataUpdate((metadata) => {
      metadata.set(
        WellKnownMimeType.MESSAGE_RSOCKET_AUTHENTICATION,
        WellKnownAuthType.BEARER.auth("unsupported")
      );
    })).toThrow(/MESSAGE_RSOCKET_COMPOSITE_METADATA/);

    await client.fireAndForget({ usesDefaultRoute: true }).block();
    const defaultRequest = socket.decodeSent(1, routingMimeType, dataMimeType) as RequestFireAndForgetFrame;
    const defaultMetadata = (defaultRequest as unknown as { metadata: Metadata<string[]> }).metadata;
    expect(defaultMetadata.payload).toEqual(["account.default"]);

    const response = client.process(SignInController, {
      login: "login@example.com",
      password: "password"
    }).block();
    const request = socket.decodeSent(2, routingMimeType, dataMimeType) as RequestResponseFrame;
    const requestMetadata = (request as unknown as { metadata: Metadata<string[]> }).metadata;

    expect(requestMetadata.mimeType.mimeType).toBe(routingMimeType.mimeType);
    expect(requestMetadata.payload).toEqual(["account.sign-in"]);

    socket.serverSend(
      new PayloadFrame(
        request.header.streamId,
        PayloadFlag.combine(PayloadFlag.NEXT, PayloadFlag.COMPLETE),
        undefined,
        dataMimeType.toPayload({ token: "jwt" })
      )
    );
    await expect(response).resolves.toEqual({ token: "jwt" });
  });

  it("accepts only authentication updates when SETUP uses direct authentication metadata", async () => {
    const authenticationMimeType = WellKnownMimeType.MESSAGE_RSOCKET_AUTHENTICATION;
    const socket = new FakeWebSocket();
    const client = new RSocket("ws://localhost/rsocket", {
      setup: setupOptions(20_000, 90_000, fakeWebSocketFactory(socket), authenticationMimeType)
    });
    const ready = client.connect().block();
    socket.open();
    await ready;

    client.metadataUpdate((metadata) => {
      metadata.set(authenticationMimeType, WellKnownAuthType.BEARER.auth("direct-token"));
    });
    expect(() => client.metadataUpdate((metadata) => {
      metadata.set(WellKnownMimeType.MESSAGE_RSOCKET_ROUTING, ["unsupported.route"]);
    })).toThrow(/MESSAGE_RSOCKET_COMPOSITE_METADATA/);
    expect(() => client.metadataUpdate((metadata) => {
      metadata.remove(WellKnownMimeType.MESSAGE_RSOCKET_ROUTING);
    })).toThrow(/MESSAGE_RSOCKET_COMPOSITE_METADATA/);
    expect(() => client.metadataUpdate(new Map([
      [WellKnownMimeType.MESSAGE_RSOCKET_ROUTING, null]
    ]))).toThrow(/MESSAGE_RSOCKET_COMPOSITE_METADATA/);
    await expect(client.metadataPush(
      ["unsupported.route"],
      { metadataMimeType: WellKnownMimeType.MESSAGE_RSOCKET_ROUTING }
    ).block()).rejects.toThrow(/cannot be sent|MESSAGE_RSOCKET_COMPOSITE_METADATA/);

    await client.fireAndForget({ authenticated: true }).block();
    const request = socket.decodeSent(1, authenticationMimeType, dataMimeType) as RequestFireAndForgetFrame;
    const requestMetadata = (request as unknown as { metadata: Metadata<{ data: string }> }).metadata;

    expect(requestMetadata.mimeType.mimeType).toBe(authenticationMimeType.mimeType);
    expect(requestMetadata.payload).toMatchObject({ data: "direct-token" });
  });

  it("reuses declarative controller class instances within one socket", async () => {
    let constructions = 0;

    /** Stateless controller declaration whose route metadata can be reused. */
    class CachedController extends FireAndForgetController<number> {
      /** Route consumed by the responder. */
      protected route = "cached.controller";

      /** Tracks materialization without changing controller behavior. */
      constructor() {
        super();
        constructions += 1;
      }
    }

    const { client } = await connect();

    await expect(client.process(CachedController, 1).block()).resolves.toBeUndefined();
    await expect(client.process(CachedController, 2).block()).resolves.toBeUndefined();

    expect(constructions).toBe(1);
  });

  it("processes class controller constructors for fire-and-forget and request-stream", async () => {
    /**
     * Request body sent when an account is deactivated.
     */
    type DeactivateAccountRequest = {
      reason: string;
    };
    /**
     * Request body that wraps a numeric id.
     */
    type IdWrapper<T> = {
      id: T;
    };
    /**
     * Stream item emitted by an online-status subscription.
     */
    type OnlineResponse = {
      accountId: number;
      lastOnlineAt: number;
      isOnline: boolean;
    };
    /**
     * Example fire-and-forget controller declared as an application class.
     */
    class DeactivateAccountController extends FireAndForgetController<DeactivateAccountRequest> {
      /** Route consumed by the Spring RSocket backend. */
      protected route = "deactivateAccount";
    }
    /**
     * Example request-stream controller declared as an application class.
     */
    class SubscribeOnlineController extends RequestStreamController<
      IdWrapper<number>,
      OnlineResponse
    > {
      /** Route consumed by the Spring RSocket backend. */
      protected route = "subscribeOnline";
    }

    const { client, socket } = await connect();
    const routingMimeType = WellKnownMimeType.MESSAGE_RSOCKET_ROUTING;
    const authenticationMimeType = WellKnownMimeType.MESSAGE_RSOCKET_AUTHENTICATION;
    client.metadataUpdate((metadata) => {
      metadata.set(routingMimeType, ["controller.default"]);
      metadata.set(authenticationMimeType, WellKnownAuthType.BEARER.auth("controller-authentication"));
    });
    const fireAndForgetMono: Mono<void> = client.process(DeactivateAccountController, {
      reason: "requested"
    });
    const streamFlux: Flux<OnlineResponse> = client.process(SubscribeOnlineController, { id: 7 });

    await expect(fireAndForgetMono.block()).resolves.toBeUndefined();
    const fireAndForget = socket.decodeSent(1, metadataMimeType, dataMimeType) as RequestFireAndForgetFrame;
    expect(fireAndForget).toBeInstanceOf(RequestFireAndForgetFrame);
    expect(decodedPayload(fireAndForget)).toEqual({ reason: "requested" });
    const fireAndForgetMetadata = (fireAndForget as unknown as { metadata: Metadata<Metadata<any>[]> }).metadata;
    expect(fireAndForgetMetadata.payload.filter((entry) => entry.mimeType.mimeType === routingMimeType.mimeType))
      .toEqual([routingMimeType.toMetadata(["deactivateAccount"])]);
    expect(fireAndForgetMetadata.payload.find((entry) => entry.mimeType.mimeType === authenticationMimeType.mimeType)?.payload)
      .toMatchObject({ data: "controller-authentication" });

    const values: OnlineResponse[] = [];
    let subscription: Subscription | undefined;
    streamFlux.subscribe({
      /** Captures subscription so the test can send demand. */
      onSubscribe(nextSubscription) {
        subscription = nextSubscription;
      },
      /** Stores typed online-status responses. */
      onNext(value) {
        values.push(value);
      },
      /** Fails the test on stream errors. */
      onError(error) {
        throw error;
      },
      /** No-op completion hook for the test subscriber. */
      onComplete() {}
    });

    subscription?.request(1);
    const stream = socket.decodeSent(2, metadataMimeType, dataMimeType) as RequestStreamFrame;
    expect(stream).toBeInstanceOf(RequestStreamFrame);
    expect(decodedPayload(stream)).toEqual({ id: 7 });
    const streamMetadata = (stream as unknown as { metadata: Metadata<Metadata<any>[]> }).metadata;
    expect(streamMetadata.payload.filter((entry) => entry.mimeType.mimeType === routingMimeType.mimeType))
      .toEqual([routingMimeType.toMetadata(["subscribeOnline"])]);
    expect(streamMetadata.payload.find((entry) => entry.mimeType.mimeType === authenticationMimeType.mimeType)?.payload)
      .toMatchObject({ data: "controller-authentication" });

    socket.serverSend(
      new PayloadFrame(
        stream.header.streamId,
        PayloadFlag.combine(PayloadFlag.NEXT, PayloadFlag.COMPLETE),
        undefined,
        dataMimeType.toPayload({
          accountId: 7,
          lastOnlineAt: 123,
          isOnline: true
        })
      )
    );
    await flush();

    expect(values).toEqual([{ accountId: 7, lastOnlineAt: 123, isOnline: true }]);
  });

  it("processes class request-channel controllers with route metadata", async () => {
    /**
     * Outbound chat message sent through a request-channel controller.
     */
    type ChatMessage = {
      text: string;
    };
    /**
     * Response item emitted by the chat channel.
     */
    type ChatAck = {
      delivered: boolean;
    };
    /**
     * Example request-channel controller declared as an application class.
     */
    class ChatController extends RequestChannelController<ChatMessage, ChatAck> {
      /** Route consumed by the Spring RSocket backend. */
      protected route = "chat.messages";
    }

    const { client, socket } = await connect();
    const routingMimeType = WellKnownMimeType.MESSAGE_RSOCKET_ROUTING;
    const authenticationMimeType = WellKnownMimeType.MESSAGE_RSOCKET_AUTHENTICATION;
    client.metadataUpdate((metadata) => {
      metadata.set(routingMimeType, ["chat.default"]);
      metadata.set(authenticationMimeType, WellKnownAuthType.BEARER.auth("channel-authentication"));
    });
    const responses: Flux<ChatAck> = client.process(
      ChatController,
      Flux.fromArray([{ data: { text: "hello" } }])
    );
    let subscription: Subscription | undefined;

    responses.subscribe({
      /** Captures subscription so the test can start the channel. */
      onSubscribe(nextSubscription) {
        subscription = nextSubscription;
      },
      /** Ignores response values in this frame-shape test. */
      onNext() {},
      /** Fails the test on stream errors. */
      onError(error) {
        throw error;
      },
      /** No-op completion hook for the test subscriber. */
      onComplete() {}
    });

    subscription?.request(1);
    await flush();

    const initial = socket.decodeSent(1, metadataMimeType, dataMimeType) as RequestChannelFrame;
    expect(initial).toBeInstanceOf(RequestChannelFrame);
    expect(decodedPayload(initial)).toBeUndefined();
    const initialMetadata = (initial as unknown as { metadata: Metadata<Metadata<any>[]> }).metadata;
    const routes = initialMetadata.payload.filter((entry) => entry.mimeType.mimeType === routingMimeType.mimeType);
    expect(routes).toHaveLength(1);
    expect(routes[0]?.payload).toEqual(["chat.messages"]);
    expect(initialMetadata.payload.find((entry) => entry.mimeType.mimeType === authenticationMimeType.mimeType)?.payload)
      .toMatchObject({ data: "channel-authentication" });

    socket.serverSend(new RequestNFrame(initial.header.streamId, 1));
    await flush();
    await flush();

    const next = socket.decodeSent(2, metadataMimeType, dataMimeType) as PayloadFrame;
    expect(next).toBeInstanceOf(PayloadFrame);
    expect(decodedPayload(next)).toEqual({ text: "hello" });
  });

  it("logs only a declarative controller interaction when controller.log is used", async () => {
    const logs: Array<Record<string, unknown>> = [];
    const { client, socket } = await connect();
    const getUser = new UserLookupController().log({
      payload: true,
      logger: (event: any) => logs.push(event)
    });

    const response = client.process(getUser, { id: 7 }).block();
    const request = socket.decodeSent(1, metadataMimeType, dataMimeType);
    socket.serverSend(
      new PayloadFrame(
        request.header.streamId,
        PayloadFlag.combine(PayloadFlag.NEXT, PayloadFlag.COMPLETE),
        undefined,
        dataMimeType.toPayload({ name: "Ada" })
      )
    );

    await expect(response).resolves.toEqual({ name: "Ada" });
    expect(logs.map((event) => event.type)).toEqual(["interaction", "interaction", "interaction"]);
    expect(logs.map((event) => event.stage)).toEqual(["send", "receive", "complete"]);
    expect(logs[0]).toMatchObject({
      interaction: "requestResponse",
      payload: expect.objectContaining({ data: { id: 7 } })
    });
    expect(logs[1]).toMatchObject({
      interaction: "requestResponse",
      value: { name: "Ada" }
    });
  });

  it("preserves request cancellation through controller interaction logging", async () => {
    const logs: Array<Record<string, unknown>> = [];
    const { client, connected, socket } = await connect();
    const getUser = new UserLookupController().log({ logger: (event: any) => logs.push(event) });
    let subscription: Subscription | undefined;

    client.process(getUser, { id: 7 }).subscribe({
      onSubscribe(nextSubscription) {
        subscription = nextSubscription;
      },
      onNext() {},
      onError() {},
      onComplete() {}
    });
    subscription?.request(1);
    await waitFor(() => socket.sent.length === 2);
    const request = socket.decodeSent(1, metadataMimeType, dataMimeType) as RequestResponseFrame;

    subscription?.cancel();
    await waitFor(() => socket.sent.length === 3);
    const cancel = socket.decodeSent(2, metadataMimeType, dataMimeType) as CancelFrame;

    expect(cancel).toBeInstanceOf(CancelFrame);
    expect(cancel.header.streamId).toBe(request.header.streamId);
    expect(logs.map((event) => event.stage)).toEqual(["send"]);
    connected.disconnect();
  });

  it("disables declarative class controller logs after they were enabled", async () => {
    const logs: Array<Record<string, unknown>> = [];
    const { client, socket } = await connect();
    const getUser = new UserLookupController()
      .log({ logger: (event: any) => logs.push(event) })
      .log(false);

    const response = client.process(getUser, { id: 7 }).block();
    const request = socket.decodeSent(1, metadataMimeType, dataMimeType);
    socket.serverSend(
      new PayloadFrame(
        request.header.streamId,
        PayloadFlag.combine(PayloadFlag.NEXT, PayloadFlag.COMPLETE),
        undefined,
        dataMimeType.toPayload({ name: "Ada" })
      )
    );

    await expect(response).resolves.toEqual({ name: "Ada" });
    expect(logs).toHaveLength(0);
  });

  it("reassembles fragmented PAYLOAD frames before decoding", async () => {
    const { client, socket } = await connect();
    const response = client.requestResponse({ hello: "fragmented" }).block();
    const request = socket.decodeSent(1, metadataMimeType, dataMimeType);
    const encoder = new TextEncoder();

    socket.serverSend(
      new PayloadFrame(
        request.header.streamId,
        PayloadFlag.combine(PayloadFlag.NEXT, PayloadFlag.FOLLOWS),
        undefined,
        WellKnownMimeType.APPLICATION_OCTET_STREAM.toPayload(encoder.encode('{"ok":'))
      )
    );
    socket.serverSend(
      new PayloadFrame(
        request.header.streamId,
        PayloadFlag.combine(PayloadFlag.NEXT, PayloadFlag.COMPLETE),
        undefined,
        WellKnownMimeType.APPLICATION_OCTET_STREAM.toPayload(encoder.encode("true}"))
      )
    );

    await expect(response).resolves.toMatchObject({ data: { ok: true } });
  });

  it("lets COMPLETE terminate a PAYLOAD even when FOLLOWS is also set", async () => {
    const { client, socket } = await connect();
    const response = client.requestResponse({ hello: "contradictory-flags" }).block();
    const request = socket.decodeSent(1, metadataMimeType, dataMimeType);

    socket.serverSend(
      new PayloadFrame(
        request.header.streamId,
        PayloadFlag.combine(PayloadFlag.NEXT, PayloadFlag.FOLLOWS, PayloadFlag.COMPLETE),
        undefined,
        dataMimeType.toPayload({ ok: true })
      )
    );

    await expect(response).resolves.toMatchObject({ data: { ok: true } });
  });

  it("preserves zero-length metadata while reassembling fragmented inbound payloads", async () => {
    const socket = new FakeWebSocket();
    const client = new RSocket("ws://localhost/rsocket", {
      setup: {
        keepAlive: 20_000,
        lifetime: 90_000,
        mimetype: {
          data: dataMimeType,
          metadata: WellKnownMimeType.APPLICATION_OCTET_STREAM
        },
        transport: fakeWebSocketFactory(socket)
      }
    });
    const ready = client.connect().block();
    socket.open();
    await ready;
    const response = client.requestResponse({ hello: "fragmented-metadata" }).block();
    const request = socket.decodeSent(1, WellKnownMimeType.APPLICATION_OCTET_STREAM, dataMimeType);
    const encoder = new TextEncoder();

    socket.serverSend(
      new PayloadFrame(
        request.header.streamId,
        PayloadFlag.combine(PayloadFlag.NEXT, PayloadFlag.FOLLOWS),
        new Metadata(WellKnownMimeType.APPLICATION_OCTET_STREAM, new Uint8Array()),
        WellKnownMimeType.APPLICATION_OCTET_STREAM.toPayload(encoder.encode('{"ok":'))
      )
    );
    socket.serverSend(
      new PayloadFrame(
        request.header.streamId,
        PayloadFlag.combine(PayloadFlag.NEXT, PayloadFlag.COMPLETE),
        undefined,
        WellKnownMimeType.APPLICATION_OCTET_STREAM.toPayload(encoder.encode("true}"))
      )
    );

    await expect(response).resolves.toMatchObject({
      data: { ok: true },
      metadata: new Uint8Array()
    });
  });

  it("moves request-channel COMPLETE to the final fragment", () => {
    const frame = new RequestChannelFrame(
      1,
      RequestChannelFlag.COMPLETE,
      1,
      undefined,
      WellKnownMimeType.APPLICATION_OCTET_STREAM.toPayload(new Uint8Array(160))
    );
    const frameLength = outboundFrameLength(frame);
    const fragments: Frame[] = [];

    expect(frameLength).toBeDefined();
    emitOutboundFrameFragments(
      frame,
      frameLength!,
      64,
      (fragment) => fragments.push(fragment)
    );

    const first = fragments[0] as RequestChannelFrame;
    const last = fragments.at(-1) as PayloadFrame;
    expect(first).toBeInstanceOf(RequestChannelFrame);
    expect(first.hasFollows()).toBe(true);
    expect(first.isComplete()).toBe(false);
    expect(last).toBeInstanceOf(PayloadFrame);
    expect(last.hasFollows()).toBe(false);
    expect(last.isComplete()).toBe(true);
  });

  it("maps Reactive Streams demand to REQUEST_STREAM and REQUEST_N", async () => {
    const { client, socket } = await connect();
    const values: unknown[] = [];
    let subscription: Subscription | undefined;

    client.requestStream({ route: "numbers" }).subscribe({
      onSubscribe(nextSubscription) {
        subscription = nextSubscription;
      },
      onNext(value) {
        values.push(value.data);
      },
      onError(error) {
        throw error;
      },
      onComplete() {}
    });

    expect(socket.sent).toHaveLength(1);
    subscription?.request(2);

    const request = socket.decodeSent(1, metadataMimeType, dataMimeType) as RequestStreamFrame;
    expect(request).toBeInstanceOf(RequestStreamFrame);
    expect(request.header.streamId).toBe(1);
    expect(request.request).toBe(2);

    socket.serverSend(
      new PayloadFrame(
        request.header.streamId,
        PayloadFlag.NEXT,
        undefined,
        dataMimeType.toPayload({ n: 1 })
      )
    );
    await flush();
    expect(values).toEqual([{ n: 1 }]);

    subscription?.request(3);
    const requestN = socket.decodeSent(2, metadataMimeType, dataMimeType) as RequestNFrame;
    expect(requestN).toBeInstanceOf(RequestNFrame);
    expect(requestN.header.streamId).toBe(1);
    expect(requestN.request).toBe(3);

    subscription?.cancel();
    const cancel = socket.decodeSent(3, metadataMimeType, dataMimeType) as CancelFrame;
    expect(cancel).toBeInstanceOf(CancelFrame);
    expect(cancel.header.streamId).toBe(request.header.streamId);
  });

  it("cancels an already-started stream when downstream demand is invalid", async () => {
    const { client, socket } = await connect({ reconnect: false });
    const errors: unknown[] = [];
    let subscription: Subscription | undefined;

    client.requestStream({ route: "invalid-demand" }).subscribe({
      onSubscribe(nextSubscription) {
        subscription = nextSubscription;
      },
      onNext() {},
      onError(error) {
        errors.push(error);
      },
      onComplete() {}
    });

    subscription?.request(1);
    const request = socket.decodeSent(1, metadataMimeType, dataMimeType) as RequestStreamFrame;
    subscription?.request(0);
    await waitFor(() => errors.length === 1);

    const cancel = socket.decodeSent(2, metadataMimeType, dataMimeType) as CancelFrame;
    expect(cancel).toBeInstanceOf(CancelFrame);
    expect(cancel.header.streamId).toBe(request.header.streamId);
    expect(errors[0]).toBeInstanceOf(RangeError);
    expect(socket.readyState).not.toBe(WS_CLOSED);
  });

  it("bounds huge Reactive Streams demand to one replenished protocol window", async () => {
    const { client, socket } = await connect();
    let subscription: Subscription | undefined;

    client.requestStream({ route: "big" }).subscribe({
      onSubscribe(nextSubscription) {
        subscription = nextSubscription;
      },
      onNext() {},
      onError(error) {
        throw error;
      },
      onComplete() {}
    });

    subscription?.request(Number.MAX_SAFE_INTEGER);

    const request = socket.decodeSent(1, metadataMimeType, dataMimeType) as RequestStreamFrame;
    expect(request.request).toBe(0x7fffffff);
    expect(socket.sent).toHaveLength(2);

    // Reach the low-water mark without allocating or dispatching a billion test payloads.
    const internal = subscription as unknown as { wireRequested: number };
    internal.wireRequested = 0x3fffffff;
    socket.serverSend(
      new PayloadFrame(
        request.header.streamId,
        PayloadFlag.NEXT,
        undefined,
        dataMimeType.toPayload({ n: 1 })
      )
    );
    await flush();

    const requestN = socket.decodeSent(2, metadataMimeType, dataMimeType) as RequestNFrame;
    expect(requestN.request).toBe(0x40000001);
  });

  it("does not send reentrant demand after a final NEXT and COMPLETE payload", async () => {
    const { client, socket } = await connect();
    let subscription: Subscription | undefined;
    let completed = false;

    client.requestStream({ route: "final" }).subscribe({
      onSubscribe(nextSubscription) {
        subscription = nextSubscription;
      },
      onNext() {
        subscription?.request(1);
      },
      onError(error) {
        throw error;
      },
      onComplete() {
        completed = true;
      }
    });

    subscription?.request(1);
    const request = socket.decodeSent(1, metadataMimeType, dataMimeType) as RequestStreamFrame;
    socket.serverSend(
      new PayloadFrame(
        request.header.streamId,
        PayloadFlag.combine(PayloadFlag.NEXT, PayloadFlag.COMPLETE),
        undefined,
        dataMimeType.toPayload({ done: true })
      )
    );
    await waitFor(() => completed);

    expect(socket.sent).toHaveLength(2);
  });

  it("propagates stream ERROR frames through the response publisher", async () => {
    const { client, socket } = await connect();
    const values: unknown[] = [];
    const errors: unknown[] = [];
    let subscription: Subscription | undefined;

    client.requestStream({ route: "errors" }).subscribe({
      onSubscribe(nextSubscription) {
        subscription = nextSubscription;
      },
      onNext(value) {
        values.push(value.data);
      },
      onError(error) {
        errors.push(error);
      },
      onComplete() {}
    });

    subscription?.request(1);
    const request = socket.decodeSent(1, metadataMimeType, dataMimeType) as RequestStreamFrame;

    socket.serverSend(
      new ErrorFrame(
        request.header.streamId,
        FrameErrorCode.APPLICATION_ERROR,
        WellKnownMimeType.TEXT_PLAIN.toPayload("stream boom")
      )
    );
    await waitFor(() => errors.length === 1);

    expect(values).toHaveLength(0);
    expect((errors[0] as Error).message).toContain("stream boom");
  });

  it("ignores responder CANCEL on a requester-owned stream", async () => {
    const { client, socket } = await connect();
    const errors: unknown[] = [];
    const values: unknown[] = [];
    let completed = false;
    let subscription: Subscription | undefined;

    client.requestStream({ route: "cancelled" }).subscribe({
      onSubscribe(nextSubscription) {
        subscription = nextSubscription;
      },
      onNext(value) {
        values.push(value.data);
      },
      onError(error) {
        errors.push(error);
      },
      onComplete() {
        completed = true;
      }
    });

    subscription?.request(1);
    const request = socket.decodeSent(1, metadataMimeType, dataMimeType) as RequestStreamFrame;
    socket.serverSend(new CancelFrame(request.header.streamId));
    await flush();
    socket.serverSend(
      new PayloadFrame(
        request.header.streamId,
        PayloadFlag.combine(PayloadFlag.NEXT, PayloadFlag.COMPLETE),
        undefined,
        dataMimeType.toPayload({ ok: true })
      )
    );
    await waitFor(() => completed);

    expect(errors).toHaveLength(0);
    expect(values).toEqual([{ ok: true }]);
    expect(socket.readyState).not.toBe(WS_CLOSED);
  });

  it("does not complete a stream after its onNext callback cancels it", async () => {
    const { client, socket } = await connect();
    let subscription: Subscription | undefined;
    let completed = false;

    client.requestStream({ route: "cancel-on-next" }).subscribe({
      onSubscribe(nextSubscription) {
        subscription = nextSubscription;
      },
      onNext() {
        subscription?.cancel();
      },
      onError(error) {
        throw error;
      },
      onComplete() {
        completed = true;
      }
    });

    subscription?.request(1);
    const request = socket.decodeSent(1, metadataMimeType, dataMimeType) as RequestStreamFrame;
    socket.serverSend(
      new PayloadFrame(
        request.header.streamId,
        PayloadFlag.combine(PayloadFlag.NEXT, PayloadFlag.COMPLETE),
        undefined,
        dataMimeType.toPayload({ value: 1 })
      )
    );
    await flush();

    expect(completed).toBe(false);
    expect(socket.decodeSent(2, metadataMimeType, dataMimeType)).toBeInstanceOf(CancelFrame);
  });

  it("does not close the connection when a stream error callback throws", async () => {
    const { client, socket } = await connect();
    let subscription: Subscription | undefined;

    client.requestStream({ route: "errors" }).subscribe({
      onSubscribe(nextSubscription) {
        subscription = nextSubscription;
      },
      onNext() {},
      onError() {
        throw new Error("consumer error handler failed");
      },
      onComplete() {}
    });

    subscription?.request(1);
    const request = socket.decodeSent(1, metadataMimeType, dataMimeType) as RequestStreamFrame;

    socket.serverSend(
      new ErrorFrame(
        request.header.streamId,
        FrameErrorCode.APPLICATION_ERROR,
        WellKnownMimeType.TEXT_PLAIN.toPayload("stream boom")
      )
    );
    await flush();

    expect(socket.readyState).not.toBe(WS_CLOSED);
    expect(socket.sent).toHaveLength(2);
  });

  it("does not close the connection when a stream complete callback throws", async () => {
    const { client, socket } = await connect();
    let subscription: Subscription | undefined;

    client.requestStream({ route: "complete" }).subscribe({
      onSubscribe(nextSubscription) {
        subscription = nextSubscription;
      },
      onNext() {},
      onError(error) {
        throw error;
      },
      onComplete() {
        throw new Error("consumer complete handler failed");
      }
    });

    subscription?.request(1);
    const request = socket.decodeSent(1, metadataMimeType, dataMimeType) as RequestStreamFrame;

    socket.serverSend(new PayloadFrame(request.header.streamId, PayloadFlag.COMPLETE));
    await flush();

    expect(socket.readyState).not.toBe(WS_CLOSED);
    expect(socket.sent).toHaveLength(2);
  });

  it("cancels a response stream when the subscriber onSubscribe callback throws", async () => {
    const { client, socket } = await connect();

    expect(() => {
      client.requestStream({ route: "subscribe-throws" }).subscribe({
        onSubscribe() {
          throw new Error("consumer subscribe failed");
        },
        onNext() {},
        onError() {},
        onComplete() {}
      });
    }).not.toThrow();

    await flush();

    expect(socket.sent).toHaveLength(1);
    expect(((client as any).client as { streams: Map<number, unknown> }).streams.size).toBe(0);
  });

  it("closes the connection when responder sends PAYLOAD without requester demand", async () => {
    const { client, socket } = await connect({ reconnect: false });
    const values: unknown[] = [];
    const errors: unknown[] = [];
    let subscription: Subscription | undefined;

    client.requestStream({ route: "numbers" }).subscribe({
      onSubscribe(nextSubscription) {
        subscription = nextSubscription;
      },
      onNext(value) {
        values.push(value.data);
      },
      onError(error) {
        errors.push(error);
      },
      onComplete() {}
    });

    subscription?.request(1);
    const request = socket.decodeSent(1, metadataMimeType, dataMimeType) as RequestStreamFrame;

    socket.serverSend(
      new PayloadFrame(
        request.header.streamId,
        PayloadFlag.NEXT,
        undefined,
        dataMimeType.toPayload({ n: 1 })
      )
    );
    await flush();

    socket.serverSend(
      new PayloadFrame(
        request.header.streamId,
        PayloadFlag.NEXT,
        undefined,
        dataMimeType.toPayload({ n: 2 })
      )
    );
    await waitFor(() => socket.readyState === WS_CLOSED);

    const error = socket.decodeSent(2, metadataMimeType, dataMimeType) as ErrorFrame;
    expect(values).toEqual([{ n: 1 }]);
    expect(error).toBeInstanceOf(ErrorFrame);
    expect(error.header.streamId).toBe(0);
    expect(error.code).toBe(FrameErrorCode.CONNECTION_ERROR);
    expect(errors).toHaveLength(1);
    expect((errors[0] as Error).message).toContain("Responder sent PAYLOAD without requester demand");
  });

  it("rejects a stream PAYLOAD without NEXT or COMPLETE", async () => {
    const { client, socket } = await connect({ reconnect: false });
    const errors: unknown[] = [];
    let subscription: Subscription | undefined;

    client.requestStream({ route: "invalid-payload" }).subscribe({
      onSubscribe(nextSubscription) {
        subscription = nextSubscription;
      },
      onNext() {},
      onError(error) {
        errors.push(error);
      },
      onComplete() {}
    });

    subscription?.request(1);
    const request = socket.decodeSent(1, metadataMimeType, dataMimeType) as RequestStreamFrame;
    socket.serverSend(new PayloadFrame(request.header.streamId, 0));
    await waitFor(() => errors.length === 1);

    expect(errors[0]).toMatchObject({
      code: FrameErrorCode.INVALID,
      message: "RSocket PAYLOAD must set FOLLOWS, NEXT, COMPLETE, or a valid combination"
    });
    expect(socket.decodeSent(2, metadataMimeType, dataMimeType)).toBeInstanceOf(CancelFrame);
    expect(socket.readyState).not.toBe(WS_CLOSED);
  });

  it("ignores responder REQUEST_N when a stream has no outbound publisher", async () => {
    const { client, socket } = await connect({ reconnect: false });
    const errors: unknown[] = [];
    let completed = false;
    let subscription: Subscription | undefined;

    client.requestStream({ route: "invalid-request-n" }).subscribe({
      onSubscribe(nextSubscription) {
        subscription = nextSubscription;
      },
      onNext() {},
      onError(error) {
        errors.push(error);
      },
      onComplete() {
        completed = true;
      }
    });

    subscription?.request(1);
    const request = socket.decodeSent(1, metadataMimeType, dataMimeType) as RequestStreamFrame;
    socket.serverSend(new RequestNFrame(request.header.streamId, 1));
    await flush();
    socket.serverSend(new PayloadFrame(request.header.streamId, PayloadFlag.COMPLETE));
    await waitFor(() => completed);

    expect(errors).toHaveLength(0);
    expect(socket.readyState).not.toBe(WS_CLOSED);
  });

  it("ignores responder handshake frames after setup", async () => {
    for (const frame of [
      new ResumeOkFrame(0n),
      new SetupFrame(20_000, 90_000, metadataMimeType, dataMimeType, undefined, 1, 0, 0)
    ]) {
      const { socket } = await connect({ reconnect: false });

      socket.serverSend(frame);
      await flush();

      expect(socket.readyState).not.toBe(WS_CLOSED);
      expect(socket.sent).toHaveLength(1);
    }
  });

  it("ignores METADATA_PUSH carried by a non-zero stream", async () => {
    const { socket } = await connect({ reconnect: false });
    const bytes = new MetadataPushFrame(WellKnownMimeType.TEXT_PLAIN.toMetadata("invalid")).toUint8Array().slice();
    bytes[3] = 1;

    socket.dispatchMessage(bytes);
    await flush();

    expect(socket.readyState).not.toBe(WS_CLOSED);
    expect(socket.sent).toHaveLength(1);
  });

  it("ignores setup and resume rejection errors after setup completed", async () => {
    for (const code of [
      FrameErrorCode.INVALID_SETUP,
      FrameErrorCode.UNSUPPORTED_SETUP,
      FrameErrorCode.REJECTED_SETUP,
      FrameErrorCode.REJECTED_RESUME
    ]) {
      const { client, socket } = await connect({ reconnect: false });
      const response = client.requestResponse({ establish: true }).block();
      const request = socket.decodeSent(1, metadataMimeType, dataMimeType) as RequestResponseFrame;
      socket.serverSend(
        new PayloadFrame(
          request.header.streamId,
          PayloadFlag.combine(PayloadFlag.NEXT, PayloadFlag.COMPLETE),
          undefined,
          dataMimeType.toPayload({ ok: true })
        )
      );
      await response;

      socket.serverSend(new ErrorFrame(0, code, WellKnownMimeType.TEXT_PLAIN.toPayload("stale handshake error")));
      await flush();

      expect(socket.readyState).not.toBe(WS_CLOSED);
      expect(socket.sent).toHaveLength(2);
    }
  });

  it("lets active streams finish after responder CONNECTION_CLOSE", async () => {
    const { client, socket } = await connect({ reconnect: false });
    const response = client.requestResponse({ slow: true }).block();
    const request = socket.decodeSent(1, metadataMimeType, dataMimeType) as RequestResponseFrame;

    socket.serverSend(
      new ErrorFrame(
        0,
        FrameErrorCode.CONNECTION_CLOSE,
        WellKnownMimeType.TEXT_PLAIN.toPayload("server draining")
      )
    );
    await flush();

    expect(socket.readyState).not.toBe(WS_CLOSED);
    await expect(client.fireAndForget({ rejected: true }).block()).rejects.toThrow(
      "responder is closing"
    );

    socket.serverSend(
      new PayloadFrame(
        request.header.streamId,
        PayloadFlag.combine(PayloadFlag.NEXT, PayloadFlag.COMPLETE),
        undefined,
        dataMimeType.toPayload({ ok: true })
      )
    );

    await expect(response).resolves.toMatchObject({ data: { ok: true } });
    await waitFor(() => socket.readyState === WS_CLOSED);
    expect(socket.closeCode).toBe(1000);
  });

  it("closes when a stream-scoped ERROR code is carried on stream zero", async () => {
    const { socket } = await connect({ reconnect: false });
    socket.dispatchMessage(frameWithStreamId(
      new ErrorFrame(1, FrameErrorCode.APPLICATION_ERROR, WellKnownMimeType.TEXT_PLAIN.toPayload("bad stream")),
      0
    ));
    await waitFor(() => socket.readyState === WS_CLOSED);

    const error = socket.decodeSent(1, metadataMimeType, dataMimeType) as ErrorFrame;
    expect(error).toBeInstanceOf(ErrorFrame);
    expect(error.header.streamId).toBe(0);
    expect(error.code).toBe(FrameErrorCode.CONNECTION_ERROR);
    expect(socket.closeCode).toBe(3002);
  });

  it("ignores an ERROR continuation for an unknown stream", async () => {
    const { connected, socket } = await connect({ reconnect: false });
    socket.dispatchMessage(frameWithStreamId(
      new ErrorFrame(0, FrameErrorCode.CONNECTION_ERROR, WellKnownMimeType.TEXT_PLAIN.toPayload("unknown stream")),
      99
    ));
    await flush();

    expect(socket.readyState).not.toBe(WS_CLOSED);
    expect(socket.sent).toHaveLength(1);
    connected.disconnect();
  });

  it("closes when SETUP is rejected before any acceptance signal", async () => {
    const { socket } = await connect({ reconnect: false });

    socket.serverSend(
      new ErrorFrame(0, FrameErrorCode.REJECTED_SETUP, WellKnownMimeType.TEXT_PLAIN.toPayload("setup rejected"))
    );
    await waitFor(() => socket.readyState === WS_CLOSED);

    expect(socket.sent).toHaveLength(1);
  });

  it("does not buffer fragmented PAYLOAD frames for unknown streams", async () => {
    const { client, socket } = await connect({ reconnect: false });

    socket.serverSend(
      new PayloadFrame(
        99,
        PayloadFlag.NEXT | PayloadFlag.FOLLOWS,
        undefined,
        dataMimeType.toPayload({ orphan: true })
      )
    );
    await flush();

    expect(socket.readyState).not.toBe(WS_CLOSED);
    expect(((client as any).client as { fragments: Map<number, unknown> }).fragments.size).toBe(0);
  });

  it("does not invoke application codecs for PAYLOAD frames on unknown streams", async () => {
    class ThrowingDecodeMimeType extends MimeType<unknown> {
      /** Makes accidental application-level decoding observable. */
      protected override deserializePayload(): Payload<unknown> {
        throw new Error("application decoder must not run");
      }
    }

    const socket = new FakeWebSocket();
    const client = new RSocket("ws://localhost/rsocket", {
      setup: {
        ...setupOptions(20_000, 90_000, fakeWebSocketFactory(socket)),
        mimetype: {
          data: new ThrowingDecodeMimeType("application/x-throwing-decoder"),
          metadata: metadataMimeType
        }
      },
      reconnect: false
    });
    const ready = client.connect().block();
    socket.open();
    const connected = (await ready)!;

    socket.serverSend(
      new PayloadFrame(
        99,
        PayloadFlag.NEXT,
        undefined,
        WellKnownMimeType.APPLICATION_OCTET_STREAM.toPayload(new Uint8Array([1]))
      )
    );
    await flush();

    expect(socket.readyState).not.toBe(WS_CLOSED);
    expect(socket.sent).toHaveLength(1);
    connected.disconnect();
  });

  it("echoes KEEPALIVE payloads when RESPOND is set", async () => {
    const { socket } = await connect();
    const keepaliveData = new Uint8Array([1, 2, 3]);

    socket.serverSend(
      new KeepaliveFrame(
        KeepaliveFlag.RESPOND,
        0n,
        WellKnownMimeType.APPLICATION_OCTET_STREAM.toPayload(keepaliveData)
      )
    );
    await flush();

    const response = socket.decodeSent(1, metadataMimeType, dataMimeType) as KeepaliveFrame;
    expect(response).toBeInstanceOf(KeepaliveFrame);
    expect(response.isRequireRespond()).toBe(false);
    expect(Array.from((response as any).payload.payload)).toEqual([1, 2, 3]);
  });

  it("opens a fresh WebSocket after an unexpected disconnect and emits lifecycle events", async () => {
    const sockets: FakeWebSocket[] = [];
    const disconnects: unknown[] = [];
    const reconnects: number[] = [];
    const connected: boolean[] = [];
    const statuses: string[] = [];
    const messages: string[] = [];
    const client = new RSocket("ws://localhost/rsocket", {
      setup: setupOptions(20_000, 90_000, () => {
        const socket = new FakeWebSocket();
        sockets.push(socket);
        return socket;
      }),
      reconnect: true,
      events: {
        event: (event) => statuses.push(event.status),
        disconnect: (event) => {
          messages.push(event.message);
          disconnects.push(event.error);
        },
        reconnecting: (event) => {
          messages.push(event.message);
          reconnects.push(event.attempt);
        },
        connected: (event) => {
          messages.push(event.message);
          connected.push(event.reconnect);
        }
      }
    });

    const ready = client.connect().block();
    sockets[0]?.open();
    await ready;
    expect(sockets[0]?.decodeSent(0, metadataMimeType, dataMimeType)).toBeInstanceOf(SetupFrame);

    sockets[0]?.close(1006, "iOS sleep");
    await waitFor(() => sockets.length === 2);

    sockets[1]?.open();
    await client.connect().block();

    expect(sockets[1]?.decodeSent(0, metadataMimeType, dataMimeType)).toBeInstanceOf(SetupFrame);
    expect(disconnects).toHaveLength(1);
    expect(reconnects).toEqual([1]);
    expect(connected).toEqual([false, true]);
    expect(statuses).toEqual(["connecting", "connected", "disconnected", "reconnecting", "reconnecting", "connected"]);
    expect(messages).toEqual([
      "RSocket connection established",
      "RSocket connection interrupted",
      "Restoring RSocket connection in 0ms",
      "RSocket connection restored"
    ]);
  });

  it("attempts protocol resume before falling back to a fresh SETUP during reconnect", async () => {
    const sockets: FakeWebSocket[] = [];
    const client = new RSocket("ws://localhost/rsocket", {
      setup: setupOptions(20_000, 90_000, () => {
        const socket = new FakeWebSocket();
        sockets.push(socket);
        return socket;
      }),
      reconnect: {
        resume: {
          ttl: 10_000
        }
      }
    });

    const ready = client.connect().block();
    sockets[0]?.open();
    await ready;

    const setup = sockets[0]?.decodeSent(0, metadataMimeType, dataMimeType) as SetupFrame;
    expect(setup).toBeInstanceOf(SetupFrame);
    expect(setup.resumeToken).toEqual(expect.any(String));

    sockets[0]?.close(1006, "network lost");
    await waitFor(() => sockets.length === 2);
    sockets[1]?.open();
    await waitFor(() => (sockets[1]?.sent.length ?? 0) === 1);

    const resume = sockets[1]?.decodeSent(0, metadataMimeType, dataMimeType) as ResumeFrame;
    expect(resume).toBeInstanceOf(ResumeFrame);
    expect(resume.resumeToken).toBe(setup.resumeToken);
    expect(resume.lastReceivedServerPosition).toBe(0n);

    sockets[1]?.serverSend(new ResumeOkFrame(resume.firstAvailableClientPosition));
    await client.connect().block();

    expect(sockets[1]?.sent).toHaveLength(1);
  });

  it("keeps an active request stream and queued demand across successful Resume", async () => {
    const sockets: FakeWebSocket[] = [];
    const values: unknown[] = [];
    const errors: unknown[] = [];
    let completed = false;
    let subscription: Subscription | undefined;
    const client = new RSocket("ws://localhost/rsocket", {
      setup: setupOptions(20_000, 90_000, () => {
        const socket = new FakeWebSocket();
        sockets.push(socket);
        return socket;
      }),
      reconnect: { resume: { ttl: 10_000 } }
    });
    const ready = client.connect().block();
    sockets[0]?.open();
    const connected = (await ready)!;

    try {
      client.requestStream({ route: "resume-stream" }).subscribe({
        onSubscribe(nextSubscription) {
          subscription = nextSubscription;
        },
        onNext(payload) {
          values.push(payload.data);
        },
        onError(error) {
          errors.push(error);
        },
        onComplete() {
          completed = true;
        }
      });
      subscription?.request(1);

      const requestBytes = BigInt(sockets[0]?.sent[1]?.byteLength ?? 0);
      const request = sockets[0]?.decodeSent(1, metadataMimeType, dataMimeType) as RequestStreamFrame;
      sockets[0]?.serverSend(
        new PayloadFrame(
          request.header.streamId,
          PayloadFlag.NEXT,
          undefined,
          dataMimeType.toPayload({ value: 1 })
        )
      );
      await waitFor(() => values.length === 1);

      sockets[0]?.close(1006, "network lost");
      subscription?.request(1);
      await waitFor(() => sockets.length === 2);
      sockets[1]?.open();
      await waitFor(() => (sockets[1]?.sent.length ?? 0) === 1);

      const resume = sockets[1]?.decodeSent(0, metadataMimeType, dataMimeType) as ResumeFrame;
      expect(resume.firstAvailableClientPosition).toBe(0n);
      sockets[1]?.serverSend(new ResumeOkFrame(requestBytes));
      await client.connect().block();
      await waitFor(() => (sockets[1]?.sent.length ?? 0) === 2);

      const queuedDemand = sockets[1]?.decodeSent(1, metadataMimeType, dataMimeType) as RequestNFrame;
      expect(queuedDemand).toBeInstanceOf(RequestNFrame);
      expect(queuedDemand.header.streamId).toBe(request.header.streamId);
      expect(queuedDemand.request).toBe(1);

      sockets[1]?.serverSend(
        new PayloadFrame(
          request.header.streamId,
          PayloadFlag.combine(PayloadFlag.NEXT, PayloadFlag.COMPLETE),
          undefined,
          dataMimeType.toPayload({ value: 2 })
        )
      );
      await waitFor(() => completed);

      expect(values).toEqual([{ value: 1 }, { value: 2 }]);
      expect(errors).toHaveLength(0);
    } finally {
      connected.disconnect();
    }
  });

  it("keeps both request-channel directions and their backpressure across Resume", async () => {
    const sockets: FakeWebSocket[] = [];
    const client = new RSocket("ws://localhost/rsocket", {
      setup: setupOptions(20_000, 90_000, () => {
        const socket = new FakeWebSocket();
        sockets.push(socket);
        return socket;
      }),
      reconnect: { resume: { ttl: 10_000 } }
    });
    const ready = client.connect().block();
    sockets[0]?.open();
    const connected = (await ready)!;
    const channel = client.requestChannel<{ value: string }>();
    const values: unknown[] = [];
    const errors: unknown[] = [];
    let responseSubscription: Subscription | undefined;
    let completed = false;

    try {
      channel.subscribe({
        onSubscribe(subscription) {
          responseSubscription = subscription;
        },
        onNext(value) {
          values.push(value.data);
        },
        onError(error) {
          errors.push(error);
        },
        onComplete() {
          completed = true;
        }
      });
      responseSubscription?.request(1);
      channel.next({ data: { value: "a" } });
      await waitFor(() => (sockets[0]?.sent.length ?? 0) === 2);

      const initial = sockets[0]?.decodeSent(1, metadataMimeType, dataMimeType) as RequestChannelFrame;
      expect(initial).toBeInstanceOf(RequestChannelFrame);
      sockets[0]?.serverSend(new RequestNFrame(initial.header.streamId, 1));
      channel.next({ data: { value: "b" } });
      await waitFor(() => (sockets[0]?.sent.length ?? 0) === 3);

      const acknowledged = BigInt(
        (sockets[0]?.sent.slice(1) ?? []).reduce((length, bytes) => length + bytes.byteLength, 0)
      );
      sockets[0]?.serverSend(new KeepaliveFrame(KeepaliveFlag.NONE, acknowledged));
      await flush();
      sockets[0]?.close(1006, "network lost");
      await waitFor(() => sockets.length === 2);

      responseSubscription?.request(1);
      channel.next({ data: { value: "c" } });
      sockets[1]?.open();
      await waitFor(() => (sockets[1]?.sent.length ?? 0) === 1);
      const resume = sockets[1]?.decodeSent(0, metadataMimeType, dataMimeType) as ResumeFrame;
      expect(resume.firstAvailableClientPosition).toBe(acknowledged);
      sockets[1]?.serverSend(new ResumeOkFrame(acknowledged));
      await client.connect().block();
      await waitFor(() => (sockets[1]?.sent.length ?? 0) === 2);

      const queuedResponseDemand = sockets[1]?.decodeSent(1, metadataMimeType, dataMimeType) as RequestNFrame;
      expect(queuedResponseDemand.header.streamId).toBe(initial.header.streamId);
      expect(queuedResponseDemand.request).toBe(1);

      sockets[1]?.serverSend(new RequestNFrame(initial.header.streamId, 1));
      await waitFor(() => (sockets[1]?.sent.length ?? 0) === 3);
      const resumedOutbound = sockets[1]?.decodeSent(2, metadataMimeType, dataMimeType) as PayloadFrame;
      expect(resumedOutbound.header.streamId).toBe(initial.header.streamId);
      expect(decodedPayload(resumedOutbound)).toEqual({ value: "c" });

      channel.complete();
      await waitFor(() => (sockets[1]?.sent.length ?? 0) === 4);
      const outboundComplete = sockets[1]?.decodeSent(3, metadataMimeType, dataMimeType) as PayloadFrame;
      expect(outboundComplete.isComplete()).toBe(true);

      sockets[1]?.serverSend(
        new PayloadFrame(
          initial.header.streamId,
          PayloadFlag.combine(PayloadFlag.NEXT, PayloadFlag.COMPLETE),
          undefined,
          dataMimeType.toPayload({ accepted: true })
        )
      );
      await waitFor(() => completed);

      expect(values).toEqual([{ accepted: true }]);
      expect(errors).toHaveLength(0);
    } finally {
      connected.disconnect();
    }
  });

  it("replays an unacknowledged request-response frame after RESUME_OK", async () => {
    const sockets: FakeWebSocket[] = [];
    const client = new RSocket("ws://localhost/rsocket", {
      setup: setupOptions(20_000, 90_000, () => {
        const socket = new FakeWebSocket();
        sockets.push(socket);
        return socket;
      }),
      reconnect: { resume: { ttl: 10_000 } }
    });
    const ready = client.connect().block();
    sockets[0]?.open();
    const connected = (await ready)!;

    try {
      if (sockets[0] !== undefined) {
        sockets[0].onSend = () => {
          if (sockets[0]?.sent.length === 3) sockets[0]?.close(1006, "network lost during send");
        };
      }
      const response = client.requestResponse({ value: "replay" }).block();
      const request = sockets[0]?.decodeSent(1, metadataMimeType, dataMimeType) as RequestResponseFrame;
      await client.fireAndForget({ value: "also-replay" }).block();
      const requestPosition = BigInt(
        (sockets[0]?.sent.slice(1) ?? []).reduce((length, bytes) => length + bytes.byteLength, 0)
      );

      await waitFor(() => sockets.length === 2);
      sockets[1]?.open();
      await waitFor(() => (sockets[1]?.sent.length ?? 0) === 1);

      const resume = sockets[1]?.decodeSent(0, metadataMimeType, dataMimeType) as ResumeFrame;
      expect(resume.firstAvailableClientPosition).toBe(0n);
      if (sockets[1] !== undefined) {
        sockets[1].onSend = () => {
          if (sockets[1]?.sent.length === 2) {
            sockets[1]?.serverSend(new KeepaliveFrame(KeepaliveFlag.NONE, requestPosition));
          }
        };
      }
      sockets[1]?.serverSend(new ResumeOkFrame(0n));
      await client.connect().block();
      await waitFor(() => (sockets[1]?.sent.length ?? 0) === 3);

      const replay = sockets[1]?.decodeSent(1, metadataMimeType, dataMimeType) as RequestResponseFrame;
      expect(replay).toBeInstanceOf(RequestResponseFrame);
      expect(replay.header.streamId).toBe(request.header.streamId);
      expect(decodedPayload(replay)).toEqual({ value: "replay" });
      const replayedFnf = sockets[1]?.decodeSent(2, metadataMimeType, dataMimeType) as RequestFireAndForgetFrame;
      expect(replayedFnf).toBeInstanceOf(RequestFireAndForgetFrame);
      expect(decodedPayload(replayedFnf)).toEqual({ value: "also-replay" });

      sockets[1]?.serverSend(
        new PayloadFrame(
          replay.header.streamId,
          PayloadFlag.combine(PayloadFlag.NEXT, PayloadFlag.COMPLETE),
          undefined,
          dataMimeType.toPayload({ replayed: true })
        )
      );
      await expect(response).resolves.toMatchObject({ data: { replayed: true } });
    } finally {
      connected.disconnect();
    }
  });

  it("keeps retrying Resume after transport and responder connection errors", async () => {
    const sockets: FakeWebSocket[] = [];
    const client = new RSocket("ws://localhost/rsocket", {
      setup: setupOptions(20_000, 90_000, () => {
        const socket = new FakeWebSocket();
        sockets.push(socket);
        return socket;
      }),
      reconnect: {
        resume: {
          ttl: 10_000
        }
      }
    });

    const ready = client.connect().block();
    sockets[0]?.open();
    await ready;

    vi.useFakeTimers();
    try {
      const setup = sockets[0]?.decodeSent(0, metadataMimeType, dataMimeType) as SetupFrame;
      sockets[0]?.close(1006, "network lost");
      await vi.advanceTimersByTimeAsync(0);
      expect(sockets).toHaveLength(2);

      sockets[1]?.close(1006, "server unavailable");
      await vi.advanceTimersByTimeAsync(3_000);
      expect(sockets).toHaveLength(3);
      sockets[2]?.open();
      await vi.advanceTimersByTimeAsync(0);

      const resume = sockets[2]?.decodeSent(0, metadataMimeType, dataMimeType) as ResumeFrame;
      expect(resume).toBeInstanceOf(ResumeFrame);
      expect(resume.resumeToken).toBe(setup.resumeToken);

      sockets[2]?.serverSend(
        new ErrorFrame(
          0,
          FrameErrorCode.CONNECTION_ERROR,
          WellKnownMimeType.TEXT_PLAIN.toPayload("retry this transport")
        )
      );
      await vi.advanceTimersByTimeAsync(3_900);
      expect(sockets).toHaveLength(4);
      sockets[3]?.open();
      await vi.advanceTimersByTimeAsync(0);

      const retry = sockets[3]?.decodeSent(0, metadataMimeType, dataMimeType) as ResumeFrame;
      expect(retry).toBeInstanceOf(ResumeFrame);
      expect(retry.resumeToken).toBe(setup.resumeToken);
      sockets[3]?.serverSend(new ResumeOkFrame(retry.firstAvailableClientPosition));
      const connected = await client.connect().block();
      connected?.disconnect();
    } finally {
      vi.useRealTimers();
    }
  });

  it("ignores stale handshake errors only after protocol SETUP acceptance proof", async () => {
    for (const [name, proof] of [
      ["LEASE", new LeaseFrame(60_000, 1)],
      ["REQUEST_FNF", new RequestFireAndForgetFrame(2, 0)]
    ] as const) {
      const { connected, socket } = await connect({ reconnect: false });
      socket.serverSend(proof);
      await flush();

      socket.serverSend(
        new ErrorFrame(0, FrameErrorCode.REJECTED_SETUP, WellKnownMimeType.TEXT_PLAIN.toPayload("stale rejection"))
      );
      await flush();

      expect(socket.readyState, name).not.toBe(WS_CLOSED);
      expect(socket.sent, name).toHaveLength(1);
      connected.disconnect();
    }
  });

  it("releases the stable-uptime timer after a recovered connection proves healthy", async () => {
    const sockets: FakeWebSocket[] = [];
    const client = new RSocket("ws://localhost/rsocket", {
      setup: setupOptions(20_000, 90_000, () => {
        const socket = new FakeWebSocket();
        sockets.push(socket);
        return socket;
      }),
      reconnect: true
    });
    const ready = client.connect().block();
    sockets[0]?.open();
    const connected = (await ready)!;

    vi.useFakeTimers();
    try {
      sockets[0]?.close(1006, "network lost");
      await vi.advanceTimersByTimeAsync(0);
      expect(sockets).toHaveLength(2);

      sockets[1]?.open();
      await vi.advanceTimersByTimeAsync(0);
      const state = client as unknown as {
        stableConnectionTimer?: ReturnType<typeof setTimeout>;
      };
      expect(state.stableConnectionTimer).toBeDefined();

      await vi.advanceTimersByTimeAsync(5_000);
      expect(state.stableConnectionTimer).toBeUndefined();
    } finally {
      connected.disconnect();
      vi.useRealTimers();
    }
  });

  it("counts only resume-tracked frame types in protocol positions", async () => {
    const sockets: FakeWebSocket[] = [];
    const client = new RSocket("ws://localhost/rsocket", {
      setup: setupOptions(20_000, 90_000, () => {
        const socket = new FakeWebSocket();
        sockets.push(socket);
        return socket;
      }),
      reconnect: {
        resume: {
          ttl: 10_000
        }
      }
    });

    const ready = client.connect().block();
    sockets[0]?.open();
    await ready;

    const response = client.requestResponse({ hello: "resume-position" }).block();
    const requestBytes = BigInt(sockets[0]?.sent[1]?.byteLength ?? 0);
    const request = sockets[0]?.decodeSent(1, metadataMimeType, dataMimeType) as RequestResponseFrame;
    const responseFrame = new PayloadFrame(
      request.header.streamId,
      PayloadFlag.combine(PayloadFlag.NEXT, PayloadFlag.COMPLETE),
      undefined,
      dataMimeType.toPayload({ ok: true })
    );
    const responseBytes = BigInt(responseFrame.toUint8Array().byteLength);

    sockets[0]?.serverSend(responseFrame);
    await expect(response).resolves.toMatchObject({ data: { ok: true } });

    await client
      .metadataPush(WellKnownMimeType.TEXT_PLAIN.toMetadata("connection metadata"))
      .block();
    sockets[0]?.serverSend(
      new KeepaliveFrame(KeepaliveFlag.NONE, requestBytes, dataMimeType.toPayload({ keepalive: true }))
    );
    sockets[0]?.serverSend(new ExtensionFrame(2, ExtensionFlag.IGNORE, 1));
    await flush();

    sockets[0]?.close(1006, "network lost");
    await waitFor(() => sockets.length === 2);
    sockets[1]?.open();
    await waitFor(() => (sockets[1]?.sent.length ?? 0) === 1);

    const resume = sockets[1]?.decodeSent(0, metadataMimeType, dataMimeType) as ResumeFrame;
    expect(resume).toBeInstanceOf(ResumeFrame);
    expect(resume.firstAvailableClientPosition).toBe(requestBytes);
    expect(resume.lastReceivedServerPosition).toBe(responseBytes);

    sockets[1]?.serverSend(new ResumeOkFrame(resume.firstAvailableClientPosition));
    await client.connect().block();

    const resumedResponse = client.requestResponse({ hello: "after-resume" }).block();
    const resumedRequest = sockets[1]?.decodeSent(1, metadataMimeType, dataMimeType) as RequestResponseFrame;
    expect(resumedRequest.header.streamId).toBe(3);
    sockets[1]?.serverSend(
      new PayloadFrame(
        resumedRequest.header.streamId,
        PayloadFlag.combine(PayloadFlag.NEXT, PayloadFlag.COMPLETE),
        undefined,
        dataMimeType.toPayload({ ok: true })
      )
    );
    await expect(resumedResponse).resolves.toMatchObject({ data: { ok: true } });
  });

  it("falls back to a fresh SETUP when protocol resume is rejected", async () => {
    const sockets: FakeWebSocket[] = [];
    const resumeRejected: Array<Record<string, unknown>> = [];
    const lifecycleTypes: string[] = [];
    const client = new RSocket("ws://localhost/rsocket", {
      setup: setupOptions(20_000, 90_000, () => {
        const socket = new FakeWebSocket();
        sockets.push(socket);
        return socket;
      }),
      reconnect: {
        resume: {
          ttl: 10_000
        }
      },
      events: {
        event: (event: any) => lifecycleTypes.push(event.type),
        resumeRejected: (event: any) => resumeRejected.push(event)
      }
    });

    const ready = client.connect().block();
    sockets[0]?.open();
    await ready;
    const initialSetup = sockets[0]?.decodeSent(0, metadataMimeType, dataMimeType) as SetupFrame;

    vi.useFakeTimers();
    try {
      sockets[0]?.close(1006, "network lost");
      await vi.advanceTimersByTimeAsync(0);
      expect(sockets).toHaveLength(2);
      sockets[1]?.open();
      await vi.advanceTimersByTimeAsync(0);
      expect(sockets[1]?.decodeSent(0, metadataMimeType, dataMimeType)).toBeInstanceOf(ResumeFrame);

      sockets[1]?.serverSend(
        new ErrorFrame(
          0,
          FrameErrorCode.REJECTED_RESUME,
          WellKnownMimeType.TEXT_PLAIN.toPayload("resume rejected")
        )
      );
      await vi.advanceTimersByTimeAsync(0);
      expect(sockets).toHaveLength(3);
      sockets[2]?.open();
      await vi.advanceTimersByTimeAsync(0);

      const freshSetup = sockets[2]?.decodeSent(0, metadataMimeType, dataMimeType) as SetupFrame;
      expect(freshSetup).toBeInstanceOf(SetupFrame);
      expect(freshSetup.resumeToken).not.toBe(initialSetup.resumeToken);
      expect(resumeRejected).toHaveLength(1);
      expect(resumeRejected[0]).toMatchObject({
        type: "resumeRejected",
        status: "reconnecting",
        reconnect: true,
        recovering: true,
        willReconnect: true,
        message: "RSocket resume rejected; opening a fresh connection"
      });
      expect((resumeRejected[0]?.error as { code?: FrameErrorCode } | undefined)?.code)
        .toBe(FrameErrorCode.REJECTED_RESUME);
      expect(lifecycleTypes).toContain("resumeRejected");
      const freshConnected = (await client.connect().block())!;

      sockets[2]?.close(1006, "network lost again");
      await vi.advanceTimersByTimeAsync(3_000);
      expect(sockets).toHaveLength(4);
      sockets[3]?.open();
      await vi.advanceTimersByTimeAsync(0);
      const nextResume = sockets[3]?.decodeSent(0, metadataMimeType, dataMimeType) as ResumeFrame;
      expect(nextResume.resumeToken).toBe(freshSetup.resumeToken);
      sockets[3]?.serverSend(new ResumeOkFrame(0n));
      await client.connect().block();
      freshConnected.disconnect();
    } finally {
      vi.useRealTimers();
    }
  });

  it("fails retained interactions when Resume is rejected and SETUP starts fresh", async () => {
    const sockets: FakeWebSocket[] = [];
    const client = new RSocket("ws://localhost/rsocket", {
      setup: setupOptions(20_000, 90_000, () => {
        const socket = new FakeWebSocket();
        sockets.push(socket);
        return socket;
      }),
      reconnect: { resume: { ttl: 10_000 } }
    });
    const ready = client.connect().block();
    sockets[0]?.open();
    const connected = (await ready)!;

    try {
      const retained = client.requestResponse({ slow: true }).block();
      expect(sockets[0]?.decodeSent(1, metadataMimeType, dataMimeType)).toBeInstanceOf(RequestResponseFrame);

      sockets[0]?.close(1006, "network lost");
      await waitFor(() => sockets.length === 2);
      sockets[1]?.open();
      await waitFor(() => (sockets[1]?.sent.length ?? 0) === 1);
      sockets[1]?.serverSend(
        new ErrorFrame(
          0,
          FrameErrorCode.REJECTED_RESUME,
          WellKnownMimeType.TEXT_PLAIN.toPayload("resume state expired")
        )
      );

      await expect(retained).rejects.toThrow("resume state expired");
      await waitFor(() => sockets.length === 3);
      sockets[2]?.open();
      await client.connect().block();

      expect(sockets[2]?.decodeSent(0, metadataMimeType, dataMimeType)).toBeInstanceOf(SetupFrame);
    } finally {
      connected.disconnect();
    }
  });

  it("falls back to a fresh SETUP when RESUME_OK acknowledges an impossible client position", async () => {
    const sockets: FakeWebSocket[] = [];
    const resumeRejected: unknown[] = [];
    const client = new RSocket("ws://localhost/rsocket", {
      setup: setupOptions(20_000, 90_000, () => {
        const socket = new FakeWebSocket();
        sockets.push(socket);
        return socket;
      }),
      reconnect: {
        resume: {
          ttl: 10_000
        }
      },
      events: {
        resumeRejected: (event) => resumeRejected.push(event)
      }
    });

    const ready = client.connect().block();
    sockets[0]?.open();
    await ready;

    const response = client.requestResponse({ hello: "resume-position" }).block();
    const request = sockets[0]?.decodeSent(1, metadataMimeType, dataMimeType) as RequestResponseFrame;
    sockets[0]?.serverSend(
      new PayloadFrame(
        request.header.streamId,
        PayloadFlag.combine(PayloadFlag.NEXT, PayloadFlag.COMPLETE),
        undefined,
        dataMimeType.toPayload({ ok: true })
      )
    );
    await expect(response).resolves.toMatchObject({ data: { ok: true } });

    sockets[0]?.close(1006, "network lost");
    await waitFor(() => sockets.length === 2);
    sockets[1]?.open();
    await waitFor(() => (sockets[1]?.sent.length ?? 0) === 1);

    const resume = sockets[1]?.decodeSent(0, metadataMimeType, dataMimeType) as ResumeFrame;
    expect(resume).toBeInstanceOf(ResumeFrame);

    sockets[1]?.serverSend(new ResumeOkFrame(resume.firstAvailableClientPosition + 1n));
    await waitFor(() => sockets.length === 3);
    sockets[2]?.open();
    await waitFor(() => (sockets[2]?.sent.length ?? 0) === 1);

    expect(sockets[2]?.decodeSent(0, metadataMimeType, dataMimeType)).toBeInstanceOf(SetupFrame);
    expect(resumeRejected).toHaveLength(0);
    await client.connect().block();
  });

  it("does not open a fresh SETUP transport when disconnect aborts pending resume", async () => {
    const sockets: FakeWebSocket[] = [];
    const client = new RSocket("ws://localhost/rsocket", {
      setup: setupOptions(20_000, 90_000, () => {
        const socket = new FakeWebSocket();
        sockets.push(socket);
        return socket;
      }),
      reconnect: {
        resume: {
          ttl: 10_000
        }
      }
    });

    const ready = client.connect().block();
    sockets[0]?.open();
    const connected = (await ready)!;

    sockets[0]?.close(1006, "network lost");
    await waitFor(() => sockets.length === 2);
    sockets[1]?.open();
    await waitFor(() => (sockets[1]?.sent.length ?? 0) === 1);
    expect(sockets[1]?.decodeSent(0, metadataMimeType, dataMimeType)).toBeInstanceOf(ResumeFrame);

    connected.disconnect(1000, "done");
    await flush();
    await flush();

    expect(sockets).toHaveLength(2);
    expect(sockets[1]?.readyState).toBe(WS_CLOSED);
  });

  it("can connect again immediately after disconnect aborts an in-flight reconnect", async () => {
    const sockets: FakeWebSocket[] = [];
    const client = new RSocket("ws://localhost/rsocket", {
      setup: setupOptions(20_000, 90_000, () => {
        const socket = new FakeWebSocket();
        sockets.push(socket);
        return socket;
      }),
      reconnect: true
    });

    const initialReady = client.connect().block();
    sockets[0]?.open();
    const connected = (await initialReady)!;

    sockets[0]?.close(1006, "network lost");
    await waitFor(() => sockets.length === 2);
    connected.disconnect(1000, "cancel reconnect");

    const nextReady = client.connect().block();
    await waitFor(() => sockets.length === 3);
    sockets[2]?.open();

    const reconnected = await nextReady;
    expect(reconnected).toBeDefined();
    expect(sockets[2]?.decodeSent(0, metadataMimeType, dataMimeType)).toBeInstanceOf(SetupFrame);
    reconnected?.disconnect();
  });

  it("waits for browser online before opening a reconnect transport", async () => {
    const browser = installBrowserSignals(true);
    const sockets: FakeWebSocket[] = [];

    try {
      const client = new RSocket("ws://localhost/rsocket", {
        setup: setupOptions(20_000, 90_000, () => {
          const socket = new FakeWebSocket();
          sockets.push(socket);
          return socket;
        }),
        reconnect: {
          resume: {
            ttl: 10_000
          }
        }
      });
      const ready = client.connect().block();
      sockets[0]?.open();
      await ready;

      const setup = sockets[0]?.decodeSent(0, metadataMimeType, dataMimeType) as SetupFrame;
      browser.setOnline(false);
      sockets[0]?.close(1006, "offline");
      await flush();
      await flush();
      expect(sockets).toHaveLength(1);

      browser.setOnline(true);
      browser.dispatch("online");
      await waitFor(() => sockets.length === 2);
      sockets[1]?.open();
      await waitFor(() => (sockets[1]?.sent.length ?? 0) === 1);

      const resume = sockets[1]?.decodeSent(0, metadataMimeType, dataMimeType) as ResumeFrame;
      expect(resume).toBeInstanceOf(ResumeFrame);
      expect(resume.resumeToken).toBe(setup.resumeToken);

      sockets[1]?.serverSend(new ResumeOkFrame(resume.firstAvailableClientPosition));
      const connected = (await client.connect().block())!;
      connected.disconnect();
    } finally {
      browser.restore();
    }
  });

  it("checks keepalive lifetime on browser wake and resumes stale sessions", async () => {
    const browser = installBrowserSignals(true);
    const sockets: FakeWebSocket[] = [];

    try {
      const client = new RSocket("ws://localhost/rsocket", {
        setup: setupOptions(10_000, 5, () => {
          const socket = new FakeWebSocket();
          sockets.push(socket);
          return socket;
        }),
        reconnect: {
          resume: {
            ttl: 10_000
          }
        }
      });
      const ready = client.connect().block();
      sockets[0]?.open();
      await ready;

      const setup = sockets[0]?.decodeSent(0, metadataMimeType, dataMimeType) as SetupFrame;
      await new Promise((resolve) => setTimeout(resolve, 10));
      browser.dispatch("focus");

      await waitFor(() => sockets.length === 2);
      sockets[1]?.open();
      await waitFor(() => (sockets[1]?.sent.length ?? 0) === 1);

      const resume = sockets[1]?.decodeSent(0, metadataMimeType, dataMimeType) as ResumeFrame;
      expect(resume).toBeInstanceOf(ResumeFrame);
      expect(resume.resumeToken).toBe(setup.resumeToken);

      sockets[1]?.serverSend(new ResumeOkFrame(resume.firstAvailableClientPosition));
      const connected = (await client.connect().block())!;
      connected.disconnect();
    } finally {
      browser.restore();
    }
  });

  it("keeps increasing reconnect attempts until a connection survives the stability window", async () => {
    vi.useFakeTimers();
    const sockets: FakeWebSocket[] = [];
    const attempts: number[] = [];
    const client = new RSocket("ws://localhost/rsocket", {
      setup: setupOptions(20_000, 90_000, () => {
        const socket = new FakeWebSocket();
        sockets.push(socket);
        return socket;
      }),
      reconnect: true,
      events: {
        reconnecting: (event) => attempts.push(event.attempt)
      }
    });

    try {
      const firstReady = client.connect().block();
      sockets[0]?.open();
      await firstReady;

      sockets[0]?.close(1006, "first drop");
      await vi.advanceTimersByTimeAsync(0);
      expect(sockets).toHaveLength(2);
      sockets[1]?.open();
      await client.connect().block();

      sockets[1]?.close(1006, "second drop");
      await vi.advanceTimersByTimeAsync(3_000);
      expect(sockets).toHaveLength(3);
      sockets[2]?.open();
      const connected = (await client.connect().block())!;
      connected.disconnect();

      expect(attempts).toEqual([1, 2]);
    } finally {
      vi.useRealTimers();
    }
  });

  it("exposes lifecycle events through the constructor event handler", async () => {
    const events: string[] = [];
    const { connected } = await connect({
      events: {
        event: (event: any) => events.push(event.type)
      }
    });

    connected.disconnect(1000, "done");
    await flush();

    expect(events).toEqual(["connecting", "connected", "disconnect"]);
  });

  it("passes full lifecycle event payloads to constructor handlers", async () => {
    const events: Array<{ type: string; status: string; message: string }> = [];
    const { connected } = await connect({
      events: {
        event: (event: any) => {
          events.push({
            type: event.type,
            status: event.status,
            message: event.message
          });
        }
      }
    });

    events.length = 0;
    connected.disconnect(1000, "done");
    await flush();

    expect(events).toEqual([
      {
        type: "disconnect",
        status: "disconnected",
        message: "RSocket connection disconnected"
      }
    ]);
  });

  it("fails in-flight requests on disconnect instead of pretending to resume them", async () => {
    const { client, socket } = await connect({ reconnect: false });
    const response = client.requestResponse({ slow: true }).block();

    expect(socket.decodeSent(1, metadataMimeType, dataMimeType)).toBeInstanceOf(RequestResponseFrame);
    socket.close(1006, "network lost");

    await expect(response).rejects.toThrow("WebSocket closed");
    expect(socket.readyState).toBe(WS_CLOSED);
  });

  it("continues session cleanup when a stream error callback throws on disconnect", async () => {
    const { client, socket } = await connect({ reconnect: false });
    let subscription: Subscription | undefined;

    client.requestStream({ route: "numbers" }).subscribe({
      onSubscribe(nextSubscription) {
        subscription = nextSubscription;
      },
      onNext() {},
      onError() {
        throw new Error("consumer error");
      },
      onComplete() {}
    });
    subscription?.request(1);
    await flush();

    expect(() => socket.close(1006, "network lost")).not.toThrow();
    await waitFor(() => socket.readyState === WS_CLOSED);
  });

  it("fails active request streams after normal reconnect without automatic resubscribe", async () => {
    const sockets: FakeWebSocket[] = [];
    const errors: unknown[] = [];
    let subscription: Subscription | undefined;
    const client = new RSocket("ws://localhost/rsocket", {
      setup: setupOptions(20_000, 90_000, () => {
        const socket = new FakeWebSocket();
        sockets.push(socket);
        return socket;
      }),
      reconnect: true
    });
    const ready = client.connect().block();
    sockets[0]?.open();
    await ready;

    client.requestStream({ route: "numbers" }).subscribe({
      /** Captures subscription so the stream can start. */
      onSubscribe(nextSubscription) {
        subscription = nextSubscription;
      },
      /** Ignores values because the test verifies terminal behavior. */
      onNext() {},
      /** Stores the disconnect error produced for the active stream. */
      onError(error) {
        errors.push(error);
      },
      /** No-op completion hook for this disconnect test. */
      onComplete() {}
    });

    subscription?.request(1);
    await waitFor(() => (sockets[0]?.sent.length ?? 0) > 1);
    expect(sockets[0]?.decodeSent(1, metadataMimeType, dataMimeType)).toBeInstanceOf(RequestStreamFrame);

    sockets[0]?.close(1006, "network lost");
    await waitFor(() => errors.length === 1);
    await waitFor(() => sockets.length === 2);
    sockets[1]?.open();
    await client.connect().block();
    await flush();

    expect((errors[0] as Error).message).toContain("WebSocket closed");
    expect(sockets[1]?.sent).toHaveLength(1);
    expect(sockets[1]?.decodeSent(0, metadataMimeType, dataMimeType)).toBeInstanceOf(SetupFrame);
  });

  it("fails active request channels after normal reconnect without replay", async () => {
    const sockets: FakeWebSocket[] = [];
    const errors: unknown[] = [];
    let subscription: Subscription | undefined;
    const client = new RSocket("ws://localhost/rsocket", {
      setup: setupOptions(20_000, 90_000, () => {
        const socket = new FakeWebSocket();
        sockets.push(socket);
        return socket;
      }),
      reconnect: true
    });
    const ready = client.connect().block();
    sockets[0]?.open();
    await ready;

    const channel = client.requestChannel<{ text: string }>();
    channel.subscribe({
      /** Captures subscription so the test controls response demand precisely. */
      onSubscribe(nextSubscription) {
        subscription = nextSubscription;
      },
      /** Ignores values because the test verifies terminal behavior. */
      onNext() {},
      /** Stores the disconnect error produced for the active channel. */
      onError(error) {
        errors.push(error);
      },
      /** No-op completion hook for this disconnect test. */
      onComplete() {}
    });

    subscription?.request(1);
    channel.next({ data: { text: "a" } });
    await waitFor(() => (sockets[0]?.sent.length ?? 0) > 1);

    const first = sockets[0]?.decodeSent(1, metadataMimeType, dataMimeType) as RequestChannelFrame;
    expect(first).toBeInstanceOf(RequestChannelFrame);
    expect(first.request).toBe(1);
    expect(decodedPayload(first)).toEqual({ text: "a" });

    sockets[0]?.close(1006, "network lost");
    await waitFor(() => errors.length === 1);
    await waitFor(() => sockets.length === 2);
    sockets[1]?.open();
    await client.connect().block();
    await flush();

    expect((errors[0] as Error).message).toContain("WebSocket closed");
    expect(sockets[1]?.sent).toHaveLength(1);
    expect(sockets[1]?.decodeSent(0, metadataMimeType, dataMimeType)).toBeInstanceOf(SetupFrame);
  });

  it("does not reconnect after an explicit disconnect", async () => {
    const sockets: FakeWebSocket[] = [];
    const disconnects: unknown[] = [];
    const client = new RSocket("ws://localhost/rsocket", {
      setup: setupOptions(20_000, 90_000, () => {
        const socket = new FakeWebSocket();
        sockets.push(socket);
        return socket;
      }),
      reconnect: true,
      events: {
        disconnect: (event) => disconnects.push(event.error)
      }
    });

    const ready = client.connect().block();
    sockets[0]?.open();
    const connected = (await ready)!;

    connected.disconnect(1000, "done");
    await flush();

    expect(sockets).toHaveLength(1);
    expect(sockets[0]?.readyState).toBe(WS_CLOSED);
    const shutdown = sockets[0]?.decodeSent(1, metadataMimeType, dataMimeType) as ErrorFrame;
    expect(shutdown.code).toBe(FrameErrorCode.CONNECTION_ERROR);
    expect(disconnects).toHaveLength(1);
  });

  it("cancels the pending retry when reconnecting listener disconnects synchronously", async () => {
    vi.useFakeTimers();
    const sockets: FakeWebSocket[] = [];
    let connected: { disconnect(code?: number, reason?: string): unknown } | undefined;
    const client = new RSocket("ws://localhost/rsocket", {
      setup: setupOptions(20_000, 90_000, () => {
        const socket = new FakeWebSocket();
        sockets.push(socket);
        return socket;
      }),
      reconnect: true,
      events: {
        reconnecting: () => connected?.disconnect(1000, "stop retrying")
      }
    });

    try {
      const ready = client.connect().block();
      sockets[0]?.open();
      connected = await ready;

      sockets[0]?.close(1006, "network lost");
      await vi.advanceTimersByTimeAsync(60_000);

      expect(sockets).toHaveLength(1);
    } finally {
      connected?.disconnect();
      vi.useRealTimers();
    }
  });

  it("does not open a reconnect transport after connecting listener disconnects", async () => {
    vi.useFakeTimers();
    const sockets: FakeWebSocket[] = [];
    let connected: { disconnect(code?: number, reason?: string): unknown } | undefined;
    const client = new RSocket("ws://localhost/rsocket", {
      setup: setupOptions(20_000, 90_000, () => {
        const socket = new FakeWebSocket();
        sockets.push(socket);
        return socket;
      }),
      reconnect: true,
      events: {
        connecting: (event) => {
          if (event.reconnect) connected?.disconnect(1000, "cancel reconnect");
        }
      }
    });

    try {
      const ready = client.connect().block();
      sockets[0]?.open();
      connected = await ready;

      sockets[0]?.close(1006, "network lost");
      await vi.advanceTimersByTimeAsync(60_000);

      expect(sockets).toHaveLength(1);
    } finally {
      connected?.disconnect();
      vi.useRealTimers();
    }
  });

  it("rejects reconnect readiness when connected listener disconnects synchronously", async () => {
    const browser = installBrowserSignals(true);
    const sockets: FakeWebSocket[] = [];
    let connected: { disconnect(code?: number, reason?: string): unknown } | undefined;
    let reconnectingResolve!: () => void;
    const reconnecting = new Promise<void>((resolve) => {
      reconnectingResolve = resolve;
    });
    const client = new RSocket("ws://localhost/rsocket", {
      setup: setupOptions(20_000, 90_000, () => {
        const socket = new FakeWebSocket();
        sockets.push(socket);
        return socket;
      }),
      reconnect: true,
      events: {
        reconnecting: reconnectingResolve,
        connected: (event) => {
          if (event.reconnect) connected?.disconnect(1000, "stop after connected");
        }
      }
    });

    try {
      const initialReady = client.connect().block();
      sockets[0]?.open();
      connected = await initialReady;

      sockets[0]?.close(1006, "network lost");
      await reconnecting;
      const reconnectReady = client.connect().block();
      const reconnectOutcome = reconnectReady.then(
        () => ({ resolved: true as const, error: undefined }),
        (error: unknown) => ({ resolved: false as const, error })
      );
      browser.dispatch("online");
      await waitFor(() => sockets.length === 2);
      sockets[1]?.open();

      const outcome = await reconnectOutcome;
      expect(outcome.resolved).toBe(false);
      expect((outcome.error as Error).message).toContain("stop after connected");
      expect(sockets[1]?.readyState).toBe(WS_CLOSED);
    } finally {
      connected?.disconnect();
      browser.restore();
    }
  });

  it("keeps wake listeners installed when closed listener starts a new connection", async () => {
    const browser = installBrowserSignals(true);
    const installedAdd = globalThis.addEventListener;
    const installedRemove = globalThis.removeEventListener;
    const listeners = new Map<string, Set<EventListener>>();
    const sockets: FakeWebSocket[] = [];
    let nextReady: Promise<{ disconnect(code?: number, reason?: string): unknown } | undefined> | undefined;
    let nextConnected: { disconnect(code?: number, reason?: string): unknown } | undefined;
    let client: RSocket;

    (globalThis as any).addEventListener = (type: string, listener: EventListener): void => {
      let typed = listeners.get(type);
      if (typed === undefined) listeners.set(type, typed = new Set());
      typed.add(listener);
      installedAdd(type, listener);
    };
    (globalThis as any).removeEventListener = (type: string, listener: EventListener): void => {
      listeners.get(type)?.delete(listener);
      installedRemove(type, listener);
    };

    try {
      client = new RSocket("ws://localhost/rsocket", {
        setup: setupOptions(20_000, 90_000, () => {
          const socket = new FakeWebSocket();
          sockets.push(socket);
          return socket;
        }),
        reconnect: false,
        events: {
          closed: () => {
            nextReady = client.connect().block();
          }
        }
      });
      const ready = client.connect().block();
      sockets[0]?.open();
      const initialConnected = (await ready)!;

      sockets[0]?.close(1006, "network lost");
      await waitFor(() => sockets.length === 2);
      sockets[1]?.open();
      nextConnected = await nextReady;

      expect([...listeners.values()].reduce((total, typed) => total + typed.size, 0)).toBe(3);
      initialConnected.disconnect();
    } finally {
      nextConnected?.disconnect();
      (globalThis as any).addEventListener = installedAdd;
      (globalThis as any).removeEventListener = installedRemove;
      browser.restore();
    }
  });

  it("keeps explicit disconnect idempotent on retained connected facades", async () => {
    const events: string[] = [];
    const { connected } = await connect({
      events: {
        disconnect: (event: any) => events.push(event.type)
      }
    });

    connected.disconnect(1000, "done");

    expect(() => connected.disconnect(1002, "already done")).not.toThrow();
    expect(events).toEqual(["disconnect"]);
  });

  it("keeps the active connection when disconnect receives an invalid close code", async () => {
    const { client, socket } = await connect();
    const connected = (await client.connect().block())!;

    expect(() => connected.disconnect(1002, "protocol")).toThrow("Invalid WebSocket close code");
    expect(socket.readyState).not.toBe(WS_CLOSED);

    connected.disconnect(1000, "done");
  });

  it("keeps disconnect idempotent when the underlying WebSocket close throws", async () => {
    class CloseThrowingWebSocket extends FakeWebSocket {
      /** Simulates a browser/custom transport close failure after local RSocket cleanup started. */
      override close(_code?: number, _reason?: string): void {
        throw new Error("close failed");
      }
    }

    const socket = new CloseThrowingWebSocket();
    const events: string[] = [];
    const client = new RSocket("ws://localhost/rsocket", {
      setup: setupOptions(20_000, 90_000, fakeWebSocketFactory(socket)),
      events: {
        disconnect: (event) => events.push(event.type)
      }
    });
    const ready = client.connect().block();
    socket.open();
    const connected = (await ready)!;

    expect(() => connected.disconnect(1000, "done")).not.toThrow();
    expect(events).toEqual(["disconnect"]);
  });

  it("reconnects when keepalive lifetime expires without a close event", async () => {
    vi.useFakeTimers();
    const sockets: FakeWebSocket[] = [];
    let client: RSocket | undefined;
    let connected: { disconnect(code?: number, reason?: string): unknown } | undefined;

    try {
      client = new RSocket("ws://localhost/rsocket", {
        setup: setupOptions(1_000, 10, () => {
          const socket = new FakeWebSocket();
          sockets.push(socket);
          return socket;
        }),
        reconnect: true
      });
      const ready = client.connect().block();
      sockets[0]?.open();
      connected = (await ready)!;

      await vi.advanceTimersByTimeAsync(11);
      await vi.advanceTimersByTimeAsync(0);

      expect(sockets).toHaveLength(2);

      sockets[1]?.open();
      connected = (await client.connect().block())!;

      expect(sockets[1]?.decodeSent(0, metadataMimeType, dataMimeType)).toBeInstanceOf(SetupFrame);
    } finally {
      connected?.disconnect();
      vi.useRealTimers();
    }
  });

  it("waits for responder REQUEST_N before sending channel payload frames after the initial frame", async () => {
    const { client, socket } = await connect();
    let subscription: Subscription | undefined;

    client
      .requestChannel([{ data: { n: 1 } }, { data: { n: 2 } }])
      .subscribe({
        onSubscribe(nextSubscription) {
          subscription = nextSubscription;
        },
        onNext() {},
        onError(error) {
          throw error;
        },
        onComplete() {}
      });

    subscription?.request(1);
    await flush();

    const request = socket.decodeSent(1, metadataMimeType, dataMimeType) as RequestChannelFrame;
    expect(request).toBeInstanceOf(RequestChannelFrame);
    expect(decodedPayload(request)).toEqual({ n: 1 });
    expect(socket.sent).toHaveLength(2);

    socket.serverSend(new RequestNFrame(request.header.streamId, 2));
    await flush();
    await flush();

    const next = socket.decodeSent(2, metadataMimeType, dataMimeType) as PayloadFrame;
    expect(next).toBeInstanceOf(PayloadFrame);
    expect(next.isNext()).toBe(true);
    expect(decodedPayload(next)).toEqual({ n: 2 });

    const complete = socket.decodeSent(3, metadataMimeType, dataMimeType) as PayloadFrame;
    expect(complete).toBeInstanceOf(PayloadFrame);
    expect(complete.isComplete()).toBe(true);
  });

  it("reports request-channel iterator construction failures through the stream", async () => {
    const { client, socket } = await connect();
    const errors: unknown[] = [];
    let subscription: Subscription | undefined;
    const input: Iterable<{ data: { n: number } }> = {
      [Symbol.iterator](): Iterator<{ data: { n: number } }> {
        throw new Error("iterator construction failed");
      }
    };

    client.requestChannel(input).subscribe({
      onSubscribe(nextSubscription) {
        subscription = nextSubscription;
      },
      onNext() {},
      onError(error) {
        errors.push(error);
      },
      onComplete() {}
    });
    subscription?.request(1);
    await waitFor(() => errors.length === 1);

    expect(errors[0]).toBeInstanceOf(Error);
    expect((errors[0] as Error).message).toBe("iterator construction failed");
    expect(socket.sent).toHaveLength(1);
  });

  it("sends channel application errors only after REQUEST_CHANNEL was established", async () => {
    const { client, socket } = await connect();
    const errors: unknown[] = [];
    let subscription: Subscription | undefined;
    let reads = 0;
    const input: Iterable<{ data: { n: number } }> = {
      [Symbol.iterator]() {
        return {
          next() {
            reads += 1;
            if (reads === 1) return { done: false, value: { data: { n: 1 } } };
            throw new Error("channel source failed");
          }
        };
      }
    };

    client.requestChannel(input).subscribe({
      onSubscribe(nextSubscription) {
        subscription = nextSubscription;
      },
      onNext() {},
      onError(error) {
        errors.push(error);
      },
      onComplete() {}
    });
    subscription?.request(1);
    await waitFor(() => socket.sent.length === 2);

    const request = socket.decodeSent(1, metadataMimeType, dataMimeType) as RequestChannelFrame;
    socket.serverSend(new RequestNFrame(request.header.streamId, 1));
    await waitFor(() => errors.length === 1 && socket.sent.length === 3);

    const frame = socket.decodeSent(2, metadataMimeType, dataMimeType) as ErrorFrame;
    expect(frame).toBeInstanceOf(ErrorFrame);
    expect(frame.code).toBe(FrameErrorCode.APPLICATION_ERROR);
    expect((errors[0] as Error).message).toBe("channel source failed");
  });

  it("does not return a naturally completed request-channel iterator", async () => {
    const { client, socket } = await connect();
    let subscription: Subscription | undefined;
    let returned = 0;
    const values = [{ data: { n: 1 } }, { data: { n: 2 } }];
    const input: Iterable<{ data: { n: number } }> = {
      [Symbol.iterator]() {
        return {
          next() {
            const value = values.shift();
            return value === undefined
              ? { done: true, value: undefined as never }
              : { done: false, value };
          },
          return() {
            returned += 1;
            return { done: true, value: undefined as never };
          }
        };
      }
    };

    client.requestChannel(input).subscribe({
      onSubscribe(nextSubscription) {
        subscription = nextSubscription;
      },
      onNext() {},
      onError(error) {
        throw error;
      },
      onComplete() {}
    });

    subscription?.request(1);
    await waitFor(() => socket.sent.length === 2);
    const request = socket.decodeSent(1, metadataMimeType, dataMimeType) as RequestChannelFrame;
    socket.serverSend(new RequestNFrame(request.header.streamId, 2));
    await waitFor(() => socket.sent.length === 4);
    await flush();

    expect(returned).toBe(0);
  });

  it("does not pull channel input again before responder demand arrives", async () => {
    const { client, socket } = await connect();
    let subscription: Subscription | undefined;
    let nextCalls = 0;
    const values = [
      { data: { n: 1 } },
      { data: { n: 2 } },
      { data: { n: 3 } }
    ];
    const input: Iterable<{ data: { n: number } }> = {
      [Symbol.iterator]() {
        return {
          next() {
            nextCalls += 1;
            const value = values.shift();
            return value === undefined
              ? { done: true, value: undefined as never }
              : { done: false, value };
          }
        };
      }
    };

    client
      .requestChannel(input)
      .subscribe({
        onSubscribe(nextSubscription) {
          subscription = nextSubscription;
        },
        onNext() {},
        onError(error) {
          throw error;
        },
        onComplete() {}
      });

    subscription?.request(1);
    await waitFor(() => socket.sent.length === 2);
    await flush();

    const request = socket.decodeSent(1, metadataMimeType, dataMimeType) as RequestChannelFrame;
    expect(decodedPayload(request)).toEqual({ n: 1 });
    expect(nextCalls).toBe(1);

    socket.serverSend(new RequestNFrame(request.header.streamId, 1));
    await waitFor(() => nextCalls >= 2);
    await waitFor(() => socket.sent.length === 3);

    const next = socket.decodeSent(2, metadataMimeType, dataMimeType) as PayloadFrame;
    expect(decodedPayload(next)).toEqual({ n: 2 });
    await flush();
    expect(nextCalls).toBe(3);
    expect(socket.sent).toHaveLength(3);

    socket.serverSend(new RequestNFrame(request.header.streamId, 1));
    await waitFor(() => socket.sent.length >= 4);
    expect(decodedPayload(socket.decodeSent(3, metadataMimeType, dataMimeType) as PayloadFrame)).toEqual({ n: 3 });
  });

  it("keeps at most one async channel item prefetched beyond responder demand", async () => {
    const { client, socket } = await connect();
    let subscription: Subscription | undefined;
    let nextCalls = 0;
    const values = [
      { data: { n: 1 } },
      { data: { n: 2 } },
      { data: { n: 3 } }
    ];
    const input: AsyncIterable<{ data: { n: number } }> = {
      [Symbol.asyncIterator]() {
        return {
          async next() {
            nextCalls += 1;
            const value = values.shift();
            return value === undefined
              ? { done: true, value: undefined as never }
              : { done: false, value };
          }
        };
      }
    };

    client
      .requestChannel(input)
      .subscribe({
        onSubscribe(nextSubscription) {
          subscription = nextSubscription;
        },
        onNext() {},
        onError(error) {
          throw error;
        },
        onComplete() {}
      });

    subscription?.request(1);
    await waitFor(() => socket.sent.length === 2);

    const request = socket.decodeSent(1, metadataMimeType, dataMimeType) as RequestChannelFrame;
    expect(decodedPayload(request)).toEqual({ n: 1 });
    expect(nextCalls).toBe(1);

    socket.serverSend(new RequestNFrame(request.header.streamId, 1));
    await waitFor(() => socket.sent.length === 3);
    expect(decodedPayload(socket.decodeSent(2, metadataMimeType, dataMimeType) as PayloadFrame)).toEqual({ n: 2 });
    await flush();
    await flush();

    expect(nextCalls).toBe(3);
    expect(socket.sent).toHaveLength(3);

    socket.serverSend(new RequestNFrame(request.header.streamId, 1));
    await waitFor(() => socket.sent.length === 5);
    expect(decodedPayload(socket.decodeSent(3, metadataMimeType, dataMimeType) as PayloadFrame)).toEqual({ n: 3 });
    expect((socket.decodeSent(4, metadataMimeType, dataMimeType) as PayloadFrame).isComplete()).toBe(true);
    expect(nextCalls).toBe(4);
  });

  it("preserves channel completion prefetch through metadata-wrapped Flux input", async () => {
    const { client, socket } = await connect();
    let subscription: Subscription | undefined;
    client.metadataUpdate(WellKnownMimeType.TEXT_PLAIN.toMetadata("Bearer channel-token"));

    client
      .requestChannel(Flux.fromArray([{ data: { n: 1 } }, { data: { n: 2 } }]))
      .subscribe({
        onSubscribe(nextSubscription) {
          subscription = nextSubscription;
        },
        onNext() {},
        onError(error) {
          throw error;
        },
        onComplete() {}
      });

    subscription?.request(1);
    await waitFor(() => socket.sent.length === 2);

    const request = socket.decodeSent(1, metadataMimeType, dataMimeType) as RequestChannelFrame;
    expect(decodedPayload(request)).toEqual({ n: 1 });

    socket.serverSend(new RequestNFrame(request.header.streamId, 1));
    await waitFor(() => socket.sent.length >= 4);

    expect(decodedPayload(socket.decodeSent(2, metadataMimeType, dataMimeType) as PayloadFrame)).toEqual({ n: 2 });
    expect((socket.decodeSent(3, metadataMimeType, dataMimeType) as PayloadFrame).isComplete()).toBe(true);
  });

  it("does not lose responder demand that arrives before channel outbound awaits it", async () => {
    const { client, socket } = await connect();
    let subscription: Subscription | undefined;
    let responded = false;

    socket.onSend = () => {
      const sentIndex = socket.sent.length - 1;
      const frame = socket.decodeSent(sentIndex, metadataMimeType, dataMimeType);
      if (responded || !(frame instanceof RequestChannelFrame)) return;
      responded = true;
      socket.serverSend(new RequestNFrame(frame.header.streamId, 1));
    };

    client
      .requestChannel(Flux.fromArray([{ data: { n: 1 } }, { data: { n: 2 } }]))
      .subscribe({
        onSubscribe(nextSubscription) {
          subscription = nextSubscription;
        },
        onNext() {},
        onError(error) {
          throw error;
        },
        onComplete() {}
      });

    subscription?.request(1);
    await waitFor(() => socket.sent.length >= 3);

    const next = socket.decodeSent(2, metadataMimeType, dataMimeType) as PayloadFrame;
    expect(next).toBeInstanceOf(PayloadFrame);
    expect(decodedPayload(next)).toEqual({ n: 2 });
  });

  it("closes the connection on invalid responder REQUEST_N", async () => {
    const { client, socket } = await connect({ reconnect: false });
    let subscription: Subscription | undefined;
    const errors: unknown[] = [];

    client
      .requestChannel(Flux.fromArray([{ data: { n: 1 } }, { data: { n: 2 } }]))
      .subscribe({
        onSubscribe(nextSubscription) {
          subscription = nextSubscription;
        },
        onNext() {},
        onError(error) {
          errors.push(error);
        },
        onComplete() {}
      });

    subscription?.request(1);
    await flush();

    const request = socket.decodeSent(1, metadataMimeType, dataMimeType) as RequestChannelFrame;
    const invalidRequestN = new RequestNFrame(request.header.streamId, 1).toUint8Array().slice();
    invalidRequestN.fill(0, 6);
    socket.dispatchMessage(invalidRequestN);
    await waitFor(() => socket.readyState === WS_CLOSED);

    const error = socket.decodeSent(2, metadataMimeType, dataMimeType) as ErrorFrame;
    expect(error).toBeInstanceOf(ErrorFrame);
    expect(error.header.streamId).toBe(0);
    expect(errors).toHaveLength(1);
  });

  it("uses positional request-channel metadata in the initial frame", async () => {
    const { client, socket } = await connect();
    const route = WellKnownMimeType.MESSAGE_RSOCKET_ROUTING.toMetadata(["chat.direct"]);
    let subscription: Subscription | undefined;

    client.requestChannel([{ n: 1 }], route).subscribe({
      onSubscribe(nextSubscription) {
        subscription = nextSubscription;
      },
      onNext() {},
      onError(error) {
        throw error;
      },
      onComplete() {}
    });
    subscription?.request(1);
    await flush();

    const initial = socket.decodeSent(1, metadataMimeType, dataMimeType) as RequestChannelFrame;
    const initialMetadata = (initial as unknown as { metadata: Metadata<Metadata<any>[]> }).metadata;
    expect(decodedPayload(initial)).toBeUndefined();
    expect(initialMetadata.payload).toEqual([route]);

    socket.serverSend(new RequestNFrame(initial.header.streamId, 1));
    await flush();
    await flush();

    const next = socket.decodeSent(2, metadataMimeType, dataMimeType) as PayloadFrame;
    expect(decodedPayload(next)).toEqual({ n: 1 });
  });

  it("processes declarative request-channel controllers with a route initial frame", async () => {
    /** Routed chat channel used to verify its initial request frame. */
    class ChatMessagesController extends RequestChannelController<
      { room: string; text: string },
      { delivered: boolean }
    > {
      /** Route consumed by the chat responder. */
      protected readonly route = "chat.messages";
    }

    const { client, socket } = await connect();
    let subscription: Subscription | undefined;

    client.process(
      ChatMessagesController,
      [{ data: { room: "general", text: "hello" } }]
    ).subscribe({
      onSubscribe(nextSubscription) {
        subscription = nextSubscription;
      },
      onNext() {},
      onError(error) {
        throw error;
      },
      onComplete() {}
    });

    subscription?.request(1);
    await flush();

    const initial = socket.decodeSent(1, metadataMimeType, dataMimeType) as RequestChannelFrame;
    expect(initial).toBeInstanceOf(RequestChannelFrame);
    expect(decodedPayload(initial)).toBeUndefined();

    socket.serverSend(new RequestNFrame(initial.header.streamId, 1));
    await flush();
    await flush();

    const next = socket.decodeSent(2, metadataMimeType, dataMimeType) as PayloadFrame;
    expect(next).toBeInstanceOf(PayloadFrame);
    expect(decodedPayload(next)).toEqual({ room: "general", text: "hello" });
  });

  it("keeps routed request-channel iterable input lazy until responder demand", async () => {
    /** Routed chat channel used to verify lazy iterable consumption. */
    class LazyChatController extends RequestChannelController<
      { room: string; text: string },
      { delivered: boolean }
    > {
      /** Route consumed by the lazy chat responder. */
      protected readonly route = "chat.lazy";
    }

    const { client, socket } = await connect();
    const values = [{ data: { room: "general", text: "hello" } }];
    let nextCalls = 0;
    const input: Iterable<{ data: { room: string; text: string } }> = {
      [Symbol.iterator]() {
        return {
          next() {
            nextCalls += 1;
            const value = values.shift();
            return value === undefined
              ? { done: true, value: undefined as never }
              : { done: false, value };
          }
        };
      }
    };
    let subscription: Subscription | undefined;

    client.process(LazyChatController, input).subscribe({
      onSubscribe(nextSubscription) {
        subscription = nextSubscription;
      },
      onNext() {},
      onError(error) {
        throw error;
      },
      onComplete() {}
    });

    subscription?.request(1);
    await waitFor(() => socket.sent.length === 2);

    const initial = socket.decodeSent(1, metadataMimeType, dataMimeType) as RequestChannelFrame;
    expect(initial).toBeInstanceOf(RequestChannelFrame);
    expect(decodedPayload(initial)).toBeUndefined();
    expect(nextCalls).toBe(0);

    socket.serverSend(new RequestNFrame(initial.header.streamId, 1));
    await waitFor(() => socket.sent.length >= 4);

    const next = socket.decodeSent(2, metadataMimeType, dataMimeType) as PayloadFrame;
    expect(next).toBeInstanceOf(PayloadFrame);
    expect(decodedPayload(next)).toEqual({ room: "general", text: "hello" });
    expect(nextCalls).toBe(2);
    expect((socket.decodeSent(3, metadataMimeType, dataMimeType) as PayloadFrame).isComplete()).toBe(true);
  });

  it("preserves channel completion prefetch through routed Flux input", async () => {
    /** Routed chat channel used to verify Flux completion prefetch. */
    class PrefetchChatController extends RequestChannelController<
      { room: string; text: string },
      { delivered: boolean }
    > {
      /** Route consumed by the prefetch chat responder. */
      protected readonly route = "chat.prefetch";
    }

    const { client, socket } = await connect();
    let subscription: Subscription | undefined;

    client.process(
      PrefetchChatController,
      Flux.fromArray([{ data: { room: "general", text: "hello" } }])
    ).subscribe({
      onSubscribe(nextSubscription) {
        subscription = nextSubscription;
      },
      onNext() {},
      onError(error) {
        throw error;
      },
      onComplete() {}
    });

    subscription?.request(1);
    await waitFor(() => socket.sent.length === 2);

    const initial = socket.decodeSent(1, metadataMimeType, dataMimeType) as RequestChannelFrame;
    expect(initial).toBeInstanceOf(RequestChannelFrame);
    expect(decodedPayload(initial)).toBeUndefined();

    socket.serverSend(new RequestNFrame(initial.header.streamId, 1));
    await waitFor(() => socket.sent.length >= 4);

    const next = socket.decodeSent(2, metadataMimeType, dataMimeType) as PayloadFrame;
    expect(decodedPayload(next)).toEqual({ room: "general", text: "hello" });
    expect((socket.decodeSent(3, metadataMimeType, dataMimeType) as PayloadFrame).isComplete()).toBe(true);
  });

  it("creates a sink-style request channel when no publisher is passed", async () => {
    const { client, socket } = await connect();
    const channel = client.requestChannel();
    let subscription: Subscription | undefined;

    channel.subscribe({
      onSubscribe(nextSubscription) {
        subscription = nextSubscription;
      },
      onNext() {},
      onError(error) {
        throw error;
      },
      onComplete() {}
    });

    channel.next({ data: { n: 1 } });
    subscription?.request(1);
    await flush();

    const request = socket.decodeSent(1, metadataMimeType, dataMimeType) as RequestChannelFrame;
    expect(request).toBeInstanceOf(RequestChannelFrame);
    expect(decodedPayload(request)).toEqual({ n: 1 });

    socket.serverSend(new RequestNFrame(request.header.streamId, 1));
    channel.sink.next({ data: { n: 2 } });
    channel.complete();
    await flush();
    await flush();

    const next = socket.decodeSent(2, metadataMimeType, dataMimeType) as PayloadFrame;
    expect(next).toBeInstanceOf(PayloadFrame);
    expect(decodedPayload(next)).toEqual({ n: 2 });
    expect((socket.decodeSent(3, metadataMimeType, dataMimeType) as PayloadFrame).isComplete()).toBe(true);
  });

  it("rejects sink-style channel writes after response cancellation releases outbound input", async () => {
    const { client, socket } = await connect();
    const channel = client.requestChannel();
    let subscription: Subscription | undefined;
    channel.subscribe({
      onSubscribe(value) {
        subscription = value;
      },
      onNext() {},
      onError() {},
      onComplete() {}
    });

    channel.next({ data: { n: 1 } });
    subscription?.request(1);
    await waitFor(() => socket.sent.length === 2);
    subscription?.cancel();
    await flush();

    expect(() => channel.next({ data: { n: 2 } })).toThrow("outbound side is closed");
    expect(() => channel.complete()).toThrow("outbound side is closed");
  });

  it("creates a sink-style request channel when no source is passed", async () => {
    const { client, socket } = await connect();
    const channel = client.requestChannel();
    let subscription: Subscription | undefined;

    channel.subscribe({
      onSubscribe(nextSubscription) {
        subscription = nextSubscription;
      },
      onNext() {},
      onError(error) {
        throw error;
      },
      onComplete() {}
    });

    channel.next({ data: { n: 1 } });
    subscription?.request(1);
    await flush();

    const request = socket.decodeSent(1, metadataMimeType, dataMimeType) as RequestChannelFrame;
    expect(request).toBeInstanceOf(RequestChannelFrame);
    expect(decodedPayload(request)).toEqual({ n: 1 });
  });

  it("returns a pending request-channel publisher when the subscription is cancelled", async () => {
    const { client } = await connect();
    let subscription: Subscription | undefined;
    let returned = false;
    const input: AsyncIterable<{ data: { n: number } }> = {
      [Symbol.asyncIterator]() {
        return {
          next: () => new Promise<IteratorResult<{ data: { n: number } }>>(() => undefined),
          return: async () => {
            returned = true;
            return { done: true, value: undefined };
          }
        };
      }
    };

    client.requestChannel(input).subscribe({
      onSubscribe(nextSubscription) {
        subscription = nextSubscription;
      },
      onNext() {},
      onError(error) {
        throw error;
      },
      onComplete() {}
    });

    subscription?.request(1);
    await flush();
    subscription?.cancel();

    await waitFor(() => returned);
  });

  it("returns a request-channel publisher only once when cancellation wakes demand", async () => {
    const { client, socket } = await connect();
    let subscription: Subscription | undefined;
    let returned = 0;
    const values = [{ data: { n: 1 } }, { data: { n: 2 } }];
    const input: Iterable<{ data: { n: number } }> = {
      [Symbol.iterator]() {
        return {
          next() {
            const value = values.shift();
            return value === undefined
              ? { done: true, value: undefined as never }
              : { done: false, value };
          },
          return() {
            returned += 1;
            return { done: true, value: undefined as never };
          }
        };
      }
    };

    client.requestChannel(input).subscribe({
      onSubscribe(nextSubscription) {
        subscription = nextSubscription;
      },
      onNext() {},
      onError(error) {
        throw error;
      },
      onComplete() {}
    });

    subscription?.request(1);
    await waitFor(() => socket.sent.length === 2);
    subscription?.cancel();
    await flush();

    expect(returned).toBe(1);
  });

  it("cancels a request-channel outbound publisher after the responder completes first", async () => {
    const { client, socket } = await connect();
    let subscription: Subscription | undefined;
    let completed = false;
    let returned = false;
    let index = 0;
    const input: Iterable<{ data: { n: number } }> = {
      [Symbol.iterator]() {
        return {
          next() {
            index += 1;
            return { done: false, value: { data: { n: index } } };
          },
          return() {
            returned = true;
            return { done: true, value: undefined as never };
          }
        };
      }
    };

    client.requestChannel(input).subscribe({
      onSubscribe(nextSubscription) {
        subscription = nextSubscription;
      },
      onNext() {},
      onError(error) {
        throw error;
      },
      onComplete() {
        completed = true;
      }
    });

    subscription?.request(1);
    await waitFor(() => socket.sent.length === 2);
    const request = socket.decodeSent(1, metadataMimeType, dataMimeType) as RequestChannelFrame;
    expect(request).toBeInstanceOf(RequestChannelFrame);

    socket.serverSend(new PayloadFrame(request.header.streamId, PayloadFlag.COMPLETE));
    await waitFor(() => completed);
    subscription?.cancel();
    await waitFor(() => returned);

    expect(socket.decodeSent(2, metadataMimeType, dataMimeType)).toBeInstanceOf(CancelFrame);
  });

});

/**
 * Opens a fake WebSocket-backed `RSocket` with test MIME defaults.
 */
async function connect(options: Record<string, unknown> = {}) {
  const socket = new FakeWebSocket();
  const client = new RSocket("ws://localhost/rsocket", {
    setup: setupOptions(20_000, 90_000, fakeWebSocketFactory(socket)),
    ...options
  });
  const ready = client.connect().block();
  socket.open();
  const connected = (await ready)!;
  return { client, connected, socket };
}

/** Rewrites the wire stream ID so invalid peer frames bypass strict constructors. */
function frameWithStreamId(frame: Frame, streamId: number): Uint8Array {
  const bytes = frame.toUint8Array().slice();
  new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength).setUint32(0, streamId, false);
  return bytes;
}

/** Reads the application value from a MIME-typed decoded frame payload. */
function decodedPayload<T>(frame: { readonly payload?: Payload<T> | undefined }): T | undefined {
  return frame.payload?.payload;
}

/**
 * Extracts raw bytes from a decoded payload or metadata object.
 */
function payloadBytesOf(part: unknown): Uint8Array | undefined {
  if (part === undefined || part === null) return undefined;
  if (part instanceof Uint8Array) return part;
  if (typeof part === "object" && "toUint8Array" in part && typeof part.toUint8Array === "function") {
    const bytes = part.toUint8Array();
    if (bytes instanceof Uint8Array) return bytes;
  }
  if (typeof part === "object" && "payload" in part && part.payload instanceof Uint8Array) {
    return part.payload;
  }
  return undefined;
}

/** Extracts logical values while asserting the composite metadata entry contract. */
function compositePayloads(metadata: unknown): unknown[] {
  if (!(metadata instanceof Metadata) ||
      metadata.mimeType !== WellKnownMimeType.MESSAGE_RSOCKET_COMPOSITE_METADATA ||
      !Array.isArray(metadata.payload)) {
    throw new TypeError("Expected decoded RSocket composite metadata");
  }
  return metadata.payload.map((entry) => {
    if (!(entry instanceof Metadata)) throw new TypeError("Expected a typed composite metadata entry");
    return entry.payload;
  });
}

/**
 * Waits one macrotask so Reactor callbacks and fake WebSocket events can run.
 */
function flush(): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, 0));
}

/**
 * Polls until an asynchronous test condition becomes true.
 */
async function waitFor(predicate: () => boolean): Promise<void> {
  for (let attempt = 0; attempt < 20; attempt += 1) {
    if (predicate()) return;
    await flush();
  }
  throw new Error("Timed out waiting for condition");
}
