/** Deterministic unit tests for transport-neutral reconnect primitives. */
import {describe, expect, it, vi} from "vitest";
import {reconnectDelay} from "@/reconnect/backoff.js";
import {deferred} from "@/reconnect/deferred.js";
import {RSocketFlux} from "@/stream/session.js";
import type {RSocketPayloadFrame} from "rsocket-core-ts";
import {
  connectionEvent,
  RSocketEventHub,
  type RSocketConnectionEvent,
  type RSocketConnectionEventDraft
} from "@/reconnect/events.js";
import {
  normalizeReconnectOptions,
  RECONNECT_MIN_UPTIME_MS
} from "@/reconnect/options.js";

describe("client reconnect policy", () => {
  it("uses an immediate first retry followed by capped exponential backoff", () => {
    expect([
      reconnectDelay(-1),
      reconnectDelay(0),
      reconnectDelay(1),
      reconnectDelay(2),
      reconnectDelay(3),
      reconnectDelay(4),
      reconnectDelay(100)
    ]).toEqual([0, 0, 0, 3_000, 3_900, 5_070, 10_000]);
  });

  it("enables reconnect only when explicitly configured", () => {
    expect(normalizeReconnectOptions({})).toEqual({enabled: false});
    expect(normalizeReconnectOptions({reconnect: true})).toEqual({enabled: true});
    expect(normalizeReconnectOptions({reconnect: {resume: {ttl: 30_000}}})).toEqual({enabled: true});
    expect(normalizeReconnectOptions({reconnect: false})).toEqual({enabled: false});
    expect(RECONNECT_MIN_UPTIME_MS).toBe(5_000);
  });
});

describe("client deferred readiness", () => {
  it("resolves exactly once", async () => {
    const ready = deferred<number>();

    expect(ready.settled).toBe(false);
    ready.resolve(1);
    ready.resolve(2);
    ready.reject(new Error("late"));

    await expect(ready.mono().block()).resolves.toBe(1);
    await expect(ready.wait()).resolves.toBe(1);
    expect(ready.settled).toBe(true);
  });

  it("rejects exactly once", async () => {
    const ready = deferred<number>();
    const failure = new Error("failed");

    ready.reject(failure);
    ready.resolve(1);
    ready.reject(new Error("late"));

    await expect(ready.mono().block()).rejects.toBe(failure);
    await expect(ready.wait()).rejects.toBe(failure);
    expect(ready.settled).toBe(true);
  });

  it("detaches cancelled Mono and Promise waiters", async () => {
    const ready = deferred<number>();
    const monoNext = vi.fn();
    const monoError = vi.fn();
    const subscription = ready.mono().subscribe(monoNext, monoError);
    const abortController = new AbortController();
    const waiting = ready.wait(abortController.signal);

    subscription.dispose();
    abortController.abort(new Error("cancelled"));
    ready.resolve(1);

    await expect(waiting).rejects.toThrow("cancelled");
    expect(monoNext).not.toHaveBeenCalled();
    expect(monoError).not.toHaveBeenCalled();
  });

  it("cleans up a Promise waiter when abort-listener registration fails", async () => {
    const ready = deferred<number>();
    const failure = new Error("listener registration failed");
    const removeEventListener = vi.fn();
    const signal = {
      aborted: false,
      addEventListener: () => {
        throw failure;
      },
      removeEventListener
    } as unknown as AbortSignal;

    await expect(ready.wait(signal)).rejects.toBe(failure);
    expect(removeEventListener).toHaveBeenCalledOnce();

    ready.resolve(1);
    await expect(ready.wait()).resolves.toBe(1);
  });

  it("aborts unresolved stream source selection on cancellation", () => {
    let readinessSignal: AbortSignal | undefined;
    const source = RSocketFlux.deferSource((signal) => {
      readinessSignal = signal;
      return new Promise<{source: RSocketFlux<RSocketPayloadFrame>}>(() => undefined);
    });

    const subscription = source.subscribe(() => undefined);
    expect(readinessSignal?.aborted).toBe(false);

    subscription.dispose();
    expect(readinessSignal?.aborted).toBe(true);
  });

  it("fails deferred streams when replaying demand throws", async () => {
    const failure = new Error("request failed");
    const cancel = vi.fn();
    const source = RSocketFlux.deferSource(() => Promise.resolve({
      source: new RSocketFlux(() => ({
        request() {
          throw failure;
        },
        cancel
      }))
    }));

    await expect(source.next().block()).rejects.toBe(failure);
    expect(cancel).toHaveBeenCalledOnce();
  });

  it("aborts readiness work when deferred source creation throws", async () => {
    const failure = new Error("source failed");
    let readinessSignal: AbortSignal | undefined;
    const source = RSocketFlux.deferSource((signal) => {
      readinessSignal = signal;
      throw failure;
    });

    await expect(source.next().block()).rejects.toBe(failure);
    expect(readinessSignal?.aborted).toBe(true);
  });

  it("delivers errors thrown while reading a resolved deferred source", async () => {
    const failure = new Error("resolved source is unavailable");
    const source = RSocketFlux.deferSource(() => Promise.resolve({
      get source(): RSocketFlux<RSocketPayloadFrame> {
        throw failure;
      }
    }));

    await expect(source.next().block()).rejects.toBe(failure);
  });
});

describe("client lifecycle events", () => {
  it.each(eventCases())("derives UI state for $draft.type ($name)", ({draft, expected}) => {
    expect(connectionEvent(draft)).toMatchObject(expected);
  });

  it("delivers a stable listener snapshot and applies mutations to later events", () => {
    const hub = new RSocketEventHub();
    const calls: string[] = [];
    const third = () => calls.push("third");
    let removeSecond: () => void = () => undefined;

    hub.on("connected", () => {
      calls.push("first");
      removeSecond();
      hub.on("connected", third);
    });
    removeSecond = hub.on("connected", () => calls.push("second"));

    hub.emit(connectedEvent());
    expect(calls).toEqual(["first", "second"]);

    calls.length = 0;
    hub.emit(connectedEvent());
    expect(calls).toEqual(["first", "third"]);
  });

  it("isolates listener failures and removes empty listener sets", () => {
    const hub = new RSocketEventHub();
    const healthy = vi.fn();
    const failing = () => {
      throw new Error("listener failed");
    };
    const removeFailing = hub.on("connected", failing);
    const removeHealthy = hub.on("connected", healthy);

    expect(hub.has("connected")).toBe(true);
    expect(() => hub.emit(connectedEvent())).not.toThrow();
    expect(healthy).toHaveBeenCalledTimes(1);

    removeFailing();
    removeHealthy();
    removeHealthy();
    expect(hub.has("connected")).toBe(false);
    expect(() => hub.emit(connectedEvent())).not.toThrow();
  });

  it("deduplicates the same listener function", () => {
    const hub = new RSocketEventHub();
    const listener = vi.fn();
    hub.on("connected", listener);
    hub.on("connected", listener);

    hub.emit(connectedEvent());

    expect(listener).toHaveBeenCalledTimes(1);
  });
});

/** Complete lifecycle-state matrix used by the event derivation test. */
function eventCases(): ReadonlyArray<{
  readonly name: string;
  readonly draft: RSocketConnectionEventDraft;
  readonly expected: Partial<RSocketConnectionEvent>;
}> {
  return [
    eventCase("initial", {type: "connecting", attempt: 0, reconnect: false}, {
      status: "connecting", connected: false, recovering: false, message: "Opening RSocket connection"
    }),
    eventCase("retry", {type: "connecting", attempt: 2, reconnect: true}, {
      status: "reconnecting", connected: false, recovering: true, message: "Restoring RSocket connection"
    }),
    eventCase("restored", {type: "connected", attempt: 2, reconnect: true}, {
      status: "connected", connected: true, recovering: false, message: "RSocket connection restored"
    }),
    eventCase("interrupted", {
      type: "disconnect", attempt: 1, reconnect: true, willReconnect: true
    }, {
      status: "disconnected", connected: false, recovering: false, message: "RSocket connection interrupted"
    }),
    eventCase("scheduled", {
      type: "reconnecting", attempt: 2, reconnect: true, delayMs: 3_000
    }, {
      status: "reconnecting", connected: false, recovering: true,
      message: "Restoring RSocket connection in 3000ms"
    }),
    eventCase("resume fallback", {type: "resumeRejected", attempt: 2, reconnect: true}, {
      status: "reconnecting", connected: false, recovering: true,
      message: "RSocket resume rejected; opening a fresh connection"
    }),
    eventCase("retry failed", {
      type: "reconnectFailed", attempt: 2, reconnect: true, willReconnect: true
    }, {
      status: "reconnecting", connected: false, recovering: true,
      message: "RSocket reconnect attempt failed"
    }),
    eventCase("exhausted", {
      type: "reconnectFailed", attempt: 3, reconnect: true, willReconnect: false
    }, {
      status: "closed", connected: false, recovering: false,
      message: "RSocket reconnect attempts exhausted"
    }),
    eventCase("closed", {type: "closed", attempt: 0, reconnect: false}, {
      status: "closed", connected: false, recovering: false, message: "RSocket connection closed"
    })
  ];
}

/** Preserves discriminated draft types while pairing expected UI fields. */
function eventCase(
  name: string,
  draft: RSocketConnectionEventDraft,
  expected: Partial<RSocketConnectionEvent>
) {
  return {name, draft, expected};
}

/** Creates the event used by event-hub delivery tests. */
function connectedEvent(): RSocketConnectionEvent {
  return connectionEvent({type: "connected", attempt: 0, reconnect: false});
}
