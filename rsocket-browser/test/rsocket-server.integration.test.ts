/** Browser requester conformance against the production TypeScript responder. */
import {Flux, type Subscription} from "reactor-core-ts";
import {
  FrameErrorCode,
  FrameType,
  Metadata,
  PayloadFlag,
  WellKnownAuthType,
  WellKnownMimeType
} from "rsocket-frames-ts";
import {route} from "rsocket-core-ts";
import {describe, expect, it} from "vitest";
import {
  RequestChannelController,
  RequestResponseController,
  RequestStreamController
} from "@";
import {installBrowserSignals} from "./browser-signals.js";
import {
  type ChannelResponse,
  type ChannelValue,
  type DelayedRequest,
  type EchoValue,
  type NumberResponse,
  type NumbersRequest,
  testServerControllers
} from "./rsocket-server-controllers.js";
import {openRSocketServerFixture} from "./rsocket-server-fixture.js";
import {
  fragmentSequences,
  hasFollows,
  isNextSequence,
  observedRSocketFrames,
  type ObservedRSocketFrame
} from "./rsocket-wire.js";

const FRAGMENT_SIZE = 256;

/** Typed client declaration for the server echo route. */
class EchoController extends RequestResponseController<EchoValue, EchoValue> {
  protected readonly route = "echo";
}

/** Typed client declaration for the finite number stream. */
class NumbersController extends RequestStreamController<NumbersRequest, NumberResponse> {
  protected readonly route = "numbers";
}

/** Typed client declaration for the bidirectional channel. */
class ChannelController extends RequestChannelController<ChannelValue, ChannelResponse> {
  protected readonly route = "channel";
}

/** Timed client declaration used to verify request-response cancellation. */
class TimedController extends RequestResponseController<DelayedRequest, EchoValue> {
  protected readonly route = "delay";

  /** Creates a request with a timeout shorter than the test server delay. */
  constructor() {
    super({timeout: 10});
  }
}

describe("RSocket browser integration with rsocket-server-ts", () => {
  it("runs direct interactions, declarative controllers, and metadata-push", async () => {
    const observations = {fireAndForget: [] as number[]};
    const pushed: Metadata<any>[] = [];
    let setupCount = 0;
    const fixture = await openRSocketServerFixture({
      controllers: testServerControllers(observations),
      accept() {
        setupCount += 1;
      },
      metadataPush(metadata) {
        pushed.push(metadata);
      }
    });

    try {
      const direct = await fixture.connection
        .requestResponse({value: "direct"}, route("echo"))
        .block();
      const processed = await fixture.connection
        .process(EchoController, {value: "controller"})
        .block();
      await fixture.connection.fireAndForget(7, route("record")).block();
      await fixture.connection
        .metadataPush(WellKnownMimeType.TEXT_PLAIN.toMetadata("integration-metadata"))
        .block();

      await waitFor(() => observations.fireAndForget.length === 1 && pushed.length === 1);
      expect(direct?.data).toEqual({value: "direct"});
      expect(processed).toEqual({value: "controller"});
      expect(observations.fireAndForget).toEqual([7]);
      expect(metadataEntries(pushed[0]).some((entry) =>
        entry.mimeType.mimeType === WellKnownMimeType.TEXT_PLAIN.mimeType &&
        entry.payload === "integration-metadata"
      )).toBe(true);
      expect(setupCount).toBe(1);
    } finally {
      await fixture.close();
    }
  });

  it("propagates timeout and ERROR signals without closing the connection", async () => {
    const fixture = await openRSocketServerFixture({
      controllers: testServerControllers({fireAndForget: []})
    });

    try {
      await expect(fixture.connection
        .process(new TimedController(), {delayMs: 40, value: "slow"})
        .block()).rejects.toThrow(/timed out/i);
      await expect(fixture.connection
        .requestResponse(undefined, route("missing"))
        .block()).rejects.toMatchObject({code: FrameErrorCode.REJECTED});
      await expect(fixture.connection
        .requestResponse({value: "still-open"}, route("echo"))
        .block()).resolves.toMatchObject({data: {value: "still-open"}});
      await delay(50);
    } finally {
      await fixture.close();
    }
  });

  it("maps request-stream demand and server failures to the response publisher", async () => {
    const fixture = await openRSocketServerFixture({
      controllers: testServerControllers({fireAndForget: []})
    });
    const values: NumberResponse[] = [];
    const failures: unknown[] = [];
    let completed = false;
    let subscription: Subscription | undefined;

    try {
      fixture.connection.process(NumbersController, {count: 4}).subscribe({
        onSubscribe(next) {
          subscription = next;
          next.request(1);
        },
        onNext(value) {
          values.push(value);
        },
        onError(error) {
          failures.push(error);
        },
        onComplete() {
          completed = true;
        }
      });

      await waitFor(() => values.length === 1);
      await delay(20);
      expect(values).toEqual([{n: 1}]);
      expect(completed).toBe(false);

      subscription?.request(3);
      await waitFor(() => completed);
      expect(values).toEqual([{n: 1}, {n: 2}, {n: 3}, {n: 4}]);
      expect(failures).toEqual([]);

      const errorValues: number[] = [];
      const streamErrors: unknown[] = [];
      fixture.connection.requestStream(undefined, route("stream-error")).subscribe({
        onSubscribe(next) {
          next.request(2);
        },
        onNext(payload) {
          errorValues.push((payload.data as NumberResponse).n);
        },
        onError(error) {
          streamErrors.push(error);
        },
        onComplete() {
          throw new Error("Failing stream completed instead of emitting ERROR");
        }
      });

      await waitFor(() => streamErrors.length === 1);
      expect(errorValues).toEqual([1]);
      expect((streamErrors[0] as Error).message).toContain("stream boom");
      await expect(fixture.connection
        .requestResponse({value: "after-error"}, route("echo"))
        .block()).resolves.toMatchObject({data: {value: "after-error"}});
    } finally {
      subscription?.cancel();
      await fixture.close();
    }
  });

  it("preserves independent demand for publisher and sink request channels", async () => {
    const fixture = await openRSocketServerFixture({
      controllers: testServerControllers({fireAndForget: []})
    });
    const values: ChannelResponse[] = [];
    const failures: unknown[] = [];
    let completed = false;
    let subscription: Subscription | undefined;

    try {
      fixture.connection.process(ChannelController, Flux.fromArray([
        {data: {value: "a"}},
        {data: {value: "b"}},
        {data: {value: "c"}}
      ])).subscribe({
        onSubscribe(next) {
          subscription = next;
          next.request(1);
        },
        onNext(value) {
          values.push(value);
        },
        onError(error) {
          failures.push(error);
        },
        onComplete() {
          completed = true;
        }
      });

      await waitFor(() => values.length === 1);
      await delay(20);
      expect(values).toEqual([{kind: "channel", value: "a"}]);
      expect(completed).toBe(false);

      subscription?.request(2);
      await waitFor(() => completed);
      expect(values).toEqual([
        {kind: "channel", value: "a"},
        {kind: "channel", value: "b"},
        {kind: "channel", value: "c"}
      ]);
      expect(failures).toEqual([]);

      const channel = fixture.connection.requestChannel(undefined, route("channel"));
      const sinkValues: ChannelResponse[] = [];
      let sinkCompleted = false;
      channel.subscribe({
        onSubscribe(next) {
          next.request(2);
        },
        onNext(payload) {
          sinkValues.push(payload.data as ChannelResponse);
        },
        onError(error) {
          failures.push(error);
        },
        onComplete() {
          sinkCompleted = true;
        }
      });
      channel.next({data: {value: "sink-a"}});
      channel.next({data: {value: "sink-b"}});
      channel.complete();

      await waitFor(() => sinkCompleted);
      expect(sinkValues).toEqual([
        {kind: "channel", value: "sink-a"},
        {kind: "channel", value: "sink-b"}
      ]);
      expect(failures).toEqual([]);
    } finally {
      subscription?.cancel();
      await fixture.close();
    }
  });

  it("fragments routed request-response payloads in both directions", async () => {
    const fixture = await fragmentedFixture();
    const value = patternedText(8_000, "request-response");

    try {
      await expect(fixture.connection
        .requestResponse({value}, route("echo"))
        .block()).resolves.toMatchObject({data: {value}});

      const pair = fixture.pairs[0];
      if (pair === undefined) throw new Error("Missing WebSocket pair");
      const sent = observedRSocketFrames(pair.client.sent);
      const initial = sent.find((frame) => frame.type === FrameType.REQUEST_RESPONSE);
      if (initial === undefined) throw new Error("Missing REQUEST_RESPONSE frame");
      const requests = sent.filter((frame) => frame.streamId === initial.streamId &&
        (frame.type === FrameType.REQUEST_RESPONSE || frame.type === FrameType.PAYLOAD));
      const responses = observedRSocketFrames(pair.server.sent).filter((frame) =>
        frame.streamId === initial.streamId && frame.type === FrameType.PAYLOAD);

      assertFragmentSequence(requests, FrameType.REQUEST_RESPONSE);
      assertFragmentSequence(responses, FrameType.PAYLOAD);
      expect(responses.at(-1)!.flags & PayloadFlag.NEXT).toBe(PayloadFlag.NEXT);
      expect(responses.at(-1)!.flags & PayloadFlag.COMPLETE).toBe(PayloadFlag.COMPLETE);
    } finally {
      await fixture.close();
    }
  });

  it("counts fragmented request-stream responses as logical demand", async () => {
    const fixture = await fragmentedFixture();
    const value = patternedText(5_000, "request-stream");
    const received: NumberResponse[] = [];
    const failures: unknown[] = [];
    let completed = false;
    let subscription: Subscription | undefined;

    try {
      fixture.connection.requestStream({count: 2, value}, route("numbers")).subscribe({
        onSubscribe(next) {
          subscription = next;
          next.request(1);
        },
        onNext(payload) {
          received.push(payload.data as NumberResponse);
        },
        onError(error) {
          failures.push(error);
        },
        onComplete() {
          completed = true;
        }
      });

      await waitFor(() => received.length === 1);
      await delay(20);
      expect(received).toEqual([{n: 1, value}]);
      expect(completed).toBe(false);
      subscription?.request(1);
      await waitFor(() => completed);
      expect(received).toEqual([{n: 1, value}, {n: 2, value}]);
      expect(failures).toEqual([]);

      const pair = fixture.pairs[0];
      if (pair === undefined) throw new Error("Missing WebSocket pair");
      const sent = observedRSocketFrames(pair.client.sent);
      const initial = sent.find((frame) => frame.type === FrameType.REQUEST_STREAM);
      if (initial === undefined) throw new Error("Missing REQUEST_STREAM frame");
      const requests = sent.filter((frame) => frame.streamId === initial.streamId &&
        (frame.type === FrameType.REQUEST_STREAM || frame.type === FrameType.PAYLOAD));
      const responses = fragmentSequences(observedRSocketFrames(pair.server.sent).filter((frame) =>
        frame.streamId === initial.streamId && frame.type === FrameType.PAYLOAD
      )).filter(isNextSequence);

      assertFragmentSequence(requests, FrameType.REQUEST_STREAM);
      expect(responses).toHaveLength(2);
      for (const response of responses) assertFragmentSequence(response, FrameType.PAYLOAD);
    } finally {
      subscription?.cancel();
      await fixture.close();
    }
  });

  it("does not expose routed authentication metadata as a channel input item", async () => {
    const fixture = await openRSocketServerFixture({
      controllers: testServerControllers({fireAndForget: []})
    });

    try {
      fixture.connection.metadataUpdate((metadata) => {
        metadata.set(
          WellKnownMimeType.MESSAGE_RSOCKET_AUTHENTICATION,
          WellKnownAuthType.BEARER.auth("integration-token")
        );
      });

      await expect(fixture.connection.process(ChannelController, Flux.just({
        data: {value: "authenticated"}
      })).toArray()).resolves.toEqual([
        {kind: "channel", value: "authenticated"}
      ]);
    } finally {
      await fixture.close();
    }
  });

  it("preserves fragmented request-channel item boundaries in both directions", async () => {
    const fixture = await fragmentedFixture();
    const first = patternedText(4_000, "channel-first");
    const second = patternedText(6_000, "channel-second");
    const received: ChannelResponse[] = [];
    const failures: unknown[] = [];
    let completed = false;

    try {
      fixture.connection.requestChannel(Flux.fromArray([
        {data: {value: first}, metadata: route("channel")},
        {data: {value: second}}
      ])).subscribe({
        onSubscribe(subscription) {
          subscription.request(2);
        },
        onNext(payload) {
          received.push(payload.data as ChannelResponse);
        },
        onError(error) {
          failures.push(error);
        },
        onComplete() {
          completed = true;
        }
      });

      await waitFor(() => completed);
      expect(received).toEqual([
        {kind: "channel", value: first},
        {kind: "channel", value: second}
      ]);
      expect(failures).toEqual([]);

      const pair = fixture.pairs[0];
      if (pair === undefined) throw new Error("Missing WebSocket pair");
      const sent = observedRSocketFrames(pair.client.sent);
      const initial = sent.find((frame) => frame.type === FrameType.REQUEST_CHANNEL);
      if (initial === undefined) throw new Error("Missing REQUEST_CHANNEL frame");
      const requests = fragmentSequences(sent.filter((frame) => frame.streamId === initial.streamId &&
        (frame.type === FrameType.REQUEST_CHANNEL || frame.type === FrameType.PAYLOAD)
      )).filter(isNextSequence);
      const responses = fragmentSequences(observedRSocketFrames(pair.server.sent).filter((frame) =>
        frame.streamId === initial.streamId && frame.type === FrameType.PAYLOAD
      )).filter(isNextSequence);

      expect(requests).toHaveLength(2);
      assertFragmentSequence(requests[0]!, FrameType.REQUEST_CHANNEL);
      assertFragmentSequence(requests[1]!, FrameType.PAYLOAD);
      expect(responses).toHaveLength(2);
      for (const response of responses) assertFragmentSequence(response, FrameType.PAYLOAD);
    } finally {
      await fixture.close();
    }
  });

  it("queues one-shot requests while offline and resumes without another SETUP", async () => {
    const browser = installBrowserSignals(true);
    const events: Array<{readonly type: string; readonly reconnect: boolean}> = [];
    let setupCount = 0;

    try {
      const fixture = await openRSocketServerFixture({
        controllers: testServerControllers({fireAndForget: []}),
        resume: {ttlMs: 30_000},
        accept() {
          setupCount += 1;
        }
      }, {
        reconnect: {resume: {ttl: 30_000}},
        events: {
          event(event) {
            events.push({type: event.type, reconnect: event.reconnect});
          }
        }
      });

      try {
        browser.setOnline(false);
        fixture.pairs[0]?.client.drop();
        await waitFor(() => events.some((event) => event.type === "reconnecting"));
        const queued = fixture.socket
          .requestResponse({value: "queued"}, route("echo"))
          .block();
        await delay(20);
        expect(fixture.pairs).toHaveLength(1);

        browser.setOnline(true);
        browser.dispatch("online");
        await waitFor(() => events.some((event) => event.type === "connected" && event.reconnect));
        await expect(queued).resolves.toMatchObject({data: {value: "queued"}});
        await fixture.accepts[1];

        expect(fixture.pairs).toHaveLength(2);
        expect(setupCount).toBe(1);
        expect(events.filter((event) => event.type === "reconnectFailed")).toHaveLength(0);
        expect(fixture.acceptFailures).toEqual([]);
      } finally {
        await fixture.close();
      }
    } finally {
      browser.restore();
    }
  });

  it("continues an active demand-controlled stream after Resume", async () => {
    const browser = installBrowserSignals(true);
    const events: Array<{readonly type: string; readonly reconnect: boolean}> = [];
    let setupCount = 0;

    try {
      const fixture = await openRSocketServerFixture({
        controllers: testServerControllers({fireAndForget: []}),
        resume: {ttlMs: 30_000},
        accept() {
          setupCount += 1;
        }
      }, {
        reconnect: {resume: {ttl: 30_000}},
        events: {
          event(event) {
            events.push({type: event.type, reconnect: event.reconnect});
          }
        }
      });
      const values: NumberResponse[] = [];
      const failures: unknown[] = [];
      let completed = false;
      let subscription: Subscription | undefined;

      try {
        fixture.connection.process(NumbersController, {count: 3}).subscribe({
          onSubscribe(next) {
            subscription = next;
            next.request(1);
          },
          onNext(value) {
            values.push(value);
          },
          onError(error) {
            failures.push(error);
          },
          onComplete() {
            completed = true;
          }
        });

        await waitFor(() => values.length === 1);
        browser.setOnline(false);
        fixture.pairs[0]?.client.drop();
        await waitFor(() => events.some((event) => event.type === "reconnecting"));
        subscription?.request(2);
        await delay(20);
        expect(values).toEqual([{n: 1}]);

        browser.setOnline(true);
        browser.dispatch("online");
        await waitFor(() => completed &&
          events.some((event) => event.type === "connected" && event.reconnect));
        await fixture.accepts[1];

        expect(values).toEqual([{n: 1}, {n: 2}, {n: 3}]);
        expect(failures).toEqual([]);
        expect(fixture.pairs).toHaveLength(2);
        expect(setupCount).toBe(1);
        expect(fixture.acceptFailures).toEqual([]);
      } finally {
        subscription?.cancel();
        await fixture.close();
      }
    } finally {
      browser.restore();
    }
  });
});

/** Opens a fixture with matching small client and server frame limits. */
function fragmentedFixture() {
  return openRSocketServerFixture({
    controllers: testServerControllers({fireAndForget: []}),
    maxFrameLength: FRAGMENT_SIZE
  }, {
    setup: {fragmentSize: FRAGMENT_SIZE}
  });
}

/** Asserts one complete FOLLOWS sequence and its WebSocket message boundaries. */
function assertFragmentSequence(
  frames: readonly ObservedRSocketFrame[],
  firstType: FrameType
): void {
  expect(frames.length).toBeGreaterThan(1);
  expect(frames[0]!.type).toBe(firstType);
  expect(frames.slice(1).every((frame) => frame.type === FrameType.PAYLOAD)).toBe(true);
  expect(frames.every((frame) => frame.bytes.byteLength <= FRAGMENT_SIZE)).toBe(true);
  expect(frames.slice(0, -1).every(hasFollows)).toBe(true);
  expect(hasFollows(frames.at(-1)!)).toBe(false);
}

/** Returns direct entries from direct or composite metadata. */
function metadataEntries(value: Metadata<any> | undefined): readonly Metadata<any>[] {
  if (value === undefined) return [];
  return Array.isArray(value.payload) && value.payload.every((entry) => entry instanceof Metadata)
    ? value.payload
    : [value];
}

/** Produces deterministic text large enough to require fragmentation. */
function patternedText(length: number, seed: string): string {
  const pattern = `${seed}:`;
  return pattern.repeat(Math.ceil(length / pattern.length)).slice(0, length);
}

/** Waits until an asynchronous integration condition becomes true. */
async function waitFor(predicate: () => boolean, timeoutMs = 5_000): Promise<void> {
  const startedAt = Date.now();
  while (!predicate()) {
    if (Date.now() - startedAt > timeoutMs) {
      throw new Error("Timed out waiting for browser/server integration condition");
    }
    await delay(5);
  }
}

/** Delays one test operation by a fixed number of milliseconds. */
function delay(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
