/** Resource cleanup regressions for hostile user-provided publishers. */
import type {Publisher, Subscriber, Subscription} from "reactor-core-ts";
import {
    CancelFrame,
    PayloadFlag,
    PayloadFrame,
    RequestChannelFlag,
    RequestChannelFrame,
    RequestNFrame,
    RequestResponseFrame,
    RequestStreamFrame,
    SetupFrame,
    type Frame,
    WellKnownMimeType
} from "rsocket-frames-ts";
import {MAX_REQUEST_N, type RSocketPayloadFrame} from "rsocket-core-ts";
import {describe, expect, it, vi} from "vitest";
import {
    RequestChannelController,
    RequestResponseController,
    RequestStreamController
} from "@/controllers/classes.js";
import {ControllerRegistry} from "@/controllers/registry.js";
import {ServerBackgroundWork} from "@/session/background.js";
import {ServerInteractionDispatcher} from "@/session/interactions.js";
import {ResumeRegistry, type ResumableServerSession} from "@/resume/registry.js";
import type {ResponderSession} from "@/session/types.js";
import {ChannelInput} from "@/stream/channel-input.js";
import {RequestChannelResponder} from "@/stream/channel.js";
import {DemandControlledOutput} from "@/stream/output.js";
import {RequestResponseResponder} from "@/stream/single.js";

/** Publisher that reports cancellation and then throws from user code. */
class ThrowingCancelPublisher implements Publisher<void> {
    cancelled = false;

    /** Exposes one subscription that remains active until server shutdown. */
    subscribe(subscriber: Subscriber<void>): void {
        subscriber.onSubscribe({
            request: () => undefined,
            cancel: () => {
                this.cancelled = true;
                throw new Error("cancel failed");
            }
        });
    }
}

/** Records whether request-channel dispatch reached user code. */
class CountingChannelController extends RequestChannelController<void, never> {
    protected readonly route = [] as const;

    /** Creates a controller around one test-owned counter callback. */
    constructor(private readonly record: () => void) {
        super();
    }

    /** Records invocation without producing response items. */
    override handle(): readonly never[] {
        this.record();
        return [];
    }
}

/** Cancels its request synchronously before returning a response publisher. */
class CancellingResponseController extends RequestResponseController<void, number> {
    protected readonly route = [] as const;

    /** Retains test-owned cancellation and source callbacks. */
    constructor(
        private readonly cancel: () => void,
        private readonly source: Publisher<{data: number}>
    ) {
        super();
    }

    /** Cancels the stream before exposing its otherwise subscribable result. */
    override handle(): Publisher<{data: number}> {
        this.cancel();
        return this.source;
    }
}

/** Cancels its request synchronously before returning a stream publisher. */
class CancellingStreamController extends RequestStreamController<void, number> {
    protected readonly route = [] as const;

    /** Retains test-owned cancellation and source callbacks. */
    constructor(
        private readonly cancel: () => void,
        private readonly source: Publisher<{data: number}>
    ) {
        super();
    }

    /** Cancels the stream before exposing its otherwise subscribable source. */
    override handle(): Publisher<{data: number}> {
        this.cancel();
        return this.source;
    }
}

/** Parameterized controller race used for both single and streaming responses. */
interface CancellingControllerCase {
    readonly name: string;
    readonly controller: (
        cancel: () => void,
        source: Publisher<{data: number}>
    ) => CancellingResponseController | CancellingStreamController;
    readonly request: () => RequestResponseFrame | RequestStreamFrame;
}

/** Controller variants whose output must remain unsubscribed after reentrant cancellation. */
const CANCELLING_CONTROLLER_CASES: readonly CancellingControllerCase[] = [
    {
        name: "request-response",
        controller: (cancel, source) => new CancellingResponseController(cancel, source),
        request: () => new RequestResponseFrame(1, 0)
    },
    {
        name: "request-stream",
        controller: (cancel, source) => new CancellingStreamController(cancel, source),
        request: () => new RequestStreamFrame(1, 0, 1)
    }
];

describe("server resource cleanup", () => {
    it("does not invoke a channel controller after reentrant cancellation of its first credit", () => {
        let calls = 0;
        let dispatcher!: ServerInteractionDispatcher;
        const session = {
            connection: {},
            isTerminated: false,
            acquireRequest: () => undefined,
            send: (frame: Frame) => {
                if (frame instanceof RequestNFrame) {
                    dispatcher.handle(new CancelFrame(frame.header.streamId));
                }
            },
            unregister: (streamId: number) => dispatcher.unregister(streamId)
        } as unknown as ResponderSession;
        dispatcher = new ServerInteractionDispatcher(
            session,
            new SetupFrame(
                60_000,
                60_000,
                WellKnownMimeType.MESSAGE_RSOCKET_COMPOSITE_METADATA,
                WellKnownMimeType.APPLICATION_JSON
            ),
            new ControllerRegistry([new CountingChannelController(() => {
                calls += 1;
            })]),
            new ServerBackgroundWork()
        );

        dispatcher.handle(new RequestChannelFrame(1, RequestChannelFlag.NONE, 1));

        expect(calls).toBe(0);
        expect(dispatcher.isIdle).toBe(true);
    });

    it.each(CANCELLING_CONTROLLER_CASES)(
        "does not subscribe $name output after reentrant controller cancellation",
        ({controller, request}) => {
            let subscriptions = 0;
            let dispatcher!: ServerInteractionDispatcher;
            const source: Publisher<{data: number}> = {
                subscribe: () => {
                    subscriptions += 1;
                }
            };
            const cancel = () => dispatcher.handle(new CancelFrame(1));
            const session = {
                connection: {},
                isTerminated: false,
                acquireRequest: () => undefined,
                unregister: (streamId: number) => dispatcher.unregister(streamId)
            } as unknown as ResponderSession;
            dispatcher = new ServerInteractionDispatcher(
                session,
                new SetupFrame(
                    60_000,
                    60_000,
                    WellKnownMimeType.MESSAGE_RSOCKET_COMPOSITE_METADATA,
                    WellKnownMimeType.APPLICATION_JSON
                ),
                new ControllerRegistry([controller(cancel, source)]),
                new ServerBackgroundWork()
            );

            dispatcher.handle(request());

            expect(subscriptions).toBe(0);
            expect(dispatcher.isIdle).toBe(true);
        }
    );

    it("rejects stream output emitted before onSubscribe without writing a payload", () => {
        const sent: Frame[] = [];
        let lifecycleError: unknown;
        let lateSubscriptionCancelled = false;
        const output = new DemandControlledOutput({
            encode: () => ({}),
            send: (frame: Frame) => sent.push(frame)
        } as unknown as ResponderSession, 1, 1, {
            complete: () => undefined,
            error: (error) => {
                lifecycleError = error;
            }
        });

        output.start({
            subscribe(subscriber: Subscriber<unknown>) {
                subscriber.onNext({data: 1});
                subscriber.onSubscribe({
                    request: () => undefined,
                    cancel: () => {
                        lateSubscriptionCancelled = true;
                    }
                });
            }
        });

        expect(sent).toHaveLength(0);
        expect(lifecycleError).toMatchObject({message: "Controller publisher emitted before onSubscribe"});
        expect(lateSubscriptionCancelled).toBe(true);
    });

    it("serializes reentrant response demand before requesting controller output", () => {
        const itemCount = 128;
        let output!: DemandControlledOutput;
        let sentNext = 0;
        let completed = 0;
        let requestDepth = 0;
        let maximumRequestDepth = 0;
        const session = {
            encode: () => ({}),
            send: (frame: Frame) => {
                if (!(frame instanceof PayloadFrame) || !frame.isNext()) return;
                sentNext += 1;
                if (sentNext < itemCount) output.request(1);
            }
        } as unknown as ResponderSession;
        output = new DemandControlledOutput(session, 1, 1, {
            complete: () => {
                completed += 1;
            },
            error: (error) => {
                throw error;
            }
        });
        const source: Publisher<{data: number}> = {
            subscribe(subscriber) {
                let emitted = 0;
                subscriber.onSubscribe({
                    request(request) {
                        requestDepth += 1;
                        maximumRequestDepth = Math.max(maximumRequestDepth, requestDepth);
                        const end = Math.min(itemCount, emitted + request);
                        while (emitted < end) subscriber.onNext({data: emitted++});
                        if (emitted === itemCount) subscriber.onComplete();
                        requestDepth -= 1;
                    },
                    cancel() {
                    }
                });
            }
        };

        output.start(source);

        expect(sentNext).toBe(itemCount);
        expect(completed).toBe(1);
        expect(maximumRequestDepth).toBe(1);
    });

    it("rejects request-response output emitted before onSubscribe", () => {
        const sent: Frame[] = [];
        let streamError: unknown;
        let lateSubscriptionCancelled = false;
        const session = {
            encode: () => ({}),
            send: (frame: Frame) => sent.push(frame),
            unregister: () => undefined,
            streamError: (_streamId: number, error: unknown) => {
                streamError = error;
            }
        } as unknown as ResponderSession;
        const responder = new RequestResponseResponder(session, 1, {
            subscribe(subscriber: Subscriber<unknown>) {
                subscriber.onNext({data: 1});
                subscriber.onSubscribe({
                    request: () => undefined,
                    cancel: () => {
                        lateSubscriptionCancelled = true;
                    }
                });
            }
        });

        responder.start();

        expect(sent).toHaveLength(0);
        expect(streamError).toMatchObject({message: "Controller publisher emitted before onSubscribe"});
        expect(lateSubscriptionCancelled).toBe(true);
    });

    it.each(["complete", "error"] as const)(
        "rejects stream output %s before onSubscribe",
        (signal) => {
            let lifecycleError: unknown;
            let lateSubscriptionCancelled = false;
            const output = new DemandControlledOutput({
                encode: () => ({}),
                send: () => undefined
            } as unknown as ResponderSession, 1, 1, {
                complete: () => undefined,
                error: (error) => {
                    lifecycleError = error;
                }
            });

            output.start({
                subscribe(subscriber: Subscriber<unknown>) {
                    if (signal === "complete") subscriber.onComplete();
                    else subscriber.onError(new Error("publisher failed"));
                    subscriber.onSubscribe({
                        request: () => undefined,
                        cancel: () => {
                            lateSubscriptionCancelled = true;
                        }
                    });
                }
            });

            expect(lifecycleError).toMatchObject({
                message: `Controller publisher ${signal === "complete" ? "completed" : "failed"} before onSubscribe`
            });
            expect(lateSubscriptionCancelled).toBe(true);
        }
    );

    it.each(["complete", "error"] as const)(
        "rejects request-response output %s before onSubscribe",
        (signal) => {
            let streamError: unknown;
            let lateSubscriptionCancelled = false;
            const session = {
                send: () => undefined,
                unregister: () => undefined,
                streamError: (_streamId: number, error: unknown) => {
                    streamError = error;
                }
            } as unknown as ResponderSession;
            const responder = new RequestResponseResponder(session, 1, {
                subscribe(subscriber: Subscriber<unknown>) {
                    if (signal === "complete") subscriber.onComplete();
                    else subscriber.onError(new Error("publisher failed"));
                    subscriber.onSubscribe({
                        request: () => undefined,
                        cancel: () => {
                            lateSubscriptionCancelled = true;
                        }
                    });
                }
            });

            responder.start();

            expect(streamError).toMatchObject({
                message: `Controller publisher ${signal === "complete" ? "completed" : "failed"} before onSubscribe`
            });
            expect(lateSubscriptionCancelled).toBe(true);
        }
    );

    it("does not register an abort listener for terminal channel input", async () => {
        const input = new ChannelInput({} as ResponderSession, 1, undefined, false, {
            complete: () => undefined,
            error: () => undefined,
            cancel: () => undefined
        });
        input.terminate();
        const addEventListener = vi.fn();
        const removeEventListener = vi.fn();
        const signal = {
            aborted: false,
            addEventListener,
            removeEventListener
        } as unknown as AbortSignal;

        const iterator = input.iterable(signal)[Symbol.asyncIterator]();

        await expect(iterator.next()).resolves.toEqual({done: true, value: undefined});
        expect(addEventListener).not.toHaveBeenCalled();
        expect(removeEventListener).not.toHaveBeenCalled();
    });

    it("cancels every background subscription when an earlier cancellation throws", () => {
        const first = new ThrowingCancelPublisher();
        const second = new ThrowingCancelPublisher();
        const work = new ServerBackgroundWork();
        work.consume(first);
        work.consume(second);

        expect(() => work.close()).not.toThrow();
        expect(first.cancelled).toBe(true);
        expect(second.cancelled).toBe(true);
        expect((work as unknown as {subscriptions: Set<unknown>}).subscriptions.size).toBe(0);
    });

    it("cancels duplicate background subscriptions without retaining the first after completion", () => {
        let duplicateCancelled = false;
        const work = new ServerBackgroundWork();
        work.consume({
            subscribe(subscriber: Subscriber<void>) {
                subscriber.onSubscribe({request: () => undefined, cancel: () => undefined});
                subscriber.onSubscribe({
                    request: () => undefined,
                    cancel: () => {
                        duplicateCancelled = true;
                    }
                });
                subscriber.onComplete();
            }
        });

        expect(duplicateCancelled).toBe(true);
        expect((work as unknown as {subscriptions: Set<unknown>}).subscriptions.size).toBe(0);
    });

    it("cancels a background publisher that emits before onSubscribe", () => {
        let cancelled = false;
        let requested = false;
        const work = new ServerBackgroundWork();

        work.consume({
            subscribe(subscriber: Subscriber<void>) {
                subscriber.onNext(undefined);
                subscriber.onSubscribe({
                    request: () => {
                        requested = true;
                    },
                    cancel: () => {
                        cancelled = true;
                    }
                });
            }
        });

        expect(cancelled).toBe(true);
        expect(requested).toBe(false);
        expect((work as unknown as {subscriptions: Set<unknown>}).subscriptions.size).toBe(0);
    });

    it("isolates channel completion callbacks after sending the mandatory initial REQUEST_N", () => {
        const sent: Frame[] = [];
        let completed = 0;
        let lifecycleError: unknown;
        const session = {
            send: (frame: Frame) => sent.push(frame)
        } as unknown as ResponderSession;
        const input = new ChannelInput(session, 1, undefined, true, {
            complete: () => {
                completed += 1;
            },
            error: (error) => {
                lifecycleError = error;
            },
            cancel: () => undefined
        });

        input.start();
        expect(() => input.subscribe({
            onSubscribe: (subscription) => subscription.request(1),
            onNext: () => undefined,
            onError: () => undefined,
            onComplete: () => {
                throw new Error("completion callback failed");
            }
        })).not.toThrow();

        expect(sent).toHaveLength(1);
        expect(sent[0]).toBeInstanceOf(RequestNFrame);
        expect(completed).toBe(1);
        expect(lifecycleError).toBeUndefined();
    });

    it.each([
        ["NEXT|FOLLOWS", PayloadFlag.NEXT | PayloadFlag.FOLLOWS],
        ["FOLLOWS", PayloadFlag.FOLLOWS]
    ])("rejects an uncredited channel fragment with %s before buffering", (_name, flags) => {
        const sent: Frame[] = [];
        let lifecycleError: unknown;
        const session = {send: (frame: Frame) => sent.push(frame)} as unknown as ResponderSession;
        const input = new ChannelInput(session, 1, undefined, false, {
            complete: () => undefined,
            error: (error) => {
                lifecycleError = error;
            },
            cancel: () => undefined
        });
        input.start();
        const first = new PayloadFrame(1, PayloadFlag.NEXT);
        input.next({frame: first});
        const fragment = new PayloadFrame(1, flags);

        expect(input.acceptsPayloadFragment(fragment)).toBe(false);
        expect(lifecycleError).toMatchObject({
            message: "RSocket requester sent channel PAYLOAD without responder demand"
        });
        expect((input as unknown as {pending: unknown[]}).pending).toHaveLength(1);
    });

    it("accepts terminal FOLLOWS|COMPLETE without an additional channel credit", () => {
        const sent: Frame[] = [];
        let lifecycleError: unknown;
        const input = new ChannelInput(
            {send: (frame: Frame) => sent.push(frame)} as unknown as ResponderSession,
            1,
            undefined,
            false,
            {
                complete: () => undefined,
                error: (error) => {
                    lifecycleError = error;
                },
                cancel: () => undefined
            }
        );
        input.start();
        input.next({frame: new PayloadFrame(1, PayloadFlag.NEXT)});

        expect(input.acceptsPayloadFragment(new PayloadFrame(
            1,
            PayloadFlag.FOLLOWS | PayloadFlag.COMPLETE
        ))).toBe(true);
        expect(lifecycleError).toBeUndefined();
    });

    it("releases a Resume session by its reserved key after the token bytes mutate", () => {
        const registry = new ResumeRegistry();
        const token = Uint8Array.of(0xff, 0, 0x80, 0x61);
        const session = {} as ResumableServerSession;

        expect(registry.reserve(token, session)).toBe(true);
        token.fill(0);
        registry.release(session);

        expect(registry.size).toBe(0);
    });

    it("drops late channel fragments after the requester half has completed", () => {
        const sent: Frame[] = [];
        let lifecycleError: unknown;
        const input = new ChannelInput(
            {send: (frame: Frame) => sent.push(frame)} as unknown as ResponderSession,
            1,
            undefined,
            true,
            {
                complete: () => undefined,
                error: (error) => {
                    lifecycleError = error;
                },
                cancel: () => undefined
            }
        );
        input.start();

        expect(input.acceptsPayloadFragment(
            new PayloadFrame(1, PayloadFlag.NEXT | PayloadFlag.FOLLOWS)
        )).toBe(false);
        expect(sent).toHaveLength(1);
        expect(lifecycleError).toBeUndefined();
    });

    it("ignores late channel payloads after requester COMPLETE without a stream error", () => {
        const errors: unknown[] = [];
        const responder = new RequestChannelResponder({
            encode: () => ({}),
            send: () => undefined,
            unregister: () => undefined,
            streamError: (_streamId: number, error: unknown) => errors.push(error)
        } as unknown as ResponderSession, 1, 1, undefined, true);
        const late = new PayloadFrame(1, PayloadFlag.NEXT | PayloadFlag.FOLLOWS);

        expect(responder.acceptPayloadFragment(late)).toBe(false);
        responder.handlePayload(new PayloadFrame(1, PayloadFlag.NEXT));

        expect(errors).toEqual([]);
    });

    it("releases a wire-complete channel without discarding its queued final input", () => {
        const sent: Frame[] = [];
        let unregistered = false;
        const session = {
            encode: () => ({}),
            send: (frame: Frame) => sent.push(frame),
            unregister: () => {
                unregistered = true;
            },
            streamError: (_streamId: number, error: unknown) => {
                throw error;
            }
        } as unknown as ResponderSession;
        const frame = new PayloadFrame(
            1,
            PayloadFlag.NEXT,
            undefined,
            WellKnownMimeType.APPLICATION_JSON.toPayload({final: true})
        );
        const initial: RSocketPayloadFrame = {
            frame,
            data: {final: true}
        };
        const responder = new RequestChannelResponder(session, 1, 1, initial, true);

        responder.startInput();
        responder.start([]);
        expect(unregistered).toBe(true);

        const received: RSocketPayloadFrame[] = [];
        responder.requests().subscribe({
            onSubscribe: (subscription) => subscription.request(1),
            onNext: (value) => received.push(value),
            onError: (error) => {
                throw error;
            },
            onComplete: () => undefined
        });

        expect(received).toEqual([initial]);
        expect(unregistered).toBe(true);
    });

    it("translates unbounded local channel demand into a bounded wire window", () => {
        const sent: Frame[] = [];
        let lifecycleError: unknown;
        const session = {
            send: (frame: Frame) => sent.push(frame)
        } as unknown as ResponderSession;
        const input = new ChannelInput(session, 1, undefined, false, {
            complete: () => undefined,
            error: (error) => {
                lifecycleError = error;
            },
            cancel: () => undefined
        });

        input.start();
        input.subscribe({
            onSubscribe: (subscription) => subscription.request(Number.POSITIVE_INFINITY),
            onNext: () => undefined,
            onError: () => undefined,
            onComplete: () => undefined
        });

        expect(sent).toHaveLength(2);
        expect((sent[0] as RequestNFrame).request).toBe(1);
        expect((sent[1] as RequestNFrame).request).toBe(MAX_REQUEST_N - 1);
        const incoming = {
            frame: new PayloadFrame(1, PayloadFlag.NEXT),
            data: 1
        } satisfies RSocketPayloadFrame;
        for (let index = 0; index < 128; index += 1) input.next(incoming);
        expect(sent).toHaveLength(2);
        expect(lifecycleError).toBeUndefined();
        input.terminate();
    });

    it("publishes channel credit before a synchronous peer can consume it", () => {
        const sent: Frame[] = [];
        const received: RSocketPayloadFrame[] = [];
        let lifecycleError: unknown;
        let input: ChannelInput;
        const incoming = {
            frame: new PayloadFrame(
                1,
                PayloadFlag.NEXT,
                undefined,
                WellKnownMimeType.APPLICATION_JSON.toPayload(1)
            ),
            data: 1
        } satisfies RSocketPayloadFrame;
        const session = {
            send: (frame: Frame) => {
                sent.push(frame);
                if (frame instanceof RequestNFrame) input.next(incoming);
            }
        } as unknown as ResponderSession;
        input = new ChannelInput(session, 1, undefined, false, {
            complete: () => undefined,
            error: (error) => {
                lifecycleError = error;
            },
            cancel: () => undefined
        });

        input.start();
        input.subscribe({
            onSubscribe: (subscription) => subscription.request(1),
            onNext: (value) => received.push(value),
            onError: (error) => {
                lifecycleError = error;
            },
            onComplete: () => undefined
        });

        expect(sent).toHaveLength(1);
        expect(received).toEqual([incoming]);
        expect(lifecycleError).toBeUndefined();
        input.terminate();
    });

    it("serializes reentrant channel demand without nesting onNext callbacks", () => {
        const incoming = (value: number): RSocketPayloadFrame => ({
            frame: new PayloadFrame(1, PayloadFlag.NEXT),
            data: value
        });
        const input = new ChannelInput(
            {send: () => undefined} as unknown as ResponderSession,
            1,
            incoming(1),
            false,
            {
                complete: () => undefined,
                error: (error) => {
                    throw error;
                },
                cancel: () => undefined
            }
        );
        input.start();
        input.next(incoming(2));
        const received: number[] = [];
        let depth = 0;
        let maximumDepth = 0;
        let channelSubscription: Subscription | undefined;

        input.subscribe({
            onSubscribe: (subscription) => {
                channelSubscription = subscription;
                subscription.request(1);
            },
            onNext: (value) => {
                depth += 1;
                maximumDepth = Math.max(maximumDepth, depth);
                received.push(value.data as number);
                if (received.length === 1) channelSubscription?.request(1);
                depth -= 1;
            },
            onError: (error) => {
                throw error;
            },
            onComplete: () => undefined
        });

        expect(received).toEqual([1, 2]);
        expect(maximumDepth).toBe(1);
        input.terminate();
    });
});
