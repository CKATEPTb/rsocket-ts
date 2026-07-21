import type {Subscription} from "reactor-core-ts";
import {afterEach, describe, expect, it} from "vitest";
import {FrameErrorCode, Metadata, WellKnownMimeType} from "rsocket-frames-ts";
import {route} from "rsocket-core-ts";
import {RSocket} from "rsocket-client-ts";
import type {RSocketServerOptions} from "@/index.js";
import {
    DoubleChannelController,
    EchoController,
    fireAndForgetValues,
    RangeController,
    RecordController
} from "./controllers.js";
import {nextTurn, waitFor} from "./wait.js";

/** One connected public requester/server pair owned by a transport test. */
export interface TransportFixture {
    /** Public requester whose interactions cross the tested transport. */
    readonly requester: RSocket;
    /** Releases the requester, listener or socket, and responder. */
    close(): Promise<void>;
}

/** Opens one transport around the supplied responder options. */
export type TransportFixtureFactory = (options: RSocketServerOptions) => Promise<TransportFixture>;

/**
 * Runs the same public interaction contract over one physical transport.
 *
 * Keeping this suite shared prevents TCP and WebSocket behavior from silently
 * diverging while each transport still runs as an independent Vitest file.
 */
export function defineTransportConformance(
    name: string,
    open: TransportFixtureFactory
): void {
    describe(name, () => {
        let fixture: TransportFixture | undefined;

        afterEach(async () => {
            await fixture?.close();
            fixture = undefined;
            fireAndForgetValues.length = 0;
        });

        it("carries SETUP, fire-and-forget, metadata-push, and request-response", async () => {
            const pushed: Metadata<any>[] = [];
            fixture = await open(serverOptions({
                metadataPush(metadata) {
                    pushed.push(metadata);
                }
            }));
            const {requester} = fixture;

            await requester.fireAndForget(7, route("record")).block();
            await requester.metadataPush(
                WellKnownMimeType.TEXT_PLAIN.toMetadata("transport-metadata")
            ).block();
            const response = await requester.requestResponse({value: "echo"}, route("echo")).block();

            await waitFor(() => fireAndForgetValues.length === 1 && pushed.length === 1);
            expect(fireAndForgetValues).toEqual([7]);
            expect(response?.data).toEqual({value: "echo"});
            expect(metadataEntries(pushed[0]).some((entry) =>
                entry.mimeType.mimeType === WellKnownMimeType.TEXT_PLAIN.mimeType &&
                entry.payload === "transport-metadata"
            )).toBe(true);
        });

        it("preserves independent request-stream and request-channel demand", async () => {
            fixture = await open(serverOptions());
            const {requester} = fixture;
            const streamValues: number[] = [];
            let streamComplete = false;
            let streamSubscription: Subscription | undefined;

            requester.requestStream(3, route("range")).subscribe({
                onSubscribe(subscription) {
                    streamSubscription = subscription;
                    subscription.request(1);
                },
                onNext(payload) {
                    streamValues.push(payload.data as number);
                },
                onError(error) {
                    throw error;
                },
                onComplete() {
                    streamComplete = true;
                }
            });

            await waitFor(() => streamValues.length === 1);
            await nextTurn();
            expect(streamValues).toEqual([0]);
            expect(streamComplete).toBe(false);

            streamSubscription?.request(2);
            await waitFor(() => streamComplete);
            expect(streamValues).toEqual([0, 1, 2]);

            const channelValues: number[] = [];
            let channelComplete = false;
            let channelSubscription: Subscription | undefined;
            requester.requestChannel([1, 2, 3], route("double")).subscribe({
                onSubscribe(subscription) {
                    channelSubscription = subscription;
                    subscription.request(1);
                },
                onNext(payload) {
                    channelValues.push(payload.data as number);
                },
                onError(error) {
                    throw error;
                },
                onComplete() {
                    channelComplete = true;
                }
            });

            await waitFor(() => channelValues.length === 1);
            await nextTurn();
            expect(channelValues).toEqual([2]);
            expect(channelComplete).toBe(false);

            channelSubscription?.request(2);
            await waitFor(() => channelComplete);
            expect(channelValues).toEqual([2, 4, 6]);
        });

        it("fragments large payloads and keeps stream errors non-terminal for the connection", async () => {
            fixture = await open(serverOptions());
            const {requester} = fixture;
            const payload = {value: "fragmented-transport-".repeat(100)};

            await expect(requester.requestResponse(payload, route("echo")).block())
                .resolves.toMatchObject({data: payload});
            await expect(requester.requestResponse(undefined, route("missing")).block())
                .rejects.toMatchObject({code: FrameErrorCode.REJECTED});
            await expect(requester.requestResponse({value: "still-open"}, route("echo")).block())
                .resolves.toMatchObject({data: {value: "still-open"}});
        });
    });
}

/** Creates the common responder configuration used by both transports. */
function serverOptions(overrides: Partial<RSocketServerOptions> = {}): RSocketServerOptions {
    return {
        controllers: [RecordController, EchoController, RangeController, DoubleChannelController],
        maxFrameLength: 128,
        ...overrides
    };
}

/** Returns direct entries from either direct or composite metadata. */
function metadataEntries(value: Metadata<any> | undefined): readonly Metadata<any>[] {
    if (value === undefined) return [];
    return Array.isArray(value.payload) && value.payload.every((entry) => entry instanceof Metadata)
        ? value.payload
        : [value];
}
