import {afterEach, describe, expect, it} from "vitest";
import {compositeMetadata, readFrameTypeAndFlags, route} from "rsocket-core-ts";
import {FrameType, Metadata, PayloadFlag, WellKnownMimeType} from "rsocket-frames-ts";
import {prependChannelPayload} from "./client-engine.js";
import {
    FireAndForgetController,
    RequestChannelController,
    RequestResponseController,
    RequestStreamController,
    type RSocketRequestContext
} from "@/index.js";
import {connectTestPair, type ConnectedTestPair} from "./helpers.js";
import {waitFor} from "./wait.js";

class LargeEchoController extends RequestResponseController<string, string> {
    protected readonly route = "large-echo";

    override handle(data: string): string {
        return data;
    }
}

class MetadataEchoController extends RequestResponseController<string, string> {
    protected readonly route = "metadata-echo";

    override handle(data: string, context: RSocketRequestContext<string>) {
        return {data, metadata: context.metadataPayload as Metadata<any>};
    }
}

let metadataOnlyRequestData: unknown;

class MetadataOnlyRequestController extends RequestResponseController<void, string> {
    protected readonly route = "metadata-only-request";

    override handle(data: void): string {
        metadataOnlyRequestData = data;
        return "accepted";
    }
}

class MetadataOnlyResponseController extends RequestResponseController<void, void, unknown, string> {
    protected readonly route = "metadata-only-response";

    override handle() {
        return WellKnownMimeType.TEXT_PLAIN.toMetadata("response-metadata-".repeat(80));
    }
}

class LargeChannelController extends RequestChannelController<string, string> {
    protected readonly route = "large-channel";

    override handle(requests: import("reactor-core-ts").Flux<import("rsocket-core-ts").RSocketPayloadFrame<string>>) {
        return requests.map(({data}) => data as string);
    }
}

let recordedFireAndForget: string | undefined;

class LargeFireAndForgetController extends FireAndForgetController<string> {
    protected readonly route = "large-fnf";

    override handle(data: string): void {
        recordedFireAndForget = data;
    }
}

class LargeStreamController extends RequestStreamController<string, string> {
    protected readonly route = "large-stream";

    override handle(data: string): readonly string[] {
        return [data, data];
    }
}

describe("transparent server fragmentation and reassembly", () => {
    let pair: ConnectedTestPair | undefined;

    afterEach(async () => {
        pair?.client.close();
        await pair?.server.close().block();
        pair = undefined;
    });

    it("reassembles fragmented initial requests and fragments large responses", async () => {
        pair = await connectTestPair(
            [LargeEchoController],
            {maxFrameLength: 128},
            {maxFrameLength: 128}
        );
        const value = "payload-".repeat(100);
        const response = await pair.client.requestResponse({data: value, metadata: route("large-echo")}).block();

        expect(response?.data).toBe(value);
        const requestFrames = pair.clientTransport.sent.filter((bytes) => frameType(bytes) !== FrameType.SETUP);
        expect(frameType(requestFrames[0] as Uint8Array)).toBe(FrameType.REQUEST_RESPONSE);
        expect(requestFrames.slice(1).every((bytes) => frameType(bytes) === FrameType.PAYLOAD)).toBe(true);
        expect(requestFrames.length).toBeGreaterThan(2);
        expect(hasFollows(requestFrames[0] as Uint8Array)).toBe(true);
        expect(pair.serverTransport.sent.filter((bytes) => frameType(bytes) === FrameType.PAYLOAD).length).toBeGreaterThan(2);
    });

    it("reassembles fragmented fire-and-forget without creating a response stream", async () => {
        recordedFireAndForget = undefined;
        pair = await connectTestPair(
            [LargeFireAndForgetController],
            {maxFrameLength: 128},
            {maxFrameLength: 128}
        );
        const value = "fire-and-forget-".repeat(100);

        await pair.client.fireAndForget({data: value, metadata: route("large-fnf")}).block();
        await waitFor(() => recordedFireAndForget !== undefined);

        expect(recordedFireAndForget).toBe(value);
        const frames = pair.clientTransport.sent.filter((bytes) => frameType(bytes) !== FrameType.SETUP);
        expect(frameType(frames[0] as Uint8Array)).toBe(FrameType.REQUEST_FNF);
        expect(frames.slice(1).every((bytes) => frameType(bytes) === FrameType.PAYLOAD)).toBe(true);
        expect(pair.serverTransport.sent.some((bytes) => frameType(bytes) === FrameType.PAYLOAD)).toBe(false);
    });

    it("fragments request-stream input and each demand-controlled response independently", async () => {
        pair = await connectTestPair(
            [LargeStreamController],
            {maxFrameLength: 128},
            {maxFrameLength: 128}
        );
        const value = "stream-payload-".repeat(100);

        const responses = await pair.client.requestStream({
            data: value,
            metadata: route("large-stream")
        }).toArray();

        expect(responses.map(({data}) => data)).toEqual([value, value]);
        expect(pair.clientTransport.sent.some((bytes) =>
            frameType(bytes) === FrameType.REQUEST_STREAM && hasFollows(bytes)
        )).toBe(true);
        expect(pair.serverTransport.sent.filter((bytes) =>
            frameType(bytes) === FrameType.PAYLOAD && hasFollows(bytes)
        ).length).toBeGreaterThanOrEqual(2);
    });

    it("preserves metadata-before-data ordering across fragments", async () => {
        pair = await connectTestPair(
            [MetadataEchoController],
            {maxFrameLength: 128},
            {maxFrameLength: 128}
        );
        const text = "metadata-".repeat(80);
        const metadata = compositeMetadata(
            route("metadata-echo"),
            WellKnownMimeType.TEXT_PLAIN.toMetadata(text)
        );
        const response = await pair.client.requestResponse({data: "body", metadata}).block();

        expect(response?.data).toBe("body");
        const entries = response?.metadata as Metadata<any>[];
        expect(entries.map((entry) => entry.mimeType.mimeType)).toEqual([
            WellKnownMimeType.MESSAGE_RSOCKET_ROUTING.mimeType,
            WellKnownMimeType.TEXT_PLAIN.mimeType
        ]);
        expect(entries[1]?.payload).toBe(text);
    });

    it("reassembles a metadata-only request without inventing an empty JSON payload", async () => {
        metadataOnlyRequestData = "not-called";
        pair = await connectTestPair(
            [MetadataOnlyRequestController],
            {maxFrameLength: 128},
            {maxFrameLength: 128}
        );
        const metadata = compositeMetadata(
            route("metadata-only-request"),
            WellKnownMimeType.TEXT_PLAIN.toMetadata("request-metadata-".repeat(80))
        );

        const response = await pair.client.requestResponse({metadata}).block();

        expect(response?.data).toBe("accepted");
        expect(metadataOnlyRequestData).toBeUndefined();
    });

    it("fragments a metadata-only response while preserving its empty data value", async () => {
        pair = await connectTestPair(
            [MetadataOnlyResponseController],
            {maxFrameLength: 128},
            {maxFrameLength: 128}
        );

        const response = await pair.client.requestResponse({metadata: route("metadata-only-response")}).block();

        expect(response?.data).toBeUndefined();
        expect((response?.metadata as Metadata<string>[])[0]?.payload)
            .toBe("response-metadata-".repeat(80));
        expect(pair.serverTransport.sent.some((bytes) =>
            frameType(bytes) === FrameType.PAYLOAD && hasFollows(bytes)
        )).toBe(true);
    });

    it("reassembles and fragments every request-channel item independently", async () => {
        pair = await connectTestPair(
            [LargeChannelController],
            {maxFrameLength: 128},
            {maxFrameLength: 128}
        );
        const values = ["a".repeat(500), "b".repeat(600)];
        const responses = await pair.client.requestChannel(prependChannelPayload(
            {metadata: route("large-channel")},
            values.map((data) => ({data}))
        )).toArray();

        expect(responses.map(({data}) => data)).toEqual(values);
        expect(pair.clientTransport.sent.filter((bytes) =>
            frameType(bytes) === FrameType.PAYLOAD && hasFollows(bytes)
        ).length).toBeGreaterThan(0);
        expect(pair.serverTransport.sent.filter((bytes) =>
            frameType(bytes) === FrameType.PAYLOAD && hasFollows(bytes)
        ).length).toBeGreaterThan(0);
    });

    it("keeps small payloads in one request and one response frame", async () => {
        pair = await connectTestPair([LargeEchoController], {maxFrameLength: 512}, {maxFrameLength: 512});
        const response = await pair.client.requestResponse({data: "small", metadata: route("large-echo")}).block();

        expect(response?.data).toBe("small");
        expect(pair.clientTransport.sent.filter((bytes) => frameType(bytes) === FrameType.REQUEST_RESPONSE)).toHaveLength(1);
        expect(pair.serverTransport.sent.filter((bytes) => frameType(bytes) === FrameType.PAYLOAD)).toHaveLength(1);
    });
});

/** Reads the six-bit frame type from a raw RSocket header. */
function frameType(bytes: Uint8Array): FrameType {
    return (readFrameTypeAndFlags(bytes) >>> 10) as FrameType;
}

/** Reads the shared FOLLOWS flag from a fragmentable frame. */
function hasFollows(bytes: Uint8Array): boolean {
    return (readFrameTypeAndFlags(bytes) & PayloadFlag.FOLLOWS) !== 0;
}
