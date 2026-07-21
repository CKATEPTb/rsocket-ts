import {afterEach, beforeEach, describe, expect, it} from "vitest";
import type {ByteReader} from "bebyte";
import {
    ErrorFrame,
    FrameErrorCode,
    FrameFlag,
    FrameType,
    MetadataPushFrame,
    MimeType,
    Payload,
    PayloadFrame,
    RequestChannelFrame,
    RequestFireAndForgetFrame,
    RequestResponseFrame,
    RequestStreamFrame,
    WellKnownMimeType,
    type Frame
} from "rsocket-frames-ts";
import {decodeFramePayload, deserializeFrame, encodeMetadataInput, route} from "rsocket-core-ts";
import {prependChannelPayload} from "./client-engine.js";
import {
    DoubleChannelController,
    EchoController,
    fireAndForgetValues,
    RangeController,
    RecordController
} from "./controllers.js";
import {connectTestPair, type ConnectedTestPair} from "./helpers.js";
import {waitFor} from "./wait.js";

/** JSON-compatible setup codec that rejects one malformed wire marker. */
class RejectingJsonMimeType extends MimeType<unknown> {
    /** Encodes controller responses as ordinary JSON. */
    protected override serializePayload(value: unknown): Payload<unknown> {
        return WellKnownMimeType.APPLICATION_JSON.toPayload(value) as Payload<unknown>;
    }

    /** Rejects malformed request bytes without making them a connection error. */
    protected override deserializePayload(reader: ByteReader): Payload<unknown> {
        const bytes = reader.viewRemaining();
        if (bytes[0] === 0xff) throw new TypeError("malformed application payload");
        return WellKnownMimeType.APPLICATION_JSON.toPayload(bytes) as Payload<unknown>;
    }
}

describe("RSocket interaction frame sequences", () => {
    let pair: ConnectedTestPair | undefined;

    beforeEach(() => {
        fireAndForgetValues.length = 0;
    });

    afterEach(async () => {
        pair?.client.close();
        await pair?.server.close().block();
        pair = undefined;
    });

    it("uses sequential odd stream IDs and specification terminal frames", async () => {
        pair = await connectTestPair([
            RecordController,
            EchoController,
            RangeController,
            DoubleChannelController
        ]);
        pair.clientTransport.sent.length = 0;
        pair.serverTransport.sent.length = 0;

        await pair.client.fireAndForget({data: 1, metadata: route("record")}).block();
        await pair.client.requestResponse({data: "response", metadata: route("echo")}).block();
        await pair.client.requestStream({data: 2, metadata: route("range")}).toArray();
        await pair.client.requestChannel(prependChannelPayload(
            {metadata: route("double")},
            [{data: 2}, {data: 3}]
        )).toArray();
        await pair.client.metadataPush(route("connection.metadata")).block();

        const outgoing = pair.clientTransport.sent.map(decode);
        const incoming = pair.serverTransport.sent.map(decode);
        const initial = outgoing.filter((frame) =>
            frame.type >= FrameType.REQUEST_RESPONSE && frame.type <= FrameType.REQUEST_CHANNEL
        );

        expect(initial.map(({header}) => header.streamId)).toEqual([1, 3, 5, 7]);
        expect(initial[0]).toBeInstanceOf(RequestFireAndForgetFrame);
        expect(initial[1]).toBeInstanceOf(RequestResponseFrame);
        expect(initial[2]).toBeInstanceOf(RequestStreamFrame);
        expect(initial[3]).toBeInstanceOf(RequestChannelFrame);
        expect((initial[2] as RequestStreamFrame).request).toBeGreaterThan(0);
        expect((initial[3] as RequestChannelFrame).request).toBeGreaterThan(0);

        const response = incoming.find(({header}) => header.streamId === 3) as PayloadFrame;
        expect(response).toBeInstanceOf(PayloadFrame);
        expect(response.isNext()).toBe(true);
        expect(response.isComplete()).toBe(true);

        const streamPayloads = incoming.filter(({header, type}) =>
            header.streamId === 5 && type === FrameType.PAYLOAD
        ) as PayloadFrame[];
        expect(streamPayloads.filter((frame) => frame.isNext())).toHaveLength(2);
        expect(streamPayloads.at(-1)?.isComplete()).toBe(true);

        expect(incoming.some(({header, type}) =>
            header.streamId === 7 && type === FrameType.REQUEST_N
        )).toBe(true);
        expect(outgoing.some(({header, type}) =>
            header.streamId === 7 && type === FrameType.PAYLOAD
        )).toBe(true);
        expect(outgoing.at(-1)).toBeInstanceOf(MetadataPushFrame);
        expect(outgoing.at(-1)?.header.streamId).toBe(0);
    });

    it("rejects malformed application data per stream and keeps the connection usable", async () => {
        pair = await connectTestPair([EchoController], {}, {
            dataMimeType: new RejectingJsonMimeType("application/x-server-strict-json")
        });
        pair.serverTransport.sent.length = 0;
        const metadata = encodeMetadataInput(
            route("echo"),
            WellKnownMimeType.MESSAGE_RSOCKET_COMPOSITE_METADATA
        );

        pair.clientTransport.write(new RequestResponseFrame(
            1,
            FrameFlag.NONE,
            metadata,
            WellKnownMimeType.APPLICATION_OCTET_STREAM.toPayload(Uint8Array.of(0xff))
        ).toUint8Array());
        await waitFor(() => pair!.serverTransport.sent.length === 1);

        const rejection = decode(pair.serverTransport.sent[0] as Uint8Array) as ErrorFrame;
        expect(rejection).toBeInstanceOf(ErrorFrame);
        expect(rejection.header.streamId).toBe(1);
        expect(rejection.code).toBe(FrameErrorCode.INVALID);
        expect(pair.clientTransport.isOpen).toBe(true);
        expect(pair.serverTransport.isOpen).toBe(true);

        pair.clientTransport.write(new RequestResponseFrame(
            3,
            FrameFlag.NONE,
            metadata,
            WellKnownMimeType.APPLICATION_JSON.toPayload("still-open")
        ).toUint8Array());
        await waitFor(() => pair!.serverTransport.sent.length === 2);

        const response = decode(pair.serverTransport.sent[1] as Uint8Array) as PayloadFrame;
        expect(response.header.streamId).toBe(3);
        expect(decodeFramePayload(response).data).toBe("still-open");
    });

    it("drops a malformed fire-and-forget payload without a response frame", async () => {
        pair = await connectTestPair([RecordController, EchoController], {}, {
            dataMimeType: new RejectingJsonMimeType("application/x-server-fnf-strict-json")
        });
        pair.serverTransport.sent.length = 0;
        const composite = WellKnownMimeType.MESSAGE_RSOCKET_COMPOSITE_METADATA;

        pair.clientTransport.write(new RequestFireAndForgetFrame(
            1,
            FrameFlag.NONE,
            encodeMetadataInput(route("record"), composite),
            WellKnownMimeType.APPLICATION_OCTET_STREAM.toPayload(Uint8Array.of(0xff))
        ).toUint8Array());
        await new Promise((resolve) => setTimeout(resolve, 0));

        expect(pair.serverTransport.sent).toHaveLength(0);
        expect(fireAndForgetValues).toHaveLength(0);
        expect(pair.serverTransport.isOpen).toBe(true);

        pair.clientTransport.write(new RequestResponseFrame(
            3,
            FrameFlag.NONE,
            encodeMetadataInput(route("echo"), composite),
            WellKnownMimeType.APPLICATION_JSON.toPayload("next-request")
        ).toUint8Array());
        await waitFor(() => pair!.serverTransport.sent.length === 1);
        expect(decode(pair.serverTransport.sent[0] as Uint8Array).header.streamId).toBe(3);
    });
});

/** Decodes one integration-test frame with negotiated MIME codecs. */
function decode(bytes: Uint8Array): Frame {
    return deserializeFrame(
        bytes,
        WellKnownMimeType.MESSAGE_RSOCKET_COMPOSITE_METADATA,
        WellKnownMimeType.APPLICATION_JSON
    );
}
