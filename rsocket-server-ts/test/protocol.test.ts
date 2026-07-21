import {afterEach, describe, expect, it} from "vitest";
import {
    ErrorFrame,
    CancelFrame,
    ExtensionFlag,
    ExtensionFrame,
    FrameErrorCode,
    FrameFlag,
    FrameType,
    MetadataPushFrame,
    PayloadFlag,
    PayloadFrame,
    RequestNFrame,
    RequestResponseFlag,
    RequestResponseFrame,
    ResumeFrame,
    ResumeOkFrame,
    WellKnownMimeType
} from "rsocket-frames-ts";
import {deserializeFrame, errorPayload} from "rsocket-core-ts";
import {RSocketClient} from "./client-engine.js";
import {RSocketServer} from "@/index.js";
import {connectTestPair, type ConnectedTestPair, testClientOptions} from "./helpers.js";
import {memoryTransportPair} from "./memory-transport.js";
import {waitFor} from "./wait.js";

describe("server protocol sequencing", () => {
    let pair: ConnectedTestPair | undefined;

    afterEach(async () => {
        pair?.client.close();
        await pair?.server.close().block();
        pair = undefined;
    });

    it("requires SETUP or RESUME as the first frame on stream zero", async () => {
        const server = new RSocketServer({controllers: []});
        const transport = memoryTransportPair();
        const accepted = server.accept(transport.server).block().catch((error) => error);

        transport.client.write(new RequestResponseFrame(1, FrameFlag.NONE).toUint8Array());

        expect(await accepted).toMatchObject({message: expect.stringContaining("First RSocket frame")});
        expect(errorFrame(transport.server.sent[0] as Uint8Array).code).toBe(FrameErrorCode.INVALID_SETUP);
        expect(transport.client.isOpen).toBe(false);
        await server.close().block();
    });

    it("rejects unsupported protocol versions with UNSUPPORTED_SETUP", async () => {
        const server = new RSocketServer({controllers: []});
        const transport = memoryTransportPair();
        const accepted = server.accept(transport.server).block().catch((error) => error);
        const client = await RSocketClient.connect(testClientOptions(transport.client, {majorVersion: 2}));
        await waitFor(() => client.isClosed);

        expect(await accepted).toMatchObject({message: expect.stringContaining("Unsupported")});
        expect(errorFrame(transport.server.sent[0] as Uint8Array).code).toBe(FrameErrorCode.UNSUPPORTED_SETUP);
        await server.close().block();
    });

    it("rejects Resume-enabled SETUP when server retention is disabled", async () => {
        const server = new RSocketServer({controllers: []});
        const transport = memoryTransportPair();
        const accepted = server.accept(transport.server).block().catch((error) => error);
        const client = await RSocketClient.connect(testClientOptions(transport.client, {resumeToken: "disabled"}));
        await waitFor(() => client.isClosed);

        expect(await accepted).toMatchObject({message: expect.stringContaining("not enabled")});
        expect(errorFrame(transport.server.sent[0] as Uint8Array).code).toBe(FrameErrorCode.REJECTED_SETUP);
        await server.close().block();
    });

    it("classifies a malformed first RESUME frame as REJECTED_RESUME", async () => {
        const server = new RSocketServer({resume: {ttlMs: 1_000}});
        const transport = memoryTransportPair();
        const accepted = server.accept(transport.server).block().catch((error) => error);
        const resume = new ResumeFrame("malformed", 0n, 0n).toUint8Array();

        transport.client.write(resume.subarray(0, 12));

        await accepted;
        expect(errorFrame(transport.server.sent[0] as Uint8Array).code).toBe(FrameErrorCode.REJECTED_RESUME);
        await server.close().block();
    });

    it("ignores an unexpected RESUME_OK after SETUP", async () => {
        pair = await connectTestPair([]);

        pair.clientTransport.write(new ResumeOkFrame(0n).toUint8Array());
        await Promise.resolve();

        expect(pair.serverTransport.isOpen).toBe(true);
        expect(pair.serverTransport.sent.some((bytes) => frameType(bytes) === FrameType.ERROR)).toBe(false);
    });

    it("rejects a new RESUME frame on an established server connection", async () => {
        pair = await connectTestPair([]);

        pair.clientTransport.write(new ResumeFrame("unexpected", 0n, 0n).toUint8Array());
        await waitFor(() => !pair!.serverTransport.isOpen);

        expect(lastError(pair.serverTransport.sent).code).toBe(FrameErrorCode.CONNECTION_ERROR);
    });

    it("terminates the connection for an even requester stream ID", async () => {
        pair = await connectTestPair([]);

        pair.clientTransport.write(new RequestResponseFrame(2, FrameFlag.NONE).toUint8Array());
        await waitFor(() => !pair!.serverTransport.isOpen);

        expect(lastError(pair.serverTransport.sent).code).toBe(FrameErrorCode.CONNECTION_ERROR);
    });

    it("terminates the connection for a skipped requester stream ID", async () => {
        pair = await connectTestPair([]);

        pair.clientTransport.write(new RequestResponseFrame(3, FrameFlag.NONE).toUint8Array());
        await waitFor(() => !pair!.serverTransport.isOpen);

        expect(lastError(pair.serverTransport.sent).code).toBe(FrameErrorCode.CONNECTION_ERROR);
    });

    it.each([
        ["REQUEST_N", () => new RequestNFrame(0, 1).toUint8Array()],
        ["CANCEL", () => new CancelFrame(0).toUint8Array()],
        ["PAYLOAD", () => new PayloadFrame(0, PayloadFlag.NEXT).toUint8Array()]
    ])("ignores unknown stream-scoped %s on stream zero", async (_name, bytes) => {
        pair = await connectTestPair([]);

        pair.clientTransport.write(bytes());
        await Promise.resolve();

        expect(pair.serverTransport.isOpen).toBe(true);
        expect(pair.serverTransport.sent.some((frame) => frameType(frame) === FrameType.ERROR)).toBe(false);
    });

    it("ignores ERROR with a connection-only code on an unknown stream", async () => {
        pair = await connectTestPair([]);

        const invalid = new ErrorFrame(99, FrameErrorCode.APPLICATION_ERROR).toUint8Array();
        new DataView(invalid.buffer, invalid.byteOffset, invalid.byteLength)
            .setUint32(6, FrameErrorCode.CONNECTION_ERROR);
        pair.clientTransport.write(invalid);
        await Promise.resolve();

        expect(pair.serverTransport.isOpen).toBe(true);
        expect(pair.serverTransport.sent.some((frame) => frameType(frame) === FrameType.ERROR)).toBe(false);
    });

    it("ignores an unknown PAYLOAD before application MIME decoding", async () => {
        pair = await connectTestPair([]);
        const invalidJson = WellKnownMimeType.APPLICATION_OCTET_STREAM.toPayload(Uint8Array.of(0xff));

        pair.clientTransport.write(new PayloadFrame(99, PayloadFlag.NEXT, undefined, invalidJson).toUint8Array());
        await Promise.resolve();

        expect(pair.serverTransport.isOpen).toBe(true);
        expect(pair.serverTransport.sent.some((frame) => frameType(frame) === FrameType.ERROR)).toBe(false);
    });

    it("ignores a malformed initial request while its stream ID is being reassembled", async () => {
        pair = await connectTestPair([]);
        pair.clientTransport.write(new RequestResponseFrame(1, RequestResponseFlag.FOLLOWS).toUint8Array());
        const duplicate = new RequestResponseFrame(
            1,
            FrameFlag.NONE,
            WellKnownMimeType.APPLICATION_OCTET_STREAM.toMetadata(Uint8Array.of(1), false)
        ).toUint8Array().slice();
        duplicate[6] = 0x7f;

        pair.clientTransport.write(duplicate);
        await Promise.resolve();

        expect(pair.serverTransport.isOpen).toBe(true);
        expect(pair.serverTransport.sent.some((frame) => frameType(frame) === FrameType.ERROR)).toBe(false);
    });

    it("rejects raw frames larger than the configured server limit before decoding", async () => {
        pair = await connectTestPair([], {maxFrameLength: 128}, {maxFrameLength: 128});

        pair.clientTransport.write(new Uint8Array(129));
        await waitFor(() => !pair!.serverTransport.isOpen);

        expect(lastError(pair.serverTransport.sent).code).toBe(FrameErrorCode.CONNECTION_ERROR);
    });

    it("terminates rather than retaining a resumable session after a frame codec failure", async () => {
        pair = await connectTestPair(
            [],
            {resume: {ttlMs: 1_000}},
            {resumeToken: "malformed-frame"}
        );

        pair.serverTransport.failFrames(new RangeError("malformed frame body"));
        await waitFor(() => !pair!.serverTransport.isOpen);

        expect(lastError(pair.serverTransport.sent).code).toBe(FrameErrorCode.CONNECTION_ERROR);
        expect((pair.server as unknown as {sessions: Set<unknown>}).sessions.size).toBe(0);
    });

    it("ignores METADATA_PUSH on a non-zero stream", async () => {
        pair = await connectTestPair([]);
        const metadata = WellKnownMimeType.MESSAGE_RSOCKET_COMPOSITE_METADATA.toMetadata([]);

        pair.clientTransport.write(new MetadataPushFrame(metadata, 1).toUint8Array());
        await Promise.resolve();

        expect(pair.serverTransport.isOpen).toBe(true);
    });

    it("ignores repeated SETUP and handshake-only ERROR frames after acceptance", async () => {
        pair = await connectTestPair([]);
        const setup = pair.clientTransport.sent[0] as Uint8Array;

        pair.clientTransport.write(setup.subarray(0, 6));
        pair.clientTransport.write(new ErrorFrame(0, FrameErrorCode.REJECTED_SETUP).toUint8Array());
        await Promise.resolve();

        expect(pair.serverTransport.isOpen).toBe(true);
    });

    it("ignores an invalid metadata length marked IGNORE and preserves request sequencing", async () => {
        pair = await connectTestPair([]);
        const malformed = new RequestResponseFrame(
            1,
            FrameFlag.IGNORE,
            WellKnownMimeType.APPLICATION_OCTET_STREAM.toMetadata(Uint8Array.of(1), false)
        ).toUint8Array().slice();
        malformed[6] = 0x7f;

        pair.clientTransport.write(malformed);
        pair.clientTransport.write(new RequestResponseFrame(3, FrameFlag.NONE).toUint8Array());
        await waitFor(() => pair!.serverTransport.sent.length > 0);

        expect(pair.serverTransport.isOpen).toBe(true);
        expect(lastError(pair.serverTransport.sent).code).toBe(FrameErrorCode.REJECTED);
    });

    it("keeps an ignored fragmented request stream occupied until its final PAYLOAD", async () => {
        pair = await connectTestPair([]);
        const malformed = new RequestResponseFrame(
            1,
            FrameFlag.IGNORE | PayloadFlag.FOLLOWS,
            WellKnownMimeType.APPLICATION_OCTET_STREAM.toMetadata(Uint8Array.of(1), false)
        ).toUint8Array().slice();
        malformed[6] = 0x7f;

        pair.clientTransport.write(malformed);
        pair.clientTransport.write(new RequestResponseFrame(1, FrameFlag.NONE).toUint8Array());
        pair.clientTransport.write(new PayloadFrame(1, PayloadFlag.NEXT).toUint8Array());
        pair.clientTransport.write(new RequestResponseFrame(3, FrameFlag.NONE).toUint8Array());
        await waitFor(() => pair!.serverTransport.sent.length > 0);

        expect(pair.serverTransport.isOpen).toBe(true);
        expect(lastError(pair.serverTransport.sent).code).toBe(FrameErrorCode.REJECTED);
    });

    it("rejects a stream ERROR code on connection stream zero", async () => {
        pair = await connectTestPair([]);

        pair.clientTransport.write(errorBytesOnStream(FrameErrorCode.APPLICATION_ERROR, 1, 0));
        await waitFor(() => !pair!.serverTransport.isOpen);

        expect(lastError(pair.serverTransport.sent).code).toBe(FrameErrorCode.CONNECTION_ERROR);
    });

    it("ignores unknown extensions only when the IGNORE flag is set", async () => {
        pair = await connectTestPair([]);

        pair.clientTransport.write(new ExtensionFrame(
            0,
            ExtensionFlag.IGNORE,
            777,
            undefined,
            WellKnownMimeType.APPLICATION_OCTET_STREAM.toPayload(Uint8Array.of(0xff))
        ).toUint8Array());
        await new Promise((resolve) => setTimeout(resolve, 0));

        expect(pair.clientTransport.isOpen).toBe(true);
    });

    it("rejects required unknown extensions", async () => {
        pair = await connectTestPair([]);

        pair.clientTransport.write(new ExtensionFrame(0, ExtensionFlag.NONE, 777).toUint8Array());
        await waitFor(() => !pair!.serverTransport.isOpen);

        expect(lastError(pair.serverTransport.sent).code).toBe(FrameErrorCode.CONNECTION_ERROR);
    });
});

/** Decodes one server ERROR using the test connection MIME types. */
function errorFrame(bytes: Uint8Array): ErrorFrame {
    return deserializeFrame(
        bytes,
        WellKnownMimeType.MESSAGE_RSOCKET_COMPOSITE_METADATA,
        WellKnownMimeType.APPLICATION_JSON
    ) as ErrorFrame;
}

/** Returns the final ERROR emitted before protocol termination. */
function lastError(frames: readonly Uint8Array[]): ErrorFrame {
    return errorFrame(frames.at(-1) as Uint8Array);
}

/** Reads a frame type directly from its header. */
function frameType(bytes: Uint8Array): FrameType {
    return (bytes[4] as number) >>> 2;
}

/** Rewrites a validated ERROR frame to exercise malformed wire input. */
function errorBytesOnStream(code: FrameErrorCode, validStreamId: number, invalidStreamId: number): Uint8Array {
    const bytes = new ErrorFrame(validStreamId, code, errorPayload("bad scope")).toUint8Array();
    bytes[0] = invalidStreamId >>> 24 & 0x7f;
    bytes[1] = invalidStreamId >>> 16;
    bytes[2] = invalidStreamId >>> 8;
    bytes[3] = invalidStreamId;
    return bytes;
}
