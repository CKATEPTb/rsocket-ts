import {afterEach, describe, expect, it} from "vitest";
import type {ByteReader} from "bebyte";
import {
    Metadata,
    MetadataPushFrame,
    MimeType,
    WellKnownAuthType,
    WellKnownMimeType
} from "rsocket-frames-ts";
import {compositeMetadata, deserializeFrame, route, type RSocketPayloadInput} from "rsocket-core-ts";
import {RequestResponseController, type RSocketRequestContext} from "@/index.js";
import {connectTestPair, type ConnectedTestPair} from "./helpers.js";
import {ManualPublisher} from "./manual-publisher.js";
import {nextTurn, waitFor} from "./wait.js";

/** JSON-compatible metadata codec that rejects one malformed wire marker. */
class RejectingJsonMetadataMimeType extends MimeType<unknown> {
    /** Encodes valid metadata updates as JSON bytes. */
    protected override serializeMetadata(value: unknown): Metadata<unknown> {
        return WellKnownMimeType.APPLICATION_JSON.toMetadata(value) as Metadata<unknown>;
    }

    /** Rejects malformed bytes and otherwise delegates JSON decoding. */
    protected override deserializeMetadata(reader: ByteReader, hasPayload = true): Metadata<unknown> {
        const bytes = hasPayload ? reader.viewBytes(reader.i24()) : reader.viewRemaining();
        if (bytes[0] === 0xff) throw new TypeError("malformed application metadata");
        return WellKnownMimeType.APPLICATION_JSON.toMetadata(bytes, false) as Metadata<unknown>;
    }
}

class SecuredController extends RequestResponseController<void, string> {
    protected readonly route = "secured";

    override handle(_data: void, context: RSocketRequestContext): string {
        const entries = context.metadata as Metadata<any>[];
        const authentication = entries.find((entry) =>
            entry.mimeType === WellKnownMimeType.MESSAGE_RSOCKET_AUTHENTICATION
        );
        return authentication?.payload.data ?? "missing";
    }
}

describe("server metadata handling", () => {
    let pair: ConnectedTestPair | undefined;

    afterEach(async () => {
        pair?.client.close();
        await pair?.server.close().block();
        pair = undefined;
    });

    it("delivers direct JSON METADATA_PUSH as MIME-typed metadata", async () => {
        let received: Metadata<any> | undefined;
        pair = await connectTestPair([], {
            metadataPush: (metadata) => {
                received = metadata;
            }
        }, {metadataMimeType: WellKnownMimeType.APPLICATION_JSON});

        await pair.client.metadataPush({token: "updated"}).block();
        await waitFor(() => received !== undefined);

        expect(received).toBeInstanceOf(Metadata);
        expect(received?.mimeType).toBe(WellKnownMimeType.APPLICATION_JSON);
        expect(received?.payload).toEqual({token: "updated"});
    });

    it("isolates malformed metadata-push application content", async () => {
        let calls = 0;
        const metadataMimeType = new RejectingJsonMetadataMimeType("application/x-strict-json-metadata");
        pair = await connectTestPair([], {
            metadataPush: () => {
                calls += 1;
            }
        }, {metadataMimeType});

        pair.clientTransport.write(new MetadataPushFrame(
            new Metadata(WellKnownMimeType.APPLICATION_OCTET_STREAM, Uint8Array.of(0xff))
        ).toUint8Array());
        await nextTurn();

        expect(calls).toBe(0);
        expect(pair.clientTransport.isOpen).toBe(true);
        expect(pair.serverTransport.isOpen).toBe(true);

        await pair.client.metadataPush({token: "valid"}).block();
        await waitFor(() => calls === 1);
    });

    it("preserves authentication and route entries in composite request metadata", async () => {
        pair = await connectTestPair([SecuredController]);
        const authentication = WellKnownMimeType.MESSAGE_RSOCKET_AUTHENTICATION.toMetadata(
            WellKnownAuthType.BEARER.auth("access-token")
        );
        const response = await pair.client.requestResponse({
            metadata: compositeMetadata(authentication, route("secured"))
        }).block();

        expect(response?.data).toBe("access-token");
    });

    it("wraps a server MIME override inside negotiated composite metadata", async () => {
        pair = await connectTestPair([]);

        await pair.serverConnection.metadataPush("notice", WellKnownMimeType.TEXT_PLAIN).block();

        const bytes = pair.serverTransport.sent.at(-1) as Uint8Array;
        const frame = deserializeFrame(
            bytes,
            WellKnownMimeType.MESSAGE_RSOCKET_COMPOSITE_METADATA,
            WellKnownMimeType.APPLICATION_JSON
        ) as MetadataPushFrame;
        const entries = (frame.metadata as Metadata<Metadata<any>[]>).payload;
        expect(entries).toHaveLength(1);
        expect(entries[0]?.mimeType).toBe(WellKnownMimeType.TEXT_PLAIN);
        expect(entries[0]?.payload).toBe("notice");
    });

    it("tracks metadata-push publisher work until the server closes", async () => {
        const source = new ManualPublisher<void>();
        pair = await connectTestPair([], {metadataPush: () => source});

        await pair.client.metadataPush(route("background-metadata")).block();
        await waitFor(() => source.requested > 0);
        await pair.server.close().block();

        expect(source.cancelled).toBe(true);
        pair.client.close();
        pair = undefined;
    });

    it("supports metadata-only and data-only controller payloads independently", async () => {
        class ShapeController extends RequestResponseController<unknown, string> {
            protected readonly route = "shape";

            override handle(data: unknown, context: RSocketRequestContext): RSocketPayloadInput<string> {
                return {data: data === undefined ? "metadata-only" : "data-only", metadata: context.metadata};
            }
        }

        class DataOnlyController extends RequestResponseController<unknown, string> {
            protected readonly route = [] as const;

            override handle(): string {
                return "data-only";
            }
        }

        pair = await connectTestPair([ShapeController]);
        const metadataOnly = await pair.client.requestResponse({metadata: route("shape")}).block();
        const dataOnlyPair = await connectTestPair([DataOnlyController]);
        const dataOnly = await dataOnlyPair.client.requestResponse({data: 1}).block();

        expect(metadataOnly?.data).toBe("metadata-only");
        expect(metadataOnly?.metadata).toBeDefined();
        expect(dataOnly?.data).toBe("data-only");
        dataOnlyPair.client.close();
        await dataOnlyPair.server.close().block();
    });
});
