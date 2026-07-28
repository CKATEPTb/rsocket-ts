/** Throughput regressions for frame serialization and transport decoding hot paths. */
import {describe, expect, it} from "vitest";
import {
    FrameCodec,
    PayloadFlag,
    PayloadFrame,
    WellKnownMimeType
} from "@/index";

const ROUND_TRIPS = 25_000;
const SERIALIZATIONS = 10_000;
const COMPOSITE_SERIALIZATIONS = 25_000;
const MAX_TEST_DURATION_MS = 10_000;

/** Creates the WebSocket codec used by deterministic frame benchmarks. */
function performanceCodec(): FrameCodec<string, {readonly sequence: number; readonly value: string}> {
    return new FrameCodec({
        transport: "websocket",
        mimetype: {
            metadata: WellKnownMimeType.TEXT_PLAIN,
            data: WellKnownMimeType.APPLICATION_JSON
        }
    });
}

describe("frame codec performance", () => {
    it("round-trips decoded frames without accumulating global cache state", () => {
        const codec = performanceCodec();
        const encoded = codec.serialize(new PayloadFrame(
            1,
            PayloadFlag.NEXT,
            WellKnownMimeType.TEXT_PLAIN.toMetadata("trace"),
            WellKnownMimeType.APPLICATION_JSON.toPayload({sequence: 7, value: "payload"})
        ));
        const started = performance.now();
        let checksum = 0;

        for (let index = 0; index < ROUND_TRIPS; index += 1) {
            const decoded = codec.deserialize(encoded)[0] as PayloadFrame;
            const repeated = codec.serialize(decoded);
            checksum += repeated.byteLength + decoded.header.streamId;
        }

        expect(checksum).toBe(ROUND_TRIPS * (encoded.byteLength + 1));
        expect(performance.now() - started).toBeLessThan(MAX_TEST_DURATION_MS);
    });

    it("serializes payload-heavy frames within a bounded throughput budget", () => {
        const codec = performanceCodec();
        const payload = WellKnownMimeType.APPLICATION_JSON.toPayload({
            sequence: 1,
            value: "x".repeat(4_096)
        });
        const metadata = WellKnownMimeType.TEXT_PLAIN.toMetadata("performance");
        const started = performance.now();
        let bytes = 0;

        for (let index = 0; index < SERIALIZATIONS; index += 1) {
            bytes += codec.serialize(new PayloadFrame(index * 2 + 1, PayloadFlag.NEXT, metadata, payload)).byteLength;
        }

        expect(bytes).toBeGreaterThan(SERIALIZATIONS * 4_096);
        expect(performance.now() - started).toBeLessThan(MAX_TEST_DURATION_MS);
    });

    it("serializes common composite metadata without repeated writer growth", () => {
        const entries = [
            WellKnownMimeType.MESSAGE_RSOCKET_ROUTING.toMetadata(["account.sign-in"]),
            WellKnownMimeType.TEXT_PLAIN.toMetadata("trace-id")
        ];
        const expectedLength = WellKnownMimeType.MESSAGE_RSOCKET_COMPOSITE_METADATA
            .toMetadata(entries)
            .toUint8Array()
            .byteLength;
        const started = performance.now();
        let bytes = 0;

        for (let index = 0; index < COMPOSITE_SERIALIZATIONS; index += 1) {
            bytes += WellKnownMimeType.MESSAGE_RSOCKET_COMPOSITE_METADATA
                .toMetadata(entries)
                .toUint8Array()
                .byteLength;
        }

        expect(bytes).toBe(COMPOSITE_SERIALIZATIONS * expectedLength);
        expect(performance.now() - started).toBeLessThan(MAX_TEST_DURATION_MS);
    });
});
