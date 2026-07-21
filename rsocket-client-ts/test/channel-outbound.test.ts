/** Request-channel outbound cancellation and iterator ownership regressions. */
import {describe, expect, it} from "vitest";
import {WellKnownMimeType} from "rsocket-frames-ts";
import type {RSocketPayloadInput} from "rsocket-core-ts";
import {RequestChannelOutbound} from "@/channel/outbound.js";
import type {StreamSession} from "@/stream/index.js";

describe("request-channel outbound", () => {
    it("does not complete an empty source after reentrant cancellation of its initial frame", () => {
        let completes = 0;
        let outbound!: RequestChannelOutbound;
        outbound = new RequestChannelOutbound(
            {waitUntilWritable: () => undefined} as unknown as StreamSession,
            [],
            WellKnownMimeType.APPLICATION_JSON,
            WellKnownMimeType.MESSAGE_RSOCKET_COMPOSITE_METADATA,
            () => {
                outbound.abort();
                return 1;
            },
            () => {
                completes += 1;
            },
            () => undefined
        );

        outbound.start();

        expect(completes).toBe(0);
    });

    it("does not read ahead after a reentrant peer cancellation", () => {
        let reads = 0;
        let returns = 0;
        const input: Iterable<RSocketPayloadInput> = {
            [Symbol.iterator]() {
                return {
                    next() {
                        reads += 1;
                        return {done: false, value: {data: reads}};
                    },
                    return() {
                        returns += 1;
                        return {done: true, value: undefined};
                    }
                };
            }
        };
        let outbound!: RequestChannelOutbound;
        const session = {
            sendFrame: () => outbound.abort(),
            waitUntilWritable: () => undefined
        } as unknown as StreamSession;
        outbound = new RequestChannelOutbound(
            session,
            input,
            WellKnownMimeType.APPLICATION_JSON,
            WellKnownMimeType.MESSAGE_RSOCKET_COMPOSITE_METADATA,
            () => 1,
            () => undefined,
            () => undefined
        );
        outbound.addDemand(1);

        outbound.start();

        expect(reads).toBe(2);
        expect(returns).toBe(1);
    });
});
