/** Half-close behavior of the high-level imperative request-channel helper. */
import {describe, expect, it} from "vitest";
import type {RSocketPayloadFrame} from "rsocket-core-ts";
import {RSocketChannel} from "@/channel/sink.js";
import {RSocketFlux} from "@/stream/index.js";

/** Creates a response Flux that completes on its first demand signal. */
function completedResponses<D, M>(): RSocketFlux<RSocketPayloadFrame<D, M>> {
    return new RSocketFlux<RSocketPayloadFrame<D, M>>((subscriber) => {
        let terminated = false;
        return {
            request: () => {
                if (terminated) return;
                terminated = true;
                subscriber.onComplete();
            },
            cancel: () => {
                terminated = true;
            }
        };
    });
}

describe("RSocketChannel sink", () => {
    it("allows request payloads after the response half completes", async () => {
        const channel = new RSocketChannel<number, unknown>(() => completedResponses());

        await channel.responses.toArray();

        expect(() => channel.next({data: 1})).not.toThrow();
        channel.complete();
        expect(() => channel.next({data: 2})).toThrow("outbound side is closed");
    });
});
