import {afterEach, beforeEach, describe, expect, it} from "vitest";
import {route} from "rsocket-core-ts";
import {prependChannelPayload} from "./client-engine.js";
import {
    DoubleChannelController,
    EchoController,
    EmptyRouteController,
    fireAndForgetValues,
    RangeController,
    RecordController
} from "./controllers.js";
import {connectTestPair, type ConnectedTestPair} from "./helpers.js";

describe("RSocket server interactions through rsocket-client-ts", () => {
    let pair: ConnectedTestPair | undefined;

    beforeEach(() => {
        fireAndForgetValues.length = 0;
    });

    afterEach(async () => {
        pair?.client.close();
        await pair?.server.close().block();
        pair = undefined;
    });

    it("dispatches request-response by composite routing metadata", async () => {
        pair = await connectTestPair([EchoController]);
        const response = await pair.client.requestResponse({
            data: {id: 7, name: "Ada"},
            metadata: route("echo")
        }).block();

        expect(response?.data).toEqual({id: 7, name: "Ada"});
    });

    it("supports request-response without metadata or request data", async () => {
        pair = await connectTestPair([EmptyRouteController]);
        const response = await pair.client.requestResponse(undefined).block();

        expect(response?.data).toBe("empty");
    });

    it("processes fire-and-forget without emitting a response", async () => {
        pair = await connectTestPair([RecordController]);
        await pair.client.fireAndForget({data: 42, metadata: route("record")}).block();

        expect(fireAndForgetValues).toEqual([42]);
    });

    it("streams only requested response values", async () => {
        pair = await connectTestPair([RangeController]);
        const values = await pair.client.requestStream({data: 4, metadata: route("range")}).toArray();

        expect(values.map(({data}) => data)).toEqual([0, 1, 2, 3]);
    });

    it("applies independent request-channel demand in both directions", async () => {
        pair = await connectTestPair([DoubleChannelController]);
        const requests = prependChannelPayload(
            {metadata: route("double")},
            [{data: 1}, {data: 2}, {data: 3}]
        );
        const responses = await pair.client.requestChannel(requests).toArray();

        expect(responses.map(({data}) => data)).toEqual([2, 4, 6]);
    });
});
