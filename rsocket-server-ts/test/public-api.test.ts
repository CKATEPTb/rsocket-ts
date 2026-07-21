import {describe, expect, it} from "vitest";
import * as api from "@/index.js";

describe("rsocket-server-ts public API", () => {
    it("exports only the constructible server, declarative controllers, and request error", () => {
        expect(Object.keys(api).sort()).toEqual([
            "FireAndForgetController",
            "RSocketRequestError",
            "RSocketServer",
            "RequestChannelController",
            "RequestResponseController",
            "RequestStreamController"
        ]);
    });
});
