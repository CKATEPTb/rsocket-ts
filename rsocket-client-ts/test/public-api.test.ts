/** Runtime contract for the deliberately small requester package root. */
import {describe, expect, it} from "vitest";
import * as api from "@";

describe("public client API", () => {
    it("exports only RSocket and the four abstract controller classes", () => {
        expect(Object.keys(api).sort()).toEqual([
            "FireAndForgetController",
            "RSocket",
            "RequestChannelController",
            "RequestResponseController",
            "RequestStreamController"
        ]);
    });
});
