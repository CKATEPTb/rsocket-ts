/** Runtime contract for the deliberately small package root. */
import {expect, it} from "vitest";
import * as publicApi from "@/index.js";

it("exports only the browser facade and abstract controller classes", () => {
    expect(Object.keys(publicApi).sort()).toEqual([
        "FireAndForgetController",
        "RSocket",
        "RequestChannelController",
        "RequestResponseController",
        "RequestStreamController"
    ]);
});
