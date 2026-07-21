import {afterEach, describe, expect, it} from "vitest";
import {
    FireAndForgetController,
    RequestResponseController,
    RSocketServer
} from "@/index.js";
import {route} from "rsocket-core-ts";
import {WellKnownMimeType} from "rsocket-frames-ts";
import {connectTestPair, type ConnectedTestPair} from "./helpers.js";

class MultiRouteController extends RequestResponseController<number, number> {
    protected readonly route = ["account", "find"] as const;

    override handle(data: number): number {
        return data;
    }
}

class DirectRouteController extends RequestResponseController<string, string> {
    protected readonly route = "direct";

    override handle(data: string): string {
        return data;
    }
}

class DependencyController extends RequestResponseController<void, string> {
    protected readonly route = "dependency";

    constructor(private readonly value: string) {
        super();
    }

    override handle(): string {
        return this.value;
    }
}

describe("declarative server controllers", () => {
    let pair: ConnectedTestPair | undefined;

    afterEach(async () => {
        pair?.client.close();
        await pair?.server.close().block();
        pair = undefined;
    });

    it("matches an exact ordered sequence of routing tags", async () => {
        pair = await connectTestPair([MultiRouteController]);

        const response = await pair.client.requestResponse({data: 7, metadata: route("account", "find")}).block();

        expect(response?.data).toBe(7);
        await expect(pair.client.requestResponse({data: 7, metadata: route("find", "account")}).block())
            .rejects.toThrow("No request-response controller");
    });

    it("supports direct RSocket routing metadata without composite metadata", async () => {
        pair = await connectTestPair(
            [DirectRouteController],
            {},
            {metadataMimeType: WellKnownMimeType.MESSAGE_RSOCKET_ROUTING}
        );

        const response = await pair.client.requestResponse({data: "value", metadata: ["direct"]}).block();

        expect(response?.data).toBe("value");
    });

    it("accepts controller instances for dependency injection", async () => {
        pair = await connectTestPair([new DependencyController("injected")]);

        const response = await pair.client.requestResponse({metadata: route("dependency")}).block();

        expect(response?.data).toBe("injected");
    });

    it("allows the same route in different interaction namespaces", () => {
        class SameResponse extends RequestResponseController<void, void> {
            protected readonly route = "same";
            override handle(): void {
            }
        }
        class SameFireAndForget extends FireAndForgetController<void> {
            protected readonly route = "same";
            override handle(): void {
            }
        }

        expect(() => new RSocketServer({controllers: [SameResponse, SameFireAndForget]})).not.toThrow();
    });

    it("rejects duplicate routes within one interaction namespace", () => {
        class Duplicate extends RequestResponseController<void, void> {
            protected readonly route = ["account", "find"] as const;
            override handle(): void {
            }
        }

        expect(() => new RSocketServer({controllers: [MultiRouteController, Duplicate]}))
            .toThrow("Duplicate request-response");
    });

    it("rejects empty route tags while retaining an explicit route-less controller", () => {
        class Invalid extends RequestResponseController<void, void> {
            protected readonly route = [""] as const;
            override handle(): void {
            }
        }
        class RouteLess extends RequestResponseController<void, void> {
            protected readonly route = [] as const;
            override handle(): void {
            }
        }

        expect(() => new RSocketServer({controllers: [Invalid]})).toThrow("non-empty strings");
        expect(() => new RSocketServer({controllers: [RouteLess]})).not.toThrow();
    });
});
