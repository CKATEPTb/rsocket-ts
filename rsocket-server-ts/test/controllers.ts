import {Flux} from "reactor-core-ts";
import type {RSocketPayloadFrame} from "rsocket-core-ts";
import {
    FireAndForgetController,
    RequestChannelController,
    RequestResponseController,
    RequestStreamController
} from "@/index.js";

/** Shared fire-and-forget observations reset by each test. */
export const fireAndForgetValues: unknown[] = [];

/** Records one routed fire-and-forget value. */
export class RecordController extends FireAndForgetController<number> {
    protected readonly route = "record";

    override handle(data: number): void {
        fireAndForgetValues.push(data);
    }
}

/** Echoes arbitrary JSON request data. */
export class EchoController extends RequestResponseController<unknown, unknown> {
    protected readonly route = "echo";

    override handle(data: unknown): unknown {
        return data;
    }
}

/** Produces a finite numeric range. */
export class RangeController extends RequestStreamController<number, number> {
    protected readonly route = "range";

    override handle(count: number): Flux<number> {
        return Flux.range(0, count);
    }
}

/** Echoes each request-channel item after doubling its numeric data. */
export class DoubleChannelController extends RequestChannelController<number, number> {
    protected readonly route = "double";

    override handle(requests: Flux<RSocketPayloadFrame<number>>): Flux<number> {
        return requests.map((payload) => (payload.data as number) * 2);
    }
}

/** Handles request-response without routing metadata. */
export class EmptyRouteController extends RequestResponseController<void, string> {
    protected readonly route = [] as const;

    override handle(): string {
        return "empty";
    }
}
