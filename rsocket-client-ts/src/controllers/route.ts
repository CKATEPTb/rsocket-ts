/**
 * Route metadata helpers for route-based declarative controllers.
 */
import {route, type RSocketPayloadInput} from "rsocket-core-ts";
import {prependChannelPayload} from "@/channel/input.js";
import type {RSocketChannelInput} from "@/client/types.js";
import type {RSocketControllerRoute} from "@/controllers/types.js";

/**
 * Cached builder for route-bearing payloads.
 */
export type RSocketRoutePayloadFactory = (payload?: unknown) => RSocketPayloadInput;

/**
 * Cached builder for route-prefixed request-channel inputs.
 */
export type RSocketRouteChannelInputFactory = (input: RSocketChannelInput<any, any>) => RSocketChannelInput;

/**
 * Creates an RSocket payload that carries Spring-compatible route metadata.
 *
 * When `payload` is omitted the returned payload contains only metadata, which
 * is useful as the first frame of a routed request-channel interaction.
 */
export function routePayload(controllerRoute: RSocketControllerRoute, payload?: unknown): RSocketPayloadInput {
    return routePayloadFactory(controllerRoute)(payload);
}

/**
 * Precomputes a routing entry that payload encoding adapts to direct or
 * composite metadata according to the configured metadata MIME.
 */
export function routePayloadFactory(controllerRoute: RSocketControllerRoute): RSocketRoutePayloadFactory {
    const metadata = routeEntry(controllerRoute);
    const metadataOnlyPayload = Object.freeze({metadata});
    return (payload?: unknown) => {
        if (payload === undefined) return metadataOnlyPayload;
        return {data: payload, metadata};
    };
}

/**
 * Precomputes route metadata for repeated request-channel input creation.
 */
export function routeChannelInputFactory(controllerRoute: RSocketControllerRoute): RSocketRouteChannelInputFactory {
    const routedPayload = routePayloadFactory(controllerRoute);
    return (input) => prependChannelPayload(routedPayload(), input);
}

/**
 * Creates the routing metadata entry for one configured controller route.
 */
function routeEntry(controllerRoute: RSocketControllerRoute) {
    return typeof controllerRoute === "string" ? route(controllerRoute) : route(...controllerRoute);
}

/**
 * Prepends route metadata to an outbound request-channel input source.
 */
export function routeChannelInput(
    controllerRoute: RSocketControllerRoute,
    input: RSocketChannelInput<any, any>
): RSocketChannelInput {
    return routeChannelInputFactory(controllerRoute)(input);
}
