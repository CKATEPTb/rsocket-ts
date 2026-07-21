/** RSocket routing metadata parsing and canonical route helpers. */
import {Metadata, WellKnownMimeType} from "rsocket-frames-ts";

const EMPTY_ROUTE: readonly string[] = Object.freeze([]);
const ROUTING_MIME = WellKnownMimeType.MESSAGE_RSOCKET_ROUTING.mimeType;
const COMPOSITE_MIME = WellKnownMimeType.MESSAGE_RSOCKET_COMPOSITE_METADATA.mimeType;
const ROUTE_ENCODER = new TextEncoder();
const MAX_ROUTE_BYTES = 0xff;

/** One routing tag or an exact ordered routing-tag sequence. */
export type RSocketRoute = string | readonly string[];

/** Returns routing tags from direct or composite RSocket metadata. */
export function routingTags(metadata: Metadata<any> | unknown): readonly string[] {
    if (!(metadata instanceof Metadata)) return EMPTY_ROUTE;
    if (metadata.mimeType.mimeType === ROUTING_MIME) return validTags(metadata.payload);
    if (metadata.mimeType.mimeType !== COMPOSITE_MIME || !Array.isArray(metadata.payload)) return EMPTY_ROUTE;
    for (const entry of metadata.payload) {
        if (entry instanceof Metadata && entry.mimeType.mimeType === ROUTING_MIME) {
            return validTags(entry.payload);
        }
    }
    return EMPTY_ROUTE;
}

/** Validates and freezes one route for repeated endpoint lookup or encoding. */
export function normalizeRSocketRoute(
    route: RSocketRoute,
    allowEmpty = true
): readonly string[] {
    const tags = typeof route === "string" ? [route] : [...route];
    if (!allowEmpty && tags.length === 0) {
        throw new TypeError("RSocket route must contain at least one non-empty string");
    }
    for (const tag of tags) {
        if (typeof tag !== "string" || tag.length === 0) {
            throw new TypeError("RSocket routes must contain only non-empty strings");
        }
        if (ROUTE_ENCODER.encode(tag).byteLength > MAX_ROUTE_BYTES) {
            throw new TypeError(`RSocket route tags must be at most ${MAX_ROUTE_BYTES} UTF-8 bytes`);
        }
    }
    return tags.length === 0 ? EMPTY_ROUTE : Object.freeze(tags);
}

/** Narrows decoded metadata to a valid routing tag list. */
function validTags(value: unknown): readonly string[] {
    if (!Array.isArray(value) || !value.every((tag) => typeof tag === "string")) return EMPTY_ROUTE;
    return value;
}
