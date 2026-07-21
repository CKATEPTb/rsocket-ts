/**
 * WebSocket API validation helpers used before custom or native socket creation.
 *
 * The browser owns the HTTP upgrade, frame masking, ping/pong handling, and
 * wire-level frame validation. These helpers keep the user-supplied transport
 * boundary aligned with the browser `WebSocket` constructor and `close()` API.
 */
import {RSocketConnectionError} from "rsocket-core-ts";

/**
 * Endpoint values after applying browser WebSocket URL and protocol validation.
 */
export interface NormalizedWebSocketEndpoint {
    /** Absolute `ws:` or `wss:` URL passed to the WebSocket factory. */
    readonly url: string;
    /** Validated subprotocol argument passed to the WebSocket factory. */
    readonly protocols?: string | string[];
}

/**
 * HTTP token character set used by `Sec-WebSocket-Protocol` values.
 */
const WEB_SOCKET_PROTOCOL_TOKEN = /^[!#$%&'*+\-.^_`|~0-9A-Za-z]+$/;

/**
 * Maximum UTF-8 byte length accepted by browser `WebSocket.close()` reasons.
 */
const WEB_SOCKET_CLOSE_REASON_MAX_BYTES = 123;

/**
 * Reused encoder for close reason byte-length validation.
 */
const WEB_SOCKET_CLOSE_REASON_ENCODER = new TextEncoder();

/**
 * Normalizes and validates constructor arguments according to browser WebSocket
 * URL and subprotocol rules.
 */
export function normalizeWebSocketEndpoint(
    url: string | URL,
    protocols?: string | string[]
): NormalizedWebSocketEndpoint {
    const parsed = parseWebSocketUrl(url);
    normalizeWebSocketScheme(parsed);
    rejectWebSocketCredentials(parsed);
    rejectWebSocketFragment(parsed);

    const normalizedProtocols = protocols === undefined
        ? undefined
        : normalizeWebSocketProtocols(protocols);

    return normalizedProtocols === undefined
        ? {url: parsed.href}
        : {url: parsed.href, protocols: normalizedProtocols};
}

/** Rejects user-info because the browser WebSocket constructor forbids it. */
function rejectWebSocketCredentials(url: URL): void {
    if (url.username !== "" || url.password !== "") {
        throw new RSocketConnectionError("Invalid WebSocket URL. Credentials are not allowed.");
    }
}

/**
 * Validates `WebSocket.close(code, reason)` arguments before the underlying
 * socket sees them.
 */
export function validateWebSocketClose(code?: number, reason?: string): void {
    if (code !== undefined && (!Number.isInteger(code) || (code !== 1000 && (code < 3000 || code > 4999)))) {
        throw new RSocketConnectionError(
            `Invalid WebSocket close code ${code}. Use 1000 or an application code from 3000 to 4999.`
        );
    }

    if (reason !== undefined && WEB_SOCKET_CLOSE_REASON_ENCODER.encode(reason).byteLength > WEB_SOCKET_CLOSE_REASON_MAX_BYTES) {
        throw new RSocketConnectionError(
            `Invalid WebSocket close reason. UTF-8 length must be ${WEB_SOCKET_CLOSE_REASON_MAX_BYTES} bytes or less.`
        );
    }
}

/**
 * Parses a WebSocket URL using the current document URL as the browser base
 * when one exists.
 */
function parseWebSocketUrl(url: string | URL): URL {
    try {
        return new URL(url, currentBrowserBaseUrl());
    } catch (error) {
        throw new RSocketConnectionError("Invalid WebSocket URL", error);
    }
}

/**
 * Returns the browser location used for resolving relative WebSocket URLs.
 */
function currentBrowserBaseUrl(): string | undefined {
    return typeof globalThis.location === "object" && globalThis.location !== null
        ? globalThis.location.href
        : undefined;
}

/**
 * Applies the browser constructor's `http:` to `ws:` and `https:` to `wss:`
 * URL scheme mapping, then rejects non-WebSocket schemes.
 */
function normalizeWebSocketScheme(url: URL): void {
    if (url.protocol === "http:") {
        url.protocol = "ws:";
        return;
    }

    if (url.protocol === "https:") {
        url.protocol = "wss:";
        return;
    }

    if (url.protocol !== "ws:" && url.protocol !== "wss:") {
        throw new RSocketConnectionError("Invalid WebSocket URL scheme. Use ws:, wss:, http:, or https:.");
    }
}

/**
 * Rejects URL fragments, including an empty trailing fragment marker.
 */
function rejectWebSocketFragment(url: URL): void {
    if (url.hash !== "" || url.href.includes("#")) {
        throw new RSocketConnectionError("Invalid WebSocket URL. Fragments are not allowed.");
    }
}

/**
 * Validates the optional subprotocol argument and preserves caller shape.
 */
function normalizeWebSocketProtocols(protocols: string | string[]): string | string[] {
    if (typeof protocols === "string") {
        validateWebSocketProtocol(protocols);
        return protocols;
    }

    if (protocols.length <= 1) {
        if (protocols.length === 1) validateWebSocketProtocol(protocols[0] as string);
        return protocols.slice();
    }

    const seen = new Set<string>();
    for (const protocol of protocols) {
        validateWebSocketProtocol(protocol);
        if (seen.has(protocol)) {
            throw new RSocketConnectionError(`Duplicate WebSocket protocol "${protocol}"`);
        }
        seen.add(protocol);
    }
    return protocols.slice();
}

/**
 * Validates one `Sec-WebSocket-Protocol` token.
 */
function validateWebSocketProtocol(protocol: string): void {
    if (typeof protocol !== "string" || !WEB_SOCKET_PROTOCOL_TOKEN.test(protocol)) {
        throw new RSocketConnectionError(`Invalid WebSocket protocol "${protocol}"`);
    }
}
