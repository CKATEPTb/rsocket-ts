/**
 * Browser WebSocket factory.
 */
import {RSocketConnectionError} from "rsocket-core-ts";
import type {RSocketWebSocket} from "@/websocket/types.js";

/**
 * Creates a native browser `WebSocket`.
 */
export function defaultWebSocketFactory(url: string | URL, protocols?: string | string[]): RSocketWebSocket {
    if (typeof WebSocket === "undefined") {
        throw new RSocketConnectionError("WebSocket is not available in this runtime");
    }
    return (protocols === undefined ? new WebSocket(url) : new WebSocket(url, protocols)) as RSocketWebSocket;
}
