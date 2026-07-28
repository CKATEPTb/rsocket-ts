/** Lazy transport selection for the public requester facade. */
import type {RSocketTransportFactory} from "@/client/types.js";
import type {
    RSocketTransportOptions,
    RSocketWebSocketOptions,
    RSocketWebTransportOptions
} from "@/rsocket/options.js";
import {assertTcpAddress} from "@/tcp/options.js";
import {createWebSocketTransport} from "@/websocket/connection.js";
import {createWebTransport} from "@/webtransport/connection.js";

/** Lazily returns one reusable physical transport factory. TCP loading may be asynchronous. */
export type RSocketTransportProvider = () => RSocketTransportFactory | Promise<RSocketTransportFactory>;

/** Creates a provider without loading Node TCP code in browsers. */
export function transportProvider(
    options: RSocketTransportOptions,
    maxFrameLength?: number
): RSocketTransportProvider {
    if (options.type === "websocket") {
        let resolved: RSocketTransportFactory | undefined;
        return () => resolved ??= webSocketTransport(options);
    }
    if (options.type === "webtransport") {
        let resolved: RSocketTransportFactory | undefined;
        return () => resolved ??= webTransport(options, maxFrameLength);
    }

    let resolved: Promise<RSocketTransportFactory> | undefined;
    return () => resolved ??= import("@/tcp/connection.js")
        .then(({createTcpTransport}) => createTcpTransport({
            host: options.host,
            port: options.port,
            ...(maxFrameLength === undefined ? {} : {maxFrameLength})
        }));
}

/** Validates immutable transport settings before reconnect scheduling starts. */
export function validateTransportOptions(options: RSocketTransportOptions, resumeEnabled = false): void {
    if (options.type === "websocket") {
        webSocketTransport(options);
        return;
    }
    if (options.type === "webtransport") {
        webTransport(options);
        if (resumeEnabled && options.unreliableFireAndForget === true) {
            throw new TypeError("WebTransport unreliableFireAndForget is incompatible with RSocket Resume");
        }
        return;
    }
    assertTcpAddress(options);
}

/** Builds the browser-safe WebSocket factory from one public option object. */
function webSocketTransport(options: RSocketWebSocketOptions): RSocketTransportFactory {
    return createWebSocketTransport({
        url: options.url,
        ...(options.protocols === undefined ? {} : {protocols: options.protocols}),
        ...(options.factory === undefined ? {} : {webSocketFactory: options.factory})
    });
}

/** Builds the WebTransport factory without loading Node-only code. */
function webTransport(
    options: RSocketWebTransportOptions,
    maxFrameLength?: number
): RSocketTransportFactory {
    // Kept as a static import path so browser bundlers can tree-shake TCP independently.
    return createWebTransportFactory(options, maxFrameLength);
}

/** Isolated import target keeps option conversion out of the provider branches. */
function createWebTransportFactory(
    options: RSocketWebTransportOptions,
    maxFrameLength?: number
): RSocketTransportFactory {
    return createWebTransport({
        url: options.url,
        ...(maxFrameLength === undefined ? {} : {maxFrameLength}),
        ...(options.maxReorderBufferBytes === undefined ? {} : {
            maxReorderBufferBytes: options.maxReorderBufferBytes
        }),
        ...(options.unreliableFireAndForget === undefined ? {} : {
            unreliableFireAndForget: options.unreliableFireAndForget
        }),
        ...(options.factory === undefined ? {} : {factory: options.factory}),
        ...(options.media === undefined ? {} : {media: options.media})
    });
}
