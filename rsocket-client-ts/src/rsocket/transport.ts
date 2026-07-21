/** Lazy transport selection for the public requester facade. */
import type {RSocketTransportFactory} from "@/client/types.js";
import type {RSocketTransportOptions, RSocketWebSocketOptions} from "@/rsocket/options.js";
import {assertTcpAddress} from "@/tcp/options.js";
import {createWebSocketTransport} from "@/websocket/connection.js";

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

    let resolved: Promise<RSocketTransportFactory> | undefined;
    return () => resolved ??= import("@/tcp/connection.js")
        .then(({createTcpTransport}) => createTcpTransport({
            host: options.host,
            port: options.port,
            ...(maxFrameLength === undefined ? {} : {maxFrameLength})
        }));
}

/** Validates immutable transport settings before reconnect scheduling starts. */
export function validateTransportOptions(options: RSocketTransportOptions): void {
    if (options.type === "websocket") {
        webSocketTransport(options);
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
