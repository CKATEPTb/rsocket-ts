/** Reactor-oriented WebSocket implementation of the core transport contract. */
import {
    ReactiveWebSocketTransportConnection,
    RSocketConnectionError,
    type RSocketTransportCloseOptions
} from "rsocket-core-ts";
import type {RSocketTransportFactory} from "@/client/types.js";
import {
    WS_CLOSE_NORMAL,
    WS_CLOSE_RSOCKET_PROTOCOL_ERROR
} from "@/websocket/constants.js";
import {openWebSocket} from "@/websocket/events.js";
import {defaultWebSocketFactory} from "@/websocket/factory.js";
import {
    type NormalizedWebSocketEndpoint,
    normalizeWebSocketEndpoint,
    validateWebSocketClose
} from "@/websocket/spec.js";
import type {RSocketWebSocket, RSocketWebSocketFactory} from "@/websocket/types.js";

/** Endpoint and implementation used by a reusable WebSocket transport factory. */
export interface RSocketWebSocketTransportOptions {
    /** Absolute `ws:` or `wss:` endpoint. */
    readonly url: string | URL;
    /** Optional subprotocol or ordered subprotocol list. */
    readonly protocols?: string | string[];
    /** Optional native-compatible WebSocket factory. */
    readonly webSocketFactory?: RSocketWebSocketFactory;
}

/** One physical WebSocket carrying complete raw RSocket frames. */
export class ReactiveWebSocketConnection extends ReactiveWebSocketTransportConnection<RSocketWebSocket> {
    /** Configures one native or custom WebSocket. */
    constructor(
        socket: RSocketWebSocket,
        timeoutMs: number | undefined,
        abortSignal?: AbortSignal
    ) {
        super(socket, openWebSocket(socket, timeoutMs, abortSignal), WS_CLOSE_RSOCKET_PROTOCOL_ERROR);
    }

    /** Closes the WebSocket after validating WHATWG close constraints. */
    override close(options: RSocketTransportCloseOptions = {}): void {
        const code = options.code ?? (options.error === true ? WS_CLOSE_RSOCKET_PROTOCOL_ERROR : WS_CLOSE_NORMAL);
        validateWebSocketClose(code);
        super.close({...options, code});
    }
}


/** Creates a reusable factory after validating and normalizing its endpoint once. */
export function createWebSocketTransport(options: RSocketWebSocketTransportOptions): RSocketTransportFactory {
    const endpoint = normalizeWebSocketEndpoint(options.url, options.protocols);
    const factory = options.webSocketFactory ?? defaultWebSocketFactory;
    return ({timeoutMs, abortSignal}) => createReactiveWebSocketConnection(
        factory,
        endpoint,
        timeoutMs,
        abortSignal
    );
}

/** Creates one physical WebSocket connection for a normalized endpoint. */
export function createReactiveWebSocketConnection(
    factory: RSocketWebSocketFactory,
    endpoint: NormalizedWebSocketEndpoint,
    timeoutMs: number | undefined,
    abortSignal?: AbortSignal
): ReactiveWebSocketConnection {
    let socket: RSocketWebSocket | undefined;
    try {
        socket = factory(endpoint.url, endpoint.protocols);
        return new ReactiveWebSocketConnection(socket, timeoutMs, abortSignal);
    } catch (error) {
        if (socket !== undefined) {
            try {
                socket.close(3000, "RSocket WebSocket initialization failed");
            } catch {
                // Preserve the initialization failure; cleanup errors are secondary.
            }
        }
        throw error instanceof RSocketConnectionError
            ? error
            : new RSocketConnectionError("WebSocket connection initialization failed", error);
    }
}
