/** Core transport adapter for one accepted WebSocket. */
import {Mono} from "reactor-core-ts";
import {
    ReactiveWebSocketTransportConnection,
    RSocketConnectionError
} from "rsocket-core-ts";
import type {RSocketAcceptedWebSocket} from "@/websocket/types.js";

const OPEN = 1;

/** Accepted WebSocket where every binary message is one raw RSocket frame. */
export class ReactiveAcceptedWebSocketConnection
    extends ReactiveWebSocketTransportConnection<RSocketAcceptedWebSocket> {
    /** Wraps an already accepted WHATWG or Node-compatible socket. */
    constructor(socket: RSocketAcceptedWebSocket) {
        const opened = Mono.create<void>((sink) => {
            if (socket.readyState === OPEN) sink.success();
            else sink.error(new RSocketConnectionError("Accepted WebSocket is not open"));
        });
        super(socket, opened, 1011, true);
    }
}
