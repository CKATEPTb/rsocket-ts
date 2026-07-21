/** Core transport adapter for one accepted Node TCP socket. */
import type {Socket} from "node:net";
import {Mono} from "reactor-core-ts";
import {
    ReactiveTcpTransportConnection,
    RSocketConnectionError
} from "rsocket-core-ts";

/** Accepted TCP socket that adds and removes RSocket's 24-bit frame prefix. */
export class ReactiveAcceptedTcpConnection extends ReactiveTcpTransportConnection<Socket> {
    /** Wraps an already accepted socket without starting another connection. */
    constructor(socket: Socket, maxFrameLength: number) {
        const opened = Mono.create<void>((sink) => {
            if (socket.destroyed) sink.error(new RSocketConnectionError("Accepted TCP socket is closed"));
            else sink.success();
        });
        super(socket, maxFrameLength, opened);
    }
}
