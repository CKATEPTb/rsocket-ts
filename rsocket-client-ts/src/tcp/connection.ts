/** Node TCP implementation of the core RSocket transport contract. */
import {createConnection, type Socket} from "node:net";
import {
    DEFAULT_MAX_FRAME_LENGTH,
    ReactiveTcpTransportConnection,
    RSocketConnectionError
} from "rsocket-core-ts";
import type {RSocketTransportFactory} from "@/client/types.js";
import {openTcpSocket} from "@/tcp/events.js";
import {normalizeTcpAddress, type RSocketTcpAddress} from "@/tcp/options.js";

/** Creates a connecting Node TCP socket. */
export type RSocketTcpSocketFactory = (address: RSocketTcpAddress) => Socket;

/** Options captured by a reusable TCP transport factory. */
export interface RSocketTcpTransportOptions extends RSocketTcpAddress {
    /** Largest accepted raw RSocket frame. */
    readonly maxFrameLength?: number;
    /** Optional custom socket factory, useful for proxies and tests. */
    readonly socketFactory?: RSocketTcpSocketFactory;
}

/** One physical TCP connection carrying length-prefixed RSocket frames. */
export class ReactiveTcpConnection extends ReactiveTcpTransportConnection<Socket> {
    /** Wraps one connecting or connected Node TCP socket. */
    constructor(
        socket: Socket,
        maxFrameLength: number,
        timeoutMs: number | undefined,
        abortSignal?: AbortSignal
    ) {
        super(socket, maxFrameLength, openTcpSocket(socket, timeoutMs, abortSignal));
    }
}

/** Creates a reusable transport factory after validating its TCP address. */
export function createTcpTransport(options: RSocketTcpTransportOptions): RSocketTransportFactory {
    const address = normalizeTcpAddress(options);
    const maxFrameLength = options.maxFrameLength ?? DEFAULT_MAX_FRAME_LENGTH;
    const socketFactory = options.socketFactory ?? ((target: RSocketTcpAddress) => createConnection(target));
    return ({timeoutMs, abortSignal}) => {
        let socket: Socket | undefined;
        try {
            socket = socketFactory(address);
            return new ReactiveTcpConnection(socket, maxFrameLength, timeoutMs, abortSignal);
        } catch (error) {
            try {
                socket?.destroy();
            } catch {
                // Preserve the initialization failure; cleanup errors are secondary.
            }
            throw error instanceof RSocketConnectionError
                ? error
                : new RSocketConnectionError("TCP connection initialization failed", error);
        }
    };
}
