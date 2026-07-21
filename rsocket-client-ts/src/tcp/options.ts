/** Browser-safe TCP option validation shared by facade and Node transport code. */

/** Host and port of one RSocket TCP responder. */
export interface RSocketTcpAddress {
    /** DNS name or IP address of the RSocket server. */
    readonly host: string;
    /** TCP port of the RSocket server. */
    readonly port: number;
}

/** Rejects addresses that cannot be passed to a Node TCP socket. */
export function assertTcpAddress(address: RSocketTcpAddress): void {
    if (typeof address.host !== "string" || address.host.trim().length === 0) {
        throw new TypeError("RSocket TCP host must be a non-empty string");
    }
    if (!Number.isInteger(address.port) || address.port < 1 || address.port > 65_535) {
        throw new TypeError("RSocket TCP port must be an integer between 1 and 65535");
    }
}

/** Returns one immutable address suitable for every reconnect attempt. */
export function normalizeTcpAddress(address: RSocketTcpAddress): RSocketTcpAddress {
    assertTcpAddress(address);
    return Object.freeze({host: address.host, port: address.port});
}
