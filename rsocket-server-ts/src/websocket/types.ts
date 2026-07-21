/** Structural WebSocket contract accepted from WHATWG and common Node servers. */

/** Accepted WebSocket sufficient for binary RSocket framing. */
export interface RSocketAcceptedWebSocket {
    /** WHATWG-compatible ready state; an accepted connection must normally be `1`. */
    readonly readyState: number;
    /** Requests binary `ArrayBuffer` messages when supported. */
    binaryType?: string;
    /** Sends one raw RSocket frame as one WebSocket message. */
    send(data: BufferSource): void;
    /** Starts a WebSocket close handshake. */
    close(code?: number, reason?: string): void;
    /** Immediately destroys a failed Node WebSocket when available. */
    terminate?(): void;
    /** WHATWG event registration. */
    addEventListener?(type: string, listener: (...args: any[]) => void): void;
    /** WHATWG event removal. */
    removeEventListener?(type: string, listener: (...args: any[]) => void): void;
    /** Node EventEmitter registration. */
    on?(type: string, listener: (...args: any[]) => void): void;
    /** Node EventEmitter removal. */
    off?(type: string, listener: (...args: any[]) => void): void;
}
