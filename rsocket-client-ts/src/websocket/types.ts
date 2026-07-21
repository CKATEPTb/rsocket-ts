/** Minimal WHATWG WebSocket surface required by the adapter. */
export interface RSocketWebSocket {
    /** Binary representation requested for incoming messages. */
    binaryType: BinaryType;
    /** WHATWG WebSocket ready-state value. */
    readonly readyState: number;
    /** Sends one complete binary RSocket frame using the WHATWG binary contract. */
    send(data: BufferSource): void;
    /** Starts a WebSocket close handshake. */
    close(code?: number, reason?: string): void;
    /** Registers an open listener. */
    addEventListener(type: "open", listener: (event: Event) => void): void;
    /** Registers a message listener. */
    addEventListener(type: "message", listener: (event: MessageEvent) => void): void;
    /** Registers an error listener. */
    addEventListener(type: "error", listener: (event: Event) => void): void;
    /** Registers a close listener. */
    addEventListener(type: "close", listener: (event: CloseEvent) => void): void;
    /** Removes an open listener. */
    removeEventListener(type: "open", listener: (event: Event) => void): void;
    /** Removes a message listener. */
    removeEventListener(type: "message", listener: (event: MessageEvent) => void): void;
    /** Removes an error listener. */
    removeEventListener(type: "error", listener: (event: Event) => void): void;
    /** Removes a close listener. */
    removeEventListener(type: "close", listener: (event: CloseEvent) => void): void;
}

/** Factory used to create a native or compatible WebSocket. */
export type RSocketWebSocketFactory = (
    url: string | URL,
    protocols?: string | string[]
) => RSocketWebSocket;
