/**
 * Numeric browser WebSocket ready-state constants.
 */
/** WebSocket is still connecting. */
export const WS_CONNECTING = 0;
/** WebSocket is open and can send bytes. */
export const WS_OPEN = 1;
/** WebSocket close handshake has started. */
export const WS_CLOSING = 2;
/** WebSocket is closed. */
export const WS_CLOSED = 3;

/** Normal browser close code accepted by `WebSocket.close()`. */
export const WS_CLOSE_NORMAL = 1000;

/** Application-level close code used when an RSocket protocol failure closes a browser WebSocket. */
export const WS_CLOSE_RSOCKET_PROTOCOL_ERROR = 3002;
