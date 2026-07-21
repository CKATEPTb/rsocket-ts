/** Stream identifier generation shared by requester and responder engines. */

/** First legal stream ID for a client-side requester. */
export const CLIENT_STREAM_ID = 1;
/** First legal stream ID for a server-side requester. */
export const SERVER_STREAM_ID = 2;

/**
 * Advances a requester stream ID by two and wraps at the largest legal ID
 * with the same parity.
 */
export function nextRSocketStreamId(current: number, first: 1 | 2): number {
    const last = first === CLIENT_STREAM_ID ? 0x7fffffff : 0x7ffffffe;
    return current >= last ? first : current + 2;
}
