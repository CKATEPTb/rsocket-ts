/** Number of bytes in the RSocket frame-length prefix used by TCP. */
export const FRAME_LENGTH_PREFIX_SIZE = 3;

/** Number of bytes in the mandatory RSocket frame header. */
export const FRAME_HEADER_SIZE = 6;

/** Maximum RSocket frame size defined by the 24-bit protocol limit. */
export const MAX_FRAME_SIZE = 0xffffff;
