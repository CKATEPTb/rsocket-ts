/**
 * Public protocol constants re-exported for internal modules.
 */
export {
    DEFAULT_DATA_MIME_TYPE,
    DEFAULT_MAX_FRAME_LENGTH,
    DEFAULT_METADATA_MIME_TYPE,
    ERROR_DATA_MIME_TYPE,
    KEEPALIVE_DATA_MIME_TYPE,
    MAX_REQUEST_N
} from "@/protocol/constants.js";
export * from "@/protocol/frames.js";
export * from "@/protocol/stream-id.js";
