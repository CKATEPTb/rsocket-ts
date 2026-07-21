/** Shared RSocket protocol limits and payload MIME defaults. */
import {MAX_FRAME_SIZE, type MimeType, WellKnownMimeType} from "rsocket-frames-ts";

/** Largest legal Reactive Streams request count representable by REQUEST_N. */
export const MAX_REQUEST_N = 0x7fffffff;
/** Maximum RSocket frame length representable by a 24-bit transport prefix. */
export const DEFAULT_MAX_FRAME_LENGTH = MAX_FRAME_SIZE;

/** Default data mime type for user payloads. */
export const DEFAULT_DATA_MIME_TYPE: MimeType<any> = WellKnownMimeType.APPLICATION_JSON;
/** Default metadata mime type for routes and composite metadata. */
export const DEFAULT_METADATA_MIME_TYPE: MimeType<any> = WellKnownMimeType.MESSAGE_RSOCKET_COMPOSITE_METADATA;
/** Mime type used for ERROR frame payloads. */
export const ERROR_DATA_MIME_TYPE: MimeType<any> = WellKnownMimeType.TEXT_PLAIN;
/** Mime type used for KEEPALIVE frame payload bytes. */
export const KEEPALIVE_DATA_MIME_TYPE: MimeType<any> = WellKnownMimeType.APPLICATION_OCTET_STREAM;
