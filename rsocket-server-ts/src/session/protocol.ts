/** Pure protocol predicates used by the server session hot path. */
import {Metadata, type RequestChannelFrame, WellKnownMimeType} from "rsocket-frames-ts";
import type {RSocketPayloadFrame} from "rsocket-core-ts";

const COMPOSITE_MIME = WellKnownMimeType.MESSAGE_RSOCKET_COMPOSITE_METADATA.mimeType;
const CONTROL_METADATA_MIMES = new Set([
    WellKnownMimeType.MESSAGE_RSOCKET_MIMETYPE.mimeType,
    WellKnownMimeType.MESSAGE_RSOCKET_ACCEPT_MIMETYPES.mimeType,
    WellKnownMimeType.MESSAGE_RSOCKET_AUTHENTICATION.mimeType,
    WellKnownMimeType.MESSAGE_RSOCKET_TRACING_ZIPKIN.mimeType,
    WellKnownMimeType.MESSAGE_RSOCKET_ROUTING.mimeType
]);

/** Whether the initial channel frame contains an application item rather than a routed control envelope. */
export function hasInitialChannelItem(
    frame: RequestChannelFrame,
    payload: RSocketPayloadFrame
): boolean {
    if (payload.data !== undefined) return true;
    const dataPayload = payload.dataPayload;
    if (dataPayload !== undefined && dataPayload.toUint8Array().byteLength > 0) return true;
    const metadata = frame.metadata;
    if (!(metadata instanceof Metadata)) return metadata !== undefined;
    if (CONTROL_METADATA_MIMES.has(metadata.mimeType.mimeType)) return false;
    if (metadata.mimeType.mimeType !== COMPOSITE_MIME || !Array.isArray(metadata.payload)) return true;
    return metadata.payload.some((entry) =>
        !(entry instanceof Metadata) || !CONTROL_METADATA_MIMES.has(entry.mimeType.mimeType)
    );
}
