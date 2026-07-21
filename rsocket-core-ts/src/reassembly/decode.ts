/** Raw application-payload decoding shared by direct and fragmented frames. */
import {Metadata, type MimeType, Payload, WellKnownMimeType} from "rsocket-frames-ts";

/** MIME name used by endpoint frame decoders before application dispatch. */
const RAW_MIME_TYPE = WellKnownMimeType.APPLICATION_OCTET_STREAM.mimeType;

/** Decodes metadata retained as wire bytes, preserving already decoded values. */
export function decodeRawMetadata(
    value: Metadata<any> | undefined,
    mimeType: MimeType<any>
): Metadata<any> | undefined {
    if (value === undefined || value.mimeType.mimeType !== RAW_MIME_TYPE) return value;
    if (mimeType === value.mimeType) return value;
    return mimeType.toMetadata(value.toUint8Array(), false);
}

/** Decodes data retained as wire bytes, preserving already decoded values. */
export function decodeRawPayload(
    value: Payload<any> | undefined,
    mimeType: MimeType<any>
): Payload<any> | undefined {
    if (value === undefined || value.mimeType.mimeType !== RAW_MIME_TYPE) return value;
    if (mimeType === value.mimeType) return value;
    return mimeType.toPayload(value.toUint8Array());
}
