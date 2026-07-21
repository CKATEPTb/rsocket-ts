/** Shared public and internal protocol types. */
import type {Frame, Metadata, MimeType, Payload} from "rsocket-frames-ts";

/** Direction of one decoded frame observed by endpoint diagnostics. */
export type RSocketFrameDirection = "send" | "receive";

/**
 * Payload envelope shared by codecs and interaction implementations.
 */
export interface RSocketPayload<D = unknown, M = unknown> {
    /** Application data value. */
    readonly data?: D;
    /** Application metadata value. */
    readonly metadata?: M;
    /** MIME override for this payload's data value. */
    readonly dataMimeType?: MimeType<D>;
    /** MIME override for this payload's metadata value. */
    readonly metadataMimeType?: MimeType<M>;
}

/** Input envelope that also accepts values already encoded by their own MIME codec. */
interface RSocketPayloadInputEnvelope<D, M> {
    /** Raw application data or an independently typed encoded payload. */
    readonly data?: D | Payload<unknown>;
    /** Raw application metadata or an independently typed encoded metadata entry. */
    readonly metadata?: M | Metadata<unknown>;
    /** MIME override used only when `data` is a raw value. */
    readonly dataMimeType?: MimeType<D>;
    /** MIME override used only when `metadata` is a raw value. */
    readonly metadataMimeType?: MimeType<M>;
}

/**
 * Any payload form accepted by protocol interaction implementations.
 */
export type RSocketPayloadInput<D = unknown, M = unknown> =
    | RSocketPayloadInputEnvelope<D, M>
    | Payload<unknown>
    | Metadata<unknown>
    | D;

/**
 * Decoded data and metadata carried by an RSocket frame.
 */
export interface RSocketPayloadFrame<D = unknown, M = unknown> extends RSocketPayload<D, M> {
    /** Raw decoded RSocket frame that carried the payload. */
    readonly frame: Frame;
    /** Raw data payload object, when the frame carried data. */
    readonly dataPayload?: Payload<D>;
    /** Raw metadata payload object, when the frame carried metadata. */
    readonly metadataPayload?: Metadata<M>;
}

