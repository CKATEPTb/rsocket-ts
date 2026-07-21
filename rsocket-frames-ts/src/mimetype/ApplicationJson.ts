import {MimeType} from "@/mimetype/MimeType";
import type {ByteReader} from "bebyte";
import {Metadata} from "@/frame/context/Metadata";
import {Payload} from "@/frame/context/Payload";
import {decode, encode} from "@/utils";

/** MIME codec that converts JavaScript values to and from UTF-8 JSON. */
export class ApplicationJson<T = any> extends MimeType<T> {
    /** Encodes a JavaScript value as a data payload. */
    protected override serializePayload(payload: T): Payload<T> {
        return super.serializePayload(this.encodeJson(payload) as unknown as T);
    }

    /** Decodes all remaining bytes as JSON, with a text fallback for fragments and error data. */
    protected override deserializePayload(payload: ByteReader): Payload<T> {
        const raw = payload.viewRemaining();
        return new Payload(this, this.decodeJson(raw) as T, raw);
    }

    /** Encodes a JavaScript value as metadata. */
    protected override serializeMetadata(payload: T): Metadata<T> {
        return super.serializeMetadata(this.encodeJson(payload) as unknown as T);
    }

    /** Decodes JSON metadata, preserving non-JSON UTF-8 input as text. */
    protected override deserializeMetadata(payload: ByteReader, hasPayload: boolean = true): Metadata<T> {
        const raw = hasPayload ? payload.viewBytes(payload.i24()) : payload.viewRemaining();
        const decoded = raw.length === 0 ? "" : this.decodeJson(raw);
        return new Metadata(this, decoded as T, raw);
    }

    /** Converts a value to JSON and rejects values JSON cannot represent. */
    private encodeJson(payload: T): Uint8Array {
        const json = JSON.stringify(payload);
        if (json === undefined) throw new TypeError("JSON payload is not serializable");
        return encode(json);
    }

    /** Decodes complete JSON while retaining raw text for fragmented or error payloads. */
    private decodeJson(payload: Uint8Array): T | string | undefined {
        if (payload.length === 0) return undefined;
        const text = decode(payload);
        try {
            return JSON.parse(text) as T;
        } catch {
            return text;
        }
    }
}
