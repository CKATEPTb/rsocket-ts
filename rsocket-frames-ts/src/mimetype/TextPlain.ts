import {MimeType} from "@/mimetype/MimeType";
import type {ByteReader} from "bebyte";
import {Metadata} from "@/frame/context/Metadata";
import {Payload} from "@/frame/context/Payload";
import {decode, encode} from "@/utils";

/** MIME codec for UTF-8 plain-text data and metadata. */
export class TextPlain<T extends string = string> extends MimeType<T> {
    /** Encodes a string as a data payload. */
    protected override serializePayload(payload: T): Payload<T> {
        return super.serializePayload(encode(this.string(payload)) as unknown as T);
    }

    /** Decodes all remaining payload bytes as UTF-8 text. */
    protected override deserializePayload(payload: ByteReader): Payload<T> {
        const raw = payload.viewRemaining();
        return new Payload(this, decode(raw) as T, raw);
    }

    /** Encodes a string as metadata. */
    protected override serializeMetadata(payload: T): Metadata<T> {
        return super.serializeMetadata(encode(this.string(payload)) as unknown as T);
    }

    /** Decodes a length-delimited or metadata-only UTF-8 string. */
    protected override deserializeMetadata(payload: ByteReader, hasPayload: boolean = true): Metadata<T> {
        const raw = hasPayload ? payload.viewBytes(payload.i24()) : payload.viewRemaining();
        return new Metadata(this, decode(raw) as T, raw);
    }

    /** Validates the runtime value before passing it to `TextEncoder`. */
    private string(payload: T): string {
        if (typeof payload !== "string") throw new TypeError("Text payload must be a string");
        return payload;
    }
}
