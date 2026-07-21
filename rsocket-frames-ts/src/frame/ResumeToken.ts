import {decode, encode} from "@/utils";

/** Opaque Resume identification token carried verbatim by SETUP and RESUME. */
export type RSocketResumeToken = string | Uint8Array;

/** Takes an immutable snapshot so later caller mutation cannot change a frame. */
export function snapshotResumeToken(token: RSocketResumeToken): RSocketResumeToken {
    return typeof token === "string" ? token : token.slice();
}

/** Returns the exact bytes written into a Resume token field. */
export function resumeTokenBytes(token: RSocketResumeToken): Uint8Array {
    return typeof token === "string" ? encode(token) : token;
}

/** Preserves arbitrary bytes while retaining ergonomic strings when UTF-8 round-trips exactly. */
export function decodeResumeToken(bytes: Uint8Array): RSocketResumeToken {
    const text = decode(bytes);
    const encoded = encode(text);
    if (encoded.byteLength === bytes.byteLength) {
        let equal = true;
        for (let index = 0; index < bytes.byteLength; index += 1) {
            if (encoded[index] === bytes[index]) continue;
            equal = false;
            break;
        }
        if (equal) return text;
    }
    return bytes.slice();
}
