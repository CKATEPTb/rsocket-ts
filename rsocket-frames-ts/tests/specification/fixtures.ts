import {Metadata, Payload, WellKnownMimeType} from "@/index";

/** Raw MIME codec used when a test needs exact bytes without text conversion. */
export const binaryMimeType = WellKnownMimeType.APPLICATION_OCTET_STREAM;

/** Creates outbound raw metadata without interpreting its bytes as an encoded length prefix. */
export function metadata(...values: number[]): Metadata<Uint8Array> {
    return new Metadata(binaryMimeType, Uint8Array.from(values));
}

/** Creates an outbound raw data payload. */
export function payload(...values: number[]): Payload<Uint8Array> {
    return new Payload(binaryMimeType, Uint8Array.from(values));
}

/** Converts a whitespace-separated hexadecimal wire vector to bytes. */
export function wire(hex: string): Uint8Array {
    const compact = hex.replace(/\s+/g, "");
    if (compact.length % 2 !== 0 || !/^[\da-f]*$/i.test(compact)) {
        throw new TypeError(`Invalid hexadecimal wire vector: ${hex}`);
    }
    const result = new Uint8Array(compact.length / 2);
    for (let index = 0; index < result.length; index++) {
        result[index] = Number.parseInt(compact.slice(index * 2, index * 2 + 2), 16);
    }
    return result;
}
