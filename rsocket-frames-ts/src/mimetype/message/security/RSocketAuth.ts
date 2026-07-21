import type {ByteReader} from "bebyte";
import {MimeType} from "@/mimetype/MimeType";
import {WellKnownAuthType} from "@/mimetype/message/security";
import {AuthType} from "@/mimetype/message/security/AuthType";
import {Metadata} from "@/frame/context/Metadata";
import {createReader, createWriter} from "@/binary";
import {decodeCustomType, encodeCustomType} from "@/mimetype/encoding";

type AuthData<D> = { authType: AuthType<D>, data: D }

/**
 * # Authentication Extension
 *
 * _This extension specification is currently incubating.  While incubating the version is 0._
 *
 * ## Introduction
 * Authentication is a necessary component to any real world application. This extension specification provides a standardized mechanism for including both the type of credentials and the credentials in metadata payloads.
 *
 * ## Metadata Payload
 * This metadata type can be used in a per connection or per stream, and not individual payloads and as such it **MUST** only be used in frame types used to initiate interactions and payloads.
 * This includes [`SETUP`]{@link SetupFrame}, [`REQUEST_FNF`]{@link RequestFireAndForgetFrame}, [`REQUEST_RESPONSE`]{@link RequestResponseFrame}, [`REQUEST_STREAM`]{@link RequestStreamFrame}, and [`REQUEST_CHANNEL`]{@link RequestChannelFrame}.
 * The Metadata MIME Type is `message/x.rsocket.authentication.v0`.
 *
 * ### Metadata Contents
 * ```
 *      0                   1                   2                   3
 *      0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
 *     +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 *     |A| Auth ID/Len |   Authentication Type                        ...
 *     +---------------+---------------+---------------+---------------+
 *     |                     Authentication Payload                   ...
 *     +---------------+-----------------------------------------------+
 * ```
 *
 * * (**A**)uthentication Type: Authentication type is a well known value represented by a unique integer.  If A flag is set (a value of `1`), indicates a [Well-known Auth Type ID]{@link WellKnownAuthType}.  If A flag is not set (a value of `0`), indicates the Authentication Type Length in bytes.
 * * **Auth ID/Length**: (7 bits = max value 2^7 = 128) Unsigned 7-bit integer.  If A flag is set (a value of `1`), indicates a [Well-known Auth Type ID]{@link WellKnownAuthType}.  If A flag is not set (a value of `0`), indicates the Authentication Type Length in bytes.
 * * **Authentication Type**: the type of authentication encoding. This SHOULD be a US-ASCII string.  The string MUST NOT be null terminated.  (Not present if A flag is set)
 * * **Authentication Payload**: The authentication payload encoded as defined by the Authentication Encoding Type.
 */
export class RSocketAuth<D> extends MimeType<AuthData<D>> {
    /**
     * Serializes authentication metadata including auth type and credentials.
     *
     * If the `authType` is well-known, it writes the ID.
     * Otherwise, it writes the UTF-8 string name and the payload.
     *
     * @param {AuthData<D>} payload - Authentication data to serialize.
     * @returns {Metadata<AuthData<D>>} Metadata wrapper containing the auth data.
     */
    protected override serializeMetadata(payload: AuthData<D>): Metadata<AuthData<D>> {
        const writer = createWriter()
        if (payload.authType.isWellKnown) writer.i8(128 | payload.authType.identifier!)
        else {
            const type = encodeCustomType(payload.authType.authType, "Authentication type")
            writer.i7(type.length - 1)
            writer.write(type)
        }
        payload.authType.write(writer, payload.data)
        return new Metadata(this, payload, writer.toUint8Array())
    }

    /**
     * Deserializes binary metadata into authentication data.
     *
     * Resolves the `AuthType` based on the ID or name, then uses it to decode
     * the corresponding credentials from the stream.
     *
     * @param {ByteReader} payload - The input binary reader.
     * @param {boolean} [hasPayload=true] - Whether to expect a length-prefixed payload.
     * @returns {Metadata<AuthData<D>>} Parsed authentication metadata.
     */
    protected override deserializeMetadata(payload: ByteReader, hasPayload: boolean = true): Metadata<AuthData<D>> {
        const array = hasPayload ? payload.viewBytes(payload.i24()) : payload.viewRemaining();
        const buffer = createReader(array);
        const i8 = buffer.i8()
        const i7 = i8 & 0x7F
        const authType = i8 >> 7
            ? WellKnownAuthType.valueOf(i7)
            : WellKnownAuthType.valueOf(decodeCustomType(i7, length => buffer.viewBytes(length)))
        const data = authType.read(buffer)
        if (buffer.remaining !== 0) {
            throw new RangeError(`Authentication metadata contains ${buffer.remaining} unexpected trailing byte(s)`);
        }
        return new Metadata(this, {authType, data}, array)
    }
}
