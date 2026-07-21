import type {ByteReader, ByteWriter} from "bebyte";
import {decode, encode} from "@/utils";
import {AuthType} from "@/mimetype/message/security/AuthType";

/**
 * # Bearer Token Authentication Type
 *
 * _This extension specification is currently incubating.  While incubating the version is 0._
 *
 * ## Introduction
 * Authentication is a necessary component to any real world application. A common mechanism for authenticating is using a bearer token. A bearer token can be presented as a means of obtaining access to a resource (i.e. session ids, OAuth 2 tokens, etc).
 * This Authentication Type provides a standardized mechanism for including a bearer token in the Authentication Payload of the [Authentication Extension]{@link RSocketAuth} using the Authentication Type of `bearer`.
 *
 * ### Authentication Payload
 * ```
 *      0                   1                   2                   3
 *      0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
 *     +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 *     | Bearer Token                                              ...
 *     +---------------+-----------------------------------------------+
 * ```
 *
 * * **Bearer Token**: The UTF-8 encoded bearer token.  The string MUST NOT be null terminated.
 */
export class BearerAuthType extends AuthType<string> {
    /**
     * Reads a bearer token from the given byte stream.
     *
     * @param {ByteReader} reader - A stream from which to read the bearer token.
     * @returns {string} Decoded bearer token as a UTF-8 string.
     */
    public read(reader: ByteReader): string {
        return decode(reader.viewRemaining());
    }

    /**
     * Writes a bearer token to the output stream.
     *
     * @param {ByteWriter} writer - The stream to write to.
     * @param {string} data - The bearer token to encode and write.
     */
    public write(writer: ByteWriter, data: string): void {
        writer.write(encode(data))
    }
}
