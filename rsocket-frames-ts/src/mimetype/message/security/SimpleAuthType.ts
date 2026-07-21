import type {ByteReader, ByteWriter} from "bebyte";
import {decode, encode} from "@/utils";
import {AuthType} from "@/mimetype/message/security/AuthType";
import {assertByteLength} from "@/utils";

/**
 * # Simple Authentication Type
 *
 * _This extension specification is currently incubating.  While incubating the version is 0._
 *
 * ## Introduction
 * Authentication is a necessary component to any real world application. The most "simple" mechanism for authenticating is leveraging a username and password for authentication. This Authentication Type provides a standardized mechanism for including a username and password in the Authentication Payload of the [Authentication Extension]{@link RSocketAuth} using the Authentication Type of `simple`.
 *
 * ## Authentication Payload
 * ```
 *      0                   1                   2                   3
 *      0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
 *     +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 *     |        Username Length        |
 *     +-------------------------------+-------------------------------+
 *     |                            Username                          ...
 *     +-------------------------------+-------------------------------+
 *     |                            Password                          ...
 *     +-------------------------------+-------------------------------+
 * ```
 *
 * * **Username Length**: (16 bits = max value 2^16-1 = 65535) Unsigned 16-bit integer of Username Length in bytes.
 * * **Username**:  The UTF-8 encoded username.  The string MUST NOT be null terminated.
 * * **Password**:  The UTF-8 encoded password.  The string MUST NOT be null terminated.
 *
 * ## Security Considerations
 * The Simple Authentication Type transmits the username and password in cleartext. Additionally, it does not protect the authenticity or confidentiality of the payload that is transmitted along with it. This means that the [Transport]{@link https://github.com/rsocket/rsocket/blob/master/Protocol.md#transport-protocol} that is used should provide both authenticity and confidentiality to protect both the username and password and corresponding payload.
 *
 * The use of the UTF-8 character encoding scheme and of normalization introduces additional security considerations; see [Section 10 of RFC3629]{@link https://tools.ietf.org/html/rfc3629#section-10} and [Section 6 of RFC5198]{@link https://tools.ietf.org/html/rfc5198#section-6} for more information.
 */
export class SimpleAuthType extends AuthType<{
    username: string,
    password: string
}> {
    /**
     * Reads and decodes the authentication payload from the given ByteReader.
     *
     * @param {ByteReader} reader - The byte reader to extract the authentication data.
     * @returns {{ username: string, password: string }} The decoded credentials.
     */
    public read(reader: ByteReader): { username: string; password: string } {
        return {
            username: decode(reader.viewBytes(reader.i16())),
            password: decode(reader.viewRemaining())
        }
    }

    /**
     * Writes the authentication payload to the given ByteWriter.
     *
     * Encodes the username and password using UTF-8 and writes:
     *  - A 16-bit unsigned integer representing the byte-length of the username.
     *  - The UTF-8 encoded username.
     *  - The UTF-8 encoded password.
     *
     * @param {ByteWriter} writer - The byte writer to write the authentication data.
     * @param {{ username: string, password: string }} data - The credentials to encode and write.
     * @returns {void}
     */
    public write(writer: ByteWriter, data: { username: string; password: string }): void {
        const username = encode(data.username)
        assertByteLength("Username", username.length, 0, 0xffff)
        writer.i16(username.length)
        writer.write(username)
        writer.write(encode(data.password))
    }
}
