import {AuthType} from "@/mimetype/message/security/AuthType";
import {SimpleAuthType} from "@/mimetype/message/security/SimpleAuthType";
import {BearerAuthType} from "@/mimetype/message/security/BearerAuthType";

export {
    AuthType
}

/**
 * Namespace containing well-known authentication types for use in
 * `RSocketAuth` metadata.
 *
 * These types represent commonly used credential schemes and are associated
 * with numeric identifiers for efficient encoding. They are typically
 * used in frames that support authentication, such as `SETUP`.
 */
export namespace WellKnownAuthType {
    /**
     * Simple Authentication Type.
     * Uses a UTF-8 encoded `username` and `password` pair.
     * Identifier: `0`
     *
     * Example:
     * ```ts
     * {
     *   authType: WellKnownAuthType.SIMPLE,
     *   data: { username: "admin", password: "secret" }
     * }
     * ```
     */
    export const SIMPLE = new SimpleAuthType("simple", 0)
    /**
     * Bearer Authentication Type.
     * Uses a single UTF-8 encoded bearer token.
     * Identifier: `1`
     *
     * Example:
     * ```ts
     * {
     *   authType: WellKnownAuthType.BEARER,
     *   data: "eyJhbGciOiJIUzI1NiIsInR5cCI6..."
     * }
     * ```
     */
    export const BEARER = new BearerAuthType("bearer", 1)    /**
     * Resolves a well-known or custom authentication type by name or numeric ID.
     *
     * @param {string | number} type - The authentication type name or identifier.
     * @returns {AuthType<any>} Matching AuthType instance.
     */
    export const valueOf = AuthType.valueOf
}