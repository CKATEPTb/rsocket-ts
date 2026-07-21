/** Creates opaque tokens used by protocol Resume. */

/** Creates an opaque token using the strongest random source available. */
export function createResumeToken(): string {
    const crypto = globalThis.crypto;
    if (crypto?.randomUUID !== undefined) return crypto.randomUUID();
    if (crypto?.getRandomValues !== undefined) {
        const bytes = new Uint8Array(16);
        crypto.getRandomValues(bytes);
        let token = "";
        for (const byte of bytes) token += byte.toString(16).padStart(2, "0");
        return token;
    }
    throw new TypeError("RSocket Resume requires a cryptographically secure Web Crypto implementation");
}
