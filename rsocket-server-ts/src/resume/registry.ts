/** Opaque token registry for active and temporarily suspended logical sessions. */
import type {ResumeFrame, RSocketResumeToken} from "rsocket-frames-ts";
import type {TransportBinding} from "@/session/types.js";
import type {RSocketServerConnection} from "@/server/connection.js";

/** Minimal session surface needed to complete a RESUME handshake. */
export interface ResumableServerSession<D = unknown, M = unknown> {
    /** Stable public facade retained across physical transports. */
    readonly connection: RSocketServerConnection<D, M>;
    /** Whether this session currently permits a Resume attempt. */
    readonly isSuspended: boolean;
    /** Attaches and validates a replacement physical transport. */
    resume(binding: TransportBinding, frame: ResumeFrame): void;
    /** Returns a human-readable rejection reason without mutating state. */
    rejectResumeReason(frame: ResumeFrame): string | undefined;
}

/** Stores resume tokens without interpreting or transforming them. */
export class ResumeRegistry<D = unknown, M = unknown> {
    private readonly sessions = new Map<string, ResumableServerSession<D, M>>();
    private readonly keys = new WeakMap<ResumableServerSession<D, M>, string>();

    /** Reserves a token for exactly one logical session. */
    reserve(token: RSocketResumeToken, session: ResumableServerSession<D, M>): boolean {
        const key = resumeTokenKey(token);
        if (this.keys.has(session) || this.sessions.has(key)) return false;
        this.sessions.set(key, session);
        this.keys.set(session, key);
        return true;
    }

    /** Looks up a resumable logical session by its opaque token. */
    get(token: RSocketResumeToken): ResumableServerSession<D, M> | undefined {
        return this.sessions.get(resumeTokenKey(token));
    }

    /** Releases the immutable token key originally reserved for a session. */
    release(session: ResumableServerSession<D, M>): void {
        const key = this.keys.get(session);
        if (key === undefined) return;
        this.keys.delete(session);
        if (this.sessions.get(key) === session) this.sessions.delete(key);
    }

    /** Number of retained logical sessions, used by cleanup tests. */
    get size(): number {
        return this.sessions.size;
    }
}

const TOKEN_ENCODER = new TextEncoder();

/** Creates a collision-free Map key from the exact token bytes. */
function resumeTokenKey(token: RSocketResumeToken): string {
    const bytes = typeof token === "string" ? TOKEN_ENCODER.encode(token) : token;
    let key = `${bytes.byteLength}:`;
    const chunkSize = 0x4000;
    for (let offset = 0; offset < bytes.byteLength; offset += chunkSize) {
        key += String.fromCharCode(...bytes.subarray(offset, offset + chunkSize));
    }
    return key;
}
