/** Requester WebTransport factory backed by the shared mapping in Core. */
import {
    createWebTransportConnection,
    DEFAULT_MAX_FRAME_LENGTH,
    RSocketConnectionError
} from "rsocket-core-ts";
import type {RSocketTransportFactory} from "@/client/types.js";
import {defaultWebTransportFactory} from "@/webtransport/factory.js";
import type {
    RSocketWebTransportFactory,
    RSocketWebTransportMediaListener
} from "@/webtransport/types.js";

/** Options captured by a reusable requester WebTransport factory. */
export interface RSocketWebTransportOptions {
    /** Absolute `https:` endpoint used by the WebTransport CONNECT handshake. */
    readonly url: string | URL;
    /** Largest accepted raw RSocket frame. */
    readonly maxFrameLength?: number;
    /** Defensive cap for records waiting behind another QUIC stream. */
    readonly maxReorderBufferBytes?: number;
    /** Sends complete FNF requests as best-effort datagrams with reliable skip markers. */
    readonly unreliableFireAndForget?: boolean;
    /** Optional native-compatible WebTransport session factory. */
    readonly factory?: RSocketWebTransportFactory;
    /** Optional receiver for best-effort media extension datagrams. */
    readonly media?: RSocketWebTransportMediaListener;
}

/** Creates a reusable requester transport after validating its endpoint once. */
export function createWebTransport(options: RSocketWebTransportOptions): RSocketTransportFactory {
    const url = normalizeWebTransportUrl(options.url);
    const factory = options.factory;
    const maxFrameLength = options.maxFrameLength ?? DEFAULT_MAX_FRAME_LENGTH;
    return ({timeoutMs, abortSignal}) => {
        let session;
        try {
            session = factory === undefined
                ? defaultWebTransportFactory(url, options.unreliableFireAndForget === true)
                : factory(url);
            return createWebTransportConnection(session, {
                role: "requester",
                maxFrameLength,
                ...(options.maxReorderBufferBytes === undefined
                    ? {}
                    : {maxReorderBufferBytes: options.maxReorderBufferBytes}),
                ...(options.unreliableFireAndForget === undefined
                    ? {}
                    : {unreliableFireAndForget: options.unreliableFireAndForget}),
                ...(timeoutMs === undefined ? {} : {timeoutMs}),
                ...(abortSignal === undefined ? {} : {abortSignal})
            });
        } catch (error) {
            try {
                session?.close({closeCode: 1, reason: "RSocket WebTransport initialization failed"});
            } catch {
                // Preserve the initialization failure.
            }
            throw error instanceof RSocketConnectionError
                ? error
                : new RSocketConnectionError("WebTransport connection initialization failed", error);
        }
    };
}

/** Requires the secure URL mandated by WebTransport implementations. */
export function normalizeWebTransportUrl(input: string | URL): string {
    let url: URL;
    try {
        url = input instanceof URL ? new URL(input.href) : new URL(input);
    } catch (error) {
        throw new RSocketConnectionError("WebTransport URL must be absolute", error);
    }
    if (url.protocol !== "https:") {
        throw new RSocketConnectionError("WebTransport URL must use the https: scheme");
    }
    if (url.hash.length !== 0) throw new RSocketConnectionError("WebTransport URL must not contain a fragment");
    return url.href;
}
