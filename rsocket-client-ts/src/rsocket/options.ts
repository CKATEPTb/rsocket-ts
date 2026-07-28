/** Public option shapes and hot-path normalization for `RSocket`. */
import type {MimeType} from "rsocket-frames-ts";
import type {RSocketPayloadInput} from "rsocket-core-ts";
import type {RSocketClientConfiguration, RSocketRequestOptions} from "@/client/types.js";
import type {RSocketConnectionEventHandlers} from "@/reconnect/events.js";
import type {RSocketReconnectOptionInput} from "@/reconnect/options.js";
import type {RSocketLogOptions, RSocketLogSink} from "@/logging/types.js";
import type {RSocketTcpAddress} from "@/tcp/options.js";
import type {RSocketWebSocketFactory} from "@/websocket/types.js";
import type {
    RSocketWebTransportFactory,
    RSocketWebTransportMediaListener
} from "@/webtransport/types.js";

/** Socket-level diagnostics; interaction events are enabled per controller through `controller.log()`. */
type RSocketSocketLogInput = boolean | string | RSocketLogSink | Omit<RSocketLogOptions, "interactions">;

/** WebSocket transport selected by the transport-neutral client facade. */
export interface RSocketWebSocketOptions {
    /** Selects the WebSocket adapter. */
    readonly type: "websocket";
    /** Absolute `ws:` or `wss:` endpoint. */
    readonly url: string | URL;
    /** Optional WebSocket subprotocol or ordered preference list. */
    readonly protocols?: string | string[];
    /** Optional WHATWG-compatible WebSocket factory. */
    readonly factory?: RSocketWebSocketFactory;
}

/** Node TCP transport selected by the transport-neutral client facade. */
export interface RSocketTcpOptions extends RSocketTcpAddress {
    /** Selects the length-prefixed TCP adapter. */
    readonly type: "tcp";
}

/** WebTransport selected by the transport-neutral client facade. */
export interface RSocketWebTransportOptions {
    /** Selects the multiplexed WebTransport adapter. */
    readonly type: "webtransport";
    /** Absolute `https:` WebTransport endpoint. */
    readonly url: string | URL;
    /** Optional native-compatible session factory. */
    readonly factory?: RSocketWebTransportFactory;
    /** Defensive cap for frames waiting behind another QUIC stream. */
    readonly maxReorderBufferBytes?: number;
    /** Uses best-effort datagrams for complete FNF requests. Incompatible with Resume. */
    readonly unreliableFireAndForget?: boolean;
    /** Receives best-effort media extension datagrams. */
    readonly media?: RSocketWebTransportMediaListener;
}

/** Physical transport accepted by `new RSocket(...)`. */
export type RSocketTransportOptions = RSocketWebSocketOptions | RSocketTcpOptions | RSocketWebTransportOptions;

/** User-facing constructor options for the requester facade. */
export interface RSocketOptions<D = unknown, M = unknown> extends RSocketReconnectOptionInput {
    /** WebSocket, WebTransport, or Node TCP transport used for SETUP and reconnect attempts. */
    readonly transport: RSocketTransportOptions;
    /** Optional socket-level diagnostics. */
    readonly log?: RSocketSocketLogInput;
    /** Optional constructor-time lifecycle handlers. */
    readonly events?: RSocketConnectionEventHandlers;
    /** SETUP frame parameters. */
    readonly setup?: RSocketSetupOptions<D, M>;
}

/** Nested SETUP options used by the public socket constructor. */
export interface RSocketSetupOptions<D = unknown, M = unknown> {
    /** Interval between requester KEEPALIVE frames, in milliseconds. */
    readonly keepAlive?: number;
    /** Maximum interval without a responder KEEPALIVE, in milliseconds. */
    readonly lifetime?: number;
    /** Requests responder-issued LEASE credit before opening interactions. */
    readonly lease?: boolean;
    /** Maximum serialized frame size before automatic fragmentation. */
    readonly fragmentSize?: number;
    /** MIME codecs used for request data and metadata. */
    readonly mimetype?: RSocketMimeTypes<D, M>;
    /** Optional payload sent in SETUP. */
    readonly payload?: RSocketPayloadInput<D, M>;
}

/** Data and metadata MIME codecs used by SETUP or one interaction. */
export interface RSocketMimeTypes<D = unknown, M = unknown> {
    /** Metadata MIME codec. */
    readonly metadata?: MimeType<M>;
    /** Data MIME codec. */
    readonly data?: MimeType<D>;
}

/** Shared empty request options avoid an allocation for default interactions. */
export const EMPTY_REQUEST_OPTIONS: RSocketRequestOptions = Object.freeze({});

/** Converts facade options into transport-neutral low-level client configuration. */
export function toClientConfiguration<D, M>(
    options: RSocketOptions<D, M>,
    resumeToken?: string
): RSocketClientConfiguration<D, M> {
    const setupInput = options.setup;
    const mimetype = setupInput?.mimetype;
    const setup: NonNullable<RSocketClientConfiguration<D, M>["setup"]> = {};
    assignOptional(setup, "keepAliveMs", setupInput?.keepAlive);
    assignOptional(setup, "lifetimeMs", setupInput?.lifetime);
    assignOptional(setup, "honorLease", setupInput?.lease);
    assignOptional(setup, "resumeToken", resumeToken);
    assignOptional(setup, "dataMimeType", mimetype?.data);
    assignOptional(setup, "metadataMimeType", mimetype?.metadata);
    assignOptional(setup, "payload", setupInput?.payload);
    const mediaListener = options.transport.type === "webtransport"
        ? options.transport.media
        : undefined;
    return {
        setup,
        ...(setupInput?.fragmentSize === undefined ? {} : {maxFrameLength: setupInput.fragmentSize}),
        ...(mediaListener === undefined ? {} : {mediaListener})
    };
}

/** Returns client configuration carrying the token for the next SETUP or RESUME handshake. */
export function withClientResumeToken<D, M>(
    options: RSocketClientConfiguration<D, M>,
    token: string | undefined
): RSocketClientConfiguration<D, M> {
    if (token === undefined || options.setup?.resumeToken === token) return options;
    return {
        ...options,
        setup: {...options.setup, resumeToken: token}
    };
}

/** Converts positional MIME overrides into low-level request options. */
export function requestOptionsFromMimeTypes<D, M>(
    mimetype: RSocketMimeTypes<D, M> | undefined
): RSocketRequestOptions {
    if (mimetype === undefined) return EMPTY_REQUEST_OPTIONS;
    const options: RSocketRequestOptions = {};
    assignOptional(options, "dataMimeType", mimetype.data);
    assignOptional(options, "metadataMimeType", mimetype.metadata);
    return options;
}

/** Assigns an optional field without retaining explicit `undefined`. */
function assignOptional<T extends object, K extends keyof T>(target: T, key: K, value: T[K] | undefined): void {
    if (value !== undefined) target[key] = value;
}
