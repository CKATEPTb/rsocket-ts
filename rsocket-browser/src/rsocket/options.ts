/** Browser constructor option adaptation for the shared client facade. */
import {RSocket as ClientRSocket} from "rsocket-client-ts";

/** Client options inferred from the canonical requester constructor. */
type ClientOptions<D, M> = ConstructorParameters<typeof ClientRSocket<D, M>>[0];
/** SETUP options accepted by the canonical requester constructor. */
type ClientSetup<D, M> = NonNullable<ClientOptions<D, M>["setup"]>;
/** WebSocket branch of the canonical requester transport union. */
type WebSocketOptions<D, M> = Extract<ClientOptions<D, M>["transport"], {readonly type: "websocket"}>;
/** WebTransport branch of the canonical requester transport union. */
type WebTransportOptions<D, M> = Extract<
    ClientOptions<D, M>["transport"],
    {readonly type: "webtransport"}
>;

/** Browser-specific SETUP options add a convenient WebSocket factory. */
type BrowserSetup<D, M> = ClientSetup<D, M> & {
    /** Optional WebSocket subprotocol or ordered preference list. */
    readonly protocols?: WebSocketOptions<D, M>["protocols"];
    /** Optional `(url, protocols) => WebSocketLike` factory. */
    readonly transport?: WebSocketOptions<D, M>["factory"];
};

/** Browser WebSocket factory used by tests and internal constructor adaptation. */
export type RSocketTransportFactory = NonNullable<BrowserSetup<unknown, unknown>["transport"]>;

/** User-facing options for `new RSocket(url, options)`. */
export type RSocketOptions<D = unknown, M = unknown> =
    Omit<ClientOptions<D, M>, "transport" | "setup"> & {
        /** SETUP frame parameters plus WebSocket-only constructor options. */
        readonly setup?: BrowserSetup<D, M>;
        /** Optional mapping controls used only when the URL selects WebTransport. */
        readonly webTransport?: Omit<WebTransportOptions<D, M>, "type" | "url">;
    };

/** Single-object browser constructor form. */
export type RSocketConstructorOptions<D = unknown, M = unknown> = RSocketOptions<D, M> & {
    /** WebSocket or WebTransport endpoint URL. */
    readonly url: string | URL;
};

/** Resolves both browser constructor forms into canonical client options. */
export function browserClientOptions<D, M>(
    urlOrOptions: string | URL | RSocketConstructorOptions<D, M>,
    fallback: RSocketOptions<D, M>
): ClientOptions<D, M> {
    const {url, options} = resolveConstructor(urlOrOptions, fallback);
    const {setup: browserSetup, webTransport, reconnect = true, ...rest} = options;
    const {transport, protocols, ...setup} = browserSetup ?? {};
    if (isWebTransportEndpoint(url)) {
        if (protocols !== undefined || transport !== undefined) {
            throw new TypeError(
                "WebTransport URLs use options.webTransport; setup.protocols and setup.transport are WebSocket-only"
            );
        }
        return {
            ...rest,
            reconnect,
            transport: {
                type: "webtransport",
                url,
                ...webTransport
            },
            ...(browserSetup === undefined ? {} : {setup})
        };
    }
    if (webTransport !== undefined) {
        throw new TypeError("options.webTransport requires an https: WebTransport URL");
    }
    return {
        ...rest,
        reconnect,
        transport: {
            type: "websocket",
            url,
            ...(protocols === undefined ? {} : {protocols}),
            ...(transport === undefined ? {} : {factory: transport})
        },
        ...(browserSetup === undefined ? {} : {setup})
    };
}

/** Selects WebTransport only for an unambiguous HTTPS endpoint. */
function isWebTransportEndpoint(input: string | URL): boolean {
    try {
        const href = isUrl(input) ? input.href : String(input);
        return new URL(href).protocol === "https:";
    } catch {
        // The WebSocket adapter retains established lazy URL validation on connect().
        return false;
    }
}

/** Separates the URL from either supported constructor form. */
function resolveConstructor<D, M>(
    input: string | URL | RSocketConstructorOptions<D, M>,
    fallback: RSocketOptions<D, M>
): {readonly url: string | URL; readonly options: RSocketOptions<D, M>} {
    if (typeof input === "string" || isUrl(input)) return {url: input, options: fallback};
    const {url, ...options} = input;
    return {url, options};
}

/** Recognizes URL objects created by the current window or another browser realm. */
function isUrl(value: unknown): value is URL {
    return typeof value === "object" && value !== null &&
        Object.prototype.toString.call(value) === "[object URL]";
}
