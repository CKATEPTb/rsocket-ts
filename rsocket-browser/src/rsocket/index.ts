/** Browser-first WebSocket facade over the transport-neutral requester. */
import {RSocket as ClientRSocket} from "rsocket-client-ts";
import {browserReconnectSignals} from "@/reconnect/index.js";
import {
    browserClientOptions,
    type RSocketConstructorOptions,
    type RSocketOptions
} from "@/rsocket/options.js";

/** Private client integration hook; the symbol keeps it out of both package APIs. */
const RECONNECT_SIGNALS = Symbol.for("rsocket-client-ts.reconnect-signals");

/** RSocket requester that adds browser sleep, wake, focus, and network recovery signals. */
export class RSocket<D = unknown, M = unknown> extends ClientRSocket<D, M> {
    /** Creates a browser requester from a URL plus optional settings. */
    constructor(url: string | URL, options?: RSocketOptions<D, M>);
    /** Creates a browser requester from one options object containing the URL. */
    constructor(options: RSocketConstructorOptions<D, M>);
    constructor(
        urlOrOptions: string | URL | RSocketConstructorOptions<D, M>,
        options: RSocketOptions<D, M> = {}
    ) {
        super(withBrowserReconnectSignals(browserClientOptions(urlOrOptions, options)));
    }
}

/** Attaches browser lifecycle signals to a fresh internal client-options object. */
function withBrowserReconnectSignals<T extends object>(options: T): T {
    Object.defineProperty(options, RECONNECT_SIGNALS, {value: browserReconnectSignals});
    return options;
}
