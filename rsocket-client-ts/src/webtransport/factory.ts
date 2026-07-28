/** Browser-native WebTransport construction without a compile-time DOM constructor dependency. */
import {
    RSocketConnectionError,
    type RSocketWebTransportSession
} from "rsocket-core-ts";

/** Constructor shape implemented by browser and compatible WebTransport runtimes. */
interface WebTransportConstructor {
    /** Opens one session to an HTTPS endpoint. */
    new(url: string, options?: {readonly requireUnreliable?: boolean}): RSocketWebTransportSession;
}

/** Uses the current realm's WebTransport implementation. */
export function defaultWebTransportFactory(
    url: string,
    requireUnreliable = false
): RSocketWebTransportSession {
    const constructor = (globalThis as {WebTransport?: WebTransportConstructor}).WebTransport;
    if (constructor === undefined) {
        throw new RSocketConnectionError(
            "WebTransport is unavailable; provide transport.factory or use a supported browser/runtime"
        );
    }
    return new constructor(url, requireUnreliable ? {requireUnreliable: true} : undefined);
}
