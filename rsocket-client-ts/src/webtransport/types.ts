/** Structural requester-side WebTransport factory types. */
import type {RSocketWebTransportSession} from "rsocket-core-ts";

/** Creates one native or test WebTransport session for an absolute HTTPS URL. */
export type RSocketWebTransportFactory = (url: string) => RSocketWebTransportSession;

/** Receives one best-effort media extension datagram. */
export type RSocketWebTransportMediaListener = (payload: Uint8Array) => void;
