/** Versioned identifiers for the RSocket-over-WebTransport mapping. */

/** Current private mapping version. This is not part of the RSocket 1.0 specification. */
export const RSOCKET_WEBTRANSPORT_MAPPING_VERSION = 1;
/** Bytes before every reliable stream and extension datagram. */
export const RSOCKET_WEBTRANSPORT_PREFACE_LENGTH = 6;
/** Bytes before every reliable RSocket frame: ordinal plus 24-bit length. */
export const RSOCKET_WEBTRANSPORT_RECORD_HEADER_LENGTH = 11;
/** Bytes before a best-effort FNF frame: preface plus global ordinal. */
export const RSOCKET_WEBTRANSPORT_FNF_DATAGRAM_HEADER_LENGTH = 14;

/** Reliable stream roles negotiated by their preface. */
export const enum RSocketWebTransportStreamKind {
    CONTROL = 0x00,
    INTERACTION = 0x01,
    RELIABLE = 0x02
}

/** Unreliable extension payloads negotiated by their datagram preface. */
export const enum RSocketWebTransportDatagramKind {
    FIRE_AND_FORGET = 0x10,
    MEDIA = 0x11
}
