/** Versioned prefaces for RSocket-over-WebTransport streams and datagrams. */
import {RSocketProtocolError} from "@/errors/index.js";
import {
    RSOCKET_WEBTRANSPORT_MAPPING_VERSION,
    RSOCKET_WEBTRANSPORT_PREFACE_LENGTH,
    RSocketWebTransportDatagramKind,
    RSocketWebTransportStreamKind
} from "@/webtransport/constants.js";

/** ASCII `RSWT`, used to reject unrelated streams on a shared session. */
const MAGIC_0 = 0x52;
const MAGIC_1 = 0x53;
const MAGIC_2 = 0x57;
const MAGIC_3 = 0x54;

/** Kind accepted in a reliable stream preface. */
export type RSocketWebTransportReliableKind =
    RSocketWebTransportStreamKind.CONTROL |
    RSocketWebTransportStreamKind.INTERACTION |
    RSocketWebTransportStreamKind.RELIABLE;

/** Kind accepted in an unreliable datagram preface. */
export type RSocketWebTransportUnreliableKind =
    RSocketWebTransportDatagramKind.FIRE_AND_FORGET |
    RSocketWebTransportDatagramKind.MEDIA;

/** Encodes one reliable stream preface. */
export function encodeWebTransportStreamPreface(kind: RSocketWebTransportReliableKind): Uint8Array {
    return encodePreface(kind);
}

/** Decodes and validates one complete reliable stream preface. */
export function decodeWebTransportStreamPreface(bytes: Uint8Array): RSocketWebTransportReliableKind {
    const kind = decodePreface(bytes);
    if (kind !== RSocketWebTransportStreamKind.CONTROL &&
        kind !== RSocketWebTransportStreamKind.INTERACTION &&
        kind !== RSocketWebTransportStreamKind.RELIABLE) {
        throw new RSocketProtocolError(`Unsupported RSocket WebTransport stream kind ${kind}`);
    }
    return kind;
}

/** Encodes a datagram preface followed by its extension payload. */
export function encodeWebTransportDatagram(
    kind: RSocketWebTransportUnreliableKind,
    payload: Uint8Array,
    prefixLength = RSOCKET_WEBTRANSPORT_PREFACE_LENGTH
): Uint8Array {
    const packet = new Uint8Array(prefixLength + payload.byteLength);
    writePreface(packet, kind);
    packet.set(payload, prefixLength);
    return packet;
}

/** Decodes and validates one complete datagram preface. */
export function decodeWebTransportDatagramPreface(bytes: Uint8Array): RSocketWebTransportUnreliableKind {
    const kind = decodePreface(bytes);
    if (kind !== RSocketWebTransportDatagramKind.FIRE_AND_FORGET &&
        kind !== RSocketWebTransportDatagramKind.MEDIA) {
        throw new RSocketProtocolError(`Unsupported RSocket WebTransport datagram kind ${kind}`);
    }
    return kind;
}

/** Encodes the common mapping preface. */
function encodePreface(kind: number): Uint8Array {
    const bytes = new Uint8Array(RSOCKET_WEBTRANSPORT_PREFACE_LENGTH);
    writePreface(bytes, kind);
    return bytes;
}

/** Writes the common preface without allocating an intermediate view. */
function writePreface(bytes: Uint8Array, kind: number): void {
    bytes[0] = MAGIC_0;
    bytes[1] = MAGIC_1;
    bytes[2] = MAGIC_2;
    bytes[3] = MAGIC_3;
    bytes[4] = RSOCKET_WEBTRANSPORT_MAPPING_VERSION;
    bytes[5] = kind;
}

/** Validates the common magic and version and returns its final kind byte. */
function decodePreface(bytes: Uint8Array): number {
    if (bytes.byteLength < RSOCKET_WEBTRANSPORT_PREFACE_LENGTH) {
        throw new RSocketProtocolError("RSocket WebTransport preface is incomplete");
    }
    if (bytes[0] !== MAGIC_0 || bytes[1] !== MAGIC_1 || bytes[2] !== MAGIC_2 || bytes[3] !== MAGIC_3) {
        throw new RSocketProtocolError("RSocket WebTransport preface has invalid magic bytes");
    }
    if (bytes[4] !== RSOCKET_WEBTRANSPORT_MAPPING_VERSION) {
        throw new RSocketProtocolError(
            `Unsupported RSocket WebTransport mapping version ${bytes[4] as number}`
        );
    }
    return bytes[5] as number;
}
