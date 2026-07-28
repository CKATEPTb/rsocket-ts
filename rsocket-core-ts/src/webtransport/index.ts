/** Public WebTransport mapping surface shared by requester and responder packages. */
export {
    createWebTransportConnection,
    ReactiveWebTransportConnection
} from "@/webtransport/connection.js";
export type {
    RSocketWebTransportBidirectionalStream,
    RSocketWebTransportCloseInfo,
    RSocketWebTransportConnection,
    RSocketWebTransportConnectionOptions,
    RSocketWebTransportDatagrams,
    RSocketWebTransportReadable,
    RSocketWebTransportReader,
    RSocketWebTransportRole,
    RSocketWebTransportSession,
    RSocketWebTransportWritable,
    RSocketWebTransportWriter
} from "@/webtransport/types.js";
