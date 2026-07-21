/** Runtime contract for the transport-neutral Core package root. */
import {describe, expect, it} from "vitest";
import * as api from "@/index.js";
// @ts-expect-error Requester sessions belong to rsocket-client-ts.
import type {RSocketClient as LeakedClient} from "@";
// @ts-expect-error Responder sessions belong to rsocket-server-ts.
import type {RSocketServer as LeakedServer} from "@";

/** Compile-time sentinel retaining endpoint-boundary import checks. */
type ForbiddenEndpointExports = LeakedClient | LeakedServer;

describe("public Core API", () => {
    it("exports only transport and protocol building blocks", () => {
        expect(Object.keys(api).sort()).toEqual([
            "AsyncQueue",
            "CLIENT_STREAM_ID",
            "DEFAULT_DATA_MIME_TYPE",
            "DEFAULT_MAX_FRAME_LENGTH",
            "DEFAULT_METADATA_MIME_TYPE",
            "ERROR_DATA_MIME_TYPE",
            "IgnoredPayloadFragments",
            "KEEPALIVE_DATA_MIME_TYPE",
            "MAX_REQUEST_N",
            "RSocketConnectionError",
            "RSocketError",
            "RSocketFrameSizeError",
            "RSocketInactivityTimer",
            "RSocketProtocolError",
            "RSocketReplayBuffer",
            "ReactiveTcpTransportConnection",
            "ReactiveTransportBinding",
            "ReactiveWebSocketTransportConnection",
            "RequestFragmentAssembler",
            "SERVER_STREAM_ID",
            "TcpFrameDecoder",
            "addReactiveDemand",
            "addWebSocketListener",
            "cancelSubscription",
            "compositeMetadata",
            "compositeMetadataEntries",
            "connectionClosedError",
            "createPullAsyncIterable",
            "data",
            "decodeFramePayload",
            "decodeInitialRequestFrame",
            "decodeWebSocketMessage",
            "deserializeFrame",
            "emitOutboundFrameFragments",
            "emitSerializedOutboundFrames",
            "encodeMetadataInput",
            "encodePayloadInput",
            "encodeTcpFrame",
            "errorFromFrame",
            "errorMessage",
            "errorPayload",
            "hasIgnorableInvalidMetadataLength",
            "isConnectionErrorCode",
            "isConnectionFrame",
            "isErrorCodeValidForStream",
            "isHandshakeErrorCode",
            "isIgnorableEstablishedFrame",
            "isIgnorableUnknownStreamFrame",
            "isInitialRequestFrame",
            "isPromiseLike",
            "isPublisher",
            "isResumePositionFrame",
            "isStreamErrorCode",
            "isTcpSocketOpen",
            "metadata",
            "nextRSocketStreamId",
            "normalizeFiniteReactiveDemand",
            "normalizeRSocketRoute",
            "normalizeReactiveDemand",
            "observeAbort",
            "outboundFrameLength",
            "payloadHasMoreFragments",
            "readFrameStreamId",
            "readFrameTypeAndFlags",
            "readKeepalivePosition",
            "reassemblePayloadFrame",
            "removeWebSocketListener",
            "requiresRawPayloadDecode",
            "route",
            "routingTags",
            "tcpCloseFlux",
            "tcpErrorFlux",
            "tcpFrameFlux",
            "unrefTimer",
            "webSocketBinaryData",
            "webSocketCloseFlux",
            "webSocketEventFlux",
            "webSocketFrameFlux",
            "webSocketSendData",
            "writeWebSocketFrame"
        ]);
        expect(api).not.toHaveProperty("OutboundPayloadParts");
    });
});

void (0 as unknown as ForbiddenEndpointExports);
