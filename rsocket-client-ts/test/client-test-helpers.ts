/** Shared requester fixtures used by transport-neutral client tests. */
import {
    FrameDeserializer,
    WellKnownMimeType,
    type Frame,
    type MimeType
} from "rsocket-frames-ts";
import {RSocketClient, type RSocketClientOptions} from "@/client/index.js";
import {FakeTransportConnection} from "./fake-transport.js";

/** Default JSON data codec used by client protocol fixtures. */
export const dataMimeType: MimeType<any> = WellKnownMimeType.APPLICATION_JSON;
/** Default composite metadata codec used by client protocol fixtures. */
export const metadataMimeType: MimeType<any> = WellKnownMimeType.MESSAGE_RSOCKET_COMPOSITE_METADATA;

/** Creates minimal requester options around one in-memory physical connection. */
export function clientOptions(
    transport: FakeTransportConnection,
    resumeToken?: string
): RSocketClientOptions {
    return {
        transport: () => transport,
        setup: {
            keepAliveMs: 60_000,
            lifetimeMs: 120_000,
            dataMimeType,
            metadataMimeType,
            ...(resumeToken === undefined ? {} : {resumeToken})
        }
    };
}

/** Opens a requester over one in-memory transport. */
export function connectClient(
    transport: FakeTransportConnection,
    resumeToken?: string
): Promise<RSocketClient> {
    return RSocketClient.connect(clientOptions(transport, resumeToken));
}

/** Decodes one captured raw frame with selected MIME codecs. */
export function capturedFrame(
    bytes: Uint8Array | undefined,
    metadata: MimeType<any> = metadataMimeType,
    data: MimeType<any> = dataMimeType
): Frame {
    if (bytes === undefined) throw new Error("Expected an outbound frame");
    return FrameDeserializer.deserialize(bytes, metadata, data);
}

/** Allows cold Reactor work scheduled through promises to start. */
export function tick(): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, 0));
}
