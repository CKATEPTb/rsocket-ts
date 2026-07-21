/** Pure request-channel opening-payload classification tests. */
import {describe, expect, it} from "vitest";
import {
    RequestChannelFlag,
    RequestChannelFrame,
    WellKnownAuthType,
    WellKnownMimeType
} from "rsocket-frames-ts";
import {compositeMetadata, route, type RSocketPayloadFrame} from "rsocket-core-ts";
import {hasInitialChannelItem} from "@/session/protocol.js";

/** Builds a decoded opening context around one channel frame. */
function context(frame: RequestChannelFrame, data?: unknown): RSocketPayloadFrame {
    return {frame, ...(data === undefined ? {} : {data})};
}

describe("request-channel opening payload classification", () => {
    it("does not expose routing and authentication envelopes as application items", () => {
        const metadata = compositeMetadata(
            WellKnownMimeType.MESSAGE_RSOCKET_AUTHENTICATION.toMetadata(
                WellKnownAuthType.BEARER.auth("token")
            ),
            route("messages")
        );
        const frame = new RequestChannelFrame(1, RequestChannelFlag.METADATA, 1, metadata);

        expect(hasInitialChannelItem(frame, context(frame))).toBe(false);
    });

    it("preserves metadata-only application items alongside routing metadata", () => {
        const metadata = compositeMetadata(
            route("messages"),
            WellKnownMimeType.TEXT_PLAIN.toMetadata("application item")
        );
        const frame = new RequestChannelFrame(1, RequestChannelFlag.METADATA, 1, metadata);

        expect(hasInitialChannelItem(frame, context(frame))).toBe(true);
    });

    it("always preserves an opening data item", () => {
        const metadata = compositeMetadata(route("messages"));
        const frame = new RequestChannelFrame(1, RequestChannelFlag.METADATA, 1, metadata);

        expect(hasInitialChannelItem(frame, context(frame, false))).toBe(true);
    });
});
