/** Public API boundary tests that keep requester implementation out of Core. */
import {describe, expect, it} from "vitest";
import * as core from "@";

const ENDPOINT_RUNTIME_EXPORTS = [
    "directRSocketFluxSubscription",
    "normalizeClientOptions",
    "receiveResumeOkFrame",
    "RequestChannelOutbound",
    "RSocketChannel",
    "RSocketClient",
    "RSocketFlux",
    "RSocketLeaseError",
    "RSocketServer",
    "RSocketStreamSubscription",
    "sendHandshakeFrame"
] as const;

describe("Core public API boundary", () => {
    it("does not expose requester runtime values", () => {
        for (const name of ENDPOINT_RUNTIME_EXPORTS) expect(core).not.toHaveProperty(name);
    });
});

// These intentional errors make tsc fail if client-only types return to Core.
// @ts-expect-error Client options belong to the requester package, not Core.
type ForbiddenClientOptions = import("@").RSocketClientOptions;
// @ts-expect-error Client SETUP configuration belongs to the requester package.
type ForbiddenSetupOptions = import("@").RSocketSetupOptions;
// @ts-expect-error Request-channel input ownership is requester-specific.
type ForbiddenChannelInput = import("@").RSocketChannelInput;
// @ts-expect-error Per-request client options are not Core API.
type ForbiddenRequestOptions = import("@").RSocketRequestOptions;
// @ts-expect-error Resume session state belongs to an endpoint implementation.
type ForbiddenResumeState = import("@").RSocketResumeState;
// @ts-expect-error Connecting transport factories belong to the requester package.
type ForbiddenTransportFactory = import("@").RSocketTransportFactory;
// @ts-expect-error Server configuration belongs to rsocket-server-ts.
type ForbiddenServerOptions = import("@").RSocketServerOptions;
// @ts-expect-error Server connections belong to rsocket-server-ts.
type ForbiddenServerConnection = import("@").RSocketServerConnection;
