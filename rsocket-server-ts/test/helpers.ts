import {RSocketClient} from "./client-engine.js";
import type {RSocketClientOptions} from "./client-engine.js";
import {type MimeType, type RSocketResumeToken, WellKnownMimeType} from "rsocket-frames-ts";
import type {RSocketPayloadInput} from "rsocket-core-ts";
import type {RSocketControllerRegistration, RSocketServerConnection, RSocketServerOptions} from "@/index.js";
import {RSocketServer} from "@/index.js";
import {memoryTransportPair, type MemoryTransport} from "./memory-transport.js";

/** Active in-memory client/server pair returned to integration tests. */
export interface ConnectedTestPair {
    readonly server: RSocketServer;
    readonly serverConnection: RSocketServerConnection;
    readonly client: RSocketClient;
    readonly clientTransport: MemoryTransport;
    readonly serverTransport: MemoryTransport;
}

/** Client options varied by integration tests while retaining deterministic defaults. */
export interface TestClientOptions {
    readonly resumeToken?: RSocketResumeToken;
    readonly maxFrameLength?: number;
    readonly keepAliveMs?: number;
    readonly lifetimeMs?: number;
    readonly honorLease?: boolean;
    readonly majorVersion?: number;
    readonly minorVersion?: number;
    readonly metadataMimeType?: MimeType<any>;
    readonly dataMimeType?: MimeType<any>;
    readonly payload?: RSocketPayloadInput<any, any>;
}

/** Builds transport-neutral client options shared by SETUP and RESUME tests. */
export function testClientOptions(
    transport: MemoryTransport,
    options: TestClientOptions = {}
): RSocketClientOptions {
    return {
        transport: () => transport,
        ...(options.maxFrameLength === undefined ? {} : {maxFrameLength: options.maxFrameLength}),
        setup: {
            keepAliveMs: options.keepAliveMs ?? 60_000,
            lifetimeMs: options.lifetimeMs ?? 60_000,
            majorVersion: options.majorVersion ?? 1,
            minorVersion: options.minorVersion ?? 0,
            metadataMimeType: options.metadataMimeType ?? WellKnownMimeType.MESSAGE_RSOCKET_COMPOSITE_METADATA,
            dataMimeType: options.dataMimeType ?? WellKnownMimeType.APPLICATION_JSON,
            ...(options.resumeToken === undefined ? {} : {resumeToken: options.resumeToken}),
            ...(options.honorLease === undefined ? {} : {honorLease: options.honorLease}),
            ...(options.payload === undefined ? {} : {payload: options.payload})
        }
    };
}

/** Opens SETUP concurrently on both ends of one in-memory transport. */
export async function connectTestPair(
    controllers: readonly RSocketControllerRegistration[],
    serverOptions: Partial<Omit<RSocketServerOptions, "controllers">> = {},
    clientOptions: TestClientOptions = {}
): Promise<ConnectedTestPair> {
    const server = new RSocketServer({controllers, ...serverOptions});
    const pair = memoryTransportPair();
    const accepted = server.accept(pair.server).block();
    const connected = RSocketClient.connect(testClientOptions(pair.client, clientOptions));
    const [serverConnection, client] = await Promise.all([accepted, connected]);
    if (serverConnection === undefined) throw new Error("Server accept completed without a connection");
    return {
        server,
        serverConnection,
        client,
        clientTransport: pair.client,
        serverTransport: pair.server
    };
}
