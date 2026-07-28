/** Compile-time contract for the responder root and its connection facade. */
import type {Flux} from "reactor-core-ts";
import {MimeType} from "rsocket-frames-ts";
import type {RSocketPayloadFrame} from "rsocket-core-ts";
import {
    FireAndForgetController,
    RequestChannelController,
    RequestResponseController,
    RequestStreamController,
    RSocketRequestError,
    RSocketServer,
    type RSocketRequestContext,
    type RSocketServerConnection
} from "@";
// @ts-expect-error Responder sessions are implementation details.
import type {RSocketServerSession as LeakedServerSession} from "@";
// @ts-expect-error Controller registries are implementation details.
import type {ControllerRegistry as LeakedControllerRegistry} from "@";

/** Compile-time sentinel retaining forbidden root-import checks. */
type ForbiddenServerExports = LeakedServerSession | LeakedControllerRegistry;

/** Data expected in the client SETUP payload. */
interface SetupData {
    readonly client: string;
}

/** Metadata expected in the client SETUP payload. */
interface SetupMetadata {
    readonly tenant: string;
}

/** Metadata decoded for one routed controller request. */
interface RequestMetadata {
    readonly traceId: string;
}

/** Metadata encoded with one controller response. */
interface ResponseMetadata {
    readonly cached: boolean;
}

const textMimeType = new MimeType<string>("text/plain");

class FireController extends FireAndForgetController<string, RequestMetadata> {
    protected readonly route = "fire";
    override handle(data: string, context: RSocketRequestContext<string, RequestMetadata>): void {
        const requestData: string | undefined = context.data;
        const requestMetadata: RequestMetadata | undefined = context.metadata;
        void data;
        void requestData;
        void requestMetadata;
        // @ts-expect-error Request data stays string throughout the controller context.
        const invalidData: number | undefined = context.data;
        void invalidData;
    }
}

class ResponseController extends RequestResponseController<string, number, RequestMetadata, ResponseMetadata> {
    protected readonly route = "response";
    override handle(
        data: string,
        context: RSocketRequestContext<string, RequestMetadata>
    ): {readonly data: number; readonly metadata: ResponseMetadata} {
        const metadata: RequestMetadata | undefined = context.metadata;
        void metadata;
        return {data: data.length, metadata: {cached: true}};
    }
}

class StreamController extends RequestStreamController<string, number, RequestMetadata, ResponseMetadata> {
    protected readonly route = "stream";
    override handle(
        data: string,
        context: RSocketRequestContext<string, RequestMetadata>
    ): readonly {readonly data: number; readonly metadata: ResponseMetadata}[] {
        void context;
        return [{data: data.length, metadata: {cached: false}}];
    }
}

class ChannelController extends RequestChannelController<string, number, RequestMetadata, ResponseMetadata> {
    protected readonly route = "channel";
    override handle(
        requests: Flux<RSocketPayloadFrame<string, RequestMetadata>>,
        context: RSocketRequestContext<string, RequestMetadata>
    ): readonly {readonly data: number; readonly metadata: ResponseMetadata}[] {
        const typedRequests: Flux<RSocketPayloadFrame<string, RequestMetadata>> = requests;
        void typedRequests;
        void context;
        return [{data: 1, metadata: {cached: true}}];
    }
}

class InvalidResponseController extends RequestResponseController<string, string> {
    protected readonly route = "invalid";
    // @ts-expect-error Controller output must match its declared response type.
    override handle(): number {
        return 1;
    }
}

const server = new RSocketServer<SetupData, SetupMetadata>({
    controllers: [FireController, ResponseController, StreamController, ChannelController],
    lease: {ttlMs: 1_000, requests: 10, metadata: {tenant: "acme"}},
    accept(setup) {
        const data: SetupData | undefined = setup.data;
        const metadata: SetupMetadata | undefined = setup.metadata;
        const connection: RSocketServerConnection<SetupData, SetupMetadata> = setup.connection;
        void data;
        void metadata;
        void connection;
        // @ts-expect-error SETUP data keeps the server's declared type.
        const invalidData: string | undefined = setup.data;
        void invalidData;
        return true;
    },
    metadataPush(metadata, connection) {
        const value: SetupMetadata = metadata.payload;
        const typedConnection: RSocketServerConnection<SetupData, SetupMetadata> = connection;
        void value;
        void typedConnection;
        // @ts-expect-error METADATA_PUSH uses the declared SETUP metadata type.
        const invalidValue: string = metadata.payload;
        void invalidValue;
    },
    activityListener(activity) {
        const connection: RSocketServerConnection<SetupData, SetupMetadata> = activity.connection;
        void connection;
    }
});
new RSocketRequestError("rejected");

new RSocketServer<SetupData, SetupMetadata>({
    lease: {
        ttlMs: 1_000,
        requests: 1,
        // @ts-expect-error Raw lease metadata must match the declared SETUP metadata type.
        metadata: "invalid"
    }
});

/** Verifies the generic connection commands without opening a transport. */
function verifyConnection(connection: RSocketServerConnection<SetupData, SetupMetadata>): void {
    const setupData: SetupData | undefined = connection.setup.data;
    const setupMetadata: SetupMetadata | undefined = connection.setup.metadata;
    connection.metadataPush({tenant: "acme"});
    connection.metadataPush("maintenance", textMimeType);
    connection.metadataPush(textMimeType.toMetadata("already encoded"));
    connection.lease({ttlMs: 1_000, requests: 1, metadata: {tenant: "acme"}});
    // @ts-expect-error Raw metadata without a MIME override uses the SETUP metadata type.
    connection.metadataPush("invalid");
    // @ts-expect-error Lease metadata uses the declared SETUP metadata type.
    connection.lease({ttlMs: 1_000, requests: 1, metadata: "invalid"});
    void setupData;
    void setupMetadata;
}

type ExpectedServerMethods = "accept" | "acceptWebSocket" | "acceptWebTransport" | "listenTcp" | "close";
const serverSurface: Assert<Equal<keyof typeof server, ExpectedServerMethods>> = true;

type ExpectedConnectionMethods = "setup" | "metadataPush" | "lease" | "keepAlive" | "media" | "disconnect";
const connectionSurface: Assert<Equal<keyof RSocketServerConnection, ExpectedConnectionMethods>> = true;
type AcceptedConnection = NonNullable<Awaited<ReturnType<ReturnType<typeof server.accept>["block"]>>>;
const acceptedConnectionType: Assert<Equal<
    AcceptedConnection,
    RSocketServerConnection<SetupData, SetupMetadata>
>> = true;
type TcpListener = NonNullable<Awaited<ReturnType<ReturnType<typeof server.listenTcp>["block"]>>>;
type TcpConnection = TcpListener["connections"] extends Flux<infer C> ? C : never;
const tcpConnectionType: Assert<Equal<
    TcpConnection,
    RSocketServerConnection<SetupData, SetupMetadata>
>> = true;
void serverSurface;
void connectionSurface;
void acceptedConnectionType;
void tcpConnectionType;
void verifyConnection;
void InvalidResponseController;
void (0 as unknown as ForbiddenServerExports);

/** Compile-time equality predicate. */
type Equal<A, B> = (<T>() => T extends A ? 1 : 2) extends (<T>() => T extends B ? 1 : 2)
    ? (<T>() => T extends B ? 1 : 2) extends (<T>() => T extends A ? 1 : 2)
        ? true
        : false
    : false;

/** Fails compilation when a responder surface gains or loses a member. */
type Assert<T extends true> = T;
