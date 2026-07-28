/** Stable logical connection exposed to server callbacks and controllers. */
import {Mono} from "reactor-core-ts";
import type {Metadata, MimeType} from "rsocket-frames-ts";
import {RSocketConnectionError} from "rsocket-core-ts";
import type {RSocketLeaseOptions, RSocketSetupContext} from "@/server/types.js";

/** Internal operations supplied by the logical protocol session. */
export interface ServerConnectionOperations<D = unknown, M = unknown> {
    /** Immutable SETUP view exposed by the public facade. */
    readonly setupContext: RSocketSetupContext<D, M>;
    /** Sends one connection-scoped metadata notification. */
    metadataPush(metadata: unknown, mimeType?: MimeType<any>): void;
    /** Replaces requester lease credits. */
    lease(options: RSocketLeaseOptions<M>): void;
    /** Requests an immediate peer KEEPALIVE response. */
    keepAlive(data?: Uint8Array): void;
    /** Sends one best-effort WebTransport media extension datagram. */
    media(payload: Uint8Array): void;
    /** Terminates the logical connection. */
    disconnect(reason?: string): void;
}

/**
 * User-facing control surface for one accepted logical RSocket session.
 * `D` and `M` are the decoded data and metadata types of its SETUP payload.
 */
export interface RSocketServerConnection<D = unknown, M = unknown> {
    /** Immutable parameters and optional payload received in SETUP. */
    readonly setup: RSocketSetupContext<D, M>;

    /** Sends asynchronous connection metadata with the negotiated codec. */
    metadataPush<PM = M>(metadata: NoInfer<PM> | Metadata<unknown>, mimeType?: MimeType<PM>): Mono<void>;

    /** Replaces the current requester lease when SETUP enabled lease semantics. */
    lease(options: RSocketLeaseOptions<M>): Mono<void>;

    /** Initiates an optional server-side KEEPALIVE request. */
    keepAlive(data?: Uint8Array): Mono<void>;

    /** Sends one best-effort media datagram when this session uses WebTransport. */
    media(payload: Uint8Array): Mono<void>;

    /** Sends a connection close error and terminates the logical session. */
    disconnect(reason?: string): Mono<void>;
}

/** Internal owner that can detach a terminated facade from its protocol session. */
export interface ServerConnectionHandle<D = unknown, M = unknown> {
    /** Narrow connection exposed to user code. */
    readonly connection: RSocketServerConnection<D, M>;
    /** Releases the strong session reference while retaining immutable SETUP state. */
    release(): void;
}

/** Creates a narrow facade without exposing protocol state-machine methods. */
export function createServerConnection<D, M>(
    operations: ServerConnectionOperations<D, M>
): ServerConnectionHandle<D, M> {
    let active: ServerConnectionOperations<D, M> | undefined = operations;
    let setup: RSocketSetupContext<D, M> | undefined;
    const current = (): ServerConnectionOperations<D, M> => {
        if (active !== undefined) return active;
        throw new RSocketConnectionError("RSocket server connection is closed");
    };
    const connection: RSocketServerConnection<D, M> = Object.freeze({
        /** Returns immutable SETUP state. */
        get setup() {
            return active?.setupContext ?? setup as RSocketSetupContext<D, M>;
        },
        /** Defers one metadata notification until subscription. */
        metadataPush<PM = M>(metadata: NoInfer<PM> | Metadata<unknown>, mimeType?: MimeType<PM>): Mono<void> {
            return actionMono(() => current().metadataPush(metadata, mimeType));
        },
        /** Defers one lease replacement until subscription. */
        lease(options: RSocketLeaseOptions<M>): Mono<void> {
            return actionMono(() => current().lease(options));
        },
        /** Defers one server KEEPALIVE request until subscription. */
        keepAlive(data?: Uint8Array): Mono<void> {
            return actionMono(() => current().keepAlive(data));
        },
        /** Defers one best-effort media datagram until subscription. */
        media(payload: Uint8Array): Mono<void> {
            return actionMono(() => current().media(payload));
        },
        /** Defers logical disconnection until subscription. */
        disconnect(reason?: string): Mono<void> {
            return actionMono(() => current().disconnect(reason));
        }
    });
    return {
        connection,
        release() {
            if (active === undefined) return;
            setup = active.setupContext;
            active = undefined;
        }
    };
}

/** Defers a synchronous protocol command until Mono subscription. */
function actionMono(action: () => void): Mono<void> {
    return Mono.create<void>((sink) => {
        try {
            action();
            sink.success();
        } catch (error) {
            sink.error(error);
        }
    });
}
