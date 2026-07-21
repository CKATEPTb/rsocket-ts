/**
 * Public transport-selecting RSocket requester facade.
 *
 * The package root exports this constructor and the four declarative
 * controller classes. Transport and protocol machinery remain internal.
 */
import {Mono} from "reactor-core-ts";
import {Metadata, type MimeType, type Payload, WellKnownMimeType} from "rsocket-frames-ts";
import {
    prependChannelPayload,
    RSocketChannel,
} from "@/channel/index.js";
import {RSocketClient} from "@/client/session.js";
import {normalizeClientOptions} from "@/client/options.js";
import type {
    RSocketChannelInput,
    RSocketClientConfiguration,
    RSocketClientOptions,
    RSocketFrameActivityListener,
    RSocketRequestOptions,
    RSocketStreamRequestOptions
} from "@/client/types.js";
import {RSocketFlux} from "@/stream/index.js";
import {
    RSocketConnectionError,
    isPromiseLike,
    type RSocketPayloadFrame,
    type RSocketPayloadInput,
    unrefTimer
} from "rsocket-core-ts";
import {
    type AnyClassController,
    type ControllerArgs,
    type ControllerReturn,
    type RSocketControllerInput,
    RSocketControllerProcessor
} from "@/controllers/index.js";
import {RSocketLogger} from "@/logging/runtime.js";
import {
    RSocketMetadataStore,
    type RSocketMetadataMap,
    type RSocketMetadataPatch,
    type RSocketMetadataUpdater
} from "@/metadata/index.js";
import {
    connectionEvent,
    deferred,
    type Deferred,
    immediateReconnectSignals,
    normalizeReconnectOptions,
    RECONNECT_MIN_UPTIME_MS,
    reconnectDelay,
    type RSocketAnyConnectionEventListener,
    type RSocketConnectionEvent,
    type RSocketConnectionEventDraft,
    type RSocketConnectionEventHandlers,
    RSocketEventHub,
    type RSocketReconnectOptions,
    type RSocketReconnectSignals
} from "@/reconnect/index.js";
import {RSocketResumeCoordinator} from "@/resume/coordinator.js";
import {
    EMPTY_REQUEST_OPTIONS,
    requestOptionsFromMimeTypes,
    type RSocketMimeTypes,
    type RSocketOptions,
    type RSocketTransportOptions,
    toClientConfiguration,
    withClientResumeToken
} from "@/rsocket/options.js";
import {
    transportProvider,
    type RSocketTransportProvider,
    validateTransportOptions
} from "@/rsocket/transport.js";
import {validateWebSocketClose} from "@/websocket/spec.js";

/** Data accepted by one positional interaction call. */
type RSocketDataArgument<D> = D | Payload<unknown>;

/** Metadata accepted by one positional interaction call. */
type RSocketMetadataArgument<M> = M | Metadata<unknown>;

/** MIME override accepted by connection-level metadata push. */
type RSocketMetadataPushOptions<M> = {
    /** MIME codec used when the supplied metadata value is not already encoded. */
    readonly metadataMimeType?: MimeType<M>;
};

/** Return type selected by the optional request-channel source argument. */
type RSocketChannelReturn<OD, OM, RD, RM, Input> = Input extends undefined
    ? RSocketChannel<OD, OM, RD, RM>
    : RSocketFlux<RSocketPayloadFrame<RD, RM>>;

/** Shared envelope used when an interaction intentionally carries no payload. */
const EMPTY_INTERACTION_PAYLOAD = Object.freeze({data: undefined}) as RSocketPayloadInput<any, any>;
/** Shared no-op logger for sockets that do not enable diagnostics. */
const DISABLED_LOGGER = new RSocketLogger();
/** Private cross-package hook used by the browser facade without widening this class. */
const RECONNECT_SIGNALS = Symbol.for("rsocket-client-ts.reconnect-signals");

/**
 * Disconnected state surface returned by `disconnect()`.
 */
interface DisconnectedRSocket<D, M> {
    /** Opens the configured transport and sends the RSocket SETUP frame. */
    connect(): Mono<ConnectedRSocket<D, M>>;

    /** Executes a declarative controller through its declared interaction model. */
    process<C extends AnyClassController>(
        controllerDefinition: RSocketControllerInput<C>,
        ...args: ControllerArgs<C>
    ): ControllerReturn<C>;

    /** Sends a fire-and-forget request after a connection is available. */
    fireAndForget<PD = D, PM = M>(
        data?: RSocketDataArgument<NoInfer<PD>>,
        metadata?: RSocketMetadataArgument<NoInfer<PM>>,
        mimetype?: RSocketMimeTypes<PD, PM>
    ): Mono<void>;

    /** Sends a request-response request after a connection is available. */
    requestResponse<PD = D, PM = M>(
        data?: RSocketDataArgument<NoInfer<PD>>,
        metadata?: RSocketMetadataArgument<NoInfer<PM>>,
        mimetype?: RSocketMimeTypes<PD, PM>
    ): Mono<RSocketPayloadFrame<D, M>>;

    /** Sends a request-stream request after a connection is available. */
    requestStream<PD = D, PM = M>(
        data?: RSocketDataArgument<NoInfer<PD>>,
        metadata?: RSocketMetadataArgument<NoInfer<PM>>,
        mimetype?: RSocketMimeTypes<PD, PM>
    ): RSocketFlux<RSocketPayloadFrame<D, M>>;

    /** Starts request-channel after a connection is available. */
    requestChannel<
        PD = D,
        PM = M,
        Input extends RSocketChannelInput<NoInfer<PD>, NoInfer<PM>> | undefined = undefined
    >(
        data?: Input,
        metadata?: RSocketMetadataArgument<NoInfer<PM>>,
        mimetype?: RSocketMimeTypes<PD, PM>
    ): RSocketChannelReturn<PD, PM, D, M, Input>;

    /** Sends a connection-level METADATA_PUSH frame. */
    metadataPush<PM = M>(
        metadataPayload: RSocketMetadataArgument<NoInfer<PM>>,
        options?: RSocketMetadataPushOptions<PM>
    ): Mono<void>;

    /** Updates MIME-keyed defaults merged into subsequent outgoing metadata. */
    metadataUpdate(update: RSocketMetadataPatch | RSocketMetadataUpdater): RSocketMetadataMap;
}

/**
 * Connected state surface returned by `connect()`.
 */
interface ConnectedRSocket<D, M> extends Omit<DisconnectedRSocket<D, M>, "connect"> {
    /** Disconnects the active session and returns the disconnected surface. */
    disconnect(code?: number, reason?: string): DisconnectedRSocket<D, M>;
}

/**
 * High-level RSocket requester with WebSocket and Node TCP transports.
 *
 * The facade owns reconnect policy, event emission, metadata overlays, typed
 * controllers, and the current low-level requester session.
 * Every interaction uses Reactor `Mono` or `Flux` types from `reactor-core-ts`.
 * `D` and `M` are inferred from SETUP codecs and always describe decoded
 * responses; per-request MIME generics describe outbound values only.
 */
export class RSocket<D = unknown, M = unknown> {
    private readonly clientConfiguration: RSocketClientConfiguration<D, M>;
    private readonly transportOptions: RSocketTransportOptions;
    private readonly provideTransport: RSocketTransportProvider;
    private readonly isWebSocket: boolean;
    private readonly reconnectOptions: RSocketReconnectOptions;
    private readonly resumeCoordinator: RSocketResumeCoordinator<RSocketClient<D, M>>;
    private readonly metadataStore: RSocketMetadataStore;
    private readonly logger: RSocketLogger;
    /** Lazily created controller dispatcher scoped to this facade. */
    private controllerProcessor: RSocketControllerProcessor | undefined;
    private eventHub: RSocketEventHub | undefined;
    private anyEventListeners: Set<RSocketAnyConnectionEventListener> | undefined;
    private client: RSocketClient<D, M> | undefined;
    private readyDeferred: Deferred<this> = deferred<this>();
    private reconnectTimer: ReturnType<typeof setTimeout> | undefined;
    private stableConnectionTimer: ReturnType<typeof setTimeout> | undefined;
    private reconnectAttempts = 0;
    private connectToken = 0;
    private connectAbortController: AbortController | undefined;
    private connecting = false;
    private closeRequested = true;
    private optionsValidated = false;
    private lastError: unknown;
    private readonly reconnectSignals: RSocketReconnectSignals;
    private disposeWakeListener: (() => void) | undefined;
    /** Connected state facade with a deliberately tiny public surface. */
    private readonly connectedSurface: ConnectedRSocket<D, M> = Object.freeze({
        process: this.process.bind(this) as ConnectedRSocket<D, M>["process"],
        fireAndForget: this.fireAndForget.bind(this) as ConnectedRSocket<D, M>["fireAndForget"],
        requestResponse: this.requestResponse.bind(this) as ConnectedRSocket<D, M>["requestResponse"],
        requestStream: this.requestStream.bind(this) as ConnectedRSocket<D, M>["requestStream"],
        requestChannel: this.requestChannel.bind(this) as ConnectedRSocket<D, M>["requestChannel"],
        metadataPush: this.metadataPush.bind(this) as ConnectedRSocket<D, M>["metadataPush"],
        metadataUpdate: this.metadataUpdate.bind(this),
        disconnect: (code?: number, reason?: string) => this.disconnectNow(code, reason)
    });
    /**
     * Creates a disconnected requester for the selected transport.
     */
    constructor(options: RSocketOptions<D, M>) {
        this.reconnectSignals = internalReconnectSignals(options);
        this.logger = options.log === undefined
            ? DISABLED_LOGGER
            : new RSocketLogger(options.log);
        if (options.events !== undefined) this.registerEventHandlers(options.events);
        this.resumeCoordinator = new RSocketResumeCoordinator(options);
        const clientConfiguration = toClientConfiguration(options, this.resumeCoordinator.token);
        const setupMetadataMimeType = clientConfiguration.setup?.metadataMimeType ??
            WellKnownMimeType.MESSAGE_RSOCKET_COMPOSITE_METADATA;
        this.metadataStore = new RSocketMetadataStore(setupMetadataMimeType);
        if (this.logger.framesEnabled) {
            const activityListener: RSocketFrameActivityListener = (activity) => this.logger.frame(activity);
            this.clientConfiguration = {...clientConfiguration, activityListener};
        } else {
            this.clientConfiguration = clientConfiguration;
        }
        this.transportOptions = options.transport;
        this.provideTransport = transportProvider(options.transport, clientConfiguration.maxFrameLength);
        this.isWebSocket = options.transport.type === "websocket";
        this.reconnectOptions = normalizeReconnectOptions(options);
    }

    /**
     * Opens the configured transport, sends SETUP, and emits the connected state surface.
     *
     * The returned `Mono` is cold: the connection attempt starts when the Mono is
     * subscribed or blocked.
     */
    connect(): Mono<ConnectedRSocket<D, M>> {
        return Mono.defer(() => this.connectNow());
    }

    /**
     * Starts or joins the current connection attempt for `connect()`.
     */
    private connectNow(): Mono<ConnectedRSocket<D, M>> {
        if (this.connected) return Mono.just(this.connectedFacade());
        try {
            this.ensureConnectionStarted();
        } catch (error) {
            return Mono.error(error);
        }
        return this.readyDeferred.mono().map(() => this.connectedFacade());
    }

    /**
     * Indicates whether a live requester session is available.
     */
    private get connected(): boolean {
        return this.client !== undefined && !this.client.isClosed;
    }

    /**
     * Disconnects the active session but keeps the facade reusable.
     */
    private disconnectNow(code?: number, reason = "RSocket client disconnected"): DisconnectedRSocket<D, M> {
        if (this.closeRequested && this.client === undefined && !this.connecting && this.reconnectTimer === undefined) {
            return this.disconnectedFacade();
        }
        if (this.isWebSocket) validateWebSocketClose(code, reason);
        this.closeRequested = true;
        this.connectToken += 1;
        this.abortConnect();
        this.connecting = false;
        this.clearReconnectTimer();
        this.clearStableConnectionTimer();
        this.clearWakeListener();

        const error = new RSocketConnectionError(reason);
        this.lastError = error;
        this.readyDeferred.reject(error);

        const client = this.client;
        this.client = undefined;
        this.resumeCoordinator.dispose((retainedClient) => {
            if (retainedClient !== client) this.closeClientQuietly(retainedClient, code, reason);
        });
        if (client) this.closeClientQuietly(client, code, reason);

        this.emitLifecycle({
            type: "disconnect",
            attempt: this.reconnectAttempts,
            reconnect: false,
            error,
            willReconnect: false
        });
        return this.disconnectedFacade();
    }

    /**
     * Executes a declarative controller through its declared interaction model.
     */
    process<C extends AnyClassController>(
        controllerDefinition: RSocketControllerInput<C>,
        ...args: ControllerArgs<C>
    ): ControllerReturn<C> {
        const processor = this.controllerProcessor ??= this.createControllerProcessor();
        return processor.process(controllerDefinition, ...args);
    }

    /** Creates the controller bridge only when `process(...)` is first used. */
    private createControllerProcessor(): RSocketControllerProcessor {
        return new RSocketControllerProcessor(Object.freeze({
            fireAndForget: (payload: RSocketPayloadInput<any, any>, options?: RSocketRequestOptions) =>
                this.fireAndForgetPayload(payload, normalizeFacadeRequestOptions(options)),
            requestResponse: (payload: RSocketPayloadInput<any, any>, options?: RSocketRequestOptions) =>
                this.requestResponsePayload(payload, normalizeFacadeRequestOptions(options)),
            requestStream: (payload: RSocketPayloadInput<any, any>, options?: RSocketStreamRequestOptions) =>
                this.requestStreamFlux(payload, normalizeFacadeRequestOptions(options)),
            requestChannel: (payloads: RSocketChannelInput<any, any>, options?: RSocketStreamRequestOptions) =>
                this.requestChannelFlux(payloads, normalizeFacadeRequestOptions(options))
        }));
    }

    /**
     * Sends a fire-and-forget request after a connection is available.
     */
    fireAndForget<PD = D, PM = M>(
        data?: RSocketDataArgument<NoInfer<PD>>,
        metadata?: RSocketMetadataArgument<NoInfer<PM>>,
        mimetype?: RSocketMimeTypes<PD, PM>
    ): Mono<void> {
        return this.fireAndForgetPayload(
            interactionPayload(data, metadata),
            requestOptionsFromMimeTypes(mimetype)
        );
    }

    /**
     * Sends a request-response request and emits one response decoded by SETUP codecs.
     */
    requestResponse<PD = D, PM = M>(
        data?: RSocketDataArgument<NoInfer<PD>>,
        metadata?: RSocketMetadataArgument<NoInfer<PM>>,
        mimetype?: RSocketMimeTypes<PD, PM>
    ): Mono<RSocketPayloadFrame<D, M>> {
        return this.requestResponsePayload(
            interactionPayload(data, metadata),
            requestOptionsFromMimeTypes(mimetype)
        );
    }

    /**
     * Sends a request-stream request and returns SETUP-decoded responses on demand.
     */
    requestStream<PD = D, PM = M>(
        data?: RSocketDataArgument<NoInfer<PD>>,
        metadata?: RSocketMetadataArgument<NoInfer<PM>>,
        mimetype?: RSocketMimeTypes<PD, PM>
    ): RSocketFlux<RSocketPayloadFrame<D, M>> {
        return this.requestStreamFlux(
            interactionPayload(data, metadata),
            requestOptionsFromMimeTypes(mimetype)
        );
    }

    /**
     * Starts request-channel from an existing outbound payload source.
     */
    requestChannel<
        PD = D,
        PM = M,
        Input extends RSocketChannelInput<NoInfer<PD>, NoInfer<PM>> | undefined = undefined
    >(
        data?: Input,
        metadata?: RSocketMetadataArgument<NoInfer<PM>>,
        mimetype?: RSocketMimeTypes<PD, PM>
    ): RSocketChannelReturn<PD, PM, D, M, Input> {
        const options = requestOptionsFromMimeTypes(mimetype);
        if (data === undefined) {
            return new RSocketChannel<PD, PM, D, M>(
                (payloads) => this.requestChannelFlux(channelInputWithMetadata(payloads, metadata), options)
            ) as RSocketChannelReturn<PD, PM, D, M, Input>;
        }
        return this.requestChannelFlux(
            channelInputWithMetadata(data, metadata),
            options
        ) as RSocketChannelReturn<PD, PM, D, M, Input>;
    }

    /**
     * Sends a connection-level METADATA_PUSH frame.
     */
    metadataPush<PM = M>(
        metadataPayload: RSocketMetadataArgument<NoInfer<PM>>,
        options?: RSocketMetadataPushOptions<PM>
    ): Mono<void> {
        const normalized = normalizeFacadeRequestOptions(options);
        return this.withReadyClientMono((client) => client.metadataPush(
            this.withClientMetadataValue(metadataPayload, normalized),
            this.withClientMetadataOptions(normalized)
        ));
    }

    /**
     * Updates MIME-keyed defaults merged into subsequent outgoing metadata.
     */
    metadataUpdate(update: RSocketMetadataPatch | RSocketMetadataUpdater): RSocketMetadataMap {
        return this.metadataStore.update(update);
    }

    /** Sends one already assembled fire-and-forget payload. */
    private fireAndForgetPayload(
        payload: RSocketPayloadInput<any, any>,
        options: RSocketRequestOptions
    ): Mono<void> {
        return this.withReadyClientMono((client) => client.fireAndForget(
            this.withClientMetadata(payload, options),
            this.withClientMetadataOptions(options)
        ));
    }

    /** Sends one already assembled request-response payload. */
    private requestResponsePayload(
        payload: RSocketPayloadInput<any, any>,
        options: RSocketRequestOptions
    ): Mono<RSocketPayloadFrame<D, M>> {
        return this.withReadyClientMono((client) => client.requestResponse(
            this.withClientMetadata(payload, options),
            this.withClientMetadataOptions(options)
        ));
    }

    /**
     * Creates a request-stream Flux bound to the current connected client.
     */
    private requestStreamFlux<PD, PM>(
        payload: RSocketPayloadInput<PD, PM>,
        options: RSocketStreamRequestOptions
    ): RSocketFlux<RSocketPayloadFrame<D, M>> {
        return this.withReadyClientFlux((client) => client.requestStream(
            this.withClientMetadata(payload, options),
            this.withClientMetadataOptions(options)
        ));
    }

    /**
     * Creates a request-channel Flux bound to the current connected client.
     */
    private requestChannelFlux<PD, PM>(
        payloads: RSocketChannelInput<PD, PM>,
        options: RSocketStreamRequestOptions
    ): RSocketFlux<RSocketPayloadFrame<D, M>> {
        return this.withReadyClientFlux((client) => client.requestChannel(
            this.withClientMetadataInput(payloads, options),
            this.withClientMetadataOptions(options)
        ));
    }

    /**
     * Starts one physical transport connection attempt.
     */
    private openConnection(reconnect: boolean, cause?: unknown): void {
        const token = ++this.connectToken;
        this.abortConnect();
        const abortController = new AbortController();
        this.connectAbortController = abortController;
        const attempt = reconnect ? this.reconnectAttempts : 0;
        this.connecting = true;
        this.emitLifecycle({
            type: "connecting",
            attempt,
            reconnect,
            error: cause,
            willReconnect: this.canReconnect()
        });
        if (!this.isCurrentConnect(token)) return;

        void this.openReadyConnection(reconnect, attempt, abortController.signal).then(
            (client) => {
                if (!this.isCurrentConnect(token)) {
                    this.closeClientQuietly(client, 1000, "Stale RSocket connection");
                    return;
                }

                if (this.connectAbortController === abortController) this.connectAbortController = undefined;
                this.connecting = false;
                this.client = client;
                this.lastError = undefined;
                this.markConnectionStableAfterUptime(client, reconnect);
                client.onClose((error) => this.handleClientClose(client, error));
                if (this.client !== client || client.isClosed) return;
                this.emitLifecycle({
                    type: "connected",
                    attempt,
                    reconnect,
                    willReconnect: false
                });
                if (!this.isCurrentConnect(token) || this.client !== client || client.isClosed) return;
                this.readyDeferred.resolve(this);
            },
            (error) => {
                if (!this.isCurrentConnect(token)) return;

                if (this.connectAbortController === abortController) this.connectAbortController = undefined;
                this.connecting = false;
                this.client = undefined;
                this.lastError = error;
                if (reconnect) {
                    this.emitLifecycle({
                        type: "reconnectFailed",
                        attempt,
                        reconnect: true,
                        error,
                        willReconnect: this.canReconnect()
                    });
                }
                this.scheduleReconnect(error);
            }
        );
    }

    /**
     * Waits for runtime availability, then performs RESUME or SETUP.
     */
    private async openReadyConnection(
        reconnect: boolean,
        attempt: number,
        abortSignal: AbortSignal
    ): Promise<RSocketClient<D, M>> {
        if (reconnect && !this.reconnectSignals.isAvailable()) {
            await this.reconnectSignals.waitUntilAvailable(abortSignal);
        }
        const transportFactoryResult = this.provideTransport();
        const transportFactory = isPromiseLike<RSocketClientOptions<D, M>["transport"]>(transportFactoryResult)
            ? await transportFactoryResult
            : transportFactoryResult;
        const clientOptions = (token: string | undefined): RSocketClientOptions<D, M> => ({
            ...withClientResumeToken(this.clientConfiguration, token),
            transport: transportFactory,
            abortSignal
        });
        return this.resumeCoordinator.open(reconnect, this.lastError, {
            connect: (token) => RSocketClient.connect(clientOptions(token)),
            resume: (client, token) => RSocketClient.resume(clientOptions(token), {client}),
            isAborted: () => abortSignal.aborted,
            onFallback: ({error, rejected}) => {
                if (!rejected) return;
                this.emitLifecycle({
                    type: "resumeRejected",
                    attempt,
                    reconnect: true,
                    error,
                    willReconnect: true
                });
            }
        });
    }

    /**
     * Handles closure of the currently active low-level session.
     */
    private handleClientClose(client: RSocketClient<D, M>, error: unknown): void {
        if (client !== this.client) return;
        this.resumeCoordinator.retain(client);
        this.client = undefined;
        this.lastError = error;
        this.clearStableConnectionTimer();
        if (this.closeRequested) return;

        this.ensureReadyPending();
        this.emitLifecycle({
            type: "disconnect",
            attempt: this.reconnectAttempts,
            reconnect: false,
            error,
            willReconnect: this.canReconnect()
        });
        this.scheduleReconnect(error);
    }

    /**
     * Schedules the next connection attempt or closes the facade permanently.
     */
    private scheduleReconnect(error: unknown): void {
        this.clearReconnectTimer();
        if (this.closeRequested || this.connecting) return;
        if (!this.canReconnect()) {
            this.resumeCoordinator.reset(error);
            this.readyDeferred.reject(error);
            this.clearWakeListener();
            this.emitLifecycle({
                type: "closed",
                attempt: this.reconnectAttempts,
                reconnect: false,
                error,
                willReconnect: false
            });
            return;
        }

        this.ensureReadyPending();
        const attempt = this.reconnectAttempts + 1;
        const delayMs = this.nextReconnectDelay(attempt);
        this.reconnectAttempts = attempt;
        const timer = setTimeout(() => {
            if (this.reconnectTimer !== timer) return;
            this.reconnectTimer = undefined;
            if (!this.closeRequested && !this.connecting) this.openConnection(true, error);
        }, delayMs);
        this.reconnectTimer = timer;
        unrefTimer(timer);
        this.emitLifecycle({
            type: "reconnecting",
            attempt,
            reconnect: true,
            error,
            delayMs,
            willReconnect: true
        });
    }

    /**
     * Checks whether reconnect remains enabled by user intent.
     */
    private canReconnect(): boolean {
        return !this.closeRequested && this.reconnectOptions.enabled;
    }

    /**
     * Calculates the next reconnect delay while preserving the resume deadline.
     */
    private nextReconnectDelay(attempt: number): number {
        return this.resumeCoordinator.limitDelay(reconnectDelay(attempt));
    }

    /**
     * Resets retry count only after the connection survives PartySocket-style min uptime.
     */
    private markConnectionStableAfterUptime(client: RSocketClient<D, M>, reconnect: boolean): void {
        this.clearStableConnectionTimer();
        if (!reconnect) {
            this.reconnectAttempts = 0;
            return;
        }

        const timer = setTimeout(() => {
            if (this.stableConnectionTimer !== timer) return;
            this.stableConnectionTimer = undefined;
            if (client === this.client && !client.isClosed) this.reconnectAttempts = 0;
        }, RECONNECT_MIN_UPTIME_MS);
        this.stableConnectionTimer = timer;
        unrefTimer(timer);
    }

    /**
     * Reacts to runtime wake or network-availability events.
     */
    private handleWake(): void {
        if (this.closeRequested) return;

        const client = this.currentClient();
        if (client !== undefined) {
            client.checkLifetime();
            return;
        }

        if (this.connecting || !this.canReconnect()) return;
        this.clearReconnectTimer();
        this.openConnection(true, this.lastError);
    }

    /**
     * Installs runtime wake listeners while this facade is connected or reconnecting.
     */
    private ensureWakeListener(): void {
        if (this.disposeWakeListener !== undefined) return;

        let registered = false;
        const dispose = this.reconnectSignals.onWake(() => {
            if (registered) this.handleWake();
        });
        registered = true;
        this.disposeWakeListener = dispose;
    }

    /**
     * Removes runtime wake listeners after explicit disconnect or permanent close.
     */
    private clearWakeListener(): void {
        const dispose = this.disposeWakeListener;
        this.disposeWakeListener = undefined;
        try {
            dispose?.();
        } catch {
            // Runtime signal cleanup cannot prevent an explicit socket shutdown.
        }
    }

    /**
     * Replaces the ready promise after a previous ready promise settled.
     */
    private ensureReadyPending(): void {
        if (this.readyDeferred.settled) this.readyDeferred = deferred<this>();
    }

    /**
     * Cancels a pending reconnect timer when present.
     */
    private clearReconnectTimer(): void {
        if (this.reconnectTimer === undefined) return;
        clearTimeout(this.reconnectTimer);
        this.reconnectTimer = undefined;
    }

    /**
     * Cancels the pending stable-uptime timer.
     */
    private clearStableConnectionTimer(): void {
        if (this.stableConnectionTimer === undefined) return;
        clearTimeout(this.stableConnectionTimer);
        this.stableConnectionTimer = undefined;
    }

    /**
     * Registers constructor-time lifecycle event handlers.
     */
    private registerEventHandlers(handlers: RSocketConnectionEventHandlers): void {
        if (handlers.event !== undefined) (this.anyEventListeners ??= new Set<RSocketAnyConnectionEventListener>()).add(handlers.event);
        const eventHub = hasSpecificEventHandler(handlers)
            ? this.eventHub ??= new RSocketEventHub()
            : undefined;
        if (handlers.connecting !== undefined) eventHub?.on("connecting", handlers.connecting);
        if (handlers.connected !== undefined) eventHub?.on("connected", handlers.connected);
        if (handlers.disconnect !== undefined) eventHub?.on("disconnect", handlers.disconnect);
        if (handlers.reconnecting !== undefined) eventHub?.on("reconnecting", handlers.reconnecting);
        if (handlers.resumeRejected !== undefined) eventHub?.on("resumeRejected", handlers.resumeRejected);
        if (handlers.reconnectFailed !== undefined) eventHub?.on("reconnectFailed", handlers.reconnectFailed);
        if (handlers.closed !== undefined) eventHub?.on("closed", handlers.closed);
    }

    /**
     * Aborts the in-flight physical transport open attempt.
     */
    private abortConnect(): void {
        this.connectAbortController?.abort();
        this.connectAbortController = undefined;
    }

    /**
     * Closes a low-level session after facade state has already moved on.
     */
    private closeClientQuietly(client: RSocketClient<D, M>, code: number | undefined, reason: string): void {
        try {
            client.close(code, reason);
        } catch (error) {
            this.lastError ??= error;
        }
    }

    /**
     * Guards async connect continuations against stale attempts.
     */
    private isCurrentConnect(token: number): boolean {
        return !this.closeRequested && token === this.connectToken;
    }

    /**
     * Emits lifecycle events to subscribers and optional lifecycle logs.
     */
    private emitLifecycle(draft: RSocketConnectionEventDraft): void {
        const lifecycleLogging = this.logger.lifecycleEnabled;
        const anyListeners = this.anyEventListeners;
        const eventHub = this.eventHub;
        if (!lifecycleLogging && (anyListeners === undefined || anyListeners.size === 0) && eventHub?.has(draft.type) !== true) return;

        const event = connectionEvent(draft);
        eventHub?.emit(event);
        if (anyListeners !== undefined) this.emitAnyLifecycle(event, anyListeners);
        if (lifecycleLogging) this.logger.lifecycle(event);
    }

    /**
     * Emits one lifecycle event to all-event listeners.
     */
    private emitAnyLifecycle(
        event: RSocketConnectionEvent,
        listeners: Set<RSocketAnyConnectionEventListener>
    ): void {
        for (const listener of listeners) {
            try {
                listener(event);
            } catch {
                // UI listeners must not break reconnect scheduling.
            }
        }
    }

    /**
     * Defers a one-shot interaction until a low-level client is connected.
     */
    private withReadyClientMono<T>(factory: (client: RSocketClient<D, M>) => Mono<T>): Mono<T> {
        return Mono.defer(() => {
            const client = this.currentClient();
            if (client !== undefined) return factory(client);
            try {
                this.ensureConnectionStarted();
            } catch (error) {
                return Mono.error(error);
            }
            return this.readyDeferred.mono().map(() => this.requireClient()).flatMap(factory);
        });
    }

    /**
     * Defers a streaming interaction until a low-level client is connected.
     */
    private withReadyClientFlux<T extends RSocketPayloadFrame>(
        factory: (client: RSocketClient<D, M>) => RSocketFlux<T>
    ): RSocketFlux<T> {
        return RSocketFlux.deferSource((signal) => {
            const client = this.currentClient();
            return client !== undefined
                ? factory(client)
                : this.readyClient(signal).then((readyClient) => ({source: factory(readyClient)}));
        });
    }

    /**
     * Resolves with a connected low-level client after initial connect or reconnect.
     */
    private async readyClient(signal: AbortSignal): Promise<RSocketClient<D, M>> {
        const client = this.currentClient();
        if (client !== undefined) return client;
        this.ensureConnectionStarted();
        await this.readyDeferred.wait(signal);
        return this.requireClient();
    }

    /**
     * Validates options and starts the initial connection when no attempt is active.
     */
    private ensureConnectionStarted(): void {
        this.validateConnectOptions();
        if (this.connected || this.connecting || this.reconnectTimer !== undefined) return;
        this.closeRequested = false;
        this.ensureWakeListener();
        this.ensureReadyPending();
        this.openConnection(false);
    }

    /**
     * Returns the active low-level client without changing connection state.
     */
    private currentClient(): RSocketClient<D, M> | undefined {
        return this.client !== undefined && !this.client.isClosed ? this.client : undefined;
    }

    /**
     * Merges current client metadata into one outgoing request payload.
     */
    private withClientMetadata<D, M>(
        payload: RSocketPayloadInput<D, M>,
        options: RSocketRequestOptions
    ): RSocketPayloadInput<D, M> {
        return this.metadataStore.payload(payload, options.metadataMimeType);
    }

    /**
     * Merges current client metadata into each outgoing channel payload.
     */
    private withClientMetadataInput<D, M>(
        payloads: RSocketChannelInput<D, M>,
        options: RSocketRequestOptions
    ): RSocketChannelInput<D, M> {
        return this.metadataStore.channel(payloads, options.metadataMimeType);
    }

    /**
     * Merges current client defaults into one connection-level metadata push.
     */
    private withClientMetadataValue<M>(
        metadata: M | Metadata<M>,
        options: RSocketRequestOptions
    ): M | Metadata<any> {
        return this.metadataStore.metadata(metadata, options.metadataMimeType);
    }

    /**
     * Forces composite metadata encoding when client metadata entries are present.
     */
    private withClientMetadataOptions(options: RSocketRequestOptions): RSocketRequestOptions {
        return this.metadataStore.requestOptions(options);
    }

    /**
     * Returns the current connected client or throws a user-facing error.
     */
    private requireClient(): RSocketClient<D, M> {
        const client = this.currentClient();
        if (client !== undefined) return client;
        if (this.closeRequested) {
            throw new RSocketConnectionError(
                "RSocket is disconnected. Subscribe to socket.connect() before starting interactions.",
                this.lastError
            );
        }
        throw new RSocketConnectionError(
            "RSocket is not connected. Subscribe to socket.connect() before starting interactions.",
            this.lastError
        );
    }

    /** Validates immutable protocol and transport options once on first connect. */
    private validateConnectOptions(): void {
        if (this.optionsValidated) return;
        normalizeClientOptions(this.clientConfiguration);
        validateTransportOptions(this.transportOptions);
        this.optionsValidated = true;
    }

    /**
     * Returns this instance narrowed to the connected state surface.
     */
    private connectedFacade(): ConnectedRSocket<D, M> {
        return this.connectedSurface;
    }

    /**
     * Returns this instance narrowed to the disconnected state surface.
     */
    private disconnectedFacade(): DisconnectedRSocket<D, M> {
        return this as unknown as DisconnectedRSocket<D, M>;
    }
}

/** Returns whether constructor options contain a type-specific lifecycle handler. */
function hasSpecificEventHandler(handlers: RSocketConnectionEventHandlers): boolean {
    return handlers.connecting !== undefined ||
        handlers.connected !== undefined ||
        handlers.disconnect !== undefined ||
        handlers.reconnecting !== undefined ||
        handlers.resumeRejected !== undefined ||
        handlers.reconnectFailed !== undefined ||
        handlers.closed !== undefined;
}

/** Builds an internal payload envelope from the positional public arguments. */
function interactionPayload(data: unknown, metadata: unknown): RSocketPayloadInput<any, any> {
    if (data === undefined) {
        return metadata === undefined ? EMPTY_INTERACTION_PAYLOAD : {metadata};
    }
    return metadata === undefined ? {data} : {data, metadata};
}

/** Prepends positional request-channel metadata to the initial channel frame. */
function channelInputWithMetadata(
    input: RSocketChannelInput<any, any>,
    metadata: unknown
): RSocketChannelInput<any, any> {
    return metadata === undefined
        ? input
        : prependChannelPayload(interactionPayload(undefined, metadata), input);
}

/**
 * Normalizes optional request options while keeping the no-options path allocation-free.
 */
function normalizeFacadeRequestOptions(options: RSocketRequestOptions | null | undefined): RSocketRequestOptions {
    return options ?? EMPTY_REQUEST_OPTIONS;
}

/** Reads the browser facade's symbol-keyed reconnect signals without exposing an option. */
function internalReconnectSignals(options: RSocketOptions<any, any>): RSocketReconnectSignals {
    return (options as RSocketOptions<any, any> & {
        readonly [RECONNECT_SIGNALS]?: RSocketReconnectSignals;
    })[RECONNECT_SIGNALS] ?? immediateReconnectSignals;
}

