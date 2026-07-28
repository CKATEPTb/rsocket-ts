/**
 * Transport-neutral RSocket requester protocol session.
 *
 * This module owns SETUP, keepalive, leases, frame dispatch, stream
 * registration, request interactions, payload reassembly, and protocol-level
 * connection shutdown.
 */
import {Mono} from "reactor-core-ts";
import {
    connectionClosedError,
    deserializeFrame,
    emitSerializedOutboundFrames,
    encodeMetadataInput,
    encodePayloadInput,
    errorFromFrame,
    errorPayload,
    hasIgnorableInvalidMetadataLength,
    isConnectionFrame,
    isErrorCodeValidForStream,
    isHandshakeErrorCode,
    IgnoredPayloadFragments,
    isIgnorableEstablishedFrame,
    isIgnorableUnknownStreamFrame,
    isInitialRequestFrame,
    isResumePositionFrame,
    KEEPALIVE_DATA_MIME_TYPE,
    MAX_REQUEST_N,
    nextRSocketStreamId,
    observeAbort,
    readFrameStreamId,
    readFrameTypeAndFlags,
    readKeepalivePosition,
    reassemblePayloadFrame,
    requiresRawPayloadDecode,
    ReactiveTransportBinding,
    RSocketConnectionError,
    RSocketFrameSizeError,
    RSocketInactivityTimer,
    RSocketProtocolError,
    RSocketReplayBuffer,
    unrefTimer,
    type PayloadFragmentMap,
    type RSocketPayloadFrame,
    type RSocketPayloadInput,
    type RSocketTransportConnection
} from "rsocket-core-ts";
import {
    CancelFrame,
    ErrorFrame,
    ExtensionFrame,
    type Frame,
    FrameErrorCode,
    FrameFlag,
    FrameType,
    KeepaliveFlag,
    KeepaliveFrame,
    LeaseFrame,
    Metadata,
    MetadataPushFrame,
    type MimeType,
    Payload,
    PayloadFlag,
    PayloadFrame,
    RequestChannelFrame,
    RequestFireAndForgetFrame,
    RequestNFrame,
    RequestResponseFrame,
    RequestStreamFrame,
    ResumeFrame,
    type RSocketResumeToken,
    type ResumeOkFrame,
    SetupFlag,
    SetupFrame
} from "rsocket-frames-ts";
import {decodeResumeOkFrame, sendHandshakeFrame} from "@/client/handshake.js";
import {RSocketLeaseError} from "@/client/errors.js";
import {normalizeClientOptions, type NormalizedClientOptions} from "@/client/options.js";
import {MonoRequestController} from "@/client/request-response.js";
import {RequestChannelOutbound} from "@/channel/index.js";
import type {RSocketResumeState} from "@/resume/state.js";
import {
    RSocketFlux,
    RSocketStreamSubscription,
    type StartStream,
    type StreamController,
    type StreamSession
} from "@/stream/index.js";
import type {
    RSocketChannelInput,
    RSocketClientOptions,
    RSocketFrameActivityListener,
    RSocketRequestOptions,
    RSocketStreamRequestOptions,
    RSocketTransportOpenOptions
} from "@/client/types.js";

/**
 * Options required to start a protocol Resume handshake.
 */
export interface RSocketResumeHandshake<D = unknown, M = unknown> {
    /** Suspended logical session whose active streams must survive Resume. */
    readonly client: RSocketClient<D, M>;
}

/**
 * Listener invoked exactly once when the client session closes.
 */
type CloseListener = (error: unknown) => void;

/**
 * Alias for the optional frame activity observer.
 */
type ActivityListener = RSocketFrameActivityListener;

/**
 * Direction of a frame activity event.
 */
type ActivityDirection = Parameters<ActivityListener>[0]["direction"];

/** Pending RESUME_OK wait owned by the session's uninterrupted transport subscription. */
interface ResumeHandshakeWaiter {
    /** Completes the handshake with the validated responder frame. */
    readonly resolve: (frame: ResumeOkFrame) => void;
    /** Fails the current physical Resume attempt without terminating logical streams. */
    readonly reject: (error: unknown) => void;
    /** Optional handshake response timeout. */
    timeout?: ReturnType<typeof setTimeout>;
    /** Idempotent abort-listener cleanup. */
    abortCleanup?: () => void;
}

/** One cancellable request-channel waiter retained while Resume is pending. */
interface WritableWaiter {
    /** Continues channel draining after a transport is restored. */
    readonly resolve: () => void;
    /** Stops channel draining when the logical session terminates. */
    readonly reject: (error: unknown) => void;
    /** Idempotent channel-abort listener cleanup. */
    abortCleanup?: () => void;
}

/**
 * Shared empty options object used to avoid allocating per request.
 */
const EMPTY_REQUEST_OPTIONS: RSocketRequestOptions = {};
/** Shared transport options for connections without a timeout or abort signal. */
const EMPTY_TRANSPORT_OPEN_OPTIONS: RSocketTransportOpenOptions = Object.freeze({});

/** Shared empty byte buffer used on hot paths that need an empty payload. */
const EMPTY_BYTES = new Uint8Array(0);
/** Reusable empty KEEPALIVE payload sent by requester heartbeats. */
const EMPTY_KEEPALIVE_PAYLOAD = KEEPALIVE_DATA_MIME_TYPE.toPayload(EMPTY_BYTES);
/** Defensive cap for unacknowledged requester bytes retained by protocol Resume. */
const DEFAULT_RESUME_BUFFER_BYTES = 16 * 1024 * 1024;
/** MIME overrides keeping application content raw until stream-aware dispatch. */
const APPLICATION_MIME_OVERRIDES = Object.freeze({
    metadataMimeType: KEEPALIVE_DATA_MIME_TYPE,
    dataMimeType: KEEPALIVE_DATA_MIME_TYPE
});

/**
 * Low-level RSocket requester bound to one logical protocol session.
 *
 * Instances are created through `RSocketClient.connect(...)`. Transport
 * packages supply a factory for each physical SETUP or RESUME connection.
 */
export class RSocketClient<D = unknown, M = unknown> {
    private readonly streams = new Map<number, StreamController>();
    private readonly fragments: PayloadFragmentMap = new Map();
    private readonly ignoredPayloadFragments = new IgnoredPayloadFragments();
    private nextStreamId = 1;
    private nextServerStreamId = 2;
    private clientPosition = 0n;
    private serverPosition = 0n;
    /** Position/replay state allocated only for resumable logical sessions. */
    private readonly replayBuffer: RSocketReplayBuffer | undefined;
    private pendingFrames: Frame[] | undefined;
    private pendingIncomingFrames: Uint8Array[] | undefined;
    private resumeHandshake: ResumeHandshakeWaiter | undefined;
    private writableWaiters: Set<WritableWaiter> | undefined;
    private setupAccepted: boolean;
    private closed = false;
    private suspended = false;
    private terminated = false;
    private gracefulCloseError: unknown;
    private closeError: unknown;
    private keepAliveTimer: ReturnType<typeof setInterval> | undefined;
    private readonly lifetime = new RSocketInactivityTimer();
    private leaseRemaining = 0;
    private leaseExpiresAt = 0;
    private closeListeners: Set<CloseListener> | undefined;
    private activityListeners: Set<ActivityListener> | undefined;
    private transportBinding: ReactiveTransportBinding | undefined;
    private readonly sendOutboundFrame = (frame: Frame, bytes: Uint8Array): void => {
        this.sendSerializedFrame(frame, bytes);
    };
    /** Internal protocol surface supplied to stream controllers without widening this class's API. */
    private readonly streamSession: StreamSession = {
        sendFrame: (frame) => this.sendFrame(frame),
        sendRequestFrame: (frame) => this.sendRequestFrame(frame),
        unregisterStream: (streamId) => this.unregisterStream(streamId),
        protocolError: (error) => this.protocolError(error),
        waitUntilWritable: (abortSignal) => this.waitUntilWritable(abortSignal)
    };
    private connection: RSocketTransportConnection;

    /**
     * Creates a client around an already-created reactive transport.
     *
     * The constructor is private so callers cannot bypass the async open and
     * SETUP handshake performed by `connect`.
     */
    private constructor(
        connection: RSocketTransportConnection,
        private readonly options: NormalizedClientOptions<D, M>
    ) {
        this.connection = connection;
        this.replayBuffer = options.setup.resumeToken === undefined
            ? undefined
            : new RSocketReplayBuffer({
                maxBytes: DEFAULT_RESUME_BUFFER_BYTES,
                retainFrames: options.activityListener !== undefined
            });
        this.setupAccepted = false;
        this.attachConnection();
        if (options.activityListener !== undefined) this.onActivity(options.activityListener);
    }

    /**
     * Opens a physical transport, sends SETUP, and starts keepalive.
     */
    static async connect<D = unknown, M = unknown>(
        options: RSocketClientOptions<D, M>
    ): Promise<RSocketClient<D, M>> {
        const normalized = normalizeClientOptions(options);
        const connection = openTransport(options, normalized);
        const client = new RSocketClient<D, M>(connection, normalized);

        try {
            await connection.opened.block();
            client.sendSetupFrame();
            client.assertHandshakeConnectionOpen();
            client.startKeepAlive();
            return client;
        } catch (error) {
            client.terminateSession(error, true);
            throw error;
        }
    }

    /**
     * Opens a physical transport, sends RESUME, waits for RESUME_OK, and starts keepalive.
     */
    static async resume<D = unknown, M = unknown>(
        options: RSocketClientOptions<D, M>,
        resume: RSocketResumeHandshake<D, M>
    ): Promise<RSocketClient<D, M>> {
        const normalized = normalizeClientOptions(options);
        const token = normalized.setup.resumeToken;
        if (token === undefined) {
            throw new RSocketProtocolError("RSocket Resume requires a resume token");
        }

        const client = resume.client;
        if (!client.suspended || client.terminated) {
            throw new RSocketProtocolError("RSocket Resume requires a suspended logical session");
        }
        client.assertResumeConfiguration(normalized);
        const state = client.resumeState();

        const connection = openTransport(options, normalized);

        try {
            await connection.opened.block();
            const frame = new ResumeFrame(
                token,
                state.serverPosition,
                state.firstAvailableClientPosition,
                normalized.setup.majorVersion,
                normalized.setup.minorVersion
            );
            const responsePromise = client.beginResume(connection, options.abortSignal);
            sendHandshakeFrame(connection, normalized, frame);

            const response = await responsePromise;
            if (response.lastReceivedClientPosition > state.clientPosition) {
                throw new RSocketProtocolError("RSocket Resume responder acknowledged an impossible client position");
            }

            client.resumeWith(connection, response.lastReceivedClientPosition);
            return client;
        } catch (error) {
            client.failResumeAttempt(connection, error, false);
            try {
                connection.close({reason: "RSocket resume failed", error: true});
            } catch {
                // Preserve the original resume failure; cleanup close errors are secondary.
            }
            throw error;
        }
    }

    /**
     * Indicates whether this logical RSocket session is closed.
     */
    get isClosed(): boolean {
        return this.closed;
    }

    /** Whether this logical session is waiting for a protocol Resume attempt. */
    get isSuspended(): boolean {
        return this.suspended && !this.terminated;
    }

    /**
     * Default data MIME type negotiated through SETUP.
     */
    private get dataMimeType(): MimeType<D> {
        return this.options.setup.dataMimeType;
    }

    /**
     * Default metadata MIME type negotiated through SETUP.
     */
    private get metadataMimeType(): MimeType<M> {
        return this.options.setup.metadataMimeType;
    }

    /**
     * Returns the current protocol resume position snapshot.
     */
    private resumeState(): RSocketResumeState {
        return {
            clientPosition: this.clientPosition,
            firstAvailableClientPosition: this.replayBuffer?.firstAvailablePosition(this.clientPosition) ??
                this.clientPosition,
            serverPosition: this.serverPosition
        };
    }

    /**
     * Closes a stale session whose keepalive lifetime has expired.
     */
    checkLifetime(now = Date.now()): boolean {
        if (this.closed) return false;
        if (this.lifetime.isAlive(this.options.setup.lifetimeMs, now)) return true;
        this.loseTransport(new RSocketConnectionError("RSocket keepalive lifetime expired"), true);
        return false;
    }

    /**
     * Closes the RSocket session and its physical transport.
     */
    close(code = 1000, reason = "RSocket client closed"): void {
        if (this.terminated) return;
        const wasActive = !this.closed;
        try {
            if (wasActive && this.connection.isOpen) {
                this.sendFrame(new ErrorFrame(0, FrameErrorCode.CONNECTION_ERROR, errorPayload(reason)));
            }
        } finally {
            this.terminateSession(connectionClosedError(reason), false);
            if (wasActive) this.connection.close({code, reason});
        }
    }

    /**
     * Terminates a suspended logical session when Resume will not be retried.
     */
    abandonResume(error: unknown = this.closeError): void {
        this.terminateSession(error, this.connection.isOpen);
    }

    /**
     * Registers a listener that runs once when the session closes.
     *
     * If the session is already closed, the listener is called immediately with
     * the stored close reason.
     */
    onClose(listener: CloseListener): () => void {
        if (this.closed) {
            listener(this.closeError);
            return () => undefined;
        }

        const listeners = this.closeListeners ??= new Set<CloseListener>();
        listeners.add(listener);
        return () => {
            listeners.delete(listener);
            if (listeners.size === 0 && this.closeListeners === listeners) this.closeListeners = undefined;
        };
    }

    /**
     * Registers a diagnostic frame activity listener for sent and received frames.
     */
    private onActivity(listener: ActivityListener): () => void {
        const listeners = this.activityListeners ??= new Set<ActivityListener>();
        listeners.add(listener);
        return () => {
            listeners.delete(listener);
            if (listeners.size === 0 && this.activityListeners === listeners) this.activityListeners = undefined;
        };
    }

    /**
     * Starts a request-response interaction and emits exactly one decoded payload.
     *
     * The returned `Mono` is cold: the request frame is written only when the Mono
     * is subscribed or blocked.
     */
    requestResponse<OD = unknown, OM = unknown>(
        payload: RSocketPayloadInput<OD, OM>,
        options: RSocketRequestOptions = EMPTY_REQUEST_OPTIONS
    ): Mono<RSocketPayloadFrame<D, M>> {
        return Mono.create<RSocketPayloadFrame<D, M>>((sink) => {
            let streamId: number | undefined;
            let controller: MonoRequestController<D, M> | undefined;
            let timeout: ReturnType<typeof setTimeout> | undefined;
            try {
                this.assertCanStartRequest();
                const timeoutMs = requestTimeoutMs(options);
                const encoded = this.encode(payload, options);
                streamId = this.allocateStreamId();
                controller = new MonoRequestController<D, M>(
                    this.streamSession,
                    streamId,
                    timeoutMs === undefined
                        ? sink
                        : {
                            success: (value) => {
                                if (timeout !== undefined) clearTimeout(timeout);
                                timeout = undefined;
                                sink.success(value);
                            },
                            error: (error) => {
                                if (timeout !== undefined) clearTimeout(timeout);
                                timeout = undefined;
                                sink.error(error);
                            }
                        }
                );
                this.streams.set(streamId, controller);
                if (timeoutMs !== undefined) {
                    timeout = setTimeout(() => {
                        if (streamId === undefined || controller === undefined || this.streams.get(streamId) !== controller) return;
                        controller.fail(new RSocketConnectionError(`RSocket request-response timed out after ${timeoutMs}ms`));
                        try {
                            this.sendFrame(new CancelFrame(streamId));
                        } catch {
                            // The timeout has already failed the Mono; a closed socket cannot observe CANCEL.
                        }
                    }, timeoutMs);
                    unrefTimer(timeout);
                }
                this.sendRequestFrame(new RequestResponseFrame(streamId, FrameFlag.NONE, encoded.metadata, encoded.payload));
            } catch (error) {
                if (timeout !== undefined) clearTimeout(timeout);
                timeout = undefined;
                if (streamId !== undefined) {
                    this.streams.delete(streamId);
                    this.rollbackUnsentStreamId(streamId);
                }
                sink.error(error);
            }

            sink.onCancel(() => {
                if (timeout !== undefined) clearTimeout(timeout);
                timeout = undefined;
                if (streamId === undefined || !this.streams.delete(streamId)) return;
                try {
                    this.sendFrame(new CancelFrame(streamId));
                } catch {
                    // The stream is already locally cancelled; a closed socket cannot observe CANCEL.
                }
            });
        });
    }

    /**
     * Starts a fire-and-forget interaction and completes after the frame is sent.
     */
    fireAndForget<OD = unknown, OM = unknown>(
        payload: RSocketPayloadInput<OD, OM>,
        options: RSocketRequestOptions = EMPTY_REQUEST_OPTIONS
    ): Mono<void> {
        return Mono.create<void>((sink) => {
            let streamId: number | undefined;
            try {
                this.assertCanStartRequest();
                const encoded = this.encode(payload, options);
                streamId = this.allocateStreamId();
                this.sendRequestFrame(new RequestFireAndForgetFrame(streamId, FrameFlag.NONE, encoded.metadata, encoded.payload));
                sink.success();
            } catch (error) {
                if (streamId !== undefined) this.rollbackUnsentStreamId(streamId);
                sink.error(error);
            }
        });
    }

    /**
     * Starts a request-stream interaction backed by RSocket REQUEST_N demand.
     */
    requestStream<OD = unknown, OM = unknown>(
        payload: RSocketPayloadInput<OD, OM>,
        options: RSocketStreamRequestOptions = EMPTY_REQUEST_OPTIONS
    ): RSocketFlux<RSocketPayloadFrame<D, M>> {
        return this.createRequestFlux((initialRequestN, subscription) => {
            this.assertCanStartRequest();
            const encoded = this.encode(payload, options);
            this.startResponseStream(subscription, (streamId) => {
                this.sendRequestFrame(new RequestStreamFrame(
                    streamId,
                    FrameFlag.NONE,
                    initialRequestN,
                    encoded.metadata,
                    encoded.payload
                ));
            });
        });
    }

    /**
     * Starts a request-channel interaction using the supplied outbound publisher.
     */
    requestChannel<OD = unknown, OM = unknown>(
        payloads: RSocketChannelInput<OD, OM>,
        options: RSocketStreamRequestOptions = EMPTY_REQUEST_OPTIONS
    ): RSocketFlux<RSocketPayloadFrame<D, M>> {
        return this.createRequestFlux((initialRequestN, subscription) => {
            this.assertCanStartRequest();
            const outbound = new RequestChannelOutbound(
                this.streamSession,
                payloads,
                options.dataMimeType ?? this.dataMimeType,
                options.metadataMimeType ?? this.metadataMimeType,
                (flags, metadata, payload) => this.startResponseStream(subscription, (streamId) => {
                    this.sendRequestFrame(new RequestChannelFrame(
                        streamId,
                        flags,
                        initialRequestN,
                        metadata,
                        payload
                    ));
                }),
                () => subscription.markOutboundComplete(),
                (error, requestStarted) => subscription.failOutbound(error, requestStarted)
            );
            subscription.attachOutbound(outbound);
            outbound.start();
        });
    }

    /**
     * Sends connection-level metadata without opening a stream.
     */
    metadataPush<M = unknown>(metadata: M | Metadata<M>, options: RSocketRequestOptions = EMPTY_REQUEST_OPTIONS): Mono<void> {
        return Mono.create<void>((sink) => {
            try {
                const mimeType = options.metadataMimeType ?? this.metadataMimeType;
                const frameMetadata = encodeMetadataInput(metadata, mimeType);
                this.sendFrame(new MetadataPushFrame(frameMetadata));
                sink.success();
            } catch (error) {
                sink.error(error);
            }
        });
    }

    /**
     * Serializes and writes one frame to the active transport immediately.
     */
    private sendFrame(frame: Frame): void {
        if (this.suspended) {
            this.queueFrame(frame);
            return;
        }
        if (this.terminated) throw connectionClosedError(this.closeError);

        emitSerializedOutboundFrames(frame, this.options.maxFrameLength, this.sendOutboundFrame);
    }

    /**
     * Sends an initial interaction frame and consumes a lease only once the
     * request is ready for the wire.
     */
    private sendRequestFrame(frame: Frame): void {
        const consumesLease = this.options.setup.honorLease;
        if (consumesLease) {
            this.assertLeaseAvailable();
            this.leaseRemaining -= 1;
        }

        try {
            this.sendFrame(frame);
        } catch (error) {
            if (consumesLease) this.leaseRemaining += 1;
            throw error;
        }
    }

    /**
     * Serializes and writes one already-sized frame to the active transport.
     */
    private sendSerializedFrame(frame: Frame, bytes = frame.toUint8Array()): void {
        if (this.suspended) {
            this.queueFrame(frame);
            return;
        }
        if (this.closed) throw connectionClosedError(this.closeError);
        if (!this.connection.isOpen) {
            const error = new RSocketConnectionError("RSocket transport is not open");
            this.loseTransport(error, false);
            if (this.suspended) {
                this.queueFrame(frame);
                return;
            }
            throw error;
        }

        const maxFrameLength = this.options.maxFrameLength;
        if (bytes.length > maxFrameLength) {
            throw new RSocketFrameSizeError(bytes.length, maxFrameLength);
        }

        let retainedForResume = false;
        const replayBuffer = this.replayBuffer;
        if (replayBuffer !== undefined && isResumePositionFrame(frame.type)) {
            try {
                this.clientPosition = replayBuffer.record(
                    this.clientPosition,
                    this.activityListeners === undefined ? undefined : frame,
                    bytes
                );
                retainedForResume = true;
            } catch (error) {
                this.terminateSession(error, true);
                throw error;
            }
        }
        try {
            this.connection.write(bytes);
            if (this.activityListeners !== undefined) this.emitActivity("send", frame);
        } catch (error) {
            this.loseTransport(error, true);
            if (this.suspended && (retainedForResume || frame.type === FrameType.KEEPALIVE)) return;
            throw error;
        }
    }

    /**
     * Removes a stream and any payload fragments associated with it.
     */
    private unregisterStream(streamId: number): void {
        this.streams.delete(streamId);
        this.fragments.delete(streamId);
        this.ignoredPayloadFragments.delete(streamId);
        if (this.streams.size === 0 && this.gracefulCloseError !== undefined) {
            const error = this.gracefulCloseError;
            this.gracefulCloseError = undefined;
            this.terminateSession(error, true, false, "RSocket connection closed");
        }
    }

    /**
     * Sends an RSocket ERROR frame when possible and closes the session.
     */
    private protocolError(error: RSocketProtocolError): void {
        try {
            this.sendFrame(new ErrorFrame(0, error.code ?? FrameErrorCode.CONNECTION_ERROR, errorPayload(error)));
        } catch {
            // Closing is still required even when the error frame cannot be written.
        }
        this.terminateSession(error, true);
    }

    /**
     * Creates a cold `Flux` that allocates its stream id after initial demand.
     */
    private createRequestFlux(start: StartStream<D, M>): RSocketFlux<RSocketPayloadFrame<D, M>> {
        return new RSocketFlux<RSocketPayloadFrame<D, M>>((subscriber) => {
            return new RSocketStreamSubscription<D, M>(this.streamSession, subscriber, start);
        });
    }

    /** Sends one best-effort media payload when the selected transport supports it. */
    media(payload: Uint8Array): Mono<void> {
        return Mono.create<void>((sink) => {
            try {
                if (this.suspended || this.closed || this.terminated) {
                    throw connectionClosedError(this.closeError);
                }
                const write = this.connection.writeMedia;
                if (write === undefined) {
                    throw new RSocketConnectionError("Selected RSocket transport does not support media datagrams");
                }
                write.call(this.connection, payload);
                sink.success();
            } catch (error) {
                sink.error(error);
            }
        });
    }

    /** Assigns and registers an ID immediately before an initial streaming request is sent. */
    private startResponseStream(
        subscription: RSocketStreamSubscription<D, M>,
        send: (streamId: number) => void
    ): number {
        this.assertCanStartRequest();
        const streamId = this.allocateStreamId();
        subscription.assignStreamId(streamId);
        this.streams.set(streamId, subscription);
        try {
            subscription.markRequestStarted();
            send(streamId);
            subscription.flushInitialDemand();
            return streamId;
        } catch (error) {
            this.streams.delete(streamId);
            this.rollbackUnsentStreamId(streamId);
            subscription.markRequestUnsent();
            throw error;
        }
    }

    /**
     * Encodes data and metadata with per-request MIME overrides when supplied.
     */
    private encode(input: RSocketPayloadInput<any, any>, options: RSocketRequestOptions) {
        return encodePayloadInput(
            input,
            options.dataMimeType ?? this.dataMimeType,
            options.metadataMimeType ?? this.metadataMimeType
        );
    }

    /**
     * Allocates the next available odd requester stream id.
     */
    private allocateStreamId(): number {
        const occupied = this.streams.size;
        for (let attempts = 0; attempts <= occupied; attempts += 1) {
            const streamId = this.nextStreamId;
            this.nextStreamId = nextRSocketStreamId(streamId, 1);
            if (!this.streams.has(streamId)) return streamId;
        }
        throw new RSocketProtocolError("No free client stream IDs are available");
    }

    /**
     * Restores the allocator after a request failed before any bytes reached an
     * active transport. A changed allocator or lost transport makes delivery
     * ambiguous, so those cases deliberately retain the consumed id.
     */
    private rollbackUnsentStreamId(streamId: number): void {
        if (this.closed || this.suspended || this.terminated) return;
        if (this.nextStreamId === nextRSocketStreamId(streamId, 1)) this.nextStreamId = streamId;
    }

    /**
     * Verifies that the session is open and that requester lease permits demand.
     */
    private assertCanStartRequest(): void {
        if (this.closed) throw connectionClosedError(this.closeError);
        if (this.gracefulCloseError !== undefined) {
            throw new RSocketConnectionError("RSocket responder is closing the connection", this.gracefulCloseError);
        }
        if (!this.options.setup.honorLease) return;
        this.assertLeaseAvailable();
    }

    /** Verifies that the current requester lease grants another interaction. */
    private assertLeaseAvailable(): void {
        if (this.leaseRemaining <= 0 || Date.now() >= this.leaseExpiresAt) {
            throw new RSocketLeaseError("No active RSocket lease is available for a new request");
        }
    }

    /**
     * Writes the initial SETUP frame using normalized protocol options.
     */
    private sendSetupFrame(): void {
        const setup = this.options.setup;
        const encoded = encodePayloadInput(setup.setupPayload, setup.dataMimeType, setup.metadataMimeType);
        const flags = setup.honorLease ? SetupFlag.LEASE : FrameFlag.NONE;

        this.sendFrame(
            new SetupFrame(
                setup.keepAliveMs,
                setup.lifetimeMs,
                setup.metadataMimeType,
                setup.dataMimeType,
                setup.resumeToken,
                setup.majorVersion,
                setup.minorVersion,
                flags,
                encoded.metadata,
                encoded.payload
            )
        );
    }

    /**
     * Starts requester keepalive and lifetime monitoring.
     */
    private startKeepAlive(): void {
        if (this.closed) return;
        const keepAliveMs = this.options.setup.keepAliveMs;
        if (keepAliveMs <= 0) return;
        this.lifetime.touch();
        this.lifetime.start(this.options.setup.lifetimeMs, () => this.checkLifetime());

        this.keepAliveTimer = setInterval(() => {
            if (this.closed) return;
            try {
                this.sendFrame(new KeepaliveFrame(KeepaliveFlag.RESPOND, this.serverPosition, EMPTY_KEEPALIVE_PAYLOAD));
            } catch (error) {
                this.loseTransport(error, true);
            }
        }, keepAliveMs);
        unrefTimer(this.keepAliveTimer);
    }

    /**
     * Attaches a replacement transport, replays unacknowledged frames, and
     * flushes frames produced by active streams while the transport was down.
     */
    private resumeWith(connection: RSocketTransportConnection, peerPosition: bigint): void {
        if (!this.suspended || this.terminated) {
            throw new RSocketProtocolError("RSocket logical session is no longer resumable");
        }

        if (this.connection !== connection) {
            throw new RSocketProtocolError("RSocket Resume completed on a stale transport");
        }
        this.closed = false;
        this.suspended = true;
        this.closeError = undefined;
        this.setupAccepted = true;
        this.leaseRemaining = 0;
        this.leaseExpiresAt = 0;
        try {
            const replayBuffer = this.replayBuffer;
            if (replayBuffer === undefined) throw new RSocketProtocolError("RSocket session has no replay buffer");
            replayBuffer.replayFrom(
                peerPosition,
                this.clientPosition,
                (frame, bytes) => this.sendReplayFrame(frame, bytes)
            );
            this.assertHandshakeConnectionOpen();
            this.suspended = false;
            this.flushPendingFrames();
            this.assertHandshakeConnectionOpen();
            this.flushPendingIncomingFrames();
            this.assertHandshakeConnectionOpen();
            this.startKeepAlive();
            this.resolveWritableWaiters();
        } catch (error) {
            this.loseTransport(error, true);
            throw error;
        }
    }

    /** Writes retained bytes without assigning a second implied position. */
    private sendReplayFrame(frame: Frame | undefined, bytes: Uint8Array): void {
        if (!this.connection.isOpen) {
            throw new RSocketConnectionError("Transport closed during RSocket frame replay");
        }
        this.connection.write(bytes);
        if (frame !== undefined && this.activityListeners !== undefined) this.emitActivity("send", frame);
    }

    /** Flushes stream frames queued while the logical session was suspended. */
    private flushPendingFrames(): void {
        const frames = this.pendingFrames;
        if (frames === undefined || frames.length === 0) {
            this.pendingFrames = undefined;
            return;
        }

        this.pendingFrames = undefined;
        for (let index = 0; index < frames.length; index += 1) {
            this.sendFrame(frames[index] as Frame);
            if (!this.suspended) continue;

            const pending = this.pendingFrames ??= [];
            for (let remaining = index + 1; remaining < frames.length; remaining += 1) {
                pending.push(frames[remaining] as Frame);
            }
            throw connectionClosedError(this.closeError);
        }
    }

    /**
     * Queues resumable work while dropping transport-scoped heartbeats.
     *
     * Demand is additive, so adjacent transport outages must not retain one
     * `REQUEST_N` allocation per downstream request. A local cancellation
     * supersedes every frame queued for the same stream.
     */
    private queueFrame(frame: Frame): void {
        if (frame.type === FrameType.KEEPALIVE) return;
        const frames = this.pendingFrames ??= [];
        const streamId = frame.header.streamId;

        if (frame.type === FrameType.CANCEL) {
            let retained = 0;
            let removedUnsentRequest = false;
            for (let index = 0; index < frames.length; index += 1) {
                const pending = frames[index] as Frame;
                if (pending.header.streamId !== streamId) {
                    frames[retained] = pending;
                    retained += 1;
                } else if (isInitialRequestFrame(pending.type)) {
                    removedUnsentRequest = true;
                }
            }
            frames.length = retained;
            if (removedUnsentRequest) {
                if (retained === 0) this.pendingFrames = undefined;
                return;
            }
            frames.push(frame);
            return;
        }

        if (frame.type === FrameType.REQUEST_N) {
            for (let index = frames.length - 1; index >= 0; index -= 1) {
                const pending = frames[index] as Frame;
                if (pending.header.streamId !== streamId) continue;
                if (pending.type === FrameType.CANCEL) return;
                if (pending.type !== FrameType.REQUEST_N) break;

                const request = (pending as RequestNFrame).request + (frame as RequestNFrame).request;
                frames[index] = new RequestNFrame(streamId, Math.min(MAX_REQUEST_N, request));
                if (request > MAX_REQUEST_N) {
                    frames.push(new RequestNFrame(streamId, request - MAX_REQUEST_N));
                }
                return;
            }
        }

        frames.push(frame);
    }

    /** Dispatches frames that arrived immediately after RESUME_OK without losing order. */
    private flushPendingIncomingFrames(): void {
        const frames = this.pendingIncomingFrames;
        if (frames === undefined || frames.length === 0) {
            this.pendingIncomingFrames = undefined;
            return;
        }

        this.pendingIncomingFrames = undefined;
        for (let index = 0; index < frames.length; index += 1) {
            this.handleIncomingBytes(frames[index] as Uint8Array);
            if (!this.closed) continue;
            if (!this.suspended || this.terminated) return;

            const pending = this.pendingIncomingFrames ??= [];
            for (let remaining = index + 1; remaining < frames.length; remaining += 1) {
                pending.push(frames[remaining] as Uint8Array);
            }
            return;
        }
    }

    /**
     * Subscribes the RSocket session to transport frames, errors, and closes.
     */
    private attachConnection(): void {
        const connection = this.connection;
        const binding = new ReactiveTransportBinding(connection, {
            frame: (bytes) => {
                if (this.connection === connection) this.handleIncomingBytes(bytes);
            },
            frameError: (error) => {
                if (this.connection !== connection) return;
                if (error instanceof RSocketConnectionError) {
                    if (this.resumeHandshake !== undefined) this.failResumeAttempt(connection, error, true);
                    else this.loseTransport(error, true);
                    return;
                }
                this.protocolError(error instanceof RSocketProtocolError
                    ? error
                    : new RSocketProtocolError("Failed to decode incoming RSocket frame", {cause: error}));
            },
            error: (event) => {
                if (this.connection !== connection) return;
                const error = new RSocketConnectionError("RSocket transport error", event);
                if (this.resumeHandshake !== undefined) this.failResumeAttempt(connection, error, true);
                else this.loseTransport(error, true);
            },
            media: (payload) => {
                if (this.connection !== connection) return;
                try {
                    this.options.mediaListener?.(payload);
                } catch {
                    // Best-effort application observers cannot terminate the RSocket session.
                }
            },
            skippedFireAndForget: () => undefined,
            close: (error) => {
                if (this.connection !== connection) return;
                if (this.resumeHandshake !== undefined) this.failResumeAttempt(connection, error, false);
                else this.loseTransport(error, false);
            }
        });
        this.transportBinding = binding;
        binding.start();
    }

    /**
     * Decodes one complete inbound raw RSocket frame.
     */
    private handleIncomingBytes(bytes: Uint8Array): void {
        if (this.resumeHandshake !== undefined) {
            try {
                this.resolveResumeHandshake(decodeResumeOkFrame(bytes, this.options));
            } catch (error) {
                this.failResumeAttempt(this.connection, error, true);
            }
            return;
        }
        if (this.suspended) {
            (this.pendingIncomingFrames ??= []).push(bytes);
            return;
        }

        const maxFrameLength = this.options.maxFrameLength;
        if (bytes.length > maxFrameLength) {
            this.protocolError(new RSocketFrameSizeError(bytes.length, maxFrameLength));
            return;
        }

        try {
            const streamId = readFrameStreamId(bytes);
            const typeAndFlags = readFrameTypeAndFlags(bytes);
            const frameType = (typeAndFlags >>> 10) as FrameType;
            const replayBuffer = this.replayBuffer;
            const stream = this.streams.get(streamId);
            const activeStream = stream !== undefined || this.ignoredPayloadFragments.has(streamId);
            if (isIgnorableEstablishedFrame(frameType, streamId, activeStream)) {
                if (replayBuffer !== undefined && isResumePositionFrame(frameType)) {
                    this.serverPosition += BigInt(bytes.byteLength);
                }
                return;
            }
            if (!activeStream && isIgnorableUnknownStreamFrame(frameType, streamId)) {
                if (replayBuffer !== undefined) this.serverPosition += BigInt(bytes.byteLength);
                return;
            }
            if (frameType === FrameType.PAYLOAD && this.ignoredPayloadFragments.consume(streamId, typeAndFlags)) {
                if (replayBuffer !== undefined) this.serverPosition += BigInt(bytes.byteLength);
                return;
            }
            if (hasIgnorableInvalidMetadataLength(bytes, typeAndFlags)) {
                if (replayBuffer !== undefined && isResumePositionFrame(frameType)) {
                    this.serverPosition += BigInt(bytes.byteLength);
                }
                if (isInitialRequestFrame(frameType)) {
                    this.setupAccepted = true;
                    if (this.acceptResponderStreamId(streamId)) {
                        this.ignoredPayloadFragments.update(streamId, typeAndFlags);
                    }
                } else if (frameType === FrameType.PAYLOAD) {
                    this.fragments.delete(streamId);
                    this.ignoredPayloadFragments.update(streamId, typeAndFlags);
                }
                return;
            }
            const decodePayloadAsRaw = requiresRawPayloadDecode(frameType);
            const frame = deserializeFrame(
                bytes,
                this.metadataMimeType,
                this.dataMimeType,
                decodePayloadAsRaw ? APPLICATION_MIME_OVERRIDES : undefined,
                frameType
            );
            if (frameType === FrameType.KEEPALIVE) {
                if (replayBuffer !== undefined) {
                    replayBuffer.acknowledge(readKeepalivePosition(bytes), this.clientPosition);
                }
                this.lifetime.touch();
            }
            if (replayBuffer !== undefined && isResumePositionFrame(frameType)) {
                this.serverPosition += BigInt(bytes.byteLength);
            }
            if (this.activityListeners !== undefined) this.emitActivity("receive", frame);
            if (this.closed || this.suspended || this.terminated) return;
            const dispatchStream = stream !== undefined && (
                this.activityListeners === undefined || this.streams.get(streamId) === stream
            ) ? stream : undefined;
            this.handleFrame(frame, dispatchStream, streamId);
        } catch (error) {
            this.protocolError(new RSocketProtocolError("Failed to decode incoming RSocket frame", {cause: error}));
        }
    }

    /**
     * Dispatches one decoded frame to connection-level or stream-level handlers.
     */
    private handleFrame(frame: Frame, stream: StreamController | undefined, streamId: number): void {
        const frameType = frame.type;
        if (!this.validateFrameStreamId(frame, streamId)) return;
        if (!this.setupAccepted && confirmsSetup(frame, stream !== undefined)) {
            this.setupAccepted = true;
        }
        switch (frameType) {
            case FrameType.KEEPALIVE:
                this.handleKeepAlive(frame as KeepaliveFrame);
                return;
            case FrameType.LEASE:
                this.handleLease(frame as LeaseFrame);
                return;
            case FrameType.PAYLOAD:
                this.handlePayloadFrame(frame as PayloadFrame, stream);
                return;
            case FrameType.ERROR:
                this.handleErrorFrame(frame as ErrorFrame, stream);
                return;
            case FrameType.REQUEST_N:
                stream?.handleRequestN(frame as RequestNFrame);
                return;
            case FrameType.CANCEL: {
                // A sender may cancel an unfinished fragmented PAYLOAD sequence
                // even when CANCEL is otherwise unexpected for this direction.
                this.fragments.delete(streamId);
                stream?.handleCancel();
                return;
            }
            case FrameType.METADATA_PUSH:
            case FrameType.SETUP:
            case FrameType.RESUME:
            case FrameType.RESUME_OK:
                return;
            case FrameType.EXT:
                this.handleExtension(frame as ExtensionFrame);
                return;
            default:
                if (isInitialRequestFrame(frameType)) {
                    this.rejectResponderFrame(frame);
                    return;
                }
                if (!frame.canBeIgnored()) {
                    this.protocolError(new RSocketProtocolError("Unsupported required RSocket frame type", {
                        code: FrameErrorCode.CONNECTION_ERROR,
                        streamId
                    }));
                }
        }
    }

    /**
     * Enforces stream zero for connection frames that cannot be ignored leniently.
     */
    private validateFrameStreamId(frame: Frame, streamId: number): boolean {
        const frameType = frame.type;
        if (isIgnorableEstablishedFrame(frameType, streamId)) return false;
        const invalid = isConnectionFrame(frameType) && streamId !== 0;
        if (!invalid) return true;

        this.protocolError(
            new RSocketProtocolError("Responder sent an RSocket frame with an invalid stream ID", {
                code: FrameErrorCode.CONNECTION_ERROR,
                streamId
            })
        );
        return false;
    }

    /**
     * Responds to KEEPALIVE frames that require a responder echo.
     */
    private handleKeepAlive(frame: KeepaliveFrame): void {
        if (!frame.isRequireRespond()) return;
        this.sendFrame(new KeepaliveFrame(KeepaliveFlag.NONE, this.serverPosition, frame.payload as Payload<any> | undefined));
    }

    /**
     * Stores responder lease allowance for future requester interactions.
     */
    private handleLease(frame: LeaseFrame): void {
        this.leaseRemaining = frame.requestLimit;
        this.leaseExpiresAt = Date.now() + frame.ttl;
    }

    /**
     * Reassembles fragmented payloads and forwards complete PAYLOAD frames.
     */
    private handlePayloadFrame(frame: PayloadFrame, stream: StreamController | undefined): void {
        const streamId = frame.header.streamId;
        if (stream === undefined) {
            this.fragments.delete(streamId);
            return;
        }

        try {
            if (stream instanceof MonoRequestController &&
                !frame.hasFollows() &&
                !frame.isNext() &&
                !frame.isComplete()) {
                frame = new PayloadFrame(
                    streamId,
                    frame.header.flags | PayloadFlag.COMPLETE,
                    frame.metadata,
                    frame.payload
                );
            }
            const continuation = this.fragments.has(streamId);
            if (!stream.acceptPayloadFragment(frame, continuation)) {
                this.fragments.delete(streamId);
                return;
            }
            const payload = reassemblePayloadFrame(frame, this.fragments, this.metadataMimeType, this.dataMimeType);
            if (payload === undefined) return;
            stream.handlePayload(payload);
        } catch (error) {
            stream.fail(error);
            try {
                this.sendFrame(new CancelFrame(streamId));
            } catch {
                // The affected stream is already failed locally; transport cleanup owns write failures.
            }
        }
    }

    /**
     * Applies connection-level or stream-level ERROR frames.
     */
    private handleErrorFrame(frame: ErrorFrame, stream: StreamController | undefined): void {
        const streamId = frame.header.streamId;
        if (streamId !== 0 && stream === undefined) return;
        if (!isErrorCodeValidForStream(frame.code, streamId)) {
            this.protocolError(invalidErrorStreamId(frame));
            return;
        }
        if (streamId === 0) {
            if (this.setupAccepted && isHandshakeErrorCode(frame.code)) return;
            const error = errorFromFrame(frame);
            if (frame.code === FrameErrorCode.CONNECTION_CLOSE) {
                this.beginGracefulClose(error);
                return;
            }
            this.terminateSession(error, true);
            return;
        }

        stream?.handleError(frame);
    }

    /** Stops new requests and waits for active streams after CONNECTION_CLOSE. */
    private beginGracefulClose(error: unknown): void {
        if (this.closed || this.gracefulCloseError !== undefined) return;
        this.gracefulCloseError = error;
        if (this.streams.size !== 0) return;
        this.gracefulCloseError = undefined;
        this.terminateSession(error, true, false, "RSocket connection closed");
    }

    /**
     * Rejects required extension frames because this client has no extension
     * responder implementation.
     */
    private handleExtension(frame: ExtensionFrame): void {
        if (frame.canBeIgnored()) return;
        const streamId = frame.header.streamId;
        this.protocolError(
            new RSocketProtocolError("Unsupported required RSocket extension frame", {
                code: FrameErrorCode.CONNECTION_ERROR,
                streamId
            })
        );
    }

    /**
     * Rejects frames that only make sense for an RSocket responder.
     */
    private rejectResponderFrame(frame: Frame): void {
        const streamId = frame.header.streamId;
        if (!this.acceptResponderStreamId(streamId)) return;
        if (frame.type === FrameType.REQUEST_FNF) return;
        if (frame.canBeIgnored()) return;
        this.sendFrame(new ErrorFrame(streamId, FrameErrorCode.REJECTED, errorPayload("Responder is not configured")));
    }

    /** Validates and consumes the next server-initiated stream identifier. */
    private acceptResponderStreamId(streamId: number): boolean {
        while (this.ignoredPayloadFragments.has(this.nextServerStreamId)) {
            this.nextServerStreamId = nextRSocketStreamId(this.nextServerStreamId, 2);
        }
        if (this.streams.has(streamId)) return false;
        if (streamId <= 0 || streamId % 2 !== 0) {
            this.protocolError(new RSocketProtocolError(
                "Server request must use a positive even stream ID",
                {code: FrameErrorCode.CONNECTION_ERROR, streamId}
            ));
            return false;
        }
        if (streamId !== this.nextServerStreamId) {
            this.protocolError(new RSocketProtocolError(
                `Server request used stream ID ${streamId}; expected ${this.nextServerStreamId}`,
                {code: FrameErrorCode.CONNECTION_ERROR, streamId}
            ));
            return false;
        }
        this.nextServerStreamId = nextRSocketStreamId(streamId, 2);
        return true;
    }

    /**
     * Rejects a handshake that lost its transport before the client became usable.
     */
    private assertHandshakeConnectionOpen(): void {
        if (!this.closed && this.connection.isOpen) return;
        if (this.closeError instanceof Error) throw this.closeError;
        throw connectionClosedError(this.closeError ?? "Transport closed during RSocket handshake");
    }

    /**
     * Suspends a resumable logical session after transport loss, or terminates
     * a non-resumable session immediately.
     */
    private loseTransport(error: unknown, closeSocket: boolean): void {
        if (this.closed) return;
        if (this.replayBuffer === undefined || this.gracefulCloseError !== undefined) {
            this.terminateSession(error, closeSocket);
            return;
        }

        this.closed = true;
        this.suspended = true;
        this.closeError = error;
        this.leaseRemaining = 0;
        this.leaseExpiresAt = 0;
        this.stopTransport();
        this.pendingIncomingFrames = undefined;
        this.emitClose(error);
        if (closeSocket) this.closeTransportQuietly();
    }

    /**
     * Terminates the logical session, fails active streams, and releases every
     * retained replay/transport resource.
     */
    private terminateSession(
        error: unknown,
        closeSocket: boolean,
        protocolError = true,
        closeReason = "RSocket protocol error"
    ): void {
        if (this.terminated) return;
        this.rejectResumeHandshake(error);
        const shouldEmitClose = !this.closed;
        this.closed = true;
        this.suspended = false;
        this.terminated = true;
        this.gracefulCloseError = undefined;
        this.closeError = error;
        this.stopTransport();

        for (const controller of this.streams.values()) {
            try {
                controller.fail(error);
            } catch {
                // User subscribers must not interrupt session cleanup.
            }
        }
        this.streams.clear();
        this.fragments.clear();
        this.ignoredPayloadFragments.clear();
        this.pendingFrames = undefined;
        this.pendingIncomingFrames = undefined;
        this.rejectWritableWaiters(error);
        this.replayBuffer?.clear();
        this.activityListeners?.clear();
        this.activityListeners = undefined;
        if (shouldEmitClose) this.emitClose(error);
        if (closeSocket) this.closeTransportQuietly(protocolError, closeReason);
    }

    /** Stops heartbeat timers and removes listeners from the current transport. */
    private stopTransport(): void {
        if (this.keepAliveTimer !== undefined) clearInterval(this.keepAliveTimer);
        this.keepAliveTimer = undefined;
        this.lifetime.stop();
        const binding = this.transportBinding;
        this.transportBinding = undefined;
        try {
            binding?.dispose();
        } catch {
            // Transport listener cleanup must not suppress the session close signal.
        }
    }

    /** Closes a failed physical transport without replacing the primary error. */
    private closeTransportQuietly(
        error = true,
        reason = "RSocket protocol error"
    ): void {
        try {
            this.connection.close({reason, error});
        } catch {
            // The logical session state already reflects the original failure.
        }
    }

    /** Rejects configuration changes that the Resume protocol cannot renegotiate. */
    private assertResumeConfiguration(options: NormalizedClientOptions<D, M>): void {
        const current = this.options;
        const setup = options.setup;
        const currentSetup = current.setup;
        if (
            options.maxFrameLength === current.maxFrameLength &&
            sameResumeToken(setup.resumeToken, currentSetup.resumeToken) &&
            setup.majorVersion === currentSetup.majorVersion &&
            setup.minorVersion === currentSetup.minorVersion &&
            setup.keepAliveMs === currentSetup.keepAliveMs &&
            setup.lifetimeMs === currentSetup.lifetimeMs &&
            setup.honorLease === currentSetup.honorLease &&
            setup.dataMimeType.mimeType === currentSetup.dataMimeType.mimeType &&
            setup.metadataMimeType.mimeType === currentSetup.metadataMimeType.mimeType
        ) {
            return;
        }
        throw new RSocketProtocolError("RSocket Resume cannot change the established session configuration");
    }

    /** Attaches the replacement transport before RESUME is written, avoiding a receive gap. */
    private beginResume(
        connection: RSocketTransportConnection,
        abortSignal: AbortSignal | undefined
    ): Promise<ResumeOkFrame> {
        if (this.resumeHandshake !== undefined) {
            throw new RSocketProtocolError("An RSocket Resume attempt is already in progress");
        }

        const abortError = new RSocketConnectionError("RSocket resume aborted");
        if (abortSignal?.aborted) throw abortError;

        this.connection = connection;
        this.closed = false;
        this.suspended = true;
        this.pendingIncomingFrames = undefined;

        let registrationFailed = false;
        let registrationError: unknown;
        let abortedDuringRegistration = false;
        const promise = new Promise<ResumeOkFrame>((resolve, reject) => {
            const waiter: ResumeHandshakeWaiter = {resolve, reject};
            this.resumeHandshake = waiter;
            try {
                const release = observeAbort(abortSignal, () => {
                    abortedDuringRegistration = true;
                    this.failResumeAttempt(connection, abortError, true);
                });
                if (this.resumeHandshake === waiter) waiter.abortCleanup = release;
                else release();
            } catch (error) {
                registrationFailed = true;
                registrationError = error;
                if (this.resumeHandshake === waiter) this.resumeHandshake = undefined;
                reject(error);
                return;
            }

            const timeoutMs = this.options.connectTimeoutMs;
            if (this.resumeHandshake === waiter && timeoutMs !== undefined && timeoutMs > 0) {
                waiter.timeout = setTimeout(() => this.failResumeAttempt(
                    connection,
                    new RSocketConnectionError(`RSocket resume timed out after ${timeoutMs}ms`),
                    true
                ), timeoutMs);
                unrefTimer(waiter.timeout);
            }
        });
        // `sendHandshakeFrame` may fail before the caller reaches `await promise`.
        void promise.catch(() => undefined);
        if (registrationFailed) throw registrationError;
        if (abortedDuringRegistration) throw abortError;

        this.attachConnection();
        if (!connection.isOpen) {
            this.failResumeAttempt(
                connection,
                connectionClosedError("Transport closed during RSocket resume"),
                false
            );
        }
        return promise;
    }

    /** Resolves the active Resume handshake and leaves its frame subscription attached. */
    private resolveResumeHandshake(frame: ResumeOkFrame): void {
        const waiter = this.takeResumeHandshake();
        waiter?.resolve(frame);
    }

    /** Rejects the active Resume handshake, if any. */
    private rejectResumeHandshake(error: unknown): void {
        const waiter = this.takeResumeHandshake();
        waiter?.reject(error);
    }

    /** Clears timeout and abort resources associated with the active Resume handshake. */
    private takeResumeHandshake(): ResumeHandshakeWaiter | undefined {
        const waiter = this.resumeHandshake;
        if (waiter === undefined) return undefined;
        this.resumeHandshake = undefined;
        if (waiter.timeout !== undefined) clearTimeout(waiter.timeout);
        waiter.abortCleanup?.();
        delete waiter.abortCleanup;
        return waiter;
    }

    /** Fails one physical Resume attempt while retaining the logical session for policy handling. */
    private failResumeAttempt(
        connection: RSocketTransportConnection,
        error: unknown,
        closeSocket: boolean
    ): void {
        if (this.connection !== connection || this.terminated) return;
        this.rejectResumeHandshake(error);
        this.closed = true;
        this.suspended = true;
        this.closeError = error;
        this.pendingIncomingFrames = undefined;
        this.stopTransport();
        if (closeSocket) this.closeTransportQuietly();
    }

    /** Waits without polling while a request-channel publisher is suspended. */
    private waitUntilWritable(abortSignal: AbortSignal): Promise<void> | undefined {
        if (this.terminated) return Promise.reject(connectionClosedError(this.closeError));
        if (!this.suspended && !this.closed) return undefined;
        if (abortSignal.aborted) return Promise.reject(connectionClosedError("RSocket channel cancelled"));

        return new Promise<void>((resolve, reject) => {
            let waiter: WritableWaiter;
            const abort = (): void => {
                const waiters = this.writableWaiters;
                if (waiters?.delete(waiter) !== true) return;
                if (waiters.size === 0) this.writableWaiters = undefined;
                waiter.abortCleanup?.();
                delete waiter.abortCleanup;
                reject(connectionClosedError("RSocket channel cancelled"));
            };
            waiter = {resolve, reject};
            (this.writableWaiters ??= new Set()).add(waiter);
            try {
                const release = observeAbort(abortSignal, abort);
                if (this.writableWaiters?.has(waiter) === true) waiter.abortCleanup = release;
                else release();
            } catch (error) {
                const waiters = this.writableWaiters;
                waiters?.delete(waiter);
                if (waiters?.size === 0) this.writableWaiters = undefined;
                reject(error);
                return;
            }

            if (!this.suspended && !this.closed) this.settleWritableWaiter(waiter);
            else if (this.terminated) this.settleWritableWaiter(waiter, connectionClosedError(this.closeError));
        });
    }

    /** Continues every request-channel publisher after Resume completes. */
    private resolveWritableWaiters(): void {
        const waiters = this.writableWaiters;
        if (waiters === undefined) return;
        this.writableWaiters = undefined;
        for (const waiter of waiters) this.settleWritableWaiter(waiter);
    }

    /** Fails and releases every suspended request-channel publisher. */
    private rejectWritableWaiters(error: unknown): void {
        const waiters = this.writableWaiters;
        if (waiters === undefined) return;
        this.writableWaiters = undefined;
        for (const waiter of waiters) this.settleWritableWaiter(waiter, error);
    }

    /** Settles one writable waiter and removes its cancellation listener. */
    private settleWritableWaiter(waiter: WritableWaiter, error?: unknown): void {
        const waiters = this.writableWaiters;
        if (waiters?.delete(waiter) === true && waiters.size === 0) this.writableWaiters = undefined;
        waiter.abortCleanup?.();
        delete waiter.abortCleanup;
        if (error === undefined) waiter.resolve();
        else waiter.reject(error);
    }

    /**
     * Notifies close listeners while isolating listener exceptions.
     */
    private emitClose(error: unknown): void {
        const listeners = this.closeListeners;
        if (listeners === undefined) return;
        this.closeListeners = undefined;
        for (const listener of [...listeners]) {
            try {
                listener(error);
            } catch {
                // Consumer close handlers must not break connection cleanup.
            }
        }
        listeners.clear();
    }

    /**
     * Sends diagnostic frame activity to registered listeners.
     */
    private emitActivity(direction: ActivityDirection, frame: Frame): void {
        const listeners = this.activityListeners;
        if (listeners === undefined || listeners.size === 0 || this.options.activityEnabled?.() === false) return;
        const activity = {direction, frame};
        for (const listener of listeners) {
            try {
                listener(activity);
            } catch {
                // Activity listeners are diagnostic-only.
            }
        }
    }
}

/** Opens one physical transport and classifies synchronous factory failures. */
function openTransport<D, M>(
    options: RSocketClientOptions<D, M>,
    normalized: NormalizedClientOptions<D, M>
): RSocketTransportConnection {
    const timeoutMs = normalized.connectTimeoutMs;
    const abortSignal = options.abortSignal;
    const openOptions: RSocketTransportOpenOptions = timeoutMs === undefined && abortSignal === undefined
        ? EMPTY_TRANSPORT_OPEN_OPTIONS
        : {
            ...(timeoutMs === undefined ? {} : {timeoutMs}),
            ...(abortSignal === undefined ? {} : {abortSignal})
        };
    try {
        return options.transport(openOptions);
    } catch (error) {
        throw error instanceof RSocketConnectionError
            ? error
            : new RSocketConnectionError("RSocket transport initialization failed", error);
    }
}

/** Creates a connection-level protocol error for an ERROR stream mismatch. */
function invalidErrorStreamId(frame: ErrorFrame): RSocketProtocolError {
    return new RSocketProtocolError("Responder sent ERROR with an invalid stream ID for its code", {
        code: FrameErrorCode.CONNECTION_ERROR,
        streamId: frame.header.streamId,
        frame
    });
}

/**
 * Detects responder traffic that confirms the initial SETUP was accepted.
 */
function confirmsSetup(frame: Frame, activeStream: boolean): boolean {
    const type = frame.type;
    if (type === FrameType.LEASE) return true;
    if (type === FrameType.REQUEST_RESPONSE ||
        type === FrameType.REQUEST_FNF ||
        type === FrameType.REQUEST_STREAM ||
        type === FrameType.REQUEST_CHANNEL) {
        return true;
    }
    return activeStream &&
        (type === FrameType.PAYLOAD || type === FrameType.ERROR || type === FrameType.REQUEST_N);
}

/**
 * Reads a positive request timeout from per-request options.
 */
function requestTimeoutMs(options: RSocketRequestOptions): number | undefined {
    const timeout = options.timeout;
    if (timeout === undefined) return undefined;
    if (Number.isInteger(timeout) && timeout > 0 && timeout <= MAX_REQUEST_N) return timeout;
    throw new RSocketConnectionError(
        `RSocket request-response timeout must be an integer between 1 and ${MAX_REQUEST_N} milliseconds`
    );
}

/** Compares opaque Resume token content across independently created option snapshots. */
function sameResumeToken(
    left: RSocketResumeToken | undefined,
    right: RSocketResumeToken | undefined
): boolean {
    if (left === right) return true;
    if (left === undefined || right === undefined) return false;
    const leftBytes = typeof left === "string" ? RESUME_TOKEN_ENCODER.encode(left) : left;
    const rightBytes = typeof right === "string" ? RESUME_TOKEN_ENCODER.encode(right) : right;
    if (leftBytes.byteLength !== rightBytes.byteLength) return false;
    for (let index = 0; index < leftBytes.byteLength; index += 1) {
        if (leftBytes[index] !== rightBytes[index]) return false;
    }
    return true;
}

/** Shared UTF-8 encoder for comparing textual and binary forms of opaque Resume tokens. */
const RESUME_TOKEN_ENCODER = new TextEncoder();

