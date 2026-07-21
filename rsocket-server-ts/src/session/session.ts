/** Logical RSocket responder session spanning one or more physical transports. */
import {
    ErrorFrame,
    ExtensionFrame,
    type Frame,
    FrameErrorCode,
    FrameType,
    KeepaliveFlag,
    KeepaliveFrame,
    LeaseFrame,
    Metadata,
    MetadataPushFrame,
    type MimeType,
    Payload,
    ResumeFrame,
    ResumeOkFrame,
    SetupFrame,
    WellKnownMimeType
} from "rsocket-frames-ts";
import {
    connectionClosedError,
    decodeFramePayload,
    deserializeFrame,
    type EncodedPayload,
    encodeMetadataInput,
    encodePayloadInput,
    errorFromFrame,
    errorMessage,
    errorPayload,
    emitSerializedOutboundFrames,
    hasIgnorableInvalidMetadataLength,
    isConnectionFrame,
    isErrorCodeValidForStream,
    isHandshakeErrorCode,
    isIgnorableEstablishedFrame,
    isIgnorableUnknownStreamFrame,
    isResumePositionFrame,
    readFrameStreamId,
    readFrameTypeAndFlags,
    readKeepalivePosition,
    requiresRawPayloadDecode,
    RSocketConnectionError,
    RSocketFrameSizeError,
    type RSocketPayloadInput,
    RSocketProtocolError,
    RSocketReplayBuffer
} from "rsocket-core-ts";
import {ControllerRegistry} from "@/controllers/registry.js";
import type {ResumeRegistry, ResumableServerSession} from "@/resume/registry.js";
import {
    createServerConnection,
    type RSocketServerConnection,
    type ServerConnectionHandle,
    type ServerConnectionOperations
} from "@/server/connection.js";
import type {RSocketLeaseOptions, RSocketSetupContext} from "@/server/types.js";
import type {
    NormalizedServerOptions,
    ResponderSession,
    TransportBinding
} from "@/session/types.js";
import {ServerBackgroundWork} from "@/session/background.js";
import {ServerInteractionDispatcher} from "@/session/interactions.js";
import {requestErrorCode} from "@/session/requests.js";
import {ServerSessionTimers} from "@/session/timers.js";

const EMPTY_BYTES = new Uint8Array(0);
const EMPTY_KEEPALIVE = WellKnownMimeType.APPLICATION_OCTET_STREAM.toPayload(EMPTY_BYTES);
const RAW_FRAGMENT_MIME = WellKnownMimeType.APPLICATION_OCTET_STREAM;
const APPLICATION_MIME_OVERRIDES = Object.freeze({
    metadataMimeType: RAW_FRAGMENT_MIME,
    dataMimeType: RAW_FRAGMENT_MIME
});

/** Positional frame produced while an existing Resume replay is still being written. */
interface PendingResumeFrame {
    /** Frame retained for diagnostics after its raw bytes reach the replacement transport. */
    readonly frame: Frame;
    /** Already serialized bytes whose Resume position was assigned exactly once. */
    readonly bytes: Uint8Array;
}

/** Called when a logical session permanently releases all resources. */
export type SessionTerminationListener<D = unknown, M = unknown> = (
    session: RSocketServerSession<D, M>
) => void;

/** Protocol state for one client SETUP and all successful RESUME transports. */
export class RSocketServerSession<D = unknown, M = unknown>
    implements ResponderSession, ResumableServerSession<D, M>, ServerConnectionOperations<D, M> {
    private readonly background = new ServerBackgroundWork();
    private readonly timers = new ServerSessionTimers();
    private readonly interactions: ServerInteractionDispatcher;
    private readonly replayBuffer: RSocketReplayBuffer | undefined;
    private binding: TransportBinding | undefined;
    private clientPosition = 0n;
    private serverPosition = 0n;
    private suspended = false;
    private terminated = false;
    private activated = false;
    private pendingActivationFrames: Uint8Array[] | undefined;
    private gracefulCloseError: unknown;
    private leaseRemaining = 0;
    private leaseExpiresAt = 0;
    private resumeOutbound: PendingResumeFrame[] | undefined;
    private readonly connectionHandle: ServerConnectionHandle<D, M>;
    private readonly sendOutboundFrame = (frame: Frame, bytes: Uint8Array): void => {
        this.sendSerialized(frame, bytes);
    };
    readonly connection: RSocketServerConnection<D, M>;
    readonly setupContext: RSocketSetupContext<D, M>;

    /** Builds logical state from one validated SETUP frame. */
    constructor(
        readonly setupFrame: SetupFrame,
        initialBinding: TransportBinding,
        private readonly options: NormalizedServerOptions<D, M>,
        controllers: ControllerRegistry,
        private readonly resumes: ResumeRegistry<D, M>,
        private readonly onTerminate: SessionTerminationListener<D, M>
    ) {
        this.binding = initialBinding;
        const resumeOptions = options.resume;
        if (setupFrame.hasResume()) {
            if (resumeOptions === undefined) {
                throw new RSocketProtocolError("RSocket server Resume is not configured");
            }
            this.replayBuffer = new RSocketReplayBuffer({
                maxBytes: resumeOptions.maxBufferBytes,
                retainFrames: options.activityListener !== undefined
            });
        } else {
            this.replayBuffer = undefined;
        }
        this.connectionHandle = createServerConnection(this);
        this.connection = this.connectionHandle.connection;
        this.interactions = new ServerInteractionDispatcher(
            this,
            setupFrame,
            controllers,
            this.background
        );
        const decoded = decodeFramePayload<D, M>(setupFrame);
        const contextResumeToken = setupFrame.resumeToken === undefined
            ? undefined
            : typeof setupFrame.resumeToken === "string"
                ? setupFrame.resumeToken
                : setupFrame.resumeToken.slice();
        this.setupContext = Object.freeze({
            ...decoded,
            connection: this.connection,
            keepAliveMs: setupFrame.keepalive,
            lifetimeMs: setupFrame.lifetime,
            majorVersion: setupFrame.majorVersion,
            minorVersion: setupFrame.minorVersion,
            honorsLease: setupFrame.isRespectLease(),
            ...(contextResumeToken === undefined ? {} : {resumeToken: contextResumeToken}),
            metadataMimeType: setupFrame.metadataType,
            dataMimeType: setupFrame.dataType
        });
        this.captureActivationTraffic(initialBinding);
    }

    /** Whether this logical session currently awaits a RESUME transport. */
    get isSuspended(): boolean {
        return this.suspended && !this.terminated;
    }

    /** Whether setup rejection or connection shutdown already released this session. */
    get isTerminated(): boolean {
        return this.terminated;
    }

    /** Attaches protocol dispatch after SETUP acceptance and sends an initial lease if required. */
    activate(): void {
        if (this.activated || this.terminated) return;
        this.activated = true;
        const binding = this.binding;
        if (binding === undefined) throw new RSocketConnectionError("RSocket server transport is unavailable");
        const pending = this.pendingActivationFrames;
        this.pendingActivationFrames = undefined;
        this.attach(binding);
        this.emitActivity("receive", this.setupFrame);
        this.assertActiveBinding(binding, "RSocket session terminated during SETUP activation");
        this.timers.received();
        this.startLifetime();
        if (this.setupFrame.isRespectLease()) {
            this.sendConfiguredLease();
            this.assertActiveBinding(binding, "RSocket session terminated while sending the initial LEASE");
        }
        if (pending === undefined) return;
        for (const bytes of pending) {
            if (this.terminated || this.suspended || this.binding !== binding) return;
            this.handleIncomingBytes(bytes);
        }
    }

    /** Releases partial state when activation fails after SETUP acceptance. */
    abortActivation(error: unknown, closeTransport = true): void {
        this.terminateSession(error, closeTransport);
    }

    /** Encodes one controller response with MIME types negotiated in SETUP. */
    encode(payload: RSocketPayloadInput<any, any> | undefined): EncodedPayload {
        return encodePayloadInput(payload, this.setupFrame.dataType, this.setupFrame.metadataType);
    }

    /** Admits one new interaction while enforcing graceful close and lease state. */
    acquireRequest(): string | undefined {
        if (this.gracefulCloseError !== undefined) return "RSocket connection is closing";
        if (!this.setupFrame.isRespectLease()) return undefined;
        if (this.leaseRemaining <= 0 || Date.now() >= this.leaseExpiresAt) {
            return "RSocket request exceeds the active lease";
        }
        this.leaseRemaining -= 1;
        return undefined;
    }

    /** Sends one logical frame, fragmenting it before transport when needed. */
    send(frame: Frame): void {
        if (this.terminated) throw connectionClosedError("RSocket server session is closed");
        emitSerializedOutboundFrames(frame, this.options.maxFrameLength, this.sendOutboundFrame);
    }

    /** Removes one terminal interaction and all of its retained fragments. */
    unregister(streamId: number): void {
        this.interactions.unregister(streamId);
        this.finishGracefulCloseIfIdle();
    }

    /** Sends one controller or stream-sequence failure with a legal stream error code. */
    streamError(streamId: number, error: unknown): void {
        this.interactions.terminate(streamId, error);
        if (this.terminated) return;
        try {
            this.send(new ErrorFrame(streamId, requestErrorCode(error), errorPayload(error)));
        } catch (sendError) {
            this.protocolError(sendError);
        }
    }

    /** Sends CONNECTION_ERROR and permanently terminates a protocol-invalid session. */
    protocolError(error: unknown): void {
        if (this.terminated) return;
        const protocolError = error instanceof RSocketProtocolError
            ? error
            : new RSocketProtocolError(errorMessage(error), {cause: error});
        const binding = this.binding;
        if (binding !== undefined && binding.connection.isOpen) {
            try {
                const frame = new ErrorFrame(0, FrameErrorCode.CONNECTION_ERROR, errorPayload(protocolError));
                const bytes = frame.toUint8Array();
                binding.write(bytes);
                this.emitActivity("send", frame);
            } catch {
                // The connection is still terminated when its final ERROR cannot be written.
            }
        }
        this.terminateSession(protocolError, true);
    }

    /** Sends connection-level metadata through the negotiated metadata format. */
    metadataPush(metadata: unknown, mimeType?: MimeType<any>): void {
        const value = metadata instanceof Metadata || mimeType === undefined
            ? metadata
            : mimeType.toMetadata(metadata);
        const encoded = encodeMetadataInput(value, this.setupFrame.metadataType);
        this.send(new MetadataPushFrame(encoded));
    }

    /** Replaces the requester lease after validating SETUP lease negotiation. */
    lease(options: RSocketLeaseOptions<M>): void {
        if (!this.setupFrame.isRespectLease()) {
            throw new RSocketProtocolError("Client SETUP did not enable RSocket lease semantics");
        }
        const frameMetadata = options.metadata === undefined
            ? undefined
            : encodeMetadataInput(options.metadata, this.setupFrame.metadataType);
        const previousRemaining = this.leaseRemaining;
        const previousExpiry = this.leaseExpiresAt;
        this.leaseRemaining = options.requests;
        this.leaseExpiresAt = Date.now() + options.ttlMs;
        try {
            this.send(new LeaseFrame(options.ttlMs, options.requests, frameMetadata));
        } catch (error) {
            this.leaseRemaining = previousRemaining;
            this.leaseExpiresAt = previousExpiry;
            throw error;
        }
    }

    /** Initiates an optional server KEEPALIVE carrying the current client position. */
    keepAlive(data?: Uint8Array): void {
        const payload = data === undefined
            ? EMPTY_KEEPALIVE
            : WellKnownMimeType.APPLICATION_OCTET_STREAM.toPayload(data);
        this.send(new KeepaliveFrame(KeepaliveFlag.RESPOND, this.clientPosition, payload));
    }

    /** Sends a connection error and terminates the logical session. */
    disconnect(reason = "RSocket server disconnected"): void {
        if (this.terminated) return;
        try {
            this.send(new ErrorFrame(0, FrameErrorCode.CONNECTION_ERROR, errorPayload(reason)));
        } finally {
            this.terminateSession(new RSocketConnectionError(reason), true);
        }
    }

    /** Returns why a RESUME cannot continue, or undefined when all positions are valid. */
    rejectResumeReason(frame: ResumeFrame): string | undefined {
        if (this.terminated) return "RSocket logical session has terminated";
        if (!this.suspended) return "RSocket logical session is not suspended";
        if (frame.majorVersion !== this.setupFrame.majorVersion || frame.minorVersion !== this.setupFrame.minorVersion) {
            return "RSocket Resume protocol version does not match SETUP";
        }
        if (frame.firstAvailableClientPosition > this.clientPosition) {
            return "RSocket client cannot replay from the server's last received position";
        }
        const replay = this.replayBuffer;
        if (replay === undefined || !replay.canReplayFrom(frame.lastReceivedServerPosition, this.serverPosition)) {
            return "RSocket server no longer retains the requested replay position";
        }
        return undefined;
    }

    /** Attaches a replacement transport, sends RESUME_OK, and replays missing server frames. */
    resume(binding: TransportBinding, frame: ResumeFrame): void {
        const rejection = this.rejectResumeReason(frame);
        if (rejection !== undefined) throw new RSocketProtocolError(rejection);
        const pending: Uint8Array[] = [];
        this.binding = binding;
        binding.setHandler({
            frame: (bytes) => {
                if (this.binding === binding) pending.push(bytes);
            },
            frameError: (error) => this.handleFrameFailure(binding, error),
            error: (error) => {
                if (this.binding === binding) this.transportLost(binding, error);
            },
            close: (error) => {
                if (this.binding === binding) this.transportLost(binding, error);
            }
        });
        try {
            this.emitActivity("receive", frame);
            this.assertActiveBinding(binding, "RSocket Resume session terminated before acknowledgement");
            this.sendHandshake(new ResumeOkFrame(this.clientPosition), binding);
            this.assertActiveBinding(binding, "RSocket Resume transport closed after acknowledgement");
            this.resumeOutbound = [];
            this.replayBuffer?.replayFrom(
                frame.lastReceivedServerPosition,
                this.serverPosition,
                (retainedFrame, bytes) => {
                    binding.write(bytes);
                    if (retainedFrame !== undefined) this.emitActivity("send", retainedFrame);
                }
            );
            this.assertActiveBinding(binding, "RSocket Resume transport closed during replay");
            this.suspended = false;
            this.timers.resume();
            this.timers.received();
            this.flushResumeOutbound(binding);
            if (this.setupFrame.isRespectLease()) this.sendConfiguredLease();
            for (let index = 0; index < pending.length; index += 1) {
                const bytes = pending[index] as Uint8Array;
                if (this.binding !== binding || this.terminated) break;
                this.handleIncomingBytes(bytes);
            }
            pending.length = 0;
            this.assertActiveBinding(binding, "RSocket Resume transport closed while applying replayed client frames");
            if (this.binding === binding && !this.terminated) {
                this.attach(binding);
                this.startLifetime();
            }
        } catch (error) {
            pending.length = 0;
            this.resumeOutbound = undefined;
            this.transportLost(binding, error);
            throw error;
        }
    }

    /** Permanently closes this session from the owning server. */
    closeFromServer(reason = "RSocket server closed"): void {
        if (this.terminated) return;
        try {
            this.send(new ErrorFrame(0, FrameErrorCode.CONNECTION_ERROR, errorPayload(reason)));
        } catch {
            // Server shutdown still releases logical state after an already closed transport.
        } finally {
            this.terminateSession(new RSocketConnectionError(reason), true);
        }
    }

    /** Installs transport handlers that ignore stale physical connections. */
    private attach(binding: TransportBinding): void {
        binding.setHandler({
            frame: (bytes) => {
                if (this.binding === binding) this.handleIncomingBytes(bytes);
            },
            frameError: (error) => this.handleFrameFailure(binding, error),
            error: (error) => {
                if (this.binding === binding) this.transportLost(binding, error);
            },
            close: (error) => {
                if (this.binding === binding) this.transportLost(binding, error);
            }
        });
    }

    /** Buffers reentrant peer frames while the synchronous SETUP policy is running. */
    private captureActivationTraffic(binding: TransportBinding): void {
        const pending: Uint8Array[] = this.pendingActivationFrames = [];
        binding.setHandler({
            frame: (bytes) => {
                if (this.binding === binding && !this.activated) pending.push(bytes);
            },
            frameError: (error) => this.failPendingActivation(binding, error, false),
            error: (error) => this.failPendingActivation(binding, error, true),
            close: (error) => this.failPendingActivation(binding, error, false)
        });
    }

    /** Terminates a SETUP whose transport fails before policy acceptance completes. */
    private failPendingActivation(binding: TransportBinding, error: unknown, closeTransport: boolean): void {
        if (this.binding !== binding || this.activated || this.terminated) return;
        this.pendingActivationFrames = undefined;
        this.terminateSession(error, closeTransport);
    }

    /** Separates recoverable transport loss from terminal frame-codec violations. */
    private handleFrameFailure(binding: TransportBinding, error: unknown): void {
        if (this.binding !== binding) return;
        if (error instanceof RSocketConnectionError) {
            this.transportLost(binding, error);
            return;
        }
        this.protocolError(error instanceof RSocketProtocolError
            ? error
            : new RSocketProtocolError("Failed to decode incoming RSocket frame", {cause: error}));
    }

    /** Rejects continuation after a reentrant callback replaced or closed the active transport. */
    private assertActiveBinding(binding: TransportBinding, message: string): void {
        if (this.binding !== binding || this.terminated || !binding.connection.isOpen) {
            throw new RSocketConnectionError(message);
        }
    }

    /** Decodes one raw frame and advances implied client position before dispatch. */
    private handleIncomingBytes(bytes: Uint8Array): void {
        if (this.terminated || this.suspended) return;
        if (bytes.byteLength > this.options.maxFrameLength) {
            this.protocolError(new RSocketFrameSizeError(bytes.byteLength, this.options.maxFrameLength));
            return;
        }
        try {
            const streamId = readFrameStreamId(bytes);
            const typeAndFlags = readFrameTypeAndFlags(bytes);
            const frameType = (typeAndFlags >>> 10) as FrameType;
            const activeStream = this.interactions.hasStream(streamId);
            if (isIgnorableEstablishedFrame(frameType, streamId, activeStream)) {
                this.recordClientPosition(frameType, bytes.byteLength);
                return;
            }
            if (!activeStream && isIgnorableUnknownStreamFrame(frameType, streamId)) {
                this.recordClientPosition(frameType, bytes.byteLength);
                return;
            }
            if (frameType === FrameType.PAYLOAD && this.interactions.consumeIgnoredPayloadFragment(
                streamId,
                typeAndFlags
            )) {
                this.recordClientPosition(frameType, bytes.byteLength);
                return;
            }
            if (hasIgnorableInvalidMetadataLength(bytes, typeAndFlags)) {
                this.recordClientPosition(frameType, bytes.byteLength);
                this.interactions.ignoreInvalidMetadataFrame(streamId, frameType, typeAndFlags);
                return;
            }
            const rawPayload = requiresRawPayloadDecode(frameType);
            const frame = deserializeFrame(
                bytes,
                this.setupFrame.metadataType,
                this.setupFrame.dataType,
                rawPayload ? APPLICATION_MIME_OVERRIDES : undefined,
                frameType
            );
            if (frameType === FrameType.KEEPALIVE) {
                if (this.replayBuffer !== undefined) {
                    this.replayBuffer.acknowledge(readKeepalivePosition(bytes), this.serverPosition);
                }
                this.timers.received();
            }
            this.recordClientPosition(frameType, bytes.byteLength);
            this.emitActivity("receive", frame);
            if (this.terminated || this.suspended) return;
            this.dispatch(frame, streamId);
        } catch (error) {
            this.protocolError(new RSocketProtocolError("Failed to process incoming RSocket frame", {cause: error}));
        }
    }

    /** Routes one decoded frame to connection, request, or active-stream handling. */
    private dispatch(frame: Frame, streamId: number): void {
        if (isIgnorableEstablishedFrame(frame.type, streamId)) return;
        if (frame instanceof ErrorFrame) {
            if (streamId !== 0 && !this.interactions.hasStream(streamId)) return;
            if (!isErrorCodeValidForStream(frame.code, streamId)) {
                throw new RSocketProtocolError("RSocket ERROR code is invalid for its stream ID", {
                    streamId,
                    frame
                });
            }
        }
        if (isConnectionFrame(frame.type) && streamId !== 0) {
            throw new RSocketProtocolError("Connection-scoped RSocket frame must use stream ID 0", {streamId});
        }
        if (this.interactions.handle(frame)) return;
        switch (frame.type) {
            case FrameType.KEEPALIVE:
                this.handleKeepalive(frame as KeepaliveFrame);
                return;
            case FrameType.LEASE:
                return;
            case FrameType.METADATA_PUSH:
                this.handleMetadataPush(frame as MetadataPushFrame);
                return;
            case FrameType.ERROR:
                if (isHandshakeErrorCode((frame as ErrorFrame).code)) return;
                if ((frame as ErrorFrame).code === FrameErrorCode.CONNECTION_CLOSE) {
                    this.gracefulCloseError = errorFromFrame(frame as ErrorFrame);
                    this.finishGracefulCloseIfIdle();
                } else {
                    this.terminateSession(errorFromFrame(frame as ErrorFrame), true);
                }
                return;
            case FrameType.EXT:
                if (!(frame as ExtensionFrame).canBeIgnored()) {
                    throw new RSocketProtocolError("Unsupported required RSocket extension frame", {streamId});
                }
                return;
            case FrameType.RESUME:
                throw new RSocketProtocolError("RESUME is only valid as the first transport frame");
            case FrameType.RESUME_OK:
                return;
            default:
                if (!frame.canBeIgnored()) throw new RSocketProtocolError("Unsupported required RSocket frame type");
        }
    }

    /** Echoes requester KEEPALIVE data and current last-received client position. */
    private handleKeepalive(frame: KeepaliveFrame): void {
        if (!frame.isRequireRespond()) return;
        this.send(new KeepaliveFrame(
            KeepaliveFlag.NONE,
            this.clientPosition,
            frame.payload as Payload<any> | undefined
        ));
    }

    /** Delivers asynchronous metadata without opening an interaction stream. */
    private handleMetadataPush(frame: MetadataPushFrame): void {
        const handler = this.options.metadataPush;
        if (handler === undefined) return;
        const value = frame.metadata;
        if (value === undefined) {
            throw new RSocketProtocolError("METADATA_PUSH requires metadata");
        }
        try {
            const metadata = decodePushedMetadata<M>(value, this.setupFrame.metadataType);
            this.background.consume(handler(metadata, this.connection));
        } catch {
            // Metadata decoding and callbacks are application concerns without a response stream.
        }
    }

    /** Advances the received implied position only for Resume-enabled sessions. */
    private recordClientPosition(frameType: FrameType, frameLength: number): void {
        if (this.replayBuffer !== undefined && isResumePositionFrame(frameType)) {
            this.clientPosition += BigInt(frameLength);
        }
    }

    /** Writes one already-sized frame while maintaining server Resume positions. */
    private sendSerialized(frame: Frame, bytes = frame.toUint8Array()): void {
        if (bytes.byteLength > this.options.maxFrameLength) {
            throw new RSocketFrameSizeError(bytes.byteLength, this.options.maxFrameLength);
        }
        const replay = this.replayBuffer;
        const positional = replay !== undefined && isResumePositionFrame(frame.type);
        if (positional) {
            try {
                this.serverPosition = replay.record(this.serverPosition, frame, bytes);
            } catch (error) {
                this.terminateSession(error, true);
                throw error;
            }
        }
        const resumeOutbound = this.resumeOutbound;
        if (this.suspended || resumeOutbound !== undefined) {
            if (!positional) throw new RSocketConnectionError("Non-replayable frame cannot be sent while Resume is pending");
            resumeOutbound?.push({frame, bytes});
            return;
        }
        const binding = this.binding;
        if (binding === undefined) throw new RSocketConnectionError("RSocket server transport is unavailable");
        try {
            binding.write(bytes);
            this.emitActivity("send", frame);
        } catch (error) {
            this.transportLost(binding, error);
            if (!positional || !this.suspended) throw error;
        }
    }

    /** Writes SETUP/RESUME handshake output without assigning implied positions. */
    private sendHandshake(frame: Frame, binding: TransportBinding): void {
        const bytes = frame.toUint8Array();
        if (bytes.byteLength > this.options.maxFrameLength) {
            throw new RSocketFrameSizeError(bytes.byteLength, this.options.maxFrameLength);
        }
        binding.write(bytes);
        this.emitActivity("send", frame);
    }

    /** Flushes frames produced reentrantly during replay without assigning positions twice. */
    private flushResumeOutbound(binding: TransportBinding): void {
        const pending = this.resumeOutbound;
        if (pending === undefined) return;
        try {
            for (let index = 0; index < pending.length; index += 1) {
                const {frame, bytes} = pending[index] as PendingResumeFrame;
                this.assertActiveBinding(binding, "RSocket Resume transport closed while flushing new frames");
                binding.write(bytes);
                this.emitActivity("send", frame);
            }
        } finally {
            this.resumeOutbound = undefined;
            pending.length = 0;
        }
    }

    /** Suspends resumable state or terminates a non-resumable logical session. */
    private transportLost(binding: TransportBinding, error: unknown): void {
        if (this.terminated || this.binding !== binding) return;
        this.binding = undefined;
        binding.dispose();
        if (binding.connection.isOpen) {
            try {
                binding.close(errorMessage(error), true);
            } catch {
                binding.dispose();
            }
        }
        this.timers.stopLifetime();
        if (this.replayBuffer === undefined || this.setupFrame.resumeToken === undefined) {
            this.terminateSession(error, false);
            return;
        }
        this.suspended = true;
        const ttl = this.options.resume?.ttlMs;
        if (ttl === undefined) {
            this.terminateSession(error, false);
            return;
        }
        this.timers.startResumeExpiry(ttl, () => {
            if (this.suspended) this.terminateSession(
                new RSocketConnectionError(`RSocket Resume TTL expired after ${ttl}ms`, error),
                false
            );
        });
    }

    /** Starts the negotiated inactivity timer for the active physical transport. */
    private startLifetime(): void {
        this.timers.startLifetime(this.setupFrame.lifetime, () => {
            if (this.terminated || this.suspended) return;
            const binding = this.binding;
            if (binding === undefined) return;
            const error = new RSocketConnectionError("RSocket client lifetime expired");
            this.transportLost(binding, error);
        });
    }

    /** Reissues the configured lease after SETUP or successful Resume. */
    private sendConfiguredLease(): void {
        const configured = this.options.lease;
        if (configured === undefined) {
            throw new RSocketProtocolError("RSocket server Lease is not configured");
        }
        this.lease({
            ttlMs: configured.ttlMs,
            requests: configured.requests,
            ...(configured.metadata === undefined ? {} : {metadata: configured.metadata})
        });
    }

    /** Emits optional diagnostics while containing user listener failures. */
    private emitActivity(direction: "send" | "receive", frame: Frame): void {
        try {
            this.options.activityListener?.({connection: this.connection, direction, frame});
        } catch {
            // Diagnostics cannot alter protocol state.
        }
    }

    /** Closes normally after a peer CONNECTION_CLOSE and the final active stream. */
    private finishGracefulCloseIfIdle(): void {
        const error = this.gracefulCloseError;
        if (error === undefined || !this.interactions.isIdle) return;
        this.terminateSession(error, true, false);
    }

    /** Permanently releases transports, streams, timers, replay bytes, and token state. */
    private terminateSession(error: unknown, closeTransport: boolean, transportError = true): void {
        if (this.terminated) return;
        this.terminated = true;
        this.suspended = false;
        this.timers.close();
        const binding = this.binding;
        this.binding = undefined;
        if (binding !== undefined) {
            if (closeTransport) {
                try {
                    binding.close(errorMessage(error), transportError);
                } catch {
                    binding.dispose();
                }
            } else binding.dispose();
        }
        this.interactions.close(error);
        this.background.close();
        this.pendingActivationFrames = undefined;
        this.resumeOutbound = undefined;
        this.replayBuffer?.clear();
        this.connectionHandle.release();
        this.resumes.release(this);
        this.onTerminate(this);
    }
}

/** Decodes raw METADATA_PUSH bytes while preserving the callback's MIME wrapper contract. */
function decodePushedMetadata<M>(value: Metadata<any>, mimeType: MimeType<M>): Metadata<M> {
    if (value.mimeType.mimeType !== RAW_FRAGMENT_MIME.mimeType || mimeType === value.mimeType) {
        return value as Metadata<M>;
    }
    const bytes = value.toUint8Array();
    const decoded = mimeType.toMetadata(bytes, false);
    return decoded instanceof Metadata ? decoded : new Metadata(mimeType, decoded, bytes);
}
