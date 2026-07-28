/** Public RSocket responder that owns controllers, sessions, and transport listeners. */
import {type Disposable, Mono} from "reactor-core-ts";
import {
    ErrorFrame,
    type Frame,
    FrameErrorCode,
    FrameType,
    ResumeFrame,
    SetupFrame
} from "rsocket-frames-ts";
import {
    DEFAULT_DATA_MIME_TYPE,
    DEFAULT_METADATA_MIME_TYPE,
    createWebTransportConnection,
    deserializeFrame,
    errorMessage,
    errorPayload,
    isPromiseLike,
    readFrameStreamId,
    readFrameTypeAndFlags,
    RSocketConnectionError,
    RSocketFrameSizeError,
    RSocketProtocolError,
    ReactiveTransportBinding,
    type RSocketTransportHandler,
    type RSocketTransportConnection,
    unrefTimer,
    type RSocketWebTransportSession
} from "rsocket-core-ts";
import {ControllerRegistry} from "@/controllers/registry.js";
import {ResumeRegistry} from "@/resume/registry.js";
import type {RSocketServerConnection} from "@/server/connection.js";
import {normalizeServerOptions} from "@/server/options.js";
import type {
    RSocketServerOptions,
    RSocketTcpListenOptions,
    RSocketTcpServerListener,
    RSocketWebTransportAcceptOptions
} from "@/server/types.js";
import {RSocketServerSession} from "@/session/session.js";
import type {NormalizedServerOptions} from "@/session/types.js";
import {createTcpServerListener} from "@/tcp/listener.js";
import {ReactiveAcceptedWebSocketConnection} from "@/websocket/connection.js";
import type {RSocketAcceptedWebSocket} from "@/websocket/types.js";

/**
 * Multi-connection RSocket 1.0 responder with declarative route dispatch.
 *
 * `D` and `M` describe the SETUP payload expected from accepted clients. Those
 * types are retained by policy callbacks, accepted connections, and listeners.
 */
export class RSocketServer<D = unknown, M = unknown> {
    private readonly options: NormalizedServerOptions<D, M>;
    private readonly controllers: ControllerRegistry;
    private readonly resumes = new ResumeRegistry<D, M>();
    private readonly sessions = new Set<RSocketServerSession<D, M>>();
    private readonly listeners = new Set<RSocketTcpServerListener<D, M>>();
    private readonly pendingAccepts = new Map<ReactiveTransportBinding, (error: unknown) => void>();
    private closing: Promise<void> | undefined;
    private closed = false;

    /** Validates configuration and instantiates controller singletons once. */
    constructor(options: RSocketServerOptions<D, M> = {}) {
        this.options = normalizeServerOptions(options);
        this.controllers = new ControllerRegistry(options.controllers ?? []);
    }

    /**
     * Accepts one already-created ordered transport and completes after SETUP or
     * a successful RESUME handshake.
     */
    accept(transport: RSocketTransportConnection): Mono<RSocketServerConnection<D, M>> {
        let claimed = false;
        return Mono.create<RSocketServerConnection<D, M>>((sink) => {
            if (claimed) {
                sink.error(new RSocketConnectionError("RSocket transport is already being accepted"));
                return;
            }
            claimed = true;
            if (this.closed) {
                const error = new RSocketProtocolError("RSocket server is closed");
                try {
                    transport.close({reason: error.message, error: true});
                } catch {
                    // Rejection still reaches the caller when transport cleanup fails.
                }
                sink.error(error);
                return;
            }
            let settled = false;
            let binding: ReactiveTransportBinding;
            let opening: Disposable | undefined;
            let handshakeTimer: ReturnType<typeof setTimeout> | undefined;
            const clearHandshakeTimer = (): void => {
                if (handshakeTimer !== undefined) clearTimeout(handshakeTimer);
                handshakeTimer = undefined;
            };
            const fail = (error: unknown): void => {
                if (settled) return;
                settled = true;
                clearHandshakeTimer();
                disposeSafely(opening);
                opening = undefined;
                this.pendingAccepts.delete(binding);
                try {
                    binding.close(errorMessage(error), true);
                } catch {
                    binding.dispose();
                }
                sink.error(error);
            };
            const handler: RSocketTransportHandler = {
                frame: (bytes) => {
                    if (settled) return;
                    try {
                        const connection = this.acceptFirstFrame(binding, bytes);
                        settled = true;
                        clearHandshakeTimer();
                        disposeSafely(opening);
                        opening = undefined;
                        this.pendingAccepts.delete(binding);
                        sink.success(connection);
                    } catch (error) {
                        fail(error);
                    }
                },
                frameError: fail,
                error: fail,
                media: () => undefined,
                skippedFireAndForget: (streamId) => fail(new RSocketProtocolError(
                    `WebTransport skipped FNF stream ${streamId} before SETUP`
                )),
                close: fail
            };
            binding = new ReactiveTransportBinding(transport, handler);
            this.pendingAccepts.set(binding, fail);
            try {
                handshakeTimer = setTimeout(() => fail(new RSocketConnectionError(
                    `RSocket handshake timed out after ${this.options.handshakeTimeoutMs}ms`
                )), this.options.handshakeTimeoutMs);
                unrefTimer(handshakeTimer);
                binding.start();
                if (!settled) {
                    const disposable = transport.opened.subscribe(undefined, fail);
                    if (settled) disposeSafely(disposable);
                    else opening = disposable;
                }
            } catch (error) {
                fail(error);
            }
            sink.onCancel(() => {
                if (settled) return;
                settled = true;
                clearHandshakeTimer();
                disposeSafely(opening);
                opening = undefined;
                this.pendingAccepts.delete(binding);
                try {
                    binding.close("RSocket server accept cancelled", true);
                } catch {
                    binding.dispose();
                }
            });
        });
    }

    /** Accepts one already-open WHATWG or Node-compatible WebSocket. */
    acceptWebSocket(socket: RSocketAcceptedWebSocket): Mono<RSocketServerConnection<D, M>> {
        let claimed = false;
        return Mono.defer(() => {
            if (claimed) return Mono.error(new RSocketConnectionError("WebSocket is already being accepted"));
            claimed = true;
            try {
                return this.accept(new ReactiveAcceptedWebSocketConnection(socket));
            } catch (error) {
                closeRejectedWebSocket(socket);
                return Mono.error(error instanceof RSocketConnectionError
                    ? error
                    : new RSocketConnectionError("WebSocket connection initialization failed", error));
            }
        });
    }

    /** Accepts one already-created WebTransport session through the shared mapping. */
    acceptWebTransport(
        session: RSocketWebTransportSession,
        options: RSocketWebTransportAcceptOptions = {}
    ): Mono<RSocketServerConnection<D, M>> {
        let claimed = false;
        return Mono.defer(() => {
            if (claimed) return Mono.error(new RSocketConnectionError("WebTransport session is already being accepted"));
            claimed = true;
            try {
                if (this.options.resume !== undefined && options.unreliableFireAndForget === true) {
                    throw new RSocketConnectionError(
                        "WebTransport unreliableFireAndForget is incompatible with RSocket Resume"
                    );
                }
                return this.accept(createWebTransportConnection(session, {
                    role: "responder",
                    maxFrameLength: this.options.maxFrameLength,
                    ...(options.maxReorderBufferBytes === undefined
                        ? {}
                        : {maxReorderBufferBytes: options.maxReorderBufferBytes}),
                    ...(options.unreliableFireAndForget === undefined
                        ? {}
                        : {unreliableFireAndForget: options.unreliableFireAndForget})
                }));
            } catch (error) {
                closeRejectedWebTransport(session);
                return Mono.error(error instanceof RSocketConnectionError
                    ? error
                    : new RSocketConnectionError("WebTransport session initialization failed", error));
            }
        });
    }

    /** Starts a Node TCP listener that accepts length-prefixed RSocket clients. */
    listenTcp(options: RSocketTcpListenOptions): Mono<RSocketTcpServerListener<D, M>> {
        let claimed = false;
        return Mono.defer(() => {
            if (claimed) return Mono.error(new RSocketConnectionError("TCP listener is already being started"));
            claimed = true;
            return this.closed
                ? Mono.error(new RSocketProtocolError("RSocket server is closed"))
                : createTcpServerListener(this, options, this.options.maxFrameLength, (listener) => {
                if (this.closed) return false;
                this.listeners.add(listener);
                return true;
            }, (listener) => {
                this.listeners.delete(listener);
            });
        });
    }

    /** Stops listeners and permanently disconnects every logical session. */
    close(reason = "RSocket server closed"): Mono<void> {
        return Mono.defer(() => Mono.fromPromise(this.closePromise(reason)));
    }

    /** Shares one complete shutdown operation across concurrent subscribers. */
    private closePromise(reason: string): Promise<void> {
        if (this.closing !== undefined) return this.closing;
        this.closed = true;
        const closeError = new RSocketConnectionError(reason);
        return this.closing = Promise.resolve().then(async () => {
            const failures: unknown[] = [];
            for (const fail of this.pendingAccepts.values()) {
                try {
                    fail(closeError);
                } catch (error) {
                    failures.push(error);
                }
            }
            this.pendingAccepts.clear();
            for (const session of this.sessions) {
                try {
                    session.closeFromServer(reason);
                } catch (error) {
                    failures.push(error);
                }
            }
            this.sessions.clear();
            const listeners = [...this.listeners];
            this.listeners.clear();
            const closes = listeners.map((listener) => {
                try {
                    return listener.close().block();
                } catch (error) {
                    failures.push(error);
                    return Promise.resolve(undefined);
                }
            });
            const results = await Promise.allSettled(closes);
            const rejected = results.find((result): result is PromiseRejectedResult => result.status === "rejected");
            const failure = failures[0] ?? rejected?.reason;
            if (failure !== undefined) throw failure;
        });
    }

    /** Decodes and applies the mandatory first SETUP or RESUME transport frame. */
    private acceptFirstFrame(binding: ReactiveTransportBinding, bytes: Uint8Array): RSocketServerConnection<D, M> {
        if (this.closed) throw new RSocketProtocolError("RSocket server is closed");
        if (bytes.byteLength > this.options.maxFrameLength) {
            const error = new RSocketFrameSizeError(bytes.byteLength, this.options.maxFrameLength);
            this.reject(binding, handshakeErrorCode(bytes), error.message);
            throw error;
        }
        let frame: Frame;
        let rejectionCode = FrameErrorCode.INVALID_SETUP;
        try {
            const streamId = readFrameStreamId(bytes);
            const frameType = (readFrameTypeAndFlags(bytes) >>> 10) as FrameType;
            if (streamId !== 0 || (frameType !== FrameType.SETUP && frameType !== FrameType.RESUME)) {
                throw new RSocketProtocolError("First RSocket frame must be SETUP or RESUME on stream 0");
            }
            if (frameType === FrameType.RESUME) rejectionCode = FrameErrorCode.REJECTED_RESUME;
            frame = deserializeFrame(bytes, DEFAULT_METADATA_MIME_TYPE, DEFAULT_DATA_MIME_TYPE);
        } catch (error) {
            this.reject(binding, rejectionCode, errorMessage(error));
            throw error;
        }
        return frame instanceof SetupFrame
            ? this.acceptSetup(binding, frame)
            : this.acceptResume(binding, frame as ResumeFrame);
    }

    /** Validates SETUP policy, reserves Resume token, and activates a new session. */
    private acceptSetup(binding: ReactiveTransportBinding, frame: SetupFrame): RSocketServerConnection<D, M> {
        if (frame.majorVersion !== 1 || frame.minorVersion !== 0) {
            const reason = `Unsupported RSocket protocol version ${frame.majorVersion}.${frame.minorVersion}`;
            this.reject(binding, FrameErrorCode.UNSUPPORTED_SETUP, reason);
            throw new RSocketProtocolError(reason);
        }
        if (frame.hasResume() && this.options.resume === undefined) {
            const reason = "RSocket server Resume is not enabled";
            this.reject(binding, FrameErrorCode.REJECTED_SETUP, reason);
            throw new RSocketProtocolError(reason);
        }
        if (frame.isRespectLease() && this.options.lease === undefined) {
            const reason = "RSocket server Lease is not configured";
            this.reject(binding, FrameErrorCode.UNSUPPORTED_SETUP, reason);
            throw new RSocketProtocolError(reason);
        }
        const token = frame.resumeToken;
        if (token !== undefined && this.resumes.get(token) !== undefined) {
            const reason = "RSocket Resume token is already active";
            this.reject(binding, FrameErrorCode.REJECTED_SETUP, reason);
            throw new RSocketProtocolError(reason);
        }

        const session = new RSocketServerSession<D, M>(
            frame,
            binding,
            this.options,
            this.controllers,
            this.resumes,
            (terminated) => this.sessions.delete(terminated)
        );
        let accepted: boolean;
        try {
            const decision: unknown = this.options.accept?.(session.setupContext);
            if (isPromiseLike(decision)) {
                throw new TypeError("RSocket server accept callback must return synchronously");
            }
            accepted = decision !== false;
        } catch (error) {
            const reason = errorMessage(error);
            this.reject(binding, FrameErrorCode.REJECTED_SETUP, reason);
            session.abortActivation(error, false);
            throw error;
        }
        if (!accepted) {
            const reason = "RSocket SETUP was rejected by server policy";
            const error = new RSocketProtocolError(reason);
            this.reject(binding, FrameErrorCode.REJECTED_SETUP, reason);
            session.abortActivation(error, false);
            throw error;
        }
        if (session.isTerminated) {
            throw new RSocketProtocolError("RSocket session terminated during SETUP acceptance");
        }
        if (token !== undefined && !this.resumes.reserve(token, session)) {
            const reason = "RSocket Resume token is already active";
            const error = new RSocketProtocolError(reason);
            this.reject(binding, FrameErrorCode.REJECTED_SETUP, reason);
            session.abortActivation(error, false);
            throw error;
        }
        this.sessions.add(session);
        try {
            session.activate();
            return session.connection;
        } catch (error) {
            session.abortActivation(error);
            throw error;
        }
    }

    /** Locates retained logical state and completes the server side of Resume. */
    private acceptResume(binding: ReactiveTransportBinding, frame: ResumeFrame): RSocketServerConnection<D, M> {
        const session = this.resumes.get(frame.resumeToken);
        const reason = session === undefined
            ? "Unknown or expired RSocket Resume token"
            : session.rejectResumeReason(frame);
        if (reason !== undefined) {
            this.reject(binding, FrameErrorCode.REJECTED_RESUME, reason);
            throw new RSocketProtocolError(reason);
        }
        const resumable = session as NonNullable<typeof session>;
        try {
            resumable.resume(binding, frame);
            return resumable.connection;
        } catch (error) {
            if (binding.connection.isOpen) this.reject(binding, FrameErrorCode.REJECTED_RESUME, errorMessage(error));
            throw error;
        }
    }

    /** Writes a handshake ERROR and closes a rejected physical transport. */
    private reject(binding: ReactiveTransportBinding, code: FrameErrorCode, reason: string): void {
        try {
            binding.write(new ErrorFrame(0, code, errorPayload(reason)).toUint8Array());
        } catch {
            // The original handshake rejection remains the observable failure.
        } finally {
            try {
                binding.close(reason, true);
            } catch {
                binding.dispose();
            }
        }
    }
}

/** Contains cleanup failures from custom Reactor publishers during handshake teardown. */
function disposeSafely(disposable: Disposable | undefined): void {
    try {
        disposable?.dispose();
    } catch {
        // Handshake state is already settled; cleanup cannot replace its result.
    }
}

/** Classifies an oversized first frame without attempting full deserialization. */
function handshakeErrorCode(bytes: Uint8Array): FrameErrorCode {
    try {
        return readFrameStreamId(bytes) === 0 &&
            readFrameTypeAndFlags(bytes) >>> 10 === FrameType.RESUME
            ? FrameErrorCode.REJECTED_RESUME
            : FrameErrorCode.INVALID_SETUP;
    } catch {
        return FrameErrorCode.INVALID_SETUP;
    }
}

/** Closes a WebSocket whose accepted transport wrapper could not be created. */
function closeRejectedWebSocket(socket: RSocketAcceptedWebSocket): void {
    if (socket.terminate !== undefined) {
        try {
            socket.terminate();
            return;
        } catch {
            // Fall through to the standard close handshake.
        }
    }
    try {
        socket.close(1011, "RSocket WebSocket initialization failed");
    } catch {
        // The initialization failure remains authoritative.
    }
}

/** Closes a WebTransport session whose mapping wrapper could not be created. */
function closeRejectedWebTransport(session: RSocketWebTransportSession): void {
    try {
        session.close({closeCode: 1, reason: "RSocket WebTransport initialization failed"});
    } catch {
        // The initialization failure remains authoritative.
    }
}
