/** Reactive RSocket frame mapping over one multiplexed WebTransport session. */
import {Flux, Mono} from "reactor-core-ts";
import {
    FireAndForgetFlag,
    FrameType,
    PayloadFlag
} from "rsocket-frames-ts";
import {
    RSocketConnectionError,
    RSocketFrameSizeError,
    RSocketProtocolError
} from "@/errors/index.js";
import {DEFAULT_MAX_FRAME_LENGTH} from "@/protocol/constants.js";
import {
    readFrameStreamId,
    readFrameTypeAndFlags
} from "@/protocol/frames.js";
import type {
    RSocketTransportClose,
    RSocketTransportCloseOptions
} from "@/transport/types.js";
import {
    RSOCKET_WEBTRANSPORT_FNF_DATAGRAM_HEADER_LENGTH,
    RSOCKET_WEBTRANSPORT_PREFACE_LENGTH,
    RSOCKET_WEBTRANSPORT_RECORD_HEADER_LENGTH,
    RSocketWebTransportDatagramKind,
    RSocketWebTransportStreamKind
} from "@/webtransport/constants.js";
import {RSocketWebTransportWriteLane} from "@/webtransport/lane.js";
import {RSocketWebTransportOrderedReceiver} from "@/webtransport/ordered.js";
import {
    decodeWebTransportDatagramPreface,
    decodeWebTransportStreamPreface,
    encodeWebTransportDatagram,
    encodeWebTransportStreamPreface,
    type RSocketWebTransportReliableKind
} from "@/webtransport/preface.js";
import {
    encodeWebTransportRecord,
    encodeWebTransportSkipRecord,
    readWebTransportOrdinal,
    RSocketWebTransportRecordDecoder,
    writeWebTransportOrdinal
} from "@/webtransport/records.js";
import type {
    RSocketWebTransportBidirectionalStream,
    RSocketWebTransportConnection as RSocketWebTransportConnectionContract,
    RSocketWebTransportConnectionOptions,
    RSocketWebTransportReadable,
    RSocketWebTransportReader,
    RSocketWebTransportSession
} from "@/webtransport/types.js";

/** Default cap for frames waiting behind another QUIC stream. */
const MIN_REORDER_BUFFER_BYTES = 8 * 1024 * 1024;
/** Conservative bookkeeping charged for each pending JavaScript reorder entry. */
const REORDER_ENTRY_OVERHEAD_BYTES = 128;
/** WebTransport application close code used for mapping or protocol failures. */
const WEBTRANSPORT_PROTOCOL_ERROR = 1;
/** Avoids transient stream-limit failures under high interaction concurrency. */
const WAIT_FOR_STREAM_CREDIT = Object.freeze({waitUntilAvailable: true});

/** Frame and physical-stream context retained while restoring global order. */
interface InboundRecord {
    /** Raw RSocket frame, or absent for a best-effort skip marker. */
    readonly frame?: Uint8Array;
    /** RSocket stream ID skipped by a lost best-effort FNF. */
    readonly skippedFireAndForgetStreamId?: number;
    /** Reliable lane carrying the record, or the FNF datagram extension. */
    readonly kind: RSocketWebTransportReliableKind | RSocketWebTransportDatagramKind.FIRE_AND_FORGET;
    /** Per-interaction stream context for reliable bidi records. */
    readonly interaction?: InboundInteraction;
}

/** State attached to one incoming interaction bidi stream. */
interface InboundInteraction {
    /** Return direction of the same bidirectional stream. */
    readonly lane: RSocketWebTransportWriteLane;
    /** Logical RSocket stream id fixed by the first record. */
    streamId?: number;
}

/** Lifecycle state for one logical request interaction. */
interface InteractionState {
    /** Preferred bidi stream for outgoing records. */
    readonly lane: RSocketWebTransportWriteLane;
    /** Initial request type once observed. */
    type: FrameType | undefined;
    /** Whether this endpoint completed its request-channel direction. */
    localComplete: boolean;
    /** Whether the peer completed its request-channel direction. */
    remoteComplete: boolean;
}

/** Reader plus decoder retained until one reliable byte stream terminates. */
interface ActiveReader {
    /** Native reader cancelled during physical-session cleanup. */
    readonly reader: RSocketWebTransportReader<Uint8Array>;
    /** Decoder releasing an incomplete record on cancellation. */
    readonly decoder: RSocketWebTransportRecordDecoder;
}

/** One RSocket logical transport mapped onto a WebTransport session. */
export class ReactiveWebTransportConnection implements RSocketWebTransportConnectionContract {
    /** Resolves after the session and requester-owned control stream are ready. */
    readonly opened: Mono<void>;
    /** Globally ordered standard RSocket frames. */
    readonly frames: Flux<Uint8Array>;
    /** Reserved native error channel; terminal failures are emitted by `frames`. */
    readonly errors: Flux<unknown>;
    /** Physical WebTransport session close notifications. */
    readonly closes: Flux<RSocketTransportClose>;
    /** Best-effort media extension datagrams. */
    readonly media: Flux<Uint8Array>;
    /** Lost best-effort FNF stream IDs in the same global order as frames. */
    readonly skippedFireAndForget: Flux<number>;

    private readonly maxFrameLength: number;
    private readonly unreliableFireAndForget: boolean;
    private readonly ordered: RSocketWebTransportOrderedReceiver<InboundRecord>;
    private readonly interactionLanes = new Map<number, InteractionState>();
    private readonly outboundFragmentedFnf = new Set<number>();
    private readonly inboundFragmentedFnf = new Set<number>();
    private readonly activeReaders = new Set<ActiveReader>();
    private readonly activeObjectReaders = new Set<RSocketWebTransportReader<unknown>>();
    private readonly activeLanes = new Set<RSocketWebTransportWriteLane>();
    private readonly readyPromise: Promise<void>;
    private controlLane: RSocketWebTransportWriteLane | undefined;
    private reliableLane: RSocketWebTransportWriteLane | undefined;
    private datagramLane: RSocketWebTransportWriteLane | undefined;
    private localControlReadable: RSocketWebTransportReadable<Uint8Array> | undefined;
    private emitFrame: ((frame: Uint8Array) => void) | undefined;
    private emitMedia: ((payload: Uint8Array) => void) | undefined;
    private emitSkippedFireAndForget: ((streamId: number) => void) | undefined;
    private failFrames: ((error: unknown) => void) | undefined;
    private frameSubscriberClaimed = false;
    private mediaSubscriberClaimed = false;
    private skippedSubscriberClaimed = false;
    private receiving = false;
    private ready = false;
    private closing = false;
    private failure: unknown;
    private nextOutboundOrdinal = 0n;

    /** Validates mapping limits and starts the WebTransport handshake once. */
    constructor(
        readonly session: RSocketWebTransportSession,
        private readonly options: RSocketWebTransportConnectionOptions
    ) {
        this.maxFrameLength = normalizeFrameLength(options.maxFrameLength);
        const reorderBytes = normalizeReorderBytes(
            options.maxReorderBufferBytes,
            this.maxFrameLength
        );
        this.unreliableFireAndForget = options.unreliableFireAndForget === true;
        this.ordered = new RSocketWebTransportOrderedReceiver(reorderBytes);
        this.readyPromise = this.initialize();
        void this.readyPromise.catch(ignoreRejection);
        this.opened = Mono.fromPromise(this.readyPromise);
        this.frames = this.frameFlux();
        this.media = this.mediaFlux();
        this.skippedFireAndForget = this.skippedFireAndForgetFlux();
        this.errors = Flux.create<unknown>(() => undefined);
        this.closes = this.closeFlux();
    }

    /** Whether the session can currently accept a mapping record. */
    get isOpen(): boolean {
        return this.ready && !this.closing && this.failure === undefined;
    }

    /** Routes one complete raw frame to its control, interaction, or uni lane. */
    write(frame: Uint8Array): void {
        if (!this.isOpen) throw new RSocketConnectionError("WebTransport session is not open");
        if (frame.byteLength > this.maxFrameLength) {
            throw new RSocketFrameSizeError(frame.byteLength, this.maxFrameLength);
        }
        const typeAndFlags = readFrameTypeAndFlags(frame);
        const type = (typeAndFlags >>> 10) as FrameType;
        const streamId = readFrameStreamId(frame);

        if (type === FrameType.REQUEST_FNF) {
            this.writeFireAndForget(frame, streamId, typeAndFlags);
            return;
        }
        if (type === FrameType.METADATA_PUSH) {
            assertStreamId(streamId, 0, "METADATA_PUSH");
            this.writeReliable(frame);
            return;
        }
        if (type === FrameType.PAYLOAD && this.outboundFragmentedFnf.has(streamId)) {
            this.writeReliable(frame);
            if (!hasFollows(typeAndFlags)) this.outboundFragmentedFnf.delete(streamId);
            return;
        }
        if (isControlFrame(type, streamId)) {
            this.writeControl(frame);
            return;
        }
        if (isInteractionFrame(type, streamId)) {
            this.writeInteraction(frame, streamId, type, typeAndFlags);
            return;
        }
        throw new RSocketProtocolError(
            `RSocket ${FrameType[type] ?? type} frame cannot use WebTransport stream ${streamId}`
        );
    }

    /** Sends one media extension datagram without affecting RSocket positions. */
    writeMedia(payload: Uint8Array): void {
        if (!this.isOpen) throw new RSocketConnectionError("WebTransport session is not open");
        const lane = this.ensureDatagramLane();
        const packet = encodeWebTransportDatagram(RSocketWebTransportDatagramKind.MEDIA, payload);
        this.assertDatagramLength(packet.byteLength);
        lane.enqueue(packet);
    }

    /** Closes all stream resources and then the containing WebTransport session. */
    close(options: RSocketTransportCloseOptions = {}): void {
        if (this.closing) return;
        this.closing = true;
        this.ready = false;
        this.releaseResources(options.error === true ? new RSocketConnectionError(
            options.reason ?? "RSocket WebTransport session failed"
        ) : undefined);
        try {
            this.session.close({
                closeCode: options.code ?? (options.error === true ? WEBTRANSPORT_PROTOCOL_ERROR : 0),
                ...(options.reason === undefined ? {} : {reason: options.reason})
            });
        } catch (error) {
            if (this.failure === undefined) this.signalFailure(error);
        }
    }

    /** Waits for CONNECT and creates the requester-owned control bidi stream. */
    private async initialize(): Promise<void> {
        try {
            await waitForReady(this.session, this.options.timeoutMs, this.options.abortSignal);
            if (this.closing) throw new RSocketConnectionError("WebTransport session closed during handshake");
            if (this.unreliableFireAndForget && this.session.reliability === "reliable-only") {
                throw new RSocketConnectionError(
                    "WebTransport best-effort FNF requires an unreliable-capable session"
                );
            }
            if (this.options.role === "requester") {
                const stream = await this.session.createBidirectionalStream(WAIT_FOR_STREAM_CREDIT);
                if (this.closing) throw new RSocketConnectionError("WebTransport session closed during handshake");
                this.controlLane = this.trackLane(new RSocketWebTransportWriteLane(
                    stream.writable,
                    encodeWebTransportStreamPreface(RSocketWebTransportStreamKind.CONTROL),
                    (error) => this.signalFailure(new RSocketConnectionError(
                        "WebTransport control write failed",
                        error
                    ))
                ));
                this.localControlReadable = stream.readable;
            }
            this.ready = true;
            this.startReceiving();
        } catch (error) {
            const failure = error instanceof RSocketConnectionError || error instanceof RSocketProtocolError
                ? error
                : new RSocketConnectionError("WebTransport session initialization failed", error);
            this.signalFailure(failure);
            throw failure;
        }
    }

    /** Builds the single-consumer frame Flux used by the protocol binding. */
    private frameFlux(): Flux<Uint8Array> {
        return Flux.create<Uint8Array>((sink) => {
            if (this.frameSubscriberClaimed) {
                sink.error(new RSocketConnectionError("WebTransport frame stream supports one subscriber"));
                return;
            }
            this.frameSubscriberClaimed = true;
            if (this.failure !== undefined) {
                sink.error(this.failure);
                return;
            }
            this.emitFrame = (frame) => {
                if (!sink.isCancelled()) sink.next(frame);
            };
            this.failFrames = (error) => {
                if (!sink.isCancelled()) sink.error(error);
            };
            sink.onCancel(() => {
                this.emitFrame = undefined;
                this.failFrames = undefined;
            });
            this.startReceiving();
        });
    }

    /** Builds the single-consumer best-effort media Flux. */
    private mediaFlux(): Flux<Uint8Array> {
        return Flux.create<Uint8Array>((sink) => {
            if (this.mediaSubscriberClaimed) {
                sink.error(new RSocketConnectionError("WebTransport media stream supports one subscriber"));
                return;
            }
            this.mediaSubscriberClaimed = true;
            this.emitMedia = (payload) => {
                if (!sink.isCancelled()) sink.next(payload);
            };
            sink.onCancel(() => {
                this.emitMedia = undefined;
            });
            this.startReceiving();
        });
    }

    /** Builds the single-consumer best-effort FNF sequencing Flux. */
    private skippedFireAndForgetFlux(): Flux<number> {
        return Flux.create<number>((sink) => {
            if (this.skippedSubscriberClaimed) {
                sink.error(new RSocketConnectionError("WebTransport skipped-FNF stream supports one subscriber"));
                return;
            }
            this.skippedSubscriberClaimed = true;
            this.emitSkippedFireAndForget = (streamId) => {
                if (!sink.isCancelled()) sink.next(streamId);
            };
            sink.onCancel(() => {
                this.emitSkippedFireAndForget = undefined;
            });
        });
    }

    /** Converts the session's terminal promise to one normalized close signal. */
    private closeFlux(): Flux<RSocketTransportClose> {
        return Flux.create<RSocketTransportClose>((sink) => {
            let cancelled = false;
            sink.onCancel(() => {
                cancelled = true;
            });
            void Promise.resolve(this.session.closed).then((close) => {
                this.ready = false;
                if (cancelled || sink.isCancelled()) return;
                sink.next({
                    ...(close?.closeCode === undefined ? {} : {code: close.closeCode}),
                    reason: close?.reason ?? "WebTransport session closed"
                });
                if (!sink.isCancelled()) sink.complete();
            }, (error) => {
                this.ready = false;
                if (cancelled || sink.isCancelled()) return;
                sink.next({reason: "WebTransport session failed", cause: error});
                if (!sink.isCancelled()) sink.complete();
            });
        });
    }

    /** Starts all receive loops only after both readiness and a frame subscriber exist. */
    private startReceiving(): void {
        if (this.receiving || !this.ready || this.emitFrame === undefined || this.closing) return;
        this.receiving = true;
        const control = this.localControlReadable;
        this.localControlReadable = undefined;
        if (control !== undefined) {
            this.consumeReliable(control, RSocketWebTransportStreamKind.CONTROL, undefined, true);
        }
        this.consumeIncomingBidirectionalStreams();
        this.consumeIncomingUnidirectionalStreams();
        if (this.session.datagrams !== undefined) this.consumeDatagrams();
    }

    /** Accepts every peer-created bidi stream and classifies its preface. */
    private consumeIncomingBidirectionalStreams(): void {
        void this.readObjects(this.session.incomingBidirectionalStreams, (stream) => {
            this.consumeAcceptedBidirectional(stream);
        }, "WebTransport incoming bidi stream failed");
    }

    /** Accepts every peer-created reliable unidirectional stream. */
    private consumeIncomingUnidirectionalStreams(): void {
        void this.readObjects(this.session.incomingUnidirectionalStreams, (readable) => {
            this.consumeReliable(readable, undefined, undefined, false, true);
        }, "WebTransport incoming uni stream failed");
    }

    /** Reads unreliable FNF and media extension datagrams. */
    private consumeDatagrams(): void {
        const datagrams = this.session.datagrams;
        if (datagrams === undefined) return;
        void this.readObjects(datagrams.readable, (packet) => {
            if (packet !== null) this.handleDatagram(packet);
        },
            "WebTransport datagram receive failed", false);
    }

    /** Classifies one accepted bidi stream before reading its records. */
    private consumeAcceptedBidirectional(stream: RSocketWebTransportBidirectionalStream): void {
        let interaction: InboundInteraction | undefined;
        let kind: RSocketWebTransportReliableKind | undefined;
        this.consumeReliable(stream.readable, undefined, (decoded) => {
            kind = decoded;
            if (decoded === RSocketWebTransportStreamKind.CONTROL) {
                if (this.options.role !== "responder" || this.controlLane !== undefined) {
                    throw new RSocketProtocolError("Peer opened an unexpected WebTransport control stream");
                }
                this.controlLane = this.trackLane(new RSocketWebTransportWriteLane(
                    stream.writable,
                    undefined,
                    (error) => this.signalFailure(new RSocketConnectionError(
                        "WebTransport control response write failed",
                        error
                    ))
                ));
                return;
            }
            if (decoded !== RSocketWebTransportStreamKind.INTERACTION) {
                throw new RSocketProtocolError("Reliable FNF WebTransport lane must be unidirectional");
            }
            interaction = {
                lane: this.trackLane(new RSocketWebTransportWriteLane(
                    stream.writable,
                    undefined,
                    (error) => this.signalFailure(new RSocketConnectionError(
                        "WebTransport interaction response write failed",
                        error
                    ))
                ))
            };
        }, false, false, () => interaction, () => kind);
    }

    /** Reads one byte stream with a known or prefaced mapping kind. */
    private consumeReliable(
        readable: RSocketWebTransportReadable<Uint8Array>,
        knownKind?: RSocketWebTransportReliableKind,
        onKind?: (kind: RSocketWebTransportReliableKind) => void,
        failOnEnd = false,
        requireUni = false,
        interactionValue?: () => InboundInteraction | undefined,
        kindValue?: () => RSocketWebTransportReliableKind | undefined
    ): void {
        const decoder = new RSocketWebTransportRecordDecoder(this.maxFrameLength);
        let reader: RSocketWebTransportReader<Uint8Array>;
        try {
            reader = readable.getReader();
        } catch (error) {
            this.signalFailure(new RSocketConnectionError("WebTransport reader acquisition failed", error));
            return;
        }
        const active: ActiveReader = {reader, decoder};
        this.activeReaders.add(active);
        void (async () => {
            let kind = knownKind;
            let preface: Uint8Array | undefined = kind === undefined
                ? new Uint8Array(RSOCKET_WEBTRANSPORT_PREFACE_LENGTH)
                : undefined;
            let prefaceOffset = 0;
            try {
                while (!this.closing) {
                    const result = await reader.read();
                    if (result.done) {
                        if (preface !== undefined) {
                            throw new RSocketProtocolError("WebTransport stream ended before its mapping preface");
                        }
                        decoder.finish();
                        if (failOnEnd && !this.closing) {
                            throw new RSocketConnectionError("WebTransport control stream ended");
                        }
                        return;
                    }
                    const chunk = result.value;
                    if (!(chunk instanceof Uint8Array)) {
                        throw new RSocketProtocolError("WebTransport byte stream emitted a non-Uint8Array chunk");
                    }
                    let offset = 0;
                    if (preface !== undefined) {
                        const copied = Math.min(preface.byteLength - prefaceOffset, chunk.byteLength);
                        preface.set(chunk.subarray(0, copied), prefaceOffset);
                        prefaceOffset += copied;
                        offset = copied;
                        if (prefaceOffset !== preface.byteLength) continue;
                        kind = decodeWebTransportStreamPreface(preface);
                        preface = undefined;
                        if (requireUni && kind !== RSocketWebTransportStreamKind.RELIABLE) {
                            throw new RSocketProtocolError("WebTransport uni stream has an invalid mapping kind");
                        }
                        onKind?.(kind);
                    }
                    if (offset === chunk.byteLength) continue;
                    const activeKind = kind ?? kindValue?.();
                    if (activeKind === undefined) {
                        throw new RSocketProtocolError("WebTransport stream kind was not initialized");
                    }
                    decoder.push(chunk.subarray(offset), (ordinal, frame, skippedFireAndForgetStreamId) => {
                        const interaction = interactionValue?.();
                        this.acceptRecord(ordinal, {
                            ...(frame === undefined ? {} : {frame}),
                            ...(skippedFireAndForgetStreamId === undefined
                                ? {}
                                : {skippedFireAndForgetStreamId}),
                            kind: activeKind,
                            ...(interaction === undefined ? {} : {interaction})
                        });
                    });
                }
            } catch (error) {
                if (!this.closing) this.signalFailure(error instanceof RSocketConnectionError || error instanceof RSocketProtocolError
                    ? error
                    : new RSocketConnectionError("WebTransport stream receive failed", error));
            } finally {
                decoder.reset();
                this.activeReaders.delete(active);
                releaseReader(reader);
            }
        })();
    }

    /** Reads a stream of native objects with cancellation-safe cleanup. */
    private async readObjects<T>(
        readable: RSocketWebTransportReadable<T>,
        consume: (value: T) => void,
        failureMessage: string,
        terminal = true
    ): Promise<void> {
        let reader: RSocketWebTransportReader<T>;
        try {
            reader = readable.getReader();
        } catch (error) {
            this.signalFailure(new RSocketConnectionError(failureMessage, error));
            return;
        }
        const retainedReader = reader as RSocketWebTransportReader<unknown>;
        this.activeObjectReaders.add(retainedReader);
        try {
            while (!this.closing) {
                const result = await reader.read();
                if (result.done) return;
                try {
                    consume(result.value);
                } catch (error) {
                    if (!this.closing) {
                        this.signalFailure(error instanceof RSocketProtocolError
                            ? error
                            : new RSocketConnectionError(failureMessage, error));
                    }
                    return;
                }
            }
        } catch (error) {
            if (!this.closing && terminal) {
                this.signalFailure(error instanceof RSocketProtocolError
                    ? error
                    : new RSocketConnectionError(failureMessage, error));
            }
        } finally {
            this.activeObjectReaders.delete(retainedReader);
            releaseReader(reader);
        }
    }

    /** Restores one record's global position before endpoint dispatch. */
    private acceptRecord(ordinal: bigint, record: InboundRecord): void {
        this.ordered.accept(
            ordinal,
            record,
            REORDER_ENTRY_OVERHEAD_BYTES +
                RSOCKET_WEBTRANSPORT_RECORD_HEADER_LENGTH +
                (record.frame?.byteLength ?? 4),
            (ordered) => this.emitOrderedRecord(ordered)
        );
    }

    /** Validates lane ownership and emits one globally ordered frame. */
    private emitOrderedRecord(record: InboundRecord): void {
        const frame = record.frame;
        if (frame === undefined) {
            if (record.kind !== RSocketWebTransportStreamKind.CONTROL || !this.unreliableFireAndForget) {
                throw new RSocketProtocolError(
                    "WebTransport FNF skip marker requires the negotiated control stream"
                );
            }
            const streamId = record.skippedFireAndForgetStreamId;
            if (streamId !== undefined) this.emitSkippedFireAndForget?.(streamId);
            return;
        }
        const typeAndFlags = readFrameTypeAndFlags(frame);
        const type = (typeAndFlags >>> 10) as FrameType;
        const streamId = readFrameStreamId(frame);
        this.validateInboundLane(record, type, streamId, typeAndFlags);
        this.emitFrame?.(frame);
        if (record.kind === RSocketWebTransportStreamKind.INTERACTION) {
            this.trackInteractionLifecycle(streamId, type, typeAndFlags, false);
        }
    }

    /** Rejects frames sent on a mapping lane that does not own their scope. */
    private validateInboundLane(
        record: InboundRecord,
        type: FrameType,
        streamId: number,
        typeAndFlags: number
    ): void {
        if (record.kind === RSocketWebTransportDatagramKind.FIRE_AND_FORGET) {
            if (type !== FrameType.REQUEST_FNF || streamId === 0 || hasFollows(typeAndFlags)) {
                throw new RSocketProtocolError("Best-effort WebTransport datagram must contain one complete REQUEST_FNF");
            }
            return;
        }
        if (record.kind === RSocketWebTransportStreamKind.CONTROL) {
            if (!isControlFrame(type, streamId)) {
                throw new RSocketProtocolError("WebTransport control stream received a non-control frame");
            }
            return;
        }
        if (record.kind === RSocketWebTransportStreamKind.RELIABLE) {
            if (type === FrameType.METADATA_PUSH && streamId === 0) return;
            if (type === FrameType.REQUEST_FNF && streamId !== 0) {
                if (hasFollows(typeAndFlags)) this.inboundFragmentedFnf.add(streamId);
                return;
            }
            if (type === FrameType.PAYLOAD && this.inboundFragmentedFnf.has(streamId)) {
                if (!hasFollows(typeAndFlags)) this.inboundFragmentedFnf.delete(streamId);
                return;
            }
            throw new RSocketProtocolError("WebTransport reliable uni stream received an invalid frame");
        }
        if (!isInteractionFrame(type, streamId)) {
            throw new RSocketProtocolError("WebTransport interaction stream received an invalid frame");
        }
        const interaction = record.interaction;
        if (interaction === undefined) {
            throw new RSocketProtocolError("WebTransport interaction stream has no return lane");
        }
        if (interaction.streamId === undefined) {
            interaction.streamId = streamId;
            const current = this.interactionLanes.get(streamId);
            if (current === undefined) {
                this.interactionLanes.set(streamId, {
                    lane: interaction.lane,
                    type: initialInteractionType(type),
                    localComplete: false,
                    remoteComplete: false
                });
            } else if (current.lane !== interaction.lane) {
                interaction.lane.finish();
                this.activeLanes.delete(interaction.lane);
            }
        } else if (interaction.streamId !== streamId) {
            throw new RSocketProtocolError("WebTransport interaction stream mixed multiple RSocket stream IDs");
        }
    }

    /** Sends one FNF reliably, or opportunistically through a bounded datagram. */
    private writeFireAndForget(frame: Uint8Array, streamId: number, typeAndFlags: number): void {
        if (streamId === 0) throw new RSocketProtocolError("REQUEST_FNF requires a non-zero stream ID");
        if (hasFollows(typeAndFlags)) {
            this.outboundFragmentedFnf.add(streamId);
            this.writeReliable(frame);
            return;
        }
        if (!this.unreliableFireAndForget || this.session.datagrams === undefined) {
            this.writeReliable(frame);
            return;
        }
        const ordinal = this.takeOrdinal();
        const packet = encodeWebTransportDatagram(
            RSocketWebTransportDatagramKind.FIRE_AND_FORGET,
            frame,
            RSOCKET_WEBTRANSPORT_FNF_DATAGRAM_HEADER_LENGTH
        );
        writeWebTransportOrdinal(packet, RSOCKET_WEBTRANSPORT_PREFACE_LENGTH, ordinal);
        if (!this.fitsDatagram(packet.byteLength)) {
            this.writeReliableAt(ordinal, frame);
            return;
        }
        this.ensureDatagramLane().enqueue(packet, () => this.writeSkipMarker(ordinal, streamId));
    }

    /** Emits a marker after native datagram submission so a lost FNF cannot stall order. */
    private writeSkipMarker(ordinal: bigint, streamId: number): void {
        if (!this.isOpen) return;
        try {
            this.ensureControlLane().enqueue(encodeWebTransportSkipRecord(ordinal, streamId));
        } catch (error) {
            this.signalFailure(new RSocketConnectionError("WebTransport FNF marker write failed", error));
        }
    }

    /** Routes one globally ordered control record. */
    private writeControl(frame: Uint8Array): void {
        this.ensureControlLane().enqueue(encodeWebTransportRecord(
            this.takeOrdinal(),
            frame,
            this.maxFrameLength
        ));
    }

    /** Routes one globally ordered FNF or metadata frame to the reliable uni lane. */
    private writeReliable(frame: Uint8Array): void {
        this.writeReliableAt(this.takeOrdinal(), frame);
    }

    /** Routes a frame after an ordinal was reserved for an oversized datagram fallback. */
    private writeReliableAt(ordinal: bigint, frame: Uint8Array): void {
        this.ensureReliableLane().enqueue(encodeWebTransportRecord(
            ordinal,
            frame,
            this.maxFrameLength
        ));
    }

    /** Routes one globally ordered stream-scoped frame to its interaction bidi. */
    private writeInteraction(
        frame: Uint8Array,
        streamId: number,
        type: FrameType,
        typeAndFlags: number
    ): void {
        let state = this.interactionLanes.get(streamId);
        if (state === undefined) {
            const lane = this.createInteractionLane(streamId);
            state = {
                lane,
                type: initialInteractionType(type),
                localComplete: false,
                remoteComplete: false
            };
            this.interactionLanes.set(streamId, state);
        }
        state.lane.enqueue(encodeWebTransportRecord(
            this.takeOrdinal(),
            frame,
            this.maxFrameLength
        ));
        this.trackInteractionLifecycle(streamId, type, typeAndFlags, true);
    }

    /** Lazily creates one locally initiated bidi stream for a logical interaction. */
    private createInteractionLane(streamId: number): RSocketWebTransportWriteLane {
        let lane: RSocketWebTransportWriteLane;
        const writable = Promise.resolve(this.session.createBidirectionalStream(WAIT_FOR_STREAM_CREDIT)).then((stream) => {
            const interaction: InboundInteraction = {lane, streamId};
            this.consumeReliable(
                stream.readable,
                RSocketWebTransportStreamKind.INTERACTION,
                undefined,
                false,
                false,
                () => interaction
            );
            return stream.writable;
        });
        lane = this.trackLane(new RSocketWebTransportWriteLane(
            writable,
            encodeWebTransportStreamPreface(RSocketWebTransportStreamKind.INTERACTION),
            (error) => this.signalFailure(new RSocketConnectionError("WebTransport interaction write failed", error))
        ));
        return lane;
    }

    /** Lazily creates the persistent reliable uni stream. */
    private ensureReliableLane(): RSocketWebTransportWriteLane {
        const current = this.reliableLane;
        if (current !== undefined && !current.isClosed) return current;
        const lane = this.trackLane(new RSocketWebTransportWriteLane(
            Promise.resolve(this.session.createUnidirectionalStream(WAIT_FOR_STREAM_CREDIT)),
            encodeWebTransportStreamPreface(RSocketWebTransportStreamKind.RELIABLE),
            (error) => this.signalFailure(new RSocketConnectionError("WebTransport reliable write failed", error))
        ));
        this.reliableLane = lane;
        return lane;
    }

    /** Lazily acquires the session's datagram writer. */
    private ensureDatagramLane(): RSocketWebTransportWriteLane {
        const current = this.datagramLane;
        if (current !== undefined && !current.isClosed) return current;
        const datagrams = this.session.datagrams;
        if (datagrams === undefined) throw new RSocketConnectionError("WebTransport datagrams are unavailable");
        if (this.session.reliability === "reliable-only") {
            throw new RSocketConnectionError("WebTransport session does not support unreliable datagrams");
        }
        const writable = datagrams.createWritable?.() ?? datagrams.writable;
        if (writable === undefined) {
            throw new RSocketConnectionError("WebTransport datagram writer is unavailable");
        }
        const lane = this.trackLane(new RSocketWebTransportWriteLane(
            writable,
            undefined,
            ignoreRejection
        ));
        this.datagramLane = lane;
        return lane;
    }

    /** Returns the control lane owned by the requester and accepted by the responder. */
    private ensureControlLane(): RSocketWebTransportWriteLane {
        const lane = this.controlLane;
        if (lane === undefined || lane.isClosed) {
            throw new RSocketConnectionError("WebTransport control stream is unavailable");
        }
        return lane;
    }

    /** Assigns the next physical-session ordering value. */
    private takeOrdinal(): bigint {
        const ordinal = this.nextOutboundOrdinal;
        this.nextOutboundOrdinal += 1n;
        return ordinal;
    }

    /** Handles one complete best-effort extension datagram. */
    private handleDatagram(packet: Uint8Array): void {
        const kind = decodeWebTransportDatagramPreface(packet);
        if (kind === RSocketWebTransportDatagramKind.MEDIA) {
            this.emitMedia?.(packet.subarray(RSOCKET_WEBTRANSPORT_PREFACE_LENGTH));
            return;
        }
        if (!this.unreliableFireAndForget) {
            throw new RSocketProtocolError("Peer used WebTransport best-effort FNF without negotiation");
        }
        if (packet.byteLength < RSOCKET_WEBTRANSPORT_FNF_DATAGRAM_HEADER_LENGTH + 6) {
            throw new RSocketProtocolError("Best-effort WebTransport FNF datagram is incomplete");
        }
        const frameLength = packet.byteLength - RSOCKET_WEBTRANSPORT_FNF_DATAGRAM_HEADER_LENGTH;
        if (frameLength > this.maxFrameLength) {
            throw new RSocketFrameSizeError(frameLength, this.maxFrameLength);
        }
        const ordinal = readWebTransportOrdinal(packet, RSOCKET_WEBTRANSPORT_PREFACE_LENGTH);
        this.acceptRecord(ordinal, {
            frame: packet.subarray(RSOCKET_WEBTRANSPORT_FNF_DATAGRAM_HEADER_LENGTH),
            kind
        });
    }

    /** Tracks completion and releases per-interaction lane maps promptly. */
    private trackInteractionLifecycle(
        streamId: number,
        type: FrameType,
        typeAndFlags: number,
        local: boolean
    ): void {
        const state = this.interactionLanes.get(streamId);
        if (state === undefined) return;
        state.type ??= initialInteractionType(type);
        if (type === FrameType.CANCEL || type === FrameType.ERROR) {
            this.releaseInteraction(streamId, state);
            return;
        }
        const complete = type === FrameType.PAYLOAD && hasComplete(typeAndFlags) ||
            type === FrameType.REQUEST_CHANNEL && hasComplete(typeAndFlags);
        if (!complete) return;
        if (state.type !== FrameType.REQUEST_CHANNEL) {
            this.releaseInteraction(streamId, state);
            return;
        }
        if (local) state.localComplete = true;
        else state.remoteComplete = true;
        if (state.localComplete && state.remoteComplete) this.releaseInteraction(streamId, state);
    }

    /** Closes and forgets a terminal interaction stream. */
    private releaseInteraction(streamId: number, state: InteractionState): void {
        if (this.interactionLanes.get(streamId) !== state) return;
        this.interactionLanes.delete(streamId);
        this.activeLanes.delete(state.lane);
        state.lane.finish();
    }

    /** Retains one lane for physical-session cleanup. */
    private trackLane(lane: RSocketWebTransportWriteLane): RSocketWebTransportWriteLane {
        this.activeLanes.add(lane);
        return lane;
    }

    /** Reports one terminal mapping failure and tears down native resources. */
    private signalFailure(error: unknown): void {
        if (this.failure !== undefined || this.closing) return;
        this.failure = error;
        this.closing = true;
        this.ready = false;
        this.failFrames?.(error);
        this.releaseResources(error);
        try {
            this.session.close({
                closeCode: WEBTRANSPORT_PROTOCOL_ERROR,
                reason: error instanceof Error ? error.message : "RSocket WebTransport mapping failed"
            });
        } catch {
            // The mapping error remains the authoritative failure.
        }
    }

    /** Cancels readers, aborts writers, and clears every retained interaction. */
    private releaseResources(reason: unknown): void {
        for (const active of this.activeReaders) {
            active.decoder.reset();
            try {
                const cancelled = active.reader.cancel?.(reason);
                if (cancelled !== undefined) void Promise.resolve(cancelled).catch(ignoreRejection);
            } catch {
                // Continue releasing sibling streams.
            }
        }
        this.activeReaders.clear();
        for (const reader of this.activeObjectReaders) {
            try {
                const cancelled = reader.cancel?.(reason);
                if (cancelled !== undefined) void Promise.resolve(cancelled).catch(ignoreRejection);
            } catch {
                // Continue releasing sibling aggregate readers.
            }
        }
        this.activeObjectReaders.clear();
        for (const lane of this.activeLanes) lane.abort(reason);
        this.activeLanes.clear();
        this.interactionLanes.clear();
        this.outboundFragmentedFnf.clear();
        this.inboundFragmentedFnf.clear();
        this.ordered.clear();
        this.controlLane = undefined;
        this.reliableLane = undefined;
        this.datagramLane = undefined;
    }

    /** Rejects a datagram larger than the session's effective outgoing limit. */
    private assertDatagramLength(length: number): void {
        const limit = outgoingDatagramLimit(this.session);
        if (limit !== undefined && length > limit) throw new RSocketFrameSizeError(length, limit);
    }

    /** Whether an opportunistic FNF packet fits the current datagram path. */
    private fitsDatagram(length: number): boolean {
        const limit = outgoingDatagramLimit(this.session);
        return limit === undefined || length <= limit;
    }
}

/** Creates the shared transport contract around a requester or responder session. */
export function createWebTransportConnection(
    session: RSocketWebTransportSession,
    options: RSocketWebTransportConnectionOptions
): ReactiveWebTransportConnection {
    return new ReactiveWebTransportConnection(session, options);
}

/** Whether one frame belongs to the connection-control bidi stream. */
function isControlFrame(type: FrameType, streamId: number): boolean {
    if (streamId !== 0) return false;
    return type === FrameType.SETUP ||
        type === FrameType.KEEPALIVE ||
        type === FrameType.LEASE ||
        type === FrameType.RESUME ||
        type === FrameType.RESUME_OK ||
        type === FrameType.ERROR ||
        type === FrameType.EXT;
}

/** Whether one frame belongs to a per-interaction bidi stream. */
function isInteractionFrame(type: FrameType, streamId: number): boolean {
    if (streamId === 0) return false;
    return type === FrameType.REQUEST_RESPONSE ||
        type === FrameType.REQUEST_STREAM ||
        type === FrameType.REQUEST_CHANNEL ||
        type === FrameType.REQUEST_N ||
        type === FrameType.CANCEL ||
        type === FrameType.PAYLOAD ||
        type === FrameType.ERROR ||
        type === FrameType.EXT;
}

/** Returns an initial model or leaves follow-up-only replay traffic unresolved. */
function initialInteractionType(type: FrameType): FrameType | undefined {
    return type === FrameType.REQUEST_RESPONSE ||
        type === FrameType.REQUEST_STREAM ||
        type === FrameType.REQUEST_CHANNEL
        ? type
        : undefined;
}

/** Whether an initial or continuation fragment promises another fragment. */
function hasFollows(typeAndFlags: number): boolean {
    return (typeAndFlags & FireAndForgetFlag.FOLLOWS) !== 0;
}

/** Whether request-channel or PAYLOAD flags terminate one direction. */
function hasComplete(typeAndFlags: number): boolean {
    return (typeAndFlags & PayloadFlag.COMPLETE) !== 0;
}

/** Enforces a frame's fixed connection or interaction stream scope. */
function assertStreamId(actual: number, expected: number, frame: string): void {
    if (actual !== expected) throw new RSocketProtocolError(`${frame} requires stream ID ${expected}`);
}

/** Validates the protocol frame limit once. */
function normalizeFrameLength(value: number | undefined): number {
    const length = value ?? DEFAULT_MAX_FRAME_LENGTH;
    if (Number.isInteger(length) && length >= 6 && length <= DEFAULT_MAX_FRAME_LENGTH) return length;
    throw new RSocketFrameSizeError(length, DEFAULT_MAX_FRAME_LENGTH);
}

/** Computes and validates a bounded out-of-order memory budget. */
function normalizeReorderBytes(value: number | undefined, maxFrameLength: number): number {
    const fallback = Math.max(MIN_REORDER_BUFFER_BYTES, maxFrameLength * 2);
    const bytes = value ?? fallback;
    if (Number.isSafeInteger(bytes) && bytes >= maxFrameLength) return bytes;
    throw new RSocketProtocolError(
        `RSocket WebTransport maxReorderBufferBytes must be at least ${maxFrameLength}`
    );
}

/** Returns the outgoing datagram cap exposed by current and early implementations. */
function outgoingDatagramLimit(session: RSocketWebTransportSession): number | undefined {
    const datagrams = session.datagrams;
    const value = datagrams?.maxDatagramSize ?? datagrams?.outgoingMaxDatagramSize ?? session.maxDatagramSize;
    return typeof value === "number" && Number.isFinite(value) && value >= 0 ? value : undefined;
}

/** Waits for session readiness with requester timeout and cancellation support. */
function waitForReady(
    session: RSocketWebTransportSession,
    timeoutMs: number | undefined,
    signal: AbortSignal | undefined
): Promise<void> {
    if (signal?.aborted) return Promise.reject(new RSocketConnectionError("WebTransport handshake aborted"));
    if (timeoutMs === undefined && signal === undefined) return Promise.resolve(session.ready);
    return new Promise<void>((resolve, reject) => {
        let settled = false;
        let timer: ReturnType<typeof setTimeout> | undefined;
        const cleanup = (): void => {
            if (timer !== undefined) clearTimeout(timer);
            signal?.removeEventListener("abort", abort);
        };
        const settle = (error?: unknown): void => {
            if (settled) return;
            settled = true;
            cleanup();
            if (error === undefined) resolve();
            else reject(error);
        };
        const abort = (): void => settle(new RSocketConnectionError("WebTransport handshake aborted"));
        signal?.addEventListener("abort", abort, {once: true});
        if (timeoutMs !== undefined) {
            if (!Number.isInteger(timeoutMs) || timeoutMs <= 0) {
                settle(new RSocketConnectionError("WebTransport timeout must be a positive integer"));
                return;
            }
            timer = setTimeout(() => settle(new RSocketConnectionError(
                `WebTransport handshake timed out after ${timeoutMs}ms`
            )), timeoutMs);
        }
        void Promise.resolve(session.ready).then(() => settle(), settle);
    });
}

/** Releases a WHATWG reader lock without hiding an earlier outcome. */
function releaseReader<T>(reader: RSocketWebTransportReader<T>): void {
    try {
        reader.releaseLock?.();
    } catch {
        // A custom stream may release its lock as it completes.
    }
}

/** Handles intentionally detached cleanup and readiness promises. */
function ignoreRejection(): void {
}
