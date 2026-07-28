/** Stream-scoped request dispatch and interaction lifecycle for one server session. */
import {
    ErrorFrame,
    type Frame,
    FrameErrorCode,
    FrameType,
    Metadata,
    PayloadFrame,
    RequestChannelFrame,
    RequestFireAndForgetFrame,
    RequestNFrame,
    RequestResponseFrame,
    RequestStreamFrame,
    SetupFrame
} from "rsocket-frames-ts";
import {
    errorFromFrame,
    errorMessage,
    errorPayload,
    decodeInitialRequestFrame,
    IgnoredPayloadFragments,
    isInitialRequestFrame,
    type InitialRequestFrame,
    nextRSocketStreamId,
    type PayloadFragmentMap,
    RequestFragmentAssembler,
    reassemblePayloadFrame,
    routingTags,
    RSocketProtocolError
} from "rsocket-core-ts";
import {
    FireAndForgetController,
    RequestChannelController,
    RequestResponseController,
    RequestStreamController
} from "@/controllers/classes.js";
import {ControllerRegistry} from "@/controllers/registry.js";
import {RSocketRequestError} from "@/errors/index.js";
import {ServerBackgroundWork} from "@/session/background.js";
import {hasInitialChannelItem} from "@/session/protocol.js";
import {interactionKind, requestContext} from "@/session/requests.js";
import type {ResponderSession, ResponderStream} from "@/session/types.js";
import {
    PENDING_REQUEST_RESPONSE_STREAM,
    PendingResponderStream,
    RequestChannelResponder,
    RequestResponseResponder,
    RequestStreamResponder
} from "@/stream/index.js";

/** Owns all client-initiated stream IDs, fragments, and responder states. */
export class ServerInteractionDispatcher {
    private readonly streams = new Map<number, ResponderStream>();
    private readonly payloadFragments: PayloadFragmentMap = new Map();
    private readonly requestFragments = new RequestFragmentAssembler();
    private readonly ignoredPayloadFragments = new IgnoredPayloadFragments();
    private nextStreamId = 1;

    /** Binds interaction state to one logical session and negotiated SETUP. */
    constructor(
        private readonly session: ResponderSession,
        private readonly setup: SetupFrame,
        private readonly controllers: ControllerRegistry,
        private readonly background: ServerBackgroundWork
    ) {
    }

    /** Whether an initial request currently awaits continuation PAYLOAD frames. */
    hasRequestFragments(streamId: number): boolean {
        return this.requestFragments.has(streamId);
    }

    /** Whether an active stream currently awaits continuation PAYLOAD frames. */
    hasPayloadFragments(streamId: number): boolean {
        return this.payloadFragments.has(streamId);
    }

    /** Whether a stream ID belongs to active or partially reassembled work. */
    hasStream(streamId: number): boolean {
        return this.streams.has(streamId) ||
            this.requestFragments.has(streamId) ||
            this.payloadFragments.has(streamId) ||
            this.ignoredPayloadFragments.has(streamId);
    }

    /** Whether no active stream or incomplete initial request remains. */
    get isIdle(): boolean {
        return this.streams.size === 0 && this.requestFragments.isEmpty;
    }

    /** Handles stream-scoped frames and returns false for connection-level dispatch. */
    handle(frame: Frame): boolean {
        const streamId = frame.header.streamId;
        switch (frame.type) {
            case FrameType.REQUEST_RESPONSE:
            case FrameType.REQUEST_FNF:
            case FrameType.REQUEST_STREAM:
            case FrameType.REQUEST_CHANNEL:
                this.handleInitialRequest(frame as InitialRequestFrame);
                return true;
            case FrameType.REQUEST_N:
                if (streamId <= 0) return true;
                this.streams.get(streamId)?.handleRequestN((frame as RequestNFrame).request);
                return true;
            case FrameType.CANCEL:
                if (streamId <= 0) return true;
                this.handleCancel(streamId);
                return true;
            case FrameType.PAYLOAD:
                if (streamId <= 0) return true;
                this.handlePayload(frame as PayloadFrame);
                return true;
            case FrameType.ERROR:
                if (streamId === 0) return false;
                this.handleError(streamId, frame as ErrorFrame);
                return true;
            default:
                return false;
        }
    }

    /** Removes one terminal interaction and all retained fragments. */
    unregister(streamId: number): void {
        this.streams.delete(streamId);
        this.payloadFragments.delete(streamId);
        this.requestFragments.delete(streamId);
        this.ignoredPayloadFragments.delete(streamId);
    }

    /** Terminates one interaction locally before its ERROR frame is sent. */
    terminate(streamId: number, error: unknown): void {
        this.streams.get(streamId)?.terminate(error);
        this.unregister(streamId);
    }

    /** Terminates every interaction and releases all fragment storage. */
    close(error: unknown): void {
        for (const stream of this.streams.values()) stream.terminate(error);
        this.streams.clear();
        this.payloadFragments.clear();
        this.requestFragments.clear();
        this.ignoredPayloadFragments.clear();
    }

    /** Ignores one metadata-length violation while preserving stream sequencing. */
    ignoreInvalidMetadataFrame(streamId: number, frameType: FrameType, typeAndFlags: number): void {
        if (isInitialRequestFrame(frameType)) {
            if (!this.validateNewStreamId(streamId)) return;
            this.nextStreamId = nextRSocketStreamId(streamId, 1);
            this.session.acquireRequest();
            this.ignoredPayloadFragments.update(streamId, typeAndFlags);
            return;
        }
        if (frameType !== FrameType.PAYLOAD) return;
        if (this.requestFragments.has(streamId)) {
            this.session.unregister(streamId);
            return;
        }
        this.payloadFragments.delete(streamId);
        this.ignoredPayloadFragments.update(streamId, typeAndFlags);
    }

    /** Ignores a continuation belonging to an already-discarded fragmented payload. */
    consumeIgnoredPayloadFragment(streamId: number, typeAndFlags: number): boolean {
        return this.ignoredPayloadFragments.consume(streamId, typeAndFlags);
    }

    /** Advances request sequencing when a best-effort FNF datagram was lost. */
    skipFireAndForget(streamId: number): void {
        if (!this.validateNewStreamId(streamId)) return;
        this.nextStreamId = nextRSocketStreamId(streamId, 1);
        this.session.acquireRequest();
    }

    /** Validates a new odd stream ID and buffers a fragmented initial request. */
    private handleInitialRequest(frame: InitialRequestFrame): void {
        const streamId = frame.header.streamId;
        if (!this.validateNewStreamId(streamId)) return;
        this.nextStreamId = nextRSocketStreamId(streamId, 1);
        const rejection = this.session.acquireRequest();
        if (rejection !== undefined) {
            this.session.send(new ErrorFrame(
                streamId,
                FrameErrorCode.REJECTED,
                errorPayload(rejection)
            ));
            this.session.unregister(streamId);
            return;
        }
        if (frame.hasFollows()) {
            this.requestFragments.start(frame);
            return;
        }
        this.decodeAndDispatchInitialRequest(frame);
    }

    /** Decodes application MIME content after stream admission is established. */
    private decodeAndDispatchInitialRequest(frame: InitialRequestFrame): void {
        try {
            this.dispatchInitialRequest(decodeInitialRequestFrame(
                frame,
                this.setup.metadataType,
                this.setup.dataType
            ));
        } catch (error) {
            this.failInvalidRequest(frame.header.streamId, frame.type, error);
        }
    }

    /** Dispatches one complete request to an exact controller route. */
    private dispatchInitialRequest(frame: InitialRequestFrame): void {
        const streamId = frame.header.streamId;
        const route = routingTags(frame.metadata as Metadata<any> | undefined);
        const kind = interactionKind(frame.type);
        const registration = this.controllers.resolve(kind, route);
        if (registration === undefined) {
            if (!(frame instanceof RequestFireAndForgetFrame)) {
                this.session.send(new ErrorFrame(
                    streamId,
                    FrameErrorCode.REJECTED,
                    errorPayload(`No ${kind} controller is registered for route ${route.join(" / ") || "<empty>"}`)
                ));
            }
            this.session.unregister(streamId);
            return;
        }
        const context = requestContext(frame, this.session.connection, registration.route);
        const controller = registration.controller;
        if (frame instanceof RequestFireAndForgetFrame) {
            try {
                const result = (controller as FireAndForgetController<any, any>).handle(context.data, context);
                if (!this.session.isTerminated) this.background.consume(result);
            } catch {
                // Fire-and-forget has no response stream for application failures.
            } finally {
                this.session.unregister(streamId);
            }
            return;
        }
        try {
            if (frame instanceof RequestResponseFrame) {
                const pending = PENDING_REQUEST_RESPONSE_STREAM;
                this.streams.set(streamId, pending);
                const result = (controller as RequestResponseController<any, any, any, any>).handle(context.data, context);
                if (this.session.isTerminated || this.streams.get(streamId) !== pending) return;
                if (RequestResponseResponder.respondSynchronously(this.session, streamId, result)) return;
                const responder = new RequestResponseResponder(this.session, streamId, result);
                this.streams.set(streamId, responder);
                responder.start();
                return;
            }
            if (frame instanceof RequestStreamFrame) {
                const pending = this.reserveControllerStream(streamId);
                const source = (controller as RequestStreamController<any, any, any, any>).handle(context.data, context);
                if (this.session.isTerminated || this.streams.get(streamId) !== pending) return;
                const responder = new RequestStreamResponder(this.session, streamId, frame.request, source);
                this.streams.set(streamId, responder);
                const pendingDemand = pending.takeDemand();
                if (pendingDemand > 0) responder.handleRequestN(pendingDemand);
                responder.start();
                return;
            }
            const channelFrame = frame as RequestChannelFrame;
            const initial = hasInitialChannelItem(channelFrame, context) ? context : undefined;
            const responder = new RequestChannelResponder(
                this.session,
                streamId,
                channelFrame.request,
                initial,
                channelFrame.isComplete()
            );
            this.streams.set(streamId, responder);
            responder.startInput();
            if (this.session.isTerminated || responder.isTerminated) return;
            const source = (controller as RequestChannelController<any, any, any, any>).handle(
                responder.requests(),
                context
            );
            if (!this.session.isTerminated) responder.start(source);
        } catch (error) {
            this.session.streamError(streamId, error);
        }
    }

    /** Makes a stream visible before invoking synchronous application controller code. */
    private reserveControllerStream(streamId: number): PendingResponderStream {
        const pending = new PendingResponderStream(this.session, streamId);
        this.streams.set(streamId, pending);
        return pending;
    }

    /** Handles CANCEL for active or not-yet-reassembled initial requests. */
    private handleCancel(streamId: number): void {
        if (this.requestFragments.has(streamId)) {
            this.session.unregister(streamId);
            return;
        }
        const stream = this.streams.get(streamId);
        if (stream === PENDING_REQUEST_RESPONSE_STREAM) this.unregister(streamId);
        else stream?.handleCancel();
    }

    /** Reassembles request continuations or active-stream payload fragments. */
    private handlePayload(frame: PayloadFrame): void {
        const streamId = frame.header.streamId;
        if (this.requestFragments.has(streamId)) {
            const requestType = this.requestFragments.frameType(streamId);
            try {
                const request = this.requestFragments.continue(frame, this.setup.metadataType, this.setup.dataType);
                if (request !== undefined) this.dispatchInitialRequest(request);
            } catch (error) {
                this.failInvalidRequest(streamId, requestType, error);
            }
            return;
        }
        const stream = this.streams.get(streamId);
        if (stream === undefined) {
            this.payloadFragments.delete(streamId);
            return;
        }
        const continuation = this.payloadFragments.has(streamId);
        if (!stream.acceptPayloadFragment(frame, continuation)) {
            this.payloadFragments.delete(streamId);
            return;
        }
        let payload: PayloadFrame | undefined;
        try {
            payload = reassemblePayloadFrame(
                frame,
                this.payloadFragments,
                this.setup.metadataType,
                this.setup.dataType
            );
        } catch (error) {
            this.session.streamError(streamId, invalidRequestError(error));
            return;
        }
        if (payload === undefined) return;
        if (!payload.isNext() && !payload.isComplete()) {
            this.session.streamError(streamId, new RSocketRequestError(
                "PAYLOAD must set NEXT, COMPLETE, or both",
                FrameErrorCode.INVALID
            ));
            return;
        }
        stream.handlePayload(payload);
    }

    /** Drops invalid FNF input silently and rejects other malformed requests per stream. */
    private failInvalidRequest(streamId: number, type: FrameType | undefined, error: unknown): void {
        if (type === FrameType.REQUEST_FNF) {
            this.session.unregister(streamId);
            return;
        }
        this.session.streamError(streamId, invalidRequestError(error));
    }

    /** Applies a stream ERROR sent by the requester without echoing it. */
    private handleError(streamId: number, frame: ErrorFrame): void {
        if (this.requestFragments.has(streamId)) {
            this.session.unregister(streamId);
            return;
        }
        this.streams.get(streamId)?.handleError(errorFromFrame(frame));
    }

    /** Enforces client odd, sequential, locally unique stream identifiers. */
    private validateNewStreamId(streamId: number): boolean {
        while (this.hasStream(this.nextStreamId)) {
            this.nextStreamId = nextRSocketStreamId(this.nextStreamId, 1);
        }
        if (streamId <= 0 || streamId % 2 === 0) {
            throw new RSocketProtocolError("Client request must use a positive odd stream ID", {streamId});
        }
        if (this.streams.has(streamId) || this.requestFragments.has(streamId)) return false;
        if (streamId !== this.nextStreamId) {
            throw new RSocketProtocolError(
                `Client request used stream ID ${streamId}; expected ${this.nextStreamId}`,
                {streamId}
            );
        }
        return true;
    }

}

/** Converts application MIME and fragmented-sequence failures to ERROR[INVALID]. */
function invalidRequestError(error: unknown): RSocketRequestError {
    return error instanceof RSocketRequestError
        ? error
        : new RSocketRequestError(
            `Invalid RSocket request payload: ${errorMessage(error)}`,
            FrameErrorCode.INVALID,
            {cause: error}
        );
}
