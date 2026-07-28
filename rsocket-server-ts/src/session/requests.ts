/** Pure helpers for controller dispatch and request context construction. */
import {FrameErrorCode, FrameType} from "rsocket-frames-ts";
import {
    decodeFramePayload,
    type InitialRequestFrame,
    RSocketProtocolError
} from "rsocket-core-ts";
import type {ControllerKind} from "@/controllers/registry.js";
import type {RSocketRequestContext} from "@/controllers/types.js";
import {RSocketRequestError} from "@/errors/index.js";
import type {RSocketServerConnection} from "@/server/connection.js";

/** Builds a readonly-typed request context from a fully decoded initial frame. */
export function requestContext(
    frame: InitialRequestFrame,
    connection: RSocketServerConnection,
    route: readonly string[]
): RSocketRequestContext {
    const context = decodeFramePayload(frame) as ReturnType<typeof decodeFramePayload> & {
        connection: RSocketServerConnection;
        streamId: number;
        route: readonly string[];
    };
    context.connection = connection;
    context.streamId = frame.header.streamId;
    context.route = route;
    return context as RSocketRequestContext;
}

/** Maps initial request frame types to independent controller namespaces. */
export function interactionKind(type: FrameType): ControllerKind {
    switch (type) {
        case FrameType.REQUEST_FNF:
            return "fire-and-forget";
        case FrameType.REQUEST_RESPONSE:
            return "request-response";
        case FrameType.REQUEST_STREAM:
            return "request-stream";
        case FrameType.REQUEST_CHANNEL:
            return "request-channel";
        default:
            throw new RSocketProtocolError("Frame is not an initial RSocket request");
    }
}

/** Uses explicit controller codes and defaults other failures to APPLICATION_ERROR. */
export function requestErrorCode(error: unknown): FrameErrorCode {
    return error instanceof RSocketRequestError ? error.code : FrameErrorCode.APPLICATION_ERROR;
}
