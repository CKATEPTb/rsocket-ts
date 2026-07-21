import type {ByteReader} from "bebyte";
import {MimeType} from "@/mimetype/MimeType";
import {Metadata} from "@/frame/context/Metadata";
import {createReader, createWriter} from "@/binary";

/**
 * Bit-level flags describing Zipkin tracing metadata behavior.
 */
export type TracingZipkinFlags = {
    /** Indicates if trace/span/parent IDs are present. */
    idsSet: boolean;
    /** Forces tracing regardless of sampling. */
    debug: boolean;
    /** Marks the trace as sampled (ignored if debug is true). */
    sampled: boolean;
    /** Explicitly marks trace as not sampled (ignored if sampled/debug is true). */
    notSampled: boolean;
    /** Enables 128-bit trace ID (if false, trace ID is 64-bit). */
    traceId128: boolean;
    /** If true, includes parent span ID in metadata. */
    hasParent: boolean;
};

/**
 * Represents a deserialized payload for Zipkin Tracing metadata.
 *
 * @property {TracingZipkinFlags} flags - Bit flags affecting encoding behavior.
 * @property {bigint | [bigint, bigint]} traceId - 64- or 128-bit Trace ID.
 * @property {bigint} spanId - Unique identifier for the span.
 * @property {bigint=} parentSpanId - Optional parent span ID.
 */
export type TracingZipkinPayload = {
    flags: TracingZipkinFlags;
    traceId?: bigint | [bigint, bigint];
    spanId?: bigint;
    parentSpanId?: bigint;
};

/**
 * # Tracing (Zipkin) Metadata Extension
 *
 * _This extension specification is currently incubating.  While incubating the version is 0._
 *
 * ## Introduction
 * Observability and tracing are key requirements for robust and reliable applications.  When using distributed applications connected with RSocket, it's important to propagate metadata about the current logical operations throughout the entire system.
 * One of the most popular systems for doing this kind of tracing is [Zipkin]{@link https://zipkin.io}.  This extension specification provides an interoperable structure for Zipkin metadata payloads to contain tracing information.  It is designed such that systems can efficently communicate span and trace information to a Zipkin server and propagate that information throughout a distributed system.
 *
 * ## Metadata Payload
 * This metadata type is intended to be used per stream, and not per connection nor individual payloads and as such it **MUST** only be used in frame types used to initiate interactions and payloads.  This includes [`REQUEST_FNF`]{@link RequestFireAndForgetFrame}, [`REQUEST_RESPONSE`]{@link RequestResponseFrame}, [`REQUEST_STREAM`]{@link RequestStreamFrame}, [`REQUEST_CHANNEL`]{@link RequestChannelFrame}, and [`PAYLOAD`]{@link PayloadFrame}.
 * The Metadata MIME Type is `message/x.rsocket.tracing-zipkin.v0`.
 *
 * ### Metadata Contents
 * ```
 *      0                   1                   2                   3
 *      0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
 *     +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 *     |I|D|S|N|T|P|   |
 *     +-+-+---+---+---+-----------------------------------------------+
 *     |                                                               |
 *     +                                                               +
 *     |                                                               |
 *     +                           Trace ID                            +
 *     |                                                               |
 *     +                                                               +
 *     |                                                               |
 *     +---------------------------------------------------------------+
 *     |                                                               |
 *     +                           Span ID                             +
 *     |                                                               |
 *     +---------------------------------------------------------------+
 *     |                                                               |
 *     +                        Parent Span ID                         +
 *     |                                                               |
 *     +---------------------------------------------------------------+
 * ```
 *
 * * **Flags**: (8 bits)
 *   * (**I**)Ds Set: When zero, the metadata only includes sampling information and no IDs
 *     * For example, a health check might set only the (**N**)ot Sampled flag without generating IDs
 *   * (**D**)ebug: Tracing payload should be force traced.
 *   * (**S**)ample: Tracing payload should be accepted for tracing. (Ignored when D flag is set.)
 *   * (**N**)ot Sampled: Tracing payload should not be sampled. (Ignored when S flag or D flag is set.)
 *   * (**T**)race Id Size: Unset indicates that the Trace Id is 64-bit. Set indicates that the Trace Id is 128-bit.
 *   * (**P**)arent Span Id: Tracing payload contains a parent span id.
 * * **Trace ID**: (64 or 128 bits) Unsigned 64- or 128-bit integer ID of the trace. Every span in a trace shares this ID.
 * * **Span ID**: (64 bits) Unsigned 64-bit integer ID for a particular span. This may or may not be the same as the trace id.
 * * **Parent Span ID**: (64 bits) Unsigned 64-bit integer ID for a particular parent span.  This is an optional ID that will only be present on child spans. That is the span without a parent id is considered the root of the trace. (Not present if P flag is not set)
 */
export class RSocketTracingZipkin extends MimeType<TracingZipkinPayload> {
    /**
     * Serializes a structured tracing payload into a binary metadata block.
     *
     * @param {TracingZipkinPayload} payload - The tracing data to encode.
     * @returns {Metadata<TracingZipkinPayload>} A Metadata object with a `toUint8Array()` method.
     */
    protected override serializeMetadata(payload: TracingZipkinPayload): Metadata<TracingZipkinPayload> {
        const writer = createWriter();
        let flags = 0;
        if (payload.flags.idsSet) flags |= 128;
        if (payload.flags.debug) flags |= 64;
        if (payload.flags.sampled) flags |= 32;
        if (payload.flags.notSampled) flags |= 16;
        if (payload.flags.traceId128) flags |= 8;
        if (payload.flags.hasParent) flags |= 4;
        writer.i8(flags);

        if (payload.flags.idsSet) {
            if (payload.flags.traceId128 && Array.isArray(payload.traceId)) {
                writer.i64(payload.traceId[0]);
                writer.i64(payload.traceId[1]);
            } else if (!payload.flags.traceId128 && typeof payload.traceId === "bigint") {
                writer.i64(payload.traceId);
            } else {
                throw new TypeError("traceId must match the traceId128 flag when IDs are set");
            }
            if (payload.spanId === undefined) {
                throw new TypeError("spanId is required when tracing IDs are set");
            }
            writer.i64(payload.spanId);
            if (payload.flags.hasParent) {
                if (payload.parentSpanId === undefined) {
                    throw new TypeError("parentSpanId is required when hasParent is set");
                }
                writer.i64(payload.parentSpanId);
            }
        }

        return new Metadata(this, payload, writer.toUint8Array());
    }

    /**
     * Deserializes a binary tracing metadata payload into a structured object.
     *
     * @param {ByteReader} reader - Byte reader to extract metadata from.
     * @param {boolean} [hasPayload=true] - Whether metadata is prefixed with a length (i24).
     * @returns {Metadata<TracingZipkinPayload>} Parsed tracing metadata.
     */
    protected override deserializeMetadata(reader: ByteReader, hasPayload: boolean = true): Metadata<TracingZipkinPayload> {
        const array = hasPayload ? reader.viewBytes(reader.i24()) : reader.viewRemaining();
        const r = createReader(array);

        const flagByte = r.i8();
        const flags: TracingZipkinFlags = {
            idsSet: (flagByte & 128) === 128,
            debug: (flagByte & 64) === 64,
            sampled: (flagByte & 32) === 32,
            notSampled: (flagByte & 16) === 16,
            traceId128: (flagByte & 8) === 8,
            hasParent: (flagByte & 4) === 4
        };

        let traceId: bigint | [bigint, bigint] | undefined;
        let spanId: bigint | undefined;
        let parentSpanId: bigint | undefined;
        if (flags.idsSet) {
            traceId = flags.traceId128 ? [r.i64(), r.i64()] : r.i64();
            spanId = r.i64();
            if (flags.hasParent) parentSpanId = r.i64();
        }
        if (r.offset !== array.length) {
            throw new RangeError(`Tracing metadata contains ${array.length - r.offset} unexpected trailing byte(s)`);
        }

        const decoded: TracingZipkinPayload = {flags};
        if (traceId !== undefined) decoded.traceId = traceId;
        if (spanId !== undefined) decoded.spanId = spanId;
        if (parentSpanId !== undefined) decoded.parentSpanId = parentSpanId;
        return new Metadata(this, decoded, array);
    }
}
