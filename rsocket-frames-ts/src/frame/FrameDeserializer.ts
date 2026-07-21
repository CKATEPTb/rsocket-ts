import {rememberSerializedFrame, type Frame} from "@/frame/Frame";
import {FrameType} from "@/frame/FrameType";
import {SetupFrame} from "@/frame/wellknown/SetupFrame";
import {LeaseFrame} from "@/frame/wellknown/LeaseFrame";
import {KeepaliveFrame} from "@/frame/wellknown/KeepaliveFrame";
import {RequestResponseFrame} from "@/frame/wellknown/RequestResponseFrame";
import {RequestFireAndForgetFrame} from "@/frame/wellknown/RequestFireAndForgetFrame";
import {RequestStreamFrame} from "@/frame/wellknown/RequestStreamFrame";
import {RequestChannelFrame} from "@/frame/wellknown/RequestChannelFrame";
import {RequestNFrame} from "@/frame/wellknown/RequestNFrame";
import {CancelFrame} from "@/frame/wellknown/CancelFrame";
import {PayloadFrame} from "@/frame/wellknown/PayloadFrame";
import {ErrorFrame} from "@/frame/wellknown/ErrorFrame";
import {MetadataPushFrame} from "@/frame/wellknown/MetadataPushFrame";
import {ResumeFrame} from "@/frame/wellknown/ResumeFrame";
import {ResumeOkFrame} from "@/frame/wellknown/ResumeOkFrame";
import {ExtensionFrame} from "@/frame/wellknown/ExtensionFrame";
import {Header} from "@/frame/context/Header";
import type {MimeType} from "@/mimetype/MimeType";
import {createReader} from "@/binary";
import {assertFrameSize} from "@/frame/transport/framing";
import {UnknownFrame} from "@/frame/unknown";
import {FrameFlag} from "@/frame/FrameFlag";

/**
 * Deserializes a raw RSocket frame buffer into a strongly typed `Frame` object.
 *
 * This function reads the header to determine the `FrameType`, then delegates
 * to the appropriate frame-specific parser.
 *
 * @param {Uint8Array} buffer - The raw frame buffer to deserialize.
 * @param {MimeType<any>} metadataType - The metadata MIME type to use during decoding.
 * @param {MimeType<any>} payloadType - The payload MIME type to use during decoding.
 * @returns {Frame} A fully deserialized RSocket frame.
 * @throws {Error} If the frame type is unknown or unsupported.
 */
function deserialize(buffer: Uint8Array, metadataType: MimeType<any>, payloadType: MimeType<any>): Frame {
    assertFrameSize(buffer.length)
    const reader = createReader(buffer)
    const header = Header.from(reader)
    let frame: Frame
    switch (header.frameType) {
        case FrameType.RESERVED:
            frame = unknownFrame(header, reader)
            break
        case FrameType.SETUP:
            frame = SetupFrame.from(header, reader, metadataType, payloadType)
            break
        case FrameType.LEASE:
            frame = LeaseFrame.from(header, reader, metadataType, payloadType)
            break
        case FrameType.KEEPALIVE:
            frame = KeepaliveFrame.from(header, reader, metadataType, payloadType)
            break
        case FrameType.REQUEST_RESPONSE:
            frame = RequestResponseFrame.from(header, reader, metadataType, payloadType)
            break
        case FrameType.REQUEST_FNF:
            frame = RequestFireAndForgetFrame.from(header, reader, metadataType, payloadType)
            break
        case FrameType.REQUEST_STREAM:
            frame = RequestStreamFrame.from(header, reader, metadataType, payloadType)
            break
        case FrameType.REQUEST_CHANNEL:
            frame = RequestChannelFrame.from(header, reader, metadataType, payloadType)
            break
        case FrameType.REQUEST_N:
            frame = RequestNFrame.from(header, reader, metadataType, payloadType)
            break
        case FrameType.CANCEL:
            frame = CancelFrame.from(header, reader, metadataType, payloadType)
            break
        case FrameType.PAYLOAD:
            frame = PayloadFrame.from(header, reader, metadataType, payloadType)
            break
        case FrameType.ERROR:
            frame = ErrorFrame.from(header, reader, metadataType, payloadType)
            break
        case FrameType.METADATA_PUSH:
            frame = MetadataPushFrame.from(header, reader, metadataType, payloadType)
            break
        case FrameType.RESUME:
            frame = ResumeFrame.from(header, reader, metadataType, payloadType)
            break
        case FrameType.RESUME_OK:
            frame = ResumeOkFrame.from(header, reader, metadataType, payloadType)
            break
        case FrameType.EXT:
            frame = ExtensionFrame.from(header, reader, metadataType, payloadType)
            break
        default:
            frame = unknownFrame(header, reader)
    }
    if (reader.offset !== buffer.length) {
        throw new RangeError(`Frame ${FrameType[header.frameType]} contains ${buffer.length - reader.offset} unexpected trailing byte(s)`)
    }
    return frame
}

/** Decodes an unassigned frame only when the sender explicitly allows it to be ignored. */
function unknownFrame(header: Header, reader: ReturnType<typeof createReader>): UnknownFrame {
    if (!header.isFlagSet(FrameFlag.IGNORE)) {
        const type = Number(header.frameType).toString(16).padStart(2, "0");
        throw new RangeError(`Unknown RSocket frame type: 0x${type}`);
    }
    return UnknownFrame.from(header, reader);
}

/**
 * A utility for deserializing raw RSocket frames from binary format.
 *
 * Preserves the original bytes for zero-work re-serialization.
 */
export const FrameDeserializer = {
    /**
     * Deserializes the given buffer and retains its original wire bytes.
     *
     * @param {Uint8Array} buffer - The raw frame bytes to deserialize.
     * @param {MimeType<any>} metadataType - MIME type for metadata decoding.
     * @param {MimeType<any>} payloadType - MIME type for payload decoding.
     * @returns {Frame} Deserialized frame with `toUint8Array()` method.
     */
    deserialize: (buffer: Uint8Array, metadataType: MimeType<any>, payloadType: MimeType<any>): Frame => {
        return rememberSerializedFrame(deserialize(buffer, metadataType, payloadType), buffer)
    }
}
