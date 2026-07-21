import {MimeType} from "@/mimetype/MimeType";
import {RSocketMimeType, RSocketMimeTypes} from "@/mimetype/message/RSocketMimeType";
import {RSocketRouting} from "@/mimetype/message/RSocketRouting";
import {RSocketComposite} from "@/mimetype/message/RSocketComposite";
import {RSocketTracingZipkin} from "@/mimetype/message/RSocketTracingZipkin";
import {RSocketAuth} from "@/mimetype/message/security/RSocketAuth";
import {AuthType, WellKnownAuthType} from "@/mimetype/message/security";
import {ApplicationJson} from "@/mimetype/ApplicationJson";
import {TextPlain} from "@/mimetype/TextPlain";

export {
    MimeType, WellKnownAuthType, AuthType
}
/**
 * Namespace containing predefined and registered well-known MIME types,
 * including both standard media types and RSocket-specific types.
 */
export namespace WellKnownMimeType {
    /** AVRO encoded binary data. */
    export const APPLICATION_AVRO = new MimeType('application/avro', 0x00)
    /** Concise Binary Object Representation (CBOR). */
    export const APPLICATION_CBOR = new MimeType('application/cbor', 0x01)
    /** GraphQL query format. */
    export const APPLICATION_GRAPHQL = new TextPlain('application/graphql', 0x02)
    /** GZIP compressed data. */
    export const APPLICATION_GZIP = new MimeType('application/gzip', 0x03)
    /** JavaScript source code. */
    export const APPLICATION_JAVASCRIPT = new TextPlain('application/javascript', 0x04)
    /** JSON encoded data. */
    export const APPLICATION_JSON = new ApplicationJson('application/json', 0x05)
    /** Binary stream format. */
    export const APPLICATION_OCTET_STREAM = new MimeType('application/octet-stream', 0x06)
    /** Portable Document Format. */
    export const APPLICATION_PDF = new MimeType('application/pdf', 0x07)
    /** Apache Thrift binary protocol. */
    export const APPLICATION_THRIFT = new MimeType('application/vnd.apache.thrift.binary', 0x08)
    /** Google Protocol Buffers format. */
    export const APPLICATION_PROTOBUF = new MimeType('application/vnd.google.protobuf', 0x09)
    /** XML encoded data. */
    export const APPLICATION_XML = new TextPlain('application/xml', 0x0A)
    /** ZIP compressed archive. */
    export const APPLICATION_ZIP = new MimeType('application/zip', 0x0B)
    /** Audio encoded using aac Coding. */
    export const AUDIO_AAC = new MimeType('audio/aac', 0x0C)
    /** Audio encoded using mp3 Coding. */
    export const AUDIO_MP3 = new MimeType('audio/mp3', 0x0D)
    /** Audio encoded using mp4 Coding. */
    export const AUDIO_MP4 = new MimeType('audio/mp4', 0x0E)
    /** Audio encoded using mpeg3 Coding. */
    export const AUDIO_MPEG3 = new MimeType('audio/mpeg3', 0x0F)
    /** Audio encoded using mpeg Coding. */
    export const AUDIO_MPEG = new MimeType('audio/mpeg', 0x10)
    /** Audio encoded using ogg Coding. */
    export const AUDIO_OGG = new MimeType('audio/ogg', 0x11)
    /** Audio encoded using opus Coding. */
    export const AUDIO_OPUS = new MimeType('audio/opus', 0x12)
    /** Audio encoded using vorbis Coding. */
    export const AUDIO_VORBIS = new MimeType('audio/vorbis', 0x13)
    /** Bitmap image. */
    export const IMAGE_BMP = new MimeType('image/bmp', 0x14)
    /** GIF image. */
    export const IMAGE_GIF = new MimeType('image/gif', 0x15)
    /** HEIC image sequence. */
    export const IMAGE_HEIC_SEQUENCE = new MimeType('image/heic-sequence', 0x16)
    /** HEIC image. */
    export const IMAGE_HEIC = new MimeType('image/heic', 0x17)
    /** HEIF image sequence. */
    export const IMAGE_HEIF_SEQUENCE = new MimeType('image/heif-sequence', 0x18)
    /** HEIF image. */
    export const IMAGE_HEIF = new MimeType('image/heif', 0x19)
    /** JPEG image. */
    export const IMAGE_JPEG = new MimeType('image/jpeg', 0x1A)
    /** PNG image. */
    export const IMAGE_PNG = new MimeType('image/png', 0x1B)
    /** TIFF image. */
    export const IMAGE_TIFF = new MimeType('image/tiff', 0x1C)
    /** MIME multipart/mixed content. */
    export const MULTIPART_MIXED = new MimeType('multipart/mixed', 0x1D)
    /** CSS stylesheet. */
    export const TEXT_CSS = new TextPlain('text/css', 0x1E)
    /** CSV (Comma Separated Values) text. */
    export const TEXT_CSV = new TextPlain('text/csv', 0x1F)
    /** HTML document. */
    export const TEXT_HTML = new TextPlain('text/html', 0x20)
    /** Plain text. */
    export const TEXT_PLAIN = new TextPlain('text/plain', 0x21)
    /** XML text. */
    export const TEXT_XML = new TextPlain('text/xml', 0x22)
    /** H.264 video stream. */
    export const VIDEO_H264 = new MimeType('video/H264', 0x23)
    /** H.265 video stream. */
    export const VIDEO_H265 = new MimeType('video/H265', 0x24)
    /** VP8 video stream. */
    export const VIDEO_VP8 = new MimeType('video/VP8', 0x25)
    /** Hessian binary protocol. */
    export const APPLICATION_HESSIAN = new MimeType('application/x-hessian', 0x26)
    /** Serialized Java objects. */
    export const APPLICATION_JAVA_OBJECT = new MimeType('application/x-java-object', 0x27)
    /** CloudEvents encoded in JSON. */
    export const APPLICATION_CLOUDEVENTS_JSON = new ApplicationJson('application/cloudevents+json', 0x28)
    /** Cap'n Proto serialization format. */
    export const APPLICATION_X_CAPNP        = new MimeType("application/x-capnp", 0x29)
    /** FlatBuffers serialization format. */
    export const APPLICATION_X_FLATBUFFERS  = new MimeType("application/x-flatbuffers", 0x2A)
    /** RSocket metadata for MIME type declarations. */
    export const MESSAGE_RSOCKET_MIMETYPE = new RSocketMimeType('message/x.rsocket.mime-type.v0', 0x7A)
    /** RSocket metadata for accepted MIME types. */
    export const MESSAGE_RSOCKET_ACCEPT_MIMETYPES = new RSocketMimeTypes('message/x.rsocket.accept-mime-types.v0', 0x7b)
    /** RSocket authentication metadata. */
    export const MESSAGE_RSOCKET_AUTHENTICATION = new RSocketAuth('message/x.rsocket.authentication.v0', 0x7C)
    /** RSocket tracing metadata using Zipkin format. */
    export const MESSAGE_RSOCKET_TRACING_ZIPKIN = new RSocketTracingZipkin('message/x.rsocket.tracing-zipkin.v0', 0x7D)
    /** RSocket routing metadata. */
    export const MESSAGE_RSOCKET_ROUTING = new RSocketRouting('message/x.rsocket.routing.v0', 0x7E)
    /** RSocket composite metadata format. */
    export const MESSAGE_RSOCKET_COMPOSITE_METADATA = new RSocketComposite('message/x.rsocket.composite-metadata.v0', 0x7F)
}