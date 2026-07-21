import {
    CancelFrame,
    ErrorFrame,
    ExtensionFlag,
    ExtensionFrame,
    FireAndForgetFlag,
    Frame,
    FrameCodec,
    FrameErrorCode,
    FrameFlag,
    FrameType,
    KeepaliveFlag,
    KeepaliveFrame,
    LeaseFrame,
    Metadata,
    MetadataPushFrame,
    MimeType,
    Payload,
    PayloadFlag,
    PayloadFrame,
    RequestChannelFlag,
    RequestChannelFrame,
    RequestNFrame,
    RequestResponseFlag,
    RequestResponseFrame,
    RequestStreamFlag,
    RequestStreamFrame,
    RequestFireAndForgetFrame,
    ResumeFrame,
    ResumeOkFrame,
    SetupFlag,
    SetupFrame,
    type FrameTransport
} from "@/index";
import {binaryMimeType, metadata, payload} from "@test/specification/fixtures";

interface FrameVariant {
    readonly frame: Frame;
    readonly frameName: string;
    readonly variant: string;
    readonly flags: number;
}

const variants: FrameVariant[] = [
    variant("SETUP", "minimal", new SetupFrame(1, 1, binaryMimeType, binaryMimeType), FrameFlag.NONE),
    variant("SETUP", "lease", new SetupFrame(
        500,
        1_000,
        binaryMimeType,
        binaryMimeType,
        undefined,
        1,
        0,
        SetupFlag.LEASE
    ), SetupFlag.LEASE),
    variant("SETUP", "resume", new SetupFrame(
        500,
        1_000,
        binaryMimeType,
        binaryMimeType,
        "resume-token"
    ), SetupFlag.RESUME),
    variant("SETUP", "all optional fields", new SetupFrame(
        500,
        1_000,
        binaryMimeType,
        binaryMimeType,
        "resume-token",
        2,
        3,
        SetupFlag.LEASE,
        metadata(0xa1, 0xa2),
        payload(0xb1, 0xb2)
    ), SetupFlag.LEASE | SetupFlag.RESUME | SetupFlag.METADATA),

    variant("LEASE", "without metadata", new LeaseFrame(0, 0), FrameFlag.NONE),
    variant("LEASE", "with metadata", new LeaseFrame(1_000, 100, metadata(0xa1)), FrameFlag.METADATA),

    variant("KEEPALIVE", "acknowledgement", new KeepaliveFrame(), KeepaliveFlag.NONE),
    variant("KEEPALIVE", "respond with data", new KeepaliveFrame(
        KeepaliveFlag.RESPOND,
        0x0102030405060708n,
        payload(0xb1, 0xb2)
    ), KeepaliveFlag.RESPOND),

    variant("REQUEST_RESPONSE", "empty", new RequestResponseFrame(
        1,
        RequestResponseFlag.NONE
    ), RequestResponseFlag.NONE),
    variant("REQUEST_RESPONSE", "data", new RequestResponseFrame(
        1,
        RequestResponseFlag.NONE,
        undefined,
        payload(0xb1)
    ), RequestResponseFlag.NONE),
    variant("REQUEST_RESPONSE", "metadata and data", new RequestResponseFrame(
        1,
        RequestResponseFlag.NONE,
        metadata(0xa1),
        payload(0xb1)
    ), RequestResponseFlag.METADATA),
    variant("REQUEST_RESPONSE", "fragmented", new RequestResponseFrame(
        1,
        RequestResponseFlag.FOLLOWS,
        metadata(0xa1),
        payload(0xb1)
    ), RequestResponseFlag.FOLLOWS | RequestResponseFlag.METADATA),

    variant("REQUEST_FNF", "empty", new RequestFireAndForgetFrame(
        3,
        FireAndForgetFlag.NONE
    ), FireAndForgetFlag.NONE),
    variant("REQUEST_FNF", "metadata and data", new RequestFireAndForgetFrame(
        3,
        FireAndForgetFlag.NONE,
        metadata(0xa1),
        payload(0xb1)
    ), FireAndForgetFlag.METADATA),
    variant("REQUEST_FNF", "fragmented", new RequestFireAndForgetFrame(
        3,
        FireAndForgetFlag.FOLLOWS,
        metadata(0xa1),
        payload(0xb1)
    ), FireAndForgetFlag.FOLLOWS | FireAndForgetFlag.METADATA),

    variant("REQUEST_STREAM", "minimal demand", new RequestStreamFrame(
        5,
        RequestStreamFlag.NONE,
        1
    ), RequestStreamFlag.NONE),
    variant("REQUEST_STREAM", "metadata and data", new RequestStreamFrame(
        5,
        RequestStreamFlag.NONE,
        100,
        metadata(0xa1),
        payload(0xb1)
    ), RequestStreamFlag.METADATA),
    variant("REQUEST_STREAM", "fragmented and maximum demand", new RequestStreamFrame(
        5,
        RequestStreamFlag.FOLLOWS,
        0x7fffffff,
        metadata(0xa1),
        payload(0xb1)
    ), RequestStreamFlag.FOLLOWS | RequestStreamFlag.METADATA),

    variant("REQUEST_CHANNEL", "minimal", new RequestChannelFrame(
        7,
        RequestChannelFlag.NONE,
        1
    ), RequestChannelFlag.NONE),
    variant("REQUEST_CHANNEL", "complete", new RequestChannelFrame(
        7,
        RequestChannelFlag.COMPLETE,
        1,
        undefined,
        payload(0xb1)
    ), RequestChannelFlag.COMPLETE),
    variant("REQUEST_CHANNEL", "metadata and data", new RequestChannelFrame(
        7,
        RequestChannelFlag.NONE,
        100,
        metadata(0xa1),
        payload(0xb1)
    ), RequestChannelFlag.METADATA),
    variant("REQUEST_CHANNEL", "fragmented and complete", new RequestChannelFrame(
        7,
        RequestChannelFlag.FOLLOWS | RequestChannelFlag.COMPLETE,
        0x7fffffff,
        metadata(0xa1),
        payload(0xb1)
    ), RequestChannelFlag.FOLLOWS | RequestChannelFlag.COMPLETE | RequestChannelFlag.METADATA),

    variant("REQUEST_N", "one", new RequestNFrame(9, 1), FrameFlag.NONE),
    variant("REQUEST_N", "maximum", new RequestNFrame(9, 0x7fffffff), FrameFlag.NONE),
    variant("CANCEL", "stream", new CancelFrame(11), FrameFlag.NONE),

    variant("PAYLOAD", "next with empty data", new PayloadFrame(
        13,
        PayloadFlag.NEXT,
        undefined,
        payload()
    ), PayloadFlag.NEXT),
    variant("PAYLOAD", "complete", new PayloadFrame(
        13,
        PayloadFlag.COMPLETE
    ), PayloadFlag.COMPLETE),
    variant("PAYLOAD", "next and complete", new PayloadFrame(
        13,
        PayloadFlag.NEXT | PayloadFlag.COMPLETE,
        metadata(0xa1),
        payload(0xb1)
    ), PayloadFlag.NEXT | PayloadFlag.COMPLETE | PayloadFlag.METADATA),
    variant("PAYLOAD", "metadata fragment", new PayloadFrame(
        13,
        PayloadFlag.FOLLOWS,
        metadata(0xa1)
    ), PayloadFlag.FOLLOWS | PayloadFlag.METADATA),
    variant("PAYLOAD", "data fragment", new PayloadFrame(
        13,
        PayloadFlag.FOLLOWS,
        undefined,
        payload(0xb1)
    ), PayloadFlag.FOLLOWS),

    variant("ERROR", "connection", new ErrorFrame(
        0,
        FrameErrorCode.CONNECTION_ERROR
    ), FrameFlag.NONE),
    variant("ERROR", "stream with data", new ErrorFrame(
        15,
        FrameErrorCode.APPLICATION_ERROR,
        payload(0xb1)
    ), FrameFlag.NONE),
    variant("ERROR", "application-defined", new ErrorFrame(
        15,
        0x00000301 as FrameErrorCode,
        payload(0xb1, 0xb2)
    ), FrameFlag.NONE),

    variant("METADATA_PUSH", "empty", new MetadataPushFrame(metadata()), FrameFlag.METADATA),
    variant("METADATA_PUSH", "metadata", new MetadataPushFrame(metadata(0xa1, 0xa2)), FrameFlag.METADATA),

    variant("RESUME", "zero positions", new ResumeFrame("", 0n, 0n), FrameFlag.NONE),
    variant("RESUME", "positions and version", new ResumeFrame(
        "resume-token",
        0x7fffffffffffffffn,
        0x0102030405060708n,
        2,
        3
    ), FrameFlag.NONE),
    variant("RESUME_OK", "zero position", new ResumeOkFrame(0n), FrameFlag.NONE),
    variant("RESUME_OK", "maximum position", new ResumeOkFrame(0x7fffffffffffffffn), FrameFlag.NONE),

    variant("EXT", "minimal", new ExtensionFrame(17, ExtensionFlag.NONE, 1), ExtensionFlag.NONE),
    variant("EXT", "ignorable", new ExtensionFrame(
        17,
        ExtensionFlag.IGNORE,
        2,
        undefined,
        payload(0xb1)
    ), ExtensionFlag.IGNORE),
    variant("EXT", "all extension flags with metadata", new ExtensionFrame(
        17,
        ExtensionFlag.IGNORE
            | ExtensionFlag.EXT_1
            | ExtensionFlag.EXT_2
            | ExtensionFlag.EXT_3
            | ExtensionFlag.EXT_4
            | ExtensionFlag.EXT_5
            | ExtensionFlag.EXT_6
            | ExtensionFlag.EXT_7
            | ExtensionFlag.EXT_8,
        0x7fffffff,
        metadata(0xa1),
        payload(0xb1)
    ), 0x03ff)
];

describe("complete frame serialization matrix", () => {
    test("contains every assigned standard frame type", () => {
        const covered = [...new Set(variants.map(({frame}) => frame.type))].sort((left, right) => left - right);
        expect(covered).toEqual([
            FrameType.SETUP,
            FrameType.LEASE,
            FrameType.KEEPALIVE,
            FrameType.REQUEST_RESPONSE,
            FrameType.REQUEST_FNF,
            FrameType.REQUEST_STREAM,
            FrameType.REQUEST_CHANNEL,
            FrameType.REQUEST_N,
            FrameType.CANCEL,
            FrameType.PAYLOAD,
            FrameType.ERROR,
            FrameType.METADATA_PUSH,
            FrameType.RESUME,
            FrameType.RESUME_OK,
            FrameType.EXT
        ]);
    });

    test.each(variants)("$frameName: $variant", ({frame, flags}) => {
        expect(frame.header.flags).toBe(flags);

        for (const transport of ["websocket", "tcp"] as const) {
            const frameCodec = createCodec(transport);
            const encoded = frameCodec.serialize(frame);
            const decoded = frameCodec.deserialize(encoded);

            expect(decoded).toHaveLength(1);
            expect(snapshot(decoded[0]!)).toEqual(snapshot(frame));
            expect(frameCodec.serialize(decoded[0]!)).toEqual(encoded);
            frameCodec.finish();
        }
    });
});

describe("fragmented request sequences", () => {
    const requestFrames: ReadonlyArray<readonly [string, Frame]> = [
        ["REQUEST_RESPONSE", new RequestResponseFrame(
            1,
            RequestResponseFlag.FOLLOWS,
            metadata(0xa1)
        )],
        ["REQUEST_FNF", new RequestFireAndForgetFrame(
            3,
            FireAndForgetFlag.FOLLOWS,
            metadata(0xa1)
        )],
        ["REQUEST_STREAM", new RequestStreamFrame(
            5,
            RequestStreamFlag.FOLLOWS,
            10,
            metadata(0xa1)
        )],
        ["REQUEST_CHANNEL", new RequestChannelFrame(
            7,
            RequestChannelFlag.FOLLOWS,
            10,
            metadata(0xa1)
        )]
    ];

    test.each(requestFrames)("preserves %s followed by PAYLOAD fragments", (_, request) => {
        const fragments = [
            request,
            new PayloadFrame(request.header.streamId, PayloadFlag.FOLLOWS, metadata(0xa2), payload(0xb1)),
            new PayloadFrame(request.header.streamId, PayloadFlag.NEXT, undefined, payload(0xb2))
        ];
        const tcp = createCodec("tcp");
        const decoded = tcp.deserialize(join(...fragments.map(frame => tcp.serialize(frame))));

        expect(decoded).toHaveLength(3);
        expect(decoded.map(frame => snapshot(frame))).toEqual(fragments.map(frame => snapshot(frame)));
        expect(decoded[0]!.isFlagSet(RequestResponseFlag.FOLLOWS)).toBe(true);
        expect((decoded[1] as PayloadFrame).hasFollows()).toBe(true);
        expect((decoded[2] as PayloadFrame).hasFollows()).toBe(false);
        expect((decoded[2] as PayloadFrame).isNext()).toBe(true);
        tcp.finish();
    });

    test.each([
        [new RequestResponseFrame(1, RequestResponseFlag.NONE), false],
        [new RequestResponseFrame(1, RequestResponseFlag.FOLLOWS), true],
        [new RequestFireAndForgetFrame(3, FireAndForgetFlag.NONE), false],
        [new RequestFireAndForgetFrame(3, FireAndForgetFlag.FOLLOWS), true],
        [new RequestStreamFrame(5, RequestStreamFlag.NONE, 1), false],
        [new RequestStreamFrame(5, RequestStreamFlag.FOLLOWS, 1), true],
        [new RequestChannelFrame(7, RequestChannelFlag.NONE, 1), false],
        [new RequestChannelFrame(7, RequestChannelFlag.FOLLOWS, 1), true]
    ] as const)("reports request fragmentation state", (frame, expected) => {
        expect(frame.hasFollows()).toBe(expected);
    });

    test("preserves non-fragmented NEXT and COMPLETE semantics", () => {
        const frames = [
            new PayloadFrame(1, PayloadFlag.NEXT, undefined, payload(0xb1)),
            new PayloadFrame(1, PayloadFlag.COMPLETE),
            new PayloadFrame(1, PayloadFlag.NEXT | PayloadFlag.COMPLETE, undefined, payload(0xb2))
        ];
        const websocket = createCodec("websocket");

        const decoded = frames.map(frame => websocket.deserialize(websocket.serialize(frame))[0] as PayloadFrame);
        expect(decoded.map(frame => ({next: frame.isNext(), complete: frame.isComplete()}))).toEqual([
            {next: true, complete: false},
            {next: false, complete: true},
            {next: true, complete: true}
        ]);
    });
});

function variant(frameName: string, name: string, frame: Frame, flags: number): FrameVariant {
    return {frameName, variant: name, frame, flags};
}

function createCodec(transport: FrameTransport): FrameCodec {
    return new FrameCodec({
        transport,
        mimetype: {metadata: binaryMimeType, data: binaryMimeType}
    });
}

function snapshot(frame: Frame): unknown {
    const fields: Record<string, unknown> = {};
    for (const [key, value] of Object.entries(frame)) {
        if (key !== "header" && key !== "metadata" && key !== "payload") {
            fields[key] = normalize(value);
        }
    }
    return {
        class: frame.constructor.name,
        type: frame.type,
        streamId: frame.header.streamId,
        flags: frame.header.flags,
        canBeIgnored: frame.canBeIgnored(),
        metadata: frame.hasMetadata() ? content(frame.metadata) : undefined,
        data: content(frame.payload),
        fields
    };
}

function normalize(value: unknown): unknown {
    if (typeof value === "bigint") return value.toString();
    if (value instanceof Uint8Array) return Array.from(value);
    if (value instanceof MimeType) {
        return {mimeType: value.mimeType, identifier: value.identifier};
    }
    return value;
}

function content(value: unknown): number[] {
    if (value === undefined) return [];
    if (value instanceof Metadata || value instanceof Payload) {
        return Array.from(value.toUint8Array());
    }
    if (value instanceof Uint8Array) return Array.from(value);
    throw new TypeError(`Expected binary frame content, received ${typeof value}`);
}

function join(...chunks: Uint8Array[]): Uint8Array {
    const result = new Uint8Array(chunks.reduce((length, chunk) => length + chunk.length, 0));
    let offset = 0;
    for (const chunk of chunks) {
        result.set(chunk, offset);
        offset += chunk.length;
    }
    return result;
}
