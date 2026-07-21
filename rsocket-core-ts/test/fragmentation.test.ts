/**
 * Protocol-level tests for outbound RSocket frame fragmentation.
 */
import {describe, expect, it} from "vitest";
import {
  type Frame,
  FrameErrorCode,
  FrameFlag,
  Metadata,
  PayloadFlag,
  PayloadFrame,
  RequestChannelFlag,
  RequestChannelFrame,
  RequestFireAndForgetFrame,
  RequestNFrame,
  RequestResponseFrame,
  RequestStreamFrame,
  WellKnownMimeType
} from "rsocket-frames-ts";
import {
  emitOutboundFrameFragments,
  emitSerializedOutboundFrames,
  IgnoredPayloadFragments,
  outboundFrameLength,
  reassemblePayloadFrame,
  RequestFragmentAssembler,
  type InitialRequestFrame
} from "@";
import {concatFragmentBytes} from "@/reassembly/parts.js";

const MAX_FRAME_LENGTH = 64;
const STREAM_ID = 17;
const metadataBytes = patternedBytes(137, 29);
const dataBytes = patternedBytes(211, 83);
const metadataMimeType = WellKnownMimeType.APPLICATION_OCTET_STREAM;
const dataMimeType = WellKnownMimeType.APPLICATION_OCTET_STREAM;

describe("fragment byte retention", () => {
  it("detaches a small sole fragment from an oversized wire buffer", () => {
    const wire = new Uint8Array(256 * 1024);
    const metadata = wire.subarray(32, 64);

    const compact = concatFragmentBytes([metadata], metadata.byteLength);

    expect(compact).toEqual(metadata);
    expect(compact.buffer).not.toBe(wire.buffer);
  });
});

describe("ignored fragmented payload tracking", () => {
  it("retains an ignored stream only until its final continuation", () => {
    const ignored = new IgnoredPayloadFragments();

    ignored.update(STREAM_ID, PayloadFlag.FOLLOWS);
    expect(ignored.has(STREAM_ID)).toBe(true);
    expect(ignored.consume(STREAM_ID, PayloadFlag.FOLLOWS)).toBe(true);
    expect(ignored.has(STREAM_ID)).toBe(true);
    expect(ignored.consume(STREAM_ID, PayloadFlag.NEXT)).toBe(true);
    expect(ignored.has(STREAM_ID)).toBe(false);
    expect(ignored.consume(STREAM_ID, PayloadFlag.NEXT)).toBe(false);
  });
});

/**
 * One fragmentable frame shape and its frame-specific assertions.
 */
interface FragmentationCase {
  /** Human-readable case name. */
  readonly name: string;
  /** Original oversized frame. */
  readonly frame: Frame;
  /** Constructor expected for the first emitted fragment. */
  readonly firstType: abstract new (...args: any[]) => Frame;
  /** Optional assertion for fields unique to the initial frame type. */
  readonly assertInitial?: (frame: Frame) => void;
  /** Whether COMPLETE must move to the final fragment. */
  readonly completeOnFinal?: boolean;
}

const fragmentationCases: readonly FragmentationCase[] = [
  {
    name: "REQUEST_FNF",
    frame: new RequestFireAndForgetFrame(
      STREAM_ID,
      FrameFlag.NONE,
      new Metadata(metadataMimeType, metadataBytes),
      dataMimeType.toPayload(dataBytes)
    ),
    firstType: RequestFireAndForgetFrame
  },
  {
    name: "REQUEST_RESPONSE",
    frame: new RequestResponseFrame(
      STREAM_ID,
      FrameFlag.NONE,
      new Metadata(metadataMimeType, metadataBytes),
      dataMimeType.toPayload(dataBytes)
    ),
    firstType: RequestResponseFrame
  },
  {
    name: "REQUEST_STREAM",
    frame: new RequestStreamFrame(
      STREAM_ID,
      FrameFlag.NONE,
      23,
      new Metadata(metadataMimeType, metadataBytes),
      dataMimeType.toPayload(dataBytes)
    ),
    firstType: RequestStreamFrame,
    assertInitial: (frame) => expect((frame as RequestStreamFrame).request).toBe(23)
  },
  {
    name: "REQUEST_CHANNEL",
    frame: new RequestChannelFrame(
      STREAM_ID,
      RequestChannelFlag.COMPLETE,
      31,
      new Metadata(metadataMimeType, metadataBytes),
      dataMimeType.toPayload(dataBytes)
    ),
    firstType: RequestChannelFrame,
    assertInitial: (frame) => expect((frame as RequestChannelFrame).request).toBe(31),
    completeOnFinal: true
  },
  {
    name: "PAYLOAD",
    frame: new PayloadFrame(
      STREAM_ID,
      PayloadFlag.combine(PayloadFlag.NEXT, PayloadFlag.COMPLETE),
      new Metadata(metadataMimeType, metadataBytes),
      dataMimeType.toPayload(dataBytes)
    ),
    firstType: PayloadFrame,
    completeOnFinal: true
  }
];

describe("RSocket outbound fragmentation", () => {
  for (const testCase of fragmentationCases) {
    it(`fragments ${testCase.name} without changing its logical payload`, () => {
      const estimatedLength = outboundFrameLength(testCase.frame);
      const fragments: Frame[] = [];

      expect(estimatedLength).toBe(testCase.frame.toUint8Array().byteLength);
      expect(estimatedLength).toBeGreaterThan(MAX_FRAME_LENGTH);
      emitOutboundFrameFragments(
        testCase.frame,
        estimatedLength!,
        MAX_FRAME_LENGTH,
        (fragment) => fragments.push(fragment)
      );

      expect(fragments.length).toBeGreaterThan(2);
      expect(fragments[0]).toBeInstanceOf(testCase.firstType);
      expect(fragments.slice(1).every((frame) => frame instanceof PayloadFrame)).toBe(true);
      expect(fragments.every((frame) => frame.header.streamId === STREAM_ID)).toBe(true);
      expect(fragments.every((frame) => frame.toUint8Array().byteLength <= MAX_FRAME_LENGTH)).toBe(true);
      expect(fragments.slice(0, -1).every(hasFollows)).toBe(true);
      expect(hasFollows(fragments.at(-1)!)).toBe(false);
      testCase.assertInitial?.(fragments[0]!);

      const continuationFrames = fragments.slice(1) as PayloadFrame[];
      expect(continuationFrames.every((frame) => frame.isNext())).toBe(true);
      if (testCase.completeOnFinal) {
        expect(fragments.slice(0, -1).every((frame) => !isComplete(frame))).toBe(true);
        expect(isComplete(fragments.at(-1)!)).toBe(true);
      }

      assertMetadataBeforeData(fragments);
      expect(concatFramePart(fragments, "metadata")).toEqual(metadataBytes);
      expect(concatFramePart(fragments, "payload")).toEqual(dataBytes);
    });
  }

  it("does not fragment a frame that exactly fits the configured limit", () => {
    const frame = new RequestResponseFrame(
      STREAM_ID,
      FrameFlag.NONE,
      undefined,
      dataMimeType.toPayload(patternedBytes(53, 7))
    );
    const length = outboundFrameLength(frame)!;
    const emitted: Frame[] = [];

    emitOutboundFrameFragments(frame, length, length, (fragment) => emitted.push(fragment));

    expect(emitted).toEqual([frame]);
  });

  it("preserves empty metadata when only its header fits before data fragments", () => {
    const frame = new PayloadFrame(
      STREAM_ID,
      PayloadFlag.NEXT,
      new Metadata(metadataMimeType, new Uint8Array(0)),
      dataMimeType.toPayload(Uint8Array.of(1, 2, 3))
    );
    const fragments: Frame[] = [];

    emitOutboundFrameFragments(frame, outboundFrameLength(frame)!, 9, (fragment) => fragments.push(fragment));

    expect(fragments).toHaveLength(2);
    expect(fragments[0]?.hasMetadata()).toBe(true);
    expect(partBytes(fragments[0]?.metadata)).toEqual(new Uint8Array(0));
    expect(partBytes(fragments[0]?.payload)).toBeUndefined();
    expect(partBytes(fragments[1]?.payload)).toEqual(Uint8Array.of(1, 2, 3));
    expect(fragments.every((fragment) => fragment.toUint8Array().byteLength <= 9)).toBe(true);
  });

  it("counts and preserves an explicit empty metadata section without a wrapper object", () => {
    const frame = new PayloadFrame(
      STREAM_ID,
      PayloadFlag.combine(PayloadFlag.NEXT, FrameFlag.METADATA),
      undefined,
      dataMimeType.toPayload(Uint8Array.of(1, 2, 3))
    );
    const length = outboundFrameLength(frame)!;
    const fragments: Frame[] = [];

    expect(length).toBe(frame.toUint8Array().byteLength);
    emitOutboundFrameFragments(frame, length, 9, (fragment) => fragments.push(fragment));

    expect(fragments).toHaveLength(2);
    expect(fragments[0]?.hasMetadata()).toBe(true);
    expect(fragments[0]?.metadata).toBeUndefined();
    expect(fragments[1]?.hasMetadata()).toBe(false);
    expect(fragments.every((fragment) => fragment.toUint8Array().byteLength <= 9)).toBe(true);
  });

  it("keeps terminal-only PAYLOAD fragments free of logical NEXT semantics", () => {
    const frame = new PayloadFrame(
      STREAM_ID,
      PayloadFlag.COMPLETE,
      new Metadata(metadataMimeType, metadataBytes)
    );
    const fragments: PayloadFrame[] = [];

    emitOutboundFrameFragments(
      frame,
      outboundFrameLength(frame)!,
      MAX_FRAME_LENGTH,
      (fragment) => fragments.push(fragment as PayloadFrame)
    );

    expect(fragments.length).toBeGreaterThan(1);
    expect(fragments[0]?.hasFollows()).toBe(true);
    expect(fragments[0]?.isNext()).toBe(false);
    expect(fragments[0]?.isComplete()).toBe(false);
    expect(fragments.slice(1, -1).every((fragment) => fragment.isNext() && !fragment.isComplete())).toBe(true);
    expect(fragments.at(-1)?.isNext()).toBe(false);
    expect(fragments.at(-1)?.isComplete()).toBe(true);

    const state = new Map();
    let reassembled: PayloadFrame | undefined;
    for (const fragment of fragments) {
      reassembled = reassemblePayloadFrame(fragment, state, metadataMimeType, dataMimeType);
    }
    expect(reassembled?.isNext()).toBe(false);
    expect(reassembled?.isComplete()).toBe(true);
    expect(partBytes(reassembled?.metadata)).toEqual(metadataBytes);
    expect(state.size).toBe(0);
  });

  it("rejects a limit that cannot fit the fixed REQUEST_STREAM fields", () => {
    const frame = new RequestStreamFrame(STREAM_ID, FrameFlag.NONE, 1);
    const length = outboundFrameLength(frame)!;

    expect(() => emitOutboundFrameFragments(frame, length, 6, () => {})).toThrow("maxFrameLength 6");
  });

  it("rejects an oversized frame type that the protocol does not allow to fragment", () => {
    const frame = new RequestNFrame(STREAM_ID, 1);
    const length = frame.toUint8Array().byteLength;

    expect(outboundFrameLength(frame)).toBeUndefined();
    expect(() => emitOutboundFrameFragments(frame, length, length - 1, () => {}))
      .toThrow(`frame length ${length} exceeds configured maxFrameLength ${length - 1}`);
  });

  it("shares one serialize-or-fragment path for transport writers", () => {
    const small = new RequestResponseFrame(STREAM_ID, FrameFlag.NONE, undefined, dataMimeType.toPayload(Uint8Array.of(1)));
    const large = fragmentationCases[1]!.frame;
    const emitted: Array<{frame: Frame; bytes: Uint8Array}> = [];

    emitSerializedOutboundFrames(small, MAX_FRAME_LENGTH, (frame, bytes) => emitted.push({frame, bytes}));
    emitSerializedOutboundFrames(large, MAX_FRAME_LENGTH, (frame, bytes) => emitted.push({frame, bytes}));

    expect(emitted[0]).toMatchObject({frame: small});
    expect(emitted[0]!.bytes).toEqual(small.toUint8Array());
    expect(emitted.slice(1).length).toBeGreaterThan(1);
    expect(emitted.slice(1).every(({frame, bytes}) =>
      bytes.byteLength <= MAX_FRAME_LENGTH && bytes.byteLength === frame.toUint8Array().byteLength
    )).toBe(true);
  });
});

describe("RSocket initial request reassembly", () => {
  for (const testCase of fragmentationCases.slice(0, 4)) {
    it(`restores fragmented ${testCase.name} request semantics`, () => {
      const fragments: Frame[] = [];
      const length = outboundFrameLength(testCase.frame) as number;
      emitOutboundFrameFragments(testCase.frame, length, MAX_FRAME_LENGTH, (frame) => fragments.push(frame));
      const assembler = new RequestFragmentAssembler();
      assembler.start(fragments[0] as InitialRequestFrame);

      let request: InitialRequestFrame | undefined;
      for (const continuation of fragments.slice(1) as PayloadFrame[]) {
        request = assembler.continue(continuation, metadataMimeType, dataMimeType);
      }

      expect(request).toBeInstanceOf(testCase.firstType);
      expect(request?.header.streamId).toBe(STREAM_ID);
      expect(request?.metadata?.toUint8Array()).toEqual(metadataBytes);
      expect(request?.payload?.toUint8Array()).toEqual(dataBytes);
      expect(assembler.isEmpty).toBe(true);
      testCase.assertInitial?.(request as Frame);
      if (testCase.completeOnFinal) expect((request as RequestChannelFrame).isComplete()).toBe(true);
    });
  }

  it("accepts a COMPLETE-only final request continuation", () => {
    const assembler = new RequestFragmentAssembler();
    assembler.start(new RequestChannelFrame(
      STREAM_ID,
      RequestChannelFlag.FOLLOWS,
      1,
      undefined,
      dataMimeType.toPayload(Uint8Array.of(1, 2))
    ));

    const request = assembler.continue(
      new PayloadFrame(STREAM_ID, PayloadFlag.COMPLETE),
      metadataMimeType,
      dataMimeType
    );

    expect(request).toBeInstanceOf(RequestChannelFrame);
    expect((request as RequestChannelFrame).isComplete()).toBe(true);
    expect(request?.payload?.toUint8Array()).toEqual(Uint8Array.of(1, 2));
    expect(assembler.isEmpty).toBe(true);
  });

  it("accepts a FOLLOWS-only intermediate request continuation", () => {
    const assembler = new RequestFragmentAssembler();
    assembler.start(new RequestResponseFrame(
      STREAM_ID,
      PayloadFlag.FOLLOWS,
      undefined,
      dataMimeType.toPayload(Uint8Array.of(1))
    ));

    expect(assembler.continue(
      new PayloadFrame(
        STREAM_ID,
        PayloadFlag.FOLLOWS,
        undefined,
        dataMimeType.toPayload(Uint8Array.of(2))
      ),
      metadataMimeType,
      dataMimeType
    )).toBeUndefined();
    const request = assembler.continue(
      new PayloadFrame(
        STREAM_ID,
        PayloadFlag.NEXT,
        undefined,
        dataMimeType.toPayload(Uint8Array.of(3))
      ),
      metadataMimeType,
      dataMimeType
    );

    expect(request?.payload?.toUint8Array()).toEqual(Uint8Array.of(1, 2, 3));
    expect(assembler.isEmpty).toBe(true);
  });

  it("decodes empty fragmented request data with the configured MIME type", () => {
    const textMimeType = WellKnownMimeType.TEXT_PLAIN;
    const frame = new RequestResponseFrame(
      STREAM_ID,
      FrameFlag.NONE,
      new Metadata(metadataMimeType, metadataBytes),
      textMimeType.toPayload("")
    );
    const fragments: Frame[] = [];
    emitOutboundFrameFragments(
      frame,
      outboundFrameLength(frame)!,
      MAX_FRAME_LENGTH,
      (fragment) => fragments.push(fragment)
    );
    const assembler = new RequestFragmentAssembler();
    assembler.start(fragments[0] as InitialRequestFrame);

    let request: InitialRequestFrame | undefined;
    for (const continuation of fragments.slice(1) as PayloadFrame[]) {
      request = assembler.continue(continuation, metadataMimeType, textMimeType);
    }

    expect(fragments.length).toBeGreaterThan(1);
    expect(request?.payload?.payload).toBe("");
    expect(request?.payload?.toUint8Array()).toEqual(new Uint8Array(0));
  });
});

describe("RSocket inbound fragment reassembly", () => {
  it("accepts a FOLLOWS-only first fragment and preserves final semantics", () => {
    const fragments = new Map();
    const first = new PayloadFrame(
      STREAM_ID,
      PayloadFlag.FOLLOWS,
      undefined,
      dataMimeType.toPayload(Uint8Array.of(1))
    );
    const final = new PayloadFrame(STREAM_ID, PayloadFlag.COMPLETE);

    expect(reassemblePayloadFrame(first, fragments, metadataMimeType, dataMimeType)).toBeUndefined();
    const reassembled = reassemblePayloadFrame(final, fragments, metadataMimeType, dataMimeType);

    expect(reassembled?.isNext()).toBe(false);
    expect(reassembled?.isComplete()).toBe(true);
    expect(partBytes(reassembled?.payload)).toEqual(Uint8Array.of(1));
    expect(fragments.size).toBe(0);
  });

  it("rejects a non-fragmented PAYLOAD that has neither NEXT nor COMPLETE", () => {
    const fragments = new Map();
    const invalid = new PayloadFrame(STREAM_ID, FrameFlag.NONE);

    expect(() => reassemblePayloadFrame(invalid, fragments, metadataMimeType, dataMimeType))
      .toThrow(expect.objectContaining({
        code: FrameErrorCode.INVALID,
        message: expect.stringContaining("must set FOLLOWS, NEXT, COMPLETE"),
        streamId: STREAM_ID
      }));
    expect(fragments.size).toBe(0);
  });

  it("reassembles multi-frame metadata before data without losing flags or bytes", () => {
    const fragments = new Map();
    const first = new PayloadFrame(
      STREAM_ID,
      PayloadFlag.combine(PayloadFlag.NEXT, PayloadFlag.FOLLOWS),
      new Metadata(metadataMimeType, metadataBytes.subarray(0, 64))
    );
    const second = new PayloadFrame(
      STREAM_ID,
      PayloadFlag.combine(PayloadFlag.NEXT, PayloadFlag.FOLLOWS),
      new Metadata(metadataMimeType, metadataBytes.subarray(64)),
      dataMimeType.toPayload(dataBytes.subarray(0, 41))
    );
    const last = new PayloadFrame(
      STREAM_ID,
      PayloadFlag.combine(PayloadFlag.NEXT, PayloadFlag.COMPLETE),
      undefined,
      dataMimeType.toPayload(dataBytes.subarray(41))
    );

    expect(reassemblePayloadFrame(first, fragments, metadataMimeType, dataMimeType)).toBeUndefined();
    expect(reassemblePayloadFrame(second, fragments, metadataMimeType, dataMimeType)).toBeUndefined();
    const reassembled = reassemblePayloadFrame(last, fragments, metadataMimeType, dataMimeType);

    expect(reassembled).toBeDefined();
    expect(reassembled!.isNext()).toBe(true);
    expect(reassembled!.isComplete()).toBe(true);
    expect(reassembled!.hasFollows()).toBe(false);
    expect(partBytes(reassembled!.metadata)).toEqual(metadataBytes);
    expect(partBytes(reassembled!.payload)).toEqual(dataBytes);
    expect(fragments.size).toBe(0);
  });

  it("rejects metadata received after fragmented data and releases the sequence", () => {
    const fragments = new Map();
    const first = new PayloadFrame(
      STREAM_ID,
      PayloadFlag.combine(PayloadFlag.NEXT, PayloadFlag.FOLLOWS),
      undefined,
      dataMimeType.toPayload(dataBytes.subarray(0, 32))
    );
    const last = new PayloadFrame(
      STREAM_ID,
      PayloadFlag.NEXT,
      new Metadata(metadataMimeType, metadataBytes.subarray(0, 16))
    );

    expect(reassemblePayloadFrame(first, fragments, metadataMimeType, dataMimeType)).toBeUndefined();
    expect(() => reassemblePayloadFrame(last, fragments, metadataMimeType, dataMimeType))
      .toThrow("metadata after data");
    expect(fragments.size).toBe(0);
  });

  it("returns an unfragmented PAYLOAD without allocating fragment state", () => {
    const fragments = new Map();
    const frame = new PayloadFrame(
      STREAM_ID,
      PayloadFlag.combine(PayloadFlag.NEXT, PayloadFlag.COMPLETE),
      undefined,
      dataMimeType.toPayload(dataBytes)
    );

    expect(reassemblePayloadFrame(frame, fragments, metadataMimeType, dataMimeType)).toBe(frame);
    expect(fragments.size).toBe(0);
  });

  it("keeps interleaved fragment sequences isolated by stream ID", () => {
    const fragments = new Map();
    const first = (streamId: number, byte: number) => new PayloadFrame(
      streamId,
      PayloadFlag.combine(PayloadFlag.NEXT, PayloadFlag.FOLLOWS),
      undefined,
      dataMimeType.toPayload(Uint8Array.of(byte))
    );
    const last = (streamId: number, byte: number) => new PayloadFrame(
      streamId,
      PayloadFlag.combine(PayloadFlag.NEXT, PayloadFlag.COMPLETE),
      undefined,
      dataMimeType.toPayload(Uint8Array.of(byte))
    );

    expect(reassemblePayloadFrame(first(1, 1), fragments, metadataMimeType, dataMimeType)).toBeUndefined();
    expect(reassemblePayloadFrame(first(3, 3), fragments, metadataMimeType, dataMimeType)).toBeUndefined();
    expect(partBytes(reassemblePayloadFrame(last(1, 2), fragments, metadataMimeType, dataMimeType)?.payload))
      .toEqual(Uint8Array.of(1, 2));
    expect(fragments.size).toBe(1);
    expect(partBytes(reassemblePayloadFrame(last(3, 4), fragments, metadataMimeType, dataMimeType)?.payload))
      .toEqual(Uint8Array.of(3, 4));
    expect(fragments.size).toBe(0);
  });

  it("preserves explicitly present empty metadata across fragments", () => {
    const fragments = new Map();
    const first = new PayloadFrame(
      STREAM_ID,
      PayloadFlag.combine(PayloadFlag.NEXT, PayloadFlag.FOLLOWS),
      new Metadata(metadataMimeType, new Uint8Array(0)),
      dataMimeType.toPayload(Uint8Array.of(1))
    );
    const last = new PayloadFrame(
      STREAM_ID,
      PayloadFlag.combine(PayloadFlag.NEXT, PayloadFlag.COMPLETE),
      undefined,
      dataMimeType.toPayload(Uint8Array.of(2))
    );

    expect(reassemblePayloadFrame(first, fragments, metadataMimeType, dataMimeType)).toBeUndefined();
    const reassembled = reassemblePayloadFrame(last, fragments, metadataMimeType, dataMimeType);

    expect(reassembled?.hasMetadata()).toBe(true);
    expect(partBytes(reassembled?.metadata)).toEqual(new Uint8Array(0));
    expect(partBytes(reassembled?.payload)).toEqual(Uint8Array.of(1, 2));
  });

  it("lets COMPLETE terminate a contradictory FOLLOWS fragment", () => {
    const fragments = new Map();
    const frame = new PayloadFrame(
      STREAM_ID,
      PayloadFlag.combine(PayloadFlag.NEXT, PayloadFlag.FOLLOWS, PayloadFlag.COMPLETE),
      undefined,
      dataMimeType.toPayload(dataBytes)
    );
    const reassembled = reassemblePayloadFrame(frame, fragments, metadataMimeType, dataMimeType);

    expect(reassembled?.hasFollows()).toBe(false);
    expect(reassembled?.isComplete()).toBe(true);
    expect(partBytes(reassembled?.payload)).toEqual(dataBytes);
    expect(fragments.size).toBe(0);
  });
});

/**
 * Returns whether a fragment advertises another fragment.
 */
function hasFollows(frame: Frame): boolean {
  const candidate = frame as Frame & {hasFollows?: () => boolean};
  return candidate.hasFollows?.() === true;
}

/**
 * Returns whether a fragment carries terminal completion.
 */
function isComplete(frame: Frame): boolean {
  const candidate = frame as Frame & {isComplete?: () => boolean};
  return candidate.isComplete?.() === true;
}

/**
 * Verifies the protocol rule that all metadata bytes precede all data bytes.
 */
function assertMetadataBeforeData(frames: readonly Frame[]): void {
  let dataStarted = false;
  for (const frame of frames) {
    const metadata = partBytes(frame.metadata);
    const data = partBytes(frame.payload);
    if (dataStarted) expect(metadata?.byteLength ?? 0).toBe(0);
    if ((data?.byteLength ?? 0) > 0) dataStarted = true;
  }
}

/**
 * Concatenates one payload part across a complete fragment sequence.
 */
function concatFramePart(frames: readonly Frame[], part: "metadata" | "payload"): Uint8Array {
  const chunks = frames
    .map((frame) => partBytes(frame[part]))
    .filter((bytes): bytes is Uint8Array => bytes !== undefined && bytes.byteLength > 0);
  const length = chunks.reduce((total, bytes) => total + bytes.byteLength, 0);
  const result = new Uint8Array(length);
  let offset = 0;
  for (const bytes of chunks) {
    result.set(bytes, offset);
    offset += bytes.byteLength;
  }
  return result;
}

/**
 * Extracts serialized bytes from a payload or metadata wrapper.
 */
function partBytes(part: unknown): Uint8Array | undefined {
  if (part === undefined || part === null) return undefined;
  if (part instanceof Uint8Array) return part;
  const serializer = (part as {toUint8Array?: unknown}).toUint8Array;
  if (typeof serializer !== "function") return undefined;
  const bytes = serializer.call(part);
  return bytes instanceof Uint8Array ? bytes : undefined;
}

/**
 * Creates deterministic non-compressible-enough bytes for exact reconstruction checks.
 */
function patternedBytes(length: number, seed: number): Uint8Array {
  return Uint8Array.from({length}, (_value, index) => (index * 37 + seed) & 0xff);
}
