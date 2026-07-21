import {FrameType, PayloadFlag} from "rsocket-frames-ts";

/** Fixed RSocket header fields captured from one WebSocket binary message. */
export interface ObservedRSocketFrame {
  /** Complete RSocket frame bytes without a transport length prefix. */
  readonly bytes: Uint8Array;
  /** Decoded 31-bit stream identifier. */
  readonly streamId: number;
  /** Decoded frame type. */
  readonly type: FrameType;
  /** Decoded ten-bit frame flags. */
  readonly flags: number;
}

/** Decodes fixed RSocket headers from captured WebSocket messages. */
export function observedRSocketFrames(messages: readonly Uint8Array[]): ObservedRSocketFrame[] {
  return messages.map((bytes) => {
    if (bytes.byteLength < 6) {
      throw new Error(`RSocket frame is shorter than its 6-byte header: ${bytes.byteLength}`);
    }
    const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
    const typeAndFlags = view.getUint16(4);
    return {
      bytes,
      streamId: view.getUint32(0) & 0x7fffffff,
      type: typeAndFlags >>> 10 as FrameType,
      flags: typeAndFlags & 0x03ff
    };
  });
}

/** Splits consecutive wire frames at each fragment-sequence boundary. */
export function fragmentSequences(
  frames: readonly ObservedRSocketFrame[]
): ObservedRSocketFrame[][] {
  const sequences: ObservedRSocketFrame[][] = [];
  let current: ObservedRSocketFrame[] = [];
  for (const frame of frames) {
    current.push(frame);
    if (!hasFollows(frame)) {
      sequences.push(current);
      current = [];
    }
  }
  if (current.length > 0) sequences.push(current);
  return sequences;
}

/** Returns whether one logical fragment sequence carries an onNext signal. */
export function isNextSequence(frames: readonly ObservedRSocketFrame[]): boolean {
  return frames.some((frame) => (frame.flags & PayloadFlag.NEXT) === PayloadFlag.NEXT);
}

/** Returns whether one wire frame advertises another fragment. */
export function hasFollows(frame: ObservedRSocketFrame): boolean {
  return (frame.flags & PayloadFlag.FOLLOWS) === PayloadFlag.FOLLOWS;
}
