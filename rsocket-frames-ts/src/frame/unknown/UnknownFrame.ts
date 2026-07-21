import type {ByteReader, ByteWriter} from "bebyte";
import {Frame} from "@/frame/Frame";
import {FrameFlag} from "@/frame/FrameFlag";
import {Header} from "@/frame/context/Header";

/**
 * Preserves an unassigned frame whose `IGNORE` flag permits safe skipping.
 *
 * The body is intentionally opaque: interpreting an unknown frame would make
 * assumptions about an extension that this codec does not understand.
 */
export class UnknownFrame extends Frame {
    /** Creates an opaque ignored frame from a validated header and body. */
    private constructor(header: Header, public readonly body: Uint8Array) {
        super(header.frameType, header.streamId, header.flags, undefined, undefined, false);
        if (!header.isFlagSet(FrameFlag.IGNORE)) {
            throw new RangeError("An unknown RSocket frame must set the IGNORE flag");
        }
    }

    /**
     * Consumes the complete unknown body without interpreting it.
     *
     * @param header Parsed frame header.
     * @param reader Reader positioned at the unknown body.
     * @returns Opaque ignorable frame.
     */
    public static from(header: Header, reader: ByteReader): UnknownFrame {
        return new UnknownFrame(header, reader.viewRemaining());
    }

    /** Writes the opaque body exactly as received. */
    protected write(writer: ByteWriter): void {
        writer.write(this.body);
    }
}
