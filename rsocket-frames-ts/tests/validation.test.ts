import {
    CancelFrame,
    ExtensionFrame,
    FrameFlag,
    Header,
    PayloadFlag,
    PayloadFrame,
    RequestNFrame,
    RequestStreamFrame,
    ResumeFrame,
    SetupFlag,
    SetupFrame,
    WellKnownMimeType
} from "@/index";

describe("protocol field validation", () => {
    test("validates wire ranges while preserving semantic errors for the protocol layer", () => {
        expect(() => new CancelFrame(0)).not.toThrow();
        expect(() => new CancelFrame(0x80000000)).toThrow("Stream ID");
        expect(() => new RequestNFrame(1, 0)).toThrow("Request N");
        expect(() => new RequestStreamFrame(1, FrameFlag.NONE, 0)).toThrow("Initial request N");
    });

    test("preserves PAYLOAD flags for fragmentation and lenient peer handling", () => {
        expect(() => new PayloadFrame(1, PayloadFlag.NONE)).not.toThrow();
        expect(() => new PayloadFrame(1, PayloadFlag.FOLLOWS)).not.toThrow();
        expect(() => new PayloadFrame(1, PayloadFlag.NEXT)).not.toThrow();
        expect(() => new PayloadFrame(1, PayloadFlag.COMPLETE)).not.toThrow();
    });

    test("validates SETUP and RESUME fields", () => {
        const binary = WellKnownMimeType.APPLICATION_OCTET_STREAM;
        expect(() => new SetupFrame(0, 1, binary, binary)).toThrow("Keepalive interval");
        expect(() => new SetupFrame(1, 0, binary, binary)).toThrow("Maximum lifetime");
        expect(() => new SetupFrame(1, 1, binary, binary, undefined, 1, 0, SetupFlag.RESUME))
            .toThrow("requires a resume token");
        expect(() => new ResumeFrame("token", -1n, 0n)).toThrow("Last received server position");
    });

    test("validates header flags and extension type", () => {
        expect(() => new Header(0, 0, 0x400)).toThrow("Frame flags");
        expect(() => new ExtensionFrame(0, FrameFlag.IGNORE, 0)).toThrow("Extended type");
    });
});
