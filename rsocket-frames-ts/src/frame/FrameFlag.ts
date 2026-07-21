/**
 * Internal enum defining individual frame-level flags used in RSocket.
 * These flags modify frame behavior.
 */
enum _FrameFlag {
    /**
     * No flags are set.
     */
    NONE = 0,
    /**
     * Indicates that the frame can be safely ignored if not understood.
     */
    IGNORE = 512,
    /**
     * Indicates that the frame contains metadata.
     */
    METADATA = 256
}

/**
 * RSocket frame flags, including utility to combine multiple flags.
 */
export const FrameFlag = Object.assign({
    /**
     * Combines multiple frame flags into a single numeric bitmask.
     *
     * @param flags List of frame flags to combine.
     * @returns Combined numeric flag.
     * @example
     * ```ts
     * const flags = FrameFlag.combine(FrameFlag.METADATA, FrameFlag.IGNORE);
     * ```
     */
    combine: (...flags: FrameFlag[]) => {
        let combined = 0;
        for (const flag of flags) combined |= flag;
        return combined;
    }
}, _FrameFlag);
/** Numeric bitmask accepted by every frame header. */
export type FrameFlag = number | _FrameFlag;

/**
 * Internal enum for keepalive-specific flags.
 */
enum _KeepaliveFlag {
    /**
     * If set, the receiver must respond with a KEEPALIVE frame.
     */
    RESPOND = 128
}

/**
 * Flags applicable to KEEPALIVE frames.
 */
export const KeepaliveFlag = Object.assign({}, _KeepaliveFlag, FrameFlag);
/** Numeric bitmask accepted by a KEEPALIVE frame. */
export type KeepaliveFlag = FrameFlag | _KeepaliveFlag;

/**
 * Enum for custom [extension frame]{@link ExtensionFrame} flags.
 */
enum _ExtensionFlag {
    EXT_1 = 128,
    EXT_2 = 64,
    EXT_3 = 32,
    EXT_4 = 16,
    EXT_5 = 8,
    EXT_6 = 4,
    EXT_7 = 2,
    EXT_8 = 1
}

/**
 * Flags applicable to EXT (extension) frames.
 */
export const ExtensionFlag = Object.assign({}, _ExtensionFlag, FrameFlag);
/** Numeric bitmask accepted by an EXT frame. */
export type ExtensionFlag = FrameFlag | _ExtensionFlag;

/**
 * Internal enum for setup frame flags.
 */
enum _SetupFlag {
    /**
     * Enables session resumption support.
     */
    RESUME = 128,
    /**
     * Enables lease-based flow control.
     */
    LEASE = 64,
}

/**
 * Flags applicable to SETUP frames.
 */
export const SetupFlag = Object.assign({}, _SetupFlag, FrameFlag);
/** Numeric bitmask accepted by a SETUP frame. */
export type SetupFlag = FrameFlag | _SetupFlag;

/**
 * Enum for the `FOLLOWS` flag, indicating that more fragments follow.
 */
enum FollowsFlag {
    /**
     * Indicates that this frame is followed by more fragments.
     */
    FOLLOWS = 128
}

/**
 * Flags applicable to [FIRE_AND_FORGET]{@link RequestFireAndForgetFrame} frames.
 */
export const FireAndForgetFlag = Object.assign({}, FollowsFlag, FrameFlag);
/** Numeric bitmask accepted by a REQUEST_FNF frame. */
export type FireAndForgetFlag = FrameFlag | FollowsFlag;

/**
 * Flags applicable to [REQUEST_RESPONSE]{@link RequestResponseFrame} frames (same as [FIRE_AND_FORGET]{@link RequestFireAndForgetFrame}.
 */
export const RequestResponseFlag = FireAndForgetFlag;
/** Numeric bitmask accepted by a REQUEST_RESPONSE frame. */
export type RequestResponseFlag = FireAndForgetFlag

/**
 * Flags applicable to [REQUEST_STREAM]{@link RequestStreamFrame} frames (same as  [REQUEST_RESPONSE]{@link RequestResponseFrame}).
 */
export const RequestStreamFlag = RequestResponseFlag;
/** Numeric bitmask accepted by a REQUEST_STREAM frame. */
export type RequestStreamFlag = RequestResponseFlag;

/**
 * Enum representing the `COMPLETE` flag, indicating stream completion.
 */
enum CompleteFlag {
    COMPLETE = 64
}

/**
 * Enum for payload-specific flags.
 */
enum _PayloadFlag {
    /**
     * Indicates the presence of the next payload fragment.
     */
    NEXT = 32
}

/**
 * Flags applicable to [REQUEST_CHANNEL]{@link RequestChannelFrame} frames.
 */
export const RequestChannelFlag = Object.assign({}, FollowsFlag, CompleteFlag, FrameFlag);
/** Numeric bitmask accepted by a REQUEST_CHANNEL frame. */
export type RequestChannelFlag = FrameFlag | FollowsFlag | CompleteFlag;

/**
 * Flags applicable to [PAYLOAD]{@link PayloadFrame} frames.
 */
export const PayloadFlag = Object.assign({}, _PayloadFlag, RequestChannelFlag);
/** Numeric bitmask accepted by a PAYLOAD frame. */
export type PayloadFlag = _PayloadFlag | RequestChannelFlag;
