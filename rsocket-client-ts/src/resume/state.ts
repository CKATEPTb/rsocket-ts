/** Last known resumable byte positions for one logical RSocket client session. */
export interface RSocketResumeState {
    /** Last implied client-to-server byte position sent by this requester. */
    readonly clientPosition: bigint;
    /** Earliest retained client position that can still be replayed. */
    readonly firstAvailableClientPosition: bigint;
    /** Last implied server-to-client byte position received by this requester. */
    readonly serverPosition: bigint;
}
