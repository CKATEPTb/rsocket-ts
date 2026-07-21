/** Reactive Streams demand validation and saturation shared by both endpoint roles. */

/** Validates positive finite demand representable exactly by JavaScript. */
export function normalizeFiniteReactiveDemand(value: number): number {
    if (Number.isSafeInteger(value) && value > 0) return value;
    throw new RangeError("Reactive Streams demand must be a positive safe integer");
}

/** Validates finite or positive-infinite Reactive Streams demand. */
export function normalizeReactiveDemand(value: number): number {
    return value === Number.POSITIVE_INFINITY ? value : normalizeFiniteReactiveDemand(value);
}

/** Adds demand without overflowing JavaScript's exact integer range. */
export function addReactiveDemand(current: number, added: number): number {
    if (current === Number.POSITIVE_INFINITY || added === Number.POSITIVE_INFINITY) {
        return Number.POSITIVE_INFINITY;
    }
    return added >= Number.MAX_SAFE_INTEGER - current
        ? Number.MAX_SAFE_INTEGER
        : current + added;
}
