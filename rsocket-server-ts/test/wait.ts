/** Yields one macrotask so Reactor queues can deliver transport signals. */
export function nextTurn(): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, 0));
}

/** Waits for a deterministic asynchronous condition with a bounded timeout. */
export async function waitFor(condition: () => boolean, timeoutMs = 1_000): Promise<void> {
    const deadline = Date.now() + timeoutMs;
    while (!condition()) {
        if (Date.now() >= deadline) throw new Error("Timed out waiting for protocol condition");
        await nextTurn();
    }
}
