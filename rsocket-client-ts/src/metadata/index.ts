/**
 * Client-wide metadata overlay helpers.
 *
 * The high-level socket uses these helpers to keep an immutable metadata map
 * that is merged into outgoing interactions using the metadata MIME negotiated
 * by SETUP.
 */
import {Metadata, MimeType, type Payload, WellKnownMimeType} from "rsocket-frames-ts";
import {
    compositeMetadataEntries,
    encodeMetadataInput,
    metadata as encodeMetadataValue,
    type RSocketPayloadInput
} from "rsocket-core-ts";
import {
    channelInputIterable,
    isChannelInputAsyncIterable,
    isChannelInputIterable
} from "@/channel/input.js";
import type {RSocketChannelInput, RSocketRequestOptions} from "@/client/types.js";

/** Shared immutable empty metadata list for request hot paths. */
const EMPTY_METADATA_ENTRIES: readonly Metadata<any>[] = [];
/**
 * Value accepted by `RSocket.metadataUpdate(...)` for one MIME entry.
 */
export type RSocketClientMetadataValue = unknown;

/**
 * Immutable view of metadata entries currently attached to outgoing requests.
 */
export type RSocketMetadataMap = ReadonlyMap<MimeType<any>, Metadata<any>>;

/**
 * One MIME-keyed metadata patch entry accepted by `RSocket.metadataUpdate(...)`.
 */
export type RSocketMetadataPatchEntry = readonly [MimeType<any>, RSocketClientMetadataValue];

/**
 * Patch object accepted by `RSocket.metadataUpdate(...)`.
 */
export type RSocketMetadataPatch =
    | Metadata<any>
    | ReadonlyMap<MimeType<any>, RSocketClientMetadataValue>
    | Iterable<Metadata<any> | RSocketMetadataPatchEntry>;

/**
 * Function form accepted by `RSocket.metadataUpdate(...)`.
 */
export type RSocketMetadataUpdater = (
    metadata: RSocketMetadataEditor
) => RSocketMetadataPatch | void;

/**
 * Transactional MIME-keyed metadata editor supplied to `metadataUpdate(...)`.
 */
export interface RSocketMetadataEditor {
    /** Number of metadata entries currently stored by the client. */
    readonly size: number;

    /** Returns the encoded entry stored for one MIME type. */
    get<T>(mimeType: MimeType<T>): Metadata<T> | undefined;

    /** Returns whether one MIME type currently has an entry. */
    has<T>(mimeType: MimeType<T>): boolean;

    /** Encodes and stores a value matching the supplied `MimeType<T>`. */
    set<T>(mimeType: MimeType<T>, value: T | Metadata<T>): void;

    /** Removes the entry associated with one MIME type. */
    remove<T>(mimeType: MimeType<T>): void;
}

/**
 * Mutable metadata storage owned by the high-level socket.
 */
type RSocketMetadataState = Map<string, Metadata<any>>;

/** Local structural view of an input envelope carrying raw or pre-encoded values. */
interface MetadataPayloadEnvelope<D, M> {
    readonly data?: D | Payload<unknown>;
    readonly metadata?: M | Metadata<unknown>;
    readonly dataMimeType?: MimeType<D>;
    readonly metadataMimeType?: MimeType<M>;
}

/**
 * Owns persistent client metadata and merges it into individual interactions.
 *
 * The store keeps derived entry/value caches synchronized after each
 * transactional update and hides mutable maps from high-level client facades.
 */
export class RSocketMetadataStore {
    private readonly state: RSocketMetadataState = new Map();
    private readonly metadataRequestOptions: RSocketRequestOptions;
    private entries: readonly Metadata<any>[] = EMPTY_METADATA_ENTRIES;
    private value: Metadata<any> | undefined;
    private mergeCache: WeakMap<Metadata<any>, Metadata<any>> | undefined;

    /** Creates a store for the metadata MIME negotiated by SETUP. */
    constructor(private readonly setupMetadataMimeType: MimeType<any>) {
        this.metadataRequestOptions = Object.freeze({metadataMimeType: setupMetadataMimeType});
    }

    /** Number of persistent MIME-keyed entries. */
    get size(): number {
        return this.state.size;
    }

    /** Applies one atomic update and returns an immutable MIME-keyed snapshot. */
    update(update: RSocketMetadataPatch | RSocketMetadataUpdater): RSocketMetadataMap {
        const snapshot = applyMetadataUpdate(this.state, update, this.setupMetadataMimeType);
        this.entries = metadataEntries(this.state);
        this.value = clientMetadataValue(this.entries, this.setupMetadataMimeType);
        this.mergeCache = undefined;
        return snapshot;
    }

    /** Merges persistent metadata into one request payload. */
    payload<D, M>(
        payload: RSocketPayloadInput<D, M>,
        metadataMimeType?: MimeType<any>
    ): RSocketPayloadInput<D, M> {
        if (this.entries.length === 0 && !requiresMetadataAdaptation(metadataMimeType, this.setupMetadataMimeType)) {
            return payload;
        }
        return withClientMetadata(
            payload,
            this.entries,
            metadataMimeType,
            this.value,
            this.cache(),
            this.setupMetadataMimeType
        );
    }

    /** Merges persistent metadata into each request-channel input item. */
    channel<D, M>(
        payloads: RSocketChannelInput<D, M>,
        metadataMimeType?: MimeType<any>
    ): RSocketChannelInput<D, M> {
        if (this.entries.length === 0 && !requiresMetadataAdaptation(metadataMimeType, this.setupMetadataMimeType)) {
            return payloads;
        }
        return withClientMetadataInput(
            payloads,
            this.entries,
            metadataMimeType,
            this.value,
            this.cache(),
            this.setupMetadataMimeType
        );
    }

    /** Merges persistent metadata into one connection-level metadata value. */
    metadata<M>(value: M | Metadata<M>, metadataMimeType?: MimeType<any>): M | Metadata<any> {
        if (this.entries.length === 0 && !requiresMetadataAdaptation(metadataMimeType, this.setupMetadataMimeType)) {
            return value;
        }
        return mergeClientMetadata(
            this.entries,
            value,
            metadataMimeType,
            this.setupMetadataMimeType,
            this.cache()
        );
    }

    /** Selects composite request encoding when persistent metadata requires it. */
    requestOptions(options: RSocketRequestOptions): RSocketRequestOptions {
        if (
            this.entries.length === 0 &&
            !requiresMetadataAdaptation(options.metadataMimeType, this.setupMetadataMimeType)
        ) return options;
        if (sameMetadataMimeType(options.metadataMimeType, this.setupMetadataMimeType)) return options;
        if (options.dataMimeType === undefined && options.timeout === undefined) {
            return this.metadataRequestOptions;
        }
        const next: RSocketRequestOptions = {
            metadataMimeType: this.setupMetadataMimeType
        };
        if (options.dataMimeType !== undefined) {
            (next as {dataMimeType?: RSocketRequestOptions["dataMimeType"]}).dataMimeType = options.dataMimeType;
        }
        if (options.timeout !== undefined) {
            (next as {timeout?: number}).timeout = options.timeout;
        }
        return next;
    }

    /** Lazily allocates the identity cache used for per-request metadata overlays. */
    private cache(): WeakMap<Metadata<any>, Metadata<any>> {
        return this.mergeCache ??= new WeakMap();
    }
}

/**
 * Applies one metadata patch and returns the next immutable metadata view.
 */
function applyMetadataUpdate(
    state: RSocketMetadataState,
    update: RSocketMetadataPatch | RSocketMetadataUpdater,
    setupMetadataMimeType?: MimeType<any>
): RSocketMetadataMap {
    const next = new Map(state);
    const patch = typeof update === "function"
        ? update(new MetadataEditor(next, setupMetadataMimeType))
        : update;
    if (patch !== undefined) applyMetadataPatch(next, patch, setupMetadataMimeType);
    replaceMetadataState(state, next);
    return metadataSnapshot(state);
}

/**
 * Mutable transaction view used only for the duration of one metadata update.
 */
class MetadataEditor implements RSocketMetadataEditor {
    /** Creates an editor over an isolated candidate state. */
    constructor(
        private readonly state: RSocketMetadataState,
        private readonly setupMetadataMimeType?: MimeType<any>
    ) {
    }

    /** Number of entries in the candidate state. */
    get size(): number {
        return this.state.size;
    }

    /** Returns one candidate entry by MIME type. */
    get<T>(mimeType: MimeType<T>): Metadata<T> | undefined {
        return this.state.get(metadataKey(mimeType)) as Metadata<T> | undefined;
    }

    /** Checks one candidate entry by MIME type. */
    has<T>(mimeType: MimeType<T>): boolean {
        return this.state.has(metadataKey(mimeType));
    }

    /** Encodes and stores one candidate entry. */
    set<T>(mimeType: MimeType<T>, value: T | Metadata<T>): void {
        applyMetadataValue(this.state, mimeType, value, this.setupMetadataMimeType);
    }

    /** Removes one candidate entry. */
    remove<T>(mimeType: MimeType<T>): void {
        assertMetadataMimeTypeSupported(mimeType, this.setupMetadataMimeType);
        this.state.delete(metadataKey(mimeType));
    }
}

/**
 * Applies a direct patch to an isolated metadata state.
 */
function applyMetadataPatch(
    state: RSocketMetadataState,
    patch: RSocketMetadataPatch,
    setupMetadataMimeType?: MimeType<any>
): void {
    if (patch instanceof Metadata) {
        applyMetadataValue(state, patch.mimeType, patch, setupMetadataMimeType);
        return;
    }

    if (isMetadataPatchIterable(patch)) {
        for (const entry of patch) applyMetadataPatchEntry(state, entry, setupMetadataMimeType);
        return;
    }

    throw new TypeError(
        "RSocket.metadataUpdate expects Metadata, Map<MimeType, value>, or iterable Metadata/[MimeType, value] entries."
    );
}

/**
 * Commits a validated metadata patch without exposing partially applied state.
 */
function replaceMetadataState(state: RSocketMetadataState, next: RSocketMetadataState): void {
    state.clear();
    for (const [key, value] of next) state.set(key, value);
}

/**
 * Returns an immutable map view of the current metadata state.
 */
function metadataSnapshot(state: RSocketMetadataState): RSocketMetadataMap {
    if (state.size === 0) return new Map();
    const snapshot = new Map<MimeType<any>, Metadata<any>>();
    for (const metadata of state.values()) snapshot.set(metadata.mimeType, metadata);
    return snapshot;
}

/**
 * Adds client-wide metadata entries to one outgoing payload input.
 */
function withClientMetadata<D, M>(
    payload: RSocketPayloadInput<D, M>,
    entries: readonly Metadata<any>[],
    metadataMimeType?: MimeType<any>,
    clientMetadata?: Metadata<any>,
    mergeCache?: WeakMap<Metadata<any>, Metadata<any>>,
    targetMetadataMimeType: MimeType<any> = WellKnownMimeType.MESSAGE_RSOCKET_COMPOSITE_METADATA
): RSocketPayloadInput<D, M> {
    if (payload instanceof Metadata) {
        return mergeClientMetadata(entries, payload, undefined, targetMetadataMimeType, mergeCache);
    }
    if (isPayloadEnvelope(payload)) {
        return envelopeWithMetadata(
            payload,
            entries,
            metadataMimeType,
            clientMetadata,
            mergeCache,
            targetMetadataMimeType
        );
    }
    const effectiveClientMetadata = clientMetadata ?? clientMetadataValue(entries, targetMetadataMimeType);
    return {
        data: payload,
        metadata: effectiveClientMetadata
    } as RSocketPayloadInput<D, M>;
}

/**
 * Merges client defaults with metadata supplied by one interaction.
 * Interaction metadata wins when both sources use the same MIME key.
 */
function mergeClientMetadata(
    entries: readonly Metadata<any>[],
    value: unknown,
    metadataMimeType: MimeType<any> | undefined,
    targetMetadataMimeType: MimeType<any>,
    mergeCache?: WeakMap<Metadata<any>, Metadata<any>>
): Metadata<any> {
    return metadataWithEntries(entries, value, metadataMimeType, targetMetadataMimeType, mergeCache);
}

/**
 * Adds client-wide request metadata to the opening item of a channel source.
 */
function withClientMetadataInput<D, M>(
    payloads: RSocketChannelInput<D, M>,
    entries: readonly Metadata<any>[],
    metadataMimeType?: MimeType<any>,
    clientMetadata?: Metadata<any>,
    mergeCache?: WeakMap<Metadata<any>, Metadata<any>>,
    targetMetadataMimeType: MimeType<any> = WellKnownMimeType.MESSAGE_RSOCKET_COMPOSITE_METADATA
): RSocketChannelInput<D, M> {
    const effectiveClientMetadata = clientMetadata ?? clientMetadataValue(entries, targetMetadataMimeType);
    if (!isChannelInputAsyncIterable(payloads) && isChannelInputIterable<RSocketPayloadInput<D, M>>(payloads)) {
        return withClientMetadataIterable(
            payloads,
            entries,
            metadataMimeType,
            effectiveClientMetadata,
            mergeCache,
            targetMetadataMimeType
        );
    }
    return withClientMetadataAsync(
        payloads,
        entries,
        metadataMimeType,
        effectiveClientMetadata,
        mergeCache,
        targetMetadataMimeType
    );
}

/**
 * Adds request-level client metadata to the first item of every synchronous iteration.
 */
function withClientMetadataIterable<D, M>(
    payloads: Iterable<RSocketPayloadInput<D, M>>,
    entries: readonly Metadata<any>[],
    metadataMimeType: MimeType<any> | undefined,
    clientMetadata: Metadata<any> | undefined,
    mergeCache: WeakMap<Metadata<any>, Metadata<any>> | undefined,
    targetMetadataMimeType: MimeType<any>
): Iterable<RSocketPayloadInput<D, M>> {
    return {
        /** Creates fresh metadata-aware state for each request-channel subscription. */
        *[Symbol.iterator]() {
            let first = true;
            for (const payload of payloads) {
                yield channelPayloadWithClientMetadata(
                    payload,
                    first,
                    entries,
                    metadataMimeType,
                    clientMetadata,
                    mergeCache,
                    targetMetadataMimeType
                );
                first = false;
            }
            if (first && clientMetadata !== undefined) yield metadataOnlyChannelPayload(clientMetadata);
        }
    };
}

/**
 * Adds request-level client metadata to the first item of every asynchronous iteration.
 */
function withClientMetadataAsync<D, M>(
    payloads: RSocketChannelInput<D, M>,
    entries: readonly Metadata<any>[],
    metadataMimeType: MimeType<any> | undefined,
    clientMetadata: Metadata<any> | undefined,
    mergeCache: WeakMap<Metadata<any>, Metadata<any>> | undefined,
    targetMetadataMimeType: MimeType<any>
): AsyncIterable<RSocketPayloadInput<D, M>> {
    return {
        /** Creates fresh metadata-aware state for each request-channel subscription. */
        async *[Symbol.asyncIterator]() {
            let first = true;
            for await (const payload of channelInputIterable(payloads)) {
                yield channelPayloadWithClientMetadata(
                    payload,
                    first,
                    entries,
                    metadataMimeType,
                    clientMetadata,
                    mergeCache,
                    targetMetadataMimeType
                );
                first = false;
            }
            if (first && clientMetadata !== undefined) yield metadataOnlyChannelPayload(clientMetadata);
        }
    };
}

/** Merges persistent request metadata only into the channel-opening payload. */
function channelPayloadWithClientMetadata<D, M>(
    payload: RSocketPayloadInput<D, M>,
    first: boolean,
    entries: readonly Metadata<any>[],
    metadataMimeType: MimeType<any> | undefined,
    clientMetadata: Metadata<any> | undefined,
    mergeCache: WeakMap<Metadata<any>, Metadata<any>> | undefined,
    targetMetadataMimeType: MimeType<any>
): RSocketPayloadInput<D, M> {
    return withClientMetadata(
        payload,
        first ? entries : EMPTY_METADATA_ENTRIES,
        metadataMimeType,
        first ? clientMetadata : undefined,
        first ? mergeCache : undefined,
        targetMetadataMimeType
    );
}

/** Creates a metadata-only opening payload without exposing encoded metadata in user generics. */
function metadataOnlyChannelPayload<D, M>(metadata: Metadata<any>): RSocketPayloadInput<D, M> {
    return {metadata} as RSocketPayloadInput<D, M>;
}

/**
 * Encodes current client metadata for the SETUP metadata MIME.
 */
function clientMetadataValue(
    entries: readonly Metadata<any>[],
    targetMetadataMimeType: MimeType<any> = WellKnownMimeType.MESSAGE_RSOCKET_COMPOSITE_METADATA
): Metadata<any> | undefined {
    if (entries.length === 0) return undefined;
    if (isCompositeMetadataMimeType(targetMetadataMimeType)) return compositeMetadataEntries(entries);
    const entry = entries[0] as Metadata<any>;
    if (entries.length === 1 && metadataKey(entry.mimeType) === metadataKey(targetMetadataMimeType)) return entry;
    throw unsupportedMetadataMimeType(targetMetadataMimeType);
}

/**
 * Returns whether one MIME is the RSocket composite metadata container.
 */
function isCompositeMetadataMimeType(mimeType: MimeType<any>): boolean {
    return metadataKey(mimeType) === metadataKey(WellKnownMimeType.MESSAGE_RSOCKET_COMPOSITE_METADATA);
}

/** Returns whether a per-interaction codec must be adapted to the SETUP metadata MIME. */
function requiresMetadataAdaptation(
    source: MimeType<any> | undefined,
    target: MimeType<any>
): boolean {
    return source !== undefined && !sameMetadataMimeType(source, target);
}

/** Compares optional MIME codecs by their wire MIME names. */
function sameMetadataMimeType(left: MimeType<any> | undefined, right: MimeType<any>): boolean {
    return left !== undefined && metadataKey(left) === metadataKey(right);
}

/**
 * Returns the metadata state as an ordered entry list for a single request.
 */
function metadataEntries(state: RSocketMetadataState): readonly Metadata<any>[] {
    if (state.size === 0) return EMPTY_METADATA_ENTRIES;
    const entries = new Array<Metadata<any>>(state.size);
    let index = 0;
    for (const metadata of state.values()) {
        entries[index] = metadata;
        index += 1;
    }
    return entries;
}

/**
 * Returns a stable string key for one MIME type.
 */
function metadataKey(mimeType: MimeType<any>): string {
    return mimeType.mimeType;
}

/**
 * Applies one MIME-keyed metadata patch entry.
 */
function applyMetadataValue(
    state: RSocketMetadataState,
    mimeType: MimeType<any>,
    value: RSocketClientMetadataValue,
    setupMetadataMimeType?: MimeType<any>
): void {
    const key = metadataKey(mimeType);
    assertMetadataMimeTypeSupported(mimeType, setupMetadataMimeType);
    if (value instanceof Metadata && metadataKey(value.mimeType) !== key) {
        throw new TypeError("RSocket.metadataUpdate Metadata value must match its MimeType map key.");
    }
    const metadata = value instanceof Metadata ? value : encodeMetadataValue(value as never, mimeType);
    state.set(metadataKey(metadata.mimeType), metadata);
}

/**
 * Applies one patch item from a metadata iterable.
 */
function applyMetadataPatchEntry(
    state: RSocketMetadataState,
    entry: Metadata<any> | RSocketMetadataPatchEntry,
    setupMetadataMimeType?: MimeType<any>
): void {
    if (entry instanceof Metadata) {
        applyMetadataValue(state, entry.mimeType, entry, setupMetadataMimeType);
        return;
    }

    if (isMetadataPatchEntry(entry)) {
        applyMetadataValue(state, entry[0], entry[1], setupMetadataMimeType);
        return;
    }

    throw new TypeError(
        "RSocket.metadataUpdate iterable entries must be Metadata or [MimeType, value] tuples."
    );
}

/**
 * Merges an object payload envelope with client-wide metadata entries.
 */
function envelopeWithMetadata<D, M>(
    payload: MetadataPayloadEnvelope<D, M>,
    entries: readonly Metadata<any>[],
    metadataMimeType: MimeType<any> | undefined,
    clientMetadata: Metadata<any> | undefined,
    mergeCache: WeakMap<Metadata<any>, Metadata<any>> | undefined,
    targetMetadataMimeType: MimeType<any>
): RSocketPayloadInput<D, M> {
    const metadata = "metadata" in payload && payload.metadata !== undefined
        ? metadataWithEntries(
            entries,
            payload.metadata,
            payload.metadataMimeType ?? metadataMimeType,
            targetMetadataMimeType,
            mergeCache
        )
        : clientMetadata ?? clientMetadataValue(entries, targetMetadataMimeType);
    const next: {
        data?: D | Payload<unknown>;
        metadata?: Metadata<any>;
        dataMimeType?: MimeType<D>;
        metadataMimeType: MimeType<any>;
    } = {
        metadataMimeType: targetMetadataMimeType
    };
    if (metadata !== undefined) {
        next.metadata = metadata;
    }
    if ("data" in payload) {
        next.data = payload.data;
    }
    if ("dataMimeType" in payload) {
        next.dataMimeType = payload.dataMimeType;
    }
    return next as RSocketPayloadInput<D, M>;
}

/**
 * Merges client-wide metadata with one payload metadata value.
 */
function metadataWithEntries(
    entries: readonly Metadata<any>[],
    value: unknown,
    mimeType: MimeType<any> | undefined,
    targetMetadataMimeType: MimeType<any>,
    mergeCache?: WeakMap<Metadata<any>, Metadata<any>>
): Metadata<any> {
    if (!isCompositeMetadataMimeType(targetMetadataMimeType)) {
        const direct = value instanceof Metadata
            ? value
            : encodeMetadataValue(value as never, mimeType ?? targetMetadataMimeType);
        return encodeMetadataInput(direct, targetMetadataMimeType);
    }
    if (value instanceof Metadata) {
        const cached = mergeCache?.get(value);
        if (cached !== undefined) return cached;
        let merged: Metadata<any>;
        if (
            metadataKey(value.mimeType) === metadataKey(WellKnownMimeType.MESSAGE_RSOCKET_COMPOSITE_METADATA) &&
            Array.isArray(value.payload)
        ) {
            merged = compositeMetadataEntries(metadataEntriesWith(entries, value.payload));
        } else {
            merged = compositeMetadataEntries(metadataEntriesWith(entries, value));
        }
        mergeCache?.set(value, merged);
        return merged;
    }
    return compositeMetadataEntries(
        metadataEntriesWith(entries, encodeMetadataValue(value as never, mimeType ?? WellKnownMimeType.APPLICATION_JSON))
    );
}

/**
 * Returns metadata entries plus one or more payload entries using one allocation.
 */
function metadataEntriesWith(
    entries: readonly Metadata<any>[],
    extra: Metadata<any> | readonly Metadata<any>[]
): readonly Metadata<any>[] {
    const entryCount = entries.length;
    const extraList = Array.isArray(extra) ? extra : undefined;
    const extraLength = extraList?.length ?? 1;
    if (extraLength === 0) return entries;
    let retainedCount = 0;
    for (let index = 0; index < entryCount; index++) {
        if (!metadataListHasMimeType(extra, extraList, (entries[index] as Metadata<any>).mimeType)) retainedCount += 1;
    }
    const result = new Array<Metadata<any>>(retainedCount + extraLength);
    let resultIndex = 0;
    for (let index = 0; index < entryCount; index++) {
        const entry = entries[index] as Metadata<any>;
        if (metadataListHasMimeType(extra, extraList, entry.mimeType)) continue;
        result[resultIndex] = entry;
        resultIndex += 1;
    }
    if (extraList === undefined) {
        result[resultIndex] = extra as Metadata<any>;
    } else {
        for (let index = 0; index < extraLength; index++) result[resultIndex + index] = extraList[index] as Metadata<any>;
    }
    return result;
}

/**
 * Checks whether request metadata contains an entry with one MIME key.
 */
function metadataListHasMimeType(
    extra: Metadata<any> | readonly Metadata<any>[],
    extraList: readonly Metadata<any>[] | undefined,
    mimeType: MimeType<any>
): boolean {
    const key = metadataKey(mimeType);
    if (extraList === undefined) return metadataKey((extra as Metadata<any>).mimeType) === key;
    for (let index = 0; index < extraList.length; index++) {
        if (metadataKey((extraList[index] as Metadata<any>).mimeType) === key) return true;
    }
    return false;
}

/**
 * Validates one persistent entry against the connection metadata MIME.
 */
function assertMetadataMimeTypeSupported(
    mimeType: MimeType<any>,
    setupMetadataMimeType: MimeType<any> | undefined
): void {
    if (
        setupMetadataMimeType === undefined ||
        isCompositeMetadataMimeType(setupMetadataMimeType) ||
        metadataKey(mimeType) === metadataKey(setupMetadataMimeType)
    ) {
        return;
    }
    throw unsupportedMetadataMimeType(setupMetadataMimeType, mimeType);
}

/**
 * Creates the user-facing error for metadata unsupported by a direct SETUP MIME.
 */
function unsupportedMetadataMimeType(
    setupMetadataMimeType: MimeType<any>,
    attemptedMimeType?: MimeType<any>
): TypeError {
    const attempted = attemptedMimeType === undefined
        ? "multiple metadata entries"
        : `metadata MIME "${metadataKey(attemptedMimeType)}"`;
    return new TypeError(
        `RSocket SETUP metadata MIME "${metadataKey(setupMetadataMimeType)}" cannot store ${attempted}. ` +
        "Configure MESSAGE_RSOCKET_COMPOSITE_METADATA to use multiple metadata types."
    );
}

/**
 * Detects payload envelopes without treating codec metadata objects as envelopes.
 */
function isPayloadEnvelope<D, M>(value: RSocketPayloadInput<D, M>): value is MetadataPayloadEnvelope<D, M> {
    if (typeof value !== "object" || value === null || value instanceof Metadata) return false;
    return "data" in value || "metadata" in value || "dataMimeType" in value || "metadataMimeType" in value;
}

/**
 * Detects iterable metadata patches while excluding codec metadata objects.
 */
function isMetadataPatchIterable(value: unknown): value is Iterable<Metadata<any> | RSocketMetadataPatchEntry> {
    return typeof value === "object"
        && value !== null
        && typeof (value as Partial<Iterable<Metadata<any>>>)[Symbol.iterator] === "function"
        && !(value instanceof Metadata);
}

/**
 * Detects a MIME-keyed tuple entry from a metadata update iterable.
 */
function isMetadataPatchEntry(value: unknown): value is RSocketMetadataPatchEntry {
    return Array.isArray(value)
        && value.length >= 2
        && value[0] instanceof MimeType;
}
