/** Independent persistent metadata tests for the client package. */
import {describe, expect, it} from "vitest";
import {
    Metadata,
    WellKnownAuthType,
    WellKnownMimeType
} from "rsocket-frames-ts";
import {route, type RSocketPayload, type RSocketPayloadInput} from "rsocket-core-ts";
import {RSocketMetadataStore} from "@/metadata/index.js";

const compositeMimeType = WellKnownMimeType.MESSAGE_RSOCKET_COMPOSITE_METADATA;
const routingMimeType = WellKnownMimeType.MESSAGE_RSOCKET_ROUTING;
const authenticationMimeType = WellKnownMimeType.MESSAGE_RSOCKET_AUTHENTICATION;

describe("RSocketMetadataStore", () => {
    it("adds, reads, and removes MIME-keyed metadata atomically", () => {
        const store = new RSocketMetadataStore(compositeMimeType);
        const authenticated = store.update((metadata) => {
            expect(metadata.size).toBe(0);
            metadata.set(authenticationMimeType, WellKnownAuthType.BEARER.auth("token"));
            expect(metadata.has(authenticationMimeType)).toBe(true);
        });

        expect(store.size).toBe(1);
        expect(authenticated.get(authenticationMimeType)?.mimeType).toBe(authenticationMimeType);

        const anonymous = store.update((metadata) => {
            expect(metadata.get(authenticationMimeType)).toBeDefined();
            metadata.remove(authenticationMimeType);
        });
        expect(anonymous.has(authenticationMimeType)).toBe(false);
        expect(store.size).toBe(0);
    });

    it("stores false and null values instead of treating them as removal sentinels", () => {
        const store = new RSocketMetadataStore(compositeMimeType);

        store.update((metadata) => metadata.set(WellKnownMimeType.APPLICATION_JSON, false));
        expect(compositeEntries(store.payload({data: "false"}))
            .find((entry) => entry.mimeType === WellKnownMimeType.APPLICATION_JSON)?.payload)
            .toBe(false);

        store.update((metadata) => metadata.set(WellKnownMimeType.APPLICATION_JSON, null));
        expect(compositeEntries(store.payload({data: "null"}))
            .find((entry) => entry.mimeType === WellKnownMimeType.APPLICATION_JSON)?.payload)
            .toBeNull();

        store.update((metadata) => metadata.remove(WellKnownMimeType.APPLICATION_JSON));
        expect(store.size).toBe(0);
    });

    it("rolls back a patch when a later iterable entry is invalid", () => {
        const store = new RSocketMetadataStore(compositeMimeType);
        const stable = WellKnownMimeType.TEXT_PLAIN.toMetadata("stable");
        store.update(stable);

        expect(() => store.update([
            [WellKnownMimeType.APPLICATION_JSON, {tenant: "must-not-commit"}],
            ["invalid-mime", "invalid-value"] as never
        ])).toThrow("iterable entries");

        store.update((metadata) => {
            expect(metadata.get(WellKnownMimeType.TEXT_PLAIN)).toBe(stable);
            expect(metadata.has(WellKnownMimeType.APPLICATION_JSON)).toBe(false);
            return [];
        });
    });

    it("rejects a Metadata value stored under a different MIME key", () => {
        const store = new RSocketMetadataStore(compositeMimeType);
        const json = WellKnownMimeType.APPLICATION_JSON.toMetadata({tenant: "acme"});

        expect(() => store.update(new Map([
            [WellKnownMimeType.TEXT_PLAIN, json]
        ]))).toThrow("must match its MimeType map key");
        expect(store.size).toBe(0);
    });

    it("restricts direct SETUP metadata to its negotiated MIME type", () => {
        const store = new RSocketMetadataStore(routingMimeType);

        expect(() => store.update((metadata) => {
            metadata.set(authenticationMimeType, WellKnownAuthType.BEARER.auth("token"));
        })).toThrow("cannot store metadata MIME");

        store.update((metadata) => metadata.set(routingMimeType, ["default.route"]));
        const payload = store.payload({data: {id: 1}}) as RSocketPayload<unknown, Metadata<any>>;

        expect(payload.metadata?.mimeType).toBe(routingMimeType);
        expect(payload.metadata?.payload).toEqual(["default.route"]);
    });

    it("merges defaults while per-interaction metadata wins for the same MIME", () => {
        const store = new RSocketMetadataStore(compositeMimeType);
        store.update((metadata) => {
            metadata.set(routingMimeType, ["default.route"]);
            metadata.set(authenticationMimeType, WellKnownAuthType.BEARER.auth("token"));
        });
        const interactionRoute = route("interaction.route");
        const payload = store.payload({
            data: {id: 7},
            metadata: interactionRoute
        });
        const entries = compositeEntries(payload);

        expect((payload as RSocketPayload<{id: number}, unknown>).data).toEqual({id: 7});
        expect(entries.filter((entry) => entry.mimeType === routingMimeType)).toHaveLength(1);
        expect(entries.find((entry) => entry.mimeType === routingMimeType)?.payload)
            .toEqual(["interaction.route"]);
        expect(entries.find((entry) => entry.mimeType === authenticationMimeType)?.payload)
            .toMatchObject({data: "token"});
    });

    it("caches a merged result for a stable Metadata object", () => {
        const store = new RSocketMetadataStore(compositeMimeType);
        store.update(WellKnownMimeType.TEXT_PLAIN.toMetadata("default"));
        const interactionRoute = route("account.find");
        const payload = {data: {id: 1}, metadata: interactionRoute};

        expect(payloadMetadata(store.payload(payload)))
            .toBe(payloadMetadata(store.payload(payload)));
    });

    it("keeps channel inputs lazy and reusable while attaching defaults only to the opening payload", () => {
        const store = new RSocketMetadataStore(compositeMimeType);
        store.update(WellKnownMimeType.TEXT_PLAIN.toMetadata("default"));
        let nextCalls = 0;
        const source: Iterable<RSocketPayloadInput<{id: number}, unknown>> = {
            *[Symbol.iterator]() {
                nextCalls += 1;
                yield {data: {id: 1}};
                nextCalls += 1;
                yield {data: {id: 2}, metadata: route("second")};
            }
        };

        const wrapped = store.channel(source) as Iterable<RSocketPayloadInput<{id: number}, unknown>>;
        expect(nextCalls).toBe(0);
        const iterator = wrapped[Symbol.iterator]();
        const first = iterator.next();
        expect(nextCalls).toBe(1);
        expect(compositeEntries(first.value)).toHaveLength(1);
        const second = iterator.next();
        expect(nextCalls).toBe(2);
        const secondEntries = compositeEntries(second.value);
        expect(secondEntries).toHaveLength(1);
        expect(secondEntries[0]?.mimeType).toBe(routingMimeType);

        const repeated = Array.from(wrapped);
        expect(repeated).toHaveLength(2);
        expect(nextCalls).toBe(4);
        expect(compositeEntries(repeated[0]!)).toHaveLength(1);
    });

    it("preserves request metadata for an otherwise empty channel", () => {
        const store = new RSocketMetadataStore(compositeMimeType);
        store.update(authenticationMimeType.toMetadata(WellKnownAuthType.BEARER.auth("token")));

        const wrapped = store.channel([]) as Iterable<RSocketPayloadInput<unknown, unknown>>;
        const values = Array.from(wrapped);

        expect(values).toHaveLength(1);
        expect(compositeEntries(values[0]!)[0]?.mimeType).toBe(authenticationMimeType);
    });

    it("merges metadata-push values and preserves request option fields", () => {
        const store = new RSocketMetadataStore(compositeMimeType);
        store.update(WellKnownMimeType.TEXT_PLAIN.toMetadata("default"));
        const push = store.metadata(route("refresh"));
        const options = store.requestOptions({
            dataMimeType: WellKnownMimeType.TEXT_PLAIN,
            metadataMimeType: routingMimeType,
            timeout: 250
        });

        expect(compositeEntries(push as Metadata<any>)).toHaveLength(2);
        expect(options).toEqual({
            dataMimeType: WellKnownMimeType.TEXT_PLAIN,
            metadataMimeType: compositeMimeType,
            timeout: 250
        });
    });

    it("returns unchanged inputs on the empty-store fast path", () => {
        const store = new RSocketMetadataStore(compositeMimeType);
        const payload = {data: {id: 1}};
        const channel = [payload];
        const push = route("refresh");
        const options = {timeout: 100};

        expect(store.payload(payload)).toBe(payload);
        expect(store.channel(channel)).toBe(channel);
        expect(store.metadata(push)).toBe(push);
        expect(store.requestOptions(options)).toBe(options);
    });
});

/** Extracts the encoded metadata object from a payload input. */
function payloadMetadata(input: RSocketPayloadInput<any, any>): Metadata<any> {
    if (input instanceof Metadata) return input;
    const value = (input as RSocketPayload<any, any>).metadata;
    if (!(value instanceof Metadata)) throw new Error("Expected encoded request metadata");
    return value;
}

/** Decodes one composite metadata value into its ordered entries. */
function compositeEntries(input: RSocketPayloadInput<any, any> | Metadata<any>): Metadata<any>[] {
    const value = input instanceof Metadata ? input : payloadMetadata(input);
    if (value.mimeType !== compositeMimeType) throw new Error("Expected composite metadata");
    return compositeMimeType.toMetadata(value.toUint8Array(), false).payload;
}
