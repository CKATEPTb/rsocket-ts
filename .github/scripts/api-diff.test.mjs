import assert from "node:assert/strict";
import {describe, it} from "node:test";
import {comparePublicApi} from "./api-diff.mjs";

describe("public API semantic versions", () => {
    it("uses patch when exported declarations are unchanged", () => {
        const previous = snapshot();
        const current = structuredClone(previous);

        assert.deepEqual(comparePublicApi(previous, current), {
            bump: "patch",
            breaking: [],
            additions: []
        });
    });

    it("rejects incompatible snapshot identities instead of guessing a version", () => {
        const previous = snapshot();
        const wrongFormat = {...snapshot(), formatVersion: 2};
        const wrongPackage = {...snapshot(), packageName: "another-package"};

        assert.throws(() => comparePublicApi(previous, wrongFormat), /snapshot formats/);
        assert.throws(() => comparePublicApi(previous, wrongPackage), /fixture and another-package/);
    });

    it("uses minor for new entry points and exported declarations", () => {
        const previous = snapshot();
        const current = snapshot({
            exports: {Client: "Class", ClientOptions: "Interface"},
            items: {
                "fixture!Client:class": item("Class", "Client", "client-v1"),
                "fixture!ClientOptions:interface": item("Interface", "ClientOptions", "options-v1")
            }
        });
        current.entries["./transport"] = entry({Transport: "Interface"}, {
            "fixture!Transport:interface": item("Interface", "Transport", "transport-v1")
        });

        const result = comparePublicApi(previous, current);

        assert.equal(result.bump, "minor");
        assert.equal(result.breaking.length, 0);
        assert.equal(result.additions.length, 3);
    });

    it("uses major when an export disappears or an existing signature changes", () => {
        const previous = snapshot();
        const current = snapshot({exports: {}, items: {
            "fixture!Client:class": item("Class", "Client", "client-v2")
        }});

        const result = comparePublicApi(previous, current);

        assert.equal(result.bump, "major");
        assert.ok(result.breaking.some((message) => message.includes("Removed . export Client")));
        assert.ok(result.breaking.some((message) => message.includes("Changed . API item")));
    });

    it("uses major when conditional export precedence changes", () => {
        const previous = snapshot();
        const current = structuredClone(previous);
        current.entries["."].conditions.reverse();

        const result = comparePublicApi(previous, current);

        assert.equal(result.bump, "major");
        assert.deepEqual(result.breaking, ["Changed . export condition precedence"]);
    });

    it("treats a required interface member as breaking and an optional member as additive", () => {
        const parent = "fixture!ClientOptions:interface";
        const previous = snapshot({exports: {ClientOptions: "Interface"}, items: {
            [parent]: item("Interface", "ClientOptions", "options-v1")
        }});
        const required = structuredClone(previous);
        required.entries["."].items[`${parent}#timeout:member`] = item(
            "PropertySignature",
            "timeout",
            "timeout-v1",
            parent
        );
        const optional = structuredClone(previous);
        optional.entries["."].items[`${parent}#timeout:member`] = item(
            "PropertySignature",
            "timeout",
            "timeout-v1",
            parent,
            {optional: true}
        );

        assert.equal(comparePublicApi(previous, required).bump, "major");
        assert.equal(comparePublicApi(previous, optional).bump, "minor");
    });

    it("treats an additional overload as additive but a new abstract method as breaking", () => {
        const interfaceParent = "fixture!Requester:interface";
        const classParent = "fixture!BaseClient:class";
        const previous = snapshot({exports: {Requester: "Interface", BaseClient: "Class"}, items: {
            [interfaceParent]: item("Interface", "Requester", "requester-v1"),
            [`${interfaceParent}#request:member(1)`]: item("MethodSignature", "request", "request-v1", interfaceParent),
            [classParent]: item("Class", "BaseClient", "base-v1")
        }});
        const overload = structuredClone(previous);
        overload.entries["."].items[`${interfaceParent}#request:member(2)`] = item(
            "MethodSignature",
            "request",
            "request-v2",
            interfaceParent
        );
        const abstractMethod = structuredClone(previous);
        abstractMethod.entries["."].items[`${classParent}#connect:member(1)`] = item(
            "Method",
            "connect",
            "connect-v1",
            classParent,
            {abstract: true}
        );

        assert.equal(comparePublicApi(previous, overload).bump, "minor");
        assert.equal(comparePublicApi(previous, abstractMethod).bump, "major");
    });
});

/** Creates a package snapshot with one stable root entry point. */
function snapshot(overrides = {}) {
    return {
        formatVersion: 1,
        packageName: "fixture",
        entries: {
            ".": {
                ...entry({Client: "Class"}, {
                    "fixture!Client:class": item("Class", "Client", "client-v1")
                }),
                ...overrides
            }
        }
    };
}

/** Creates one entry-point snapshot. */
function entry(exports, items) {
    return {conditions: ["default", "types"], exports, items};
}

/** Creates one declaration item snapshot. */
function item(kind, name, hash, parent = "", options = {}) {
    return {kind, name, hash, parent, optional: false, abstract: false, ...options};
}
