import assert from "node:assert/strict";
import {describe, it} from "node:test";
import {
    cascadeBumps,
    incrementVersion,
    releaseEntries,
    strongestBump
} from "./release-logic.mjs";

const workspaces = [
    workspace("rsocket-frames-ts", "2.0.0"),
    workspace("rsocket-core-ts", "1.0.0", {"rsocket-frames-ts": "^2.0.0"}),
    workspace("rsocket-client-ts", "1.0.0", {"rsocket-core-ts": "^1.0.0"}),
    workspace("rsocket-server-ts", "1.0.0", {
        "rsocket-core-ts": "^1.0.0",
        "rsocket-frames-ts": "^2.0.0"
    }),
    workspace("rsocket-browser", "3.3.11", {
        "rsocket-core-ts": "^1.0.0",
        "rsocket-client-ts": "^1.0.0"
    })
];

describe("semantic versions", () => {
    it("selects the strongest increment and updates stable versions", () => {
        assert.equal(strongestBump("initial", "patch"), "patch");
        assert.equal(strongestBump("patch", "minor"), "minor");
        assert.equal(strongestBump("major", "minor"), "major");
        assert.equal(incrementVersion("1.0.0", "initial"), "1.0.0");
        assert.equal(incrementVersion("3.3.11", "patch"), "3.3.12");
        assert.equal(incrementVersion("3.3.11", "minor"), "3.4.0");
        assert.equal(incrementVersion("3.3.11", "major"), "4.0.0");
    });
});

describe("workspace release cascade", () => {
    it("releases every dependent after a Frames change", () => {
        const bumps = cascadeBumps(workspaces, new Map([["rsocket-frames-ts", "minor"]]));
        assert.deepEqual([...bumps], [
            ["rsocket-frames-ts", "minor"],
            ["rsocket-core-ts", "patch"],
            ["rsocket-client-ts", "patch"],
            ["rsocket-server-ts", "patch"],
            ["rsocket-browser", "patch"]
        ]);
    });

    it("keeps a stronger direct bump on a dependent", () => {
        const entries = releaseEntries(workspaces, new Map([
            ["rsocket-core-ts", "patch"],
            ["rsocket-client-ts", "major"]
        ]));
        assert.deepEqual(entries.map(({name, nextVersion, direct}) => ({name, nextVersion, direct})), [
            {name: "rsocket-core-ts", nextVersion: "1.0.1", direct: true},
            {name: "rsocket-client-ts", nextVersion: "2.0.0", direct: true},
            {name: "rsocket-server-ts", nextVersion: "1.0.1", direct: false},
            {name: "rsocket-browser", nextVersion: "3.3.12", direct: false}
        ]);
    });

    it("preserves source-change attribution when every cascade bump was API-derived", () => {
        const bumps = new Map([
            ["rsocket-core-ts", "minor"],
            ["rsocket-client-ts", "major"],
            ["rsocket-server-ts", "patch"],
            ["rsocket-browser", "patch"]
        ]);
        const entries = releaseEntries(workspaces, bumps, new Set(["rsocket-core-ts"]));

        assert.deepEqual(entries.map(({name, direct}) => ({name, direct})), [
            {name: "rsocket-core-ts", direct: true},
            {name: "rsocket-client-ts", direct: false},
            {name: "rsocket-server-ts", direct: false},
            {name: "rsocket-browser", direct: false}
        ]);
    });

    it("does not release requester packages after a server-only API change", () => {
        const entries = releaseEntries(workspaces, new Map([["rsocket-server-ts", "minor"]]));

        assert.deepEqual(entries.map(({name, bump}) => ({name, bump})), [
            {name: "rsocket-server-ts", bump: "minor"}
        ]);
    });

    it("does not publish a server for its test-only client dependency", () => {
        const graph = [
            workspace("rsocket-client-ts", "1.0.0"),
            {
                ...workspace("rsocket-server-ts", "1.0.0"),
                manifest: {
                    name: "rsocket-server-ts",
                    version: "1.0.0",
                    dependencies: {"rsocket-core-ts": "^1.0.0"},
                    devDependencies: {"rsocket-client-ts": "^1.0.0"}
                }
            },
            workspace("rsocket-browser", "3.3.11", {"rsocket-client-ts": "^1.0.0"})
        ];

        expectReleaseNames(cascadeBumps(graph, new Map([["rsocket-client-ts", "minor"]])), [
            "rsocket-client-ts",
            "rsocket-browser"
        ]);
    });
});

/** Creates the minimal workspace shape consumed by release logic. */
function workspace(name, version, dependencies = {}) {
    return {name, directory: name, manifest: {name, version, dependencies}};
}

/** Compares deterministic package names while hiding bump details irrelevant to the case. */
function expectReleaseNames(bumps, expected) {
    assert.deepEqual([...bumps.keys()], expected);
}
