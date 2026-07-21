/** Regression tests for package dependency, source, ownership, and artifact boundaries. */
import assert from "node:assert/strict";
import {existsSync, readFileSync, readdirSync} from "node:fs";
import {describe, it} from "node:test";
import path from "node:path";
import {ROOT, WORKSPACES} from "./workspaces.mjs";

const CLIENT = "rsocket-client-ts";
const SERVER = "rsocket-server-ts";
const CORE = "rsocket-core-ts";
const BROWSER = "rsocket-browser";
const ROOT_ONLY_PACKAGES = [
    BROWSER,
    CLIENT,
    CORE,
    "rsocket-frames-ts",
    SERVER
];
const RUNTIME_DEPENDENCY_FIELDS = [
    "dependencies",
    "peerDependencies",
    "optionalDependencies",
    "bundledDependencies"
];
const DIRECT_RUNTIME_DEPENDENCIES = {
    "rsocket-frames-ts": ["bebyte"],
    [CORE]: ["reactor-core-ts", "rsocket-frames-ts"],
    [CLIENT]: ["reactor-core-ts", CORE, "rsocket-frames-ts"],
    [SERVER]: ["reactor-core-ts", CORE, "rsocket-frames-ts"],
    [BROWSER]: [CLIENT, CORE]
};

describe("client/server package boundaries", () => {
    it("publishes one root entry point per package and no technical subpaths", () => {
        for (const packageName of ROOT_ONLY_PACKAGES) {
            const manifest = JSON.parse(readFileSync(path.join(ROOT, packageName, "package.json"), "utf8"));
            assert.deepEqual(Object.keys(manifest.exports ?? {}).sort(), ["."], `${packageName} must export only its root`);
        }
    });

    it("contains no opposite endpoint in production dependency graphs", () => {
        assertNoRuntimeDependency(BROWSER, SERVER);
        assertNoRuntimeDependency(CLIENT, SERVER);
        assertNoRuntimeDependency(SERVER, CLIENT);
        assertNoRuntimeDependency(CORE, CLIENT);
        assertNoRuntimeDependency(CORE, SERVER);
    });

    it("declares only the runtime dependencies each package directly imports", () => {
        for (const [packageName, expected] of Object.entries(DIRECT_RUNTIME_DEPENDENCIES)) {
            const manifest = JSON.parse(readFileSync(path.join(ROOT, packageName, "package.json"), "utf8"));
            assert.deepEqual(
                Object.keys(manifest.dependencies ?? {}).sort(),
                [...expected].sort(),
                `${packageName} runtime dependency surface drifted`
            );
        }
    });

    it("keeps every internal runtime dependency before its dependents", () => {
        const positions = new Map(WORKSPACES.map(({name}, index) => [name, index]));
        for (const workspace of WORKSPACES) {
            const manifest = JSON.parse(readFileSync(path.join(ROOT, workspace.directory, "package.json"), "utf8"));
            for (const dependency of Object.keys(manifest.dependencies ?? {})) {
                const dependencyPosition = positions.get(dependency);
                if (dependencyPosition === undefined) continue;
                assert.ok(
                    dependencyPosition < positions.get(workspace.name),
                    `${dependency} must precede ${workspace.name} in release order`
                );
            }
        }
    });

    it("contains no opposite endpoint imports in production source", () => {
        assertTreeExcludes(path.join(ROOT, CLIENT, "src"), [SERVER, `../${SERVER}`, `../../${SERVER}`]);
        assertTreeExcludes(path.join(ROOT, SERVER, "src"), [CLIENT, `../${CLIENT}`, `../../${CLIENT}`]);
        assertTreeExcludes(path.join(ROOT, CORE, "src"), [CLIENT, SERVER]);
    });

    it("keeps shared implementations in Core instead of endpoint-local copies", () => {
        for (const relative of [
            `${CLIENT}/src/async/queue.ts`,
            `${CLIENT}/src/resume/replay.ts`,
            `${CLIENT}/src/stream/cancel.ts`,
            `${CLIENT}/src/stream/demand.ts`,
            `${CLIENT}/src/tcp/framing.ts`,
            `${CLIENT}/src/websocket/frames.ts`,
            `${SERVER}/src/controllers/route.ts`,
            `${SERVER}/src/fragmentation/request.ts`,
            `${SERVER}/src/resume/replay.ts`,
            `${SERVER}/src/stream/async-queue.ts`,
            `${SERVER}/src/stream/cancel.ts`,
            `${SERVER}/src/stream/demand.ts`,
            `${SERVER}/src/tcp/framing.ts`,
            `${SERVER}/src/transport/binding.ts`
        ]) {
            assert.equal(existsSync(path.join(ROOT, relative)), false, `${relative} must be owned by ${CORE}`);
        }
        for (const relative of [
            `${CORE}/src/async/index.ts`,
            `${CORE}/src/async/cancel.ts`,
            `${CORE}/src/flow/index.ts`,
            `${CORE}/src/resume/index.ts`,
            `${CORE}/src/reassembly/request.ts`,
            `${CORE}/src/routing/index.ts`,
            `${CORE}/src/tcp/framing.ts`,
            `${CORE}/src/tcp/connection.ts`,
            `${CORE}/src/transport/binding.ts`,
            `${CORE}/src/websocket/index.ts`
        ]) {
            assert.equal(existsSync(path.join(ROOT, relative)), true, `${relative} is required`);
        }
    });

    if (process.env.VERIFY_DIST === "true") {
        it("contains no cross-endpoint references in built artifacts", () => {
            assertTreeExcludes(requiredDist(CLIENT), [SERVER]);
            assertTreeExcludes(requiredDist(SERVER), [CLIENT]);
            assertTreeExcludes(requiredDist(CORE), [CLIENT, SERVER]);
        });
    }
});

/** Verifies that one forbidden package is absent from every install-time dependency field. */
function assertNoRuntimeDependency(packageName, forbiddenName) {
    const manifest = JSON.parse(readFileSync(path.join(ROOT, packageName, "package.json"), "utf8"));
    for (const field of RUNTIME_DEPENDENCY_FIELDS) {
        const dependencies = manifest[field];
        const names = Array.isArray(dependencies) ? dependencies : Object.keys(dependencies ?? {});
        assert.equal(
            names.includes(forbiddenName),
            false,
            `${packageName} ${field} must not reference ${forbiddenName}`
        );
    }
}

/** Scans source-like files below one folder for forbidden package references. */
function assertTreeExcludes(folder, forbiddenValues) {
    for (const file of sourceFiles(folder)) {
        const source = readFileSync(file, "utf8");
        for (const forbidden of forbiddenValues) {
            assert.equal(source.includes(forbidden), false, `${path.relative(ROOT, file)} references ${forbidden}`);
        }
    }
}

/** Returns JavaScript and TypeScript files recursively in deterministic filesystem order. */
function sourceFiles(folder) {
    return readdirSync(folder, {withFileTypes: true}).flatMap((entry) => {
        const target = path.join(folder, entry.name);
        if (entry.isDirectory()) return sourceFiles(target);
        return /\.(?:[cm]?js|ts|map)$/.test(entry.name) ? [target] : [];
    });
}

/** Requires one completed package build before artifact boundary checks. */
function requiredDist(packageName) {
    const folder = path.join(ROOT, packageName, "dist");
    assert.equal(existsSync(folder), true, `${packageName} must be built before artifact verification`);
    return folder;
}
