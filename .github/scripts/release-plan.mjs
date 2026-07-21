import {existsSync} from "node:fs";
import {mkdtemp, readFile, rm, writeFile} from "node:fs/promises";
import path from "node:path";
import {capture} from "./process.mjs";
import {comparePublicApi} from "./api-diff.mjs";
import {createPublicApiSnapshot} from "./api-model.mjs";
import {extractPublishedPackage, publishedVersions} from "./npm-baseline.mjs";
import {cascadeBumps, releaseEntries} from "./release-logic.mjs";
import {readWorkspaceManifests, ROOT} from "./workspaces.mjs";

const EMPTY_TREE = "4b825dc642cb6eb9a060e54bf8d69288fbee4904";
const args = argumentsMap(process.argv.slice(2));
const head = args.get("head") ?? process.env.HEAD_SHA ?? "HEAD";
const requestedBase = args.get("base") ?? process.env.BASE_SHA ?? `${head}^`;
const initialPush = /^0+$/.test(requestedBase);
const base = initialPush ? EMPTY_TREE : requestedBase;
const output = path.resolve(ROOT, args.get("output") ?? ".release-plan.json");
const workspaces = await readWorkspaceManifests();
const directlyChanged = new Set();

for (const workspace of workspaces) {
    const changedFiles = lines(capture("git", [
        "diff",
        "--name-only",
        "--diff-filter=ACMRD",
        base,
        head,
        "--",
        workspace.directory
    ], {cwd: ROOT}));
    if (changedFiles.length > 0) directlyChanged.add(workspace.name);
}

const candidates = new Set(cascadeBumps(
    workspaces,
    new Map([...directlyChanged].map((name) => [name, "patch"]))
).keys());
const temporaryFolder = await mkdtemp(path.join(ROOT, ".release-api-"));
const bumps = new Map();
const analyses = new Map();
const snapshots = new Map();
try {
    for (const workspace of workspaces) {
        if (!candidates.has(workspace.name)) continue;
        const currentSnapshot = await createPublicApiSnapshot(
            path.join(ROOT, workspace.directory),
            path.join(temporaryFolder, "current", workspace.name)
        );
        snapshots.set(workspace.name, currentSnapshot);
        const analysis = await analyzeWorkspaceApi(workspace, currentSnapshot, temporaryFolder);
        bumps.set(workspace.name, analysis.bump);
        analyses.set(workspace.name, analysis);
    }
} finally {
    await rm(temporaryFolder, {recursive: true, force: true});
}

const releases = releaseEntries(workspaces, bumps, directlyChanged).map((release) => ({
    ...release,
    api: analyses.get(release.name),
    apiSnapshot: snapshots.get(release.name)
}));
const plan = {
    base: requestedBase,
    head,
    generatedAt: new Date().toISOString(),
    releases
};

await writeFile(output, `${JSON.stringify(plan, null, 2)}\n`);
if (releases.length === 0) {
    console.log("No package release is required for this push.");
} else {
    console.log("Release plan:");
    for (const release of releases) {
        const source = release.direct ? "changed" : "dependency cascade";
        const api = release.api;
        const summary = release.bump === "initial"
            ? "first publication"
            : `${api.breaking.length} breaking, ${api.additions.length} additive API changes`;
        console.log(
            `- ${release.name}: ${release.currentVersion} -> ${release.nextVersion} ` +
            `(${release.bump}, ${source}; ${summary})`
        );
    }
}

/** Compares one local package against its stored or published public API baseline. */
async function analyzeWorkspaceApi(workspace, currentSnapshot, temporaryFolder) {
    const storedFile = path.join(ROOT, ".github", "api", `${workspace.name}.json`);
    if (existsSync(storedFile)) {
        const previous = JSON.parse(await readFile(storedFile, "utf8"));
        return comparePublicApi(previous, currentSnapshot);
    }

    const version = workspace.manifest.version;
    const versions = publishedVersions(workspace.name, ROOT);
    if (versions.length === 0) {
        return {bump: "initial", breaking: [], additions: ["Initial public API"]};
    }
    if (!versions.includes(version)) {
        throw new Error(
            `${workspace.name}@${version} is not published and no stored API baseline exists; ` +
            `published versions: ${versions.join(", ")}`
        );
    }
    const publishedFolder = await extractPublishedPackage(
        workspace.name,
        version,
        temporaryFolder,
        ROOT
    );
    const previous = await createPublicApiSnapshot(
        publishedFolder,
        path.join(temporaryFolder, "published", workspace.name)
    );
    return comparePublicApi(previous, currentSnapshot);
}

/** Parses `--name value` CLI arguments. */
function argumentsMap(values) {
    const result = new Map();
    for (let index = 0; index < values.length; index += 2) {
        const key = values[index];
        const value = values[index + 1];
        if (!key?.startsWith("--") || value === undefined) throw new Error(`Invalid argument list near ${key ?? "end"}`);
        result.set(key.slice(2), value);
    }
    return result;
}

/** Splits command output into non-empty lines. */
function lines(value) {
    return value === undefined || value.length === 0 ? [] : value.split(/\r?\n/).filter(Boolean);
}
