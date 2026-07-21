import {readFile} from "node:fs/promises";
import path from "node:path";
import {captureNpm, runNpm} from "./process.mjs";
import {ROOT, WORKSPACES} from "./workspaces.mjs";

const args = new Set(process.argv.slice(2));
const dryRun = args.has("--dry-run") || process.env.RELEASE_DRY_RUN === "true";
const planPath = path.resolve(ROOT, [...args].find((value) => value.endsWith(".json")) ?? ".release-plan.json");
const plan = JSON.parse(await readFile(planPath, "utf8"));

if (plan.releases.length === 0) {
    console.log("Nothing to test, build, or publish.");
    process.exit(0);
}

const firstRelease = WORKSPACES.findIndex(({name}) => name === plan.releases[0].name);
if (firstRelease < 0) throw new Error(`Unknown release workspace: ${plan.releases[0].name}`);
for (const workspace of WORKSPACES.slice(0, firstRelease)) {
    console.log(`\n=== ${workspace.name}: prerequisite build ===`);
    runNpm(["run", "build", "--workspace", workspace.name], {cwd: ROOT});
}

for (const release of plan.releases) {
    console.log(`\n=== ${release.name}@${release.nextVersion} ===`);
    runNpm(["run", "test", "--workspace", release.name], {cwd: ROOT});
    runNpm(["run", "build", "--workspace", release.name], {cwd: ROOT});

    if (dryRun) {
        runNpm(["pack", "--dry-run", "--workspace", release.name], {cwd: ROOT});
        continue;
    }

    if (!await published(release.name, release.nextVersion)) {
        runNpm([
            "publish",
            "--workspace",
            release.name,
            "--provenance",
            "--access",
            "public",
            "--ignore-scripts"
        ], {cwd: ROOT});
    } else {
        console.log(`${release.name}@${release.nextVersion} is already published; continuing an interrupted release.`);
    }
    await waitForPublication(release.name, release.nextVersion);
}

/** Checks whether an exact package version is currently visible on npm. */
async function published(name, version) {
    return captureNpm(["view", `${name}@${version}`, "version"], {
        cwd: ROOT,
        quiet: true,
        allowFailure: true
    }) === version;
}

/** Waits for npm registry propagation before building the next dependent. */
async function waitForPublication(name, version) {
    const timeoutMs = positiveInteger(process.env.NPM_PUBLISH_TIMEOUT_MS, 300_000);
    const pollMs = positiveInteger(process.env.NPM_PUBLISH_POLL_MS, 5_000);
    const deadline = Date.now() + timeoutMs;
    while (Date.now() < deadline) {
        if (await published(name, version)) {
            console.log(`${name}@${version} is available from npm.`);
            return;
        }
        await new Promise((resolve) => setTimeout(resolve, pollMs));
    }
    throw new Error(`Timed out waiting for ${name}@${version} to appear on npm`);
}

/** Reads a positive integer environment option with a stable fallback. */
function positiveInteger(value, fallback) {
    if (value === undefined) return fallback;
    const parsed = Number(value);
    if (!Number.isInteger(parsed) || parsed <= 0) throw new Error(`Expected a positive integer, received ${value}`);
    return parsed;
}
