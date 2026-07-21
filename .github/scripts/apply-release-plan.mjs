import {mkdir, readFile, writeFile} from "node:fs/promises";
import path from "node:path";
import {ROOT, WORKSPACES} from "./workspaces.mjs";

const planFile = path.resolve(ROOT, process.argv[2] ?? ".release-plan.json");
const plan = JSON.parse(await readFile(planFile, "utf8"));
const versions = new Map(plan.releases.map((release) => [release.name, release.nextVersion]));

for (const workspace of WORKSPACES) {
    const version = versions.get(workspace.name);
    if (version === undefined) continue;
    const manifestFile = path.join(ROOT, workspace.directory, "package.json");
    const manifest = JSON.parse(await readFile(manifestFile, "utf8"));
    manifest.version = version;
    updateDependencyRanges(manifest, versions);
    await writeFile(manifestFile, `${JSON.stringify(manifest, null, 2)}\n`);
    console.log(`Updated ${workspace.name} to ${version}`);
}

const apiFolder = path.join(ROOT, ".github", "api");
await mkdir(apiFolder, {recursive: true});
for (const release of plan.releases) {
    if (release.apiSnapshot === undefined) continue;
    const file = path.join(apiFolder, `${release.name}.json`);
    await writeFile(file, `${JSON.stringify(release.apiSnapshot, null, 2)}\n`);
}

/** Updates internal dependency ranges in every supported dependency field. */
function updateDependencyRanges(manifest, versions) {
    for (const field of ["dependencies", "devDependencies", "optionalDependencies", "peerDependencies"]) {
        const dependencies = manifest[field];
        if (dependencies === undefined) continue;
        for (const [name, version] of versions) {
            if (Object.hasOwn(dependencies, name)) dependencies[name] = `^${version}`;
        }
    }
}
