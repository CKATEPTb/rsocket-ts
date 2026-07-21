import {readFile} from "node:fs/promises";
import {fileURLToPath} from "node:url";
import path from "node:path";

/** Absolute monorepo root shared by release and development scripts. */
export const ROOT = fileURLToPath(new URL("../../", import.meta.url));

/** Publish and build order required by the internal dependency graph. */
export const WORKSPACES = Object.freeze([
    Object.freeze({name: "rsocket-frames-ts", directory: "rsocket-frames-ts"}),
    Object.freeze({name: "rsocket-core-ts", directory: "rsocket-core-ts"}),
    Object.freeze({name: "rsocket-client-ts", directory: "rsocket-client-ts"}),
    Object.freeze({name: "rsocket-server-ts", directory: "rsocket-server-ts"}),
    Object.freeze({name: "rsocket-browser", directory: "rsocket-browser"})
]);

/** Reads one workspace package manifest from disk. */
export async function readWorkspaceManifest(workspace) {
    const file = path.join(ROOT, workspace.directory, "package.json");
    return JSON.parse(await readFile(file, "utf8"));
}

/** Reads package manifests in deterministic dependency order. */
export async function readWorkspaceManifests() {
    return Promise.all(WORKSPACES.map(async (workspace) => ({
        ...workspace,
        manifest: await readWorkspaceManifest(workspace)
    })));
}
