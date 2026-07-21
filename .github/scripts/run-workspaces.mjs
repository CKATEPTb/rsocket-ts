import {realpathSync} from "node:fs";
import {fileURLToPath} from "node:url";
import {runNpm} from "./process.mjs";
import {ROOT, WORKSPACES} from "./workspaces.mjs";

/** Executes one npm script for selected workspaces in dependency order; an empty selection means all. */
export function runWorkspaces(script, selectedNames = []) {
    const selected = selectedNames.length === 0 ? undefined : new Set(selectedNames);
    for (const workspace of WORKSPACES) {
        if (selected !== undefined && !selected.has(workspace.name)) continue;
        console.log(`\n=== ${workspace.name}: ${script} ===`);
        runNpm(["run", script, "--workspace", workspace.name], {cwd: ROOT});
    }
}

if (process.argv[1] !== undefined && realpathSync(process.argv[1]) === realpathSync(fileURLToPath(import.meta.url))) {
    const script = process.argv[2];
    if (script !== "test" && script !== "build") throw new Error("Expected test or build workspace script");
    runWorkspaces(script, process.argv.slice(3));
}
