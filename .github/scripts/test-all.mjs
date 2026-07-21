import {run} from "./process.mjs";
import {ROOT, WORKSPACES} from "./workspaces.mjs";
import {runWorkspaces} from "./run-workspaces.mjs";

run(process.execPath, [
    "--test",
    ".github/scripts/architecture.test.mjs",
    ".github/scripts/api-diff.test.mjs",
    ".github/scripts/api-model.test.mjs",
    ".github/scripts/release-logic.test.mjs"
], {cwd: ROOT});
for (const workspace of WORKSPACES) {
    runWorkspaces("test", [workspace.name]);
    runWorkspaces("build", [workspace.name]);
}
run(process.execPath, ["--test", ".github/scripts/architecture.test.mjs"], {
    cwd: ROOT,
    env: {...process.env, VERIFY_DIST: "true"}
});
