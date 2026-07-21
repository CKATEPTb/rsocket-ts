import {readFile} from "node:fs/promises";
import path from "node:path";
import {capture, run} from "./process.mjs";
import {ROOT} from "./workspaces.mjs";

const planPath = path.resolve(ROOT, process.argv[2] ?? ".release-plan.json");
const plan = JSON.parse(await readFile(planPath, "utf8"));
if (plan.releases.length === 0) {
    console.log("No release metadata to commit.");
    process.exit(0);
}

run("git", ["config", "user.name", "github-actions[bot]"], {cwd: ROOT});
run("git", ["config", "user.email", "41898282+github-actions[bot]@users.noreply.github.com"], {cwd: ROOT});
run("git", [
    "add",
    "package-lock.json",
    ...plan.releases.flatMap(({name, directory}) => [
        `${directory}/package.json`,
        `.github/api/${name}.json`
    ])
], {cwd: ROOT});

const staged = capture("git", ["diff", "--cached", "--name-only"], {cwd: ROOT});
if (staged !== undefined && staged.length > 0) {
    const summary = plan.releases.map(({name, nextVersion}) => `${name}@${nextVersion}`).join(", ");
    run("git", ["commit", "-m", `chore(release): publish ${summary} [skip ci]`], {cwd: ROOT});
}

for (const release of plan.releases) {
    const tag = `${release.name}@${release.nextVersion}`;
    const exists = capture("git", ["rev-parse", "--verify", `refs/tags/${tag}`], {
        cwd: ROOT,
        quiet: true,
        allowFailure: true
    });
    if (exists === undefined) run("git", ["tag", "-a", tag, "-m", tag], {cwd: ROOT});
}

run("git", ["push", "origin", "HEAD:production", "--follow-tags"], {cwd: ROOT});
