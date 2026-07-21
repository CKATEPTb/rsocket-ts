/** Supported semantic version increments ordered by precedence. */
export const BUMP_RANK = Object.freeze({initial: 0, patch: 1, minor: 2, major: 3});

/** Chooses the strongest of two optional semantic increments. */
export function strongestBump(left, right) {
    if (left === undefined) return right;
    if (right === undefined) return left;
    return BUMP_RANK[left] >= BUMP_RANK[right] ? left : right;
}

/** Increments a stable three-part semantic version. */
export function incrementVersion(version, bump) {
    const match = /^(\d+)\.(\d+)\.(\d+)$/.exec(version);
    if (!match) throw new Error(`Unsupported package version: ${version}`);
    const major = Number(match[1]);
    const minor = Number(match[2]);
    const patch = Number(match[3]);
    if (bump === "initial") return version;
    if (bump === "major") return `${major + 1}.0.0`;
    if (bump === "minor") return `${major}.${minor + 1}.0`;
    return `${major}.${minor}.${patch + 1}`;
}

/** Expands directly changed packages through all internal dependents. */
export function cascadeBumps(workspaces, directBumps) {
    const bumps = new Map(directBumps);
    let changed = true;
    while (changed) {
        changed = false;
        for (const workspace of workspaces) {
            if (bumps.has(workspace.name)) continue;
            const dependencies = dependencyNames(workspace.manifest);
            if (![...bumps.keys()].some((name) => dependencies.has(name))) continue;
            bumps.set(workspace.name, "patch");
            changed = true;
        }
    }
    return bumps;
}

/** Creates ordered release records with current and next versions. */
export function releaseEntries(workspaces, requestedBumps, directlyChanged = new Set(requestedBumps.keys())) {
    const bumps = cascadeBumps(workspaces, requestedBumps);
    return workspaces.flatMap((workspace) => {
        const bump = bumps.get(workspace.name);
        if (bump === undefined) return [];
        return [{
            name: workspace.name,
            directory: workspace.directory,
            currentVersion: workspace.manifest.version,
            nextVersion: incrementVersion(workspace.manifest.version, bump),
            bump,
            direct: directlyChanged.has(workspace.name)
        }];
    });
}

/** Collects install-time dependencies that require a dependent package release. */
function dependencyNames(manifest) {
    return new Set([
        ...Object.keys(manifest.dependencies ?? {}),
        ...Object.keys(manifest.optionalDependencies ?? {}),
        ...Object.keys(manifest.peerDependencies ?? {})
    ]);
}
