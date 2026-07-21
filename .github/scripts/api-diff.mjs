/** Compares deterministic public API snapshots and classifies their SemVer impact. */

/** API item kinds whose required addition breaks existing interface implementers. */
const INTERFACE_CONTRACT_MEMBERS = new Set([
    "CallSignature",
    "ConstructSignature",
    "IndexSignature",
    "MethodSignature",
    "PropertySignature"
]);

/** Compares two package API snapshots and returns the required semantic increment. */
export function comparePublicApi(previous, current) {
    if (previous.formatVersion !== current.formatVersion) {
        throw new Error(
            `Cannot compare public API snapshot formats ${previous.formatVersion} and ${current.formatVersion}`
        );
    }
    if (previous.packageName !== current.packageName) {
        throw new Error(`Cannot compare public APIs for ${previous.packageName} and ${current.packageName}`);
    }
    const breaking = [];
    const additions = [];
    compareEntries(previous.entries ?? {}, current.entries ?? {}, breaking, additions);
    return {
        bump: breaking.length > 0 ? "major" : additions.length > 0 ? "minor" : "patch",
        breaking,
        additions
    };
}

/** Compares package export subpaths. */
function compareEntries(previous, current, breaking, additions) {
    for (const [name, entry] of Object.entries(previous)) {
        const next = current[name];
        if (next === undefined) {
            breaking.push(`Removed export entry point ${name}`);
            continue;
        }
        compareEntry(name, entry, next, breaking, additions);
    }
    for (const name of Object.keys(current)) {
        if (previous[name] === undefined) additions.push(`Added export entry point ${name}`);
    }
}

/** Compares conditions, exported names, and declaration items for one entry point. */
function compareEntry(name, previous, current, breaking, additions) {
    const previousConditions = previous.conditions ?? [];
    const currentConditions = current.conditions ?? [];
    compareSet(
        previousConditions,
        currentConditions,
        (condition) => breaking.push(`Removed ${name} export condition ${condition}`),
        (condition) => additions.push(`Added ${name} export condition ${condition}`)
    );
    if (sameSet(previousConditions, currentConditions) &&
        previousConditions.some((condition, index) => currentConditions[index] !== condition)) {
        breaking.push(`Changed ${name} export condition precedence`);
    }
    compareRecord(
        previous.exports ?? {},
        current.exports ?? {},
        (symbol) => breaking.push(`Removed ${name} export ${symbol}`),
        (symbol) => additions.push(`Added ${name} export ${symbol}`),
        (symbol) => breaking.push(`Changed ${name} export ${symbol}`)
    );

    const previousItems = previous.items ?? {};
    const currentItems = current.items ?? {};
    compareRecord(
        previousItems,
        currentItems,
        (reference) => breaking.push(`Removed ${name} API item ${reference}`),
        (reference, item) => {
            const message = `Added ${name} API item ${reference}`;
            if (isBreakingAddition(item, previousItems)) breaking.push(message);
            else additions.push(message);
        },
        (reference) => breaking.push(`Changed ${name} API item ${reference}`),
        (left, right) => left.hash === right.hash
    );
}

/** Whether adding one member tightens an already published implementation contract. */
function isBreakingAddition(item, previousItems) {
    const parent = previousItems[item.parent];
    if (parent === undefined) return false;
    if (parent.kind === "Class") return item.abstract === true;
    if (parent.kind !== "Interface" || item.optional === true || !INTERFACE_CONTRACT_MEMBERS.has(item.kind)) {
        return false;
    }
    return !Object.values(previousItems).some((candidate) =>
        candidate.parent === item.parent && candidate.kind === item.kind && candidate.name === item.name
    );
}

/** Compares two records with caller-provided change handlers. */
function compareRecord(previous, current, removed, added, changed, equal = Object.is) {
    for (const [key, value] of Object.entries(previous)) {
        const next = current[key];
        if (next === undefined) removed(key, value);
        else if (!equal(value, next)) changed(key, value, next);
    }
    for (const [key, value] of Object.entries(current)) {
        if (previous[key] === undefined) added(key, value);
    }
}

/** Compares two string sets without depending on declaration order. */
function compareSet(previous, current, removed, added) {
    const oldValues = new Set(previous);
    const newValues = new Set(current);
    for (const value of oldValues) if (!newValues.has(value)) removed(value);
    for (const value of newValues) if (!oldValues.has(value)) added(value);
}

/** Whether two arrays contain the same unique values regardless of order. */
function sameSet(left, right) {
    if (left.length !== right.length) return false;
    const values = new Set(left);
    return values.size === left.length && right.every((value) => values.has(value));
}
