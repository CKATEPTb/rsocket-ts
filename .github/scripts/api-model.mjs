/** Generates compact, deterministic public TypeScript API snapshots. */
import {createHash} from "node:crypto";
import {existsSync} from "node:fs";
import {mkdir, readFile} from "node:fs/promises";
import path from "node:path";
import {Extractor, ExtractorConfig} from "@microsoft/api-extractor";
import {WORKSPACES} from "./workspaces.mjs";

/** Snapshot schema version, incremented only when the stored representation changes. */
const SNAPSHOT_VERSION = 1;

/** API Extractor fields that do not affect TypeScript consumers. */
const NON_SEMANTIC_FIELDS = new Set([
    "canonicalReference",
    "docComment",
    "fileUrlPath",
    "members",
    "preserveMemberOrder",
    "releaseTag",
    "sourceLocation"
]);

/** Names of packages whose referenced public types belong to this release graph. */
const INTERNAL_PACKAGE_NAMES = new Set(WORKSPACES.map(({name}) => name));

/** Generates the public API snapshot for all typed package export entry points. */
export async function createPublicApiSnapshot(packageFolder, outputFolder) {
    const manifestFile = path.join(packageFolder, "package.json");
    const manifest = JSON.parse(await readFile(manifestFile, "utf8"));
    const declarations = declarationEntries(manifest, packageFolder);
    if (declarations.length === 0) throw new Error(`${manifest.name} has no public TypeScript declaration entry point`);

    await mkdir(outputFolder, {recursive: true});
    const entries = {};
    for (const declaration of declarations) {
        entries[declaration.name] = await extractEntry({
            declaration,
            manifest,
            manifestFile,
            packageFolder,
            outputFolder
        });
    }
    return {formatVersion: SNAPSHOT_VERSION, packageName: manifest.name, entries};
}

/** Resolves public export subpaths, conditions, and declaration entry files. */
export function declarationEntries(manifest, packageFolder) {
    const targets = exportTargets(manifest);
    return [...targets].map(([name, target]) => {
        const typesTarget = resolveTypesTarget(target) ?? (name === "." ? manifest.types ?? manifest.typings : undefined);
        const file = typesTarget === undefined ? undefined : declarationFile(packageFolder, typesTarget);
        if (file === undefined || !existsSync(file)) {
            throw new Error(`Cannot resolve the declaration entry for ${manifest.name}${name === "." ? "" : name.slice(1)}`);
        }
        return {name, file, conditions: exportConditions(target)};
    }).sort((left, right) => left.name.localeCompare(right.name));
}

/** Runs API Extractor for one package export subpath. */
async function extractEntry({declaration, manifest, manifestFile, packageFolder, outputFolder}) {
    const output = path.join(outputFolder, `${entryFileName(declaration.name)}.api.json`);
    const bundledPackages = Object.keys(manifest.dependencies ?? {}).filter((name) => INTERNAL_PACKAGE_NAMES.has(name));
    const config = ExtractorConfig.prepare({
        configObject: {
            projectFolder: packageFolder,
            mainEntryPointFilePath: declaration.file,
            bundledPackages,
            testMode: true,
            compiler: {
                overrideTsconfig: {
                    compilerOptions: {
                        target: "ES2022",
                        module: "ESNext",
                        moduleResolution: "Bundler",
                        skipLibCheck: true
                    },
                    files: [declaration.file]
                },
                skipLibCheck: true
            },
            apiReport: {enabled: false},
            docModel: {
                enabled: true,
                apiJsonFilePath: output,
                includeForgottenExports: true,
                releaseTagsToTrim: ["@internal"]
            },
            dtsRollup: {enabled: false},
            tsdocMetadata: {enabled: false}
        },
        configObjectFullPath: path.join(packageFolder, "api-extractor.generated.json"),
        packageJsonFullPath: manifestFile
    });
    const messages = [];
    const result = Extractor.invoke(config, {
        localBuild: true,
        showVerboseMessages: false,
        messageCallback(message) {
            messages.push(message.text);
            message.handled = true;
        }
    });
    if (!result.succeeded) {
        throw new Error(`API extraction failed for ${manifest.name}${declaration.name}: ${messages.join("; ")}`);
    }
    const model = JSON.parse(await readFile(output, "utf8"));
    return snapshotEntry(model, declaration, await externalReexports(declaration.file));
}

/** Converts an API Extractor model into a hash-based entry snapshot. */
function snapshotEntry(model, declaration, externalExports) {
    const entryPoint = model.members?.find(({kind}) => kind === "EntryPoint");
    if (entryPoint === undefined) throw new Error(`API Extractor did not emit an entry point for ${declaration.name}`);
    const exports = new Map(externalExports);
    const index = new Map();
    const roots = [];
    for (const item of entryPoint.members ?? []) {
        indexItems(item, undefined, index);
        if (!isExported(item.canonicalReference)) continue;
        addExport(exports, item.name, item.kind);
        roots.push(item.canonicalReference);
    }
    return {
        conditions: declaration.conditions,
        exports: Object.fromEntries([...exports].map(([name, kinds]) => [name, [...kinds].sort().join("|")]).sort()),
        items: collectReachableItems(roots, index)
    };
}

/** Indexes declarations so exported signatures can resolve forgotten helper types. */
function indexItems(item, parent, target) {
    const reference = item.canonicalReference;
    const owner = typeof reference === "string" ? reference : parent;
    if (typeof reference === "string") target.set(reference, {item, parent});
    for (const member of item.members ?? []) indexItems(member, owner, target);
}

/** Records only declarations reachable from an entry point's exported API. */
function collectReachableItems(roots, index) {
    const target = {};
    const pending = [...roots];
    const visited = new Set();
    while (pending.length > 0) {
        const reference = pending.pop();
        if (typeof reference !== "string" || visited.has(reference)) continue;
        visited.add(reference);
        const record = index.get(reference);
        if (record === undefined) continue;
        collectItem(record.item, record.parent, target);
        for (const member of record.item.members ?? []) pending.push(member.canonicalReference);
        for (const token of record.item.excerptTokens ?? []) pending.push(token.canonicalReference);
    }
    return Object.fromEntries(Object.entries(target).sort(([left], [right]) => left.localeCompare(right)));
}

/** Records the semantic shape of one reachable declaration item. */
function collectItem(item, parent, target) {
    const reference = item.canonicalReference;
    if (typeof reference !== "string") return;
    const semantic = {};
    for (const [key, value] of Object.entries(item)) {
        if (!NON_SEMANTIC_FIELDS.has(key)) semantic[key] = value;
    }
    target[reference] = {
        kind: item.kind,
        name: item.name ?? "",
        parent: parent ?? "",
        optional: item.isOptional === true,
        abstract: item.isAbstract === true,
        hash: createHash("sha256").update(stableJson(semantic)).digest("base64url")
    };
}

/** Extracts named re-exports whose declarations belong to another package. */
async function externalReexports(file) {
    const source = stripComments(await readFile(file, "utf8"));
    const exports = new Map();
    const namedPattern = /\bexport\s+(type\s+)?\{([\s\S]*?)\}\s+from\s+["']([^"']+)["']/g;
    for (const match of source.matchAll(namedPattern)) {
        const moduleName = match[3];
        if (moduleName.startsWith(".")) continue;
        for (const rawElement of match[2].split(",")) {
            const element = rawElement.trim();
            if (element.length === 0) continue;
            const elementTypeOnly = element.startsWith("type ");
            const declaration = elementTypeOnly ? element.slice(5).trim() : element;
            const [imported, exported = imported] = declaration.split(/\s+as\s+/);
            const type = match[1] !== undefined || elementTypeOnly ? "type" : "value";
            addExport(exports, exported.trim(), `reexport:${moduleName}:${type}`);
        }
    }
    const namespacePattern = /\bexport\s+\*\s+as\s+([A-Za-z_$][\w$]*)\s+from\s+["']([^"']+)["']/g;
    for (const match of source.matchAll(namespacePattern)) {
        if (!match[2].startsWith(".")) addExport(exports, match[1], `reexport:${match[2]}:namespace`);
    }
    const starPattern = /\bexport\s+\*\s+from\s+["']([^"']+)["']/g;
    for (const match of source.matchAll(starPattern)) {
        if (!match[1].startsWith(".")) addExport(exports, `*:${match[1]}`, `reexport:${match[1]}:all`);
    }
    return exports;
}

/** Removes comments before scanning generated declaration export statements. */
function stripComments(source) {
    return source.replace(/\/\*[\s\S]*?\*\//g, "").replace(/\/\/.*$/gm, "");
}

/** Adds one export kind without losing declaration merging information. */
function addExport(exports, name, kind) {
    if (typeof name !== "string" || name.length === 0) return;
    const kinds = exports.get(name) ?? new Set();
    kinds.add(kind);
    exports.set(name, kinds);
}

/** Whether a canonical reference represents an entry-point export rather than a reachable helper. */
function isExported(reference) {
    const separator = typeof reference === "string" ? reference.indexOf("!") : -1;
    return separator >= 0 && reference[separator + 1] !== "~";
}

/** Returns the package export map as subpath/target pairs. */
function exportTargets(manifest) {
    const exports = manifest.exports;
    if (exports === undefined) return new Map([[".", manifest.types ?? manifest.typings]]);
    if (typeof exports === "object" && !Array.isArray(exports) && exports !== null &&
        Object.keys(exports).some((key) => key.startsWith("."))) {
        return new Map(Object.entries(exports));
    }
    return new Map([[".", exports]]);
}

/** Finds the first explicit declaration target in a conditional export value. */
function resolveTypesTarget(target) {
    if (typeof target === "string") return target.endsWith(".d.ts") ? target : runtimeDeclarationTarget(target);
    if (Array.isArray(target)) {
        for (const value of target) {
            const resolved = resolveTypesTarget(value);
            if (resolved !== undefined) return resolved;
        }
        return undefined;
    }
    if (target === null || typeof target !== "object") return undefined;
    if (Object.hasOwn(target, "types")) return resolveTypesTarget(target.types);
    for (const value of Object.values(target)) {
        const resolved = resolveTypesTarget(value);
        if (resolved !== undefined) return resolved;
    }
    return undefined;
}

/** Collects conditional export keys because removing one can break a runtime. */
function exportConditions(target) {
    if (typeof target === "string") return ["default"];
    const result = [];
    const visit = (value, parent = "") => {
        if (Array.isArray(value)) return value.forEach((item) => visit(item, parent));
        if (value === null || typeof value !== "object") return;
        for (const [condition, nested] of Object.entries(value)) {
            const name = parent.length === 0 ? condition : `${parent}>${condition}`;
            result.push(name);
            visit(nested, name);
        }
    };
    visit(target);
    return result;
}

/** Resolves one package-relative declaration target. */
function declarationFile(packageFolder, target) {
    if (typeof target !== "string") return undefined;
    return path.resolve(packageFolder, target);
}

/** Converts a runtime target to its conventional declaration filename. */
function runtimeDeclarationTarget(target) {
    return /\.(?:mjs|cjs|js)$/.test(target) ? target.replace(/\.(?:mjs|cjs|js)$/, ".d.ts") : undefined;
}

/** Produces a filesystem-safe deterministic filename for an export subpath. */
function entryFileName(name) {
    return name === "." ? "root" : createHash("sha256").update(name).digest("hex").slice(0, 16);
}

/** Serializes arbitrary model data with stable object-key ordering. */
function stableJson(value) {
    if (Array.isArray(value)) return `[${value.map(stableJson).join(",")}]`;
    if (value !== null && typeof value === "object") {
        return `{${Object.keys(value).sort().map((key) => `${JSON.stringify(key)}:${stableJson(value[key])}`).join(",")}}`;
    }
    return JSON.stringify(value);
}
