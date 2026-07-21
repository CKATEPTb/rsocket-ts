/** Downloads an exact published npm package for first-time API baseline migration. */
import {mkdir} from "node:fs/promises";
import path from "node:path";
import {captureNpm, captureNpmResult, run} from "./process.mjs";

/** Reads every published version of a package, returning an empty list for an unpublished name. */
export function publishedVersions(name, cwd) {
    const result = captureNpmResult(["view", name, "versions", "--json"], {
        cwd,
        quiet: true
    });
    if (result.status !== 0) {
        const error = `${result.stdout}\n${result.stderr}`;
        if (/\bE404\b|404 Not Found/i.test(error)) return [];
        throw new Error(`Unable to read published versions for ${name}: ${error.trim()}`);
    }
    const versions = JSON.parse(result.stdout);
    return Array.isArray(versions) ? versions : [versions];
}

/** Downloads and extracts an exact package version below the supplied temporary directory. */
export async function extractPublishedPackage(name, version, temporaryFolder, cwd) {
    const packageFolder = path.join(temporaryFolder, `${safeName(name)}-${version}`);
    const archiveFolder = path.join(packageFolder, "archive");
    const extractedFolder = path.join(packageFolder, "extracted");
    await mkdir(archiveFolder, {recursive: true});
    await mkdir(extractedFolder, {recursive: true});
    const output = captureNpm([
        "pack",
        `${name}@${version}`,
        "--json",
        "--pack-destination",
        archiveFolder
    ], {cwd, quiet: true});
    const [{filename} = {}] = JSON.parse(output);
    if (typeof filename !== "string") throw new Error(`npm pack did not return an archive for ${name}@${version}`);
    const archive = path.isAbsolute(filename) ? filename : path.join(archiveFolder, filename);
    run("tar", ["-xzf", archive, "-C", extractedFolder], {cwd});
    return path.join(extractedFolder, "package");
}

/** Converts a package name into a safe temporary directory component. */
function safeName(name) {
    return name.replace(/[^a-z0-9._-]+/gi, "-");
}
