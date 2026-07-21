/** Shared declaration alias rewriting for every workspace package. */
import {existsSync, statSync} from "node:fs";
import {readdir, readFile, writeFile} from "node:fs/promises";
import path from "node:path";

const distRoot = path.resolve("dist");

for (const file of await declarationFiles(distRoot)) {
    const source = await readFile(file, "utf8");
    const rewritten = source.replace(/(["'])@(?:\/([^"']+))?\1/g, (_, quote, target = "index") => {
        const absoluteTarget = declarationTarget(target);
        let relative = path.relative(path.dirname(file), absoluteTarget).replaceAll(path.sep, "/");
        if (!relative.startsWith(".")) relative = `./${relative}`;
        return `${quote}${relative}${quote}`;
    });
    if (rewritten !== source) await writeFile(file, rewritten);
}

/** Resolves an alias to the ESM file emitted beside its declaration. */
function declarationTarget(target) {
    const absolute = path.join(distRoot, target);
    if (path.extname(absolute) !== "") return absolute;
    return existsSync(absolute) && statSync(absolute).isDirectory()
        ? path.join(absolute, "index.js")
        : `${absolute}.js`;
}

/** Returns every generated declaration file below one directory. */
async function declarationFiles(directory) {
    const entries = await readdir(directory, {withFileTypes: true});
    return (await Promise.all(entries.map(async (entry) => {
        const current = path.join(directory, entry.name);
        if (entry.isDirectory()) return declarationFiles(current);
        return entry.isFile() && entry.name.endsWith(".d.ts") ? [current] : [];
    }))).flat();
}
