import {fileURLToPath} from "node:url";
import {defineConfig} from "vite";

const sourceRoot = fileURLToPath(new URL("./src", import.meta.url));

/** Builds the server runtime as tree-shakeable ESM while preserving its modules. */
export default defineConfig({
    resolve: {alias: {"@": sourceRoot}},
    build: {
        target: "es2022",
        outDir: "dist",
        emptyOutDir: true,
        minify: false,
        sourcemap: true,
        lib: {
            entry: {index: fileURLToPath(new URL("./src/index.ts", import.meta.url))},
            formats: ["es"]
        },
        rolldownOptions: {
            external: ["node:net", "reactor-core-ts", "rsocket-core-ts", "rsocket-frames-ts"],
            treeshake: true,
            output: {
                preserveModules: true,
                preserveModulesRoot: "src",
                entryFileNames: "[name].js",
                chunkFileNames: "chunks/[name]-[hash].js"
            }
        }
    }
});
