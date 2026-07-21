import {fileURLToPath} from "node:url";
import {defineConfig} from "vite";

const sourceRoot = fileURLToPath(new URL("./src", import.meta.url));
const sourceEntry = fileURLToPath(new URL("./src/index.ts", import.meta.url));

export default defineConfig({
    resolve: {
        alias: {
            "@": sourceRoot
        }
    },
    build: {
        target: "es2022",
        outDir: "dist",
        emptyOutDir: true,
        minify: false,
        sourcemap: true,
        lib: {
            entry: sourceEntry,
            formats: ["es"],
            fileName: "index"
        },
        rolldownOptions: {
            external: ["bebyte"],
            treeshake: true,
            output: {
                exports: "named"
            }
        }
    }
});
