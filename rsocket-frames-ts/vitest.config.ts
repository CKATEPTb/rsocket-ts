/** Vitest configuration for protocol and codec tests. */
import {fileURLToPath} from "node:url";
import {defineConfig} from "vitest/config";

const sourceRoot = fileURLToPath(new URL("./src", import.meta.url));
const testRoot = fileURLToPath(new URL("./tests", import.meta.url));

export default defineConfig({
    resolve: {
        alias: {
            "@": sourceRoot,
            "@test": testRoot
        }
    },
    test: {
        environment: "node",
        globals: true,
        include: ["tests/**/*.test.ts"],
        exclude: ["node_modules/**", "dist/**"]
    }
});
