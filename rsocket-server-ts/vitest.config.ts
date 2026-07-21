import {fileURLToPath} from "node:url";
import {defineConfig} from "vitest/config";

/** Resolves source aliases for protocol and transport integration tests. */
export default defineConfig({
    resolve: {
        alias: {"@": fileURLToPath(new URL("./src", import.meta.url))}
    },
    test: {environment: "node", include: ["test/**/*.test.ts"]}
});
