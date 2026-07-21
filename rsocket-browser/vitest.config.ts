/**
 * Vitest configuration for the RSocket browser client test suite.
 */
import { fileURLToPath } from "node:url";
import { defineConfig } from "vitest/config";

/**
 * Absolute source root used by the `@` alias in unit tests.
 */
const sourceRoot = fileURLToPath(new URL("./src", import.meta.url));

export default defineConfig({
  resolve: {
    alias: {
      "@": sourceRoot
    }
  },
  test: {
    environment: "node",
    include: ["test/**/*.test.ts"],
    exclude: [
      "node_modules/**",
      "dist/**",
      "partysocket/**"
    ]
  }
});
