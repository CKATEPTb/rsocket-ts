import { fileURLToPath } from "node:url";
import { defineConfig } from "vite";

const sourceRoot = fileURLToPath(new URL("./src", import.meta.url));
const sourceEntry = (path: string) => fileURLToPath(new URL(`./src/${path}`, import.meta.url));
const external = [
  "reactor-core-ts",
  "rsocket-client-ts",
  "rsocket-core-ts",
  "rsocket-frames-ts"
];

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
      entry: {
        index: sourceEntry("index.ts"),
      },
      formats: ["es"]
    },
    rolldownOptions: {
      external,
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
