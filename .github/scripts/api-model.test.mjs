import assert from "node:assert/strict";
import {mkdtemp, mkdir, rm, writeFile} from "node:fs/promises";
import path from "node:path";
import {describe, it} from "node:test";
import {createPublicApiSnapshot} from "./api-model.mjs";
import {ROOT} from "./workspaces.mjs";

describe("public API extraction", () => {
    it("captures package entry points, exports, and reachable signatures", async () => {
        const folder = await mkdtemp(path.join(ROOT, ".api-test-"));
        try {
            await mkdir(path.join(folder, "dist"));
            await writeFile(path.join(folder, "package.json"), JSON.stringify({
                name: "api-model-fixture",
                version: "1.0.0",
                type: "module",
                exports: {
                    ".": {
                        types: "./dist/index.d.ts",
                        import: "./dist/index.js"
                    }
                }
            }));
            await writeFile(path.join(folder, "dist", "index.d.ts"), "export {Client} from \"./client.js\";\n");
            await writeFile(path.join(folder, "dist", "client.d.ts"), [
                "declare const PRIVATE_STATE: unique symbol;",
                "interface ClientOptions {",
                "    readonly timeout?: number;",
                "}",
                "interface PrivateState {",
                "    readonly secret: string;",
                "}",
                "export declare class Client {",
                "    private [PRIVATE_STATE];",
                "    private state: PrivateState;",
                "    connect(options?: ClientOptions): Promise<void>;",
                "}"
            ].join("\n"));

            const snapshot = await createPublicApiSnapshot(folder, path.join(folder, "model"));
            const root = snapshot.entries["."];

            assert.deepEqual(root.conditions, ["types", "import"]);
            assert.match(root.exports.Client, /Class/);
            assert.deepEqual(Object.keys(root.exports), ["Client"]);
            assert.ok(Object.keys(root.items).some((reference) => reference.endsWith("Client:class")));
            assert.ok(Object.values(root.items).some(({name}) => name === "ClientOptions"));
            assert.ok(Object.values(root.items).every(({name}) => name !== "PRIVATE_STATE" && name !== "PrivateState"));
            assert.ok(Object.values(root.items).every(({hash}) => typeof hash === "string" && hash.length > 20));
        } finally {
            await rm(folder, {recursive: true, force: true});
        }
    });
});
