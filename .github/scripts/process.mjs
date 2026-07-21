import {spawnSync} from "node:child_process";
import {existsSync} from "node:fs";
import path from "node:path";

/** Platform-specific npm command and fixed argument prefix. */
const NPM = npmInvocation();

/** Runs npm without routing controlled arguments through a command shell. */
export function runNpm(args, options = {}) {
    run(NPM.command, [...NPM.args, ...args], options);
}

/** Captures npm stdout without routing arguments through a command shell. */
export function captureNpm(args, options = {}) {
    return capture(NPM.command, [...NPM.args, ...args], options);
}

/** Captures npm stdout, stderr, and status when callers must classify failures. */
export function captureNpmResult(args, options = {}) {
    return captureResult(NPM.command, [...NPM.args, ...args], options);
}

/** Runs a command with inherited output and fails on a non-zero exit code. */
export function run(command, args, options = {}) {
    const result = spawnSync(command, args, {
        cwd: options.cwd,
        env: options.env ?? process.env,
        stdio: "inherit",
        shell: false
    });
    if (result.error) throw result.error;
    if (result.status !== 0) {
        throw new Error(`${command} ${args.join(" ")} exited with code ${result.status}`);
    }
}

/** Runs a command and returns trimmed UTF-8 stdout. */
export function capture(command, args, options = {}) {
    const result = captureResult(command, args, options);
    if (result.status !== 0 && options.allowFailure !== true) {
        throw new Error(`${command} ${args.join(" ")} exited with code ${result.status}`);
    }
    return result.status === 0 ? result.stdout : undefined;
}

/** Captures a process result without interpreting a non-zero exit status. */
export function captureResult(command, args, options = {}) {
    const result = spawnSync(command, args, {
        cwd: options.cwd,
        env: options.env ?? process.env,
        encoding: "utf8",
        stdio: ["ignore", "pipe", "pipe"],
        shell: false
    });
    if (result.error) throw result.error;
    const output = {
        status: result.status ?? 1,
        stdout: result.stdout.trim(),
        stderr: result.stderr.trim()
    };
    if (!options.quiet && output.stderr.length > 0) process.stderr.write(`${output.stderr}\n`);
    return output;
}

/** Resolves npm to its JavaScript CLI on Windows and its executable elsewhere. */
function npmInvocation() {
    const configured = process.env.npm_execpath;
    if (configured !== undefined && configured.length > 0) {
        return {command: process.execPath, args: [configured]};
    }
    if (process.platform !== "win32") return {command: "npm", args: []};

    const located = spawnSync("where.exe", ["npm.cmd"], {encoding: "utf8", shell: false});
    if (located.status === 0) {
        for (const shim of located.stdout.split(/\r?\n/).filter(Boolean)) {
            const cli = path.join(path.dirname(shim), "node_modules", "npm", "bin", "npm-cli.js");
            if (existsSync(cli)) return {command: process.execPath, args: [cli]};
        }
    }
    throw new Error("Unable to locate npm-cli.js");
}
