#!/usr/bin/env node

import { execFile } from "node:child_process";
import { lstat, mkdtemp, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import { promisify } from "node:util";

const execFileAsync = promisify(execFile);
const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const expectedFiles = [
  "package/LICENSE",
  "package/README.md",
  "package/index.js",
  "package/package.json",
  "package/skills/cue/SKILL.md",
];

async function run(command, args, options = {}) {
  try {
    return await execFileAsync(command, args, {
      cwd: root,
      maxBuffer: 16 * 1024 * 1024,
      ...options,
    });
  } catch (error) {
    const output = [error?.stdout, error?.stderr].filter(Boolean).join("\n").trim();
    throw new Error(`${command} ${args.join(" ")} failed${output ? `\n${output}` : ""}`, {
      cause: error,
    });
  }
}

const manifest = JSON.parse(await readFile(join(root, "package.json"), "utf8"));
const metadata = JSON.parse(
  (await run("cargo", ["metadata", "--locked", "--no-deps", "--format-version", "1"]))
    .stdout,
);
const workspaceVersions = new Set(
  metadata.packages
    .filter((item) => metadata.workspace_members.includes(item.id))
    .map((item) => item.version),
);
if (workspaceVersions.size !== 1 || !workspaceVersions.has(manifest.version)) {
  throw new Error(
    `npm version ${manifest.version} does not match Cargo workspace versions ${[
      ...workspaceVersions,
    ].join(", ")}`,
  );
}

const temporaryDirectory = await mkdtemp(join(tmpdir(), "cue-npm-package-"));
try {
  let tarball = process.argv[2] ? resolve(process.argv[2]) : undefined;
  if (!tarball) {
    const packed = JSON.parse(
      (
        await run("npm", [
          "pack",
          "--json",
          "--ignore-scripts",
          "--pack-destination",
          temporaryDirectory,
        ])
      ).stdout,
    );
    if (packed.length !== 1 || !packed[0]?.filename) {
      throw new Error(`npm pack returned unexpected metadata: ${JSON.stringify(packed)}`);
    }
    tarball = join(temporaryDirectory, packed[0].filename);
  }

  const tarballStats = await lstat(tarball);
  if (!tarballStats.isFile() || tarballStats.isSymbolicLink()) {
    throw new Error(`npm package is not a regular file: ${tarball}`);
  }
  const archiveFiles = (await run("tar", ["-tzf", tarball])).stdout
    .trim()
    .split("\n")
    .filter(Boolean)
    .sort();
  if (JSON.stringify(archiveFiles) !== JSON.stringify(expectedFiles)) {
    throw new Error(`unexpected npm package files:\n${archiveFiles.join("\n")}`);
  }

  const installDirectory = join(temporaryDirectory, "install");
  await run(
    "npm",
    [
      "install",
      "--ignore-scripts",
      "--no-audit",
      "--no-fund",
      "--prefix",
      installDirectory,
      tarball,
    ],
    { cwd: temporaryDirectory },
  );
  const entry = join(installDirectory, "node_modules", "@zendev-lab", "cue", "index.js");
  const installed = await import(pathToFileURL(entry).href);
  if (typeof installed.cueSkillsRoot !== "string") {
    throw new Error("@zendev-lab/cue does not export cueSkillsRoot");
  }
  const skillFile = join(installed.cueSkillsRoot, "cue", "SKILL.md");
  const skillStats = await lstat(skillFile);
  if (!skillStats.isFile() || skillStats.isSymbolicLink()) {
    throw new Error(`installed Cue Skill is not a regular file: ${skillFile}`);
  }
  const source = await readFile(join(root, "skills", "cue", "SKILL.md"));
  const packaged = await readFile(skillFile);
  if (!source.equals(packaged)) throw new Error("packaged Cue Skill differs from its authority");
  if (!packaged.toString("utf8").startsWith("---\nname: cue\n")) {
    throw new Error("packaged Cue Skill does not declare name: cue");
  }
  process.stdout.write(`Verified @zendev-lab/cue@${manifest.version} (${tarball})\n`);
} finally {
  await rm(temporaryDirectory, { recursive: true });
}
