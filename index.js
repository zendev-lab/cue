import { fileURLToPath } from "node:url";

/** Filesystem-provider root containing the canonical `cue` Skill. */
export const cueSkillsRoot = fileURLToPath(new URL("./skills", import.meta.url));
