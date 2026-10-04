#!/usr/bin/env node

import fs from "node:fs";
import path from "node:path";
import process from "node:process";
import { createBuildCommand, NapiCli } from "@napi-rs/cli";
import { getEnv } from "@voidzero-dev/vite-task-client";

interface TargetMatrixEntry {
  target: string;
  flags: string;
}

function getTargetMatrix(): TargetMatrixEntry[] {
  const pkgPath = path.resolve(process.cwd(), "package.json");

  let targets: string[] = [];

  if (fs.existsSync(pkgPath)) {
    try {
      const pkgContent = fs.readFileSync(pkgPath, "utf-8");
      const pkgJson = JSON.parse(pkgContent);
      if (pkgJson?.napi?.targets && Array.isArray(pkgJson.napi.targets)) {
        targets = pkgJson.napi.targets;
      }
    } catch (err) {
      console.warn(
        "Could not read targets from package.json, falling back to default matrix.",
        err,
      );
    }
  }

  if (targets.length === 0) {
    targets = [
      "x86_64-apple-darwin",
      "aarch64-apple-darwin",
      "x86_64-pc-windows-msvc",
      "i686-pc-windows-msvc",
      "aarch64-pc-windows-msvc",
      "x86_64-unknown-linux-gnu",
      "aarch64-unknown-linux-gnu",
      "x86_64-unknown-linux-musl",
      "aarch64-unknown-linux-musl",
      "armv7-unknown-linux-gnueabihf",
      "powerpc64le-unknown-linux-gnu",
      "s390x-unknown-linux-gnu",
      "wasm32-wasip1-threads",
    ];
  }

  return targets.map((target) => {
    let flags = "-x";
    if (
      target.includes("linux-gnu") ||
      target.includes("gnueabihf") ||
      target.includes("powerpc") ||
      target.includes("s390x")
    ) {
      flags = "--use-napi-cross";
    }
    return { target, flags };
  });
}

function parseArgs() {
  const args = process.argv.slice(2);

  const buildAll = args.includes("--target-all") || args.includes("--all");
  const isRelease = args.includes("--release") || args.includes("-r");
  const dryRun = args.includes("--dry-run");

  const useCross = args.includes("--use-cross");
  const useNapiCross = args.includes("--use-napi-cross");
  const crossCompile = args.includes("-x") || args.includes("--cross-compile");

  let targetFilter: string | null = null;
  const targetIdx = args.indexOf("--target");
  if (targetIdx !== -1 && args[targetIdx + 1] && !args[targetIdx + 1].startsWith("-")) {
    targetFilter = args[targetIdx + 1];
  } else {
    const tIdx = args.indexOf("-t");
    if (tIdx !== -1 && args[tIdx + 1] && !args[tIdx + 1].startsWith("-")) {
      targetFilter = args[tIdx + 1];
    }
  }

  const filteredArgs = args.filter(
    (arg) =>
      arg !== "--target-all" &&
      arg !== "--all" &&
      arg !== "--dry-run" &&
      arg !== "--build-all",
  );

  return {
    targetFilter,
    isRelease,
    dryRun,
    buildAll,
    useCross,
    useNapiCross,
    crossCompile,
    filteredArgs,
  };
}

async function runLocalCI() {
  // Leverage vite-task-client for tracking task environment variables if running under VP Task Runner
  const debugEnv = getEnv("DEBUG") || process.env.DEBUG;
  if (debugEnv) {
    console.log(`[Local CI] Running with DEBUG=${debugEnv}`);
  }

  const TARGET_MATRIX = getTargetMatrix();
  const {
    targetFilter,
    isRelease,
    dryRun,
    buildAll,
    useCross,
    useNapiCross,
    crossCompile,
    filteredArgs,
  } = parseArgs();

  const buildCommand = createBuildCommand(filteredArgs);
  const options = buildCommand.getOptions();
  const cli = new NapiCli();

  let targetsToBuild: Array<{ target: string | null; flags: string }> = [];

  if (targetFilter) {
    const match = TARGET_MATRIX.find((t) => t.target === targetFilter);
    targetsToBuild = match
      ? [match]
      : [{ target: targetFilter, flags: "-x" }];
  } else if (buildAll) {
    targetsToBuild = TARGET_MATRIX;
  } else {
    targetsToBuild = [{ target: null, flags: "" }];
  }

  console.log(`\n🚀 Starting Local CI Pipeline (${targetsToBuild.length} target(s))...`);

  for (const { target, flags } of targetsToBuild) {
    const isNapiCrossTarget =
      target &&
      (target.includes("gnueabihf") ||
        target.includes("powerpc") ||
        target.includes("s390x") ||
        (target.includes("linux-gnu") && !target.includes("x86_64")));

    const effectiveUseNapiCross =
      useNapiCross || flags.includes("--use-napi-cross") || Boolean(target && isNapiCrossTarget);
    const effectiveCrossCompile =
      crossCompile || flags.includes("-x") || Boolean(target && !isNapiCrossTarget);

    console.log(`\n⚙️  Building target: ${target || "default host"}`);
    if (dryRun) {
      console.log(`   [Dry Run] Skipped build execution for target: ${target || "default host"}.`);
      continue;
    }

    try {
      const buildOpts: Record<string, any> = {
        ...options,
        cwd: options.cwd || process.cwd(),
        outputDir: options.outputDir || "./dist",
        release: isRelease,
        cargoOptions: buildCommand.cargoOptions,
      };

      if (target) {
        buildOpts.target = target;
      }
      if (useCross) buildOpts.useCross = true;
      if (effectiveUseNapiCross) buildOpts.useNapiCross = true;
      if (effectiveCrossCompile) buildOpts.crossCompile = true;

      const { task } = await cli.build(buildOpts as any);
      await task;
      console.log(`✅ Target ${target || "default host"} built successfully.`);
    } catch (err: any) {
      console.error(`❌ Target ${target || "default host"} failed to build:`, err?.message || err);
      if (!buildAll) {
        process.exit(1);
      }
    }
  }

  console.log(`\n✨ Local CI build process complete!`);
}

runLocalCI().catch((err) => {
  console.error("Fatal error during local CI build:", err);
  process.exit(1);
});
