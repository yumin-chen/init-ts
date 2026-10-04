import { defineConfig } from "vite-plus";

export default defineConfig({
  pack: {
    entry: ["./src/main.ts"],
    format: "esm",
    outDir: "dist",
    exports: true,
    dts: {
      generator: "tsgo",
    },
    deps: {
      resolveDepSubpath: true,
    },
  },
  test: {
    include: ["src/**/*.{test,spec}.ts"],
  },
  staged: {
    "*": ["vp check --fix", "npm run ci -- --dry-run"],
  },
  fmt: {
    ignore: ["examples/**"],
  },
  lint: {
    ignorePatterns: ["examples/**"],
    jsPlugins: [{ name: "vite-plus", specifier: "vite-plus/oxlint-plugin" }],
    rules: { "vite-plus/prefer-vite-plus-imports": "error" },
    options: { typeAware: true, typeCheck: true },
  },
  run: {
    cache: true,
  },
  tasks: {
    "ci:build": {
      command: "node --import @oxc-node/core/register scripts/cross-build.ts --target-all",
      cache: {
        input: [{ auto: true }, "!dist/**", "!target/**"],
        output: ["dist/**"],
        env: ["DEBUG", "NODE_ENV", "MACOSX_DEPLOYMENT_TARGET"],
      },
    },
    "ci:cross-build": {
      command: "node --import @oxc-node/core/register scripts/cross-build.ts --target-all",
      cache: {
        input: [{ auto: true }, "!dist/**", "!target/**"],
        output: ["dist/**"],
        env: ["DEBUG", "NODE_ENV", "MACOSX_DEPLOYMENT_TARGET"],
      },
    },
    "ci:test": {
      command: "vp test",
      cache: {
        input: [{ auto: true }],
      },
    },
    "ci:publish": {
      command: "vp pack",
      cache: {
        input: [{ auto: true }, "!dist/**"],
        output: ["dist/**"],
      },
    },
  },
});
