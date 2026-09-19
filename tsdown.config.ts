import { defineConfig } from "tsdown";

export default defineConfig({
  dts: true,
  exports: true,
  format: "esm",
  entry: ["./src/main.ts"],
  outDir: "dist",
});
