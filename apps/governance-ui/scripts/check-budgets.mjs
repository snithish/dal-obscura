import { gzipSync } from "node:zlib";
import { readdir, readFile } from "node:fs/promises";
import { join } from "node:path";

const root = new URL("../dist/assets/", import.meta.url);
const limits = {
  jsGzipKiB: 200,
  cssGzipKiB: 50,
  fontsKiB: 120,
};

const entries = await readdir(root, { withFileTypes: true });
const files = entries.filter((entry) => entry.isFile()).map((entry) => entry.name);
if (!files.length) throw new Error("Production assets are missing. Run pnpm build first.");

async function bytes(name) {
  return readFile(join(root.pathname, name));
}

async function sumGzip(names) {
  const buffers = await Promise.all(names.map(bytes));
  return buffers.reduce((total, buffer) => total + gzipSync(buffer).byteLength, 0);
}

async function sumRaw(names) {
  const buffers = await Promise.all(names.map(bytes));
  return buffers.reduce((total, buffer) => total + buffer.byteLength, 0);
}

const js = files.filter((name) => name.endsWith(".js"));
const css = files.filter((name) => name.endsWith(".css"));
const fonts = files.filter((name) => name.endsWith(".woff2"));
const measured = {
  jsGzipKiB: (await sumGzip(js)) / 1024,
  cssGzipKiB: (await sumGzip(css)) / 1024,
  fontsKiB: (await sumRaw(fonts)) / 1024,
};

console.log(JSON.stringify({ limits, measured }, null, 2));

const failures = Object.entries(limits)
  .filter(([name, limit]) => measured[name] > limit)
  .map(([name, limit]) => `${name} ${measured[name].toFixed(2)} KiB exceeds ${limit} KiB`);
if (failures.length) {
  console.error(`UI production budget failed:\n- ${failures.join("\n- ")}`);
  process.exitCode = 1;
}
