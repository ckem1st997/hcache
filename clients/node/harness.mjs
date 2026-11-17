// Shared by the node-redis and ioredis checks: every check runs, nothing aborts the run.
import { isDeepStrictEqual, inspect } from "node:util";
import { readFileSync } from "node:fs";

export const PORT = Number(process.env.HCACHE_PORT ?? 6400);
// HCACHE_PROTO=2|3 forces a protocol; unset means the library's default handshake.
export const PROTO = process.env.HCACHE_PROTO ? Number(process.env.HCACHE_PROTO) : undefined;

const CHECK_TIMEOUT_MS = 30_000;
const AFTER_HANG_TIMEOUT_MS = 3_000;

const short = (v) => inspect(v, { depth: 3, breakLength: Infinity }).slice(0, 200);

export function eq(got, want) {
  if (!isDeepStrictEqual(got, want)) throw new Error(`got ${short(got)}, want ${short(want)}`);
}

export function ok(cond, msg) {
  if (!cond) throw new Error(msg);
}

export async function raises(fn, text) {
  let got;
  try {
    got = await fn();
  } catch (e) {
    const msg = String(e?.message ?? e);
    ok(msg.toLowerCase().includes(text.toLowerCase()), `error ${msg} lacks '${text}'`);
    return;
  }
  throw new Error(`no error, got ${short(got)}`);
}

export function version(pkg) {
  const path = new URL(`./node_modules/${pkg}/package.json`, import.meta.url);
  return JSON.parse(readFileSync(path, "utf8")).version;
}

export async function run(label, checks) {
  console.log(`== ${label} protocol=${PROTO ?? "default"}`);
  const failed = [];
  let limit = CHECK_TIMEOUT_MS;
  for (const [name, fn] of Object.entries(checks)) {
    let timer;
    const timeout = new Promise((_, reject) => {
      timer = setTimeout(() => reject(new Error(`timed out after ${limit} ms`)), limit);
    });
    try {
      await Promise.race([fn(), timeout]);
      console.log(`PASS ${name}`);
    } catch (e) {
      if (String(e?.message).startsWith("timed out")) limit = AFTER_HANG_TIMEOUT_MS;
      failed.push(name);
      console.log(`FAIL ${name}: ${e?.name ?? "Error"}: ${String(e?.message ?? e).slice(0, 300)}`);
    } finally {
      clearTimeout(timer);
    }
  }
  const total = Object.keys(checks).length;
  const tag = label.split(" ")[1];
  console.log(`== ${tag}: ${total - failed.length}/${total} passed; failed: [${failed.join(", ")}]`);
  process.exit(failed.length ? 1 : 0);
}
