// HCache spike against node-redis. Mirrors clients/python/test_client.py check for check.
import { randomBytes } from "node:crypto";
import { createClient, defineScript, RESP_TYPES } from "redis";
import { PORT, PROTO, eq, ok, raises, run, version } from "./harness.mjs";

const P = "nr:";
const echoArg = defineScript({
  SCRIPT: "return ARGV[1]",
  NUMBER_OF_KEYS: 0,
  parseCommand(parser, arg) {
    parser.push(arg);
  },
  transformReply: undefined,
});
const opts = {
  socket: { host: "127.0.0.1", port: PORT, connectTimeout: 30_000, reconnectStrategy: false },
  scripts: { echoArg },
  ...(PROTO ? { RESP: PROTO } : {}),
};

function open() {
  const c = createClient(opts);
  c.on("error", () => {}); // errors surface through the failing command
  return c;
}

const r = open();
let connectError;
try {
  await r.connect();
} catch (e) {
  connectError = e;
}
const buf = () => r.withTypeMapping({ [RESP_TYPES.BLOB_STRING]: Buffer });
const range = (n) => Array.from({ length: n }, (_, i) => i);

const checks = {
  async connect_ping() {
    eq(await r.ping(), "PONG");
    eq(await r.echo("hi"), "hi");
  },
  async set_get() {
    eq(await r.set(P + "s", "hello"), "OK");
    eq(await r.get(P + "s"), "hello");
    eq(await r.get(P + "missing"), null);
  },
  async binary_key_and_value() {
    const v = Buffer.concat(range(4).map(() => Buffer.from(range(256))));
    const k = Buffer.concat([Buffer.from(P), Buffer.from([0, 13, 10, 255]), Buffer.from(" bin")]);
    await r.set(k, v);
    ok((await buf().get(k)).equals(v), "binary value differs");
    await r.set(P + "empty", Buffer.alloc(0));
    eq((await buf().get(P + "empty")).length, 0);
    await r.set(P + "uni", "xin chào 🌏");
    eq(await r.get(P + "uni"), "xin chào 🌏");
  },
  async big_value_8mib() {
    const v = randomBytes(8 << 20);
    await r.set(P + "big", v);
    ok((await buf().get(P + "big")).equals(v), "8 MiB value differs");
    await r.del(P + "big");
  },
  async counters() {
    await r.del(P + "n");
    eq(await r.incr(P + "n"), 1);
    eq(await r.incrBy(P + "n", 41), 42);
    eq(await r.decr(P + "n"), 41);
    eq(await r.decrBy(P + "n", 50), -9);
    eq(await r.get(P + "n"), "-9");
  },
  async counter_errors() {
    await r.set(P + "abc", "abc");
    await raises(() => r.incr(P + "abc"), "not an integer");
    const max = (2n ** 63n - 1n).toString();
    await r.set(P + "max", max);
    await raises(() => r.incr(P + "max"), "overflow");
    eq(await r.get(P + "max"), max);
  },
  async hashes() {
    await r.del(P + "h");
    eq(await r.hSet(P + "h", { a: "1", b: "2", c: "3" }), 3);
    eq(await r.hSet(P + "h", "a", "9"), 0);
    eq(await r.hGet(P + "h", "a"), "9");
    eq(await r.hGet(P + "h", "zz"), null);
    eq({ ...(await r.hGetAll(P + "h")) }, { a: "9", b: "2", c: "3" });
    eq(await r.hLen(P + "h"), 3);
    eq(await r.hDel(P + "h", ["a", "zz"]), 1);
    eq({ ...(await r.hGetAll(P + "nohash")) }, {});
  },
  async wrong_type() {
    await r.set(P + "str", "x");
    await raises(() => r.hGet(P + "str", "f"), "WRONGTYPE");
    await r.del(P + "h2");
    await r.hSet(P + "h2", "f", "v");
    await raises(() => r.get(P + "h2"), "WRONGTYPE");
    await raises(() => r.incr(P + "h2"), "WRONGTYPE");
  },
  async mset_mget() {
    eq(await r.mSet({ [P + "m1"]: "1", [P + "m2"]: "2" }), "OK");
    eq(await r.mGet([P + "m1", P + "nope", P + "m2"]), ["1", null, "2"]);
  },
  async del_exists_unlink() {
    await r.mSet({ [P + "d1"]: "1", [P + "d2"]: "2", [P + "d3"]: "3" });
    eq(await r.exists([P + "d1", P + "d2", P + "nope", P + "d1"]), 3);
    eq(await r.del([P + "d1", P + "nope"]), 1);
    eq(await r.unlink([P + "d2", P + "d3"]), 2);
    eq(await r.exists([P + "d1", P + "d2", P + "d3"]), 0);
  },
  async keys_pattern() {
    await r.mSet({ [P + "k:1"]: "a", [P + "k:2"]: "b", [P + "k:x["]: "c" });
    eq((await r.keys(P + "k:?")).sort(), [P + "k:1", P + "k:2"]);
    eq(await r.keys(P + "k:x\\["), [P + "k:x["]);
    eq(await r.keys(P + "nothing*"), []);
  },
  async eval_values() {
    eq(
      await r.eval("return {KEYS[1], ARGV[1], 7, {1, 2}}", { keys: [P + "k"], arguments: ["a"] }),
      [P + "k", "a", 7, [1, 2]],
    );
    eq(await r.eval("return false"), null);
    eq(await r.eval("return redis.status_reply('FINE')"), "FINE");
    eq(await r.eval("return 3.99"), 3);
    const bin = Buffer.from([0, 255]);
    ok((await buf().eval("return ARGV[1]", { arguments: [bin] })).equals(bin), "binary ARGV differs");
  },
  async eval_calls_redis() {
    const script = "redis.call('SET', KEYS[1], ARGV[1]); return redis.call('GET', KEYS[1])";
    eq(await r.eval(script, { keys: [P + "ev"], arguments: ["from-lua"] }), "from-lua");
    eq(await r.get(P + "ev"), "from-lua");
    await r.del(P + "evn");
    eq(await r.eval("return redis.call('INCRBY', KEYS[1], 5)", { keys: [P + "evn"] }), 5);
  },
  async eval_errors() {
    await raises(() => r.eval("return redis.call('NOSUCH')"), "unknown command");
    await raises(() => r.eval("return redis.error_reply('boom')"), "boom");
    await raises(() => r.eval("this is not lua"), "ERR");
    await raises(() => r.eval("error('thrown')"), "thrown");
    eq(await r.ping(), "PONG");
  },
  async script_helper_evalsha() {
    eq(await r.echoArg("x"), "x");
    eq(await r.echoArg("y"), "y");
  },
  async pipeline_plain_10000() {
    await r.del(P + "pn");
    const p = r.multi();
    for (let i = 0; i < 10000; i++) p.incr(P + "pn");
    eq(await p.execAsPipeline(), range(10000).map((i) => i + 1));
  },
  async pipeline_with_error_in_middle() {
    await r.set(P + "pe", "abc");
    // execAsPipeline rejects with the first error only, so use node-redis's auto-pipelining:
    // commands issued in the same tick go out in one write.
    const settled = await Promise.allSettled([r.set(P + "pa", "1"), r.incr(P + "pe"), r.get(P + "pa")]);
    const out = settled.map((x) => (x.status === "fulfilled" ? x.value : x.reason));
    eq(out[0], "OK");
    ok(out[1] instanceof Error, `expected an error, got ${out[1]}`);
    eq(out[2], "1");
  },
  async x_pipeline_transaction_multi_exec() {
    eq(await r.multi().set(P + "tx", "1").get(P + "tx").exec(), ["OK", "1"]);
  },
  async x_set_with_expiry_and_nx() {
    eq(await r.set(P + "ex", "v", { EX: 60 }), "OK");
    const ttl = await r.ttl(P + "ex");
    ok(ttl > 0 && ttl <= 60, `ttl ${ttl}`);
    eq(await r.set(P + "ex", "other", { NX: true }), null);
  },
  async concurrency_16_threads() {
    await r.del(P + "c");
    await Promise.all(
      range(16).map(async () => {
        const c = open();
        await c.connect();
        try {
          for (let i = 0; i < 500; i++) await c.incr(P + "c");
        } finally {
          c.destroy();
        }
      }),
    );
    eq(await r.get(P + "c"), "8000");
  },
  async unknown_command_keeps_connection() {
    await raises(() => r.sendCommand(["NOSUCHCMD", "a"]), "unknown command");
    await raises(() => r.sendCommand(["GET"]), "wrong number of arguments");
    eq(await r.ping(), "PONG");
  },
  async hgetall_100k_fields() {
    await r.del(P + "bigh");
    const p = r.multi();
    for (let i = 0; i < 100000; i += 1000) {
      p.hSet(P + "bigh", Object.fromEntries(range(1000).map((j) => [`f${i + j}`, `v${i + j}`])));
    }
    eq((await p.execAsPipeline()).reduce((sum, n) => sum + n, 0), 100000);
    const got = await r.hGetAll(P + "bigh");
    eq(Object.keys(got).length, 100000);
    eq(got.f99999, "v99999");
    eq(await r.del(P + "bigh"), 1);
  },
  async client_side_commands() {
    eq(await r.clientSetName("nr-test"), "OK");
    eq(typeof (await r.info()), "string");
    await r.select(0); // node-redis returns nothing from select
    await raises(() => r.sendCommand(["SELECT", "16"]), "out of range");
  },
  async lists() {
    const k = P + "l", k2 = P + "l2";
    await r.del([k, k2]);
    eq(await r.rPush(k, ["a", "b", "c"]), 3);
    eq(await r.lPush(k, ["z", "y"]), 5); // y z a b c
    eq(await r.lLen(k), 5);
    eq(await r.lRange(k, 0, -1), ["y", "z", "a", "b", "c"]);
    eq(await r.lRange(k, -2, -1), ["b", "c"]);
    eq(await r.lRange(k, 10, 20), []);
    eq(await r.lIndex(k, 0), "y");
    eq(await r.lIndex(k, -1), "c");
    eq(await r.lIndex(k, 99), null);
    eq(await r.lSet(k, 1, "Z"), "OK");
    await raises(() => r.lSet(k, 99, "x"), "out of range");
    eq(await r.lPop(k), "y");
    eq(await r.rPop(k), "c");
    eq(await r.lPopCount(k, 2), ["Z", "a"]);
    eq(await r.rPopCount(k, 5), ["b"]);
    eq(await r.lPop(k), null);
    eq(await r.lPopCount(k, 2), null);
    eq(await r.exists(k), 0);
    await r.rPush(k, ["a", "b", "a", "c", "a"]);
    eq(await r.lRem(k, 2, "a"), 2);
    eq(await r.lRange(k, 0, -1), ["b", "c", "a"]);
    eq(await r.lRem(k, -1, "a"), 1);
    eq(await r.lRem(k, 0, "nope"), 0);
    await r.rPush(k, ["d", "e", "f"]); // b c d e f
    eq(await r.lTrim(k, 1, -2), "OK");
    eq(await r.lRange(k, 0, -1), ["c", "d", "e"]);
    eq(await r.lInsert(k, "BEFORE", "d", "cd"), 4);
    eq(await r.lInsert(k, "AFTER", "e", "ef"), 5);
    eq(await r.lInsert(k, "BEFORE", "missing", "x"), -1);
    eq(await r.lInsert(P + "nolist", "BEFORE", "a", "x"), 0);
    eq(await r.lRange(k, 0, -1), ["c", "cd", "d", "e", "ef"]);
    await r.rPush(k, "d");
    eq(await r.lPos(k, "d"), 2);
    eq(await r.lPos(k, "d", { RANK: 2 }), 5);
    eq(await r.lPosCount(k, "d", 0), [2, 5]);
    eq(await r.lPos(k, "nope"), null);
    eq(await r.lMove(k, k2, "LEFT", "RIGHT"), "c");
    eq(await r.lMove(k, k2, "RIGHT", "LEFT"), "d");
    eq(await r.lRange(k2, 0, -1), ["d", "c"]);
    eq(await r.rPopLPush(k, k2), "ef");
    eq(await r.lRange(k2, 0, -1), ["ef", "d", "c"]);
    eq(await r.lMove(P + "nolist", k2, "LEFT", "LEFT"), null);
    eq(await r.type(k), "list");
    await raises(() => r.get(k), "WRONGTYPE");
    await r.set(P + "lstr", "x");
    await raises(() => r.lPush(P + "lstr", "x"), "WRONGTYPE");
    eq(await r.del([k, k2, P + "lstr"]), 3);
  },
  async sets() {
    const k = ["a", "b", "c"].map((s) => P + "set" + s);
    const d = P + "setd";
    const arr = (x) => [...x].sort();
    await r.del([...k, d]);
    eq(await r.sAdd(k[0], ["1", "2", "3", "4", "4"]), 4);
    eq(await r.sAdd(k[0], "1"), 0);
    eq(await r.sCard(k[0]), 4);
    eq(Number(await r.sIsMember(k[0], "2")), 1);
    eq(Number(await r.sIsMember(k[0], "9")), 0);
    eq((await r.smIsMember(k[0], ["1", "9", "4"])).map(Number), [1, 0, 1]);
    eq(arr(await r.sMembers(k[0])), ["1", "2", "3", "4"]);
    eq(await r.sRem(k[0], ["4", "9"]), 1);
    await r.sAdd(k[1], ["2", "3", "5"]);
    await r.sAdd(k[2], ["3", "6"]);
    eq(arr(await r.sInter([k[0], k[1]])), ["2", "3"]);
    eq(arr(await r.sInter([k[0], k[1], k[2]])), ["3"]);
    eq(arr(await r.sUnion([k[0], k[1], k[2]])), ["1", "2", "3", "5", "6"]);
    eq(arr(await r.sDiff([k[0], k[1]])), ["1"]);
    eq(arr(await r.sDiff([k[0], k[1], k[2]])), ["1"]);
    eq(await r.sInterStore(d, [k[0], k[1]]), 2);
    eq(arr(await r.sMembers(d)), ["2", "3"]);
    eq(await r.sUnionStore(d, [k[0], k[1]]), 4);
    eq(await r.sDiffStore(d, [k[1], k[0]]), 1);
    eq(arr(await r.sMembers(d)), ["5"]);
    eq(await r.sInterCard([k[0], k[1]]), 2);
    eq(await r.sInterCard([k[0], k[1]], { LIMIT: 1 }), 1);
    eq(Number(await r.sMove(k[0], k[2], "1")), 1);
    eq(Number(await r.sMove(k[0], k[2], "nope")), 0);
    eq(arr(await r.sMembers(k[2])), ["1", "3", "6"]);
    ok(["1", "3", "6"].includes(await r.sRandMember(k[2])), "srandmember outside set");
    eq(new Set(await r.sRandMemberCount(k[2], 2)).size, 2);
    eq((await r.sRandMemberCount(k[2], 10)).length, 3);
    eq((await r.sRandMemberCount(k[2], -5)).length, 5);
    eq(await r.sRandMember(P + "noset"), null);
    ok(["1", "3", "6"].includes(await r.sPop(k[2])), "spop outside set");
    eq((await r.sPopCount(k[2], 5)).length, 2);
    eq(await r.exists(k[2]), 0);
    eq(await r.sPop(k[2]), null);
    const big = P + "setbig";
    await r.del(big);
    await r.sAdd(big, range(300).map((i) => `m${i}`));
    const seen = new Set();
    let cursor = "0";
    for (let i = 0; i < 1000; i++) {
      const res = await r.sScan(big, cursor, { COUNT: 50 });
      res.members.forEach((x) => seen.add(x));
      cursor = String(res.cursor);
      if (cursor === "0") break;
    }
    eq(seen.size, 300);
    eq(await r.type(big), "set");
    await raises(() => r.get(big), "WRONGTYPE");
    await r.del([big, ...k, d]);
  },
  async set_reply_shape() {
    // RESP3 has a set type. node-redis 5 turns it into an Array unless a
    // type mapping asks for a Set, so the members are what is checked here;
    // the type byte on the wire is checked by the server's own tests.
    await r.del(P + "shp");
    await r.sAdd(P + "shp", ["a", "b"]);
    const want = Object;
    const bad = [];
    for (const [name, got] of [
      ["SMEMBERS", await r.sMembers(P + "shp")],
      ["SINTER", await r.sInter([P + "shp", P + "shp"])],
      ["SUNION", await r.sUnion([P + "shp", P + "shp"])],
      ["SDIFF", await r.sDiff([P + "shp"])],
    ]) {
      const members = [...got].map(String).sort().join(",");
      const expected = "a,b";
      if (!(got instanceof want) || members !== expected) bad.push(`${name} gave ${members}`);
    }
    ok(bad.length === 0, `want ${want.name}: ${bad.join("; ")}`);
    await r.del(P + "shp");
  },
  async zsets_basic() {
    // membership and order only; score replies are checked in zset_scores
    const k = P + "z";
    const m = (score, value) => ({ score, value });
    await r.del(k);
    eq(await r.zAdd(k, [m(1, "a"), m(2.5, "b"), m(1.5, "c")]), 3); // a c b
    eq(await r.zAdd(k, m(1, "a")), 0);
    eq(await r.zAdd(k, m(5, "a"), { CH: true }), 1); // c b a
    eq(await r.zAdd(k, [m(7, "d"), m(5, "a")], { CH: true }), 1); // c b a d
    eq(await r.zAdd(k, m(9, "a"), { condition: "NX" }), 0);
    eq(await r.zRange(k, 0, -1), ["c", "b", "a", "d"]);
    eq(await r.zAdd(k, m(1, "zz"), { condition: "XX" }), 0);
    eq(await r.zCard(k), 4);
    eq(await r.zAdd(k, m(3, "a"), { comparison: "GT", CH: true }), 0);
    eq(await r.zAdd(k, m(8, "a"), { comparison: "GT", CH: true }), 1); // c b d a
    eq(await r.zRange(k, 0, -1), ["c", "b", "d", "a"]);
    eq(await r.zAdd(k, m(10, "a"), { comparison: "LT", CH: true }), 0);
    eq(await r.zAdd(k, m(6, "a"), { comparison: "LT", CH: true }), 1); // c b a d
    eq(await r.zRange(k, 0, -1), ["c", "b", "a", "d"]);
    eq(await r.zAddIncr(k, m(1, "a"), { condition: "NX" }), null);
    eq(await r.zAddIncr(k, m(1, "nope"), { condition: "XX" }), null);
    eq(await r.zRank(k, "c"), 0);
    eq(await r.zRank(k, "d"), 3);
    eq(await r.zRevRank(k, "d"), 0);
    eq(await r.zRevRank(k, "c"), 3);
    eq(await r.zRank(k, "nope"), null);
    eq(await r.zRevRank(k, "nope"), null);
    eq(await r.zCount(k, 2, 6), 2);
    eq(await r.zCount(k, "(2.5", 7), 2);
    eq(await r.zCount(k, "(1.5", "(7"), 2);
    eq(await r.zCount(k, "-inf", "+inf"), 4);
    eq(await r.zCount(k, 100, 200), 0);
    eq(await r.zRem(k, ["a", "nope"]), 1);
    eq(await r.zRange(k, 0, -1), ["c", "b", "d"]);
    eq(await r.type(k), "zset");
    await raises(() => r.get(k), "WRONGTYPE");
    await raises(() => r.sendCommand(["ZADD", k, "notanumber", "a"]), "not a valid float");
    await raises(() => r.sendCommand(["ZADD", k, "NX", "XX", "1", "a"]), "not compatible");
    eq(await r.del(k), 1);
  },
  async zset_scores() {
    const k = P + "zsc";
    const m = (score, value) => ({ score, value });
    await r.del(k);
    await r.zAdd(k, [m(1, "a"), m(2.5, "b"), m(1.5, "c")]);
    eq(await r.zScore(k, "a"), 1);
    eq(await r.zScore(k, "c"), 1.5);
    eq(await r.zScore(k, "nope"), null);
    eq(await r.zmScore(k, ["a", "nope", "c"]), [1, null, 1.5]);
    eq(await r.zAddIncr(k, m(4.25, "a")), 5.25);
    eq(await r.zAddIncr(k, m(-0.25, "a"), { condition: "XX" }), 5);
    eq(await r.zIncrBy(k, 0.5, "c"), 2);
    eq(await r.zIncrBy(k, 1, "new"), 1); // new c b a
    eq(await r.zRankWithScore(k, "c"), { rank: 1, score: 2 });
    eq(await r.zRevRankWithScore(k, "c"), { rank: 2, score: 2 });
    eq(await r.zRankWithScore(k, "nope"), null);
    eq(await r.zRangeWithScores(k, 0, -1), [
      { value: "new", score: 1 }, { value: "c", score: 2 }, { value: "b", score: 2.5 }, { value: "a", score: 5 },
    ]);
    eq(await r.zRangeWithScores(k, 0, 1, { REV: true }), [{ value: "a", score: 5 }, { value: "b", score: 2.5 }]);
    eq(await r.zRangeByScoreWithScores(k, 2, 3), [{ value: "c", score: 2 }, { value: "b", score: 2.5 }]);
    eq(await r.zRangeWithScores(k, 5, 2.5, { BY: "SCORE", REV: true }), [{ value: "a", score: 5 }, { value: "b", score: 2.5 }]);
    eq(await r.zRangeWithScores(k, 2, 5, { BY: "SCORE", LIMIT: { offset: 1, count: 1 } }), [{ value: "b", score: 2.5 }]);
    await r.del(k);
    await r.zAdd(k, [m(-Infinity, "lo"), m(Infinity, "hi"), m(1.5, "mid"), m(-0.1, "frac")]);
    eq(await r.zRange(k, 0, -1), ["lo", "frac", "mid", "hi"]);
    eq(await r.zRangeWithScores(k, 0, -1), [
      { value: "lo", score: -Infinity }, { value: "frac", score: -0.1 }, { value: "mid", score: 1.5 }, { value: "hi", score: Infinity },
    ]);
    eq(await r.zScore(k, "hi"), Infinity);
    eq(await r.zScore(k, "lo"), -Infinity);
    eq(await r.zmScore(k, ["lo", "hi"]), [-Infinity, Infinity]);
    eq(await r.zRangeByScore(k, "-inf", "+inf"), ["lo", "frac", "mid", "hi"]);
    eq(await r.zRangeByScore(k, "(-inf", "(inf"), ["frac", "mid"]);
    eq(await r.zCount(k, "-inf", 0), 2);
    eq(await r.zIncrBy(k, 5, "hi"), Infinity);
    const res = await r.zScan(k, "0");
    eq(Object.fromEntries(res.members.map((e) => [e.value, e.score])), { lo: -Infinity, frac: -0.1, mid: 1.5, hi: Infinity });
    await r.del(k);
  },
  async zset_pop() {
    const k = P + "zp";
    await r.del(k);
    await r.zAdd(k, [{ score: 1, value: "a" }, { score: 2, value: "b" }, { score: 3, value: "c" },
      { score: 4.5, value: "d" }, { score: 5, value: "e" }]);
    eq(await r.zPopMin(k), { value: "a", score: 1 });
    eq(await r.zPopMax(k), { value: "e", score: 5 });
    eq(await r.zPopMinCount(k, 2), [{ value: "b", score: 2 }, { value: "c", score: 3 }]);
    eq(await r.zPopMaxCount(k, 5), [{ value: "d", score: 4.5 }]);
    eq(await r.zPopMin(k), null);
    eq(await r.zPopMaxCount(k, 3), []);
    eq(await r.exists(k), 0);
  },
  async zsets_ranges() {
    const k = P + "zr";
    await r.del(k);
    await r.zAdd(k, ["a", "b", "c", "d", "e"].map((value, i) => ({ score: i + 1, value })));
    eq(await r.zRange(k, 0, 1), ["a", "b"]);
    eq(await r.zRange(k, -2, -1), ["d", "e"]);
    eq(await r.zRange(k, 0, 1, { REV: true }), ["e", "d"]);
    eq(await r.zRange(k, 2, 3, { BY: "SCORE" }), ["b", "c"]);
    eq(await r.zRange(k, "(1", "+inf", { BY: "SCORE", LIMIT: { offset: 1, count: 2 } }), ["c", "d"]);
    eq(await r.zRange(k, "+inf", "(2", { BY: "SCORE", REV: true, LIMIT: { offset: 0, count: 2 } }), ["e", "d"]);
    eq(await r.zRangeByScore(k, "-inf", 3), ["a", "b", "c"]);
    eq(await r.zRangeByScore(k, "(1", "(4"), ["b", "c"]);
    eq(await r.zRangeByScore(k, 1, 5, { LIMIT: { offset: 1, count: 2 } }), ["b", "c"]);
    // node-redis has no ZREVRANGE / ZREVRANGEBYSCORE wrappers (ZRANGE ... REV replaces them)
    eq(await r.sendCommand(["ZREVRANGE", k, "0", "1"]), ["e", "d"]);
    eq(await r.sendCommand(["ZREVRANGEBYSCORE", k, "5", "4"]), ["e", "d"]);
    eq(await r.sendCommand(["ZREVRANGEBYSCORE", k, "+inf", "-inf", "LIMIT", "0", "1"]), ["e"]);
    eq(await r.zRange(k, 10, 20), []);
    eq(await r.zRange(P + "nozset", 0, -1), []);
    eq(await r.zRemRangeByRank(k, 0, 1), 2); // c d e
    eq(await r.zRemRangeByScore(k, "(3", 4), 1); // c e
    eq(await r.zRange(k, 0, -1), ["c", "e"]);
    eq(await r.zRemRangeByScore(k, "-inf", "+inf"), 2);
    eq(await r.exists(k), 0);
    const big = P + "zbig";
    await r.del(big);
    await r.zAdd(big, range(300).map((i) => ({ score: i, value: `m${i}` })));
    const seen = new Map();
    let cursor = "0";
    for (let i = 0; i < 1000; i++) {
      const res = await r.zScan(big, cursor, { COUNT: 50 });
      res.members.forEach((e) => seen.set(e.value, e.score));
      cursor = String(res.cursor);
      if (cursor === "0") break;
    }
    eq(seen.size, 300);
    eq(seen.get("m299"), 299);
    await r.del(big);
  },
  async zsets_store() {
    const [a, c, d] = [P + "zsa", P + "zsb", P + "zsd"];
    const scores = async (key) => {
      const res = await r.zScan(key, "0", { COUNT: 1000 });
      return Object.fromEntries(res.members.map((e) => [e.value, e.score]));
    };
    await r.del([a, c, d]);
    await r.zAdd(a, [{ score: 1, value: "x" }, { score: 2, value: "y" }, { score: 3, value: "z" }]);
    await r.zAdd(c, [{ score: 10, value: "y" }, { score: 20, value: "z" }, { score: 0.5, value: "w" }]);
    eq(await r.zUnionStore(d, [a, c]), 4);
    eq(await r.zRange(d, 0, -1), ["w", "x", "y", "z"]);
    eq(await r.zUnionStore(d, [{ key: a, weight: 2 }, { key: c, weight: 0.5 }]), 4);
    eq(await scores(d), { w: 0.25, x: 2, y: 9, z: 16 });
    eq(await r.zUnionStore(d, [a, c], { AGGREGATE: "MAX" }), 4);
    eq((await scores(d)).z, 20);
    eq(await r.zInterStore(d, [a, c]), 2);
    eq(await scores(d), { y: 12, z: 23 });
    eq(await r.zInterStore(d, [{ key: a, weight: 3 }, { key: c, weight: 2 }]), 2);
    eq(await scores(d), { y: 26, z: 49 });
    eq(await r.zInterStore(d, [a, c], { AGGREGATE: "MIN" }), 2);
    eq((await scores(d)).y, 2);
    await r.sAdd(P + "zss", ["y", "q"]);
    eq(await r.zInterStore(d, [a, P + "zss"]), 1); // sets count as score 1
    eq((await scores(d)).y, 3);
    eq(await r.zInterStore(d, [a, P + "nozset"]), 0);
    eq(await r.exists(d), 0);
    await r.del([a, c, d, P + "zss"]);
  },
  async string_key_extras() {
    const h = P + "hx";
    await r.del([P + "f", P + "gr", P + "grnew", P + "abc", h]);
    eq(await r.incrByFloat(P + "f", 1.5), "1.5");
    eq(await r.incrByFloat(P + "f", 2), "3.5");
    eq(await r.incrByFloat(P + "f", -0.25), "3.25");
    eq(await r.get(P + "f"), "3.25");
    await r.set(P + "abc", "abc");
    await raises(() => r.incrByFloat(P + "abc", 1), "not a valid float");
    await r.set(P + "gr", "Hello World");
    eq(await r.getRange(P + "gr", 0, 4), "Hello");
    eq(await r.getRange(P + "gr", -5, -1), "World");
    eq(await r.getRange(P + "gr", 20, 30), "");
    eq(await r.setRange(P + "gr", 6, "Redis"), 11);
    eq(await r.get(P + "gr"), "Hello Redis");
    eq(await r.setRange(P + "gr", 13, "!"), 14);
    eq(await r.get(P + "gr"), "Hello Redis\u0000\u0000!");
    eq(await r.setRange(P + "grnew", 2, "x"), 3);
    eq(await r.get(P + "grnew"), "\u0000\u0000x");
    await r.hSet(h, { a: "1", b: "hello", c: "2.5" });
    eq((await r.hKeys(h)).sort(), ["a", "b", "c"]);
    eq((await r.hVals(h)).sort(), ["1", "2.5", "hello"]);
    eq(await r.hStrLen(h, "b"), 5);
    eq(await r.hStrLen(h, "nope"), 0);
    eq(await r.hIncrByFloat(h, "c", 0.5), "3");
    eq(await r.hIncrByFloat(h, "new", 1.5), "1.5");
    eq(await r.hIncrBy(h, "a", 4), 5);
    await raises(() => r.hIncrByFloat(h, "b", 1), "not a float");
    ok(["a", "b", "c", "new"].includes(await r.hRandField(h)), "hrandfield outside hash");
    eq((await r.hRandFieldCount(h, 2)).length, 2);
    eq((await r.hRandFieldCount(h, -6)).length, 6);
    eq(await r.hRandField(P + "nohash"), null);
    eq(await r.hRandFieldCount(P + "nohash", 3), []);
    eq(Number(await r.hExists(h, "a")), 1);
    eq(await r.hmGet(h, ["a", "zz"]), ["5", null]);
    eq(Number(await r.hSetNX(h, "a", "x")), 0);
    await r.del([P + "f", P + "gr", P + "grnew", P + "abc", h]);
  },
  async hrandfield_values() {
    const h = P + "hrf";
    await r.del(h);
    await r.hSet(h, { a: "1", b: "hello", c: "3" });
    const got = await r.hRandFieldCountWithValues(h, 3);
    eq(got.length, 3);
    eq(Object.fromEntries(got.map((e) => [e.field, e.value])), { a: "1", b: "hello", c: "3" });
    eq((await r.hRandFieldCountWithValues(h, -5)).length, 5);
    await r.del(h);
  },
  async keys_misc() {
    const names = ["r1", "r2", "r3", "cp1", "cp2", "ms1", "ms2", "ms3", "ty:s", "ty:h", "ty:l", "ty:set", "ty:z", "ty:int"];
    await r.del(names.map((n) => P + n));
    await r.set(P + "r1", "v");
    eq(await r.rename(P + "r1", P + "r2"), "OK");
    eq(await r.exists([P + "r1", P + "r2"]), 1);
    await raises(() => r.rename(P + "nokey", P + "x"), "no such key");
    await r.set(P + "r1", "other");
    eq(Number(await r.renameNX(P + "r1", P + "r2")), 0);
    eq(await r.get(P + "r2"), "v");
    eq(Number(await r.renameNX(P + "r1", P + "r3")), 1);
    eq(await r.get(P + "r3"), "other");
    await r.rPush(P + "cp1", ["a", "b"]);
    eq(Number(await r.copy(P + "cp1", P + "cp2")), 1);
    eq(Number(await r.copy(P + "cp1", P + "cp2")), 0);
    eq(Number(await r.copy(P + "cp1", P + "cp2", { REPLACE: true })), 1);
    eq(await r.lRange(P + "cp2", 0, -1), ["a", "b"]);
    await r.rPush(P + "cp1", "c");
    eq(await r.lLen(P + "cp2"), 2); // a copy, not an alias
    eq(Number(await r.copy(P + "nokey", P + "cp3")), 0);
    eq(Number(await r.mSetNX({ [P + "ms1"]: "1", [P + "ms2"]: "2" })), 1);
    eq(Number(await r.mSetNX({ [P + "ms1"]: "x", [P + "ms3"]: "3" })), 0);
    eq(await r.exists(P + "ms3"), 0);
    eq(await r.get(P + "ms1"), "1");
    eq(await r.touch([P + "r2", P + "cp1", P + "nokey"]), 2);
    await r.set(P + "ty:s", "x");
    await r.hSet(P + "ty:h", "f", "v");
    await r.rPush(P + "ty:l", "x");
    await r.sAdd(P + "ty:set", "x");
    await r.zAdd(P + "ty:z", { score: 1, value: "x" });
    eq(await Promise.all(["s", "h", "l", "set", "z"].map((t) => r.type(P + "ty:" + t))),
      ["string", "hash", "list", "set", "zset"]);
    eq(await r.type(P + "nokey"), "none");
    for (const n of ["ty:s", "ty:h", "ty:l", "ty:set", "ty:z"]) {
      const enc = await r.objectEncoding(P + n);
      ok(typeof enc === "string" && enc.length > 0, `encoding of ${n}: ${enc}`);
    }
    eq(await r.objectEncoding(P + "nokey"), null);
    ok(["embstr", "raw"].includes(await r.objectEncoding(P + "ty:s")), "string encoding");
    await r.set(P + "ty:int", "12345");
    eq(await r.objectEncoding(P + "ty:int"), "int");
    const scanType = async (type) => {
      const got = new Set();
      let cursor = "0";
      for (let i = 0; i < 10000; i++) {
        const res = await r.scan(cursor, { MATCH: P + "ty:*", COUNT: 100, TYPE: type });
        res.keys.forEach((x) => got.add(x));
        cursor = String(res.cursor);
        if (cursor === "0") break;
      }
      return [...got].sort();
    };
    eq(await scanType("string"), [P + "ty:int", P + "ty:s"]);
    eq(await scanType("hash"), [P + "ty:h"]);
    eq(await scanType("list"), [P + "ty:l"]);
    eq(await scanType("set"), [P + "ty:set"]);
    eq(await scanType("zset"), [P + "ty:z"]);
    const rk = await r.randomKey();
    ok(rk !== null && (await r.exists(rk)) === 1, `randomkey ${rk}`);
    await r.del(names.map((n) => P + n));
  },
  async pubsub_roundtrip() {
    const sub = open();
    await sub.connect();
    const q = [];
    const onMessage = (message, channel) => q.push({ type: "message", channel, message });
    const onPattern = (message, channel) => q.push({ type: "pmessage", channel, message });
    const next = async (ms = 5000) => {
      const end = Date.now() + ms;
      while (!q.length) {
        if (Date.now() > end) throw new Error(`no pub/sub message within ${ms} ms`);
        await new Promise((res) => setTimeout(res, 20));
      }
      return q.shift();
    };
    const [ch, ch2, pat, both] = [P + "chan", P + "chan2", P + "pat:*", P + "pat:both"];
    try {
      await sub.subscribe(ch, onMessage);
      await sub.pSubscribe(pat, onPattern);
      eq(await r.publish(ch, "hello"), 1);
      eq(await next(), { type: "message", channel: ch, message: "hello" });
      eq(await r.publish(P + "pat:one", "p1"), 1);
      eq(await next(), { type: "pmessage", channel: P + "pat:one", message: "p1" });
      eq(await r.publish(P + "other", "x"), 0);
      eq(await r.publish(ch2, "x"), 0);
      await sub.subscribe(both, onMessage);
      await sub.pSubscribe(P + "pat:b*", onPattern);
      eq(await r.publish(both, "twice"), 3);
      const got = [await next(), await next(), await next()];
      eq(got.map((m) => m.type).sort(), ["message", "pmessage", "pmessage"]);
      eq(got.every((m) => m.message === "twice"), true);
      eq(await r.pubSubNumSub([ch, ch2]), { [ch]: 1, [ch2]: 0 });
      eq(await r.pubSubNumPat(), 2);
      eq((await r.pubSubChannels(P + "*")).sort(), [ch, both].sort());
      await sub.pUnsubscribe(P + "pat:b*");
      await sub.unsubscribe(ch);
      eq(await r.publish(ch, "gone"), 0);
      await sub.pUnsubscribe(pat);
      eq(await r.publish(P + "pat:one", "gone"), 0);
      await sub.unsubscribe();
      eq(await r.pubSubNumSub([both]), { [both]: 0 });
      eq(q.length, 0);
    } finally {
      sub.destroy();
    }
  },
  async tx_new_types() {
    await r.del([P + "txl", P + "txz", P + "txs", P + "w", P + "txstr"]);
    let out = await r.multi().rPush(P + "txl", ["a", "b"]).zAdd(P + "txz", [{ score: 1.5, value: "m" }, { score: 2, value: "n" }])
      .sAdd(P + "txs", ["x", "y"]).lRange(P + "txl", 0, -1).zRange(P + "txz", 0, -1).sCard(P + "txs").exec();
    eq(out, [2, 2, 2, ["a", "b"], ["m", "n"], 2]);
    out = await r.multi().lPop(P + "txl").zRem(P + "txz", "m").sPopCount(P + "txs", 2).exec();
    eq(out[0], "a");
    eq(out[1], 1);
    eq([...out[2]].sort(), ["x", "y"]);
    await r.set(P + "txstr", "s");
    // an error inside EXEC does not roll back the other commands
    try {
      await r.multi().rPush(P + "txl", "q").lPush(P + "txstr", "bad").lLen(P + "txl").exec();
      throw new Error("EXEC should have reported the failed command");
    } catch (e) {
      ok(Array.isArray(e?.errorIndexes), `unexpected error ${e}`);
      eq(e.errorIndexes, [1]);
      eq(e.replies[0], 2);
      ok(/WRONGTYPE/.test(String(e.replies[1].message ?? e.replies[1])), `expected WRONGTYPE, got ${e.replies[1]}`);
      eq(e.replies[2], 2);
    }
    // WATCH aborts EXEC when the key changed
    await r.set(P + "w", "1");
    const w = open();
    await w.connect();
    try {
      eq(await w.watch(P + "w"), "OK");
      await r.set(P + "w", "2");
      let aborted = false;
      try {
        await w.multi().zAdd(P + "txz", { score: 1, value: "w" }).exec();
      } catch (e) {
        aborted = /watched keys/.test(String(e?.message));
        if (!aborted) throw e;
      }
      ok(aborted, "EXEC should have aborted with a WatchError");
    } finally {
      w.destroy();
    }
    eq(await r.zScore(P + "txz", "w"), null);
    await r.del([P + "txl", P + "txz", P + "txs", P + "w", P + "txstr"]);
  },
  async lua_new_types() {
    const [z, l, s] = [P + "lz", P + "ll", P + "ls"];
    await r.del([z, l, s]);
    const script = `
redis.call('ZADD', KEYS[1], 1.5, 'a', 2, 'b', 3, 'c')
redis.call('RPUSH', KEYS[2], 'x', 'y', 'z')
redis.call('SADD', KEYS[3], 'm', 'n')
return {redis.call('ZRANGE', KEYS[1], 0, -1), redis.call('LRANGE', KEYS[2], 0, -1),
        redis.call('SCARD', KEYS[3]), redis.call('ZSCORE', KEYS[1], 'a'),
        redis.call('ZRANGE', KEYS[1], 0, 0, 'WITHSCORES'), redis.call('TYPE', KEYS[2])['ok']}`;
    eq(await r.eval(script, { keys: [z, l, s] }), [["a", "b", "c"], ["x", "y", "z"], 2, "1.5", ["a", "1.5"], "list"]);
    eq(await r.zRange(z, 0, -1), ["a", "b", "c"]);
    eq(await r.eval("return redis.call('ZPOPMIN', KEYS[1])", { keys: [z] }), ["a", "1.5"]);
    eq(await r.eval("return redis.call('LPOP', KEYS[1], 2)", { keys: [l] }), ["x", "y"]);
    eq([...(await r.eval("return redis.call('SMEMBERS', KEYS[1])", { keys: [s] }))].sort(), ["m", "n"]);
    eq(await r.eval("return redis.call('ZINCRBY', KEYS[1], 0.5, 'b')", { keys: [z] }), "2.5");
    eq(await r.eval("return redis.call('EXISTS', KEYS[1])", { keys: [P + "none"] }), 0);
    await raises(() => r.eval("return redis.call('LPUSH', KEYS[1], 'x')", { keys: [z] }), "WRONGTYPE");
    eq(await r.eval("return redis.pcall('LPUSH', KEYS[1], 'x')['err'] ~= nil", { keys: [z] }), 1);
    const sha = await r.scriptLoad("return redis.call('ZRANGEBYSCORE', KEYS[1], ARGV[1], ARGV[2], 'WITHSCORES')");
    eq(await r.evalSha(sha, { keys: [z], arguments: ["-inf", "+inf"] }), ["b", "2.5", "c", "3"]);
    await r.del([z, l, s]);
  },
  async config_info() {
    const mm = await r.configGet("maxmemory");
    ok("maxmemory" in mm, `CONFIG GET maxmemory: ${JSON.stringify(mm)}`);
    ok(Number.isInteger(Number(mm.maxmemory)), `maxmemory ${mm.maxmemory}`);
    const pol = await r.configGet("maxmemory-policy");
    ok(typeof pol["maxmemory-policy"] === "string" && pol["maxmemory-policy"].length > 0, `policy ${JSON.stringify(pol)}`);
    const both = await r.configGet("maxmemory*");
    ok("maxmemory" in both && "maxmemory-policy" in both, `maxmemory*: ${JSON.stringify(both)}`);
    const info = await r.info();
    ok(typeof info === "string" && info.length > 0, "empty INFO");
    ok(info.split("\r\n").some((line) => /^\w+:/.test(line)), "INFO has no key:value line");
    eq(typeof (await r.info("memory")), "string");
    eq(typeof (await r.info("keyspace")), "string");
  },
  async flushall_dbsize() {
    await r.set(P + "last", "1");
    ok((await r.dbSize()) >= 1, "dbsize is 0 after a SET");
    eq(await r.flushAll(), "OK");
    eq(await r.dbSize(), 0);
    eq(await r.get(P + "last"), null);
  },
};

if (connectError) {
  console.log(`connect failed: ${String(connectError.message).slice(0, 300)}`);
  for (const name of Object.keys(checks)) {
    checks[name] = async () => {
      throw connectError;
    };
  }
}
await run(`node node-redis ${version("redis")}`, checks);
