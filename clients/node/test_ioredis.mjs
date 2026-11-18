// Hotstash spike against ioredis. Mirrors clients/python/test_client.py check for check.
import { randomBytes } from "node:crypto";
import Redis from "ioredis";
import { PORT, PROTO, eq, ok, raises, run, version } from "./harness.mjs";

const P = "io:";
// ioredis speaks RESP2 only; HOTSTASH_PROTO is reported but cannot change the wire protocol.
const opts = { port: PORT, host: "127.0.0.1", connectTimeout: 30_000, maxRetriesPerRequest: 1 };
const r = new Redis(opts);
r.on("error", () => {}); // errors surface through the failing command

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
    ok((await r.getBuffer(k)).equals(v), "binary value differs");
    await r.set(P + "empty", Buffer.alloc(0));
    eq((await r.getBuffer(P + "empty")).length, 0);
    await r.set(P + "uni", "xin chào 🌏");
    eq(await r.get(P + "uni"), "xin chào 🌏");
  },
  async big_value_8mib() {
    const v = randomBytes(8 << 20);
    await r.set(P + "big", v);
    ok((await r.getBuffer(P + "big")).equals(v), "8 MiB value differs");
    await r.del(P + "big");
  },
  async counters() {
    await r.del(P + "n");
    eq(await r.incr(P + "n"), 1);
    eq(await r.incrby(P + "n", 41), 42);
    eq(await r.decr(P + "n"), 41);
    eq(await r.decrby(P + "n", 50), -9);
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
    eq(await r.hset(P + "h", { a: "1", b: "2", c: "3" }), 3);
    eq(await r.hset(P + "h", "a", "9"), 0);
    eq(await r.hget(P + "h", "a"), "9");
    eq(await r.hget(P + "h", "zz"), null);
    eq(await r.hgetall(P + "h"), { a: "9", b: "2", c: "3" });
    eq(await r.hlen(P + "h"), 3);
    eq(await r.hdel(P + "h", "a", "zz"), 1);
    eq(await r.hgetall(P + "nohash"), {});
  },
  async wrong_type() {
    await r.set(P + "str", "x");
    await raises(() => r.hget(P + "str", "f"), "WRONGTYPE");
    await r.del(P + "h2");
    await r.hset(P + "h2", "f", "v");
    await raises(() => r.get(P + "h2"), "WRONGTYPE");
    await raises(() => r.incr(P + "h2"), "WRONGTYPE");
  },
  async mset_mget() {
    eq(await r.mset({ [P + "m1"]: "1", [P + "m2"]: "2" }), "OK");
    eq(await r.mget(P + "m1", P + "nope", P + "m2"), ["1", null, "2"]);
  },
  async del_exists_unlink() {
    await r.mset({ [P + "d1"]: "1", [P + "d2"]: "2", [P + "d3"]: "3" });
    eq(await r.exists(P + "d1", P + "d2", P + "nope", P + "d1"), 3);
    eq(await r.del(P + "d1", P + "nope"), 1);
    eq(await r.unlink(P + "d2", P + "d3"), 2);
    eq(await r.exists(P + "d1", P + "d2", P + "d3"), 0);
  },
  async keys_pattern() {
    await r.mset({ [P + "k:1"]: "a", [P + "k:2"]: "b", [P + "k:x["]: "c" });
    eq((await r.keys(P + "k:?")).sort(), [P + "k:1", P + "k:2"]);
    eq(await r.keys(P + "k:x\\["), [P + "k:x["]);
    eq(await r.keys(P + "nothing*"), []);
  },
  async eval_values() {
    eq(await r.eval("return {KEYS[1], ARGV[1], 7, {1, 2}}", 1, P + "k", "a"), [P + "k", "a", 7, [1, 2]]);
    eq(await r.eval("return false", 0), null);
    eq(await r.eval("return redis.status_reply('FINE')", 0), "FINE");
    eq(await r.eval("return 3.99", 0), 3);
    const bin = Buffer.from([0, 255]);
    ok((await r.evalBuffer("return ARGV[1]", 0, bin)).equals(bin), "binary ARGV differs");
  },
  async eval_calls_redis() {
    const script = "redis.call('SET', KEYS[1], ARGV[1]); return redis.call('GET', KEYS[1])";
    eq(await r.eval(script, 1, P + "ev", "from-lua"), "from-lua");
    eq(await r.get(P + "ev"), "from-lua");
    await r.del(P + "evn");
    eq(await r.eval("return redis.call('INCRBY', KEYS[1], 5)", 1, P + "evn"), 5);
  },
  async eval_errors() {
    await raises(() => r.eval("return redis.call('NOSUCH')", 0), "unknown command");
    await raises(() => r.eval("return redis.error_reply('boom')", 0), "boom");
    await raises(() => r.eval("this is not lua", 0), "ERR");
    await raises(() => r.eval("error('thrown')", 0), "thrown");
    eq(await r.ping(), "PONG");
  },
  async script_helper_evalsha() {
    r.defineCommand("echoarg", { numberOfKeys: 0, lua: "return ARGV[1]" });
    eq(await r.echoarg("x"), "x");
    eq(await r.echoarg("y"), "y");
  },
  async pipeline_plain_10000() {
    await r.del(P + "pn");
    const p = r.pipeline();
    for (let i = 0; i < 10000; i++) p.incr(P + "pn");
    const out = await p.exec();
    eq(out.map(([err, v]) => (err ? String(err) : v)), range(10000).map((i) => i + 1));
  },
  async pipeline_with_error_in_middle() {
    await r.set(P + "pe", "abc");
    const out = await r.pipeline().set(P + "pa", "1").incr(P + "pe").get(P + "pa").exec();
    eq(out[0], [null, "OK"]);
    ok(out[1][0] instanceof Error, `expected an error, got ${out[1]}`);
    eq(out[2], [null, "1"]);
  },
  async x_pipeline_transaction_multi_exec() {
    const out = await r.multi().set(P + "tx", "1").get(P + "tx").exec();
    eq(out, [[null, "OK"], [null, "1"]]);
  },
  async x_set_with_expiry_and_nx() {
    eq(await r.set(P + "ex", "v", "EX", 60), "OK");
    const ttl = await r.ttl(P + "ex");
    ok(ttl > 0 && ttl <= 60, `ttl ${ttl}`);
    eq(await r.set(P + "ex", "other", "NX"), null);
  },
  async concurrency_16_threads() {
    await r.del(P + "c");
    await Promise.all(
      range(16).map(async () => {
        const c = new Redis(opts);
        c.on("error", () => {});
        try {
          for (let i = 0; i < 500; i++) await c.incr(P + "c");
        } finally {
          c.disconnect();
        }
      }),
    );
    eq(await r.get(P + "c"), "8000");
  },
  async unknown_command_keeps_connection() {
    await raises(() => r.call("NOSUCHCMD", "a"), "unknown command");
    await raises(() => r.call("GET"), "wrong number of arguments");
    eq(await r.ping(), "PONG");
  },
  async hgetall_100k_fields() {
    await r.del(P + "bigh");
    const p = r.pipeline();
    for (let i = 0; i < 100000; i += 1000) {
      p.hset(P + "bigh", Object.fromEntries(range(1000).map((j) => [`f${i + j}`, `v${i + j}`])));
    }
    const out = await p.exec();
    eq(out.reduce((sum, [, n]) => sum + n, 0), 100000);
    const got = await r.hgetall(P + "bigh");
    eq(Object.keys(got).length, 100000);
    eq(got.f99999, "v99999");
    eq(await r.del(P + "bigh"), 1);
  },
  async client_side_commands() {
    eq(await r.client("SETNAME", "io-test"), "OK");
    eq(typeof (await r.info()), "string");
    eq(await r.select(0), "OK");
    await raises(() => r.call("SELECT", "16"), "out of range");
  },
  async lists() {
    const k = P + "l", k2 = P + "l2";
    await r.del(k, k2);
    eq(await r.rpush(k, "a", "b", "c"), 3);
    eq(await r.lpush(k, "z", "y"), 5); // y z a b c
    eq(await r.llen(k), 5);
    eq(await r.lrange(k, 0, -1), ["y", "z", "a", "b", "c"]);
    eq(await r.lrange(k, -2, -1), ["b", "c"]);
    eq(await r.lrange(k, 10, 20), []);
    eq(await r.lindex(k, 0), "y");
    eq(await r.lindex(k, -1), "c");
    eq(await r.lindex(k, 99), null);
    eq(await r.lset(k, 1, "Z"), "OK");
    await raises(() => r.lset(k, 99, "x"), "out of range");
    eq(await r.lpop(k), "y");
    eq(await r.rpop(k), "c");
    eq(await r.lpop(k, 2), ["Z", "a"]);
    eq(await r.rpop(k, 5), ["b"]);
    eq(await r.lpop(k), null);
    eq(await r.lpop(k, 2), null);
    eq(await r.exists(k), 0);
    await r.rpush(k, "a", "b", "a", "c", "a");
    eq(await r.lrem(k, 2, "a"), 2);
    eq(await r.lrange(k, 0, -1), ["b", "c", "a"]);
    eq(await r.lrem(k, -1, "a"), 1);
    eq(await r.lrem(k, 0, "nope"), 0);
    await r.rpush(k, "d", "e", "f"); // b c d e f
    eq(await r.ltrim(k, 1, -2), "OK");
    eq(await r.lrange(k, 0, -1), ["c", "d", "e"]);
    eq(await r.linsert(k, "BEFORE", "d", "cd"), 4);
    eq(await r.linsert(k, "AFTER", "e", "ef"), 5);
    eq(await r.linsert(k, "BEFORE", "missing", "x"), -1);
    eq(await r.linsert(P + "nolist", "BEFORE", "a", "x"), 0);
    eq(await r.lrange(k, 0, -1), ["c", "cd", "d", "e", "ef"]);
    await r.rpush(k, "d");
    eq(await r.lpos(k, "d"), 2);
    eq(await r.lpos(k, "d", "RANK", 2), 5);
    eq(await r.lpos(k, "d", "COUNT", 0), [2, 5]);
    eq(await r.lpos(k, "nope"), null);
    eq(await r.lmove(k, k2, "LEFT", "RIGHT"), "c");
    eq(await r.lmove(k, k2, "RIGHT", "LEFT"), "d");
    eq(await r.lrange(k2, 0, -1), ["d", "c"]);
    eq(await r.rpoplpush(k, k2), "ef");
    eq(await r.lrange(k2, 0, -1), ["ef", "d", "c"]);
    eq(await r.lmove(P + "nolist", k2, "LEFT", "LEFT"), null);
    eq(await r.type(k), "list");
    await raises(() => r.get(k), "WRONGTYPE");
    await r.set(P + "lstr", "x");
    await raises(() => r.lpush(P + "lstr", "x"), "WRONGTYPE");
    eq(await r.del(k, k2, P + "lstr"), 3);
  },
  async sets() {
    const k = ["a", "b", "c"].map((s) => P + "set" + s);
    const d = P + "setd";
    await r.del(...k, d);
    eq(await r.sadd(k[0], "1", "2", "3", "4", "4"), 4);
    eq(await r.sadd(k[0], "1"), 0);
    eq(await r.scard(k[0]), 4);
    eq(await r.sismember(k[0], "2"), 1);
    eq(await r.sismember(k[0], "9"), 0);
    eq(await r.smismember(k[0], "1", "9", "4"), [1, 0, 1]);
    eq((await r.smembers(k[0])).sort(), ["1", "2", "3", "4"]);
    eq(await r.srem(k[0], "4", "9"), 1);
    await r.sadd(k[1], "2", "3", "5");
    await r.sadd(k[2], "3", "6");
    eq((await r.sinter(k[0], k[1])).sort(), ["2", "3"]);
    eq(await r.sinter(k[0], k[1], k[2]), ["3"]);
    eq((await r.sunion(k[0], k[1], k[2])).sort(), ["1", "2", "3", "5", "6"]);
    eq(await r.sdiff(k[0], k[1]), ["1"]);
    eq(await r.sdiff(k[0], k[1], k[2]), ["1"]);
    eq(await r.sinterstore(d, k[0], k[1]), 2);
    eq((await r.smembers(d)).sort(), ["2", "3"]);
    eq(await r.sunionstore(d, k[0], k[1]), 4);
    eq(await r.sdiffstore(d, k[1], k[0]), 1);
    eq(await r.smembers(d), ["5"]);
    eq(await r.sintercard(2, k[0], k[1]), 2);
    eq(await r.sintercard(2, k[0], k[1], "LIMIT", 1), 1);
    eq(await r.smove(k[0], k[2], "1"), 1);
    eq(await r.smove(k[0], k[2], "nope"), 0);
    eq((await r.smembers(k[2])).sort(), ["1", "3", "6"]);
    ok(["1", "3", "6"].includes(await r.srandmember(k[2])), "srandmember outside set");
    eq(new Set(await r.srandmember(k[2], 2)).size, 2);
    eq((await r.srandmember(k[2], 10)).length, 3);
    eq((await r.srandmember(k[2], -5)).length, 5);
    eq(await r.srandmember(P + "noset"), null);
    ok(["1", "3", "6"].includes(await r.spop(k[2])), "spop outside set");
    eq((await r.spop(k[2], 5)).length, 2);
    eq(await r.exists(k[2]), 0);
    eq(await r.spop(k[2]), null);
    const big = P + "setbig";
    await r.del(big);
    await r.sadd(big, ...range(300).map((i) => `m${i}`));
    const seen = new Set();
    let cur = "0";
    for (let i = 0; i < 1000; i++) {
      const [next, items] = await r.sscan(big, cur, "COUNT", 50);
      items.forEach((x) => seen.add(x));
      cur = next;
      if (cur === "0") break;
    }
    eq(seen.size, 300);
    eq(await r.type(big), "set");
    await raises(() => r.get(big), "WRONGTYPE");
    await r.del(big, ...k, d);
  },
  async zsets_basic() {
    // membership and order only; score replies are checked in zset_scores
    const k = P + "z";
    await r.del(k);
    eq(await r.zadd(k, 1, "a", 2.5, "b", 1.5, "c"), 3); // a c b
    eq(await r.zadd(k, 1, "a"), 0);
    eq(await r.zadd(k, "CH", 5, "a"), 1); // c b a
    eq(await r.zadd(k, "CH", 7, "d", 5, "a"), 1); // c b a d
    eq(await r.zadd(k, "NX", 9, "a"), 0);
    eq(await r.zrange(k, 0, -1), ["c", "b", "a", "d"]);
    eq(await r.zadd(k, "XX", 1, "zz"), 0);
    eq(await r.zcard(k), 4);
    eq(await r.zadd(k, "GT", "CH", 3, "a"), 0);
    eq(await r.zadd(k, "GT", "CH", 8, "a"), 1); // c b d a
    eq(await r.zrange(k, 0, -1), ["c", "b", "d", "a"]);
    eq(await r.zadd(k, "LT", "CH", 10, "a"), 0);
    eq(await r.zadd(k, "LT", "CH", 6, "a"), 1); // c b a d
    eq(await r.zrange(k, 0, -1), ["c", "b", "a", "d"]);
    eq(await r.zadd(k, "NX", "INCR", 1, "a"), null);
    eq(await r.zadd(k, "XX", "INCR", 1, "nope"), null);
    eq(await r.zrank(k, "c"), 0);
    eq(await r.zrank(k, "d"), 3);
    eq(await r.zrevrank(k, "d"), 0);
    eq(await r.zrevrank(k, "c"), 3);
    eq(await r.zrank(k, "nope"), null);
    eq(await r.zrevrank(k, "nope"), null);
    eq(await r.zcount(k, 2, 6), 2);
    eq(await r.zcount(k, "(2.5", 7), 2);
    eq(await r.zcount(k, "(1.5", "(7"), 2);
    eq(await r.zcount(k, "-inf", "+inf"), 4);
    eq(await r.zcount(k, 100, 200), 0);
    eq(await r.zrem(k, "a", "nope"), 1);
    eq(await r.zrange(k, 0, -1), ["c", "b", "d"]);
    eq(await r.type(k), "zset");
    await raises(() => r.get(k), "WRONGTYPE");
    await raises(() => r.zadd(k, "notanumber", "a"), "not a valid float");
    await raises(() => r.zadd(k, "NX", "XX", 1, "a"), "not compatible");
    eq(await r.del(k), 1);
  },
  async zset_scores() {
    const k = P + "zsc";
    await r.del(k);
    await r.zadd(k, 1, "a", 2.5, "b", 1.5, "c");
    eq(await r.zscore(k, "a"), "1");
    eq(await r.zscore(k, "c"), "1.5");
    eq(await r.zscore(k, "nope"), null);
    eq(await r.zmscore(k, "a", "nope", "c"), ["1", null, "1.5"]);
    eq(await r.zadd(k, "INCR", 4.25, "a"), "5.25");
    eq(await r.zadd(k, "XX", "INCR", -0.25, "a"), "5");
    eq(await r.zincrby(k, 0.5, "c"), "2");
    eq(await r.zincrby(k, 1, "new"), "1"); // new c b a
    eq(await r.zrank(k, "c", "WITHSCORE"), [1, "2"]);
    eq(await r.zrevrank(k, "c", "WITHSCORE"), [2, "2"]);
    eq(await r.zrank(k, "nope", "WITHSCORE"), null);
    eq(await r.zrange(k, 0, -1, "WITHSCORES"), ["new", "1", "c", "2", "b", "2.5", "a", "5"]);
    eq(await r.zrange(k, 0, 1, "REV", "WITHSCORES"), ["a", "5", "b", "2.5"]);
    eq(await r.zrangebyscore(k, 2, 3, "WITHSCORES"), ["c", "2", "b", "2.5"]);
    eq(await r.zrevrange(k, 0, 0, "WITHSCORES"), ["a", "5"]);
    eq(await r.zrevrangebyscore(k, 5, 2.5, "WITHSCORES"), ["a", "5", "b", "2.5"]);
    eq(await r.zrange(k, 2, 5, "BYSCORE", "LIMIT", 1, 1, "WITHSCORES"), ["b", "2.5"]);
    await r.del(k);
    await r.zadd(k, "-inf", "lo", "+inf", "hi", 1.5, "mid", -0.1, "frac");
    eq(await r.zrange(k, 0, -1), ["lo", "frac", "mid", "hi"]);
    eq(await r.zrange(k, 0, -1, "WITHSCORES"), ["lo", "-inf", "frac", "-0.1", "mid", "1.5", "hi", "inf"]);
    eq(await r.zscore(k, "hi"), "inf");
    eq(await r.zscore(k, "lo"), "-inf");
    eq(await r.zmscore(k, "lo", "hi"), ["-inf", "inf"]);
    eq(await r.zrangebyscore(k, "-inf", "+inf"), ["lo", "frac", "mid", "hi"]);
    eq(await r.zrangebyscore(k, "(-inf", "(inf"), ["frac", "mid"]);
    eq(await r.zcount(k, "-inf", 0), 2);
    eq(await r.zincrby(k, 5, "hi"), "inf");
    const [, flat] = await r.zscan(k, 0);
    eq(Object.fromEntries(range(flat.length / 2).map((i) => [flat[2 * i], flat[2 * i + 1]])),
      { lo: "-inf", frac: "-0.1", mid: "1.5", hi: "inf" });
    await r.del(k);
  },
  async zset_pop() {
    const k = P + "zp";
    await r.del(k);
    await r.zadd(k, 1, "a", 2, "b", 3, "c", 4.5, "d", 5, "e");
    eq(await r.zpopmin(k), ["a", "1"]);
    eq(await r.zpopmax(k), ["e", "5"]);
    eq(await r.zpopmin(k, 2), ["b", "2", "c", "3"]);
    eq(await r.zpopmax(k, 5), ["d", "4.5"]);
    eq(await r.zpopmin(k), []);
    eq(await r.zpopmax(k, 3), []);
    eq(await r.exists(k), 0);
  },
  async zsets_ranges() {
    const k = P + "zr";
    await r.del(k);
    await r.zadd(k, 1, "a", 2, "b", 3, "c", 4, "d", 5, "e");
    eq(await r.zrange(k, 0, 1), ["a", "b"]);
    eq(await r.zrange(k, -2, -1), ["d", "e"]);
    eq(await r.zrange(k, 0, 1, "REV"), ["e", "d"]);
    eq(await r.zrange(k, 2, 3, "BYSCORE"), ["b", "c"]);
    eq(await r.zrange(k, "(1", "+inf", "BYSCORE", "LIMIT", 1, 2), ["c", "d"]);
    eq(await r.zrange(k, "+inf", "(2", "BYSCORE", "REV", "LIMIT", 0, 2), ["e", "d"]);
    eq(await r.zrangebyscore(k, "-inf", 3), ["a", "b", "c"]);
    eq(await r.zrangebyscore(k, "(1", "(4"), ["b", "c"]);
    eq(await r.zrangebyscore(k, 1, 5, "LIMIT", 1, 2), ["b", "c"]);
    eq(await r.zrevrangebyscore(k, 5, 4), ["e", "d"]);
    eq(await r.zrevrangebyscore(k, "+inf", "-inf", "LIMIT", 0, 1), ["e"]);
    eq(await r.zrevrange(k, 0, 1), ["e", "d"]);
    eq(await r.zrange(k, 10, 20), []);
    eq(await r.zrange(P + "nozset", 0, -1), []);
    eq(await r.zremrangebyrank(k, 0, 1), 2); // c d e
    eq(await r.zremrangebyscore(k, "(3", 4), 1); // c e
    eq(await r.zrange(k, 0, -1), ["c", "e"]);
    eq(await r.zremrangebyscore(k, "-inf", "+inf"), 2);
    eq(await r.exists(k), 0);
    const big = P + "zbig";
    await r.del(big);
    await r.zadd(big, ...range(300).flatMap((i) => [i, `m${i}`]));
    const seen = new Map();
    let cur = "0";
    for (let i = 0; i < 1000; i++) {
      const [next, items] = await r.zscan(big, cur, "COUNT", 50);
      for (let j = 0; j < items.length; j += 2) seen.set(items[j], items[j + 1]);
      cur = next;
      if (cur === "0") break;
    }
    eq(seen.size, 300);
    eq(seen.get("m299"), "299");
    await r.del(big);
  },
  async zsets_store() {
    const [a, c, d] = [P + "zsa", P + "zsb", P + "zsd"];
    const scores = async (key) => {
      const [, flat] = await r.zscan(key, 0, "COUNT", 1000);
      return Object.fromEntries(range(flat.length / 2).map((i) => [flat[2 * i], flat[2 * i + 1]]));
    };
    await r.del(a, c, d);
    await r.zadd(a, 1, "x", 2, "y", 3, "z");
    await r.zadd(c, 10, "y", 20, "z", 0.5, "w");
    eq(await r.zunionstore(d, 2, a, c), 4);
    eq(await r.zrange(d, 0, -1), ["w", "x", "y", "z"]);
    eq(await r.zunionstore(d, 2, a, c, "WEIGHTS", 2, 0.5), 4);
    eq(await scores(d), { w: "0.25", x: "2", y: "9", z: "16" });
    eq(await r.zunionstore(d, 2, a, c, "AGGREGATE", "MAX"), 4);
    eq((await scores(d)).z, "20");
    eq(await r.zinterstore(d, 2, a, c), 2);
    eq(await scores(d), { y: "12", z: "23" });
    eq(await r.zinterstore(d, 2, a, c, "WEIGHTS", 3, 2), 2);
    eq(await scores(d), { y: "26", z: "49" });
    eq(await r.zinterstore(d, 2, a, c, "AGGREGATE", "MIN"), 2);
    eq((await scores(d)).y, "2");
    await r.sadd(P + "zss", "y", "q");
    eq(await r.zinterstore(d, 2, a, P + "zss"), 1); // sets count as score 1
    eq((await scores(d)).y, "3");
    eq(await r.zinterstore(d, 2, a, P + "nozset"), 0);
    eq(await r.exists(d), 0);
    await r.del(a, c, d, P + "zss");
  },
  async string_key_extras() {
    const h = P + "hx";
    await r.del(P + "f", P + "gr", P + "grnew", P + "abc", h);
    eq(await r.incrbyfloat(P + "f", 1.5), "1.5");
    eq(await r.incrbyfloat(P + "f", 2), "3.5");
    eq(await r.incrbyfloat(P + "f", -0.25), "3.25");
    eq(await r.get(P + "f"), "3.25");
    await r.set(P + "abc", "abc");
    await raises(() => r.incrbyfloat(P + "abc", 1), "not a valid float");
    await r.set(P + "gr", "Hello World");
    eq(await r.getrange(P + "gr", 0, 4), "Hello");
    eq(await r.getrange(P + "gr", -5, -1), "World");
    eq(await r.getrange(P + "gr", 20, 30), "");
    eq(await r.setrange(P + "gr", 6, "Redis"), 11);
    eq(await r.get(P + "gr"), "Hello Redis");
    eq(await r.setrange(P + "gr", 13, "!"), 14);
    eq(await r.get(P + "gr"), "Hello Redis\u0000\u0000!");
    eq(await r.setrange(P + "grnew", 2, "x"), 3);
    eq(await r.get(P + "grnew"), "\u0000\u0000x");
    await r.hset(h, { a: "1", b: "hello", c: "2.5" });
    eq((await r.hkeys(h)).sort(), ["a", "b", "c"]);
    eq((await r.hvals(h)).sort(), ["1", "2.5", "hello"]);
    eq(await r.hstrlen(h, "b"), 5);
    eq(await r.hstrlen(h, "nope"), 0);
    eq(await r.hincrbyfloat(h, "c", 0.5), "3");
    eq(await r.hincrbyfloat(h, "new", 1.5), "1.5");
    eq(await r.hincrby(h, "a", 4), 5);
    await raises(() => r.hincrbyfloat(h, "b", 1), "not a float");
    ok(["a", "b", "c", "new"].includes(await r.hrandfield(h)), "hrandfield outside hash");
    eq((await r.hrandfield(h, 2)).length, 2);
    eq((await r.hrandfield(h, -6)).length, 6);
    eq(await r.hrandfield(P + "nohash"), null);
    eq(await r.hrandfield(P + "nohash", 3), []);
    eq(await r.hexists(h, "a"), 1);
    eq(await r.hmget(h, "a", "zz"), ["5", null]);
    eq(await r.hsetnx(h, "a", "x"), 0);
    await r.del(P + "f", P + "gr", P + "grnew", P + "abc", h);
  },
  async hrandfield_values() {
    const h = P + "hrf";
    await r.del(h);
    await r.hset(h, { a: "1", b: "hello", c: "3" });
    const got = await r.hrandfield(h, 3, "WITHVALUES");
    eq(got.length, 6); // ioredis is RESP2-only: flat [f, v, f, v, ...]
    eq(Object.fromEntries(range(3).map((i) => [got[2 * i], got[2 * i + 1]])), { a: "1", b: "hello", c: "3" });
    eq((await r.hrandfield(h, -5, "WITHVALUES")).length, 10);
    await r.del(h);
  },
  async keys_misc() {
    const names = ["r1", "r2", "r3", "cp1", "cp2", "ms1", "ms2", "ms3", "ty:s", "ty:h", "ty:l", "ty:set", "ty:z", "ty:int"];
    await r.del(...names.map((n) => P + n));
    await r.set(P + "r1", "v");
    eq(await r.rename(P + "r1", P + "r2"), "OK");
    eq(await r.exists(P + "r1", P + "r2"), 1);
    await raises(() => r.rename(P + "nokey", P + "x"), "no such key");
    await r.set(P + "r1", "other");
    eq(await r.renamenx(P + "r1", P + "r2"), 0);
    eq(await r.get(P + "r2"), "v");
    eq(await r.renamenx(P + "r1", P + "r3"), 1);
    eq(await r.get(P + "r3"), "other");
    await r.rpush(P + "cp1", "a", "b");
    eq(await r.copy(P + "cp1", P + "cp2"), 1);
    eq(await r.copy(P + "cp1", P + "cp2"), 0);
    eq(await r.copy(P + "cp1", P + "cp2", "REPLACE"), 1);
    eq(await r.lrange(P + "cp2", 0, -1), ["a", "b"]);
    await r.rpush(P + "cp1", "c");
    eq(await r.llen(P + "cp2"), 2); // a copy, not an alias
    eq(await r.copy(P + "nokey", P + "cp3"), 0);
    eq(await r.msetnx(P + "ms1", "1", P + "ms2", "2"), 1);
    eq(await r.msetnx(P + "ms1", "x", P + "ms3", "3"), 0);
    eq(await r.exists(P + "ms3"), 0);
    eq(await r.get(P + "ms1"), "1");
    eq(await r.touch(P + "r2", P + "cp1", P + "nokey"), 2);
    await r.set(P + "ty:s", "x");
    await r.hset(P + "ty:h", "f", "v");
    await r.rpush(P + "ty:l", "x");
    await r.sadd(P + "ty:set", "x");
    await r.zadd(P + "ty:z", 1, "x");
    eq(await Promise.all(["s", "h", "l", "set", "z"].map((t) => r.type(P + "ty:" + t))),
      ["string", "hash", "list", "set", "zset"]);
    eq(await r.type(P + "nokey"), "none");
    for (const n of ["ty:s", "ty:h", "ty:l", "ty:set", "ty:z"]) {
      const enc = await r.object("ENCODING", P + n);
      ok(typeof enc === "string" && enc.length > 0, `encoding of ${n}: ${enc}`);
    }
    eq(await r.object("ENCODING", P + "nokey"), null);
    ok(["embstr", "raw"].includes(await r.object("ENCODING", P + "ty:s")), "string encoding");
    await r.set(P + "ty:int", "12345");
    eq(await r.object("ENCODING", P + "ty:int"), "int");
    const scanType = async (type) => {
      const got = new Set();
      let cur = "0";
      for (let i = 0; i < 10000; i++) {
        const [next, keys] = await r.scan(cur, "MATCH", P + "ty:*", "COUNT", 100, "TYPE", type);
        keys.forEach((x) => got.add(x));
        cur = next;
        if (cur === "0") break;
      }
      return [...got].sort();
    };
    eq(await scanType("string"), [P + "ty:int", P + "ty:s"]);
    eq(await scanType("hash"), [P + "ty:h"]);
    eq(await scanType("list"), [P + "ty:l"]);
    eq(await scanType("set"), [P + "ty:set"]);
    eq(await scanType("zset"), [P + "ty:z"]);
    const rk = await r.randomkey();
    ok(rk !== null && (await r.exists(rk)) === 1, `randomkey ${rk}`);
    await r.del(...names.map((n) => P + n));
  },
  async pubsub_roundtrip() {
    const sub = new Redis(opts);
    sub.on("error", () => {});
    const q = [];
    sub.on("message", (channel, message) => q.push({ type: "message", channel, message }));
    sub.on("pmessage", (pattern, channel, message) => q.push({ type: "pmessage", pattern, channel, message }));
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
      eq(await sub.subscribe(ch), 1);
      eq(await sub.psubscribe(pat), 2);
      eq(await r.publish(ch, "hello"), 1);
      eq(await next(), { type: "message", channel: ch, message: "hello" });
      eq(await r.publish(P + "pat:one", "p1"), 1);
      eq(await next(), { type: "pmessage", pattern: pat, channel: P + "pat:one", message: "p1" });
      eq(await r.publish(P + "other", "x"), 0);
      eq(await r.publish(ch2, "x"), 0);
      eq(await sub.subscribe(both), 3);
      eq(await sub.psubscribe(P + "pat:b*"), 4);
      eq(await r.publish(both, "twice"), 3);
      const got = [await next(), await next(), await next()];
      eq(got.map((m) => m.type).sort(), ["message", "pmessage", "pmessage"]);
      eq(got.every((m) => m.message === "twice"), true);
      eq(await r.pubsub("NUMSUB", ch, ch2), [ch, 1, ch2, 0]);
      eq(await r.pubsub("NUMPAT"), 2);
      eq((await r.pubsub("CHANNELS", P + "*")).sort(), [ch, both].sort());
      eq(await sub.punsubscribe(P + "pat:b*"), 3);
      eq(await sub.unsubscribe(ch), 2);
      eq(await r.publish(ch, "gone"), 0);
      eq(await sub.punsubscribe(pat), 1);
      eq(await r.publish(P + "pat:one", "gone"), 0);
      eq(await sub.unsubscribe(), 0);
      eq(await r.pubsub("NUMSUB", both), [both, 0]);
      eq(q.length, 0);
    } finally {
      sub.disconnect();
    }
  },
  async tx_new_types() {
    await r.del(P + "txl", P + "txz", P + "txs", P + "w", P + "txstr");
    let out = await r.multi().rpush(P + "txl", "a", "b").zadd(P + "txz", 1.5, "m", 2, "n").sadd(P + "txs", "x", "y")
      .lrange(P + "txl", 0, -1).zrange(P + "txz", 0, -1).scard(P + "txs").exec();
    eq(out, [[null, 2], [null, 2], [null, 2], [null, ["a", "b"]], [null, ["m", "n"]], [null, 2]]);
    out = await r.multi().lpop(P + "txl").zrem(P + "txz", "m").spop(P + "txs", 2).exec();
    eq(out[0], [null, "a"]);
    eq(out[1], [null, 1]);
    eq(out[2][1].sort(), ["x", "y"]);
    await r.set(P + "txstr", "s");
    // an error inside EXEC does not roll back the other commands
    out = await r.multi().rpush(P + "txl", "q").lpush(P + "txstr", "bad").llen(P + "txl").exec();
    eq(out[0], [null, 2]);
    ok(out[1][0] instanceof Error && /WRONGTYPE/.test(out[1][0].message), `expected WRONGTYPE, got ${out[1]}`);
    eq(out[2], [null, 2]);
    // WATCH aborts EXEC when the key changed
    await r.set(P + "w", "1");
    const c2 = new Redis(opts);
    c2.on("error", () => {});
    const w = new Redis(opts);
    w.on("error", () => {});
    try {
      eq(await w.watch(P + "w"), "OK");
      await c2.set(P + "w", "2");
      eq(await w.multi().zadd(P + "txz", 1, "w").exec(), null);
    } finally {
      c2.disconnect();
      w.disconnect();
    }
    eq(await r.zscore(P + "txz", "w"), null);
    await r.del(P + "txl", P + "txz", P + "txs", P + "w", P + "txstr");
  },
  async lua_new_types() {
    const [z, l, s] = [P + "lz", P + "ll", P + "ls"];
    await r.del(z, l, s);
    const script = `
redis.call('ZADD', KEYS[1], 1.5, 'a', 2, 'b', 3, 'c')
redis.call('RPUSH', KEYS[2], 'x', 'y', 'z')
redis.call('SADD', KEYS[3], 'm', 'n')
return {redis.call('ZRANGE', KEYS[1], 0, -1), redis.call('LRANGE', KEYS[2], 0, -1),
        redis.call('SCARD', KEYS[3]), redis.call('ZSCORE', KEYS[1], 'a'),
        redis.call('ZRANGE', KEYS[1], 0, 0, 'WITHSCORES'), redis.call('TYPE', KEYS[2])['ok']}`;
    eq(await r.eval(script, 3, z, l, s), [["a", "b", "c"], ["x", "y", "z"], 2, "1.5", ["a", "1.5"], "list"]);
    eq(await r.zrange(z, 0, -1), ["a", "b", "c"]);
    eq(await r.eval("return redis.call('ZPOPMIN', KEYS[1])", 1, z), ["a", "1.5"]);
    eq(await r.eval("return redis.call('LPOP', KEYS[1], 2)", 1, l), ["x", "y"]);
    eq((await r.eval("return redis.call('SMEMBERS', KEYS[1])", 1, s)).sort(), ["m", "n"]);
    eq(await r.eval("return redis.call('ZINCRBY', KEYS[1], 0.5, 'b')", 1, z), "2.5");
    eq(await r.eval("return redis.call('EXISTS', KEYS[1])", 1, P + "none"), 0);
    await raises(() => r.eval("return redis.call('LPUSH', KEYS[1], 'x')", 1, z), "WRONGTYPE");
    eq(await r.eval("return redis.pcall('LPUSH', KEYS[1], 'x')['err'] ~= nil", 1, z), 1);
    r.defineCommand("zbyscore", {
      numberOfKeys: 1,
      lua: "return redis.call('ZRANGEBYSCORE', KEYS[1], ARGV[1], ARGV[2], 'WITHSCORES')",
    });
    eq(await r.zbyscore(z, "-inf", "+inf"), ["b", "2.5", "c", "3"]);
    await r.del(z, l, s);
  },
  async config_info() {
    const mm = await r.config("GET", "maxmemory");
    eq(mm[0], "maxmemory");
    ok(Number.isInteger(Number(mm[1])), `maxmemory ${mm[1]}`);
    const pol = await r.config("GET", "maxmemory-policy");
    eq(pol[0], "maxmemory-policy");
    ok(typeof pol[1] === "string" && pol[1].length > 0, `policy ${pol[1]}`);
    const both = await r.config("GET", "maxmemory*");
    ok(both.includes("maxmemory") && both.includes("maxmemory-policy"), `maxmemory*: ${both}`);
    const info = await r.info();
    ok(typeof info === "string" && info.length > 0, "empty INFO");
    ok(info.split("\r\n").some((line) => /^\w+:/.test(line)), "INFO has no key:value line");
    eq(typeof (await r.info("memory")), "string");
    eq(typeof (await r.info("keyspace")), "string");
  },
  async flushall_dbsize() {
    await r.set(P + "last", "1");
    ok((await r.dbsize()) >= 1, "dbsize is 0 after a SET");
    eq(await r.flushall(), "OK");
    eq(await r.dbsize(), 0);
    eq(await r.get(P + "last"), null);
  },
};

if (PROTO === 3) console.log("note: ioredis has no RESP3 mode; running RESP2");
await run(`node ioredis ${version("ioredis")}`, checks);
