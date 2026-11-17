"""HCache spike against redis-py. Every check runs; nothing aborts the run.

Checks named x_* use Redis features outside the spike's scope; they show
what a real application would hit, they are not expected to pass.
"""
import os
import sys
import threading
import time

import redis

PORT = int(os.environ.get("HCACHE_PORT", "6400"))
# HCACHE_PROTO=2 forces RESP2; unset means the library's default handshake.
PROTO = os.environ.get("HCACHE_PROTO")
# HCACHE_TLS_CA=<ca file> connects with TLS and checks the server against it.
TLS_CA = os.environ.get("HCACHE_TLS_CA")
TLS = {"ssl": True, "ssl_ca_certs": TLS_CA} if TLS_CA else {}
KW = dict({"protocol": int(PROTO)} if PROTO else {}, **TLS)
P = "py:"
failed = []


def check(name, fn):
    try:
        fn()
        print(f"PASS {name}")
    except Exception as e:  # report everything, keep going
        failed.append(name)
        print(f"FAIL {name}: {type(e).__name__}: {str(e)[:300]}")


def eq(got, want):
    assert got == want, f"got {got!r:.200}, want {want!r:.200}"


def raises(fn, text):
    try:
        got = fn()
    except redis.ResponseError as e:
        assert text.lower() in str(e).lower(), f"error {e!s} lacks {text!r}"
        return
    raise AssertionError(f"no error, got {got!r:.100}")


r = redis.Redis(port=PORT, socket_timeout=30, **KW)


def connect_ping():
    eq(r.ping(), True)
    eq(r.echo("hi"), b"hi")


def set_get():
    eq(r.set(P + "s", "hello"), True)
    eq(r.get(P + "s"), b"hello")
    eq(r.get(P + "missing"), None)


def binary_key_and_value():
    v = bytes(range(256)) * 4
    k = P.encode() + b"\x00\r\n\xff bin"
    r.set(k, v)
    eq(r.get(k), v)
    r.set(P + "empty", b"")
    eq(r.get(P + "empty"), b"")
    r.set(P + "uni", "xin chào 🌏")
    eq(r.get(P + "uni").decode(), "xin chào 🌏")


def big_value_8mib():
    v = os.urandom(8 << 20)
    r.set(P + "big", v)
    eq(r.get(P + "big") == v, True)
    r.delete(P + "big")


def counters():
    r.delete(P + "n")
    eq(r.incr(P + "n"), 1)
    eq(r.incrby(P + "n", 41), 42)
    eq(r.decr(P + "n"), 41)
    eq(r.decrby(P + "n", 50), -9)
    eq(r.get(P + "n"), b"-9")


def counter_errors():
    r.set(P + "abc", "abc")
    raises(lambda: r.incr(P + "abc"), "not an integer")
    r.set(P + "max", str(2**63 - 1))
    raises(lambda: r.incr(P + "max"), "overflow")
    eq(r.get(P + "max"), str(2**63 - 1).encode())


def hashes():
    r.delete(P + "h")
    eq(r.hset(P + "h", mapping={"a": "1", "b": "2", "c": "3"}), 3)
    eq(r.hset(P + "h", "a", "9"), 0)
    eq(r.hget(P + "h", "a"), b"9")
    eq(r.hget(P + "h", "zz"), None)
    eq(r.hgetall(P + "h"), {b"a": b"9", b"b": b"2", b"c": b"3"})
    eq(r.hlen(P + "h"), 3)
    eq(r.hdel(P + "h", "a", "zz"), 1)
    eq(r.hgetall(P + "nohash"), {})


def wrong_type():
    r.set(P + "str", "x")
    raises(lambda: r.hget(P + "str", "f"), "WRONGTYPE")
    r.delete(P + "h2")
    r.hset(P + "h2", "f", "v")
    raises(lambda: r.get(P + "h2"), "WRONGTYPE")
    raises(lambda: r.incr(P + "h2"), "WRONGTYPE")


def mset_mget():
    eq(r.mset({P + "m1": "1", P + "m2": "2"}), True)
    eq(r.mget(P + "m1", P + "nope", P + "m2"), [b"1", None, b"2"])


def del_exists_unlink():
    r.mset({P + "d1": "1", P + "d2": "2", P + "d3": "3"})
    eq(r.exists(P + "d1", P + "d2", P + "nope", P + "d1"), 3)
    eq(r.delete(P + "d1", P + "nope"), 1)
    eq(r.unlink(P + "d2", P + "d3"), 2)
    eq(r.exists(P + "d1", P + "d2", P + "d3"), 0)


def keys_pattern():
    r.mset({P + "k:1": "a", P + "k:2": "b", P + "k:x[": "c"})
    eq(sorted(r.keys(P + "k:?")), [P.encode() + b"k:1", P.encode() + b"k:2"])
    eq(r.keys(P + "k:x\\["), [P.encode() + b"k:x["])
    eq(r.keys(P + "nothing*"), [])


def eval_values():
    eq(
        r.eval("return {KEYS[1], ARGV[1], 7, {1, 2}}", 1, P + "k", "a"),
        [P.encode() + b"k", b"a", 7, [1, 2]],
    )
    eq(r.eval("return false", 0), None)
    eq(r.eval("return redis.status_reply('FINE')", 0), b"FINE")
    eq(r.eval("return 3.99", 0), 3)
    eq(r.eval("return ARGV[1]", 0, b"\x00\xff"), b"\x00\xff")


def eval_calls_redis():
    script = "redis.call('SET', KEYS[1], ARGV[1]); return redis.call('GET', KEYS[1])"
    eq(r.eval(script, 1, P + "ev", "from-lua"), b"from-lua")
    eq(r.get(P + "ev"), b"from-lua")
    eq(r.eval("return redis.call('INCRBY', KEYS[1], 5)", 1, P + "evn"), 5)


def eval_errors():
    raises(lambda: r.eval("return redis.call('NOSUCH')", 0), "unknown command")
    raises(lambda: r.eval("return redis.error_reply('boom')", 0), "boom")
    raises(lambda: r.eval("this is not lua", 0), "ERR")
    raises(lambda: r.eval("error('thrown')", 0), "thrown")
    eq(r.ping(), True)


def script_helper_evalsha():
    s = r.register_script("return ARGV[1]")
    eq(s(args=["x"]), b"x")
    eq(s(args=["y"]), b"y")


def pipeline_plain_10000():
    r.delete(P + "pn")
    p = r.pipeline(transaction=False)
    for _ in range(10000):
        p.incr(P + "pn")
    out = p.execute()
    eq(out, list(range(1, 10001)))


def pipeline_with_error_in_middle():
    r.set(P + "pe", "abc")
    p = r.pipeline(transaction=False)
    p.set(P + "pa", "1").incr(P + "pe").get(P + "pa")
    out = p.execute(raise_on_error=False)
    eq(out[0], True)
    assert isinstance(out[1], redis.ResponseError), repr(out[1])
    eq(out[2], b"1")


def x_pipeline_transaction_multi_exec():
    p = r.pipeline()  # redis-py's default pipeline wraps in MULTI/EXEC
    p.set(P + "tx", "1").get(P + "tx")
    eq(p.execute(), [True, b"1"])


def x_set_with_expiry_and_nx():
    eq(r.set(P + "ex", "v", ex=60), True)
    assert 0 < r.ttl(P + "ex") <= 60
    eq(r.set(P + "ex", "other", nx=True), None)


def concurrency_16_threads():
    r.delete(P + "c")
    errors = []

    def work():
        try:
            c = redis.Redis(port=PORT, socket_timeout=30, **KW)
            for _ in range(500):
                c.incr(P + "c")
            c.close()
        except Exception as e:
            errors.append(e)

    ts = [threading.Thread(target=work) for _ in range(16)]
    [t.start() for t in ts]
    [t.join() for t in ts]
    eq(errors, [])
    eq(r.get(P + "c"), b"8000")


def unknown_command_keeps_connection():
    raises(lambda: r.execute_command("NOSUCHCMD", "a"), "unknown command")
    raises(lambda: r.execute_command("GET"), "wrong number of arguments")
    eq(r.ping(), True)


def hgetall_100k_fields():
    r.delete(P + "bigh")
    p = r.pipeline(transaction=False)
    for i in range(0, 100000, 1000):
        p.hset(P + "bigh", mapping={f"f{j}": f"v{j}" for j in range(i, i + 1000)})
    eq(sum(p.execute()), 100000)
    got = r.hgetall(P + "bigh")
    eq(len(got), 100000)
    eq(got[b"f99999"], b"v99999")
    eq(r.delete(P + "bigh"), 1)


def client_side_commands():
    eq(r.client_setname("py-test"), True)
    assert isinstance(r.info(), dict)
    eq(r.select(0), True)
    raises(lambda: r.execute_command("SELECT", "16"), "out of range")


def bl(*xs):
    return [x if isinstance(x, bytes) else str(x).encode() for x in xs]


def lists():
    k, k2 = P + "l", P + "l2"
    r.delete(k, k2)
    eq(r.rpush(k, "a", "b", "c"), 3)
    eq(r.lpush(k, "z", "y"), 5)  # y z a b c
    eq(r.llen(k), 5)
    eq(r.lrange(k, 0, -1), bl("y", "z", "a", "b", "c"))
    eq(r.lrange(k, -2, -1), bl("b", "c"))
    eq(r.lrange(k, 10, 20), [])
    eq(r.lindex(k, 0), b"y")
    eq(r.lindex(k, -1), b"c")
    eq(r.lindex(k, 99), None)
    eq(r.lset(k, 1, "Z"), True)
    raises(lambda: r.lset(k, 99, "x"), "out of range")
    eq(r.lpop(k), b"y")
    eq(r.rpop(k), b"c")
    eq(r.lpop(k, 2), bl("Z", "a"))
    eq(r.rpop(k, 5), bl("b"))
    eq(r.lpop(k), None)
    eq(r.lpop(k, 2), None)
    eq(r.exists(k), 0)
    r.rpush(k, "a", "b", "a", "c", "a")
    eq(r.lrem(k, 2, "a"), 2)
    eq(r.lrange(k, 0, -1), bl("b", "c", "a"))
    eq(r.lrem(k, -1, "a"), 1)
    eq(r.lrem(k, 0, "nope"), 0)
    r.rpush(k, "d", "e", "f")  # b c d e f
    eq(r.ltrim(k, 1, -2), True)
    eq(r.lrange(k, 0, -1), bl("c", "d", "e"))
    eq(r.linsert(k, "BEFORE", "d", "cd"), 4)
    eq(r.linsert(k, "AFTER", "e", "ef"), 5)
    eq(r.linsert(k, "BEFORE", "missing", "x"), -1)
    eq(r.linsert(P + "nolist", "BEFORE", "a", "x"), 0)
    eq(r.lrange(k, 0, -1), bl("c", "cd", "d", "e", "ef"))
    r.rpush(k, "d")
    eq(r.lpos(k, "d"), 2)
    eq(r.lpos(k, "d", rank=2), 5)
    eq(r.lpos(k, "d", count=0), [2, 5])
    eq(r.lpos(k, "nope"), None)
    eq(r.lmove(k, k2, "LEFT", "RIGHT"), b"c")
    eq(r.lmove(k, k2, "RIGHT", "LEFT"), b"d")
    eq(r.lrange(k2, 0, -1), bl("d", "c"))
    eq(r.rpoplpush(k, k2), b"ef")
    eq(r.lrange(k2, 0, -1), bl("ef", "d", "c"))
    eq(r.lmove(P + "nolist", k2, "LEFT", "LEFT"), None)
    eq(r.type(k), b"list")
    raises(lambda: r.get(k), "WRONGTYPE")
    r.set(P + "lstr", "x")
    raises(lambda: r.lpush(P + "lstr", "x"), "WRONGTYPE")
    eq(r.delete(k, k2, P + "lstr"), 3)


def sets():
    k = [P + "set" + s for s in "abc"]
    r.delete(*k, P + "setd")
    eq(r.sadd(k[0], "1", "2", "3", "4", "4"), 4)
    eq(r.sadd(k[0], "1"), 0)
    eq(r.scard(k[0]), 4)
    eq(r.sismember(k[0], "2"), True)
    eq(r.sismember(k[0], "9"), False)
    eq(r.smismember(k[0], ["1", "9", "4"]), [True, False, True])
    eq(r.smembers(k[0]), set(bl("1", "2", "3", "4")))
    eq(r.srem(k[0], "4", "9"), 1)
    r.sadd(k[1], "2", "3", "5")
    r.sadd(k[2], "3", "6")
    eq(r.sinter(k[0], k[1]), set(bl("2", "3")))
    eq(r.sinter(k[0], k[1], k[2]), set(bl("3")))
    eq(r.sunion(k[0], k[1], k[2]), set(bl("1", "2", "3", "5", "6")))
    eq(r.sdiff(k[0], k[1]), set(bl("1")))
    eq(r.sdiff(k[0], k[1], k[2]), set(bl("1")))
    eq(r.sinterstore(P + "setd", [k[0], k[1]]), 2)
    eq(r.smembers(P + "setd"), set(bl("2", "3")))
    eq(r.sunionstore(P + "setd", [k[0], k[1]]), 4)
    eq(r.sdiffstore(P + "setd", [k[1], k[0]]), 1)
    eq(r.smembers(P + "setd"), set(bl("5")))
    eq(r.sintercard(2, [k[0], k[1]]), 2)
    eq(r.sintercard(2, [k[0], k[1]], limit=1), 1)
    eq(r.smove(k[0], k[2], "1"), True)
    eq(r.smove(k[0], k[2], "nope"), False)
    eq(r.smembers(k[2]), set(bl("1", "3", "6")))
    assert r.srandmember(k[2]) in bl("1", "3", "6")
    eq(len(set(r.srandmember(k[2], 2))), 2)
    eq(len(r.srandmember(k[2], 10)), 3)
    eq(len(r.srandmember(k[2], -5)), 5)
    eq(r.srandmember(P + "noset"), None)
    p = r.spop(k[2])
    assert p in bl("1", "3", "6"), p
    eq(len(r.spop(k[2], 5)), 2)
    eq(r.exists(k[2]), 0)
    eq(r.spop(k[2]), None)
    big = P + "setbig"
    r.delete(big)
    r.sadd(big, *[f"m{i}" for i in range(300)])
    seen = set()
    cur = 0
    for _ in range(1000):
        cur, items = r.sscan(big, cur, count=50)
        seen.update(items)
        if cur == 0:
            break
    eq(len(seen), 300)
    eq(len(list(r.sscan_iter(big, match="m1*"))), 111)
    eq(r.type(big), b"set")
    raises(lambda: r.get(big), "WRONGTYPE")
    r.delete(big, *k, P + "setd")


def zsets_basic():
    # membership and order only; score replies are checked in zset_scores
    k = P + "z"
    r.delete(k)
    eq(r.zadd(k, {"a": 1, "b": 2.5, "c": 1.5}), 3)  # a c b
    eq(r.zadd(k, {"a": 1}), 0)
    eq(r.zadd(k, {"a": 5}, ch=True), 1)  # c b a
    eq(r.zadd(k, {"d": 7, "a": 5}, ch=True), 1)  # c b a d
    eq(r.zadd(k, {"a": 9}, nx=True), 0)
    eq(r.zrange(k, 0, -1), bl("c", "b", "a", "d"))
    eq(r.zadd(k, {"zz": 1}, xx=True), 0)
    eq(r.zcard(k), 4)
    eq(r.zadd(k, {"a": 3}, gt=True, ch=True), 0)
    eq(r.zadd(k, {"a": 8}, gt=True, ch=True), 1)  # c b d a
    eq(r.zrange(k, 0, -1), bl("c", "b", "d", "a"))
    eq(r.zadd(k, {"a": 10}, lt=True, ch=True), 0)
    eq(r.zadd(k, {"a": 6}, lt=True, ch=True), 1)  # c b a d
    eq(r.zrange(k, 0, -1), bl("c", "b", "a", "d"))
    eq(r.zadd(k, {"a": 1}, incr=True, nx=True), None)
    eq(r.zadd(k, {"nope": 1}, incr=True, xx=True), None)
    eq(r.zrank(k, "c"), 0)
    eq(r.zrank(k, "d"), 3)
    eq(r.zrevrank(k, "d"), 0)
    eq(r.zrevrank(k, "c"), 3)
    eq(r.zrank(k, "nope"), None)
    eq(r.zrevrank(k, "nope"), None)
    eq(r.zcount(k, 2, 6), 2)
    eq(r.zcount(k, "(2.5", 7), 2)
    eq(r.zcount(k, "(1.5", "(7"), 2)
    eq(r.zcount(k, "-inf", "+inf"), 4)
    eq(r.zcount(k, 100, 200), 0)
    eq(r.zrem(k, "a", "nope"), 1)
    eq(r.zrange(k, 0, -1), bl("c", "b", "d"))
    eq(r.type(k), b"zset")
    raises(lambda: r.get(k), "WRONGTYPE")
    raises(lambda: r.zadd(k, {"a": "notanumber"}), "not a valid float")
    raises(lambda: r.execute_command("ZADD", k, "NX", "XX", 1, "a"), "not compatible")
    eq(r.delete(k), 1)


def streams_and_blocking():
    import threading
    s, q = P + "stream", P + "queue"
    r.delete(s, q)
    first = r.xadd(s, {"n": "1"})
    second = r.xadd(s, {"n": "2", "bin": b"\x00\xff"})
    eq(r.xlen(s), 2)
    eq(r.type(s), b"stream")
    eq(r.xrange(s), [(first, {b"n": b"1"}), (second, {b"n": b"2", b"bin": b"\x00\xff"})])
    eq(r.xrevrange(s, count=1), [(second, {b"n": b"2", b"bin": b"\x00\xff"})])
    got = r.xread({s: first})
    rows = got[s.encode()] if isinstance(got, dict) else got[0][1]
    while len(rows) == 1 and isinstance(rows[0], list):
        rows = rows[0]
    eq(rows, [(second, {b"n": b"2", b"bin": b"\x00\xff"})])
    eq(r.xread({s: "$"}, block=30) in ([], {}, None), True)
    eq(r.xgroup_create(s, "g", id="0"), True)
    got = r.xreadgroup("g", "me", {s: ">"}, count=1)
    rows = got[s.encode()] if isinstance(got, dict) else got[0][1]
    while len(rows) == 1 and isinstance(rows[0], list):
        rows = rows[0]
    eq(rows, [(first, {b"n": b"1"})])
    eq(r.xpending(s, "g")["pending"], 1)
    eq(r.xack(s, "g", first), 1)
    eq(r.xpending(s, "g")["pending"], 0)
    eq(r.xinfo_stream(s)["length"], 2)
    eq(r.xdel(s, first), 1)
    eq(r.xtrim(s, maxlen=0, approximate=False), 1)
    eq(r.exists(s), 1)

    # a second connection wakes the blocked one
    other = redis.Redis(port=PORT, socket_timeout=30, **KW)
    threading.Timer(0.15, lambda: other.xadd(s, {"late": "1"})).start()
    got = r.xread({s: "$"}, block=5000)
    rows = got[s.encode()] if isinstance(got, dict) else got[0][1]
    while len(rows) == 1 and isinstance(rows[0], list):
        rows = rows[0]
    eq(rows[0][1], {b"late": b"1"})
    threading.Timer(0.15, lambda: other.rpush(q, "job")).start()
    eq(tuple(r.blpop([q], timeout=5)), (q.encode(), b"job"))
    eq(r.blpop([q], timeout=0.05), None)
    threading.Timer(0.15, lambda: other.zadd(q, {"m": 2})).start()
    eq(tuple(r.bzpopmin([q], timeout=5)), (q.encode(), b"m", 2.0))
    eq(r.client_setname("py-check"), True)
    eq(r.client_getname(), "py-check" if isinstance(r.client_getname(), str) else b"py-check")
    eq(any(c["name"] == "py-check" for c in r.client_list()), True)
    other.close()
    r.delete(s, q)


def pairs(reply):
    # redis-py gives member/score tuples in RESP2 and lists in RESP3, and in
    # RESP3 the one pair of ZPOPMIN without a count comes back flat.
    if len(reply) == 2 and not isinstance(reply[0], (list, tuple)):
        reply = [reply]
    return [tuple(p) for p in reply]


def zset_scores():
    k = P + "zsc"
    r.delete(k)
    r.zadd(k, {"a": 1, "b": 2.5, "c": 1.5})
    eq(r.zscore(k, "a"), 1.0)
    eq(r.zscore(k, "c"), 1.5)
    eq(r.zscore(k, "nope"), None)
    eq(r.zmscore(k, ["a", "nope", "c"]), [1.0, None, 1.5])
    eq(r.zadd(k, {"a": 4.25}, incr=True), 5.25)
    eq(r.zadd(k, {"a": -0.25}, incr=True, xx=True), 5.0)
    eq(r.zincrby(k, 0.5, "c"), 2.0)
    eq(r.zincrby(k, 1, "new"), 1.0)  # new c b a
    eq(r.zrank(k, "c", withscore=True), [1, 2.0])
    eq(r.zrevrank(k, "c", withscore=True), [2, 2.0])
    eq(r.zrank(k, "nope", withscore=True), None)
    eq(pairs(r.zrange(k, 0, -1, withscores=True)), [(b"new", 1.0), (b"c", 2.0), (b"b", 2.5), (b"a", 5.0)])
    eq(pairs(r.zrange(k, 0, 1, desc=True, withscores=True)), [(b"a", 5.0), (b"b", 2.5)])
    eq(pairs(r.zrangebyscore(k, 2, 3, withscores=True)), [(b"c", 2.0), (b"b", 2.5)])
    eq(pairs(r.zrevrange(k, 0, 0, withscores=True)), [(b"a", 5.0)])
    eq(pairs(r.zrevrangebyscore(k, 5, 2.5, withscores=True)), [(b"a", 5.0), (b"b", 2.5)])
    eq(pairs(r.zrange(k, 2, 5, byscore=True, withscores=True, offset=1, num=1)), [(b"b", 2.5)])
    r.delete(k)
    r.zadd(k, {"lo": float("-inf"), "hi": float("inf"), "mid": 1.5, "frac": -0.1})
    eq(r.zrange(k, 0, -1), bl("lo", "frac", "mid", "hi"))
    eq(pairs(r.zrange(k, 0, -1, withscores=True)),
       [(b"lo", float("-inf")), (b"frac", -0.1), (b"mid", 1.5), (b"hi", float("inf"))])
    eq(r.zscore(k, "hi"), float("inf"))
    eq(r.zscore(k, "lo"), float("-inf"))
    eq(r.zmscore(k, ["lo", "hi"]), [float("-inf"), float("inf")])
    eq(r.zrangebyscore(k, "-inf", "+inf"), bl("lo", "frac", "mid", "hi"))
    eq(r.zrangebyscore(k, "(-inf", "(inf"), bl("frac", "mid"))
    eq(r.zcount(k, "-inf", 0), 2)
    eq(r.zincrby(k, 5, "hi"), float("inf"))
    eq(dict(r.zscan(k, 0)[1]), {b"lo": float("-inf"), b"frac": -0.1, b"mid": 1.5, b"hi": float("inf")})
    r.delete(k)


def zset_pop():
    k = P + "zp"
    r.delete(k)
    r.zadd(k, {"a": 1, "b": 2, "c": 3, "d": 4.5, "e": 5})
    eq(pairs(r.zpopmin(k)), [(b"a", 1.0)])
    eq(pairs(r.zpopmax(k)), [(b"e", 5.0)])
    eq(pairs(r.zpopmin(k, 2)), [(b"b", 2.0), (b"c", 3.0)])
    eq(pairs(r.zpopmax(k, 5)), [(b"d", 4.5)])
    eq(pairs(r.zpopmin(k)), [])
    eq(pairs(r.zpopmax(k, 3)), [])
    eq(r.exists(k), 0)


def zsets_ranges():
    k = P + "zr"
    r.delete(k)
    r.zadd(k, {"a": 1, "b": 2, "c": 3, "d": 4, "e": 5})
    eq(r.zrange(k, 0, 1), bl("a", "b"))
    eq(r.zrange(k, -2, -1), bl("d", "e"))
    eq(r.zrange(k, 0, 1, desc=True), bl("e", "d"))
    eq(r.zrange(k, 2, 3, byscore=True), bl("b", "c"))
    eq(r.zrange(k, "(1", "+inf", byscore=True, offset=1, num=2), bl("c", "d"))
    eq(r.zrange(k, "+inf", "(2", byscore=True, desc=True, offset=0, num=2), bl("e", "d"))
    eq(r.zrangebyscore(k, "-inf", 3), bl("a", "b", "c"))
    eq(r.zrangebyscore(k, "(1", "(4"), bl("b", "c"))
    eq(r.zrangebyscore(k, 1, 5, start=1, num=2), bl("b", "c"))
    eq(r.zrevrangebyscore(k, 5, 4), bl("e", "d"))
    eq(r.zrevrangebyscore(k, "+inf", "-inf", start=0, num=1), bl("e"))
    eq(r.zrevrange(k, 0, 1), bl("e", "d"))
    eq(r.zrange(k, 10, 20), [])
    eq(r.zrange(P + "nozset", 0, -1), [])
    eq(r.zremrangebyrank(k, 0, 1), 2)  # c d e
    eq(r.zremrangebyscore(k, "(3", 4), 1)  # c e
    eq(r.zrange(k, 0, -1), bl("c", "e"))
    eq(r.zremrangebyscore(k, "-inf", "+inf"), 2)
    eq(r.exists(k), 0)
    big = P + "zbig"
    r.delete(big)
    r.zadd(big, {f"m{i}": i for i in range(300)})
    seen = {}
    cur = 0
    for _ in range(1000):
        cur, items = r.zscan(big, cur, count=50)
        seen.update(dict(items))
        if cur == 0:
            break
    eq(len(seen), 300)
    eq(seen[b"m299"], 299.0)
    eq(len(list(r.zscan_iter(big, match="m2*"))), 111)
    r.delete(big)


def scores(key):
    """member -> score through ZSCAN, whose reply shape is the same in RESP2 and RESP3."""
    return dict(r.zscan(key, 0, count=1000)[1])


def zsets_store():
    a, c, d = P + "zsa", P + "zsb", P + "zsd"
    r.delete(a, c, d)
    r.zadd(a, {"x": 1, "y": 2, "z": 3})
    r.zadd(c, {"y": 10, "z": 20, "w": 0.5})
    eq(r.zunionstore(d, [a, c]), 4)
    eq(r.zrange(d, 0, -1), bl("w", "x", "y", "z"))
    eq(r.zunionstore(d, {a: 2, c: 0.5}), 4)
    eq(scores(d), {b"w": 0.25, b"x": 2.0, b"y": 9.0, b"z": 16.0})
    eq(r.zunionstore(d, [a, c], aggregate="MAX"), 4)
    eq(scores(d)[b"z"], 20.0)
    eq(r.zinterstore(d, [a, c]), 2)
    eq(scores(d), {b"y": 12.0, b"z": 23.0})
    eq(r.zinterstore(d, {a: 3, c: 2}), 2)
    eq(scores(d), {b"y": 26.0, b"z": 49.0})
    eq(r.zinterstore(d, [a, c], aggregate="MIN"), 2)
    eq(scores(d)[b"y"], 2.0)
    r.sadd(P + "zss", "y", "q")
    eq(r.zinterstore(d, [a, P + "zss"]), 1)  # sets count as score 1
    eq(scores(d)[b"y"], 3.0)
    eq(r.zinterstore(d, [a, P + "nozset"]), 0)
    eq(r.exists(d), 0)
    r.delete(a, c, d, P + "zss")


def string_key_extras():
    h = P + "hx"
    r.delete(P + "f", P + "gr", P + "grnew", P + "abc", h)
    eq(r.incrbyfloat(P + "f", 1.5), 1.5)
    eq(r.incrbyfloat(P + "f", 2), 3.5)
    eq(r.incrbyfloat(P + "f", -0.25), 3.25)
    eq(r.get(P + "f"), b"3.25")
    r.set(P + "abc", "abc")
    raises(lambda: r.incrbyfloat(P + "abc", 1), "not a valid float")
    r.set(P + "gr", "Hello World")
    eq(r.getrange(P + "gr", 0, 4), b"Hello")
    eq(r.getrange(P + "gr", -5, -1), b"World")
    eq(r.getrange(P + "gr", 20, 30), b"")
    eq(r.setrange(P + "gr", 6, "Redis"), 11)
    eq(r.get(P + "gr"), b"Hello Redis")
    eq(r.setrange(P + "gr", 13, "!"), 14)
    eq(r.get(P + "gr"), b"Hello Redis\x00\x00!")
    eq(r.setrange(P + "grnew", 2, "x"), 3)
    eq(r.get(P + "grnew"), b"\x00\x00x")
    r.hset(h, mapping={"a": "1", "b": "hello", "c": "2.5"})
    eq(sorted(r.hkeys(h)), bl("a", "b", "c"))
    eq(sorted(r.hvals(h)), sorted(bl("1", "hello", "2.5")))
    eq(r.hstrlen(h, "b"), 5)
    eq(r.hstrlen(h, "nope"), 0)
    eq(r.hincrbyfloat(h, "c", 0.5), 3.0)
    eq(r.hincrbyfloat(h, "new", 1.5), 1.5)
    eq(r.hincrby(h, "a", 4), 5)
    raises(lambda: r.hincrbyfloat(h, "b", 1), "not a float")
    assert r.hrandfield(h) in bl("a", "b", "c", "new")
    eq(len(r.hrandfield(h, 2)), 2)
    eq(len(r.hrandfield(h, -6)), 6)
    eq(r.hrandfield(P + "nohash"), None)
    eq(r.hrandfield(P + "nohash", 3), [])
    eq(r.hexists(h, "a"), True)
    eq(r.hmget(h, ["a", "zz"]), [b"5", None])
    eq(r.hsetnx(h, "a", "x"), False)
    r.delete(P + "f", P + "gr", P + "grnew", P + "abc", h)


def hrandfield_values():
    h = P + "hrf"
    r.delete(h)
    r.hset(h, mapping={"a": "1", "b": "hello", "c": "3"})
    got = r.hrandfield(h, 3, withvalues=True)
    # RESP2: flat [f, v, f, v]; RESP3: [[f, v], ...]. redis-py hands both back as the server sent them.
    flat = [x for e in got for x in (e if isinstance(e, (list, tuple)) else [e])]
    eq(len(flat), 6)
    eq(dict(zip(flat[::2], flat[1::2])), {b"a": b"1", b"b": b"hello", b"c": b"3"})
    eq(len(r.hrandfield(h, -5, withvalues=True)), 5 if PROTO == "3" else 10)
    r.delete(h)


def keys_misc():
    names = ["r1", "r2", "r3", "cp1", "cp2", "ms1", "ms2", "ms3", "ty:s", "ty:h", "ty:l", "ty:set", "ty:z", "ty:int"]
    r.delete(*[P + n for n in names])
    r.set(P + "r1", "v")
    eq(r.rename(P + "r1", P + "r2"), True)
    eq(r.exists(P + "r1", P + "r2"), 1)
    raises(lambda: r.rename(P + "nokey", P + "x"), "no such key")
    r.set(P + "r1", "other")
    eq(r.renamenx(P + "r1", P + "r2"), False)
    eq(r.get(P + "r2"), b"v")
    eq(r.renamenx(P + "r1", P + "r3"), True)
    eq(r.get(P + "r3"), b"other")
    r.rpush(P + "cp1", "a", "b")
    eq(r.copy(P + "cp1", P + "cp2"), True)
    eq(r.copy(P + "cp1", P + "cp2"), False)
    eq(r.copy(P + "cp1", P + "cp2", replace=True), True)
    eq(r.lrange(P + "cp2", 0, -1), bl("a", "b"))
    r.rpush(P + "cp1", "c")
    eq(r.llen(P + "cp2"), 2)  # a copy, not an alias
    eq(r.copy(P + "nokey", P + "cp3"), False)
    eq(r.msetnx({P + "ms1": "1", P + "ms2": "2"}), True)
    eq(r.msetnx({P + "ms1": "x", P + "ms3": "3"}), False)
    eq(r.exists(P + "ms3"), 0)
    eq(r.get(P + "ms1"), b"1")
    eq(r.touch(P + "r2", P + "cp1", P + "nokey"), 2)
    r.set(P + "ty:s", "x")
    r.hset(P + "ty:h", "f", "v")
    r.rpush(P + "ty:l", "x")
    r.sadd(P + "ty:set", "x")
    r.zadd(P + "ty:z", {"x": 1})
    eq([r.type(P + "ty:" + t) for t in ("s", "h", "l", "set", "z")],
       [b"string", b"hash", b"list", b"set", b"zset"])
    eq(r.type(P + "nokey"), b"none")
    for n in ("ty:s", "ty:h", "ty:l", "ty:set", "ty:z"):
        enc = r.object("encoding", P + n)
        assert isinstance(enc, bytes) and enc, enc
    eq(r.object("encoding", P + "nokey"), None)
    assert r.object("encoding", P + "ty:s") in (b"embstr", b"raw")
    r.set(P + "ty:int", "12345")
    eq(r.object("encoding", P + "ty:int"), b"int")
    seen = {}
    for t in ("string", "hash", "list", "set", "zset"):
        got = set()
        cur = 0
        for _ in range(10000):
            cur, keys = r.scan(cur, match=P + "ty:*", count=100, _type=t)
            got.update(keys)
            if cur == 0:
                break
        seen[t] = sorted(got)
    pp = P.encode()
    eq(seen["string"], [pp + b"ty:int", pp + b"ty:s"])
    eq(seen["hash"], [pp + b"ty:h"])
    eq(seen["list"], [pp + b"ty:l"])
    eq(seen["set"], [pp + b"ty:set"])
    eq(seen["zset"], [pp + b"ty:z"])
    eq(sorted(r.scan_iter(match=P + "ty:*")),
       sorted(pp + b"ty:" + x for x in (b"int", b"s", b"h", b"l", b"set", b"z")))
    rk = r.randomkey()
    assert rk is not None and r.exists(rk) == 1, rk
    r.delete(*[P + n for n in names])


def recv(ps, want_type, timeout=5):
    """Next message of the wanted type; a missing message fails instead of hanging."""
    end = time.time() + timeout
    while time.time() < end:
        m = ps.get_message(timeout=0.2)
        if m is not None and m["type"] == want_type:
            return m
    raise AssertionError(f"no {want_type} message within {timeout}s")


def pubsub_roundtrip():
    ps = r.pubsub()
    ch, ch2, pat, both = P + "chan", P + "chan2", P + "pat:*", P + "pat:both"
    try:
        ps.subscribe(ch)
        eq(recv(ps, "subscribe"), {"type": "subscribe", "pattern": None, "channel": ch.encode(), "data": 1})
        ps.psubscribe(pat)
        eq(recv(ps, "psubscribe")["data"], 2)
        eq(r.publish(ch, "hello"), 1)
        eq(recv(ps, "message"), {"type": "message", "pattern": None, "channel": ch.encode(), "data": b"hello"})
        eq(r.publish(P + "pat:one", "p1"), 1)
        eq(recv(ps, "pmessage"), {"type": "pmessage", "pattern": pat.encode(),
                                  "channel": (P + "pat:one").encode(), "data": b"p1"})
        eq(r.publish(P + "other", "x"), 0)
        eq(r.publish(ch2, "x"), 0)
        # a channel matching both a subscription and a pattern
        ps.subscribe(both)
        eq(recv(ps, "subscribe")["data"], 3)
        ps.psubscribe(P + "pat:b*")
        eq(recv(ps, "psubscribe")["data"], 4)
        eq(r.publish(both, "twice"), 3)
        eq([recv(ps, "message")["data"], recv(ps, "pmessage")["data"], recv(ps, "pmessage")["data"]],
           [b"twice"] * 3)
        eq(r.pubsub_numsub(ch, ch2), [(ch.encode(), 1), (ch2.encode(), 0)])
        eq(r.pubsub_numpat(), 2)
        eq(sorted(r.pubsub_channels(P + "*")), sorted([ch.encode(), both.encode()]))
        ps.punsubscribe(P + "pat:b*")
        eq(recv(ps, "punsubscribe")["data"], 3)
        ps.unsubscribe(ch)
        eq(recv(ps, "unsubscribe")["data"], 2)
        eq(r.publish(ch, "gone"), 0)
        ps.punsubscribe(pat)
        eq(recv(ps, "punsubscribe")["data"], 1)
        eq(r.publish(P + "pat:one", "gone"), 0)
        ps.unsubscribe()
        recv(ps, "unsubscribe")
        eq(r.pubsub_numsub(both), [(both.encode(), 0)])
    finally:
        ps.close()


def tx_new_types():
    for n in ("txl", "txz", "txs", "w"):
        r.delete(P + n)
    p = r.pipeline()  # MULTI/EXEC
    p.rpush(P + "txl", "a", "b").zadd(P + "txz", {"m": 1.5, "n": 2}).sadd(P + "txs", "x", "y")
    p.lrange(P + "txl", 0, -1).zrange(P + "txz", 0, -1).scard(P + "txs")
    eq(p.execute(), [2, 2, 2, bl("a", "b"), bl("m", "n"), 2])
    p = r.pipeline()
    p.lpop(P + "txl").zrem(P + "txz", "m").spop(P + "txs", 2)
    out = p.execute()
    eq(out[0], b"a")
    eq(out[1], 1)
    eq(sorted(out[2]), bl("x", "y"))
    r.set(P + "txstr", "s")
    # an error inside EXEC does not roll back the other commands
    p = r.pipeline()
    p.rpush(P + "txl", "q").lpush(P + "txstr", "bad").llen(P + "txl")
    out = p.execute(raise_on_error=False)
    eq(out[0], 2)
    assert isinstance(out[1], redis.ResponseError) and "WRONGTYPE" in str(out[1]), repr(out[1])
    eq(out[2], 2)
    # WATCH aborts EXEC when the key changed
    r.set(P + "w", "1")
    with r.pipeline() as p:
        p.watch(P + "w")
        r2 = redis.Redis(port=PORT, socket_timeout=30, **KW)
        r2.set(P + "w", "2")
        r2.close()
        p.multi()
        p.zadd(P + "txz", {"w": 1})
        try:
            p.execute()
            raise AssertionError("EXEC should have aborted")
        except redis.WatchError:
            pass
    eq(r.zscore(P + "txz", "w"), None)
    r.delete(P + "txl", P + "txz", P + "txs", P + "w", P + "txstr")


def lua_new_types():
    z, l, s = P + "lz", P + "ll", P + "ls"
    r.delete(z, l, s)
    script = """
redis.call('ZADD', KEYS[1], 1.5, 'a', 2, 'b', 3, 'c')
redis.call('RPUSH', KEYS[2], 'x', 'y', 'z')
redis.call('SADD', KEYS[3], 'm', 'n')
return {redis.call('ZRANGE', KEYS[1], 0, -1), redis.call('LRANGE', KEYS[2], 0, -1),
        redis.call('SCARD', KEYS[3]), redis.call('ZSCORE', KEYS[1], 'a'),
        redis.call('ZRANGE', KEYS[1], 0, 0, 'WITHSCORES'), redis.call('TYPE', KEYS[2])['ok']}
"""
    out = r.eval(script, 3, z, l, s)
    eq(out, [bl("a", "b", "c"), bl("x", "y", "z"), 2, b"1.5", bl("a", "1.5"), b"list"])
    eq(r.zrange(z, 0, -1), bl("a", "b", "c"))
    eq(r.eval("return redis.call('ZPOPMIN', KEYS[1])", 1, z), bl("a", "1.5"))
    eq(r.eval("return redis.call('LPOP', KEYS[1], 2)", 1, l), bl("x", "y"))
    eq(sorted(r.eval("return redis.call('SMEMBERS', KEYS[1])", 1, s)), bl("m", "n"))
    eq(r.eval("return redis.call('ZINCRBY', KEYS[1], 0.5, 'b')", 1, z), b"2.5")
    eq(r.eval("return redis.call('EXISTS', KEYS[1])", 1, P + "none"), 0)
    raises(lambda: r.eval("return redis.call('LPUSH', KEYS[1], 'x')", 1, z), "WRONGTYPE")
    eq(r.eval("return redis.pcall('LPUSH', KEYS[1], 'x')['err'] ~= nil", 1, z), 1)
    sc = r.register_script("return redis.call('ZRANGEBYSCORE', KEYS[1], ARGV[1], ARGV[2], 'WITHSCORES')")
    eq(sc(keys=[z], args=["-inf", "+inf"]), bl("b", "2.5", "c", "3"))
    r.delete(z, l, s)


def config_info():
    mm = r.config_get("maxmemory")
    assert isinstance(mm, dict) and "maxmemory" in mm, mm
    int(mm["maxmemory"])
    pol = r.config_get("maxmemory-policy")
    assert pol.get("maxmemory-policy"), pol
    both = r.config_get("maxmemory*")
    assert "maxmemory" in both and "maxmemory-policy" in both, both
    info = r.info()
    assert isinstance(info, dict) and len(info) > 3, info
    assert isinstance(r.info("memory"), dict)
    assert isinstance(r.info("keyspace"), dict)


def databases_users_command():
    other = redis.Redis(port=PORT, db=3, socket_timeout=30, **KW)
    other.set(P + "db", "three")
    eq(r.get(P + "db"), None)
    eq(other.get(P + "db"), b"three")
    eq(r.move(P + "nothing", 3), False)
    other.flushdb()
    other.close()

    assert r.command_count() > 150
    assert "get" in r.command()
    name = P + "user"
    secret = r.acl_genpass()
    eq(r.acl_setuser(name, enabled=True, passwords=["+" + (secret if isinstance(secret, str) else secret.decode())],
                     keys=[P + "mine:*"], commands=["+get", "+set", "+ping"]), True)
    assert r.acl_whoami() in ("default", b"default")
    u = redis.Redis(port=PORT, username=name, password=secret, socket_timeout=30, **KW)
    eq(u.ping(), True)
    eq(u.set(P + "mine:a", "1"), True)
    eq(u.get(P + "mine:a"), b"1")
    raises(lambda: u.get(P + "db"), "No permissions to access")
    raises(lambda: u.delete(P + "mine:a"), "no permissions to run")
    u.close()
    bad = redis.Redis(port=PORT, username=name, password="wrong", socket_timeout=30, **KW)
    try:
        bad.ping()
        raise AssertionError("a wrong password was accepted")
    except redis.AuthenticationError:
        pass
    bad.close()
    eq(r.acl_deluser(name), 1)


def p1_commands():
    h, a, b = P + "p1:h", P + "p1:hll:a", P + "p1:hll:b"
    r.delete(h, a, b, P + "p1:copy", P + "p1:x", P + "p1:y", P + "p1:fn")
    r.hset(h, mapping={"f": "1", "g": "2"})
    if hasattr(r, "hexpire"):
        eq(r.hexpire(h, 100, "f", "missing"), [1, -2])
        eq(r.httl(h, "f", "g"), [100, -1])
        eq(r.hpersist(h, "f", "g"), [1, -1])
        eq(r.hpexpire(h, 0, "g"), [2])
        eq(r.hkeys(h), [b"f"])
    else:
        eq(r.execute_command("HEXPIRE", h, 100, "FIELDS", 1, "f"), [1])

    eq(r.pfadd(a, "x", "y", "z"), 1)
    eq(r.pfadd(b, "z", "w"), 1)
    eq(r.pfcount(a, b), 4)
    eq(r.pfmerge(a, b), True)
    eq(r.pfcount(a), 4)

    payload = r.dump(h)
    assert isinstance(payload, bytes) and len(payload) > 10
    assert r.restore(P + "p1:copy", 0, payload) in (b"OK", True)
    eq(r.hgetall(P + "p1:copy"), {b"f": b"1"})
    raises(lambda: r.restore(P + "p1:copy", 0, payload), "BUSYKEY")
    assert r.restore(P + "p1:copy", 5000, payload, replace=True) in (b"OK", True)
    assert 0 < r.pttl(P + "p1:copy") <= 5000
    raises(lambda: r.restore(P + "p1:bad", 0, payload[:-1]), "checksum")
    eq(r.dump(P + "p1:none"), None)

    assert r.memory_usage(h) > 0
    eq(r.memory_usage(P + "p1:none"), None)
    r.mset({P + "p1:x": "ohmytext", P + "p1:y": "mynewtext"})
    eq(r.lcs(P + "p1:x", P + "p1:y"), b"mytext")
    eq(r.lcs(P + "p1:x", P + "p1:y", len=True), 6)

    lib = ("#!lua name=pylib\n"
           "redis.register_function('py_put', function(k, a) return redis.call('SET', k[1], a[1]) end)\n"
           "redis.register_function{function_name='py_get', "
           "callback=function(k) return redis.call('GET', k[1]) end, flags={'no-writes'}}")
    eq(r.function_load(lib, replace=True), b"pylib")
    eq(r.fcall("py_put", 1, P + "p1:fn", "v"), b"OK")
    eq(r.fcall_ro("py_get", 1, P + "p1:fn"), b"v")
    raises(lambda: r.fcall_ro("py_put", 1, P + "p1:fn", "w"), "write flag")
    assert len(r.function_list()) == 1
    raises(lambda: r.function_load(lib), "already exists")
    r.function_delete("pylib")
    raises(lambda: r.fcall("py_get", 1, P + "p1:fn"), "Function not found")

    r.config_set("notify-keyspace-events", "KEA")
    try:
        ps = r.pubsub()
        ps.psubscribe("__key*@0__:" + P + "p1:note", "__keyevent@0__:del")
        recv(ps, "psubscribe")
        recv(ps, "psubscribe")
        r.set(P + "p1:note", "1")
        m = recv(ps, "pmessage")
        eq((m["channel"], m["data"]), (("__keyspace@0__:" + P + "p1:note").encode(), b"set"))
        r.delete(P + "p1:note")
        got = sorted((recv(ps, "pmessage")["data"], recv(ps, "pmessage")["data"]))
        eq(got, sorted((b"del", (P + "p1:note").encode())))
        ps.close()
    finally:
        r.config_set("notify-keyspace-events", "")


def p2_commands():
    k = P + "p2:"
    # GEO
    eq(r.geoadd(k + "geo", (13.361389, 38.115556, "Palermo", 15.087269, 37.502669, "Catania")), 2)
    eq(round(r.geodist(k + "geo", "Palermo", "Catania", unit="km"), 4), 166.2742)
    hashes = r.geohash(k + "geo", "Palermo", "Nowhere")
    eq([h.decode() if isinstance(h, bytes) else h for h in hashes], ["sqc8b49rny0", None])
    pos = r.geopos(k + "geo", "Catania", "Nowhere")
    assert abs(pos[0][0] - 15.087269) < 1e-5 and pos[1] is None, pos
    eq(r.geosearch(k + "geo", longitude=15, latitude=37, radius=200, unit="km", sort="ASC"), [b"Catania", b"Palermo"])
    near = r.geosearch(k + "geo", member="Palermo", width=400, height=400, unit="km", sort="ASC", withdist=True)
    eq(near[0][0], b"Palermo")
    spot = r.geosearch(k + "geo", member="Catania", radius=1, unit="km", withcoord=True)[0][1]
    assert abs(float(spot[0]) - 15.087269) < 1e-5, spot
    eq(r.geosearchstore(k + "geo2", k + "geo", longitude=15, latitude=37, radius=100, unit="km"), 1)
    eq(r.georadius(k + "geo", 15, 37, 200, unit="km", sort="DESC"), [b"Palermo", b"Catania"])
    eq(r.georadiusbymember(k + "geo", "Palermo", 10, unit="km"), [b"Palermo"])
    # BITFIELD
    bf = r.bitfield(k + "bits")
    eq(bf.set("u8", 0, 255).get("i8", 0).incrby("u8", 0, 1).execute(), [0, -1, 0])
    eq(r.bitfield(k + "bits", default_overflow="SAT").incrby("u2", 100, 5).execute(), [3])
    eq(r.bitfield(k + "bits", default_overflow="FAIL").incrby("u2", 100, 5).execute(), [None])
    eq(r.bitfield_ro(k + "bits", "u2", 100), [3])
    # SORT
    r.rpush(k + "ids", 3, 1, 2)
    r.mset({k + "w_1": 30, k + "w_2": 10, k + "w_3": 20, k + "n_1": "one", k + "n_2": "two", k + "n_3": "three"})
    eq(r.sort(k + "ids"), [b"1", b"2", b"3"])
    eq(r.sort(k + "ids", desc=True, start=0, num=2), [b"3", b"2"])
    eq(r.sort(k + "ids", by=k + "w_*", get=["#", k + "n_*"]), [b"2", b"two", b"3", b"three", b"1", b"one"])
    eq(r.sort(k + "ids", by=k + "w_*", store=k + "sorted"), 3)
    eq(r.lrange(k + "sorted", 0, -1), [b"2", b"3", b"1"])
    eq(r.sort_ro(k + "ids", alpha=True), [b"1", b"2", b"3"])
    # SWAPDB, SLOWLOG, OBJECT FREQ
    other = redis.Redis(port=PORT, db=7, socket_timeout=30, **KW)
    other.set(k + "swap", "seven")
    eq(r.swapdb(0, 7), True)
    eq(r.get(k + "swap"), b"seven")
    eq(r.swapdb(0, 7), True)
    eq(r.get(k + "swap"), None)
    other.delete(k + "swap")
    other.close()
    old = r.config_get("slowlog-log-slower-than")["slowlog-log-slower-than"]
    try:
        r.config_set("slowlog-log-slower-than", 0)
        r.slowlog_reset()
        r.set(k + "slow", "v")
        entry = [e for e in r.slowlog_get(10) if e["command"].startswith(b"SET ")][0]
        assert entry["duration"] >= 0 and entry["id"] >= 0, entry
        assert r.slowlog_len() >= 1
    finally:
        r.config_set("slowlog-log-slower-than", old)
    policy = r.config_get("maxmemory-policy")["maxmemory-policy"]
    try:
        r.config_set("maxmemory-policy", "allkeys-lfu")
        assert r.object("freq", k + "slow") >= 0
    finally:
        r.config_set("maxmemory-policy", policy)
    # MONITOR
    with r.monitor() as m:
        r.set(k + "seen", "1")
        for event in m.listen():
            if event["command"].startswith("SET") and "seen" in event["command"]:
                eq(event["db"], 0)
                break
    # CLIENT TRACKING, sent to a second connection.
    sink = redis.Redis(port=PORT, socket_timeout=30, protocol=2, **TLS)
    ps = sink.pubsub()
    ps.execute_command("CLIENT", "ID")
    sink_id = ps.parse_response()
    ps.subscribe("__redis__:invalidate")
    recv(ps, "subscribe")
    reader = redis.Redis(port=PORT, socket_timeout=30, single_connection_client=True, **KW)
    assert reader.client_tracking_on(clientid=sink_id)
    eq(reader.get(k + "cached"), None)
    eq(reader.client_getredir(), sink_id)
    r.set(k + "cached", "v")
    m = recv(ps, "message")
    eq(m["data"], [(k + "cached").encode()])
    assert reader.client_tracking_off()
    reader.close()
    ps.close()
    sink.close()


def durability_and_kill():
    eq(r.config_get("appendfsync")["appendfsync"] in ("always", "everysec", "no"), True)
    old = r.config_get("appendfsync")["appendfsync"]
    eq(r.config_set("appendfsync", "always"), True)
    eq(r.set(P + "durable", "1"), True)
    eq(r.config_set("appendfsync", old), True)
    eq(r.config_get("timeout")["timeout"], "0")
    try:
        eq(r.save(), True)
    except redis.ResponseError as e:
        assert "without a data file" in str(e), str(e)
    victim = redis.Redis(port=PORT, socket_timeout=30, single_connection_client=True, **KW)
    vid = victim.client_id()
    eq(r.client_kill_filter(_id=vid), 1)
    # redis-py reconnects by itself, so the list is what tells.
    assert all(int(c["id"]) != vid for c in r.client_list())
    eq(r.client_kill_filter(_id=vid), 0)
    other = redis.Redis(port=PORT, socket_timeout=30, single_connection_client=True, **KW)
    addr = other.client_info()["addr"]
    eq(r.client_kill(addr), True)
    raises(lambda: r.client_kill(addr), "No such client")
    eq(r.client_kill_filter(_type="pubsub"), 0)
    eq(r.ping(), True)


def flushall_dbsize():
    r.set(P + "last", "1")
    assert r.dbsize() >= 1
    eq(r.flushall(), True)
    eq(r.dbsize(), 0)
    eq(r.get(P + "last"), None)


CHECKS = [
    connect_ping, set_get, binary_key_and_value, big_value_8mib, counters,
    counter_errors, hashes, wrong_type, mset_mget, del_exists_unlink,
    keys_pattern, eval_values, eval_calls_redis, eval_errors,
    script_helper_evalsha, pipeline_plain_10000, pipeline_with_error_in_middle,
    x_pipeline_transaction_multi_exec, x_set_with_expiry_and_nx,
    concurrency_16_threads, unknown_command_keeps_connection,
    hgetall_100k_fields, client_side_commands,
    lists, sets, zsets_basic, zset_scores, zset_pop, zsets_ranges, zsets_store,
    string_key_extras, hrandfield_values, keys_misc, pubsub_roundtrip, tx_new_types,
    lua_new_types, config_info, streams_and_blocking,
    databases_users_command, p1_commands, p2_commands, durability_and_kill,
    flushall_dbsize,
]

print(f"== python redis-py {redis.__version__} protocol={PROTO or 'default'}{' tls' if TLS else ''}")
for c in CHECKS:
    check(c.__name__, c)
print(f"== python: {len(CHECKS) - len(failed)}/{len(CHECKS)} passed; failed: {failed}")
sys.exit(1 if failed else 0)
