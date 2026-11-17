// HCache spike against StackExchange.Redis. Every check runs; nothing aborts the run.
// Checks named x_* use Redis features outside the spike's scope; they are not expected to pass.
using System.Reflection;
using System.Text;
using StackExchange.Redis;

int port = int.Parse(Environment.GetEnvironmentVariable("HCACHE_PORT") ?? "6400");
string? proto = Environment.GetEnvironmentVariable("HCACHE_PROTO");
bool logHandshake = Environment.GetEnvironmentVariable("HCACHE_LOG") == "1";
const string P = "cs:";
var failed = new List<string>();

ConfigurationOptions Options()
{
    var o = new ConfigurationOptions
    {
        AllowAdmin = true,
        SyncTimeout = 30000,
        AsyncTimeout = 30000,
        ConnectTimeout = 30000,
        AbortOnConnectFail = true,
    };
    o.EndPoints.Add("127.0.0.1", port);
    if (proto == "2") o.Protocol = RedisProtocol.Resp2;
    return o;
}

string Show(object? o) => o switch
{
    null => "null",
    byte[] b => "b\"" + Encoding.Latin1.GetString(b).Replace("\0", "\\x00").Replace("\r", "\\r").Replace("\n", "\\n") + "\"",
    RedisValue v => v.IsNull ? "null" : Show((byte[]?)v),
    System.Collections.IEnumerable e and not string => "[" + string.Join(", ", e.Cast<object?>().Select(Show)) + "]",
    _ => o.ToString() ?? "?",
};

string Cut(string s, int n) => s.Length <= n ? s : s[..n];

void Eq(object? got, object? want)
{
    string g = Show(got), w = Show(want);
    if (g != w) throw new Exception($"got {Cut(g, 200)}, want {Cut(w, 200)}");
}

void Raises(Func<object?> fn, string text)
{
    object? got;
    try { got = fn(); }
    catch (RedisServerException e)
    {
        if (!e.Message.Contains(text, StringComparison.OrdinalIgnoreCase))
            throw new Exception($"error {e.Message} lacks '{text}'");
        return;
    }
    throw new Exception($"no error, got {Cut(Show(got), 100)}");
}

void Check(string name, Action fn)
{
    try { fn(); Console.WriteLine($"PASS {name}"); }
    catch (Exception e)
    {
        failed.Add(name);
        if (e is AggregateException ae && ae.InnerException != null) e = ae.InnerException;
        Console.WriteLine($"FAIL {name}: {e.GetType().Name}: {Cut(e.Message, 300)}");
    }
}

string version = typeof(ConnectionMultiplexer).Assembly.GetCustomAttribute<AssemblyInformationalVersionAttribute>()?.InformationalVersion ?? "?";
version = version.Split('+')[0];
Console.WriteLine($"== dotnet StackExchange.Redis {version} protocol={proto ?? "default"}");

ConnectionMultiplexer? mux = null;
IDatabase db = null!;
IServer server = null!;
string? connectError = null;
try
{
    mux = ConnectionMultiplexer.Connect(Options(), logHandshake ? Console.Error : null);
    db = mux.GetDatabase();
    server = mux.GetServer("127.0.0.1", port);
}
catch (Exception e) { connectError = e.Message; }

byte[] B(string s) => Encoding.UTF8.GetBytes(s);
RedisKey K(string s) => P + s;

void ConnectPing()
{
    db.Ping();
    Eq((string?)db.Execute("ECHO", "hi"), "hi");
}

void SetGet()
{
    Eq(db.StringSet(K("s"), "hello"), true);
    Eq(db.StringGet(K("s")), B("hello"));
    Eq(db.StringGet(K("missing")), null);
}

void BinaryKeyAndValue()
{
    var v = Enumerable.Repeat(Enumerable.Range(0, 256).Select(i => (byte)i), 4).SelectMany(x => x).ToArray();
    var k = B(P).Concat(new byte[] { 0, 13, 10, 0xff }).Concat(B(" bin")).ToArray();
    db.StringSet(k, v);
    Eq(db.StringGet(k), v);
    db.StringSet(K("empty"), Array.Empty<byte>());
    Eq(db.StringGet(K("empty")), Array.Empty<byte>());
    db.StringSet(K("uni"), "xin chào 🌏");
    Eq((string?)db.StringGet(K("uni")), "xin chào 🌏");
}

void Big8Mib()
{
    var v = new byte[8 << 20];
    Random.Shared.NextBytes(v);
    db.StringSet(K("big"), v);
    Eq(((byte[]?)db.StringGet(K("big")))?.AsSpan().SequenceEqual(v), true);
    db.KeyDelete(K("big"));
}

void Counters()
{
    db.KeyDelete(K("n"));
    Eq(db.StringIncrement(K("n")), 1L);
    Eq(db.StringIncrement(K("n"), 41), 42L);
    Eq(db.StringDecrement(K("n")), 41L);
    Eq(db.StringDecrement(K("n"), 50), -9L);
    Eq(db.StringGet(K("n")), B("-9"));
}

void CounterErrors()
{
    db.StringSet(K("abc"), "abc");
    Raises(() => db.StringIncrement(K("abc")), "not an integer");
    db.StringSet(K("max"), long.MaxValue.ToString());
    Raises(() => db.StringIncrement(K("max")), "overflow");
    Eq(db.StringGet(K("max")), B(long.MaxValue.ToString()));
}

void Hashes()
{
    db.KeyDelete(K("h"));
    // HashSet(entries) is void in this library; HSET via Execute to see the integer reply (3).
    Eq((long)db.Execute("HSET", K("h"), "a", "1", "b", "2", "c", "3"), 3L);
    Eq(db.HashSet(K("h"), "a", "9"), false); // false = field existed (HSET returned 0)
    Eq(db.HashGet(K("h"), "a"), B("9"));
    Eq(db.HashGet(K("h"), "zz"), null);
    var all = db.HashGetAll(K("h")).ToDictionary(e => (string)e.Name!, e => (string)e.Value!);
    Eq(all.OrderBy(x => x.Key).Select(x => x.Key + "=" + x.Value), new[] { "a=9", "b=2", "c=3" });
    Eq(db.HashLength(K("h")), 3L);
    Eq(db.HashDelete(K("h"), new RedisValue[] { "a", "zz" }), 1L);
    Eq(db.HashGetAll(K("nohash")).Length, 0);
}

void WrongType()
{
    db.StringSet(K("str"), "x");
    Raises(() => db.HashGet(K("str"), "f"), "WRONGTYPE");
    db.KeyDelete(K("h2"));
    db.HashSet(K("h2"), "f", "v");
    Raises(() => db.StringGet(K("h2")), "WRONGTYPE");
    Raises(() => db.StringIncrement(K("h2")), "WRONGTYPE");
}

void MsetMget()
{
    Eq(db.StringSet(new KeyValuePair<RedisKey, RedisValue>[] { new(K("m1"), "1"), new(K("m2"), "2") }), true);
    Eq(db.StringGet(new RedisKey[] { K("m1"), K("nope"), K("m2") }), new RedisValue[] { "1", RedisValue.Null, "2" });
}

void DelExistsUnlink()
{
    db.StringSet(new KeyValuePair<RedisKey, RedisValue>[] { new(K("d1"), "1"), new(K("d2"), "2"), new(K("d3"), "3") });
    Eq(db.KeyExists(new RedisKey[] { K("d1"), K("d2"), K("nope"), K("d1") }), 3L);
    Eq(db.KeyDelete(new RedisKey[] { K("d1"), K("nope") }), 1L);
    Eq((long)db.Execute("UNLINK", K("d2"), K("d3")), 2L); // no UNLINK API; raw command
    Eq(db.KeyExists(new RedisKey[] { K("d1"), K("d2"), K("d3") }), 0L);
}

void KeysPattern()
{
    db.StringSet(new KeyValuePair<RedisKey, RedisValue>[] { new(K("k:1"), "a"), new(K("k:2"), "b"), new(K("k:x["), "c") });
    // IServer.Keys (SCAN or KEYS decided by the library; see HCACHE_LOG / report).
    Eq(server.Keys(pattern: P + "k:?").Select(k => (string)k!).Order(StringComparer.Ordinal), new[] { P + "k:1", P + "k:2" });
    Eq(server.Keys(pattern: P + "k:x\\[").Select(k => (string)k!), new[] { P + "k:x[" });
    Eq(server.Keys(pattern: P + "nothing*").Count(), 0);
}

void EvalValues()
{
    var r = db.ScriptEvaluate("return {KEYS[1], ARGV[1], 7, {1, 2}}", new RedisKey[] { K("k") }, new RedisValue[] { "a" });
    var arr = (RedisResult[])r!;
    Eq(arr.Length, 4);
    Eq((byte[]?)arr[0], B(P + "k"));
    Eq((byte[]?)arr[1], B("a"));
    Eq((long)arr[2], 7L);
    Eq(((RedisResult[])arr[3]!).Select(x => (long)x), new long[] { 1, 2 });
    Eq(db.ScriptEvaluate("return false").IsNull, true);
    Eq((string?)db.ScriptEvaluate("return redis.status_reply('FINE')"), "FINE");
    Eq((long)db.ScriptEvaluate("return 3.99"), 3L);
    Eq((byte[]?)db.ScriptEvaluate("return ARGV[1]", null, new RedisValue[] { new byte[] { 0, 0xff } }), new byte[] { 0, 0xff });
}

void EvalCallsRedis()
{
    var script = "redis.call('SET', KEYS[1], ARGV[1]); return redis.call('GET', KEYS[1])";
    Eq((byte[]?)db.ScriptEvaluate(script, new RedisKey[] { K("ev") }, new RedisValue[] { "from-lua" }), B("from-lua"));
    Eq(db.StringGet(K("ev")), B("from-lua"));
    Eq((long)db.ScriptEvaluate("return redis.call('INCRBY', KEYS[1], 5)", new RedisKey[] { K("evn") }), 5L);
}

void EvalErrors()
{
    Raises(() => db.ScriptEvaluate("return redis.call('NOSUCH')"), "unknown command");
    Raises(() => db.ScriptEvaluate("return redis.error_reply('boom')"), "boom");
    Raises(() => db.ScriptEvaluate("this is not lua"), "ERR");
    Raises(() => db.ScriptEvaluate("error('thrown')"), "thrown");
    db.Ping();
}

void ScriptHelperEvalsha()
{
    // LuaScript.Prepare + Load(server) sends SCRIPT LOAD; then run by SHA1 (EVALSHA).
    var loaded = LuaScript.Prepare("return ARGV[1]").Load(server);
    Eq((byte[]?)db.ScriptEvaluate(loaded.Hash, null, new RedisValue[] { "x" }), B("x"));
    Eq((byte[]?)db.ScriptEvaluate(loaded.Hash, null, new RedisValue[] { "y" }), B("y"));
}

void PipelinePlain10000()
{
    db.KeyDelete(K("pn"));
    var batch = db.CreateBatch();
    var tasks = Enumerable.Range(0, 10000).Select(_ => batch.StringIncrementAsync(K("pn"))).ToArray();
    batch.Execute();
    Task.WaitAll(tasks);
    Eq(tasks.Select(t => t.Result), Enumerable.Range(1, 10000).Select(i => (long)i));
}

void PipelineWithErrorInMiddle()
{
    db.StringSet(K("pe"), "abc");
    var batch = db.CreateBatch();
    var t0 = batch.StringSetAsync(K("pa"), "1");
    var t1 = batch.StringIncrementAsync(K("pe"));
    var t2 = batch.StringGetAsync(K("pa"));
    batch.Execute();
    try { Task.WaitAll(t0, t1, t2); } catch (AggregateException) { }
    Eq(t0.Result, true);
    if (t1.Exception?.InnerException is not RedisServerException) throw new Exception($"t1 status {t1.Status}, not a RedisServerException");
    Eq(t2.Result, B("1"));
}

void XPipelineTransactionMultiExec()
{
    var tx = db.CreateTransaction();
    var a = tx.StringSetAsync(K("tx"), "1");
    var b = tx.StringGetAsync(K("tx"));
    if (!tx.Execute()) throw new Exception("transaction not committed");
    Eq(new object?[] { a.Result, b.Result }, new object?[] { true, (RedisValue)"1" });
}

void XSetWithExpiryAndNx()
{
    Eq(db.StringSet(K("ex"), "v", TimeSpan.FromSeconds(60)), true);
    var ttl = db.KeyTimeToLive(K("ex"));
    if (!(ttl > TimeSpan.Zero && ttl <= TimeSpan.FromSeconds(60))) throw new Exception($"ttl {ttl}");
    Eq(db.StringSet(K("ex"), "other", null, When.NotExists), false);
}

void Concurrency16Threads()
{
    db.KeyDelete(K("c"));
    var errors = new System.Collections.Concurrent.ConcurrentBag<Exception>();
    var ts = Enumerable.Range(0, 16).Select(_ => new Thread(() =>
    {
        try
        {
            using var m = ConnectionMultiplexer.Connect(Options());
            var d = m.GetDatabase();
            for (int i = 0; i < 500; i++) d.StringIncrement(K("c"));
        }
        catch (Exception e) { errors.Add(e); }
    })).ToList();
    ts.ForEach(t => t.Start());
    ts.ForEach(t => t.Join());
    Eq(errors.Select(e => e.GetType().Name + ": " + e.Message), Array.Empty<string>());
    Eq(db.StringGet(K("c")), B("8000"));
}

void UnknownCommandKeepsConnection()
{
    Raises(() => db.Execute("NOSUCHCMD", "a"), "unknown command");
    Raises(() => db.Execute("GET"), "wrong number of arguments");
    db.Ping();
}

void Hgetall100kFields()
{
    db.KeyDelete(K("bigh"));
    var batch = db.CreateBatch();
    var tasks = new List<Task<RedisResult>>();
    for (int i = 0; i < 100000; i += 1000)
    {
        var args = new List<object> { K("bigh") };
        for (int j = i; j < i + 1000; j++) { args.Add($"f{j}"); args.Add($"v{j}"); }
        tasks.Add(batch.ExecuteAsync("HSET", args.ToArray()));
    }
    batch.Execute();
    Task.WaitAll(tasks.ToArray());
    Eq(tasks.Sum(t => (long)t.Result), 100000L);
    var got = db.HashGetAll(K("bigh"));
    Eq(got.Length, 100000);
    Eq((string?)got.First(e => e.Name == "f99999").Value, "v99999");
    Eq(db.KeyDelete(K("bigh")), true);
}

void ClientSideCommands()
{
    Eq((string?)db.Execute("CLIENT", "SETNAME", "cs-test"), "OK");
    if (server.Info() == null) throw new Exception("INFO returned null");
    Eq((string?)db.Execute("SELECT", "0"), "OK");
    Raises(() => db.Execute("SELECT", "16"), "out of range");
}

void FlushallDbsize()
{
    db.StringSet(K("last"), "1");
    if (!(server.DatabaseSize() >= 1)) throw new Exception("dbsize < 1");
    server.FlushAllDatabases();
    Eq(server.DatabaseSize(), 0L);
    Eq(db.StringGet(K("last")), null);
}

string[] Sv(IEnumerable<RedisValue> xs) => xs.Select(x => (string?)x ?? "<null>").ToArray();
string[] Sorted(IEnumerable<RedisValue> xs) => Sv(xs).Order(StringComparer.Ordinal).ToArray();
string Num(double d) => d.ToString("R", System.Globalization.CultureInfo.InvariantCulture);
string[] Ze(IEnumerable<SortedSetEntry> xs) => xs.Select(e => $"{e.Element}={Num(e.Score)}").ToArray();
SortedSetEntry Z(string member, double score) => new(member, score);
RedisKey[] Ks(params string[] names) => names.Select(n => (RedisKey)(P + n)).ToArray();
void Clear(params string[] names) => db.KeyDelete(Ks(names));
Dictionary<string, double> Scores(string key) =>
    db.SortedSetScan(K(key), pageSize: 1000).ToDictionary(e => (string)e.Element!, e => e.Score);

void Lists()
{
    var k = K("l"); var k2 = K("l2");
    Clear("l", "l2");
    Eq(db.ListRightPush(k, new RedisValue[] { "a", "b", "c" }), 3L);
    Eq(db.ListLeftPush(k, new RedisValue[] { "z", "y" }), 5L); // y z a b c
    Eq(db.ListLength(k), 5L);
    Eq(Sv(db.ListRange(k, 0, -1)), new[] { "y", "z", "a", "b", "c" });
    Eq(Sv(db.ListRange(k, -2, -1)), new[] { "b", "c" });
    Eq(db.ListRange(k, 10, 20).Length, 0);
    Eq((string?)db.ListGetByIndex(k, 0), "y");
    Eq((string?)db.ListGetByIndex(k, -1), "c");
    Eq(db.ListGetByIndex(k, 99).IsNull, true);
    db.ListSetByIndex(k, 1, "Z");
    Raises(() => { db.ListSetByIndex(k, 99, "x"); return null; }, "out of range");
    Eq((string?)db.ListLeftPop(k), "y");
    Eq((string?)db.ListRightPop(k), "c");
    Eq(Sv(db.ListLeftPop(k, 2)), new[] { "Z", "a" });
    Eq(Sv(db.ListRightPop(k, 5)), new[] { "b" });
    Eq(db.ListLeftPop(k).IsNull, true);
    Eq(db.ListLeftPop(k, 2).Length, 0);
    Eq(db.KeyExists(k), false);
    db.ListRightPush(k, new RedisValue[] { "a", "b", "a", "c", "a" });
    Eq(db.ListRemove(k, "a", 2), 2L);
    Eq(Sv(db.ListRange(k, 0, -1)), new[] { "b", "c", "a" });
    Eq(db.ListRemove(k, "a", -1), 1L);
    Eq(db.ListRemove(k, "nope", 0), 0L);
    db.ListRightPush(k, new RedisValue[] { "d", "e", "f" }); // b c d e f
    db.ListTrim(k, 1, -2);
    Eq(Sv(db.ListRange(k, 0, -1)), new[] { "c", "d", "e" });
    Eq(db.ListInsertBefore(k, "d", "cd"), 4L);
    Eq(db.ListInsertAfter(k, "e", "ef"), 5L);
    Eq(db.ListInsertBefore(k, "missing", "x"), -1L);
    Eq(db.ListInsertBefore(K("nolist"), "a", "x"), 0L);
    Eq(Sv(db.ListRange(k, 0, -1)), new[] { "c", "cd", "d", "e", "ef" });
    db.ListRightPush(k, "d");
    Eq(db.ListPosition(k, "d"), 2L);
    Eq(db.ListPosition(k, "d", 2), 5L);
    Eq(db.ListPositions(k, "d", 0), new long[] { 2, 5 });
    Eq(db.ListPosition(k, "nope"), -1L);
    Eq((string?)db.ListMove(k, k2, ListSide.Left, ListSide.Right), "c");
    Eq((string?)db.ListMove(k, k2, ListSide.Right, ListSide.Left), "d");
    Eq(Sv(db.ListRange(k2, 0, -1)), new[] { "d", "c" });
    Eq((string?)db.ListRightPopLeftPush(k, k2), "ef");
    Eq(Sv(db.ListRange(k2, 0, -1)), new[] { "ef", "d", "c" });
    Eq(db.ListMove(K("nolist"), k2, ListSide.Left, ListSide.Left).IsNull, true);
    Eq(db.KeyType(k), RedisType.List);
    Raises(() => db.StringGet(k), "WRONGTYPE");
    db.StringSet(K("lstr"), "x");
    Raises(() => db.ListLeftPush(K("lstr"), "x"), "WRONGTYPE");
    Eq(db.KeyDelete(Ks("l", "l2", "lstr")), 3L);
}

void Sets()
{
    RedisKey a = K("seta"), b = K("setb"), c = K("setc"), d = K("setd");
    Clear("seta", "setb", "setc", "setd");
    Eq(db.SetAdd(a, new RedisValue[] { "1", "2", "3", "4", "4" }), 4L);
    Eq(db.SetAdd(a, new RedisValue[] { "1" }), 0L);
    Eq(db.SetLength(a), 4L);
    Eq(db.SetContains(a, "2"), true);
    Eq(db.SetContains(a, "9"), false);
    Eq(db.SetContains(a, new RedisValue[] { "1", "9", "4" }), new[] { true, false, true });
    Eq(Sorted(db.SetMembers(a)), new[] { "1", "2", "3", "4" });
    Eq(db.SetRemove(a, new RedisValue[] { "4", "9" }), 1L);
    db.SetAdd(b, new RedisValue[] { "2", "3", "5" });
    db.SetAdd(c, new RedisValue[] { "3", "6" });
    Eq(Sorted(db.SetCombine(SetOperation.Intersect, a, b)), new[] { "2", "3" });
    Eq(Sorted(db.SetCombine(SetOperation.Intersect, new[] { a, b, c })), new[] { "3" });
    Eq(Sorted(db.SetCombine(SetOperation.Union, new[] { a, b, c })), new[] { "1", "2", "3", "5", "6" });
    Eq(Sorted(db.SetCombine(SetOperation.Difference, a, b)), new[] { "1" });
    Eq(Sorted(db.SetCombine(SetOperation.Difference, new[] { a, b, c })), new[] { "1" });
    Eq(db.SetCombineAndStore(SetOperation.Intersect, d, a, b), 2L);
    Eq(Sorted(db.SetMembers(d)), new[] { "2", "3" });
    Eq(db.SetCombineAndStore(SetOperation.Union, d, a, b), 4L);
    Eq(db.SetCombineAndStore(SetOperation.Difference, d, b, a), 1L);
    Eq(Sorted(db.SetMembers(d)), new[] { "5" });
    Eq(db.SetIntersectionLength(new[] { a, b }), 2L);
    Eq(db.SetIntersectionLength(new[] { a, b }, 1), 1L);
    Eq(db.SetMove(a, c, "1"), true);
    Eq(db.SetMove(a, c, "nope"), false);
    Eq(Sorted(db.SetMembers(c)), new[] { "1", "3", "6" });
    var one = (string?)db.SetRandomMember(c);
    if (one is not ("1" or "3" or "6")) throw new Exception($"srandmember outside set: {one}");
    Eq(db.SetRandomMembers(c, 2).Distinct().Count(), 2);
    Eq(db.SetRandomMembers(c, 10).Length, 3);
    Eq(db.SetRandomMembers(c, -5).Length, 5);
    Eq(db.SetRandomMember(K("noset")).IsNull, true);
    var popped = (string?)db.SetPop(c);
    if (popped is not ("1" or "3" or "6")) throw new Exception($"spop outside set: {popped}");
    Eq(db.SetPop(c, 5).Length, 2);
    Eq(db.KeyExists(c), false);
    Eq(db.SetPop(c).IsNull, true);
    var big = K("setbig");
    db.KeyDelete(big);
    db.SetAdd(big, Enumerable.Range(0, 300).Select(i => (RedisValue)$"m{i}").ToArray());
    Eq(db.SetScan(big, pageSize: 50).Select(x => (string)x!).Distinct().Count(), 300);
    Eq(db.KeyType(big), RedisType.Set);
    Raises(() => db.StringGet(big), "WRONGTYPE");
    Clear("setbig", "seta", "setb", "setc", "setd");
}

void ZsetsBasic()
{
    // membership and order only; score replies are checked in ZsetScores
    var k = K("z");
    db.KeyDelete(k);
    Eq(db.SortedSetAdd(k, new[] { Z("a", 1), Z("b", 2.5), Z("c", 1.5) }), 3L); // a c b
    Eq(db.SortedSetAdd(k, new[] { Z("a", 1) }), 0L);
    Eq(db.SortedSetUpdate(k, "a", 5), true); // SortedSetUpdate reports a changed score (CH)
    Eq(db.SortedSetAdd(k, "d", 7), true); // c b a d
    Eq(db.SortedSetAdd(k, "a", 9, SortedSetWhen.NotExists), false);
    Eq(Sv(db.SortedSetRangeByRank(k, 0, -1)), new[] { "c", "b", "a", "d" });
    Eq(db.SortedSetAdd(k, "zz", 1, SortedSetWhen.Exists), false);
    Eq(db.SortedSetLength(k), 4L);
    Eq(db.SortedSetUpdate(k, "a", 3, SortedSetWhen.GreaterThan), false);
    Eq(Sv(db.SortedSetRangeByRank(k, 0, -1)), new[] { "c", "b", "a", "d" }); // GT 3 < 5 keeps 5
    Eq(db.SortedSetUpdate(k, "a", 8, SortedSetWhen.GreaterThan), true); // c b d a
    Eq(Sv(db.SortedSetRangeByRank(k, 0, -1)), new[] { "c", "b", "d", "a" });
    Eq(db.SortedSetUpdate(k, "a", 10, SortedSetWhen.LessThan), false);
    Eq(db.SortedSetUpdate(k, "a", 6, SortedSetWhen.LessThan), true); // c b a d
    Eq(Sv(db.SortedSetRangeByRank(k, 0, -1)), new[] { "c", "b", "a", "d" });
    Eq(db.SortedSetRank(k, "c"), 0L);
    Eq(db.SortedSetRank(k, "d"), 3L);
    Eq(db.SortedSetRank(k, "d", Order.Descending), 0L);
    Eq(db.SortedSetRank(k, "c", Order.Descending), 3L);
    Eq(db.SortedSetRank(k, "nope"), null);
    Eq(db.SortedSetRank(k, "nope", Order.Descending), null);
    Eq(db.SortedSetLength(k, 2, 6), 2L);
    Eq(db.SortedSetLength(k, 2.5, 7, Exclude.Start), 2L);
    Eq(db.SortedSetLength(k, 1.5, 7, Exclude.Both), 2L);
    Eq(db.SortedSetLength(k, double.NegativeInfinity, double.PositiveInfinity), 4L);
    Eq(db.SortedSetLength(k, 100, 200), 0L);
    Eq(db.SortedSetRemove(k, new RedisValue[] { "a", "nope" }), 1L);
    Eq(Sv(db.SortedSetRangeByRank(k, 0, -1)), new[] { "c", "b", "d" });
    Eq(db.KeyType(k), RedisType.SortedSet);
    Raises(() => db.StringGet(k), "WRONGTYPE");
    Raises(() => db.Execute("ZADD", k, "notanumber", "a"), "not a valid float");
    Raises(() => db.Execute("ZADD", k, "NX", "XX", "1", "a"), "not compatible");
    Eq(db.KeyDelete(k), true);
}

void ZsetScores()
{
    var k = K("zsc");
    double inf = double.PositiveInfinity, ninf = double.NegativeInfinity;
    db.KeyDelete(k);
    db.SortedSetAdd(k, new[] { Z("a", 1), Z("b", 2.5), Z("c", 1.5) });
    Eq(db.SortedSetScore(k, "a"), 1.0);
    Eq(db.SortedSetScore(k, "c"), 1.5);
    Eq(db.SortedSetScore(k, "nope"), null);
    Eq(db.SortedSetScores(k, new RedisValue[] { "a", "nope", "c" }), new double?[] { 1.0, null, 1.5 });
    Eq(db.SortedSetIncrement(k, "a", 4.0), 5.0);
    Eq(db.SortedSetIncrement(k, "a", 0.25), 5.25);
    Eq(db.SortedSetIncrement(k, "a", -0.25), 5.0);
    Eq(db.SortedSetIncrement(k, "c", 0.5), 2.0);
    Eq(db.SortedSetIncrement(k, "new", 1), 1.0); // new c b a
    Eq(Ze(db.SortedSetRangeByRankWithScores(k, 0, -1)), new[] { "new=1", "c=2", "b=2.5", "a=5" });
    Eq(Ze(db.SortedSetRangeByRankWithScores(k, 0, 1, Order.Descending)), new[] { "a=5", "b=2.5" });
    Eq(Ze(db.SortedSetRangeByScoreWithScores(k, 2, 3)), new[] { "c=2", "b=2.5" });
    Eq(Ze(db.SortedSetRangeByScoreWithScores(k, 2.5, 5, order: Order.Descending)), new[] { "a=5", "b=2.5" });
    Eq(Ze(db.SortedSetRangeByScoreWithScores(k, 2, 5, skip: 1, take: 1)), new[] { "b=2.5" });
    db.KeyDelete(k);
    db.SortedSetAdd(k, new[] { Z("lo", ninf), Z("hi", inf), Z("mid", 1.5), Z("frac", -0.1) });
    Eq(Sv(db.SortedSetRangeByRank(k, 0, -1)), new[] { "lo", "frac", "mid", "hi" });
    Eq(Ze(db.SortedSetRangeByRankWithScores(k, 0, -1)),
        new[] { $"lo={Num(ninf)}", "frac=-0.1", "mid=1.5", $"hi={Num(inf)}" });
    Eq(db.SortedSetScore(k, "hi"), inf);
    Eq(db.SortedSetScore(k, "lo"), ninf);
    Eq(db.SortedSetScores(k, new RedisValue[] { "lo", "hi" }), new double?[] { ninf, inf });
    Eq(Sv(db.SortedSetRangeByScore(k, ninf, inf)), new[] { "lo", "frac", "mid", "hi" });
    Eq(Sv(db.SortedSetRangeByScore(k, ninf, inf, Exclude.Both)), new[] { "frac", "mid" });
    Eq(db.SortedSetLength(k, ninf, 0), 2L);
    Eq(db.SortedSetIncrement(k, "hi", 5), inf);
    Eq(Scores("zsc").OrderBy(e => e.Key).Select(e => $"{e.Key}={Num(e.Value)}"),
        new[] { "frac=-0.1", $"hi={Num(inf)}", $"lo={Num(ninf)}", "mid=1.5" });
    db.KeyDelete(k);
}

void ZsetPop()
{
    var k = K("zp");
    db.KeyDelete(k);
    db.SortedSetAdd(k, new[] { Z("a", 1), Z("b", 2), Z("c", 3), Z("d", 4.5), Z("e", 5) });
    Eq(Ze(new[] { db.SortedSetPop(k, Order.Ascending)!.Value }), new[] { "a=1" });
    Eq(Ze(new[] { db.SortedSetPop(k, Order.Descending)!.Value }), new[] { "e=5" });
    Eq(Ze(db.SortedSetPop(k, 2, Order.Ascending)), new[] { "b=2", "c=3" });
    Eq(Ze(db.SortedSetPop(k, 5, Order.Descending)), new[] { "d=4.5" });
    Eq(db.SortedSetPop(k, Order.Ascending), null);
    Eq(db.SortedSetPop(k, 3, Order.Descending).Length, 0);
    Eq(db.KeyExists(k), false);
}

void ZsetsRanges()
{
    var k = K("zr");
    db.KeyDelete(k);
    db.SortedSetAdd(k, new[] { Z("a", 1), Z("b", 2), Z("c", 3), Z("d", 4), Z("e", 5) });
    Eq(Sv(db.SortedSetRangeByRank(k, 0, 1)), new[] { "a", "b" });
    Eq(Sv(db.SortedSetRangeByRank(k, -2, -1)), new[] { "d", "e" });
    Eq(Sv(db.SortedSetRangeByRank(k, 0, 1, Order.Descending)), new[] { "e", "d" });
    Eq(Sv(db.SortedSetRangeByScore(k, 2, 3)), new[] { "b", "c" });
    Eq(Sv(db.SortedSetRangeByScore(k, 1, double.PositiveInfinity, Exclude.Start, Order.Ascending, 1, 2)), new[] { "c", "d" });
    Eq(Sv(db.SortedSetRangeByScore(k, 2, double.PositiveInfinity, Exclude.Start, Order.Descending, 0, 2)), new[] { "e", "d" });
    Eq(Sv(db.SortedSetRangeByScore(k, double.NegativeInfinity, 3)), new[] { "a", "b", "c" });
    Eq(Sv(db.SortedSetRangeByScore(k, 1, 4, Exclude.Both)), new[] { "b", "c" });
    Eq(Sv(db.SortedSetRangeByScore(k, 1, 5, Exclude.None, Order.Ascending, 1, 2)), new[] { "b", "c" });
    Eq(Sv(db.SortedSetRangeByScore(k, 4, 5, order: Order.Descending)), new[] { "e", "d" });
    Eq(Sv(db.SortedSetRangeByScore(k, double.NegativeInfinity, double.PositiveInfinity, Exclude.None, Order.Descending, 0, 1)), new[] { "e" });
    Eq(db.SortedSetRangeByRank(k, 10, 20).Length, 0);
    Eq(db.SortedSetRangeByRank(K("nozset"), 0, -1).Length, 0);
    Eq(db.SortedSetRemoveRangeByRank(k, 0, 1), 2L); // c d e
    Eq(db.SortedSetRemoveRangeByScore(k, 3, 4, Exclude.Start), 1L); // c e
    Eq(Sv(db.SortedSetRangeByRank(k, 0, -1)), new[] { "c", "e" });
    Eq(db.SortedSetRemoveRangeByScore(k, double.NegativeInfinity, double.PositiveInfinity), 2L);
    Eq(db.KeyExists(k), false);
    var big = K("zbig");
    db.KeyDelete(big);
    db.SortedSetAdd(big, Enumerable.Range(0, 300).Select(i => Z($"m{i}", i)).ToArray());
    var seen = db.SortedSetScan(big, pageSize: 50).ToDictionary(e => (string)e.Element!, e => e.Score);
    Eq(seen.Count, 300);
    Eq(seen["m299"], 299.0);
    db.KeyDelete(big);
}

void ZsetsStore()
{
    RedisKey a = K("zsa"), c = K("zsb"), d = K("zsd");
    Clear("zsa", "zsb", "zsd", "zss");
    db.SortedSetAdd(a, new[] { Z("x", 1), Z("y", 2), Z("z", 3) });
    db.SortedSetAdd(c, new[] { Z("y", 10), Z("z", 20), Z("w", 0.5) });
    Eq(db.SortedSetCombineAndStore(SetOperation.Union, d, a, c), 4L);
    Eq(Sv(db.SortedSetRangeByRank(d, 0, -1)), new[] { "w", "x", "y", "z" });
    Eq(db.SortedSetCombineAndStore(SetOperation.Union, d, new[] { a, c }, new double[] { 2, 0.5 }), 4L);
    Eq(Scores("zsd").OrderBy(e => e.Key).Select(e => $"{e.Key}={Num(e.Value)}"), new[] { "w=0.25", "x=2", "y=9", "z=16" });
    Eq(db.SortedSetCombineAndStore(SetOperation.Union, d, new[] { a, c }, null, Aggregate.Max), 4L);
    Eq(Scores("zsd")["z"], 20.0);
    Eq(db.SortedSetCombineAndStore(SetOperation.Intersect, d, a, c), 2L);
    Eq(Scores("zsd").OrderBy(e => e.Key).Select(e => $"{e.Key}={Num(e.Value)}"), new[] { "y=12", "z=23" });
    Eq(db.SortedSetCombineAndStore(SetOperation.Intersect, d, new[] { a, c }, new double[] { 3, 2 }), 2L);
    Eq(Scores("zsd").OrderBy(e => e.Key).Select(e => $"{e.Key}={Num(e.Value)}"), new[] { "y=26", "z=49" });
    Eq(db.SortedSetCombineAndStore(SetOperation.Intersect, d, new[] { a, c }, null, Aggregate.Min), 2L);
    Eq(Scores("zsd")["y"], 2.0);
    db.SetAdd(K("zss"), new RedisValue[] { "y", "q" });
    Eq(db.SortedSetCombineAndStore(SetOperation.Intersect, d, a, K("zss")), 1L); // sets count as score 1
    Eq(Scores("zsd")["y"], 3.0);
    Eq(db.SortedSetCombineAndStore(SetOperation.Intersect, d, a, K("nozset")), 0L);
    Eq(db.KeyExists(d), false);
    Clear("zsa", "zsb", "zsd", "zss");
}

void StringKeyExtras()
{
    var h = K("hx");
    Clear("f", "gr", "grnew", "abc", "hx");
    Eq(db.StringIncrement(K("f"), 1.5), 1.5);
    Eq(db.StringIncrement(K("f"), 2.0), 3.5);
    Eq(db.StringIncrement(K("f"), -0.25), 3.25);
    Eq((string?)db.StringGet(K("f")), "3.25");
    db.StringSet(K("abc"), "abc");
    Raises(() => db.StringIncrement(K("abc"), 1.0), "not a valid float");
    db.StringSet(K("gr"), "Hello World");
    Eq((string?)db.StringGetRange(K("gr"), 0, 4), "Hello");
    Eq((string?)db.StringGetRange(K("gr"), -5, -1), "World");
    Eq((string?)db.StringGetRange(K("gr"), 20, 30), "");
    Eq((long)db.StringSetRange(K("gr"), 6, "Redis"), 11L);
    Eq((string?)db.StringGet(K("gr")), "Hello Redis");
    Eq((long)db.StringSetRange(K("gr"), 13, "!"), 14L);
    Eq(db.StringGet(K("gr")), B("Hello Redis\0\0!"));
    Eq((long)db.StringSetRange(K("grnew"), 2, "x"), 3L);
    Eq(db.StringGet(K("grnew")), B("\0\0x"));
    db.HashSet(h, new[] { new HashEntry("a", "1"), new HashEntry("b", "hello"), new HashEntry("c", "2.5") });
    Eq(Sorted(db.HashKeys(h)), new[] { "a", "b", "c" });
    Eq(Sorted(db.HashValues(h)), new[] { "1", "2.5", "hello" });
    Eq(db.HashStringLength(h, "b"), 5L);
    Eq(db.HashStringLength(h, "nope"), 0L);
    Eq(db.HashIncrement(h, "c", 0.5), 3.0);
    Eq(db.HashIncrement(h, "new", 1.5), 1.5);
    Eq(db.HashIncrement(h, "a", 4L), 5L);
    Raises(() => db.HashIncrement(h, "b", 1.0), "not a float");
    var rf = (string?)db.HashRandomField(h);
    if (rf is not ("a" or "b" or "c" or "new")) throw new Exception($"hrandfield outside hash: {rf}");
    Eq(db.HashRandomFields(h, 2).Length, 2);
    Eq(db.HashRandomFields(h, -6).Length, 6);
    Eq(db.HashRandomField(K("nohash")).IsNull, true);
    Eq(db.HashRandomFields(K("nohash"), 3).Length, 0);
    Eq(db.HashExists(h, "a"), true);
    Eq(Sv(db.HashGet(h, new RedisValue[] { "a", "zz" })), new[] { "5", "<null>" });
    Eq(db.HashSet(h, "a", "x", When.NotExists), false);
    Clear("f", "gr", "grnew", "abc", "hx");
}

void HrandfieldValues()
{
    var h = K("hrf");
    db.KeyDelete(h);
    db.HashSet(h, new[] { new HashEntry("a", "1"), new HashEntry("b", "hello"), new HashEntry("c", "3") });
    var got = db.HashRandomFieldsWithValues(h, 3);
    Eq(got.Length, 3);
    Eq(got.OrderBy(e => (string)e.Name!).Select(e => $"{e.Name}={e.Value}"), new[] { "a=1", "b=hello", "c=3" });
    Eq(db.HashRandomFieldsWithValues(h, -5).Length, 5);
    db.KeyDelete(h);
}

void KeysMisc()
{
    var names = new[] { "r1", "r2", "r3", "cp1", "cp2", "ms1", "ms2", "ms3", "ty:s", "ty:h", "ty:l", "ty:set", "ty:z", "ty:int" };
    Clear(names);
    db.StringSet(K("r1"), "v");
    Eq(db.KeyRename(K("r1"), K("r2")), true);
    Eq(db.KeyExists(Ks("r1", "r2")), 1L);
    Raises(() => db.KeyRename(K("nokey"), K("x")), "no such key");
    db.StringSet(K("r1"), "other");
    Eq(db.KeyRename(K("r1"), K("r2"), When.NotExists), false);
    Eq((string?)db.StringGet(K("r2")), "v");
    Eq(db.KeyRename(K("r1"), K("r3"), When.NotExists), true);
    Eq((string?)db.StringGet(K("r3")), "other");
    db.ListRightPush(K("cp1"), new RedisValue[] { "a", "b" });
    Eq(db.KeyCopy(K("cp1"), K("cp2")), true);
    Eq(db.KeyCopy(K("cp1"), K("cp2")), false);
    Eq(db.KeyCopy(K("cp1"), K("cp2"), replace: true), true);
    Eq(Sv(db.ListRange(K("cp2"), 0, -1)), new[] { "a", "b" });
    db.ListRightPush(K("cp1"), "c");
    Eq(db.ListLength(K("cp2")), 2L); // a copy, not an alias
    Eq(db.KeyCopy(K("nokey"), K("cp3")), false);
    Eq(db.StringSet(new KeyValuePair<RedisKey, RedisValue>[] { new(K("ms1"), "1"), new(K("ms2"), "2") }, When.NotExists), true);
    Eq(db.StringSet(new KeyValuePair<RedisKey, RedisValue>[] { new(K("ms1"), "x"), new(K("ms3"), "3") }, When.NotExists), false);
    Eq(db.KeyExists(K("ms3")), false);
    Eq((string?)db.StringGet(K("ms1")), "1");
    Eq(db.KeyTouch(Ks("r2", "cp1", "nokey")), 2L);
    db.StringSet(K("ty:s"), "x");
    db.HashSet(K("ty:h"), "f", "v");
    db.ListRightPush(K("ty:l"), "x");
    db.SetAdd(K("ty:set"), "x");
    db.SortedSetAdd(K("ty:z"), "x", 1);
    Eq(new[] { "ty:s", "ty:h", "ty:l", "ty:set", "ty:z", "nokey" }.Select(n => db.KeyType(K(n)).ToString()),
        new[] { "String", "Hash", "List", "Set", "SortedSet", "None" });
    foreach (var n in new[] { "ty:s", "ty:h", "ty:l", "ty:set", "ty:z" })
    {
        var enc = db.KeyEncoding(K(n));
        if (string.IsNullOrEmpty(enc)) throw new Exception($"encoding of {n}: {enc}");
    }
    Eq(db.KeyEncoding(K("nokey")), null);
    var se = db.KeyEncoding(K("ty:s"));
    if (se is not ("embstr" or "raw")) throw new Exception($"string encoding {se}");
    db.StringSet(K("ty:int"), "12345");
    Eq(db.KeyEncoding(K("ty:int")), "int");
    // IServer.Keys has no TYPE filter, so SCAN ... TYPE goes through Execute
    List<string> ScanType(string type)
    {
        var got = new HashSet<string>();
        string cursor = "0";
        for (int i = 0; i < 10000; i++)
        {
            var res = (RedisResult[])db.Execute("SCAN", cursor, "MATCH", P + "ty:*", "COUNT", "100", "TYPE", type)!;
            cursor = (string)res[0]!;
            foreach (var key in (string[])res[1]!) got.Add(key);
            if (cursor == "0") break;
        }
        return got.Order(StringComparer.Ordinal).ToList();
    }
    Eq(ScanType("string"), new[] { P + "ty:int", P + "ty:s" });
    Eq(ScanType("hash"), new[] { P + "ty:h" });
    Eq(ScanType("list"), new[] { P + "ty:l" });
    Eq(ScanType("set"), new[] { P + "ty:set" });
    Eq(ScanType("zset"), new[] { P + "ty:z" });
    var rk = db.KeyRandom();
    if (((string?)rk) is null || !db.KeyExists(rk)) throw new Exception($"randomkey {rk}");
    Clear(names);
}

void PubsubRoundtrip()
{
    var sub = mux!.GetSubscriber();
    var q = new System.Collections.Concurrent.BlockingCollection<(string Kind, string Channel, string Message)>();
    string ch = P + "chan", ch2 = P + "chan2", pat = P + "pat:*", both = P + "pat:both";
    (string Kind, string Channel, string Message) Next()
    {
        if (!q.TryTake(out var m, 5000)) throw new Exception("no pub/sub message within 5000 ms");
        return m;
    }
    var onMessage = new Action<RedisChannel, RedisValue>((c, v) => q.Add(("message", c.ToString(), v.ToString())));
    var onPattern = new Action<RedisChannel, RedisValue>((c, v) => q.Add(("pmessage", c.ToString(), v.ToString())));
    try
    {
        sub.Subscribe(RedisChannel.Literal(ch), onMessage);
        sub.Subscribe(RedisChannel.Pattern(pat), onPattern);
        Eq(sub.Publish(RedisChannel.Literal(ch), "hello"), 1L);
        Eq(Next(), ("message", ch, "hello"));
        Eq(sub.Publish(RedisChannel.Literal(P + "pat:one"), "p1"), 1L);
        Eq(Next(), ("pmessage", P + "pat:one", "p1"));
        Eq(sub.Publish(RedisChannel.Literal(P + "other"), "x"), 0L);
        Eq(sub.Publish(RedisChannel.Literal(ch2), "x"), 0L);
        sub.Subscribe(RedisChannel.Literal(both), onMessage);
        sub.Subscribe(RedisChannel.Pattern(P + "pat:b*"), onPattern);
        Eq(sub.Publish(RedisChannel.Literal(both), "twice"), 3L);
        var got = new[] { Next(), Next(), Next() };
        Eq(got.Select(m => m.Kind).Order().ToArray(), new[] { "message", "pmessage", "pmessage" });
        Eq(got.All(m => m.Message == "twice"), true);
        Eq(server.SubscriptionSubscriberCount(RedisChannel.Literal(ch)), 1L);
        Eq(server.SubscriptionSubscriberCount(RedisChannel.Literal(ch2)), 0L);
        Eq(server.SubscriptionPatternCount(), 2L);
        Eq(server.SubscriptionChannels(RedisChannel.Pattern(P + "*")).Select(c => c.ToString()).Order(StringComparer.Ordinal).ToArray(),
            new[] { ch, both }.Order(StringComparer.Ordinal).ToArray());
        sub.Unsubscribe(RedisChannel.Pattern(P + "pat:b*"), onPattern);
        sub.Unsubscribe(RedisChannel.Literal(ch), onMessage);
        Eq(sub.Publish(RedisChannel.Literal(ch), "gone"), 0L);
        sub.Unsubscribe(RedisChannel.Pattern(pat), onPattern);
        Eq(sub.Publish(RedisChannel.Literal(P + "pat:one"), "gone"), 0L);
        sub.UnsubscribeAll();
        Eq(server.SubscriptionSubscriberCount(RedisChannel.Literal(both)), 0L);
        Eq(q.Count, 0);
    }
    finally { sub.UnsubscribeAll(); }
}

void TxNewTypes()
{
    Clear("txl", "txz", "txs", "w", "txstr");
    var tx = db.CreateTransaction();
    var rp = tx.ListRightPushAsync(K("txl"), new RedisValue[] { "a", "b" });
    var zp = tx.SortedSetAddAsync(K("txz"), new[] { Z("m", 1.5), Z("n", 2) });
    var sp = tx.SetAddAsync(K("txs"), new RedisValue[] { "x", "y" });
    var lr = tx.ListRangeAsync(K("txl"), 0, -1);
    var zr = tx.SortedSetRangeByRankAsync(K("txz"), 0, -1);
    var sc = tx.SetLengthAsync(K("txs"));
    if (!tx.Execute()) throw new Exception("transaction not committed");
    Eq(new[] { rp.Result, zp.Result, sp.Result, sc.Result }, new[] { 2L, 2L, 2L, 2L });
    Eq(Sv(lr.Result), new[] { "a", "b" });
    Eq(Sv(zr.Result), new[] { "m", "n" });
    tx = db.CreateTransaction();
    var lp = tx.ListLeftPopAsync(K("txl"));
    var zrm = tx.SortedSetRemoveAsync(K("txz"), "m");
    var spp = tx.SetPopAsync(K("txs"), 2);
    if (!tx.Execute()) throw new Exception("transaction not committed");
    Eq((string?)lp.Result, "a");
    Eq(zrm.Result, true);
    Eq(Sorted(spp.Result), new[] { "x", "y" });
    db.StringSet(K("txstr"), "s");
    // an error inside EXEC does not roll back the other commands
    tx = db.CreateTransaction();
    var c0 = tx.ListRightPushAsync(K("txl"), "q");
    var c1 = tx.ListLeftPushAsync(K("txstr"), "bad");
    var c2 = tx.ListLengthAsync(K("txl"));
    tx.Execute();
    try { Task.WaitAll(c0, c1, c2); } catch (AggregateException) { }
    Eq(c0.Result, 2L);
    if (c1.Exception?.InnerException is not RedisServerException e1 || !e1.Message.Contains("WRONGTYPE"))
        throw new Exception($"c1 status {c1.Status}: {c1.Exception?.InnerException?.Message}");
    Eq(c2.Result, 2L);
    // a failed condition (WATCH + compare) aborts EXEC
    db.StringSet(K("w"), "2");
    tx = db.CreateTransaction();
    tx.AddCondition(Condition.StringEqual(K("w"), "1"));
    var za = tx.SortedSetAddAsync(K("txz"), "w", 1);
    Eq(tx.Execute(), false);
    Eq(db.SortedSetScore(K("txz"), "w"), null);
    Clear("txl", "txz", "txs", "w", "txstr");
}

void LuaNewTypes()
{
    RedisKey z = K("lz"), l = K("ll"), s = K("ls");
    Clear("lz", "ll", "ls");
    var script = @"
redis.call('ZADD', KEYS[1], 1.5, 'a', 2, 'b', 3, 'c')
redis.call('RPUSH', KEYS[2], 'x', 'y', 'z')
redis.call('SADD', KEYS[3], 'm', 'n')
return {redis.call('ZRANGE', KEYS[1], 0, -1), redis.call('LRANGE', KEYS[2], 0, -1),
        redis.call('SCARD', KEYS[3]), redis.call('ZSCORE', KEYS[1], 'a'),
        redis.call('ZRANGE', KEYS[1], 0, 0, 'WITHSCORES'), redis.call('TYPE', KEYS[2])['ok']}";
    string[] Strs(RedisResult r) => ((RedisResult[])r!).Select(x => (string)x!).ToArray();
    var arr = (RedisResult[])db.ScriptEvaluate(script, new[] { z, l, s })!;
    Eq(arr.Length, 6);
    Eq(Strs(arr[0]), new[] { "a", "b", "c" });
    Eq(Strs(arr[1]), new[] { "x", "y", "z" });
    Eq((long)arr[2], 2L);
    Eq((string?)arr[3], "1.5");
    Eq(Strs(arr[4]), new[] { "a", "1.5" });
    Eq((string?)arr[5], "list");
    Eq(Sv(db.SortedSetRangeByRank(z, 0, -1)), new[] { "a", "b", "c" });
    Eq(Strs(db.ScriptEvaluate("return redis.call('ZPOPMIN', KEYS[1])", new[] { z })), new[] { "a", "1.5" });
    Eq(Strs(db.ScriptEvaluate("return redis.call('LPOP', KEYS[1], 2)", new[] { l })), new[] { "x", "y" });
    Eq(Strs(db.ScriptEvaluate("return redis.call('SMEMBERS', KEYS[1])", new[] { s })).Order(StringComparer.Ordinal).ToArray(), new[] { "m", "n" });
    Eq((string?)db.ScriptEvaluate("return redis.call('ZINCRBY', KEYS[1], 0.5, 'b')", new[] { z }), "2.5");
    Eq((long)db.ScriptEvaluate("return redis.call('EXISTS', KEYS[1])", new[] { K("none") }), 0L);
    Raises(() => db.ScriptEvaluate("return redis.call('LPUSH', KEYS[1], 'x')", new[] { z }), "WRONGTYPE");
    Eq((long)db.ScriptEvaluate("return redis.pcall('LPUSH', KEYS[1], 'x')['err'] ~= nil", new[] { z }), 1L);
    var loaded = LuaScript.Prepare("return redis.call('ZRANGEBYSCORE', KEYS[1], ARGV[1], ARGV[2], 'WITHSCORES')").Load(server);
    Eq(Strs(db.ScriptEvaluate(loaded.Hash, new[] { z }, new RedisValue[] { "-inf", "+inf" })), new[] { "b", "2.5", "c", "3" });
    Clear("lz", "ll", "ls");
}

void ConfigInfo()
{
    var mm = server.ConfigGet("maxmemory");
    var mmv = mm.FirstOrDefault(e => e.Key == "maxmemory").Value;
    if (mmv == null || !long.TryParse(mmv, out _)) throw new Exception($"CONFIG GET maxmemory: {Show(mm.Select(e => e.Key + "=" + e.Value))}");
    var pol = server.ConfigGet("maxmemory-policy").FirstOrDefault(e => e.Key == "maxmemory-policy").Value;
    if (string.IsNullOrEmpty(pol)) throw new Exception("CONFIG GET maxmemory-policy returned nothing");
    var both = server.ConfigGet("maxmemory*").Select(e => e.Key).ToHashSet();
    if (!(both.Contains("maxmemory") && both.Contains("maxmemory-policy"))) throw new Exception($"maxmemory*: {string.Join(",", both)}");
    var info = server.Info();
    if (info.Length == 0 || !info.SelectMany(g => g).Any()) throw new Exception("INFO parsed to no key:value pairs");
    if (server.Info("memory") == null || server.Info("keyspace") == null) throw new Exception("INFO section returned null");
}

var checks = new (string, Action)[]
{
    ("connect_ping", ConnectPing), ("set_get", SetGet), ("binary_key_and_value", BinaryKeyAndValue),
    ("big_value_8mib", Big8Mib), ("counters", Counters), ("counter_errors", CounterErrors),
    ("hashes", Hashes), ("wrong_type", WrongType), ("mset_mget", MsetMget),
    ("del_exists_unlink", DelExistsUnlink), ("keys_pattern", KeysPattern), ("eval_values", EvalValues),
    ("eval_calls_redis", EvalCallsRedis), ("eval_errors", EvalErrors),
    ("script_helper_evalsha", ScriptHelperEvalsha), ("pipeline_plain_10000", PipelinePlain10000),
    ("pipeline_with_error_in_middle", PipelineWithErrorInMiddle),
    ("x_pipeline_transaction_multi_exec", XPipelineTransactionMultiExec),
    ("x_set_with_expiry_and_nx", XSetWithExpiryAndNx), ("concurrency_16_threads", Concurrency16Threads),
    ("unknown_command_keeps_connection", UnknownCommandKeepsConnection),
    ("hgetall_100k_fields", Hgetall100kFields), ("client_side_commands", ClientSideCommands),
    ("lists", Lists), ("sets", Sets), ("zsets_basic", ZsetsBasic), ("zset_scores", ZsetScores),
    ("zset_pop", ZsetPop), ("zsets_ranges", ZsetsRanges), ("zsets_store", ZsetsStore),
    ("string_key_extras", StringKeyExtras), ("hrandfield_values", HrandfieldValues),
    ("keys_misc", KeysMisc), ("pubsub_roundtrip", PubsubRoundtrip), ("tx_new_types", TxNewTypes),
    ("lua_new_types", LuaNewTypes), ("config_info", ConfigInfo),
    ("flushall_dbsize", FlushallDbsize),
};

if (connectError != null)
{
    Console.WriteLine($"CONNECT FAILED: {connectError}");
    foreach (var (name, _) in checks)
    {
        failed.Add(name);
        Console.WriteLine($"FAIL {name}: connect failed: {Cut(connectError, 300)}");
    }
}
else
{
    foreach (var (name, fn) in checks) Check(name, fn);
}
Console.WriteLine($"== dotnet: {checks.Length - failed.Count}/{checks.Length} passed; failed: [{string.Join(", ", failed)}]");
mux?.Dispose();
return failed.Count > 0 ? 1 : 0;
