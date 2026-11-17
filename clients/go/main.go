// HCache spike against go-redis. Every check runs; nothing aborts the run.
//
// Checks named x_* use Redis features outside the spike's scope; they show
// what a real application would hit, they are not expected to pass.
package main

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"math"
	"os"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	P       = "go:"
	timeout = 30 * time.Second
)

var (
	ctx    = context.Background()
	port   = envOr("HCACHE_PORT", "6400")
	proto  = os.Getenv("HCACHE_PROTO")
	failed []string
	r      *redis.Client
)

func envOr(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}

func newClient() *redis.Client {
	o := &redis.Options{
		Addr:         "127.0.0.1:" + port,
		DialTimeout:  timeout,
		ReadTimeout:  timeout,
		WriteTimeout: timeout,
	}
	if proto != "" {
		n, err := strconv.Atoi(proto)
		if err != nil {
			panic(err)
		}
		o.Protocol = n
	}
	return redis.NewClient(o)
}

// ---- assertions (panic = failed check) ----

func trunc(s string, n int) string {
	if len(s) > n {
		return s[:n]
	}
	return s
}

func eq(got, want any) {
	if !reflect.DeepEqual(got, want) {
		panic(fmt.Sprintf("got %s, want %s", trunc(fmt.Sprintf("%#v", got), 200), trunc(fmt.Sprintf("%#v", want), 200)))
	}
}

func assert(cond bool, msg string) {
	if !cond {
		panic("AssertionError: " + msg)
	}
}

func must(err error) {
	if err != nil {
		panic(fmt.Sprintf("%T: %v", err, err))
	}
}

func isNil(err error) bool { return errors.Is(err, redis.Nil) }

// raises: fn must fail with a server error reply containing text (case-insensitive).
func raises(fn func() error, text string) {
	err := fn()
	if err == nil {
		panic("no error")
	}
	var re redis.Error
	if !errors.As(err, &re) || isNil(err) {
		panic(fmt.Sprintf("not a server error reply: %T: %v", err, err))
	}
	assert(strings.Contains(strings.ToLower(err.Error()), strings.ToLower(text)),
		fmt.Sprintf("error %q lacks %q", err.Error(), text))
}

func check(name string, fn func()) {
	defer func() {
		if e := recover(); e != nil {
			failed = append(failed, name)
			fmt.Printf("FAIL %s: %s\n", name, trunc(fmt.Sprint(e), 300))
		}
	}()
	fn()
	fmt.Printf("PASS %s\n", name)
}

// ---- checks ----

func connect_ping() {
	s, err := r.Ping(ctx).Result()
	must(err)
	eq(s == "PONG", true)
	e, err := r.Echo(ctx, "hi").Result()
	must(err)
	eq(e, "hi")
}

func set_get() {
	s, err := r.Set(ctx, P+"s", "hello", 0).Result()
	must(err)
	eq(s == "OK", true)
	g, err := r.Get(ctx, P+"s").Result()
	must(err)
	eq(g, "hello")
	_, err = r.Get(ctx, P+"missing").Result()
	eq(isNil(err), true)
}

func binary_key_and_value() {
	b := make([]byte, 0, 1024)
	for i := 0; i < 4; i++ {
		for j := 0; j < 256; j++ {
			b = append(b, byte(j))
		}
	}
	v := string(b)
	k := P + "\x00\r\n\xff bin"
	must(r.Set(ctx, k, v, 0).Err())
	g, err := r.Get(ctx, k).Result()
	must(err)
	eq(g, v)
	must(r.Set(ctx, P+"empty", "", 0).Err())
	g, err = r.Get(ctx, P+"empty").Result()
	must(err)
	eq(g, "")
	must(r.Set(ctx, P+"uni", "xin chào 🌏", 0).Err())
	g, err = r.Get(ctx, P+"uni").Result()
	must(err)
	eq(g, "xin chào 🌏")
}

func big_value_8mib() {
	v := make([]byte, 8<<20)
	_, err := rand.Read(v)
	must(err)
	must(r.Set(ctx, P+"big", v, 0).Err())
	g, err := r.Get(ctx, P+"big").Bytes()
	must(err)
	eq(string(g) == string(v), true)
	must(r.Del(ctx, P+"big").Err())
}

func counters() {
	must(r.Del(ctx, P+"n").Err())
	n, err := r.Incr(ctx, P+"n").Result()
	must(err)
	eq(n, int64(1))
	n, err = r.IncrBy(ctx, P+"n", 41).Result()
	must(err)
	eq(n, int64(42))
	n, err = r.Decr(ctx, P+"n").Result()
	must(err)
	eq(n, int64(41))
	n, err = r.DecrBy(ctx, P+"n", 50).Result()
	must(err)
	eq(n, int64(-9))
	g, err := r.Get(ctx, P+"n").Result()
	must(err)
	eq(g, "-9")
}

func counter_errors() {
	must(r.Set(ctx, P+"abc", "abc", 0).Err())
	raises(func() error { return r.Incr(ctx, P+"abc").Err() }, "not an integer")
	max := strconv.FormatInt(1<<63-1, 10)
	must(r.Set(ctx, P+"max", max, 0).Err())
	raises(func() error { return r.Incr(ctx, P+"max").Err() }, "overflow")
	g, err := r.Get(ctx, P+"max").Result()
	must(err)
	eq(g, max)
}

func hashes() {
	must(r.Del(ctx, P+"h").Err())
	n, err := r.HSet(ctx, P+"h", map[string]any{"a": "1", "b": "2", "c": "3"}).Result()
	must(err)
	eq(n, int64(3))
	n, err = r.HSet(ctx, P+"h", "a", "9").Result()
	must(err)
	eq(n, int64(0))
	g, err := r.HGet(ctx, P+"h", "a").Result()
	must(err)
	eq(g, "9")
	_, err = r.HGet(ctx, P+"h", "zz").Result()
	eq(isNil(err), true)
	all, err := r.HGetAll(ctx, P+"h").Result()
	must(err)
	eq(all, map[string]string{"a": "9", "b": "2", "c": "3"})
	n, err = r.HLen(ctx, P+"h").Result()
	must(err)
	eq(n, int64(3))
	n, err = r.HDel(ctx, P+"h", "a", "zz").Result()
	must(err)
	eq(n, int64(1))
	all, err = r.HGetAll(ctx, P+"nohash").Result()
	must(err)
	eq(all, map[string]string{})
}

func wrong_type() {
	must(r.Set(ctx, P+"str", "x", 0).Err())
	raises(func() error { return r.HGet(ctx, P+"str", "f").Err() }, "WRONGTYPE")
	must(r.Del(ctx, P+"h2").Err())
	must(r.HSet(ctx, P+"h2", "f", "v").Err())
	raises(func() error { return r.Get(ctx, P+"h2").Err() }, "WRONGTYPE")
	raises(func() error { return r.Incr(ctx, P+"h2").Err() }, "WRONGTYPE")
}

func mset_mget() {
	s, err := r.MSet(ctx, P+"m1", "1", P+"m2", "2").Result()
	must(err)
	eq(s == "OK", true)
	g, err := r.MGet(ctx, P+"m1", P+"nope", P+"m2").Result()
	must(err)
	eq(g, []any{"1", nil, "2"})
}

func del_exists_unlink() {
	must(r.MSet(ctx, P+"d1", "1", P+"d2", "2", P+"d3", "3").Err())
	n, err := r.Exists(ctx, P+"d1", P+"d2", P+"nope", P+"d1").Result()
	must(err)
	eq(n, int64(3))
	n, err = r.Del(ctx, P+"d1", P+"nope").Result()
	must(err)
	eq(n, int64(1))
	n, err = r.Unlink(ctx, P+"d2", P+"d3").Result()
	must(err)
	eq(n, int64(2))
	n, err = r.Exists(ctx, P+"d1", P+"d2", P+"d3").Result()
	must(err)
	eq(n, int64(0))
}

func keys_pattern() {
	must(r.MSet(ctx, P+"k:1", "a", P+"k:2", "b", P+"k:x[", "c").Err())
	k, err := r.Keys(ctx, P+"k:?").Result()
	must(err)
	sort.Strings(k)
	eq(k, []string{P + "k:1", P + "k:2"})
	k, err = r.Keys(ctx, P+"k:x\\[").Result()
	must(err)
	eq(k, []string{P + "k:x["})
	k, err = r.Keys(ctx, P+"nothing*").Result()
	must(err)
	eq(len(k), 0)
}

func eval_values() {
	v, err := r.Eval(ctx, "return {KEYS[1], ARGV[1], 7, {1, 2}}", []string{P + "k"}, "a").Result()
	must(err)
	eq(v, []any{P + "k", "a", int64(7), []any{int64(1), int64(2)}})
	v, err = r.Eval(ctx, "return false", nil).Result()
	eq(isNil(err), true)
	eq(v, nil)
	v, err = r.Eval(ctx, "return redis.status_reply('FINE')", nil).Result()
	must(err)
	eq(v, "FINE")
	v, err = r.Eval(ctx, "return 3.99", nil).Result()
	must(err)
	eq(v, int64(3))
	v, err = r.Eval(ctx, "return ARGV[1]", nil, "\x00\xff").Result()
	must(err)
	eq(v, "\x00\xff")
}

func eval_calls_redis() {
	script := "redis.call('SET', KEYS[1], ARGV[1]); return redis.call('GET', KEYS[1])"
	v, err := r.Eval(ctx, script, []string{P + "ev"}, "from-lua").Result()
	must(err)
	eq(v, "from-lua")
	g, err := r.Get(ctx, P+"ev").Result()
	must(err)
	eq(g, "from-lua")
	v, err = r.Eval(ctx, "return redis.call('INCRBY', KEYS[1], 5)", []string{P + "evn"}).Result()
	must(err)
	eq(v, int64(5))
}

func eval_errors() {
	raises(func() error { return r.Eval(ctx, "return redis.call('NOSUCH')", nil).Err() }, "unknown command")
	raises(func() error { return r.Eval(ctx, "return redis.error_reply('boom')", nil).Err() }, "boom")
	raises(func() error { return r.Eval(ctx, "this is not lua", nil).Err() }, "ERR")
	raises(func() error { return r.Eval(ctx, "error('thrown')", nil).Err() }, "thrown")
	s, err := r.Ping(ctx).Result()
	must(err)
	eq(s == "PONG", true)
}

func script_helper_evalsha() {
	s := redis.NewScript("return ARGV[1]") // Run tries EVALSHA first
	v, err := s.Run(ctx, r, nil, "x").Result()
	must(err)
	eq(v, "x")
	v, err = s.Run(ctx, r, nil, "y").Result()
	must(err)
	eq(v, "y")
}

func pipeline_plain_10000() {
	must(r.Del(ctx, P+"pn").Err())
	p := r.Pipeline()
	for i := 0; i < 10000; i++ {
		p.Incr(ctx, P+"pn")
	}
	cmds, err := p.Exec(ctx)
	must(err)
	out := make([]int64, len(cmds))
	for i, c := range cmds {
		out[i] = c.(*redis.IntCmd).Val()
	}
	want := make([]int64, 10000)
	for i := range want {
		want[i] = int64(i + 1)
	}
	eq(out, want)
}

func pipeline_with_error_in_middle() {
	must(r.Set(ctx, P+"pe", "abc", 0).Err())
	p := r.Pipeline()
	c0 := p.Set(ctx, P+"pa", "1", 0)
	c1 := p.Incr(ctx, P+"pe")
	c2 := p.Get(ctx, P+"pa")
	_, _ = p.Exec(ctx) // returns the first error; per-command results are inspected below
	must(c0.Err())
	eq(c0.Val() == "OK", true)
	var re redis.Error
	assert(errors.As(c1.Err(), &re), fmt.Sprintf("out[1] not a server error: %v", c1.Err()))
	must(c2.Err())
	eq(c2.Val(), "1")
}

func x_pipeline_transaction_multi_exec() {
	p := r.TxPipeline() // wraps in MULTI/EXEC
	c0 := p.Set(ctx, P+"tx", "1", 0)
	c1 := p.Get(ctx, P+"tx")
	_, err := p.Exec(ctx)
	must(err)
	eq(c0.Val() == "OK", true)
	eq(c1.Val(), "1")
}

func x_set_with_expiry_and_nx() {
	s, err := r.Set(ctx, P+"ex", "v", 60*time.Second).Result()
	must(err)
	eq(s == "OK", true)
	ttl, err := r.TTL(ctx, P+"ex").Result()
	must(err)
	assert(ttl > 0 && ttl <= 60*time.Second, fmt.Sprintf("ttl %v", ttl))
	// SET key value NX (SetNX with no expiry would send SETNX instead)
	_, err = r.SetArgs(ctx, P+"ex", "other", redis.SetArgs{Mode: "NX"}).Result()
	eq(isNil(err), true)
}

func concurrency_16_threads() {
	must(r.Del(ctx, P+"c").Err())
	var mu sync.Mutex
	var errs []error
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c := newClient()
			defer c.Close()
			for j := 0; j < 500; j++ {
				if err := c.Incr(ctx, P+"c").Err(); err != nil {
					mu.Lock()
					errs = append(errs, err)
					mu.Unlock()
					return
				}
			}
		}()
	}
	wg.Wait()
	if len(errs) > 0 {
		panic(fmt.Sprintf("%d worker errors, first: %v", len(errs), errs[0]))
	}
	g, err := r.Get(ctx, P+"c").Result()
	must(err)
	eq(g, "8000")
}

func unknown_command_keeps_connection() {
	raises(func() error { return r.Do(ctx, "NOSUCHCMD", "a").Err() }, "unknown command")
	raises(func() error { return r.Do(ctx, "GET").Err() }, "wrong number of arguments")
	s, err := r.Ping(ctx).Result()
	must(err)
	eq(s == "PONG", true)
}

func hgetall_100k_fields() {
	must(r.Del(ctx, P+"bigh").Err())
	p := r.Pipeline()
	var cmds []*redis.IntCmd
	for i := 0; i < 100000; i += 1000 {
		m := make(map[string]any, 1000)
		for j := i; j < i+1000; j++ {
			m[fmt.Sprintf("f%d", j)] = fmt.Sprintf("v%d", j)
		}
		cmds = append(cmds, p.HSet(ctx, P+"bigh", m))
	}
	_, err := p.Exec(ctx)
	must(err)
	var sum int64
	for _, c := range cmds {
		sum += c.Val()
	}
	eq(sum, int64(100000))
	got, err := r.HGetAll(ctx, P+"bigh").Result()
	must(err)
	eq(len(got), 100000)
	eq(got["f99999"], "v99999")
	n, err := r.Del(ctx, P+"bigh").Result()
	must(err)
	eq(n, int64(1))
}

func client_side_commands() {
	ok, err := r.Do(ctx, "CLIENT", "SETNAME", "go-test").Text()
	must(err)
	eq(ok == "OK", true)
	info, err := r.Info(ctx).Result()
	must(err)
	assert(len(info) > 0, "empty INFO")
	s, err := r.Do(ctx, "SELECT", 0).Text()
	must(err)
	eq(s == "OK", true)
	raises(func() error { return r.Do(ctx, "SELECT", "16").Err() }, "out of range")
}

func flushall_dbsize() {
	must(r.Set(ctx, P+"last", "1", 0).Err())
	n, err := r.DBSize(ctx).Result()
	must(err)
	assert(n >= 1, fmt.Sprintf("dbsize %d", n))
	s, err := r.FlushAll(ctx).Result()
	must(err)
	eq(s == "OK", true)
	n, err = r.DBSize(ctx).Result()
	must(err)
	eq(n, int64(0))
	_, err = r.Get(ctx, P+"last").Result()
	eq(isNil(err), true)
}

// ---- lists, sets, sorted sets, keys, pub/sub, transactions, Lua, config ----

func ss(xs ...string) []string { return xs }

func sorted(xs []string) []string {
	out := append([]string{}, xs...)
	sort.Strings(out)
	return out
}

func zm(score float64, member string) redis.Z { return redis.Z{Score: score, Member: member} }

func i64(c *redis.IntCmd) int64 {
	n, err := c.Result()
	must(err)
	return n
}

func str(c *redis.StringCmd) string {
	s, err := c.Result()
	must(err)
	return s
}

func strs(c *redis.StringSliceCmd) []string {
	s, err := c.Result()
	must(err)
	return s
}

func lists() {
	k, k2 := P+"l", P+"l2"
	must(r.Del(ctx, k, k2).Err())
	eq(i64(r.RPush(ctx, k, "a", "b", "c")), int64(3))
	eq(i64(r.LPush(ctx, k, "z", "y")), int64(5)) // y z a b c
	eq(i64(r.LLen(ctx, k)), int64(5))
	eq(strs(r.LRange(ctx, k, 0, -1)), ss("y", "z", "a", "b", "c"))
	eq(strs(r.LRange(ctx, k, -2, -1)), ss("b", "c"))
	eq(len(strs(r.LRange(ctx, k, 10, 20))), 0)
	eq(str(r.LIndex(ctx, k, 0)), "y")
	eq(str(r.LIndex(ctx, k, -1)), "c")
	_, err := r.LIndex(ctx, k, 99).Result()
	eq(isNil(err), true)
	s, err := r.LSet(ctx, k, 1, "Z").Result()
	must(err)
	eq(s, "OK")
	raises(func() error { return r.LSet(ctx, k, 99, "x").Err() }, "out of range")
	eq(str(r.LPop(ctx, k)), "y")
	eq(str(r.RPop(ctx, k)), "c")
	eq(strs(r.LPopCount(ctx, k, 2)), ss("Z", "a"))
	eq(strs(r.RPopCount(ctx, k, 5)), ss("b"))
	_, err = r.LPop(ctx, k).Result()
	eq(isNil(err), true)
	_, err = r.LPopCount(ctx, k, 2).Result()
	eq(isNil(err), true)
	eq(i64(r.Exists(ctx, k)), int64(0))
	must(r.RPush(ctx, k, "a", "b", "a", "c", "a").Err())
	eq(i64(r.LRem(ctx, k, 2, "a")), int64(2))
	eq(strs(r.LRange(ctx, k, 0, -1)), ss("b", "c", "a"))
	eq(i64(r.LRem(ctx, k, -1, "a")), int64(1))
	eq(i64(r.LRem(ctx, k, 0, "nope")), int64(0))
	must(r.RPush(ctx, k, "d", "e", "f").Err()) // b c d e f
	s, err = r.LTrim(ctx, k, 1, -2).Result()
	must(err)
	eq(s, "OK")
	eq(strs(r.LRange(ctx, k, 0, -1)), ss("c", "d", "e"))
	eq(i64(r.LInsert(ctx, k, "BEFORE", "d", "cd")), int64(4))
	eq(i64(r.LInsertAfter(ctx, k, "e", "ef")), int64(5))
	eq(i64(r.LInsertBefore(ctx, k, "missing", "x")), int64(-1))
	eq(i64(r.LInsertBefore(ctx, P+"nolist", "a", "x")), int64(0))
	eq(strs(r.LRange(ctx, k, 0, -1)), ss("c", "cd", "d", "e", "ef"))
	must(r.RPush(ctx, k, "d").Err())
	eq(i64(r.LPos(ctx, k, "d", redis.LPosArgs{})), int64(2))
	eq(i64(r.LPos(ctx, k, "d", redis.LPosArgs{Rank: 2})), int64(5))
	ps, err := r.LPosCount(ctx, k, "d", 0, redis.LPosArgs{}).Result()
	must(err)
	eq(ps, []int64{2, 5})
	_, err = r.LPos(ctx, k, "nope", redis.LPosArgs{}).Result()
	eq(isNil(err), true)
	eq(str(r.LMove(ctx, k, k2, "LEFT", "RIGHT")), "c")
	eq(str(r.LMove(ctx, k, k2, "RIGHT", "LEFT")), "d")
	eq(strs(r.LRange(ctx, k2, 0, -1)), ss("d", "c"))
	eq(str(r.RPopLPush(ctx, k, k2)), "ef")
	eq(strs(r.LRange(ctx, k2, 0, -1)), ss("ef", "d", "c"))
	_, err = r.LMove(ctx, P+"nolist", k2, "LEFT", "LEFT").Result()
	eq(isNil(err), true)
	eq(r.Type(ctx, k).Val(), "list")
	raises(func() error { return r.Get(ctx, k).Err() }, "WRONGTYPE")
	must(r.Set(ctx, P+"lstr", "x", 0).Err())
	raises(func() error { return r.LPush(ctx, P+"lstr", "x").Err() }, "WRONGTYPE")
	eq(i64(r.Del(ctx, k, k2, P+"lstr")), int64(3))
}

func sets() {
	k := []string{P + "seta", P + "setb", P + "setc"}
	d := P + "setd"
	must(r.Del(ctx, k[0], k[1], k[2], d).Err())
	eq(i64(r.SAdd(ctx, k[0], "1", "2", "3", "4", "4")), int64(4))
	eq(i64(r.SAdd(ctx, k[0], "1")), int64(0))
	eq(i64(r.SCard(ctx, k[0])), int64(4))
	b, err := r.SIsMember(ctx, k[0], "2").Result()
	must(err)
	eq(b, true)
	b, err = r.SIsMember(ctx, k[0], "9").Result()
	must(err)
	eq(b, false)
	bs, err := r.SMIsMember(ctx, k[0], "1", "9", "4").Result()
	must(err)
	eq(bs, []bool{true, false, true})
	eq(sorted(strs(r.SMembers(ctx, k[0]))), ss("1", "2", "3", "4"))
	eq(i64(r.SRem(ctx, k[0], "4", "9")), int64(1))
	must(r.SAdd(ctx, k[1], "2", "3", "5").Err())
	must(r.SAdd(ctx, k[2], "3", "6").Err())
	eq(sorted(strs(r.SInter(ctx, k[0], k[1]))), ss("2", "3"))
	eq(sorted(strs(r.SInter(ctx, k[0], k[1], k[2]))), ss("3"))
	eq(sorted(strs(r.SUnion(ctx, k[0], k[1], k[2]))), ss("1", "2", "3", "5", "6"))
	eq(sorted(strs(r.SDiff(ctx, k[0], k[1]))), ss("1"))
	eq(sorted(strs(r.SDiff(ctx, k[0], k[1], k[2]))), ss("1"))
	eq(i64(r.SInterStore(ctx, d, k[0], k[1])), int64(2))
	eq(sorted(strs(r.SMembers(ctx, d))), ss("2", "3"))
	eq(i64(r.SUnionStore(ctx, d, k[0], k[1])), int64(4))
	eq(i64(r.SDiffStore(ctx, d, k[1], k[0])), int64(1))
	eq(strs(r.SMembers(ctx, d)), ss("5"))
	eq(i64(r.SInterCard(ctx, 0, k[0], k[1])), int64(2))
	eq(i64(r.SInterCard(ctx, 1, k[0], k[1])), int64(1))
	b, err = r.SMove(ctx, k[0], k[2], "1").Result()
	must(err)
	eq(b, true)
	b, err = r.SMove(ctx, k[0], k[2], "nope").Result()
	must(err)
	eq(b, false)
	eq(sorted(strs(r.SMembers(ctx, k[2]))), ss("1", "3", "6"))
	one := str(r.SRandMember(ctx, k[2]))
	assert(one == "1" || one == "3" || one == "6", "srandmember outside set: "+one)
	eq(len(strs(r.SRandMemberN(ctx, k[2], 10))), 3)
	eq(len(strs(r.SRandMemberN(ctx, k[2], -5))), 5)
	two := strs(r.SRandMemberN(ctx, k[2], 2))
	eq(len(two), 2)
	assert(two[0] != two[1], "srandmember count returned a duplicate")
	_, err = r.SRandMember(ctx, P+"noset").Result()
	eq(isNil(err), true)
	one = str(r.SPop(ctx, k[2]))
	assert(one == "1" || one == "3" || one == "6", "spop outside set: "+one)
	eq(len(strs(r.SPopN(ctx, k[2], 5))), 2)
	eq(i64(r.Exists(ctx, k[2])), int64(0))
	_, err = r.SPop(ctx, k[2]).Result()
	eq(isNil(err), true)
	big := P + "setbig"
	must(r.Del(ctx, big).Err())
	members := make([]any, 300)
	for i := range members {
		members[i] = fmt.Sprintf("m%d", i)
	}
	must(r.SAdd(ctx, big, members...).Err())
	seen := map[string]bool{}
	var cur uint64
	for i := 0; i < 1000; i++ {
		keys, next, err := r.SScan(ctx, big, cur, "", 50).Result()
		must(err)
		for _, x := range keys {
			seen[x] = true
		}
		cur = next
		if cur == 0 {
			break
		}
	}
	eq(len(seen), 300)
	eq(r.Type(ctx, big).Val(), "set")
	raises(func() error { return r.Get(ctx, big).Err() }, "WRONGTYPE")
	must(r.Del(ctx, big, k[0], k[1], k[2], d).Err())
}

func zrange(c *redis.StringSliceCmd) []string { return strs(c) }

func zsets_basic() {
	// membership and order only; score replies are checked in zset_scores
	k := P + "z"
	must(r.Del(ctx, k).Err())
	eq(i64(r.ZAdd(ctx, k, zm(1, "a"), zm(2.5, "b"), zm(1.5, "c"))), int64(3)) // a c b
	eq(i64(r.ZAdd(ctx, k, zm(1, "a"))), int64(0))
	eq(i64(r.ZAddArgs(ctx, k, redis.ZAddArgs{Ch: true, Members: []redis.Z{zm(5, "a")}})), int64(1)) // c b a
	eq(i64(r.ZAddArgs(ctx, k, redis.ZAddArgs{Ch: true, Members: []redis.Z{zm(7, "d"), zm(5, "a")}})), int64(1))
	eq(i64(r.ZAddNX(ctx, k, zm(9, "a"))), int64(0))
	eq(zrange(r.ZRange(ctx, k, 0, -1)), ss("c", "b", "a", "d"))
	eq(i64(r.ZAddXX(ctx, k, zm(1, "zz"))), int64(0))
	eq(i64(r.ZCard(ctx, k)), int64(4))
	eq(i64(r.ZAddArgs(ctx, k, redis.ZAddArgs{GT: true, Ch: true, Members: []redis.Z{zm(3, "a")}})), int64(0))
	eq(i64(r.ZAddArgs(ctx, k, redis.ZAddArgs{GT: true, Ch: true, Members: []redis.Z{zm(8, "a")}})), int64(1)) // c b d a
	eq(zrange(r.ZRange(ctx, k, 0, -1)), ss("c", "b", "d", "a"))
	eq(i64(r.ZAddArgs(ctx, k, redis.ZAddArgs{LT: true, Ch: true, Members: []redis.Z{zm(10, "a")}})), int64(0))
	eq(i64(r.ZAddArgs(ctx, k, redis.ZAddArgs{LT: true, Ch: true, Members: []redis.Z{zm(6, "a")}})), int64(1)) // c b a d
	eq(zrange(r.ZRange(ctx, k, 0, -1)), ss("c", "b", "a", "d"))
	_, err := r.ZAddArgsIncr(ctx, k, redis.ZAddArgs{NX: true, Members: []redis.Z{zm(1, "a")}}).Result()
	eq(isNil(err), true)
	_, err = r.ZAddArgsIncr(ctx, k, redis.ZAddArgs{XX: true, Members: []redis.Z{zm(1, "nope")}}).Result()
	eq(isNil(err), true)
	eq(i64(r.ZRank(ctx, k, "c")), int64(0))
	eq(i64(r.ZRank(ctx, k, "d")), int64(3))
	eq(i64(r.ZRevRank(ctx, k, "d")), int64(0))
	eq(i64(r.ZRevRank(ctx, k, "c")), int64(3))
	_, err = r.ZRank(ctx, k, "nope").Result()
	eq(isNil(err), true)
	_, err = r.ZRevRank(ctx, k, "nope").Result()
	eq(isNil(err), true)
	eq(i64(r.ZCount(ctx, k, "2", "6")), int64(2))
	eq(i64(r.ZCount(ctx, k, "(2.5", "7")), int64(2))
	eq(i64(r.ZCount(ctx, k, "(1.5", "(7")), int64(2))
	eq(i64(r.ZCount(ctx, k, "-inf", "+inf")), int64(4))
	eq(i64(r.ZCount(ctx, k, "100", "200")), int64(0))
	eq(i64(r.ZRem(ctx, k, "a", "nope")), int64(1))
	eq(zrange(r.ZRange(ctx, k, 0, -1)), ss("c", "b", "d"))
	eq(r.Type(ctx, k).Val(), "zset")
	raises(func() error { return r.Get(ctx, k).Err() }, "WRONGTYPE")
	raises(func() error { return r.Do(ctx, "ZADD", k, "notanumber", "a").Err() }, "not a valid float")
	raises(func() error { return r.Do(ctx, "ZADD", k, "NX", "XX", 1, "a").Err() }, "not compatible")
	eq(i64(r.Del(ctx, k)), int64(1))
}

func zset_scores() {
	k := P + "zsc"
	inf, ninf := math.Inf(1), math.Inf(-1)
	must(r.Del(ctx, k).Err())
	must(r.ZAdd(ctx, k, zm(1, "a"), zm(2.5, "b"), zm(1.5, "c")).Err())
	f, err := r.ZScore(ctx, k, "a").Result()
	must(err)
	eq(f, 1.0)
	f, err = r.ZScore(ctx, k, "c").Result()
	must(err)
	eq(f, 1.5)
	_, err = r.ZScore(ctx, k, "nope").Result()
	eq(isNil(err), true)
	fs, err := r.ZMScore(ctx, k, "a", "nope", "c").Result()
	must(err)
	eq(fs, []float64{1, 0, 1.5}) // go-redis maps a nil score to 0
	f, err = r.ZAddArgsIncr(ctx, k, redis.ZAddArgs{Members: []redis.Z{zm(4.25, "a")}}).Result()
	must(err)
	eq(f, 5.25)
	f, err = r.ZAddArgsIncr(ctx, k, redis.ZAddArgs{XX: true, Members: []redis.Z{zm(-0.25, "a")}}).Result()
	must(err)
	eq(f, 5.0)
	f, err = r.ZIncrBy(ctx, k, 0.5, "c").Result()
	must(err)
	eq(f, 2.0)
	f, err = r.ZIncrBy(ctx, k, 1, "new").Result() // new c b a
	must(err)
	eq(f, 1.0)
	rk, err := r.ZRankWithScore(ctx, k, "c").Result()
	must(err)
	eq(rk, redis.RankScore{Rank: 1, Score: 2})
	rk, err = r.ZRevRankWithScore(ctx, k, "c").Result()
	must(err)
	eq(rk, redis.RankScore{Rank: 2, Score: 2})
	_, err = r.ZRankWithScore(ctx, k, "nope").Result()
	eq(isNil(err), true)
	zs, err := r.ZRangeWithScores(ctx, k, 0, -1).Result()
	must(err)
	eq(zs, []redis.Z{zm(1, "new"), zm(2, "c"), zm(2.5, "b"), zm(5, "a")})
	zs, err = r.ZRangeArgsWithScores(ctx, redis.ZRangeArgs{Key: k, Start: 0, Stop: 1, Rev: true}).Result()
	must(err)
	eq(zs, []redis.Z{zm(5, "a"), zm(2.5, "b")})
	zs, err = r.ZRangeByScoreWithScores(ctx, k, &redis.ZRangeBy{Min: "2", Max: "3"}).Result()
	must(err)
	eq(zs, []redis.Z{zm(2, "c"), zm(2.5, "b")})
	zs, err = r.ZRevRangeWithScores(ctx, k, 0, 0).Result()
	must(err)
	eq(zs, []redis.Z{zm(5, "a")})
	zs, err = r.ZRevRangeByScoreWithScores(ctx, k, &redis.ZRangeBy{Min: "2.5", Max: "5"}).Result()
	must(err)
	eq(zs, []redis.Z{zm(5, "a"), zm(2.5, "b")})
	zs, err = r.ZRangeArgsWithScores(ctx, redis.ZRangeArgs{Key: k, Start: 2, Stop: 5, ByScore: true, Offset: 1, Count: 1}).Result()
	must(err)
	eq(zs, []redis.Z{zm(2.5, "b")})
	must(r.Del(ctx, k).Err())
	must(r.ZAdd(ctx, k, zm(ninf, "lo"), zm(inf, "hi"), zm(1.5, "mid"), zm(-0.1, "frac")).Err())
	eq(zrange(r.ZRange(ctx, k, 0, -1)), ss("lo", "frac", "mid", "hi"))
	zs, err = r.ZRangeWithScores(ctx, k, 0, -1).Result()
	must(err)
	eq(zs, []redis.Z{zm(ninf, "lo"), zm(-0.1, "frac"), zm(1.5, "mid"), zm(inf, "hi")})
	f, err = r.ZScore(ctx, k, "hi").Result()
	must(err)
	eq(f, inf)
	f, err = r.ZScore(ctx, k, "lo").Result()
	must(err)
	eq(f, ninf)
	fs, err = r.ZMScore(ctx, k, "lo", "hi").Result()
	must(err)
	eq(fs, []float64{ninf, inf})
	eq(strs(r.ZRangeByScore(ctx, k, &redis.ZRangeBy{Min: "-inf", Max: "+inf"})), ss("lo", "frac", "mid", "hi"))
	eq(strs(r.ZRangeByScore(ctx, k, &redis.ZRangeBy{Min: "(-inf", Max: "(inf"})), ss("frac", "mid"))
	eq(i64(r.ZCount(ctx, k, "-inf", "0")), int64(2))
	f, err = r.ZIncrBy(ctx, k, 5, "hi").Result()
	must(err)
	eq(f, inf)
	flat, _, err := r.ZScan(ctx, k, 0, "", 100).Result()
	must(err)
	got := map[string]string{}
	for i := 0; i+1 < len(flat); i += 2 {
		got[flat[i]] = flat[i+1]
	}
	eq(got, map[string]string{"lo": "-inf", "frac": "-0.1", "mid": "1.5", "hi": "inf"})
	must(r.Del(ctx, k).Err())
}

func zset_pop() {
	k := P + "zp"
	must(r.Del(ctx, k).Err())
	must(r.ZAdd(ctx, k, zm(1, "a"), zm(2, "b"), zm(3, "c"), zm(4.5, "d"), zm(5, "e")).Err())
	zs, err := r.ZPopMin(ctx, k).Result()
	must(err)
	eq(zs, []redis.Z{zm(1, "a")})
	zs, err = r.ZPopMax(ctx, k).Result()
	must(err)
	eq(zs, []redis.Z{zm(5, "e")})
	zs, err = r.ZPopMin(ctx, k, 2).Result()
	must(err)
	eq(zs, []redis.Z{zm(2, "b"), zm(3, "c")})
	zs, err = r.ZPopMax(ctx, k, 5).Result()
	must(err)
	eq(zs, []redis.Z{zm(4.5, "d")})
	zs, err = r.ZPopMin(ctx, k).Result()
	must(err)
	eq(len(zs), 0)
	zs, err = r.ZPopMax(ctx, k, 3).Result()
	must(err)
	eq(len(zs), 0)
	eq(i64(r.Exists(ctx, k)), int64(0))
}

func zsets_ranges() {
	k := P + "zr"
	must(r.Del(ctx, k).Err())
	must(r.ZAdd(ctx, k, zm(1, "a"), zm(2, "b"), zm(3, "c"), zm(4, "d"), zm(5, "e")).Err())
	eq(strs(r.ZRange(ctx, k, 0, 1)), ss("a", "b"))
	eq(strs(r.ZRange(ctx, k, -2, -1)), ss("d", "e"))
	eq(strs(r.ZRangeArgs(ctx, redis.ZRangeArgs{Key: k, Start: 0, Stop: 1, Rev: true})), ss("e", "d"))
	eq(strs(r.ZRangeArgs(ctx, redis.ZRangeArgs{Key: k, Start: 2, Stop: 3, ByScore: true})), ss("b", "c"))
	eq(strs(r.ZRangeArgs(ctx, redis.ZRangeArgs{Key: k, Start: "(1", Stop: "+inf", ByScore: true, Offset: 1, Count: 2})), ss("c", "d"))
	eq(strs(r.ZRangeArgs(ctx, redis.ZRangeArgs{Key: k, Start: "+inf", Stop: "(2", ByScore: true, Rev: true, Offset: 0, Count: 2})), ss("e", "d"))
	eq(strs(r.ZRangeByScore(ctx, k, &redis.ZRangeBy{Min: "-inf", Max: "3"})), ss("a", "b", "c"))
	eq(strs(r.ZRangeByScore(ctx, k, &redis.ZRangeBy{Min: "(1", Max: "(4"})), ss("b", "c"))
	eq(strs(r.ZRangeByScore(ctx, k, &redis.ZRangeBy{Min: "1", Max: "5", Offset: 1, Count: 2})), ss("b", "c"))
	eq(strs(r.ZRevRangeByScore(ctx, k, &redis.ZRangeBy{Min: "4", Max: "5"})), ss("e", "d"))
	eq(strs(r.ZRevRangeByScore(ctx, k, &redis.ZRangeBy{Min: "-inf", Max: "+inf", Offset: 0, Count: 1})), ss("e"))
	eq(strs(r.ZRevRange(ctx, k, 0, 1)), ss("e", "d"))
	eq(len(strs(r.ZRange(ctx, k, 10, 20))), 0)
	eq(len(strs(r.ZRange(ctx, P+"nozset", 0, -1))), 0)
	eq(i64(r.ZRemRangeByRank(ctx, k, 0, 1)), int64(2))       // c d e
	eq(i64(r.ZRemRangeByScore(ctx, k, "(3", "4")), int64(1)) // c e
	eq(strs(r.ZRange(ctx, k, 0, -1)), ss("c", "e"))
	eq(i64(r.ZRemRangeByScore(ctx, k, "-inf", "+inf")), int64(2))
	eq(i64(r.Exists(ctx, k)), int64(0))
	big := P + "zbig"
	must(r.Del(ctx, big).Err())
	zz := make([]redis.Z, 300)
	for i := range zz {
		zz[i] = zm(float64(i), fmt.Sprintf("m%d", i))
	}
	must(r.ZAdd(ctx, big, zz...).Err())
	seen := map[string]string{}
	var cur uint64
	for i := 0; i < 1000; i++ {
		flat, next, err := r.ZScan(ctx, big, cur, "", 50).Result()
		must(err)
		for j := 0; j+1 < len(flat); j += 2 {
			seen[flat[j]] = flat[j+1]
		}
		cur = next
		if cur == 0 {
			break
		}
	}
	eq(len(seen), 300)
	eq(seen["m299"], "299")
	must(r.Del(ctx, big).Err())
}

func zsets_store() {
	a, c, d := P+"zsa", P+"zsb", P+"zsd"
	scores := func(key string) map[string]string {
		flat, _, err := r.ZScan(ctx, key, 0, "", 1000).Result()
		must(err)
		out := map[string]string{}
		for i := 0; i+1 < len(flat); i += 2 {
			out[flat[i]] = flat[i+1]
		}
		return out
	}
	must(r.Del(ctx, a, c, d).Err())
	must(r.ZAdd(ctx, a, zm(1, "x"), zm(2, "y"), zm(3, "z")).Err())
	must(r.ZAdd(ctx, c, zm(10, "y"), zm(20, "z"), zm(0.5, "w")).Err())
	eq(i64(r.ZUnionStore(ctx, d, &redis.ZStore{Keys: []string{a, c}})), int64(4))
	eq(strs(r.ZRange(ctx, d, 0, -1)), ss("w", "x", "y", "z"))
	eq(i64(r.ZUnionStore(ctx, d, &redis.ZStore{Keys: []string{a, c}, Weights: []float64{2, 0.5}})), int64(4))
	eq(scores(d), map[string]string{"w": "0.25", "x": "2", "y": "9", "z": "16"})
	eq(i64(r.ZUnionStore(ctx, d, &redis.ZStore{Keys: []string{a, c}, Aggregate: "MAX"})), int64(4))
	eq(scores(d)["z"], "20")
	eq(i64(r.ZInterStore(ctx, d, &redis.ZStore{Keys: []string{a, c}})), int64(2))
	eq(scores(d), map[string]string{"y": "12", "z": "23"})
	eq(i64(r.ZInterStore(ctx, d, &redis.ZStore{Keys: []string{a, c}, Weights: []float64{3, 2}})), int64(2))
	eq(scores(d), map[string]string{"y": "26", "z": "49"})
	eq(i64(r.ZInterStore(ctx, d, &redis.ZStore{Keys: []string{a, c}, Aggregate: "MIN"})), int64(2))
	eq(scores(d)["y"], "2")
	must(r.SAdd(ctx, P+"zss", "y", "q").Err())
	eq(i64(r.ZInterStore(ctx, d, &redis.ZStore{Keys: []string{a, P + "zss"}})), int64(1)) // sets count as score 1
	eq(scores(d)["y"], "3")
	eq(i64(r.ZInterStore(ctx, d, &redis.ZStore{Keys: []string{a, P + "nozset"}})), int64(0))
	eq(i64(r.Exists(ctx, d)), int64(0))
	must(r.Del(ctx, a, c, d, P+"zss").Err())
}

func string_key_extras() {
	h := P + "hx"
	must(r.Del(ctx, P+"f", P+"gr", P+"grnew", P+"abc", h).Err())
	f, err := r.IncrByFloat(ctx, P+"f", 1.5).Result()
	must(err)
	eq(f, 1.5)
	f, err = r.IncrByFloat(ctx, P+"f", 2).Result()
	must(err)
	eq(f, 3.5)
	f, err = r.IncrByFloat(ctx, P+"f", -0.25).Result()
	must(err)
	eq(f, 3.25)
	eq(str(r.Get(ctx, P+"f")), "3.25")
	must(r.Set(ctx, P+"abc", "abc", 0).Err())
	raises(func() error { return r.IncrByFloat(ctx, P+"abc", 1).Err() }, "not a valid float")
	must(r.Set(ctx, P+"gr", "Hello World", 0).Err())
	eq(str(r.GetRange(ctx, P+"gr", 0, 4)), "Hello")
	eq(str(r.GetRange(ctx, P+"gr", -5, -1)), "World")
	eq(str(r.GetRange(ctx, P+"gr", 20, 30)), "")
	eq(i64(r.SetRange(ctx, P+"gr", 6, "Redis")), int64(11))
	eq(str(r.Get(ctx, P+"gr")), "Hello Redis")
	eq(i64(r.SetRange(ctx, P+"gr", 13, "!")), int64(14))
	eq(str(r.Get(ctx, P+"gr")), "Hello Redis\x00\x00!")
	eq(i64(r.SetRange(ctx, P+"grnew", 2, "x")), int64(3))
	eq(str(r.Get(ctx, P+"grnew")), "\x00\x00x")
	must(r.HSet(ctx, h, "a", "1", "b", "hello", "c", "2.5").Err())
	eq(sorted(strs(r.HKeys(ctx, h))), ss("a", "b", "c"))
	eq(sorted(strs(r.HVals(ctx, h))), ss("1", "2.5", "hello"))
	eq(i64(r.HStrLen(ctx, h, "b")), int64(5))
	eq(i64(r.HStrLen(ctx, h, "nope")), int64(0))
	f, err = r.HIncrByFloat(ctx, h, "c", 0.5).Result()
	must(err)
	eq(f, 3.0)
	f, err = r.HIncrByFloat(ctx, h, "new", 1.5).Result()
	must(err)
	eq(f, 1.5)
	eq(i64(r.HIncrBy(ctx, h, "a", 4)), int64(5))
	raises(func() error { return r.HIncrByFloat(ctx, h, "b", 1).Err() }, "not a float")
	one := strs(r.HRandField(ctx, h, 1))
	eq(len(one), 1)
	assert(one[0] == "a" || one[0] == "b" || one[0] == "c" || one[0] == "new", "hrandfield outside hash")
	eq(len(strs(r.HRandField(ctx, h, 2))), 2)
	eq(len(strs(r.HRandField(ctx, h, -6))), 6)
	eq(len(strs(r.HRandField(ctx, P+"nohash", 3))), 0)
	b, err := r.HExists(ctx, h, "a").Result()
	must(err)
	eq(b, true)
	vals, err := r.HMGet(ctx, h, "a", "zz").Result()
	must(err)
	eq(vals, []any{"5", nil})
	b, err = r.HSetNX(ctx, h, "a", "x").Result()
	must(err)
	eq(b, false)
	must(r.Del(ctx, P+"f", P+"gr", P+"grnew", P+"abc", h).Err())
}

func hrandfield_values() {
	h := P + "hrf"
	must(r.Del(ctx, h).Err())
	must(r.HSet(ctx, h, "a", "1", "b", "hello", "c", "3").Err())
	kvs, err := r.HRandFieldWithValues(ctx, h, 3).Result()
	must(err)
	eq(len(kvs), 3)
	got := map[string]string{}
	for _, kv := range kvs {
		got[kv.Key] = kv.Value
	}
	eq(got, map[string]string{"a": "1", "b": "hello", "c": "3"})
	kvs, err = r.HRandFieldWithValues(ctx, h, -5).Result()
	must(err)
	eq(len(kvs), 5)
	must(r.Del(ctx, h).Err())
}

func keys_misc() {
	names := []string{"r1", "r2", "r3", "cp1", "cp2", "ms1", "ms2", "ms3", "ty:s", "ty:h", "ty:l", "ty:set", "ty:z", "ty:int"}
	del := func() {
		for _, n := range names {
			must(r.Del(ctx, P+n).Err())
		}
	}
	del()
	must(r.Set(ctx, P+"r1", "v", 0).Err())
	s, err := r.Rename(ctx, P+"r1", P+"r2").Result()
	must(err)
	eq(s, "OK")
	eq(i64(r.Exists(ctx, P+"r1", P+"r2")), int64(1))
	raises(func() error { return r.Rename(ctx, P+"nokey", P+"x").Err() }, "no such key")
	must(r.Set(ctx, P+"r1", "other", 0).Err())
	b, err := r.RenameNX(ctx, P+"r1", P+"r2").Result()
	must(err)
	eq(b, false)
	eq(str(r.Get(ctx, P+"r2")), "v")
	b, err = r.RenameNX(ctx, P+"r1", P+"r3").Result()
	must(err)
	eq(b, true)
	eq(str(r.Get(ctx, P+"r3")), "other")
	must(r.RPush(ctx, P+"cp1", "a", "b").Err())
	b, err = r.MSetNX(ctx, P+"ms1", "1", P+"ms2", "2").Result()
	must(err)
	eq(b, true)
	b, err = r.MSetNX(ctx, P+"ms1", "x", P+"ms3", "3").Result()
	must(err)
	eq(b, false)
	eq(i64(r.Exists(ctx, P+"ms3")), int64(0))
	eq(str(r.Get(ctx, P+"ms1")), "1")
	eq(i64(r.Touch(ctx, P+"r2", P+"cp1", P+"nokey")), int64(2))
	must(r.Set(ctx, P+"ty:s", "x", 0).Err())
	must(r.HSet(ctx, P+"ty:h", "f", "v").Err())
	must(r.RPush(ctx, P+"ty:l", "x").Err())
	must(r.SAdd(ctx, P+"ty:set", "x").Err())
	must(r.ZAdd(ctx, P+"ty:z", zm(1, "x")).Err())
	var types []string
	for _, t := range []string{"s", "h", "l", "set", "z"} {
		v, err := r.Type(ctx, P+"ty:"+t).Result()
		must(err)
		types = append(types, v)
	}
	eq(types, ss("string", "hash", "list", "set", "zset"))
	eq(r.Type(ctx, P+"nokey").Val(), "none")
	for _, n := range []string{"ty:s", "ty:h", "ty:l", "ty:set", "ty:z"} {
		enc, err := r.ObjectEncoding(ctx, P+n).Result()
		must(err)
		assert(enc != "", "empty encoding of "+n)
	}
	_, err = r.ObjectEncoding(ctx, P+"nokey").Result()
	eq(isNil(err), true)
	enc := r.ObjectEncoding(ctx, P+"ty:s").Val()
	assert(enc == "embstr" || enc == "raw", "string encoding "+enc)
	must(r.Set(ctx, P+"ty:int", "12345", 0).Err())
	eq(r.ObjectEncoding(ctx, P+"ty:int").Val(), "int")
	scanType := func(t string) []string {
		got := map[string]bool{}
		var cur uint64
		for i := 0; i < 10000; i++ {
			keys, next, err := r.ScanType(ctx, cur, P+"ty:*", 100, t).Result()
			must(err)
			for _, x := range keys {
				got[x] = true
			}
			cur = next
			if cur == 0 {
				break
			}
		}
		var out []string
		for x := range got {
			out = append(out, x)
		}
		sort.Strings(out)
		return out
	}
	eq(scanType("string"), ss(P+"ty:int", P+"ty:s"))
	eq(scanType("hash"), ss(P+"ty:h"))
	eq(scanType("list"), ss(P+"ty:l"))
	eq(scanType("set"), ss(P+"ty:set"))
	eq(scanType("zset"), ss(P+"ty:z"))
	rk, err := r.RandomKey(ctx).Result()
	must(err)
	eq(i64(r.Exists(ctx, rk)), int64(1))
	del()
}

// go-redis always appends "DB <n>" to COPY, so this check is separate from keys_misc.
func copy_keys() {
	must(r.Del(ctx, P+"cp1", P+"cp2").Err())
	must(r.RPush(ctx, P+"cp1", "a", "b").Err())
	eq(i64(r.Copy(ctx, P+"cp1", P+"cp2", 0, false)), int64(1))
	eq(i64(r.Copy(ctx, P+"cp1", P+"cp2", 0, false)), int64(0))
	eq(i64(r.Copy(ctx, P+"cp1", P+"cp2", 0, true)), int64(1))
	eq(strs(r.LRange(ctx, P+"cp2", 0, -1)), ss("a", "b"))
	must(r.RPush(ctx, P+"cp1", "c").Err())
	eq(i64(r.LLen(ctx, P+"cp2")), int64(2)) // a copy, not an alias
	eq(i64(r.Copy(ctx, P+"nokey", P+"cp3", 0, false)), int64(0))
	must(r.Del(ctx, P+"cp1", P+"cp2").Err())
}

func pubsub_roundtrip() {
	ch, ch2, pat, both := P+"chan", P+"chan2", P+"pat:*", P+"pat:both"
	ps := r.Subscribe(ctx, ch)
	defer ps.Close()
	recv := func() any {
		c, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		m, err := ps.Receive(c)
		must(err)
		return m
	}
	sub := func(want string, count int) {
		s, ok := recv().(*redis.Subscription)
		assert(ok, "expected a subscription confirmation")
		eq(s.Kind, want)
		eq(s.Count, count)
	}
	msg := func(wantChannel, wantPayload, wantPattern string) {
		m, ok := recv().(*redis.Message)
		assert(ok, "expected a message")
		eq(m.Channel, wantChannel)
		eq(m.Payload, wantPayload)
		eq(m.Pattern, wantPattern)
	}
	sub("subscribe", 1)
	must(ps.PSubscribe(ctx, pat))
	sub("psubscribe", 2)
	eq(i64(r.Publish(ctx, ch, "hello")), int64(1))
	msg(ch, "hello", "")
	eq(i64(r.Publish(ctx, P+"pat:one", "p1")), int64(1))
	msg(P+"pat:one", "p1", pat)
	eq(i64(r.Publish(ctx, P+"other", "x")), int64(0))
	eq(i64(r.Publish(ctx, ch2, "x")), int64(0))
	must(ps.Subscribe(ctx, both))
	sub("subscribe", 3)
	must(ps.PSubscribe(ctx, P+"pat:b*"))
	sub("psubscribe", 4)
	eq(i64(r.Publish(ctx, both, "twice")), int64(3))
	for i := 0; i < 3; i++ {
		m, ok := recv().(*redis.Message)
		assert(ok, "expected one of three deliveries")
		eq(m.Payload, "twice")
	}
	ns, err := r.PubSubNumSub(ctx, ch, ch2).Result()
	must(err)
	eq(ns, map[string]int64{ch: 1, ch2: 0})
	np, err := r.PubSubNumPat(ctx).Result()
	must(err)
	eq(np, int64(2))
	chans, err := r.PubSubChannels(ctx, P+"*").Result()
	must(err)
	eq(sorted(chans), sorted(ss(ch, both)))
	must(ps.PUnsubscribe(ctx, P+"pat:b*"))
	sub("punsubscribe", 3)
	must(ps.Unsubscribe(ctx, ch))
	sub("unsubscribe", 2)
	eq(i64(r.Publish(ctx, ch, "gone")), int64(0))
	must(ps.PUnsubscribe(ctx, pat))
	sub("punsubscribe", 1)
	eq(i64(r.Publish(ctx, P+"pat:one", "gone")), int64(0))
	must(ps.Unsubscribe(ctx))
	sub("unsubscribe", 0)
	ns, err = r.PubSubNumSub(ctx, both).Result()
	must(err)
	eq(ns, map[string]int64{both: 0})
}

func tx_new_types() {
	must(r.Del(ctx, P+"txl", P+"txz", P+"txs", P+"w", P+"txstr").Err())
	p := r.TxPipeline()
	rp := p.RPush(ctx, P+"txl", "a", "b")
	zp := p.ZAdd(ctx, P+"txz", zm(1.5, "m"), zm(2, "n"))
	sp := p.SAdd(ctx, P+"txs", "x", "y")
	lr := p.LRange(ctx, P+"txl", 0, -1)
	zr := p.ZRange(ctx, P+"txz", 0, -1)
	sc := p.SCard(ctx, P+"txs")
	_, err := p.Exec(ctx)
	must(err)
	eq([]int64{rp.Val(), zp.Val(), sp.Val(), sc.Val()}, []int64{2, 2, 2, 2})
	eq(lr.Val(), ss("a", "b"))
	eq(zr.Val(), ss("m", "n"))
	p = r.TxPipeline()
	lp := p.LPop(ctx, P+"txl")
	zrm := p.ZRem(ctx, P+"txz", "m")
	spp := p.SPopN(ctx, P+"txs", 2)
	_, err = p.Exec(ctx)
	must(err)
	eq(lp.Val(), "a")
	eq(zrm.Val(), int64(1))
	eq(sorted(spp.Val()), ss("x", "y"))
	must(r.Set(ctx, P+"txstr", "s", 0).Err())
	// an error inside EXEC does not roll back the other commands
	p = r.TxPipeline()
	c0 := p.RPush(ctx, P+"txl", "q")
	c1 := p.LPush(ctx, P+"txstr", "bad")
	c2 := p.LLen(ctx, P+"txl")
	_, err = p.Exec(ctx)
	assert(err != nil, "EXEC should report the failed command")
	eq(c0.Val(), int64(2))
	raises(func() error { return c1.Err() }, "WRONGTYPE")
	eq(c2.Val(), int64(2))
	// WATCH aborts EXEC when the key changed
	must(r.Set(ctx, P+"w", "1", 0).Err())
	err = r.Watch(ctx, func(tx *redis.Tx) error {
		must(r.Set(ctx, P+"w", "2", 0).Err())
		_, err := tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
			pipe.ZAdd(ctx, P+"txz", zm(1, "w"))
			return nil
		})
		return err
	}, P+"w")
	assert(errors.Is(err, redis.TxFailedErr), fmt.Sprintf("EXEC should have aborted, got %v", err))
	_, err = r.ZScore(ctx, P+"txz", "w").Result()
	eq(isNil(err), true)
	must(r.Del(ctx, P+"txl", P+"txz", P+"txs", P+"w", P+"txstr").Err())
}

func lua_new_types() {
	z, l, s := P+"lz", P+"ll", P+"ls"
	must(r.Del(ctx, z, l, s).Err())
	script := `
redis.call('ZADD', KEYS[1], 1.5, 'a', 2, 'b', 3, 'c')
redis.call('RPUSH', KEYS[2], 'x', 'y', 'z')
redis.call('SADD', KEYS[3], 'm', 'n')
return {redis.call('ZRANGE', KEYS[1], 0, -1), redis.call('LRANGE', KEYS[2], 0, -1),
        redis.call('SCARD', KEYS[3]), redis.call('ZSCORE', KEYS[1], 'a'),
        redis.call('ZRANGE', KEYS[1], 0, 0, 'WITHSCORES'), redis.call('TYPE', KEYS[2])['ok']}`
	out, err := r.Eval(ctx, script, []string{z, l, s}).Result()
	must(err)
	eq(out, []any{[]any{"a", "b", "c"}, []any{"x", "y", "z"}, int64(2), "1.5", []any{"a", "1.5"}, "list"})
	eq(strs(r.ZRange(ctx, z, 0, -1)), ss("a", "b", "c"))
	out, err = r.Eval(ctx, "return redis.call('ZPOPMIN', KEYS[1])", []string{z}).Result()
	must(err)
	eq(out, []any{"a", "1.5"})
	out, err = r.Eval(ctx, "return redis.call('LPOP', KEYS[1], 2)", []string{l}).Result()
	must(err)
	eq(out, []any{"x", "y"})
	out, err = r.Eval(ctx, "return redis.call('SMEMBERS', KEYS[1])", []string{s}).Result()
	must(err)
	var members []string
	for _, m := range out.([]any) {
		members = append(members, m.(string))
	}
	eq(sorted(members), ss("m", "n"))
	out, err = r.Eval(ctx, "return redis.call('ZINCRBY', KEYS[1], 0.5, 'b')", []string{z}).Result()
	must(err)
	eq(out, "2.5")
	out, err = r.Eval(ctx, "return redis.call('EXISTS', KEYS[1])", []string{P + "none"}).Result()
	must(err)
	eq(out, int64(0))
	raises(func() error { return r.Eval(ctx, "return redis.call('LPUSH', KEYS[1], 'x')", []string{z}).Err() }, "WRONGTYPE")
	out, err = r.Eval(ctx, "return redis.pcall('LPUSH', KEYS[1], 'x')['err'] ~= nil", []string{z}).Result()
	must(err)
	eq(out, int64(1))
	sc := redis.NewScript("return redis.call('ZRANGEBYSCORE', KEYS[1], ARGV[1], ARGV[2], 'WITHSCORES')")
	out, err = sc.Run(ctx, r, []string{z}, "-inf", "+inf").Result()
	must(err)
	eq(out, []any{"b", "2.5", "c", "3"})
	must(r.Del(ctx, z, l, s).Err())
}

func config_info() {
	mm, err := r.ConfigGet(ctx, "maxmemory").Result()
	must(err)
	v, found := mm["maxmemory"]
	assert(found, fmt.Sprintf("CONFIG GET maxmemory: %v", mm))
	_, err = strconv.ParseInt(v, 10, 64)
	must(err)
	pol, err := r.ConfigGet(ctx, "maxmemory-policy").Result()
	must(err)
	assert(pol["maxmemory-policy"] != "", fmt.Sprintf("CONFIG GET maxmemory-policy: %v", pol))
	both, err := r.ConfigGet(ctx, "maxmemory*").Result()
	must(err)
	_, a := both["maxmemory"]
	_, b := both["maxmemory-policy"]
	assert(a && b, fmt.Sprintf("CONFIG GET maxmemory*: %v", both))
	info, err := r.Info(ctx).Result()
	must(err)
	assert(strings.Contains(info, ":"), "INFO has no key:value line")
	for _, sec := range []string{"memory", "keyspace"} {
		_, err = r.Info(ctx, sec).Result()
		must(err)
	}
}

func main() {
	r = newClient()
	checks := []struct {
		name string
		fn   func()
	}{
		{"connect_ping", connect_ping}, {"set_get", set_get},
		{"binary_key_and_value", binary_key_and_value}, {"big_value_8mib", big_value_8mib},
		{"counters", counters}, {"counter_errors", counter_errors}, {"hashes", hashes},
		{"wrong_type", wrong_type}, {"mset_mget", mset_mget},
		{"del_exists_unlink", del_exists_unlink}, {"keys_pattern", keys_pattern},
		{"eval_values", eval_values}, {"eval_calls_redis", eval_calls_redis},
		{"eval_errors", eval_errors}, {"script_helper_evalsha", script_helper_evalsha},
		{"pipeline_plain_10000", pipeline_plain_10000},
		{"pipeline_with_error_in_middle", pipeline_with_error_in_middle},
		{"x_pipeline_transaction_multi_exec", x_pipeline_transaction_multi_exec},
		{"x_set_with_expiry_and_nx", x_set_with_expiry_and_nx},
		{"concurrency_16_threads", concurrency_16_threads},
		{"unknown_command_keeps_connection", unknown_command_keeps_connection},
		{"hgetall_100k_fields", hgetall_100k_fields},
		{"client_side_commands", client_side_commands},
		{"lists", lists}, {"sets", sets}, {"zsets_basic", zsets_basic}, {"zset_scores", zset_scores},
		{"zset_pop", zset_pop}, {"zsets_ranges", zsets_ranges}, {"zsets_store", zsets_store},
		{"string_key_extras", string_key_extras}, {"hrandfield_values", hrandfield_values},
		{"keys_misc", keys_misc}, {"copy_keys", copy_keys},
		{"pubsub_roundtrip", pubsub_roundtrip}, {"tx_new_types", tx_new_types},
		{"lua_new_types", lua_new_types}, {"config_info", config_info},
		{"flushall_dbsize", flushall_dbsize},
	}
	p := proto
	if p == "" {
		p = "default"
	}
	fmt.Printf("== go go-redis %s protocol=%s\n", redis.Version(), p)
	for _, c := range checks {
		check(c.name, c.fn)
	}
	fmt.Printf("== go: %d/%d passed; failed: [%s]\n", len(checks)-len(failed), len(checks), strings.Join(failed, ", "))
	if len(failed) > 0 {
		os.Exit(1)
	}
}
