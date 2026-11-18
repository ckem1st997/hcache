//! Load generator for the Spike A bad cases. Speaks plain RESP over TCP,
//! so the same scenarios run against Hotstash and against Valkey. `embedded`
//! is the exception: it drives Hotstash in-process.

mod hist;
mod wire;

use hist::Hist;
use hotstash_core::resp::{encode_command, Reply};
use hotstash_core::testutil::Rng;
use std::collections::HashMap;
use std::io::{self, Read, Write};
use std::net::TcpStream;
use std::process::ExitCode;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use wire::Scanner;

const USAGE: &str = "\
usage: hload <scenario> [--option value ...]

scenarios
  baseline   GET/SET mix                      (reference numbers)
  scan       case 1: p99 of other clients while KEYS and HGETALL run
  hashtag    case 2: every key carries the same hash tag
  eval       case 3: EVAL per second
  embedded   case 4: in-process, many application threads
  kill       case 5: SCRIPT KILL against a busy script
  churn      case 6: overwrite/delete churn with mixed value sizes
  multi      case 7: MSET/MGET and exclusive commands amid skewed load

options (defaults)
  --addr 127.0.0.1:6379   --secs 5        --clients 16   --pipeline 1
  --keys 100000           --value 64      --write-pct 10 --zipf 0 (0 = uniform)
  --big 1000000 (fields of the big hash, scan)
  --threads 64  --shards 64 (embedded)
  --multi-clients 4 --multi-keys 8 (multi)
  --seed 1";

type Gen = Box<dyn FnMut(&mut Rng, &mut Vec<u8>) + Send>;

struct Opts(HashMap<String, String>);

impl Opts {
    fn parse(args: &[String]) -> Result<Opts, String> {
        let mut map = HashMap::new();
        let mut it = args.iter();
        while let Some(flag) = it.next() {
            let name = flag
                .strip_prefix("--")
                .ok_or_else(|| format!("unexpected argument {flag}"))?;
            let value = it.next().ok_or_else(|| format!("{flag} needs a value"))?;
            map.insert(name.to_string(), value.clone());
        }
        Ok(Opts(map))
    }

    fn get<T: std::str::FromStr>(&self, name: &str, default: T) -> Result<T, String> {
        match self.0.get(name) {
            Some(v) => v.parse().map_err(|_| format!("--{name}: bad value {v}")),
            None => Ok(default),
        }
    }
}

/// Which key the next command uses.
#[derive(Clone)]
struct KeyDist {
    n: u64,
    /// Cumulative probabilities by rank, for a Zipf distribution.
    cdf: Option<Arc<Vec<f64>>>,
    prefix: &'static str,
}

impl KeyDist {
    fn new(n: u64, zipf: f64, prefix: &'static str) -> KeyDist {
        let n = n.max(1);
        let cdf = (zipf > 0.0).then(|| {
            let mut acc = 0.0;
            let mut cdf: Vec<f64> = (1..=n)
                .map(|rank| {
                    acc += 1.0 / (rank as f64).powf(zipf);
                    acc
                })
                .collect();
            for p in &mut cdf {
                *p /= acc;
            }
            Arc::new(cdf)
        });
        KeyDist { n, cdf, prefix }
    }

    fn pick(&self, rng: &mut Rng) -> u64 {
        match &self.cdf {
            None => rng.below(self.n),
            Some(cdf) => {
                let u = (rng.next_u64() >> 11) as f64 / (1u64 << 53) as f64;
                let rank = cdf.partition_point(|&p| p < u) as u64;
                // Spread the hot ranks over the keyspace (and the shards).
                rank.min(self.n - 1).wrapping_mul(0x9E37_79B9_7F4A_7C15) % self.n
            }
        }
    }

    fn key(&self, i: u64) -> Vec<u8> {
        format!("{}{:012}", self.prefix, i).into_bytes()
    }

    fn next(&self, rng: &mut Rng) -> Vec<u8> {
        self.key(self.pick(rng))
    }
}

struct Group {
    name: &'static str,
    clients: usize,
    pipeline: usize,
    /// Pause between batches; `None` means as fast as possible.
    pace: Option<Duration>,
    gen: Box<dyn Fn(usize) -> Gen + Send + Sync>,
}

#[derive(Default)]
struct Outcome {
    hist: Hist,
    ops: u64,
    errors: u64,
}

fn connect(addr: &str) -> io::Result<TcpStream> {
    let stream = TcpStream::connect(addr)?;
    stream.set_nodelay(true)?;
    Ok(stream)
}

/// One client: send a batch, wait for all its replies, repeat.
fn client(
    addr: &str,
    seed: u64,
    pipeline: usize,
    pace: Option<Duration>,
    mut gen: Gen,
    stop: &AtomicBool,
) -> io::Result<Outcome> {
    let mut stream = connect(addr)?;
    let mut rng = Rng::new(seed);
    let mut out = Outcome::default();
    let mut request = Vec::new();
    let mut scanner = Scanner::default();
    let mut chunk = vec![0u8; 64 * 1024];
    while !stop.load(Ordering::Relaxed) {
        request.clear();
        for _ in 0..pipeline {
            gen(&mut rng, &mut request);
        }
        let started = Instant::now();
        stream.write_all(&request)?;
        let mut pending = pipeline;
        while pending > 0 {
            let n = stream.read(&mut chunk)?;
            if n == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "server closed the connection",
                ));
            }
            let (done, errors) = scanner
                .feed(&chunk[..n])
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            pending = pending.saturating_sub(done);
            out.errors += errors as u64;
        }
        out.hist
            .record_n(started.elapsed().as_nanos() as u64, pipeline as u64);
        out.ops += pipeline as u64;
        if let Some(pace) = pace {
            std::thread::sleep(pace);
        }
    }
    Ok(out)
}

/// Run all groups side by side for `secs`; one merged outcome per group.
fn run(addr: &str, seed: u64, secs: f64, groups: Vec<Group>) -> io::Result<Vec<Outcome>> {
    let stop = AtomicBool::new(false);
    let results = std::thread::scope(|scope| {
        let mut handles = Vec::new();
        for (g, group) in groups.iter().enumerate() {
            for c in 0..group.clients {
                let gen = (group.gen)(c);
                let seed = seed ^ ((g as u64) << 32) ^ (c as u64 + 1).wrapping_mul(0x9E37_79B9);
                let stop = &stop;
                handles.push((
                    g,
                    scope.spawn(move || client(addr, seed, group.pipeline, group.pace, gen, stop)),
                ));
            }
        }
        std::thread::sleep(Duration::from_secs_f64(secs));
        stop.store(true, Ordering::Relaxed);
        let mut merged: Vec<Outcome> = groups.iter().map(|_| Outcome::default()).collect();
        for (g, handle) in handles {
            let outcome = handle
                .join()
                .map_err(|_| io::Error::other("a client thread panicked"))??;
            merged[g].hist.merge(&outcome.hist);
            merged[g].ops += outcome.ops;
            merged[g].errors += outcome.errors;
        }
        Ok::<_, io::Error>(merged)
    })?;
    for (group, outcome) in groups.iter().zip(&results) {
        report(group.name, outcome, secs);
    }
    Ok(results)
}

fn report(name: &str, o: &Outcome, secs: f64) {
    println!(
        "{name:<14} {:>10.0} ops/s  p50 {:>9}  p99 {:>9}  p99.9 {:>9}  max {:>9}  errors {}",
        o.ops as f64 / secs,
        micros(o.hist.quantile(0.50)),
        micros(o.hist.quantile(0.99)),
        micros(o.hist.quantile(0.999)),
        micros(o.hist.max()),
        o.errors
    );
}

fn micros(nanos: u64) -> String {
    format!("{:.1}us", nanos as f64 / 1000.0)
}

/// One command, one reply, on a fresh connection.
fn request<A: AsRef<[u8]>>(addr: &str, args: &[A]) -> io::Result<Reply> {
    let mut stream = connect(addr)?;
    roundtrip(&mut stream, args)
}

fn roundtrip<A: AsRef<[u8]>>(stream: &mut TcpStream, args: &[A]) -> io::Result<Reply> {
    let mut buf = Vec::new();
    encode_command(args, &mut buf);
    stream.write_all(&buf)?;
    wire::read_reply(stream)
}

/// Send `count` commands over one connection, in pipelined batches.
fn preload(addr: &str, count: u64, mut command: impl FnMut(u64, &mut Vec<u8>)) -> io::Result<()> {
    const BATCH: u64 = 1000;
    let mut stream = connect(addr)?;
    let mut scanner = Scanner::default();
    let mut chunk = vec![0u8; 64 * 1024];
    let mut buf = Vec::new();
    let mut i = 0;
    while i < count {
        let end = (i + BATCH).min(count);
        buf.clear();
        (i..end).for_each(|j| command(j, &mut buf));
        stream.write_all(&buf)?;
        let mut pending = (end - i) as usize;
        while pending > 0 {
            let n = stream.read(&mut chunk)?;
            if n == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "server closed the connection",
                ));
            }
            let (done, errors) = scanner
                .feed(&chunk[..n])
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            if errors > 0 {
                return Err(io::Error::other(
                    "the server answered a preload command with an error",
                ));
            }
            pending = pending.saturating_sub(done);
        }
        i = end;
    }
    Ok(())
}

fn preload_keys(addr: &str, dist: &KeyDist, value: &[u8]) -> io::Result<()> {
    preload(addr, dist.n, |i, buf| {
        encode_command(&[b"SET".as_ref(), &dist.key(i), value], buf)
    })
}

/// Lines of INFO that are `name:number`, for before/after comparisons.
fn info_counters(addr: &str) -> io::Result<HashMap<String, i64>> {
    let Reply::Bulk(text) = request(addr, &["INFO"])? else {
        return Ok(HashMap::new());
    };
    let text = String::from_utf8_lossy(&text);
    Ok(text
        .lines()
        .filter_map(|l| l.split_once(':'))
        .filter_map(|(k, v)| Some((k.to_string(), v.trim().parse().ok()?)))
        .collect())
}

fn print_counter_deltas(
    before: &HashMap<String, i64>,
    after: &HashMap<String, i64>,
    names: &[&str],
) {
    for name in names {
        if let (Some(b), Some(a)) = (before.get(*name), after.get(*name)) {
            println!("  {name}: +{}", a - b);
        }
    }
}

struct Common {
    addr: String,
    secs: f64,
    clients: usize,
    pipeline: usize,
    value: Vec<u8>,
    write_pct: u64,
    seed: u64,
    keys: u64,
    zipf: f64,
}

impl Common {
    fn new(o: &Opts) -> Result<Common, String> {
        Ok(Common {
            addr: o.get("addr", "127.0.0.1:6379".to_string())?,
            secs: o.get("secs", 5.0)?,
            clients: o.get("clients", 16)?,
            pipeline: o.get::<usize>("pipeline", 1)?.max(1),
            value: vec![b'x'; o.get("value", 64)?],
            write_pct: o.get("write-pct", 10)?,
            seed: o.get("seed", 1)?,
            keys: o.get("keys", 100_000)?,
            zipf: o.get("zipf", 0.0)?,
        })
    }

    /// The GET/SET mix every scenario uses as its foreground load.
    fn mix(&self, name: &'static str, dist: &KeyDist) -> Group {
        let (dist, value, write_pct) = (dist.clone(), self.value.clone(), self.write_pct);
        Group {
            name,
            clients: self.clients,
            pipeline: self.pipeline,
            pace: None,
            gen: Box::new(move |_| {
                let (dist, value) = (dist.clone(), value.clone());
                Box::new(move |rng, buf| {
                    let key = dist.next(rng);
                    if rng.below(100) < write_pct {
                        encode_command(&[b"SET".as_ref(), &key, &value], buf);
                    } else {
                        encode_command(&[b"GET".as_ref(), &key], buf);
                    }
                })
            }),
        }
    }
}

fn single(
    name: &'static str,
    pace: Option<Duration>,
    gen: impl Fn() -> Gen + Send + Sync + 'static,
) -> Group {
    Group {
        name,
        clients: 1,
        pipeline: 1,
        pace,
        gen: Box::new(move |_| gen()),
    }
}

fn baseline(c: &Common, prefix: &'static str) -> io::Result<()> {
    let dist = KeyDist::new(c.keys, c.zipf, prefix);
    preload_keys(&c.addr, &dist, &c.value)?;
    run(&c.addr, c.seed, c.secs, vec![c.mix("get/set", &dist)])?;
    Ok(())
}

fn scan(c: &Common, o: &Opts) -> Result<(), String> {
    let big: u64 = o.get("big", 1_000_000)?;
    let dist = KeyDist::new(c.keys, c.zipf, "key:");
    let io = |e: io::Error| e.to_string();
    preload_keys(&c.addr, &dist, &c.value).map_err(io)?;
    preload(&c.addr, big, |i, buf| {
        encode_command(
            &[
                b"HSET".as_ref(),
                b"bighash",
                format!("field:{i}").as_bytes(),
                b"value",
            ],
            buf,
        );
    })
    .map_err(io)?;
    println!("-- quiet");
    run(&c.addr, c.seed, c.secs, vec![c.mix("get/set", &dist)]).map_err(io)?;
    println!("-- while HGETALL on {big} fields runs in a loop");
    let hgetall = single("hgetall", None, || {
        Box::new(|_, buf| encode_command(&["HGETALL", "bighash"], buf))
    });
    run(
        &c.addr,
        c.seed,
        c.secs,
        vec![c.mix("get/set", &dist), hgetall],
    )
    .map_err(io)?;
    println!("-- while KEYS * over {} keys runs in a loop", c.keys + 1);
    let keys = single("keys", None, || {
        Box::new(|_, buf| encode_command(&["KEYS", "*"], buf))
    });
    run(&c.addr, c.seed, c.secs, vec![c.mix("get/set", &dist), keys]).map_err(io)?;
    Ok(())
}

fn eval(c: &Common) -> io::Result<()> {
    const SCRIPT: &str = "return redis.call('INCR', KEYS[1])";
    let dist = KeyDist::new(c.keys, c.zipf, "ctr:");
    let group = Group {
        name: "eval",
        clients: c.clients,
        pipeline: c.pipeline,
        pace: None,
        gen: Box::new(move |_| {
            let dist = dist.clone();
            Box::new(move |rng, buf| {
                encode_command(
                    &[b"EVAL".as_ref(), SCRIPT.as_bytes(), b"1", &dist.next(rng)],
                    buf,
                )
            })
        }),
    };
    run(&c.addr, c.seed, c.secs, vec![group])?;
    Ok(())
}

fn embedded(c: &Common, o: &Opts) -> Result<(), String> {
    let threads: usize = o.get("threads", 64)?;
    let shards: usize = o.get("shards", 64)?;
    let db = hotstash::Db::open(
        None,
        hotstash::Config {
            shards,
            ..hotstash::Config::default()
        },
    )
    .map_err(|e| e.to_string())?;
    let dist = KeyDist::new(c.keys, c.zipf, "key:");
    {
        let session = db.session().map_err(|e| e.to_string())?;
        for i in 0..dist.n {
            session.execute(&[b"SET".as_ref(), &dist.key(i), &c.value]);
        }
    }
    let stop = AtomicBool::new(false);
    let errors = AtomicU64::new(0);
    let mut total = Outcome::default();
    std::thread::scope(|scope| -> Result<(), String> {
        let mut handles = Vec::new();
        for t in 0..threads {
            let session = db.session().map_err(|e| e.to_string())?;
            let (dist, stop, errors) = (&dist, &stop, &errors);
            handles.push(scope.spawn(move || {
                let mut rng = Rng::new(c.seed ^ (t as u64 + 1).wrapping_mul(0x9E37_79B9));
                let mut out = Outcome::default();
                while !stop.load(Ordering::Relaxed) {
                    let key = dist.next(&mut rng);
                    let started = Instant::now();
                    let reply = if rng.below(100) < c.write_pct {
                        session.execute(&[b"SET".as_ref(), &key, &c.value])
                    } else {
                        session.execute(&[b"GET".as_ref(), &key])
                    };
                    out.hist.record_n(started.elapsed().as_nanos() as u64, 1);
                    out.ops += 1;
                    if reply.is_error() {
                        errors.fetch_add(1, Ordering::Relaxed);
                    }
                }
                out
            }));
        }
        std::thread::sleep(Duration::from_secs_f64(c.secs));
        stop.store(true, Ordering::Relaxed);
        for handle in handles {
            let out = handle
                .join()
                .map_err(|_| "an application thread panicked".to_string())?;
            total.hist.merge(&out.hist);
            total.ops += out.ops;
        }
        Ok(())
    })?;
    total.errors = errors.load(Ordering::Relaxed);
    println!("-- embedded, {threads} threads, {shards} shards");
    report("get/set", &total, c.secs);
    println!(
        "  deferred by event loops: {} (expected 0: nothing but application threads here)",
        db.stats().deferred
    );
    Ok(())
}

fn kill(c: &Common) -> io::Result<()> {
    const BUSY: &str = "local i = 0 while true do i = i + 1 end";
    const BEFORE_KILL: Duration = Duration::from_secs(1);
    const GIVE_UP: Duration = Duration::from_secs(30);
    let addr = c.addr.as_str();
    std::thread::scope(|scope| {
        let script = scope.spawn(move || -> io::Result<(Reply, Duration)> {
            let started = Instant::now();
            let reply = request(addr, &["EVAL", BUSY, "0"])?;
            Ok((reply, started.elapsed()))
        });
        let mut conn = connect(addr)?;
        std::thread::sleep(Duration::from_millis(100));
        let mut pings = Hist::default();
        let begun = Instant::now();
        while begun.elapsed() < BEFORE_KILL {
            let started = Instant::now();
            roundtrip(&mut conn, &["PING"])?;
            pings.record_n(started.elapsed().as_nanos() as u64, 1);
            std::thread::sleep(Duration::from_millis(1));
        }
        println!(
            "PING while the script runs: p50 {}  p99 {}  max {}",
            micros(pings.quantile(0.5)),
            micros(pings.quantile(0.99)),
            micros(pings.max())
        );
        let kill_sent = Instant::now();
        loop {
            let reply = roundtrip(&mut conn, &["SCRIPT", "KILL"])?;
            if reply == Reply::Ok {
                break;
            }
            if kill_sent.elapsed() > GIVE_UP {
                return Err(io::Error::other(format!(
                    "SCRIPT KILL never got through: {reply:?}"
                )));
            }
            std::thread::sleep(Duration::from_millis(1));
        }
        println!(
            "SCRIPT KILL answered after {}",
            micros(kill_sent.elapsed().as_nanos() as u64)
        );
        let (reply, ran) = script
            .join()
            .map_err(|_| io::Error::other("the script thread panicked"))??;
        println!(
            "script ended {} after the kill was sent (ran {:.2}s): {reply:?}",
            micros(kill_sent.elapsed().as_nanos() as u64),
            ran.as_secs_f64()
        );
        let started = Instant::now();
        let reply = roundtrip(&mut conn, &["SET", "after-kill", "1"])?;
        println!(
            "first write after the kill: {reply:?} in {}",
            micros(started.elapsed().as_nanos() as u64)
        );
        Ok(())
    })
}

fn churn(c: &Common) -> io::Result<()> {
    const MAX_VALUE: u64 = 1024;
    let dist = KeyDist::new(c.keys, c.zipf, "key:");
    let blob = vec![b'v'; MAX_VALUE as usize];
    let group = Group {
        name: "churn",
        clients: c.clients,
        pipeline: c.pipeline,
        pace: None,
        gen: Box::new(move |_| {
            let (dist, blob) = (dist.clone(), blob.clone());
            Box::new(move |rng, buf| {
                let key = dist.next(rng);
                if rng.below(100) < 30 {
                    encode_command(&[b"DEL".as_ref(), &key], buf);
                } else {
                    let len = 8 + rng.below(MAX_VALUE - 8) as usize;
                    encode_command(&[b"SET".as_ref(), &key, &blob[..len]], buf);
                }
            })
        }),
    };
    run(&c.addr, c.seed, c.secs, vec![group])?;
    let info = info_counters(&c.addr)?;
    match info.get("used_memory_rss") {
        Some(rss) => println!("  used_memory_rss: {rss}"),
        None => println!("  the server reports no RSS; read it from the operating system"),
    }
    Ok(())
}

fn multi(c: &Common, o: &Opts) -> Result<(), String> {
    const TOUCH: &str = "return redis.call('GET', KEYS[1])";
    let multi_clients: usize = o.get("multi-clients", 4)?;
    let multi_keys: usize = o.get::<usize>("multi-keys", 8)?.max(2);
    let dist = KeyDist::new(c.keys, c.zipf, "key:");
    let io = |e: io::Error| e.to_string();
    preload_keys(&c.addr, &dist, &c.value).map_err(io)?;
    println!("-- single-key load alone");
    run(&c.addr, c.seed, c.secs, vec![c.mix("get/set", &dist)]).map_err(io)?;

    let before = info_counters(&c.addr).map_err(io)?;
    let (mdist, value) = (dist.clone(), c.value.clone());
    let multi = Group {
        name: "mset/mget",
        clients: multi_clients,
        pipeline: 1,
        pace: None,
        gen: Box::new(move |_| {
            let (dist, value) = (mdist.clone(), value.clone());
            Box::new(move |rng, buf| {
                let write = rng.chance(2);
                let mut args: Vec<Vec<u8>> = vec![if write {
                    b"MSET".to_vec()
                } else {
                    b"MGET".to_vec()
                }];
                for _ in 0..multi_keys {
                    args.push(dist.next(rng));
                    if write {
                        args.push(value.clone());
                    }
                }
                encode_command(&args, buf);
            })
        }),
    };
    let edist = dist.clone();
    let exclusive = single("eval", Some(Duration::from_millis(10)), move || {
        let dist = edist.clone();
        Box::new(move |rng, buf| {
            encode_command(
                &[b"EVAL".as_ref(), TOUCH.as_bytes(), b"1", &dist.next(rng)],
                buf,
            )
        })
    });
    println!("-- with {multi_clients} clients on MSET/MGET of {multi_keys} keys and one EVAL every 10 ms");
    run(
        &c.addr,
        c.seed,
        c.secs,
        vec![c.mix("get/set", &dist), multi, exclusive],
    )
    .map_err(io)?;
    let after = info_counters(&c.addr).map_err(io)?;
    print_counter_deltas(
        &before,
        &after,
        &[
            "deferred",
            "multi_deferrals",
            "multi_escalations",
            "exclusive",
            "tasks_run",
        ],
    );
    Ok(())
}

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let Some(scenario) = args.first() else {
        eprintln!("{USAGE}");
        return ExitCode::from(2);
    };
    let result = Opts::parse(&args[1..]).and_then(|o| {
        let c = Common::new(&o)?;
        let io = |e: io::Error| e.to_string();
        match scenario.as_str() {
            "baseline" => baseline(&c, "key:").map_err(io),
            "scan" => scan(&c, &o),
            // Needs `--hash-tags` on the Hotstash side; a standalone Valkey
            // has one keyspace and ignores tags.
            "hashtag" => baseline(&c, "{hot}:").map_err(io),
            "eval" => eval(&c).map_err(io),
            "embedded" => embedded(&c, &o),
            "kill" => kill(&c).map_err(io),
            "churn" => churn(&c).map_err(io),
            "multi" => multi(&c, &o),
            other => Err(format!("unknown scenario {other}\n{USAGE}")),
        }
    });
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("hload: {e}");
            ExitCode::FAILURE
        }
    }
}
