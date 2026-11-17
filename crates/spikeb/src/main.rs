//! Spike B driver: one configuration per process, or `--all` to spawn them.

use std::process::{Command, ExitCode};
use std::time::Instant;

use spikeb::compact::CompactTable;
use spikeb::mem;
use spikeb::pointer::PointerTable;
use spikeb::workload::{expiry_for, fill_value, make_key};
use spikeb::Table;

const USAGE: &str = "usage: spikeb --layout pointer|compact --n <count> --vlen <bytes> --ttl 0|1\n       spikeb --all [--n <count>]";
const DEFAULT_ALL_N: u64 = 10_000_000;
const KEY_LEN: usize = 16;
const TTL_BYTES: usize = 8;
const VERIFY_BUDGET_SECS: f64 = 60.0;
const VERIFY_PROBE: u64 = 100_000;
const MISSING_PROBES: u64 = 10_000;

/// (layout, vlen, ttl) for `--all`: the 12 main configs, then the
/// embedding-threshold runs (compact, no TTL).
fn configs() -> Vec<(&'static str, usize, u8)> {
    let mut v = Vec::new();
    for layout in ["pointer", "compact"] {
        for vlen in [16, 32, 100] {
            for ttl in [0, 1] {
                v.push((layout, vlen, ttl));
            }
        }
    }
    v.extend([8, 48, 64, 128].map(|vlen| ("compact", vlen, 0)));
    v
}

fn parse_args(args: &[String]) -> Result<Vec<(String, String)>, String> {
    let mut out = Vec::new();
    let mut it = args.iter();
    while let Some(flag) = it.next() {
        let name = flag
            .strip_prefix("--")
            .ok_or_else(|| format!("unexpected argument {flag}"))?;
        if name == "all" {
            out.push((name.to_string(), String::new()));
            continue;
        }
        let v = it.next().ok_or_else(|| format!("{flag} needs a value"))?;
        out.push((name.to_string(), v.clone()));
    }
    Ok(out)
}

fn run_one<T: Table>(mut t: T, n: u64, vlen: usize, ttl: bool) -> Result<String, String> {
    let mut key = [0u8; KEY_LEN];
    let mut val = vec![0u8; vlen];
    let before = mem::sample()?;
    let start = Instant::now();
    for i in 0..n {
        make_key(i, &mut key);
        fill_value(i, &mut val);
        t.insert(&key, &val, ttl.then(|| expiry_for(i)))
            .map_err(|e| format!("insert {i}: {e}"))?;
    }
    let load_secs = start.elapsed().as_secs_f64();
    let after = mem::sample()?;
    if t.len() as u64 != n {
        return Err(format!("len {} != {n}", t.len()));
    }

    // Verify: full pass unless the first slice projects past the time budget.
    let mut expect = vec![0u8; vlen];
    let mut check = |i: u64| -> Result<(), String> {
        make_key(i, &mut key);
        fill_value(i, &mut expect);
        if t.get(&key) != Some(expect.as_slice()) {
            return Err(format!("verify: wrong value for key {i}"));
        }
        let want = ttl.then(|| expiry_for(i));
        if t.expiry(&key) != want {
            return Err(format!("verify: wrong expiry for key {i}"));
        }
        Ok(())
    };
    let vstart = Instant::now();
    let probe = n.min(VERIFY_PROBE);
    for i in 0..probe {
        check(i)?;
    }
    let projected = vstart.elapsed().as_secs_f64() * n as f64 / probe.max(1) as f64;
    let stride = if projected > VERIFY_BUDGET_SECS {
        (projected / VERIFY_BUDGET_SECS).ceil() as u64
    } else {
        1
    };
    let mut i = probe;
    while i < n {
        check(i)?;
        i += stride;
    }
    let verify_secs = vstart.elapsed().as_secs_f64();
    let verify = if stride == 1 {
        "full".to_string()
    } else {
        format!("sampled_stride_{stride}")
    };
    // Missing keys: indices never inserted, and a mutated live key.
    for j in 0..MISSING_PROBES {
        make_key(n + j, &mut key);
        if t.get(&key).is_some() {
            return Err(format!("verify: key {} should be missing", n + j));
        }
    }
    make_key(0, &mut key);
    key[KEY_LEN - 1] ^= 1;
    if t.get(&key).is_some() {
        return Err("verify: mutated key should be missing".into());
    }

    let delta = |a: u64, b: u64| (a as f64 - b as f64) / n as f64;
    let stats = t.stats();
    Ok(format!(
        "n={n} vlen={vlen} ttl={} rss_bytes={} private_bytes={} peak_rss_bytes={} \
         bytes_per_key_rss={:.2} bytes_per_key_private={:.2} load_secs={load_secs:.2} \
         verify={verify} verify_secs={verify_secs:.2} {stats}",
        u8::from(ttl),
        after.rss,
        after.private,
        after.peak_rss,
        delta(after.rss, before.rss),
        delta(after.private, before.private),
    ))
}

fn single(opts: &[(String, String)]) -> Result<(), String> {
    let get = |k: &str| {
        opts.iter()
            .find(|(n, _)| n == k)
            .map(|(_, v)| v.as_str())
            .ok_or_else(|| format!("missing --{k}"))
    };
    let num = |k: &str| -> Result<u64, String> {
        get(k)?
            .parse()
            .map_err(|_| format!("--{k} must be a number"))
    };
    let layout = get("layout")?;
    let n = num("n")?;
    let vlen = num("vlen")? as usize;
    let ttl = match num("ttl")? {
        0 => false,
        1 => true,
        _ => return Err("--ttl must be 0 or 1".into()),
    };
    let line = match layout {
        "pointer" => run_one(PointerTable::new(), n, vlen, ttl)?,
        "compact" => run_one(CompactTable::new(), n, vlen, ttl)?,
        _ => return Err("--layout must be pointer or compact".into()),
    };
    println!("layout={layout} {line}");
    Ok(())
}

fn field<'a>(line: &'a str, name: &str) -> Option<&'a str> {
    line.split_whitespace()
        .find_map(|kv| kv.strip_prefix(name)?.strip_prefix('='))
}

fn all(n: u64) -> Result<(), String> {
    let exe = std::env::current_exe().map_err(|e| format!("current_exe: {e}"))?;
    let mut rows = Vec::new();
    for (layout, vlen, ttl) in configs() {
        let out = Command::new(&exe)
            .args(["--layout", layout, "--n", &n.to_string()])
            .args(["--vlen", &vlen.to_string(), "--ttl", &ttl.to_string()])
            .output()
            .map_err(|e| format!("spawn: {e}"))?;
        let stdout = String::from_utf8_lossy(&out.stdout);
        let line = stdout
            .lines()
            .find(|l| l.starts_with("layout="))
            .unwrap_or("");
        if out.status.success() && !line.is_empty() {
            println!("{line}");
            rows.push((layout, vlen, ttl, Some(line.to_string())));
        } else {
            let err = String::from_utf8_lossy(&out.stderr);
            println!(
                "layout={layout} n={n} vlen={vlen} ttl={ttl} FAILED status={} stderr={}",
                out.status,
                err.trim()
            );
            rows.push((layout, vlen, ttl, None));
        }
    }
    println!();
    println!(
        "n = {n}. Memory is Windows working set / private bytes (Linux: VmRSS / resident-shared)."
    );
    println!();
    println!(
        "| layout | vlen | ttl | min B/key | WS B/key | WS overhead | private B/key | private overhead | peak WS B/key | load s | verify |"
    );
    println!("|---|---|---|---|---|---|---|---|---|---|---|");
    for (layout, vlen, ttl, line) in rows {
        let min = (KEY_LEN + vlen + if ttl == 1 { TTL_BYTES } else { 0 }) as f64;
        let Some(line) = line else {
            println!("| {layout} | {vlen} | {ttl} | {min} | FAILED | | | | | | |");
            continue;
        };
        let num = |k: &str| field(&line, k).and_then(|v| v.parse::<f64>().ok());
        let (Some(ws), Some(pv), Some(peak), Some(secs)) = (
            num("bytes_per_key_rss"),
            num("bytes_per_key_private"),
            num("peak_rss_bytes"),
            num("load_secs"),
        ) else {
            println!("| {layout} | {vlen} | {ttl} | {min} | UNPARSEABLE | | | | | | |");
            continue;
        };
        println!(
            "| {layout} | {vlen} | {ttl} | {min:.0} | {ws:.2} | {:.2} | {pv:.2} | {:.2} | {:.2} | {secs:.1} | {} |",
            ws - min,
            pv - min,
            peak / n as f64,
            field(&line, "verify").unwrap_or("?"),
        );
    }
    Ok(())
}

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let result = parse_args(&args).and_then(|opts| {
        if opts.iter().any(|(k, _)| k == "all") {
            let n = match opts.iter().find(|(k, _)| k == "n") {
                Some((_, v)) => v.parse().map_err(|_| "--n must be a number".to_string())?,
                None => DEFAULT_ALL_N,
            };
            all(n)
        } else {
            single(&opts)
        }
    });
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("error: {e}\n{USAGE}");
            ExitCode::from(2)
        }
    }
}
