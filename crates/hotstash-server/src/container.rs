//! What the process takes from where it runs: the memory a container lets
//! it use, transparent huge pages, and the signals that ask it to stop.

/// What `--maxmemory auto` leaves for the data under a memory limit. The
/// rest is for what the allocator holds beyond what it hands out, thread
/// stacks, buffers and Lua: a quarter, at least 128 MiB, but never more
/// than half, so a small container still gets half for data.
pub fn auto_maxmemory(limit: u64) -> u64 {
    const RESERVE_MIN: u64 = 128 << 20;
    limit - (limit / 4).max(RESERVE_MIN.min(limit / 2))
}

/// The cgroup v2 path of the process from `/proc/self/cgroup`: the `0::`
/// line. A path that climbs out with `..` is not followed.
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
fn v2_path(own: &str) -> Option<&str> {
    let path = own.lines().find_map(|line| line.strip_prefix("0::"))?;
    (!path.split('/').any(|part| part == "..")).then_some(path)
}

/// A limit as cgroup files write it: bytes, or `max` for none. cgroup v1
/// writes no limit as a number near 2^63.
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
fn parse_limit(text: &str) -> Option<u64> {
    const NO_LIMIT: u64 = 1 << 62;
    text.trim().parse().ok().filter(|&n| n < NO_LIMIT)
}

/// The memory limit the process runs under: the smallest `memory.max` or
/// `memory.high` of its cgroup and those above it (v2), else the v1
/// `memory.limit_in_bytes`. `None` when nothing limits it.
#[cfg(target_os = "linux")]
pub fn memory_limit() -> Option<u64> {
    use std::path::{Path, PathBuf};
    const ROOT: &str = "/sys/fs/cgroup";
    let read = |path: &Path| {
        std::fs::read_to_string(path)
            .ok()
            .as_deref()
            .and_then(parse_limit)
    };
    let own = std::fs::read_to_string("/proc/self/cgroup").unwrap_or_default();
    if let Some(path) = v2_path(&own) {
        let mut dir = PathBuf::from(ROOT).join(path.trim_start_matches('/'));
        let mut limit: Option<u64> = None;
        loop {
            for file in ["memory.max", "memory.high"] {
                if let Some(n) = read(&dir.join(file)) {
                    limit = Some(limit.map_or(n, |l| l.min(n)));
                }
            }
            if dir == Path::new(ROOT) || !dir.pop() {
                break;
            }
        }
        if limit.is_some() {
            return limit;
        }
    }
    read(Path::new("/sys/fs/cgroup/memory/memory.limit_in_bytes"))
}

#[cfg(not(target_os = "linux"))]
pub fn memory_limit() -> Option<u64> {
    None
}

/// Turns transparent huge pages off for this process, as Redis and Valkey
/// do by default: a huge page faulted in for a few small keys holds 2 MiB,
/// and compacting memory for them stalls the threads that touch it. With
/// them, a million small keys took 180 bytes each instead of 147.
#[cfg(target_os = "linux")]
pub fn disable_thp() -> std::io::Result<()> {
    let off: libc::c_ulong = 1;
    let unused: libc::c_ulong = 0;
    // SAFETY: PR_SET_THP_DISABLE reads only its integer arguments and
    // changes a flag of this process; no memory of ours is involved.
    let rc = unsafe { libc::prctl(libc::PR_SET_THP_DISABLE, off, unused, unused, unused) };
    match rc {
        0 => Ok(()),
        _ => Err(std::io::Error::last_os_error()),
    }
}

#[cfg(not(target_os = "linux"))]
pub fn disable_thp() -> std::io::Result<()> {
    Ok(())
}

/// SIGTERM and SIGINT, held back from every thread and taken by `wait`
/// instead of a handler, so stopping runs as ordinary code. Made before
/// any other thread starts: threads inherit the mask of their parent.
#[cfg(unix)]
pub struct Signals(libc::sigset_t);

#[cfg(unix)]
impl Signals {
    pub fn block() -> std::io::Result<Signals> {
        // SAFETY: an all-zero sigset_t is a valid value that sigemptyset
        // then sets; every pointer is to a local that outlives the call.
        unsafe {
            let mut set: libc::sigset_t = std::mem::zeroed();
            libc::sigemptyset(&mut set);
            libc::sigaddset(&mut set, libc::SIGTERM);
            libc::sigaddset(&mut set, libc::SIGINT);
            match libc::pthread_sigmask(libc::SIG_BLOCK, &set, std::ptr::null_mut()) {
                0 => Ok(Signals(set)),
                rc => Err(std::io::Error::from_raw_os_error(rc)),
            }
        }
    }

    /// Waits for one of the signals and returns its number.
    pub fn wait(&self) -> std::io::Result<i32> {
        let mut signal = 0;
        // SAFETY: the set was made by `block`; `signal` outlives the call.
        match unsafe { libc::sigwait(&self.0, &mut signal) } {
            0 => Ok(signal),
            rc => Err(std::io::Error::from_raw_os_error(rc)),
        }
    }
}

#[cfg(unix)]
pub fn signal_name(signal: i32) -> &'static str {
    match signal {
        libc::SIGTERM => "SIGTERM",
        libc::SIGINT => "SIGINT",
        _ => "a signal",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const MIB: u64 = 1 << 20;

    #[test]
    fn auto_maxmemory_keeps_a_quarter_but_at_least_128_mib_and_at_most_half() {
        assert_eq!(auto_maxmemory(4096 * MIB), 3072 * MIB);
        assert_eq!(auto_maxmemory(1024 * MIB), 768 * MIB);
        assert_eq!(auto_maxmemory(512 * MIB), 384 * MIB);
        assert_eq!(auto_maxmemory(256 * MIB), 128 * MIB);
        assert_eq!(auto_maxmemory(128 * MIB), 64 * MIB);
        assert_eq!(auto_maxmemory(64 * MIB), 32 * MIB);
        assert_eq!(auto_maxmemory(0), 0);
    }

    #[test]
    fn v2_path_is_the_unified_line() {
        let own = "12:memory:/docker/abc\n0::/kubepods/pod1/c1\n";
        assert_eq!(v2_path(own), Some("/kubepods/pod1/c1"));
        assert_eq!(v2_path("0::/\n"), Some("/"));
        assert_eq!(v2_path("4:memory:/x\n"), None);
        assert_eq!(v2_path("0::/../..\n"), None);
        assert_eq!(v2_path(""), None);
    }

    #[test]
    fn parse_limit_reads_bytes_and_no_limit() {
        assert_eq!(parse_limit("536870912\n"), Some(512 * MIB));
        assert_eq!(parse_limit("max\n"), None);
        assert_eq!(parse_limit("9223372036854771712\n"), None);
        assert_eq!(parse_limit(""), None);
        assert_eq!(parse_limit("-1"), None);
    }
}
