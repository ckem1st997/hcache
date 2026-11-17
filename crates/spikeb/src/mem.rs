//! Process memory counters.
//!
//! Windows: working set, private usage (committed private bytes) and peak
//! working set. Linux: VmRSS, VmHWM and resident-minus-shared pages as the
//! private estimate (4 KiB pages assumed).

#[derive(Debug, Clone, Copy)]
pub struct MemSample {
    pub rss: u64,
    pub private: u64,
    pub peak_rss: u64,
}

#[cfg(windows)]
pub fn sample() -> Result<MemSample, String> {
    use windows_sys::Win32::System::ProcessStatus::{
        K32GetProcessMemoryInfo, PROCESS_MEMORY_COUNTERS, PROCESS_MEMORY_COUNTERS_EX,
    };
    use windows_sys::Win32::System::Threading::GetCurrentProcess;

    // SAFETY: PROCESS_MEMORY_COUNTERS_EX is plain integers; all-zero is valid.
    let mut c: PROCESS_MEMORY_COUNTERS_EX = unsafe { std::mem::zeroed() };
    let cb = std::mem::size_of::<PROCESS_MEMORY_COUNTERS_EX>() as u32;
    c.cb = cb;
    // SAFETY: the pseudo-handle from GetCurrentProcess is always valid, and the
    // buffer is a live PROCESS_MEMORY_COUNTERS_EX (its prefix is
    // PROCESS_MEMORY_COUNTERS) of exactly `cb` bytes.
    let ok = unsafe {
        K32GetProcessMemoryInfo(
            GetCurrentProcess(),
            (&mut c as *mut PROCESS_MEMORY_COUNTERS_EX).cast::<PROCESS_MEMORY_COUNTERS>(),
            cb,
        )
    };
    if ok == 0 {
        return Err(format!(
            "K32GetProcessMemoryInfo failed: {}",
            std::io::Error::last_os_error()
        ));
    }
    Ok(MemSample {
        rss: c.WorkingSetSize as u64,
        private: c.PrivateUsage as u64,
        peak_rss: c.PeakWorkingSetSize as u64,
    })
}

#[cfg(not(windows))]
pub fn sample() -> Result<MemSample, String> {
    const PAGE: u64 = 4096;
    let status = std::fs::read_to_string("/proc/self/status")
        .map_err(|e| format!("read /proc/self/status: {e}"))?;
    let kb = |name: &str| -> Result<u64, String> {
        status
            .lines()
            .find_map(|l| l.strip_prefix(name))
            .and_then(|rest| rest.split_whitespace().next())
            .and_then(|n| n.parse::<u64>().ok())
            .map(|n| n * 1024)
            .ok_or_else(|| format!("{name} not found in /proc/self/status"))
    };
    let statm = std::fs::read_to_string("/proc/self/statm")
        .map_err(|e| format!("read /proc/self/statm: {e}"))?;
    let mut f = statm.split_whitespace().skip(1);
    let resident: u64 = f
        .next()
        .and_then(|s| s.parse().ok())
        .ok_or("bad /proc/self/statm")?;
    let shared: u64 = f
        .next()
        .and_then(|s| s.parse().ok())
        .ok_or("bad /proc/self/statm")?;
    Ok(MemSample {
        rss: kb("VmRSS:")?,
        private: resident.saturating_sub(shared) * PAGE,
        peak_rss: kb("VmHWM:")?,
    })
}
