//! Deterministic keys, values and expiries derived from the key index, so
//! nothing has to be kept around to verify them.

const GOLDEN: u64 = 0x9E37_79B9_7F4A_7C15;
const SEED_KEY_A: u64 = 0xA076_1D64_78BD_642F;
const SEED_KEY_B: u64 = 0xE703_7ED1_A0B4_28DB;
const SEED_VALUE: u64 = 0x8EBC_6AF0_9C88_C6E3;
const SEED_EXPIRY: u64 = 0x5899_65CC_7537_4CC3;

/// SplitMix64 finalizer; a bijection on u64.
pub fn mix(x: u64) -> u64 {
    let mut z = x.wrapping_add(GOLDEN);
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

/// 16-byte key; the first word is a bijection of `i`, so keys are unique.
pub fn make_key(i: u64, out: &mut [u8; 16]) {
    out[..8].copy_from_slice(&mix(i ^ SEED_KEY_A).to_le_bytes());
    out[8..].copy_from_slice(&mix(i ^ SEED_KEY_B).to_le_bytes());
}

/// Pseudo-random bytes of `out.len()` derived from `i`.
pub fn fill_value(i: u64, out: &mut [u8]) {
    let mut state = mix(i ^ SEED_VALUE);
    for chunk in out.chunks_mut(8) {
        state = state.wrapping_add(GOLDEN);
        let word = mix(state).to_le_bytes();
        chunk.copy_from_slice(&word[..chunk.len()]);
    }
}

pub fn expiry_for(i: u64) -> u64 {
    mix(i ^ SEED_EXPIRY)
}
