pub fn compress_zstd(b: &[u8]) -> Vec<u8> {
    let mut out = vec![];
    zstd::stream::copy_encode(b, &mut out, 3).expect("it's in memory");
    out
}

pub fn decompress_zstd(b: &[u8]) -> Vec<u8> {
    let mut out = vec![];
    zstd::stream::copy_decode(b, &mut out).expect("it's in memory");
    out
}

