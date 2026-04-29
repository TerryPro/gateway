#![allow(dead_code)]

/// 将字节数组编码成小写十六进制字符串。
pub fn encode_hex_lower(data: &[u8]) -> String {
    let mut s = String::with_capacity(data.len() * 2);
    for b in data {
        s.push(hex_char((b >> 4) & 0x0F));
        s.push(hex_char(b & 0x0F));
    }
    s
}

/// 将十六进制字符串解析为字节数组。
pub fn decode_hex(input: &str) -> Result<Vec<u8>, String> {
    if !input.len().is_multiple_of(2) {
        return Err("length must be even".to_string());
    }
    let bytes = input.as_bytes();
    let mut out = Vec::with_capacity(bytes.len() / 2);
    let mut i = 0;
    while i < bytes.len() {
        let hi = from_hex_char(bytes[i]).ok_or_else(|| "invalid hex".to_string())?;
        let lo = from_hex_char(bytes[i + 1]).ok_or_else(|| "invalid hex".to_string())?;
        out.push((hi << 4) | lo);
        i += 2;
    }
    Ok(out)
}

/// 将单个十六进制半字节转换为字符。
fn hex_char(v: u8) -> char {
    match v {
        0..=9 => (b'0' + v) as char,
        10..=15 => (b'a' + (v - 10)) as char,
        _ => '0',
    }
}

/// 将十六进制字符转为半字节。
fn from_hex_char(c: u8) -> Option<u8> {
    match c {
        b'0'..=b'9' => Some(c - b'0'),
        b'a'..=b'f' => Some(c - b'a' + 10),
        b'A'..=b'F' => Some(c - b'A' + 10),
        _ => None,
    }
}
