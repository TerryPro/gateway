use common::resp::RespValue;

/// 将 LIST 命令的响应格式化输出为设备状态表格。
pub fn print_list_resp(value: &RespValue) {
    match value {
        RespValue::Array(devices) => {
            if devices.is_empty() {
                println!("  (no devices)");
                return;
            }
            println!(
                "{:<20} {:<10} {:<20} LAST_SEEN_TS",
                "DEVICE_ID", "ONLINE", "ADDR"
            );
            println!("{}", "-".repeat(70));
            for device in devices {
                if let RespValue::Array(fields) = device {
                    let mut device_id = String::new();
                    let mut online = String::new();
                    let mut addr = String::new();
                    let mut last_seen_ts = String::new();

                    let mut i = 0;
                    while i + 1 < fields.len() {
                        if let RespValue::BulkString(k) = &fields[i] {
                            let key = String::from_utf8_lossy(k);
                            if let RespValue::BulkString(v) = &fields[i + 1] {
                                let val = String::from_utf8_lossy(v);
                                match key.as_ref() {
                                    "device_id" => device_id = val.to_string(),
                                    "online" => online = val.to_string(),
                                    "addr" => addr = val.to_string(),
                                    "last_seen_ts" => last_seen_ts = val.to_string(),
                                    _ => {}
                                }
                            }
                        }
                        i += 2;
                    }
                    let online_str = if online == "1" { "yes" } else { "no" };
                    if addr.is_empty() {
                        addr = "-".to_string();
                    }
                    if last_seen_ts.is_empty() || last_seen_ts == "0" {
                        last_seen_ts = "-".to_string();
                    }
                    println!("{:<20} {:<10} {:<20} {}", device_id, online_str, addr, last_seen_ts);
                }
            }
        }
        RespValue::NullBulkString => println!("  (nil)"),
        RespValue::Error(e) => eprintln!("(error) {e}"),
        _ => print_resp(value),
    }
}

/// 将 CLILIST 命令的响应格式化输出为 CLI 会话表格。
pub fn print_clilist_resp(value: &RespValue) {
    match value {
        RespValue::Array(items) => {
            if items.is_empty() {
                println!("  (no cli sessions)");
                return;
            }
            println!("{:<30} {:<12} LAST_CMD", "CLIENT_ID", "LAST_SEEN_TS");
            println!("{}", "-".repeat(70));
            for item in items {
                if let RespValue::Array(fields) = item {
                    let mut client_id = String::new();
                    let mut last_seen_ts = String::new();
                    let mut last_cmd = String::new();
                    let mut i = 0;
                    while i + 1 < fields.len() {
                        if let RespValue::BulkString(k) = &fields[i] {
                            let key = String::from_utf8_lossy(k);
                            if let RespValue::BulkString(v) = &fields[i + 1] {
                                let val = String::from_utf8_lossy(v);
                                match key.as_ref() {
                                    "client_id" => client_id = val.to_string(),
                                    "last_seen_ts" => last_seen_ts = val.to_string(),
                                    "last_cmd" => last_cmd = val.to_string(),
                                    _ => {}
                                }
                            }
                        }
                        i += 2;
                    }
                    if client_id.is_empty() {
                        client_id = "-".to_string();
                    }
                    if last_seen_ts.is_empty() || last_seen_ts == "0" {
                        last_seen_ts = "-".to_string();
                    }
                    if last_cmd.is_empty() {
                        last_cmd = "-".to_string();
                    }
                    println!("{:<30} {:<12} {}", client_id, last_seen_ts, last_cmd);
                }
            }
        }
        RespValue::NullBulkString => println!("  (nil)"),
        RespValue::Error(e) => eprintln!("(error) {e}"),
        _ => print_resp(value),
    }
}

/// 将 RESP 响应友好输出到终端。
pub fn print_resp(value: &RespValue) {
    match value {
        RespValue::SimpleString(s) => println!("{s}"),
        RespValue::Error(e) => eprintln!("{e}"),
        RespValue::Integer(v) => println!("{v}"),
        RespValue::BulkString(v) => println!("{}", String::from_utf8_lossy(v)),
        RespValue::NullBulkString => println!("(nil)"),
        RespValue::Array(arr) => {
            for (i, item) in arr.iter().enumerate() {
                print!("{}. ", i + 1);
                print_resp_inline(item);
                println!();
            }
        }
    }
}

/// 将 RESP 值以单行格式输出。
fn print_resp_inline(value: &RespValue) {
    match value {
        RespValue::SimpleString(s) => print!("{s}"),
        RespValue::Error(e) => print!("(error) {e}"),
        RespValue::Integer(v) => print!("{v}"),
        RespValue::BulkString(v) => print!("{}", String::from_utf8_lossy(v)),
        RespValue::NullBulkString => print!("(nil)"),
        RespValue::Array(arr) => {
            print!("[");
            for (i, item) in arr.iter().enumerate() {
                if i > 0 {
                    print!(", ");
                }
                print_resp_inline(item);
            }
            print!("]");
        }
    }
}
