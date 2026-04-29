/// 本地命令处理结果。
pub enum LocalCommandAction {
    Continue,
    Exit,
    NotLocal,
}

/// 处理本地命令（HELP/QUIT/EXIT）。
pub fn handle_local_command(line: &str) -> LocalCommandAction {
    if line.eq_ignore_ascii_case("QUIT") || line.eq_ignore_ascii_case("EXIT") {
        return LocalCommandAction::Exit;
    }
    if line.eq_ignore_ascii_case("HELP") {
        print_help();
        return LocalCommandAction::Continue;
    }
    LocalCommandAction::NotLocal
}

/// 输出 REPL 模式帮助信息。
fn print_help() {
    println!("commands:");
    println!("  PING");
    println!("  LIST");
    println!("  CLILIST");
    println!("  KEY <device_id> [param_id...]  (query current value cache)");
    println!("  STAT <device_id>");
    println!("  CONN <device_id>");
    println!("  CONA");
    println!("  SEND <device_id> <command_code:C00001~C99999> [timeout_ms]");
    println!("  KICK <device_id>");
    println!("  KICA");
    println!("  SUB <device_id>");
    println!("  SUBALL [topic_filter]           (default: #, e.g. gw/#)");
    println!("  UNSUB [subscription_id]");
    println!("  PS <device_id> <param_id...>   (alias: SUBCFG_SET)");
    println!("      e.g. PS dev001 P00001 P00002 P00003~P00010");
    println!("  PL                             (list all local subscriptions with IDs)");
    println!("  TL | TOPICS                    (list observed topics in this session)");
    println!("local:");
    println!("  HELP");
    println!("  Ctrl+Q  (快速取消全部订阅，并清理参数订阅配置)");
    println!("  QUIT / EXIT");
}

#[cfg(test)]
mod tests {
    use super::{LocalCommandAction, handle_local_command};

    /// 验证退出命令会返回 Exit。
    #[test]
    fn quit_returns_exit() {
        assert!(matches!(
            handle_local_command("quit"),
            LocalCommandAction::Exit
        ));
    }

    /// 验证非本地命令会返回 NotLocal。
    #[test]
    fn ping_returns_not_local() {
        assert!(matches!(
            handle_local_command("PING"),
            LocalCommandAction::NotLocal
        ));
    }
}
