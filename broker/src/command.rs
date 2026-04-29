/// Broker 支持的控制命令集合，用于统一 RESP 与 MQTT 两条入口的命令语义。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BrokerCommand {
    Ping,
    Connect,
    List,
    Status,
    Key,
    Kick,
    Send,
    CliList,
    SubcfgSet,
    SubcfgGet,
    SubcfgDel,
    SubcfgList,
}

impl BrokerCommand {
    /// 将输入命令名规范化并解析为命令枚举，解析失败返回 `None`。
    pub fn parse(raw: &str) -> Option<Self> {
        match raw.trim().to_ascii_uppercase().as_str() {
            "PING" => Some(Self::Ping),
            "CONNECT" => Some(Self::Connect),
            "LIST" => Some(Self::List),
            "STATUS" => Some(Self::Status),
            "KEY" => Some(Self::Key),
            "KICK" => Some(Self::Kick),
            "SEND" => Some(Self::Send),
            "CLILIST" => Some(Self::CliList),
            "SUBCFG_SET" => Some(Self::SubcfgSet),
            "SUBCFG_GET" => Some(Self::SubcfgGet),
            "SUBCFG_DEL" => Some(Self::SubcfgDel),
            "SUBCFG_LIST" => Some(Self::SubcfgList),
            _ => None,
        }
    }

    /// 返回命令标准大写名称，便于回写到统一命令参数列表。
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Ping => "PING",
            Self::Connect => "CONNECT",
            Self::List => "LIST",
            Self::Status => "STATUS",
            Self::Key => "KEY",
            Self::Kick => "KICK",
            Self::Send => "SEND",
            Self::CliList => "CLILIST",
            Self::SubcfgSet => "SUBCFG_SET",
            Self::SubcfgGet => "SUBCFG_GET",
            Self::SubcfgDel => "SUBCFG_DEL",
            Self::SubcfgList => "SUBCFG_LIST",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::BrokerCommand;

    /// 验证命令解析应支持大小写不敏感与前后空白。
    #[test]
    fn parse_command_should_be_case_insensitive() {
        assert_eq!(BrokerCommand::parse(" ping "), Some(BrokerCommand::Ping));
        assert_eq!(
            BrokerCommand::parse("subcfg_set"),
            Some(BrokerCommand::SubcfgSet)
        );
    }

    /// 验证未知命令应返回 `None`，避免被误识别。
    #[test]
    fn parse_unknown_command_should_return_none() {
        assert_eq!(BrokerCommand::parse("BAD_CMD"), None);
    }
}
