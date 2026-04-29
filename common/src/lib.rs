//! 共享协议与编解码能力。

pub mod archive;
pub mod storage;
pub mod tsmeta;

pub mod device_proto {
    use std::io;

    use bytes::{Buf, BufMut, BytesMut};
    use tokio_util::codec::{Decoder, Encoder};

    /// 私有协议魔数，用于快速判定帧头是否合法。
    pub const MAGIC: u16 = 0xCAFE;
    /// 协议版本，便于后续演进。
    pub const VERSION: u8 = 1;
    /// 单帧最大负载，防止异常数据导致内存膨胀。
    pub const MAX_PAYLOAD: usize = 1024 * 1024;
    const HEADER_LEN: usize = 12;

    /// 设备侧消息类型。
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    #[repr(u8)]
    pub enum MsgType {
        Hello = 1,
        Telemetry = 2,
        Command = 3,
        CommandReply = 4,
        Heartbeat = 5,
        Error = 255,
    }

    /// 将 u8 安全转换为消息类型。
    impl TryFrom<u8> for MsgType {
        type Error = io::Error;

        /// 从字节码映射到消息类型，未知类型直接报错。
        fn try_from(value: u8) -> Result<Self, io::Error> {
            match value {
                1 => Ok(Self::Hello),
                2 => Ok(Self::Telemetry),
                3 => Ok(Self::Command),
                4 => Ok(Self::CommandReply),
                5 => Ok(Self::Heartbeat),
                255 => Ok(Self::Error),
                _ => Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("unknown msg type: {value}"),
                )),
            }
        }
    }

    /// 私有协议帧结构。
    #[derive(Debug, Clone)]
    pub struct Frame {
        pub msg_type: MsgType,
        pub request_id: u32,
        pub payload: Vec<u8>,
    }

    /// 创建 HELLO 帧，负载为设备 ID 文本。
    pub fn hello_frame(device_id: &str) -> Frame {
        Frame {
            msg_type: MsgType::Hello,
            request_id: 0,
            payload: device_id.as_bytes().to_vec(),
        }
    }

    /// 创建设备遥测帧，负载为 UTF-8 文本。
    pub fn telemetry_frame(text: &str) -> Frame {
        Frame {
            msg_type: MsgType::Telemetry,
            request_id: 0,
            payload: text.as_bytes().to_vec(),
        }
    }

    /// 创建指令下发帧。
    pub fn command_frame(request_id: u32, command: Vec<u8>) -> Frame {
        Frame {
            msg_type: MsgType::Command,
            request_id,
            payload: command,
        }
    }

    /// 创建指令回令帧。
    pub fn command_reply_frame(request_id: u32, reply: Vec<u8>) -> Frame {
        Frame {
            msg_type: MsgType::CommandReply,
            request_id,
            payload: reply,
        }
    }

    /// 将负载尝试按 UTF-8 解码。
    pub fn payload_as_text(payload: &[u8]) -> Result<&str, io::Error> {
        std::str::from_utf8(payload)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))
    }

    /// 设备私有协议编解码器。
    #[derive(Debug, Default)]
    pub struct DeviceCodec;

    /// 负责帧编码。
    impl Encoder<Frame> for DeviceCodec {
        type Error = io::Error;

        /// 按固定头 + 可变长负载编码并写入输出缓冲区。
        fn encode(&mut self, item: Frame, dst: &mut BytesMut) -> Result<(), Self::Error> {
            if item.payload.len() > MAX_PAYLOAD {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "payload too large",
                ));
            }

            dst.reserve(HEADER_LEN + item.payload.len());
            dst.put_u16(MAGIC);
            dst.put_u8(VERSION);
            dst.put_u8(item.msg_type as u8);
            dst.put_u32(item.request_id);
            dst.put_u32(item.payload.len() as u32);
            dst.put_slice(&item.payload);
            Ok(())
        }
    }

    /// 负责帧解码。
    impl Decoder for DeviceCodec {
        type Item = Frame;
        type Error = io::Error;

        /// 从输入缓冲区增量解析完整帧。
        fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
            if src.len() < HEADER_LEN {
                return Ok(None);
            }

            let mut cursor = &src[..];
            let magic = cursor.get_u16();
            if magic != MAGIC {
                return Err(io::Error::new(io::ErrorKind::InvalidData, "invalid magic"));
            }

            let version = cursor.get_u8();
            if version != VERSION {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("unsupported version: {version}"),
                ));
            }

            let msg_type = MsgType::try_from(cursor.get_u8())?;
            let request_id = cursor.get_u32();
            let payload_len = cursor.get_u32() as usize;
            if payload_len > MAX_PAYLOAD {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "payload too large",
                ));
            }

            if src.len() < HEADER_LEN + payload_len {
                return Ok(None);
            }

            src.advance(HEADER_LEN);
            let payload = src.split_to(payload_len).to_vec();

            Ok(Some(Frame {
                msg_type,
                request_id,
                payload,
            }))
        }
    }
}

pub mod resp {
    use std::io;

    use bytes::{BufMut, BytesMut};

    /// RESP 值类型，当前实现覆盖网关控制面所需最小集合。
    #[derive(Debug, Clone)]
    pub enum RespValue {
        SimpleString(String),
        Error(String),
        Integer(i64),
        BulkString(Vec<u8>),
        NullBulkString,
        Array(Vec<RespValue>),
    }

    /// 对命令参数进行 RESP 数组编码，便于客户端与服务端复用。
    pub fn encode_command(args: &[String]) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.extend_from_slice(format!("*{}\r\n", args.len()).as_bytes());
        for arg in args {
            let bytes = arg.as_bytes();
            buf.extend_from_slice(format!("${}\r\n", bytes.len()).as_bytes());
            buf.extend_from_slice(bytes);
            buf.extend_from_slice(b"\r\n");
        }
        buf
    }

    /// 编码 RESP 值。
    pub fn encode_value(value: &RespValue) -> Vec<u8> {
        let mut out = BytesMut::new();
        encode_value_inner(value, &mut out);
        out.to_vec()
    }

    /// 解析输入缓冲区中的一个 RESP 值，返回值与已消费字节数。
    pub fn decode_value(src: &[u8]) -> Result<Option<(RespValue, usize)>, io::Error> {
        if src.is_empty() {
            return Ok(None);
        }
        parse_value(src, 0)
    }

    /// 便捷方法：从 RESP 数组中提取命令字符串数组。
    pub fn as_command_args(value: RespValue) -> Result<Vec<String>, io::Error> {
        let arr = match value {
            RespValue::Array(v) => v,
            _ => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "command must be RESP array",
                ));
            }
        };

        let mut out = Vec::with_capacity(arr.len());
        for item in arr {
            match item {
                RespValue::BulkString(v) => out.push(String::from_utf8(v).map_err(|e| {
                    io::Error::new(io::ErrorKind::InvalidData, format!("utf8 error: {e}"))
                })?),
                RespValue::SimpleString(s) => out.push(s),
                _ => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "command argument must be string",
                    ));
                }
            }
        }
        Ok(out)
    }

    /// 内部函数：编码 RESP 值到缓冲区。
    fn encode_value_inner(value: &RespValue, out: &mut BytesMut) {
        match value {
            RespValue::SimpleString(s) => {
                out.put_u8(b'+');
                out.extend_from_slice(s.as_bytes());
                out.extend_from_slice(b"\r\n");
            }
            RespValue::Error(s) => {
                out.put_u8(b'-');
                out.extend_from_slice(s.as_bytes());
                out.extend_from_slice(b"\r\n");
            }
            RespValue::Integer(v) => {
                out.put_u8(b':');
                out.extend_from_slice(v.to_string().as_bytes());
                out.extend_from_slice(b"\r\n");
            }
            RespValue::BulkString(v) => {
                out.put_u8(b'$');
                out.extend_from_slice(v.len().to_string().as_bytes());
                out.extend_from_slice(b"\r\n");
                out.extend_from_slice(v);
                out.extend_from_slice(b"\r\n");
            }
            RespValue::NullBulkString => out.extend_from_slice(b"$-1\r\n"),
            RespValue::Array(arr) => {
                out.put_u8(b'*');
                out.extend_from_slice(arr.len().to_string().as_bytes());
                out.extend_from_slice(b"\r\n");
                for item in arr {
                    encode_value_inner(item, out);
                }
            }
        }
    }

    /// 内部函数：按 RESP 规则解析一个值。
    fn parse_value(src: &[u8], offset: usize) -> Result<Option<(RespValue, usize)>, io::Error> {
        if offset >= src.len() {
            return Ok(None);
        }
        match src[offset] {
            b'+' => parse_simple(src, offset).map(|o| o.map(|(s, n)| (RespValue::SimpleString(s), n))),
            b'-' => parse_simple(src, offset).map(|o| o.map(|(s, n)| (RespValue::Error(s), n))),
            b':' => parse_integer(src, offset).map(|o| o.map(|(v, n)| (RespValue::Integer(v), n))),
            b'$' => parse_bulk(src, offset),
            b'*' => parse_array(src, offset),
            _ => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid resp prefix",
            )),
        }
    }

    /// 内部函数：解析简单字符串或错误字符串。
    fn parse_simple(src: &[u8], offset: usize) -> Result<Option<(String, usize)>, io::Error> {
        let (line, next) = match read_line(src, offset + 1)? {
            Some(v) => v,
            None => return Ok(None),
        };
        Ok(Some((line, next)))
    }

    /// 内部函数：解析整数。
    fn parse_integer(src: &[u8], offset: usize) -> Result<Option<(i64, usize)>, io::Error> {
        let (line, next) = match read_line(src, offset + 1)? {
            Some(v) => v,
            None => return Ok(None),
        };
        let value = line
            .parse::<i64>()
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;
        Ok(Some((value, next)))
    }

    /// 内部函数：解析 Bulk String。
    fn parse_bulk(src: &[u8], offset: usize) -> Result<Option<(RespValue, usize)>, io::Error> {
        let (line, mut next) = match read_line(src, offset + 1)? {
            Some(v) => v,
            None => return Ok(None),
        };
        let len = line
            .parse::<isize>()
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;
        if len == -1 {
            return Ok(Some((RespValue::NullBulkString, next)));
        }
        if len < 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid bulk string length",
            ));
        }
        let len = len as usize;
        if src.len() < next + len + 2 {
            return Ok(None);
        }
        let data = src[next..next + len].to_vec();
        next += len;
        if &src[next..next + 2] != b"\r\n" {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "bulk string missing CRLF",
            ));
        }
        next += 2;
        Ok(Some((RespValue::BulkString(data), next)))
    }

    /// 内部函数：解析 RESP 数组。
    fn parse_array(src: &[u8], offset: usize) -> Result<Option<(RespValue, usize)>, io::Error> {
        let (line, mut next) = match read_line(src, offset + 1)? {
            Some(v) => v,
            None => return Ok(None),
        };
        let len = line
            .parse::<isize>()
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;
        if len < 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "null array not supported",
            ));
        }
        let len = len as usize;
        let mut arr = Vec::with_capacity(len);
        for _ in 0..len {
            let (value, consumed) = match parse_value(src, next)? {
                Some(v) => v,
                None => return Ok(None),
            };
            arr.push(value);
            next = consumed;
        }
        Ok(Some((RespValue::Array(arr), next)))
    }

    /// 内部函数：读取以 CRLF 结束的一行（不含 CRLF）。
    fn read_line(src: &[u8], start: usize) -> Result<Option<(String, usize)>, io::Error> {
        if start >= src.len() {
            return Ok(None);
        }
        for i in start..src.len().saturating_sub(1) {
            if src[i] == b'\r' && src[i + 1] == b'\n' {
                let line = std::str::from_utf8(&src[start..i])
                    .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?
                    .to_string();
                return Ok(Some((line, i + 2)));
            }
        }
        Ok(None)
    }
}
