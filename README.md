# kfilter — Kafka Protobuf 消息过滤工具（Go 版）

仿 Redpanda Console 的消息过滤体验，但跑在命令行：指定 topic / 分区 / 起始 offset，
按每个 topic 自己的 `.proto` 文件动态解码消息（无需预编译 proto），用 JS 表达式过滤，
限定命中条数，输出 JSON。

单文件可执行程序（Windows/Linux/macOS），不依赖 Python、不需要 protoc。

## 特性

- **运行时解析 proto**：`proto/<topic>.proto` 放进目录即可（或 `-proto-file` 指定任意路径），
  自动处理 `import`，默认取文件中第一个 message，也可用 `-msg` 指定。
- **多 topic 共享 proto（Redpanda mappings 式映射）**：真实业务往往多个 topic 共用几个 proto 文件
  （如 `shared_events.proto` + `common.proto`，一个定义业务事件、一个定义公共类型），
  用 `-map 'topic=消息类型'` 建立映射即可，不必按 topic 命名 proto 文件，见下文。
- **offset 语义与 Redpanda Console 对齐**：
  - `-offset -50` 表示 newest-50（从最新位置往前 50 条开始）
  - `-offset 12345678900` 表示绝对 offset
  - `-oldest` 从分区最早的消息开始
  - `-from-time "2026-09-20 10:00:00"` 按时间戳定位起点（也支持 Unix 秒/毫秒）
  - `-partition 4` 只看分区 4，缺省扫所有分区
- **没有"窗口耗尽"概念**：offset/时间戳只是起点，从起点持续消费——积压扫完后接着等新消息，
  直到命中 `-limit` 条、`-timeout` 到期或 Ctrl+C。`-limit` 是上限不是目标，
  扫描范围内没凑满就继续等（不需要等可 Ctrl+C 或调小 `-timeout`）。
- **过滤表达式（JS）**，两种写法等价：
  - Redpanda 风格函数体：`-expr 'if (value.vendorId === "vendor-002") { return true; } return false;'`
  - 完整函数：`-expr 'function(m){ return m.value.items[0].vendorId === "vendor-002"; }'`
  - 上下文字段：`value`（解码后的消息）、`key`、`partition`、`offset`、`timestamp`（毫秒）、`headers`、`topic`。
    完整函数风格入参 `m` 即整个上下文对象。
- **字段命名**：protobuf 字段自动转 lowerCamelCase（JSON 名），如 `req_time` → `reqTime`、
  `vendor_id` → `vendorId`、`min_price` → `minPrice`（与 Redpanda Console 显示一致）。
- **限流输出**：命中 `-limit` 条即停；可 `-out hits.jsonl` 落盘 JSON Lines。
- **protobuf 类型映射**：int64/uint64 → JSON number（goja 中可安全比较，JS Number 精度内）；
  bytes → base64 字符串；enum → 枚举名字符串；map → 对象；repeated → 数组。
- **输出就是 JSON**：stdout 打印每条命中的完整 JSON（value 部分缩进美化），`-out hits.jsonl`
  落盘为 JSON Lines（每行一个 JSON 对象，含 topic/partition/offset/timestamp/key/headers/value）。
- **JSON 消息（非 protobuf）**：加 `-raw` 即可。value 按 JSON 解析后参与同一套 JS 过滤表达式，
  不是合法 JSON 时降级为原始字符串。
- **SSH 隧道**：本地不能直连线上集群时，支持 SOCKS5 动态转发或内置 SSH 客户端，见下文。
- `-raw`：消息本身是 JSON 时跳过 proto 解码。

## 快速开始

```bash
# 1. 把每个 topic 的 proto 文件放进 proto/ 目录（文件名 = topic 名 + .proto）
#    例如 topic my_topic → proto/my_topic.proto
#    或者用 -proto-file 直接指定任意路径的 proto 文件（文件名不必等于 topic 名）

# 2. 从指定分区/offset 过滤
kfilter -brokers broker1:9092 -topic my_topic -partition 4 \
  -offset 12345678900 -limit 50 \
  -proto-file proto/my_topic.proto \
  -expr 'if (value.vendorId === "vendor-002") { return true; } return false;'

# 3. 最新 2000 条里找（等价 Redpanda 的 "Newest - 50" 思路，但可自定义窗口）
kfilter -brokers broker1:9092 -topic my_topic -offset -2000 -limit 10 \
  -expr 'm.value.channelId === "ch-001" && m.value.vendorId === "vendor-001"'

# 4. 嵌套字段 / repeated
kfilter ... -expr 'm.value.items.some(i => i.vendorId === "vendor-002")'

# 5. 结果落盘
kfilter ... -out hits.jsonl

# 5b. 按时间戳定位起点（本地时区；也支持 Unix 秒/毫秒），从该时刻起持续消费
#     直到命中 -limit 条或 -timeout 到期
kfilter -brokers broker1:9092 -topic my_topic \
  -from-time "2026-09-20 10:00:00" -limit 10 \
  -expr 'value.vendorId === "vendor-002"'

# 6. 集群开了 SASL/TLS（9093）
kfilter -brokers broker1:9093 -tls -sasl scram-sha256 \
  -sasl-user alice -sasl-pass '***' \
  -topic my_topic -expr 'value.vendorId === "vendor-002"'

# 7. 本地不能直连线上 Kafka：先开 SOCKS5 隧道，再走隧道查询
#    （另开终端）ssh -D 1080 user@jump-host -N
kfilter -brokers broker-internal:9092 -topic my_topic \
  -socks5 127.0.0.1:1080 -expr 'value.vendorId === "vendor-002"'

# 8. 或使用内置 SSH 隧道（免外部 ssh 客户端）
kfilter -brokers broker-internal:9092 -topic my_topic \
  -ssh-tunnel user@jump-host -ssh-key ~/.ssh/id_rsa \
  -expr 'value.vendorId === "vendor-002"'
```

> 以上示例为 bash 写法。**Windows PowerShell 用户注意**：内层字符串引号要反过来写，
> 外层双引号 + 内层单引号：`-expr "value.someField === '100200'"`。
> PowerShell 5.1 向 exe 传参会吃掉内嵌双引号，详见下文「Shell 引号注意」。

## proto 文件指定方式

三个参数按优先级生效：

1. **`-proto-file`（最高）**：直接指定主文件路径，文件名不必等于 topic 名；主文件所在目录的相对
   import 优先解析。`-proto` 与 `-proto-file` 同时给时后者优先（即使 `-proto` 目录里有同名文件，
   已用回归测试 `proto_priority_test.go` 锁定）。
2. **`-map`（Redpanda mappings 式映射）**：`-map 'topic1=类型1,topic2=类型2'`。设置后不再要求
   `<topic>.proto` 命名约定，而是扫描 `-proto` 目录（可多目录，逗号分隔）下**全部** `.proto` 文件，
   在其中查找映射指定的消息类型——对应 Redpanda Console 的：

   ```yaml
   serde:
     protobuf:
       enabled: true
       mappings:
         - topicName: svc_order_req
           valueProtoType: OrderRequest
         - topicName: svc_order_rsp
           valueProtoType: OrderResponse
   ```

   kfilter 等价写法：`-map 'svc_order_req=OrderRequest,svc_order_rsp=OrderResponse'`。
   类型支持三种写法：裸短名（`OrderRequest`，须全局唯一，重名会报错并列出候选）、
   全限定名（`app.TraceEvent`，推荐，等价 Redpanda 的 valueProtoType）、package+短名自动匹配。
   跨文件 `import` 与嵌套类型（如 `shop.Receipt.Item`）自动解析。
3. **`-proto` 目录约定（兜底）**：都没给映射时，按 topic 名找 `<dir>/<topic>.proto` 作主文件。

示例：目录里两个文件——`shared_events.proto`（无 package，定义 OrderRequest/OrderResponse 等业务事件，
作为映射的类型来源）和它 import 的 `common.proto`（package shop，定义 Order/Receipt 及枚举等公共类型）：

```bash
# bash 写法
kfilter -brokers ... -topic svc_order_req \
  -proto /path/to/protos \
  -map 'svc_order_req=OrderRequest,svc_order_rsp=OrderResponse' \
  -expr 'value.reqId === "req-001"'
```

```powershell
# Windows PowerShell 写法（内外引号互换）
.\kfilter.exe -brokers ... -topic svc_order_req `
  -proto C:\protos `
  -map "svc_order_req=OrderRequest,svc_order_rsp=OrderResponse" `
  -expr "value.reqId === 'req-001'"
```

> 注意：若 Redpanda 配置里的类型带包前缀（如 `svc_event_push → app.TraceEvent`），
> 说明对应 proto 文件声明了 `package app;`。该文件不在当前目录时会报清晰错误——把那个 proto
> 文件也放进 `-proto` 目录即可，无需改映射。

## Shell 引号注意（重要）

过滤表达式里的字符串**必须带引号**，且解码后的 proto 字段几乎都是字符串类型。
不同 shell 的正确写法不同，写错会导致 `===` 比较**永远 false、静默 0 命中**（排查：看启动打印的 `expr=...` 里引号是否还在）：

- **Windows PowerShell（推荐写法：外层双引号 + 内层单引号）**：
  ```powershell
  .\kfilter.exe ... -expr "value.someField === '100200'"
  ```
  ⚠️ **不要**写 `-expr 'value.someField === "100200"'`：Windows PowerShell 5.1 向原生 exe 传参时
  不转义内嵌双引号，内层 `"100200"` 会被 CRT 解析吃掉，exe 收到裸数字 `100200`，
  与字符串字段做 `===` 永远 false。（PowerShell 7.3+ 已修复此行为，两种写法都行。）
- **bash / zsh / Git Bash**：单引号包外层即可：`-expr 'value.someField === "100200"'`
- **cmd**：外层双引号 + 内层反斜杠转义：`-expr "value.someField === \"100200\""`

## SSH 隧道

本地不能直连线上 Kafka 时，两种方式：

### 方式一：`-socks5` 动态转发（推荐，多 broker 集群通吃）

先建隧道（保持不关，或加 `-fNT` 后台）：`ssh -D 1080 user@jump-host -N`，
然后 kfilter 加参数 `-socks5 127.0.0.1:1080`。

原理：broker 元数据里返回的 advertised 地址被原样交给跳板机去解析连接，
**不需要**知道每个 broker 的内网地址映射，多 broker 集群天然可用
（等价 Redpanda Console 的 socks5Proxy 配置）。

### 方式二：`-ssh-tunnel` 内置 SSH 客户端

```bash
kfilter -brokers broker1:9092,broker2:9092,broker3:9092 -topic ... \
  -ssh-tunnel user@jump-host -ssh-key ~/.ssh/id_rsa
```

工具自动对 `-brokers` 列出的每个地址建立 SSH 转发（本地随机端口 → 跳板机 → broker），
并把 broker advertised 的地址重写到对应转发端口。凭据：`-ssh-key`（默认尝试
`~/.ssh/id_ed25519`、`~/.ssh/id_rsa`）、`-ssh-key-pass`（私钥口令）、`-ssh-password`。

> 限制：方式二只重写 `-brokers` 里列出的地址，多 broker 集群把全部 broker 地址逗号分隔写全
> （要求跳板机与所有 broker 互通）；不确定集群拓扑时优先用方式一。两参数互斥，均可与 `-sasl`/`-tls` 叠加。

## 全部参数

| 参数 | 默认 | 说明 |
|---|---|---|
| `-brokers` | `127.0.0.1:9092` | broker 列表，逗号分隔（或环境变量 `KAFKA_BROKERS`） |
| `-topic` | 必填 | topic 名 |
| `-partition` | `-1` | 分区号，-1 = 全部分区 |
| `-offset` | `-50` | 起始消费位置：≥0 绝对值；<0 相对 newest（-50 = 从 newest-50 开始） |
| `-oldest` | false | 从日志起点开始（覆盖 `-offset` 和 `-from-time`） |
| `-from-time` | 空 | 按时间戳定位起始位置：纯数字 Unix 秒/毫秒（≥1e12 视为毫秒），或 `2006-01-02 15:04:05`（本地时区）；时间戳晚于全部消息时从当前头部开始等新消息 |
| `-limit` | `50` | 最多输出命中条数（**上限不是目标**：offset 只是起点，没有"窗口耗尽提前返回"——未凑满会持续消费到 `-timeout`） |
| `-timeout` | `60` | 持续消费总时长上限（秒） |
| `-proto` | `proto` | proto 查找目录（逗号分隔多个）；配 `-map` 时为类型扫描根目录 |
| `-proto-file` | 空 | 直接指定 proto 文件路径（优先级最高；主文件所在目录的相对 import 优先解析） |
| `-map` | 空 | topic→消息类型映射（Redpanda serde.protobuf.mappings 等价），如 `-map 'topic=类型'` 逗号分隔多项 |
| `-msg` | 第一个 message | 消息类型全名或短名（如 `app.TraceEvent`） |
| `-expr` | 空 | JS 过滤表达式，空 = 全部输出 |
| `-timeout` | `60` | 持续消费总时长上限（秒） |
| `-out` | 空 | JSON Lines 输出文件 |
| `-raw` | false | value 是 JSON，不做 protobuf 解码 |
| `-sasl` | 空 | SASL 机制：`plain` / `scram-sha256` / `scram-sha512` |
| `-sasl-user` / `-sasl-pass` | 空 | SASL 凭据 |
| `-tls` | false | 启用 TLS（9093 端口场景） |
| `-tls-insecure` | false | 跳过证书校验（自签证书） |
| `-socks5` | 空 | SOCKS5 代理地址（如 `127.0.0.1:1080`，配 `ssh -D` 动态转发） |
| `-ssh-tunnel` | 空 | 内置 SSH 隧道目标 `user@host[:port]`（与 `-socks5` 互斥） |
| `-ssh-key` | `~/.ssh/id_*` | SSH 私钥路径 |
| `-ssh-key-pass` | 空 | SSH 私钥口令 |
| `-ssh-password` | 空 | SSH 密码认证 |
| `-v` | false | 详细日志 |

## 自带自测

不连 Kafka，验证 proto 动态解码 + 过滤表达式链路：

```bash
kfilter -selftest
```

## 跨平台

代码为纯 Go（无 CGO），单命令交叉编译，产物在 `dist/`：

| 文件 | 平台 |
|---|---|
| `kfilter-windows-amd64.exe` | Windows x64 |
| `kfilter-linux-amd64` | Linux x64（服务器常见） |
| `kfilter-macos-arm64` | macOS Apple Silicon (M1/M2/M3/M4) |
| `kfilter-macos-amd64` | macOS Intel |

重新编译（需 Go 1.25+）：

```bash
GOOS=linux  GOARCH=amd64 go build -trimpath -ldflags "-s -w" -o dist/kfilter-linux-amd64 .
GOOS=darwin GOARCH=arm64 go build -trimpath -ldflags "-s -w" -o dist/kfilter-macos-arm64 .
GOOS=darwin GOARCH=amd64 go build -trimpath -ldflags "-s -w" -o dist/kfilter-macos-amd64 .
```

Linux/macOS 下先 `chmod +x kfilter-linux-amd64` 再执行。

## 关于 exe 体积

编译加 `-ldflags "-s -w"` 去掉符号表和调试信息后约 **22MB**（原始 30.4MB）。剩下的体积来自
运行时内嵌的三个纯 Go 库：goja（完整 JS 引擎，~8MB）、protobuf + protocompile（动态 proto 编译器）、
franz-go（Kafka 协议栈）+ SSH 客户端（x/crypto/ssh），以及 Go runtime 本身（~2MB 基线）。
换来的是**零依赖单文件**，拷到任何 Windows/Linux/macOS 机器直接跑。
若还要更小可用 UPX 压缩（约再减 60%，但启动时多一次解压、且可能被杀软误报）。

## 设计说明

- Kafka 客户端 [franz-go](https://github.com/twmb/franz-go)：`AddConsumePartitions` 定向消费指定分区/offset，
  不提交消费组位移，纯只读。
- proto 编译 [protocompile](https://github.com/bufbuild/protocompile)：纯 Go 运行时解析 `.proto`，
  支持 proto3/proto2、import、嵌套 message、well-known types。
- 表达式 [goja](https://github.com/dop251/goja)：纯 Go 的 ES5.1+ JS 引擎（支持 ES6 部分特性如箭头函数）。
- 输出字段名与 Redpanda Console 一致（lowerCamelCase JSON 名）。
