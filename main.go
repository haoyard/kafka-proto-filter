// kfilter — Kafka protobuf 消息过滤工具（Go 版，仿 Redpanda Console 的消息过滤体验）
//
// 用法示例:
//
//	kfilter -brokers broker1:9092 -topic my_topic -partition 4 \
//	        -offset 12345678900 -limit 50 \
//	        -expr 'if (value.vendorId === "vendor-002") { return true; } return false;'
//
// 支持两种表达式风格：
//  1. Redpanda 风格函数体（value/key/partition/offset/timestamp/headers 为入参）：
//     -expr 'if (value.vendorId === "vendor-002") { return true; } return false;'
//  2. 完整 JS 函数（入参 m = {value, key, partition, offset, timestamp, headers, topic}）：
//     -expr 'function(m){ return m.value.vendorId === "vendor-002"; }'
package main

import (
	"context"
	"crypto/tls"
	"encoding/base64"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"os/signal"
	"os/user"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/bufbuild/protocompile"
	"github.com/dop251/goja"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"
	"golang.org/x/crypto/ssh"
	"golang.org/x/net/proxy"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"
)

const version = "1.0.0"

// ---------- CLI 参数 ----------

var (
	brokers     = flag.String("brokers", envOr("KAFKA_BROKERS", "127.0.0.1:9092"), "Kafka broker 地址，逗号分隔")
	topic       = flag.String("topic", "", "目标 topic（必填）")
	partition   = flag.Int("partition", -1, "分区号；-1 表示所有分区")
	startOffset = flag.Int64("offset", -50, "起始消费位置：>=0 为绝对 offset（0 = 从日志最早的可用 offset 开始，受保留策略影响可能 >0，自动校正）；<0 相对最新（如 -50 = 从 newest-50 开始）。持续消费直到 -limit 命中或 -timeout 到期")
	fromTime    = flag.String("from-time", "", "按时间戳定位起始消费位置（优先于 -offset）：纯数字为 Unix 秒/毫秒（>=1e12 视为毫秒），或 '2006-01-02 15:04:05'（本地时区）")
	maxResults  = flag.Int("limit", 50, "最多输出命中条数（上限而非目标；未凑满会持续消费到 -timeout）")
	protoDir    = flag.String("proto", "proto", "proto 查找目录（逗号分隔），按 topic 找 <dir>/<topic>.proto；配合 -map 时为类型查找根目录")
	protoFile   = flag.String("proto-file", "", "直接指定 proto 文件路径（优先级高于 -proto 目录中的同名文件；主文件所在目录的相对 import 优先解析）")
	msgName     = flag.String("msg", "", "消息类型全名或短名（如 app.TraceEvent）；默认取 proto 文件里定义的第一个 message")
	topicMap    = flag.String("map", "", "topic→消息类型映射（仿 Redpanda serde.protobuf.mappings），如 -map 'svc_order_req=OrderRequest,svc_event_push=app.TraceEvent'；设置后忽略 <topic>.proto 文件名约定，在 -proto 目录全部 .proto 文件中查找该类型")
	timeoutSec  = flag.Int("timeout", 60, "持续消费的总时长上限（秒）；0 = 不限时长，一直等到命中 -limit 条或 Ctrl+C")
	expr        = flag.String("expr", "", "过滤表达式（JS），空 = 全部输出")
	verbose     = flag.Bool("v", false, "额外打印扫描进度到 stderr")
	outFile     = flag.String("out", "", "命中结果写入 JSON Lines 文件")
	raw         = flag.Bool("raw", false, "不解析 protobuf，把 value 直接当 JSON/字符串处理")
	selftest    = flag.Bool("selftest", false, "运行内置自测（不连 Kafka）")
	showVer     = flag.Bool("version", false, "打印版本")
	saslMech    = flag.String("sasl", "", "SASL 认证机制：plain / scram-sha256 / scram-sha512（需同时给 -sasl-user/-sasl-pass）")
	saslUser    = flag.String("sasl-user", "", "SASL 用户名")
	saslPass    = flag.String("sasl-pass", "", "SASL 密码")
	tlsEnabled  = flag.Bool("tls", false, "启用 TLS（端口 9093 场景）")
	tlsInsecure = flag.Bool("tls-insecure", false, "跳过 TLS 证书校验（自签证书场景）")
	socks5Addr  = flag.String("socks5", "", "经 SOCKS5 代理连 Kafka（如 ssh -D 1080 user@jump 建的动态隧道：127.0.0.1:1080）；broker advertised 地址由远端解析，天然兼容多 broker 集群")
	sshTunnel   = flag.String("ssh-tunnel", "", "内置 SSH 隧道：user@host:port（如 deploy@jump.example.com:22），把 -brokers 的地址经该 SSH 服务器转发；需 -ssh-key 或 ssh-agent，多 broker 场景建议改用 -socks5")
	sshKeyPath  = flag.String("ssh-key", "", "SSH 私钥路径（默认尝试 ~/.ssh/id_ed25519、~/.ssh/id_rsa）")
	sshKeyPass  = flag.String("ssh-key-pass", "", "SSH 私钥口令（可选）")
	sshPassword = flag.String("ssh-password", "", "SSH 密码认证（可选，优先用密钥）")
)

func envOr(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}

// ---------- SSH 隧道 ----------

// tunnelOptions 返回隧道拨号选项。两个互斥入口：
//   -socks5：外部 ssh -D 动态转发。broker advertised 地址原样交给 SOCKS5 服务端解析连接，
//            多 broker 集群天然可用（推荐）。
//   -ssh-tunnel：内置 SSH 客户端。对 -brokers 列出的每个地址在 SSH 服务器侧建立直连转发，
//            并把 advertised 地址重写到本地转发端口（处理单 broker / 全集群两种情况）。
func tunnelOptions() []kgo.Opt {
	switch {
	case *socks5Addr != "" && *sshTunnel != "":
		fmt.Fprintln(os.Stderr, "错误: -socks5 与 -ssh-tunnel 只能二选一")
		os.Exit(2)
	case *socks5Addr != "":
		dialer, err := proxy.SOCKS5("tcp", *socks5Addr, nil, proxy.Direct)
		if err != nil {
			fmt.Fprintf(os.Stderr, "SOCKS5 初始化失败: %v\n", err)
			os.Exit(1)
		}
		ctxDialer, ok := dialer.(proxy.ContextDialer)
		if !ok {
			fmt.Fprintln(os.Stderr, "SOCKS5 dialer 不支持 Context")
			os.Exit(1)
		}
		return []kgo.Opt{kgo.Dialer(func(ctx context.Context, network, addr string) (net.Conn, error) {
			return ctxDialer.DialContext(ctx, network, addr)
		})}
	case *sshTunnel != "":
		tm := newSSHTunnelManager()
		return []kgo.Opt{kgo.Dialer(tm.dialFunc)}
	}
	return nil
}

// ---------- 内置 SSH 隧道管理 ----------

type sshTunnelManager struct {
	mu      sync.Mutex
	client  *ssh.Client
	tunnels map[string]string // 线上 "host:port" -> 本地 "127.0.0.1:port"
	mapping func(string) string
}

func newSSHTunnelManager() *sshTunnelManager {
	tm := &sshTunnelManager{tunnels: map[string]string{}}
	tm.mapping = func(advertised string) string {
		tm.mu.Lock()
		defer tm.mu.Unlock()
		if local, ok := tm.tunnels[normalizeAddr(advertised)]; ok {
			return local
		}
		return advertised // 未映射的地址原样返回（连不上会报清晰错误）
	}
	tm.setup()
	return tm
}

func normalizeAddr(addr string) string {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return net.JoinHostPort(strings.TrimSpace(addr), "9092")
	}
	if host == "" {
		host = "127.0.0.1"
	}
	return net.JoinHostPort(host, port)
}

func (tm *sshTunnelManager) setup() {
	// 解析 ssh 目标 user@host:port
	spec := *sshTunnel
	idx := strings.Index(spec, "@")
	sshUser := os.Getenv("USER")
	if u, err := user.Current(); err == nil && u.Username != "" {
		sshUser = u.Username
	}
	if idx >= 0 {
		sshUser = spec[:idx]
		spec = spec[idx+1:]
	}
	sshHost, sshPort, err := net.SplitHostPort(spec)
	if err != nil {
		sshHost, sshPort = spec, "22"
	}

	auths, err := sshAuthMethods()
	if err != nil {
		fmt.Fprintf(os.Stderr, "SSH 认证准备失败: %v\n", err)
		os.Exit(1)
	}
	cfg := &ssh.ClientConfig{
		User:            sshUser,
		Auth:            auths,
		HostKeyCallback: ssh.InsecureIgnoreHostKey(), //nolint:gosec // 跳板机场景常用；如需校验可后续加 known_hosts
		Timeout:         10 * time.Second,
	}
	cl, err := ssh.Dial("tcp", net.JoinHostPort(sshHost, sshPort), cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "SSH 连接 %s@%s:%s 失败: %v\n", sshUser, sshHost, sshPort, err)
		os.Exit(1)
	}
	tm.client = cl
	fmt.Fprintf(os.Stderr, "SSH 隧道已建立: %s@%s:%s\n", sshUser, sshHost, sshPort)

	// 对 -brokers 的每个地址建立远端直连转发
	for _, b := range strings.Split(*brokers, ",") {
		target := normalizeAddr(strings.TrimSpace(b))
		host, port, _ := net.SplitHostPort(target)
		local, err := net.Listen("tcp", "127.0.0.1:0") // 系统分配本地端口
		if err != nil {
			fmt.Fprintf(os.Stderr, "本地监听失败: %v\n", err)
			os.Exit(1)
		}
		localAddr := local.Addr().String()
		tm.tunnels[target] = localAddr
		go tm.serveForward(local, host, port)
		fmt.Fprintf(os.Stderr, "转发: %s (Kafka) -> %s (本地)\n", target, localAddr)
	}
}

func sshAuthMethods() ([]ssh.AuthMethod, error) {
	var methods []ssh.AuthMethod
	if *sshKeyPath == "" {
		home := os.Getenv("USERPROFILE")
		if home == "" {
			if u, err := user.Current(); err == nil {
				home = u.HomeDir
			}
		}
		for _, name := range []string{"id_ed25519", "id_rsa"} {
			p := filepath.Join(home, ".ssh", name)
			if _, err := os.Stat(p); err == nil {
				*sshKeyPath = p
				break
			}
		}
	}
	if *sshKeyPath != "" {
		key, err := os.ReadFile(*sshKeyPath)
		if err != nil {
			return nil, fmt.Errorf("读取私钥 %s: %w", *sshKeyPath, err)
		}
		var signer ssh.Signer
		if *sshKeyPass != "" {
			signer, err = ssh.ParsePrivateKeyWithPassphrase(key, []byte(*sshKeyPass))
		} else {
			signer, err = ssh.ParsePrivateKey(key)
		}
		if err != nil {
			return nil, fmt.Errorf("解析私钥 %s: %w", *sshKeyPath, err)
		}
		methods = append(methods, ssh.PublicKeys(signer))
	}
	if *sshPassword != "" {
		methods = append(methods, ssh.Password(*sshPassword))
	}
	if len(methods) == 0 {
		return nil, fmt.Errorf("未提供认证方式（-ssh-key / -ssh-password）")
	}
	return methods, nil
}

// serveForward 在本地端口上接受连接，经 SSH 打开到 Kafka broker 的通道
func (tm *sshTunnelManager) serveForward(local net.Listener, remoteHost, remotePort string) {
	for {
		conn, err := local.Accept()
		if err != nil {
			return // listener 已关闭
		}
		go func(c net.Conn) {
			defer c.Close()
			remote, err := tm.client.Dial("tcp", net.JoinHostPort(remoteHost, remotePort))
			if err != nil {
				fmt.Fprintf(os.Stderr, "SSH 转发到 %s:%s 失败: %v\n", remoteHost, remotePort, err)
				return
			}
			defer remote.Close()
			pipe := func(dst, src net.Conn) {
				io.Copy(dst, src) //nolint:errcheck
				dst.SetReadDeadline(time.Now())
			}
			go pipe(remote, c)
			pipe(c, remote)
		}(conn)
	}
}

// dialFunc 供 kgo 使用：把 advertised 地址重写到本地转发端口
func (tm *sshTunnelManager) dialFunc(ctx context.Context, network, addr string) (net.Conn, error) {
	var d net.Dialer
	return d.DialContext(ctx, network, tm.mapping(addr))
}

// saslTLSOptions 根据命令行参数构造 SASL/TLS 选项
func saslTLSOptions() []kgo.Opt {
	var opts []kgo.Opt
	var mech sasl.Mechanism
	switch strings.ToLower(*saslMech) {
	case "":
		// 无认证
	case "plain":
		mech = plain.Plain(func(ctx context.Context) (plain.Auth, error) {
			return plain.Auth{User: *saslUser, Pass: *saslPass}, nil
		})
	case "scram-sha256":
		mech = scram.Sha256(func(ctx context.Context) (scram.Auth, error) {
			return scram.Auth{User: *saslUser, Pass: *saslPass}, nil
		})
	case "scram-sha512":
		mech = scram.Sha512(func(ctx context.Context) (scram.Auth, error) {
			return scram.Auth{User: *saslUser, Pass: *saslPass}, nil
		})
	default:
		fmt.Fprintf(os.Stderr, "不支持的 SASL 机制: %s（可选 plain / scram-sha256 / scram-sha512）\n", *saslMech)
		os.Exit(2)
	}
	if mech != nil {
		opts = append(opts, kgo.SASL(mech))
	}
	if *tlsEnabled || *tlsInsecure {
		opts = append(opts, kgo.DialTLSConfig(&tls.Config{
			InsecureSkipVerify: *tlsInsecure, //nolint:gosec // 自签证书场景由用户显式开启
		}))
	}
	return opts
}

func main() {
	flag.Parse()
	if *showVer {
		fmt.Println("kfilter", version)
		return
	}
	if *selftest {
		runSelftest()
		return
	}
	if *topic == "" {
		fmt.Fprintln(os.Stderr, "错误: 必须指定 -topic")
		flag.Usage()
		os.Exit(2)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	start := time.Now()

	// 0) topic→类型映射（仿 Redpanda serde.protobuf.mappings）
	mappings, err := parseTopicMap(*topicMap)
	if err != nil {
		fmt.Fprintf(os.Stderr, "-map 参数错误: %v\n", err)
		os.Exit(2)
	}

	// 1) proto 解码器
	dec, err := newProtoDecoderMapped(*protoDir, *protoFile, *topic, *msgName, *raw, mappings)
	if err != nil {
		fmt.Fprintf(os.Stderr, "proto 加载失败: %v\n", err)
		os.Exit(1)
	}

	// 2) 过滤表达式
	filterFn, err := compileFilter(*expr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "过滤表达式错误: %v\n", err)
		os.Exit(1)
	}

	// 3) 客户端 + 元数据
	opts := []kgo.Opt{
		kgo.SeedBrokers(strings.Split(*brokers, ",")...),
		// direct 分区消费（AddConsumePartitions）不参与消费组、不会提交位移，无需 DisableAutoCommit
		kgo.FetchMaxPartitionBytes(8<<20),
		kgo.DialTimeout(10 * time.Second),
	}
	if opt := tunnelOptions(); opt != nil {
		opts = append(opts, opt...)
	}
	if opt := saslTLSOptions(); opt != nil {
		opts = append(opts, opt...)
	}
	cl, err := kgo.NewClient(opts...)
	if err != nil {
		fmt.Fprintf(os.Stderr, "创建 Kafka 客户端失败: %v\n", err)
		os.Exit(1)
	}
	defer cl.Close()

	admin := kadm.NewClient(cl)
	mdCtx, mdCancel := context.WithTimeout(ctx, 15*time.Second)
	md, err := admin.Metadata(mdCtx, *topic)
	mdCancel()
	if err != nil {
		fmt.Fprintf(os.Stderr, "获取 topic 元数据失败: %v\n", err)
		os.Exit(1)
	}
	td, ok := md.Topics[*topic]
	if !ok {
		fmt.Fprintf(os.Stderr, "topic %q 不存在（或无权限）\n", *topic)
		os.Exit(1)
	}
	if td.Err != nil {
		fmt.Fprintf(os.Stderr, "topic %q 元数据错误: %v\n", *topic, td.Err)
		os.Exit(1)
	}

	// 0.4) 参数组合提示：-from-time 优先于 -offset
	setFlags := map[string]bool{}
	flag.Visit(func(f *flag.Flag) { setFlags[f.Name] = true })
	if setFlags["offset"] && setFlags["from-time"] {
		fmt.Fprintln(os.Stderr, "提示: 同时设置了 -offset 和 -from-time，以 -from-time 为准（-offset 被忽略）")
	}

	// 0.5) 时间戳起点
	var fromTimeMs int64
	if *fromTime != "" {
		ms, perr := parseFromTime(*fromTime)
		if perr != nil {
			fmt.Fprintf(os.Stderr, "-from-time 解析失败: %v\n", perr)
			os.Exit(2)
		}
		fromTimeMs = ms
	}

	oCtx, oCancel := context.WithTimeout(ctx, 15*time.Second)
	startOffs, err := admin.ListStartOffsets(oCtx, *topic)
	var afterOffs kadm.ListedOffsets
	if err == nil {
		endOffs, err2 := admin.ListEndOffsets(oCtx, *topic)
		if err2 != nil {
			err = err2
		}
		if err == nil && fromTimeMs > 0 {
			afterOffs, err = admin.ListOffsetsAfterMilli(oCtx, fromTimeMs, *topic)
		}
		oCancel()
		if err != nil {
			fmt.Fprintf(os.Stderr, "获取分区 offset 失败: %v\n", err)
			os.Exit(1)
		}

		type target struct {
			p     int32
			start int64
		}
		var targets []target
		// 收集分区号
		pids := make([]int32, 0, len(td.Partitions))
		for pid := range td.Partitions {
			if *partition >= 0 && pid != int32(*partition) {
				continue
			}
			pids = append(pids, pid)
		}
		sort.Slice(pids, func(i, j int) bool { return pids[i] < pids[j] })
		if len(pids) == 0 {
			fmt.Fprintf(os.Stderr, "没有匹配的分区（-partition %d）\n", *partition)
			os.Exit(1)
		}
		for _, pid := range pids {
			firstRec, ok1 := startOffs.Lookup(*topic, pid)
			endRec, ok2 := endOffs.Lookup(*topic, pid)
			if !ok1 || !ok2 {
				fmt.Fprintf(os.Stderr, "警告: 分区 %d 缺少 offset 元数据，跳过\n", pid)
				continue
			}
			first := firstRec.Offset
			hw := endRec.Offset
			if first < 0 {
				first = 0
			}
			var s int64
			switch {
			case fromTimeMs > 0:
				at, ok := afterOffs.Lookup(*topic, pid)
				if ok && at.Offset >= 0 {
					s = at.Offset
				} else {
					s = hw // 时间戳晚于现有全部消息：从当前头部开始等新消息
				}
			default:
				s = *startOffset
				if s < 0 {
					s += hw // 相对最新
				}
			}
			if s < first {
				fmt.Fprintf(os.Stderr, "警告: 分区 %d 请求的起点 %d 早于日志最早可用 offset %d，已校正（日志保留策略可能已清理旧数据）\n", pid, s, first)
				s = first
			}
			targets = append(targets, target{p: pid, start: s})
		}

		// 打印计划
		var b strings.Builder
		fmt.Fprintf(&b, "topic=%s 起始=[", *topic)
		for i, tg := range targets {
			if i > 0 {
				b.WriteString(",")
			}
			fmt.Fprintf(&b, "%d:%d", tg.p, tg.start)
		}
		fmt.Fprintf(&b, "] limit=%d", *maxResults)
		if fromTimeMs > 0 {
			fmt.Fprintf(&b, " from-time=%d(ms)", fromTimeMs)
		}
		if *expr != "" {
			fmt.Fprintf(&b, " expr=%q", *expr)
		}
		fmt.Println(b.String())

		// 4) 订阅并消费（从起点持续消费：积压扫完后接着等新消息，直到 -limit/-timeout）
		cons := map[string]map[int32]kgo.Offset{}
		cons[*topic] = map[int32]kgo.Offset{}
		for _, tg := range targets {
			cons[*topic][tg.p] = kgo.NewOffset().At(tg.start)
		}
		if len(cons[*topic]) > 0 {
			cl.AddConsumePartitions(cons)
		}

		var deadline time.Time // 零值 = 无超时（-timeout 0）
		if *timeoutSec > 0 {
			deadline = time.Now().Add(time.Duration(*timeoutSec) * time.Second)
		}
		var outF *os.File
		if *outFile != "" {
			f, err := os.Create(*outFile)
			if err != nil {
				fmt.Fprintf(os.Stderr, "创建输出文件失败: %v\n", err)
				os.Exit(1)
			}
			defer f.Close()
			outF = f
		}

		var scanned, hits int64
		var lastData time.Time
		hintShown := false // 长时间无数据的排查提示只打一次

		for hits < int64(*maxResults) {
			if ctx.Err() != nil {
				fmt.Fprintln(os.Stderr, "\n收到中断信号，提前结束")
				break
			}
			if !deadline.IsZero() && time.Now().After(deadline) {
				fmt.Fprintln(os.Stderr, "\n达到超时时间，提前结束")
				break
			}

			pollCtx, pollCancel := context.WithTimeout(ctx, 500*time.Millisecond)
			fetches := cl.PollRecords(pollCtx, 512)
			pollCancel()
			if fetches.IsClientClosed() {
				break
			}
			var fetchErr error
			fetches.EachError(func(t string, p int32, err error) {
				// topic==""/partition==-1 是 franz-go 在单次 poll 的 ctx 到期时注入的
				// 客户端级伪错误（500ms 内无数据即触发），属正常轮询节奏而非连接问题——
				// 真正的连接故障由"15s 无数据提示"兜底诊断；需要细看时加 -v
				if t == "" || p < 0 {
					if *verbose && ctx.Err() == nil {
						fmt.Fprintf(os.Stderr, "[v] poll 空转: %v\n", err)
					}
					return
				}
				fetchErr = err
				fmt.Fprintf(os.Stderr, "消费错误 %s-%d: %v\n", t, p, err)
			})
			if fetchErr != nil && fetches.NumRecords() == 0 && fetchErr != context.DeadlineExceeded {
				break
			}
			n := fetches.NumRecords()
			if n > 0 {
				lastData = time.Now()
			}
			fetches.EachRecord(func(r *kgo.Record) {
				for i := range targets {
					if targets[i].p != r.Partition {
						continue
					}
					scanned++
					pass, obj := evaluate(dec, filterFn, *topic, r)
					if pass {
						hits++
						printHit(obj)
						if outF != nil {
							b, _ := json.Marshal(obj)
							outF.Write(append(b, '\n'))
						}
					}
					return
				}
			})
			if hits >= int64(*maxResults) {
				break
			}
			if n == 0 && lastData.IsZero() {
				// 从未收到任何数据：超过 15s 给一次排查提示（隧道建连慢/网络不通/认证失败等）
				if !hintShown && time.Since(start) > 15*time.Second {
					hintShown = true
					fmt.Fprintln(os.Stderr, "提示: 已 15s 未收到任何消息。若持续无输出，请检查：")
					fmt.Fprintln(os.Stderr, "  1) 隧道是否存活（-socks5 端口上有 ssh -D 进程吗；SSH 方式需在跳板机侧放行 broker 端口）")
					fmt.Fprintln(os.Stderr, "  2) broker 地址是否为集群内网地址（隧道方式下 advertised 地址需能被跳板机解析）")
					fmt.Fprintln(os.Stderr, "  3) SASL/TLS 参数是否与集群一致；可加 -v 看更多日志")
				}
				time.Sleep(200 * time.Millisecond)
			}
		}

		elapsed := time.Since(start).Round(time.Millisecond)
		fmt.Printf("\n完成: 扫描 %d 条, 命中 %d 条, 耗时 %s\n", scanned, hits, elapsed)
		if hits > 0 && *outFile != "" {
			fmt.Printf("结果已写入 %s\n", *outFile)
		}
		return
	}
	// 不会到这里
}

// ---------- 单条消息求值 ----------

func evaluate(dec *protoDecoder, fn filterFunc, topic string, r *kgo.Record) (bool, map[string]any) {
	value, derr := dec.decode(r.Value)
	if derr != nil {
		value = map[string]any{"_decode_error": derr.Error()}
	}
	headers := map[string]string{}
	for _, h := range r.Headers {
		headers[h.Key] = string(h.Value)
	}
	keyAny := any(nil)
	if len(r.Key) > 0 {
		if isPrintable(r.Key) {
			keyAny = string(r.Key)
		} else {
			keyAny = base64.StdEncoding.EncodeToString(r.Key)
		}
	}
	obj := map[string]any{
		"topic":     topic,
		"partition": r.Partition,
		"offset":    r.Offset,
		"timestamp": r.Timestamp.UnixMilli(),
		"time":      r.Timestamp.Format("2006-01-02 15:04:05"),
		"key":       keyAny,
		"headers":   headers,
		"value":     value,
	}
	pass, err := fn(obj)
	if err != nil {
		pass = false
	}
	return pass, obj
}

func isPrintable(b []byte) bool {
	if len(b) == 0 {
		return false
	}
	for _, c := range b {
		if c < 0x20 || c > 0x7e {
			return false
		}
	}
	return true
}

func printHit(obj map[string]any) {
	hitN++
	fmt.Printf("---- 命中 #%d  [partition=%v offset=%v time=%v] ----\n",
		hitN, obj["partition"], obj["offset"], obj["time"])
	vb, _ := json.MarshalIndent(obj["value"], "", "  ")
	fmt.Println(string(vb))
}

var hitN int

// ---------- 过滤表达式（goja） ----------

type filterFunc func(map[string]any) (bool, error)

func compileFilter(expr string) (filterFunc, error) {
	if strings.TrimSpace(expr) == "" {
		return func(map[string]any) (bool, error) { return true, nil }, nil
	}
	vm := goja.New()
	t := strings.TrimSpace(expr)

	var fnSrc string
	isFunc := strings.HasPrefix(t, "function") || strings.HasPrefix(t, "async function")
	if !isFunc && strings.Contains(t, "=>") {
		// 箭头函数：(m) => ... / m => ...
		isFunc = true
	}
	if isFunc {
		fnSrc = "(" + t + ")"
	} else {
		// Redpanda 风格函数体
		if !strings.Contains(t, "return") && !strings.Contains(t, ";") {
			t = "return (" + t + ");"
		}
		fnSrc = "(function(value, key, partition, offset, timestamp, headers, m){ " + t + " })"
	}
	v, err := vm.RunString(fnSrc)
	if err != nil {
		return nil, fmt.Errorf("语法错误: %v", err)
	}
	fn, ok := goja.AssertFunction(v)
	if !ok {
		return nil, fmt.Errorf("表达式必须是一个函数")
	}
	wrapped := strings.Contains(fnSrc, "value, key, partition")
	return func(obj map[string]any) (bool, error) {
		var args []goja.Value
		if wrapped {
			// Redpanda 风格包装函数：位置参数
			args = []goja.Value{
				vm.ToValue(obj["value"]),
				vm.ToValue(obj["key"]),
				vm.ToValue(obj["partition"]),
				vm.ToValue(obj["offset"]),
				vm.ToValue(obj["timestamp"]),
				vm.ToValue(obj["headers"]),
				vm.ToValue(obj),
			}
		} else {
			args = []goja.Value{vm.ToValue(obj)}
		}
		res, err := fn(goja.Undefined(), args...)
		if err != nil {
			return false, err
		}
		return res.ToBoolean(), nil
	}, nil
}

// ---------- protobuf 动态解码 ----------

type protoDecoder struct {
	raw     bool
	msgName string
	desc    protoreflect.MessageDescriptor
}

// parseTopicMap 解析 -map 参数："topic1=Type1,topic2=pkg.Type2" → map
// 类型支持短名（OrderRequest）或全限定名（app.TraceEvent）。
func parseTopicMap(spec string) (map[string]string, error) {
	out := map[string]string{}
	spec = strings.TrimSpace(spec)
	if spec == "" {
		return out, nil
	}
	for _, item := range strings.Split(spec, ",") {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}
		k, v, ok := strings.Cut(item, "=")
		k, v = strings.TrimSpace(k), strings.TrimSpace(v)
		if !ok || k == "" || v == "" {
			return nil, fmt.Errorf("映射项格式错误: %q（应为 topic=消息类型）", item)
		}
		out[k] = v
	}
	return out, nil
}

// parseFromTime 解析 -from-time：纯数字为 Unix 秒/毫秒（>=1e12 视为毫秒，否则秒），
// 其余按本地时区解析常见日期格式。
func parseFromTime(v string) (int64, error) {
	v = strings.TrimSpace(v)
	if n, err := strconv.ParseInt(v, 10, 64); err == nil {
		if n >= 1e12 {
			return n, nil
		}
		return n * 1000, nil
	}
	for _, layout := range []string{"2006-01-02 15:04:05", "2006-01-02T15:04:05", "2006-01-02 15:04", "2006-01-02"} {
		if t, err := time.ParseInLocation(layout, v, time.Local); err == nil {
			return t.UnixMilli(), nil
		}
	}
	return 0, fmt.Errorf("无法解析 %q：支持 Unix 秒/毫秒（纯数字）或 2006-01-02 15:04:05 格式", v)
}

// compileAllProtos 编译 importPaths 下所有 .proto 文件（递归），返回全部 FileDescriptor。
// 用于 -map 映射模式：类型可能定义在任意一个共享 proto 文件里。
func compileAllProtos(importPaths []string) ([]protoreflect.FileDescriptor, error) {
	// 收集去重后的 proto 文件（相对路径以任一 importPath 为根）
	var files []string
	seen := map[string]bool{}
	for _, root := range importPaths {
		err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() || !strings.HasSuffix(path, ".proto") {
				return nil
			}
			rel, err := filepath.Rel(root, path)
			if err != nil {
				return err
			}
			rel = filepath.ToSlash(rel)
			if !seen[rel] {
				seen[rel] = true
				files = append(files, rel)
			}
			return nil
		})
		if err != nil {
			return nil, fmt.Errorf("遍历目录 %s 失败: %v", root, err)
		}
	}
	if len(files) == 0 {
		return nil, fmt.Errorf("目录 %v 下没有任何 .proto 文件", importPaths)
	}
	sort.Strings(files)

	compiler := &protocompile.Compiler{
		Resolver: protocompile.WithStandardImports(&protocompile.SourceResolver{ImportPaths: importPaths}),
	}
	// 一次性编译所有文件，import 关系自动去重解析
	fds, err := compiler.Compile(context.Background(), files...)
	if err != nil {
		return nil, fmt.Errorf("编译 proto 文件失败: %v", err)
	}
	out := make([]protoreflect.FileDescriptor, 0, len(fds))
	for _, fd := range fds {
		out = append(out, fd)
	}
	return out, nil
}

// findMessageGlobal 在一组 FileDescriptor 中按名字查 message。
// name 可以是全限定名（app.TraceEvent）、package+短名，或裸短名（跨文件唯一时）。
func findMessageGlobal(fds []protoreflect.FileDescriptor, name string) (protoreflect.MessageDescriptor, error) {
	// 1) 精确全限定名
	var byFull []protoreflect.MessageDescriptor
	var byShort []protoreflect.MessageDescriptor
	for _, fd := range fds {
		var walk func(mds protoreflect.MessageDescriptors)
		walk = func(mds protoreflect.MessageDescriptors) {
			for i := 0; i < mds.Len(); i++ {
				md := mds.Get(i)
				full := string(md.FullName())
				if full == name {
					byFull = append(byFull, md)
				} else if string(md.Name()) == name {
					byShort = append(byShort, md)
				}
				walk(md.Messages())
			}
		}
		walk(fd.Messages())
	}
	if len(byFull) == 1 {
		return byFull[0], nil
	}
	if len(byFull) > 1 {
		var locs []string
		for _, md := range byFull {
			locs = append(locs, string(md.FullName()))
		}
		return nil, fmt.Errorf("类型 %q 匹配到多个 message（全限定名歧义），请使用更精确的名字", name)
	}
	// 2) 全限定名没命中 → 试 package+短名（如 app.TraceEvent 在 package app 文件里）
	if strings.Contains(name, ".") {
		lastDot := strings.LastIndex(name, ".")
		_, short := name[:lastDot], name[lastDot+1:]
		for _, md := range byShort {
			if strings.HasSuffix(string(md.FullName()), name) || short == string(md.Name()) {
				// FullName 形如 <package>.<short> 且 package 匹配
				if strings.HasSuffix(string(md.FullName()), "."+short) {
					return md, nil
				}
			}
		}
		if len(byShort) == 1 {
			return byShort[0], nil
		}
	}
	// 3) 裸短名：跨文件唯一才可用
	if name != "" && !strings.Contains(name, ".") {
		if len(byShort) == 1 {
			return byShort[0], nil
		}
		if len(byShort) > 1 {
			var locs []string
			for _, md := range byShort {
				locs = append(locs, string(md.FullName()))
			}
			sort.Strings(locs)
			return nil, fmt.Errorf("短名 %q 在多个文件中重复定义，请使用全限定名（如 app.TraceEvent）。候选: %s", name, strings.Join(locs, ", "))
		}
	}
	return nil, fmt.Errorf("在所有 proto 文件中找不到 message %q", name)
}

func newProtoDecoder(dirSpec, protoFile, topic, msgName string, raw bool) (*protoDecoder, error) {
	return newProtoDecoderMapped(dirSpec, protoFile, topic, msgName, raw, nil)
}

// newProtoDecoderMapped 支持 Redpanda 风格的 topic→类型映射（mappings 非空时启用）：
// 不再要求 <topic>.proto 命名约定，而是扫描目录下所有 .proto 文件，
// 从中解析出 mappings[topic] 指定的消息类型。
func newProtoDecoderMapped(dirSpec, protoFile, topic, msgName string, raw bool, mappings map[string]string) (*protoDecoder, error) {
	if raw {
		return &protoDecoder{raw: true}, nil
	}
	var importPaths []string
	for _, d := range strings.Split(dirSpec, ",") {
		d = strings.TrimSpace(d)
		if d != "" {
			importPaths = append(importPaths, d)
		}
	}
	if len(importPaths) == 0 {
		return nil, fmt.Errorf("未提供 proto 目录")
	}

	// 映射模式：在全部 .proto 文件里找映射指定的类型
	if mappedType, ok := mappings[topic]; ok && protoFile == "" {
		fds, err := compileAllProtos(importPaths)
		if err != nil {
			return nil, err
		}
		// -msg 显式指定时优先于映射值（少见但保持一致语义）
		want := mappedType
		if msgName != "" {
			want = msgName
		}
		desc, err := findMessageGlobal(fds, want)
		if err != nil {
			return nil, fmt.Errorf("topic %s 映射的类型查找失败: %v", topic, err)
		}
		return &protoDecoder{msgName: string(desc.FullName()), desc: desc}, nil
	}

	// 确定主文件（相对名，供 SourceResolver 解析）与所在目录
	var mainFile string
	if protoFile != "" {
		abs, err := filepath.Abs(protoFile)
		if err != nil {
			return nil, fmt.Errorf("解析 proto 路径失败: %v", err)
		}
		if _, err := os.Stat(abs); err != nil {
			return nil, fmt.Errorf("proto 文件不存在: %s", protoFile)
		}
		dir := filepath.Dir(abs)
		// 主文件所在目录放最前，保证同目录相对 import 优先从这里解析
		importPaths = append([]string{dir}, importPaths...)
		mainFile = filepath.Base(abs)
	} else {
		// 按 topic 找 <dir>/<topic>.proto
		var foundDir string
		for _, d := range importPaths {
			cand := filepath.Join(d, topic+".proto")
			if _, err := os.Stat(cand); err == nil {
				foundDir = d
				break
			}
		}
		if foundDir == "" {
			hint := "（可用 -proto-file 直接指定文件，或用 -map 'topic=消息类型' 启用 Redpanda mappings 式映射；若消息本身是 JSON 可加 -raw）"
			return nil, fmt.Errorf("在 %v 下找不到 %s.proto%s", importPaths, topic, hint)
		}
		importPaths = append([]string{foundDir}, importPaths...)
		mainFile = topic + ".proto"
	}

	compiler := &protocompile.Compiler{
		Resolver: protocompile.WithStandardImports(&protocompile.SourceResolver{ImportPaths: importPaths}),
	}
	fds, err := compiler.Compile(context.Background(), mainFile)
	if err != nil {
		return nil, fmt.Errorf("编译 %s 失败: %v", mainFile, err)
	}
	if len(fds) == 0 {
		return nil, fmt.Errorf("编译结果为空")
	}
	// protocompile 返回的顺序不保证主文件在第一位，按路径精确匹配主文件
	var fd protoreflect.FileDescriptor
	for _, f := range fds {
		if f.Path() == mainFile || strings.HasSuffix(f.Path(), mainFile) {
			fd = f
			break
		}
	}
	if fd == nil {
		fd = fds[len(fds)-1]
	}

	var desc protoreflect.MessageDescriptor
	if msgName != "" {
		desc = findMessage(fd, msgName)
		if desc == nil {
			return nil, fmt.Errorf("在 %s 中找不到 message %q", mainFile, msgName)
		}
	} else {
		if fd.Messages().Len() == 0 {
			return nil, fmt.Errorf("%s 中没有定义 message", mainFile)
		}
		desc = fd.Messages().Get(0)
	}
	return &protoDecoder{msgName: string(desc.FullName()), desc: desc}, nil
}

func findMessage(fd protoreflect.FileDescriptor, name string) protoreflect.MessageDescriptor {
	var walk func(mds protoreflect.MessageDescriptors) protoreflect.MessageDescriptor
	walk = func(mds protoreflect.MessageDescriptors) protoreflect.MessageDescriptor {
		for i := 0; i < mds.Len(); i++ {
			md := mds.Get(i)
			if string(md.FullName()) == name || string(md.Name()) == name {
				return md
			}
			if got := walk(md.Messages()); got != nil {
				return got
			}
		}
		return nil
	}
	return walk(fd.Messages())
}

func (d *protoDecoder) decode(value []byte) (any, error) {
	if d.raw {
		var v any
		if err := json.Unmarshal(value, &v); err != nil {
			return string(value), nil
		}
		return v, nil
	}
	m := dynamicpb.NewMessage(d.desc)
	if err := proto.Unmarshal(value, m); err != nil {
		return nil, err
	}
	return msgToAny(m), nil
}

// msgToAny 把 dynamic message 转为通用 Go 值（键使用 proto JSON 名 / lowerCamelCase）
func msgToAny(m protoreflect.Message) map[string]any {
	out := map[string]any{}
	fields := m.Descriptor().Fields()
	for i := 0; i < fields.Len(); i++ {
		fd := fields.Get(i)
		if !m.Has(fd) {
			continue
		}
		out[fd.JSONName()] = fieldToAny(m, fd)
	}
	return out
}

func fieldToAny(m protoreflect.Message, fd protoreflect.FieldDescriptor) any {
	v := m.Get(fd)
	switch {
	case fd.IsList():
		list := v.List()
		arr := make([]any, 0, list.Len())
		for i := 0; i < list.Len(); i++ {
			arr = append(arr, scalarToAny(fd, list.Get(i)))
		}
		return arr
	case fd.IsMap():
		mp := v.Map()
		valFD := fd.MapValue()
		out := map[string]any{}
		mp.Range(func(k protoreflect.MapKey, val protoreflect.Value) bool {
			if valFD.Message() != nil {
				out[keyString(k, fd.MapKey())] = msgToAny(val.Message())
			} else {
				out[keyString(k, fd.MapKey())] = scalarToAny(valFD, val)
			}
			return true
		})
		return out
	default:
		if fd.Message() != nil {
			return msgToAny(v.Message())
		}
		return scalarToAny(fd, v)
	}
}

func keyString(k protoreflect.MapKey, kd protoreflect.FieldDescriptor) string {
	switch kd.Kind() {
	case protoreflect.StringKind:
		return k.String()
	case protoreflect.BoolKind:
		return strconv.FormatBool(k.Bool())
	default:
		return strconv.FormatInt(k.Int(), 10)
	}
}

func scalarToAny(fd protoreflect.FieldDescriptor, v protoreflect.Value) any {
	if fd.Message() != nil {
		return msgToAny(v.Message())
	}
	switch fd.Kind() {
	case protoreflect.BoolKind:
		return v.Bool()
	case protoreflect.Int32Kind, protoreflect.Sint32Kind, protoreflect.Sfixed32Kind:
		return int32(v.Int())
	case protoreflect.Uint32Kind, protoreflect.Fixed32Kind:
		return uint32(v.Uint())
	case protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Sfixed64Kind:
		return v.Int()
	case protoreflect.Uint64Kind, protoreflect.Fixed64Kind:
		return v.Uint()
	case protoreflect.FloatKind:
		return float64(v.Float())
	case protoreflect.DoubleKind:
		return v.Float()
	case protoreflect.StringKind:
		return v.String()
	case protoreflect.BytesKind:
		return base64.StdEncoding.EncodeToString(v.Bytes())
	case protoreflect.EnumKind:
		ed := fd.Enum()
		if ev := ed.Values().ByNumber(v.Enum()); ev != nil {
			return string(ev.Name())
		}
		return int32(v.Enum())
	default:
		return fmt.Sprintf("%v", v.Interface())
	}
}

// ---------- 内置自测 ----------

const testProtoSrc = `syntax = "proto3";
package selftest;

message Item {
  string item_id = 1;
  string channel_id = 2;
  string vendor_id = 3;
  double min_price = 4;
}

message SampleMsg {
  string req_time = 1;
  repeated Item items = 2;
  int64 seq = 3;
  map<string, string> tags = 4;
}
`

func runSelftest() {
	dir, err := os.MkdirTemp("", "kfilter-selftest")
	if err != nil {
		fmt.Println("FAIL: mkdtemp:", err)
		os.Exit(1)
	}
	defer os.RemoveAll(dir)
	if err := os.WriteFile(filepath.Join(dir, "selftest.proto"), []byte(testProtoSrc), 0644); err != nil {
		fmt.Println("FAIL: write proto:", err)
		os.Exit(1)
	}
	dec, err := newProtoDecoder(dir, "", "selftest", "SampleMsg", false)
	if err != nil {
		fmt.Println("FAIL: proto decode init:", err)
		os.Exit(1)
	}
	// 构造消息
	m := dynamicpb.NewMessage(dec.desc)
	setField(m, "req_time", "1789987881653")
	itemDesc := dec.desc.Fields().ByName("items").Message()
	item1 := dynamicpb.NewMessage(itemDesc)
	setField(item1, "item_id", "item-aaa")
	setField(item1, "vendor_id", "vendor-001")
	setField(item1, "min_price", float64(11))
	item2 := dynamicpb.NewMessage(itemDesc)
	setField(item2, "item_id", "item-bbb")
	setField(item2, "vendor_id", "vendor-002")
	setField(item2, "min_price", float64(22.5))
	list := m.Mutable(m.Descriptor().Fields().ByName("items")).List()
	list.Append(protoreflect.ValueOfMessage(item1))
	list.Append(protoreflect.ValueOfMessage(item2))
	m.Set(m.Descriptor().Fields().ByName("seq"), protoreflect.ValueOfInt64(1234567890123))
	tm := m.Mutable(m.Descriptor().Fields().ByName("tags")).Map()
	tm.Set(protoreflect.ValueOfString("zone").MapKey(), protoreflect.ValueOfString("east-1"))
	bin, err := proto.Marshal(m)
	if err != nil {
		fmt.Println("FAIL: marshal:", err)
		os.Exit(1)
	}

	// 解码
	val, err := dec.decode(bin)
	if err != nil {
		fmt.Println("FAIL: unmarshal/decode:", err)
		os.Exit(1)
	}
	b, _ := json.MarshalIndent(val, "", "  ")
	fmt.Println("解码结果:")
	fmt.Println(string(b))

	obj := map[string]any{"value": val, "partition": int32(4), "offset": int64(12345678900)}

	cases := []struct {
		expr string
		want bool
	}{
		{`function(m){ return m.value.items[1].vendorId === "vendor-002"; }`, true},
		{`function(m){ return m.value.items[0].vendorId === "vendor-002"; }`, false},
		{`if (value.items[1].vendorId === "vendor-002") { return true; } return false;`, true},
		{`value.items[1].minPrice === 22.5`, true},
		{`m.value.seq === 1234567890123`, true},
		{`m.value.tags.zone === "east-1"`, true},
	}
	failed := 0
	for i, c := range cases {
		fn, err := compileFilter(c.expr)
		if err != nil {
			fmt.Printf("case %d 编译失败: %v\n", i+1, err)
			failed++
			continue
		}
		got, err := fn(obj)
		if err != nil {
			fmt.Printf("case %d 执行失败: %v\n", i+1, err)
			failed++
			continue
		}
		status := "PASS"
		if got != c.want {
			status = "FAIL"
			failed++
		}
		fmt.Printf("case %d %s: %s => %v (期望 %v)\n", i+1, status, c.expr, got, c.want)
	}
	if failed > 0 {
		fmt.Printf("\n自测失败: %d 个用例未通过\n", failed)
		os.Exit(1)
	}
	fmt.Println("\n自测全部通过 ✓")
}

func setField(m *dynamicpb.Message, name string, v any) {
	fd := m.Descriptor().Fields().ByName(protoreflect.Name(name))
	if fd == nil {
		// 兼容：按 JSON 名（lowerCamelCase）再找一次
		fd = m.Descriptor().Fields().ByJSONName(name)
	}
	if fd == nil {
		panic("no field " + name)
	}
	switch x := v.(type) {
	case string:
		m.Set(fd, protoreflect.ValueOfString(x))
	case int64:
		m.Set(fd, protoreflect.ValueOfInt64(x))
	case float64:
		m.Set(fd, protoreflect.ValueOfFloat64(x))
	case int32:
		m.Set(fd, protoreflect.ValueOfInt32(x))
	default:
		panic("unsupported")
	}
}
