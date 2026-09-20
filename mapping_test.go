package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"
)

// 映射模式回归测试：多个 topic 共享两个 proto 文件（无 package 主文件 + 有 package 被引文件，
// 含跨文件引用与跨文件嵌套类型），复刻 Redpanda serde.protobuf.mappings 的语义。
// proto 内容为中性示例，不含业务字段。

func writeSharedProtos(base string) string {
	dir := filepath.Join(base, "protos")
	os.MkdirAll(dir, 0755)
	// 被引用文件：package shop，含嵌套 message
	os.WriteFile(filepath.Join(dir, "common.proto"), []byte(`syntax = "proto3";
package shop;

enum PayType {
  PAY_TYPE_UNSPECIFIED = 0;
  PAY_ONLINE = 1;
}

message Order {
  string order_id = 1;
  int64 amount = 2;
}

message Receipt {
  string receipt_id = 1;
  message Item {
    string sku = 1;
    int32 qty = 2;
  }
  repeated Item items = 2;
}
`), 0644)
	// 主文件：无 package（message 全名 = 短名），import common.proto
	os.WriteFile(filepath.Join(dir, "shared_events.proto"), []byte(`syntax = "proto3";

import "common.proto";

message OrderRequest {
  string req_id = 1;
  int64 req_time = 2;
  shop.PayType pay_type = 3;
  shop.Order payload = 4;
  shop.Receipt.Item item = 5;
}

message OrderResponse {
  string resp_id = 1;
  bool ok = 2;
}
`), 0644)
	return dir
}

func TestMappingSharedProtos(t *testing.T) {
	dir := writeSharedProtos(t.TempDir())
	mappings := map[string]string{
		"svc_order_req": "OrderRequest",
		"svc_order_rsp": "OrderResponse",
	}

	dec, err := newProtoDecoderMapped(dir, "", "svc_order_req", "", false, mappings)
	if err != nil {
		t.Fatal(err)
	}
	if dec.msgName != "OrderRequest" {
		t.Fatalf("want OrderRequest, got %s", dec.msgName)
	}
	// 跨文件引用：payload 是 shop.Order
	if f := dec.desc.Fields().ByName("payload"); f == nil || f.Message() == nil {
		t.Fatal("payload field missing")
	} else if got := string(f.Message().FullName()); got != "shop.Order" {
		t.Fatalf("payload type = %s, want shop.Order", got)
	}
	// 跨文件嵌套类型：item 是 shop.Receipt.Item
	if f := dec.desc.Fields().ByName("item"); f == nil || f.Message() == nil {
		t.Fatal("item field missing")
	} else if got := string(f.Message().FullName()); got != "shop.Receipt.Item" {
		t.Fatalf("item type = %s, want shop.Receipt.Item", got)
	}

	// 同一目录映射到另一个类型
	dec2, err := newProtoDecoderMapped(dir, "", "svc_order_rsp", "", false, mappings)
	if err != nil {
		t.Fatal(err)
	}
	if dec2.msgName != "OrderResponse" {
		t.Fatalf("want OrderResponse, got %s", dec2.msgName)
	}
}

// 映射模式端到端：构造 → 序列化 → 动态解码 → 字段值核对（含 enum 名、跨文件嵌套）
func TestMappingRoundTripDecode(t *testing.T) {
	dir := writeSharedProtos(t.TempDir())
	mappings := map[string]string{"svc_order_req": "OrderRequest"}
	dec, err := newProtoDecoderMapped(dir, "", "svc_order_req", "", false, mappings)
	if err != nil {
		t.Fatal(err)
	}

	m := dynamicpb.NewMessage(dec.desc)
	fdReqID := dec.desc.Fields().ByName("req_id")
	m.Set(fdReqID, protoreflect.ValueOfString("req-001"))
	m.Set(dec.desc.Fields().ByName("req_time"), protoreflect.ValueOfInt64(1700000000000))
	m.Set(dec.desc.Fields().ByName("pay_type"), protoreflect.ValueOfEnum(1)) // PAY_ONLINE
	orderDesc := dec.desc.Fields().ByName("payload").Message()
	order := dynamicpb.NewMessage(orderDesc)
	order.Set(orderDesc.Fields().ByName("order_id"), protoreflect.ValueOfString("ord-009"))
	m.Set(dec.desc.Fields().ByName("payload"), protoreflect.ValueOfMessage(order))

	bin, err := proto.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	val, err := dec.decode(bin)
	if err != nil {
		t.Fatal(err)
	}
	obj, ok := val.(map[string]any)
	if !ok {
		t.Fatalf("decode result type = %T", val)
	}
	if obj["reqId"] != "req-001" {
		t.Fatalf("reqId = %v", obj["reqId"])
	}
	payload := obj["payload"].(map[string]any)
	if payload["orderId"] != "ord-009" {
		t.Fatalf("payload.orderId = %v", payload["orderId"])
	}
	if obj["payType"] != "PAY_ONLINE" {
		t.Fatalf("payType = %v（期望枚举名）", obj["payType"])
	}
}

// 全限定名映射 + 缺失类型报清晰错误
func TestMappingQualifiedAndMissing(t *testing.T) {
	dir := writeSharedProtos(t.TempDir())
	// 全限定名可用
	if _, err := newProtoDecoderMapped(dir, "", "t1", "", false, map[string]string{"t1": "shop.Order"}); err != nil {
		t.Fatalf("全限定名映射失败: %v", err)
	}
	// 不存在的类型报清晰错误
	_, err := newProtoDecoderMapped(dir, "", "t2", "", false, map[string]string{"t2": "app.TraceEvent"})
	if err == nil {
		t.Fatal("缺失类型应报错")
	}
	if !strings.Contains(err.Error(), "app.TraceEvent") {
		t.Fatalf("错误信息应包含类型名: %v", err)
	}
}

// 短名歧义检测：两个文件定义同名 message，裸短名报错并提示候选，全限定名消歧
func TestMappingAmbiguousShortName(t *testing.T) {
	dir := t.TempDir()
	write := func(name, pkg string) {
		src := "syntax = \"proto3\";\npackage " + pkg + ";\nmessage Dup { string x = 1; }\n"
		if err := os.WriteFile(filepath.Join(dir, name), []byte(src), 0644); err != nil {
			t.Fatal(err)
		}
	}
	write("a.proto", "aa")
	write("b.proto", "bb")

	_, err := newProtoDecoderMapped(dir, "", "tx", "", false, map[string]string{"tx": "Dup"})
	if err == nil {
		t.Fatal("裸短名歧义应报错")
	}
	if !strings.Contains(err.Error(), "aa.Dup") || !strings.Contains(err.Error(), "bb.Dup") {
		t.Fatalf("错误信息应列出候选: %v", err)
	}
	if _, err := newProtoDecoderMapped(dir, "", "tx", "", false, map[string]string{"tx": "aa.Dup"}); err != nil {
		t.Fatalf("全限定名应能消歧: %v", err)
	}
}

// parseTopicMap 参数解析
func TestParseTopicMap(t *testing.T) {
	m, err := parseTopicMap("a=Foo, b=bar.Baz ,,")
	if err != nil {
		t.Fatal(err)
	}
	if m["a"] != "Foo" || m["b"] != "bar.Baz" || len(m) != 2 {
		t.Fatalf("parse result = %v", m)
	}
	if _, err := parseTopicMap("bad-item"); err == nil {
		t.Fatal("缺 = 的项应报错")
	}
}
