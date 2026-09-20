package main

import (
	"os"
	"path/filepath"
	"testing"
)

// 两份同名但内容不同的 proto：用独有字段区分来源
func writeTestProtos(base string) (dirProto, fileMain string) {
	dirProto = filepath.Join(base, "proto")
	os.MkdirAll(dirProto, 0755)
	// -proto 目录里的版本：含 from_dir 字段
	os.WriteFile(filepath.Join(dirProto, "sample-msg.proto"), []byte(`syntax = "proto3";
package demo;
message SampleMsg { string vendor_id = 1; string from_dir = 2; }
`), 0644)
	// -proto-file 指定的版本：含 from_file 字段
	fileMain = filepath.Join(base, "sample-msg.proto")
	os.WriteFile(fileMain, []byte(`syntax = "proto3";
package demo;
message SampleMsg { string vendor_id = 1; string from_file = 2; }
`), 0644)
	return
}

func TestPriorityFileWins(t *testing.T) {
	base := t.TempDir()
	dirProto, fileMain := writeTestProtos(base)

	// 同时给 -proto(目录) 和 -proto-file(文件)，目录里有同名文件
	dec, err := newProtoDecoder(dirProto, fileMain, "sample-msg", "SampleMsg", false)
	if err != nil {
		t.Fatal(err)
	}
	if dec.desc.Fields().ByName("from_file") == nil {
		t.Fatalf("FAIL: 未使用 -proto-file 的文件; 实际消息: %s", dec.msgName)
	}
	if dec.desc.Fields().ByName("from_dir") != nil {
		t.Fatal("FAIL: 错误地使用了 -proto 目录里的同名文件")
	}
	t.Log("PASS: -proto-file 优先于 -proto 目录中的同名文件")
}

func TestPriorityDirOnly(t *testing.T) {
	base := t.TempDir()
	dirProto, _ := writeTestProtos(base)

	// 只给 -proto 目录：按 topic 名找到目录里的版本
	dec, err := newProtoDecoder(dirProto, "", "sample-msg", "SampleMsg", false)
	if err != nil {
		t.Fatal(err)
	}
	if dec.desc.Fields().ByName("from_dir") == nil {
		t.Fatal("FAIL: 仅 -proto 时未使用目录中的文件")
	}
	t.Log("PASS: 仅 -proto 时按 topic 名使用目录中的文件")
}

// 主文件 -proto-file 里 import 的公共类型：应从主文件目录优先解析
func TestImportResolvesFromMainFileDir(t *testing.T) {
	base := t.TempDir()
	dirProto := filepath.Join(base, "proto")
	os.MkdirAll(dirProto, 0755)
	// 主文件 import "common/base.proto"
	os.WriteFile(filepath.Join(base, "sample-msg.proto"), []byte(`syntax = "proto3";
package demo;
import "common/base.proto";
message SampleMsg { string vendor_id = 1; Base base = 2; }
`), 0644)
	os.MkdirAll(filepath.Join(base, "common"), 0755)
	os.WriteFile(filepath.Join(base, "common", "base.proto"), []byte(`syntax = "proto3";
package demo;
message Base { string from_main_dir = 1; }
`), 0644)
	os.MkdirAll(filepath.Join(dirProto, "common"), 0755)
	os.WriteFile(filepath.Join(dirProto, "common", "base.proto"), []byte(`syntax = "proto3";
package demo;
message Base { string from_proto_dir = 1; }
`), 0644)

	dec, err := newProtoDecoder(dirProto, filepath.Join(base, "sample-msg.proto"), "sample-msg", "SampleMsg", false)
	if err != nil {
		t.Fatal(err)
	}
	baseFD := dec.desc.Fields().ByName("base")
	if baseFD == nil {
		t.Fatal("base 字段缺失")
	}
	bd := baseFD.Message()
	if bd.Fields().ByName("from_main_dir") == nil {
		t.Fatal("FAIL: import 未优先从主文件所在目录解析")
	}
	if bd.Fields().ByName("from_proto_dir") != nil {
		t.Fatal("FAIL: import 错误地解析到了 -proto 目录")
	}
	t.Log("PASS: import 优先从主文件所在目录解析")
}
