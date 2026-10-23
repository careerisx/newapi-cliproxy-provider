// Command newapi-provider 构建 CLIProxyAPI v8 的原生 New API provider 插件。
// 本文件只处理 C ABI、跨语言缓冲区与宿主回调，业务 RPC 交给内部 dispatcher。
package main

/*
#include <stdint.h>
#include <stdlib.h>

typedef struct {
	void* ptr;
	size_t len;
} cliproxy_buffer;

typedef int (*cliproxy_host_call_fn)(void*, const char*, const uint8_t*, size_t, cliproxy_buffer*);
typedef void (*cliproxy_host_free_fn)(void*, size_t);

typedef struct {
	uint32_t abi_version;
	void* host_ctx;
	cliproxy_host_call_fn call;
	cliproxy_host_free_fn free_buffer;
} cliproxy_host_api;

typedef int (*cliproxy_plugin_call_fn)(char*, uint8_t*, size_t, cliproxy_buffer*);
typedef void (*cliproxy_plugin_free_fn)(void*, size_t);
typedef void (*cliproxy_plugin_shutdown_fn)(void);

typedef struct {
	uint32_t abi_version;
	cliproxy_plugin_call_fn call;
	cliproxy_plugin_free_fn free_buffer;
	cliproxy_plugin_shutdown_fn shutdown;
} cliproxy_plugin_api;

extern int cliproxyPluginCall(char*, uint8_t*, size_t, cliproxy_buffer*);
extern void cliproxyPluginFree(void*, size_t);
extern void cliproxyPluginShutdown(void);

// stored_host 保存宿主提供的回调表，其有效期由插件生命周期约束。
static const cliproxy_host_api* stored_host;

// store_host_api 在 ABI 初始化时保存宿主回调表。
static void store_host_api(const cliproxy_host_api* host) {
	stored_host = host;
}

// call_host_api 调用宿主 RPC，不接管宿主返回缓冲区的所有权。
static int call_host_api(const char* method, const uint8_t* request, size_t request_len, cliproxy_buffer* response) {
	if (stored_host == NULL || stored_host->call == NULL) {
		return 1;
	}
	return stored_host->call(stored_host->host_ctx, method, request, request_len, response);
}

// free_host_buffer 通过宿主分配器释放宿主返回的缓冲区。
static void free_host_buffer(void* ptr, size_t len) {
	if (stored_host != NULL && stored_host->free_buffer != NULL && ptr != NULL) {
		stored_host->free_buffer(ptr, len);
	}
}
*/
import "C"

import (
	"fmt"
	"unsafe"

	"github.com/router-for-me/CLIProxyAPI/v8/sdk/pluginabi"

	"newapi-provider/internal/plugin"
)

// main 仅满足 c-shared 主包要求，宿主通过导出接口驱动插件。
func main() {}

// maxRequestLen 防止 size_t 转换为 C.int 时溢出并导致 C.GoBytes 崩溃。
const maxRequestLen = 1<<31 - 1

// dispatcher 持有插件状态，所有网络访问经 C 宿主回调桥接。
var dispatcher = plugin.NewManager(plugin.NewHostBridge(callHost))

// cliproxy_plugin_init 校验 ABI 并向宿主安装调用、释放和关闭入口。
//
//export cliproxy_plugin_init
func cliproxy_plugin_init(host *C.cliproxy_host_api, plug *C.cliproxy_plugin_api) C.int {
	if host == nil || host.abi_version != C.uint32_t(pluginabi.ABIVersion) || plug == nil {
		return 1
	}
	C.store_host_api(host)
	plug.abi_version = C.uint32_t(pluginabi.ABIVersion)
	plug.call = C.cliproxy_plugin_call_fn(C.cliproxyPluginCall)
	plug.free_buffer = C.cliproxy_plugin_free_fn(C.cliproxyPluginFree)
	plug.shutdown = C.cliproxy_plugin_shutdown_fn(C.cliproxyPluginShutdown)
	return 0
}

// cliproxyPluginCall 复制宿主请求并分发 RPC，返回由插件分配的响应缓冲区。
//
//export cliproxyPluginCall
func cliproxyPluginCall(method *C.char, request *C.uint8_t, requestLen C.size_t, response *C.cliproxy_buffer) C.int {
	if response != nil {
		response.ptr = nil
		response.len = 0
	}
	if method == nil {
		writeResponse(response, plugin.ErrEnvelope("invalid_method", "method is required"))
		return 1
	}
	var req []byte
	if request != nil && requestLen > 0 {
		if requestLen > maxRequestLen {
			writeResponse(response, plugin.ErrEnvelope("plugin_error", "request exceeds maximum size"))
			return 0
		}
		req = C.GoBytes(unsafe.Pointer(request), C.int(requestLen))
	}
	raw, err := dispatcher.HandleCall(C.GoString(method), req)
	if err != nil {
		writeResponse(response, plugin.ErrEnvelope("plugin_error", err.Error()))
		return 1
	}
	writeResponse(response, raw)
	return 0
}

// cliproxyPluginFree 释放插件为宿主响应分配的 C 缓冲区。
//
//export cliproxyPluginFree
func cliproxyPluginFree(ptr unsafe.Pointer, _ C.size_t) {
	if ptr != nil {
		C.free(ptr)
	}
}

// cliproxyPluginShutdown 停止目录刷新并等待在途宿主调用结束。
//
//export cliproxyPluginShutdown
func cliproxyPluginShutdown() {
	_, _ = dispatcher.HandleCall(pluginabi.MethodPluginShutdown, nil)
}

// callHost 将内部 RawCaller 转接到宿主 C API；错误只包含方法名和返回码。
func callHost(method string, payload []byte) ([]byte, error) {
	cMethod := C.CString(method)
	defer C.free(unsafe.Pointer(cMethod))
	var req *C.uint8_t
	if len(payload) > 0 {
		req = (*C.uint8_t)(C.CBytes(payload))
		defer C.free(unsafe.Pointer(req))
	}
	var resp C.cliproxy_buffer
	if rc := C.call_host_api(cMethod, req, C.size_t(len(payload)), &resp); rc != 0 {
		return nil, fmt.Errorf("host callback %s failed (rc=%d)", method, rc)
	}
	if resp.ptr == nil || resp.len == 0 {
		return nil, fmt.Errorf("host callback %s returned empty buffer", method)
	}
	if resp.len > maxRequestLen {
		C.free_host_buffer(resp.ptr, resp.len)
		return nil, fmt.Errorf("host callback %s returned oversized buffer", method)
	}
	out := C.GoBytes(unsafe.Pointer(resp.ptr), C.int(resp.len))
	C.free_host_buffer(resp.ptr, resp.len)
	return out, nil
}

// writeResponse 使用 C 分配器返回响应，所有权交给宿主调用插件释放入口归还。
func writeResponse(response *C.cliproxy_buffer, raw []byte) {
	if response == nil || len(raw) == 0 {
		return
	}
	response.ptr = C.CBytes(raw)
	response.len = C.size_t(len(raw))
}
