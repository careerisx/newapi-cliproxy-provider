// Package messages 实现 Messages 上游与三种客户端协议的请求、响应及 SSE 转换。
package messages

import (
	"net/http"

)

// EndpointPath is the upstream endpoint path for the Messages route (FR-004).
// EndpointPath 目标上游协议的标准端点路径，独立于模型目录和路由管理器。
var EndpointPath = "/v1/messages"

// anthropicVersion 是 Messages 上游要求的协议版本请求头值。
const anthropicVersion = "2023-06-01"

// AuthHeaders returns the upstream authentication headers for a Messages
// request: Bearer authorization plus the anthropic-version header. The
// executor wires these in M5; the key is never logged (FR-007, FR-011).
// AuthHeaders 构造当前协议的上游认证请求头，不记录密钥。
func AuthHeaders(key string) http.Header {
	h := make(http.Header)
	h.Set("Authorization", "Bearer "+key)
	h.Set("anthropic-version", anthropicVersion)
	return h
}
