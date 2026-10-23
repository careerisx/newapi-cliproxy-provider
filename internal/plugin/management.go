package plugin

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"

	"github.com/router-for-me/CLIProxyAPI/v8/sdk/pluginapi"

	"newapi-provider/resources"
)

// registerManagement 只在公开静态资源声明菜单，敏感 GET/POST/PATCH 均由宿主管理认证保护。
func (m *Manager) registerManagement(request []byte) ([]byte, error) {
	var req pluginapi.ManagementRegistrationRequest
	if json.Unmarshal(request, &req) != nil {
		return ErrEnvelope("invalid_request", "管理注册请求无效"), nil
	}
	return okEnvelope(map[string]any{
		"routes": []map[string]string{
			{"method": "GET", "path": "/plugins/" + pluginName + "/auths"},
			{"method": "POST", "path": "/plugins/" + pluginName + "/auths"},
			{"method": "PATCH", "path": "/plugins/" + pluginName + "/auths"},
		},
		"resources": []map[string]string{{"path": "/login", "menu": "New API 认证管理", "description": "添加、编辑及查看当前插件认证"}},
	}), nil
}

// handleManagement 公开路径仅提供无秘密静态页面，认证读写只接受宿主管理路由。
func (m *Manager) handleManagement(request []byte) ([]byte, error) {
	var req struct {
		// ManagementRequest 是宿主路由分发后的请求。
		pluginapi.ManagementRequest
		// HostCallbackID 保留本次宿主回调上下文。
		HostCallbackID string `json:"host_callback_id,omitempty"`
	}
	if json.Unmarshal(request, &req) != nil {
		return ErrEnvelope("invalid_request", "管理请求无效"), nil
	}
	if req.Method == http.MethodGet && strings.HasSuffix(req.Path, "/resource/plugins/"+pluginName+"/login") {
		return okEnvelope(pluginapi.ManagementResponse{
			StatusCode: http.StatusOK,
			Headers: http.Header{
				"Content-Type": {"text/html; charset=utf-8"}, "Cache-Control": {"no-store"},
				"Referrer-Policy": {"no-referrer"}, "X-Content-Type-Options": {"nosniff"},
				// 不限制父页面来源，便于 CPA 内嵌；管理请求仍限定同源并要求鉴权。
				"Content-Security-Policy": {"default-src 'none'; script-src 'unsafe-inline'; style-src 'unsafe-inline'; connect-src 'self'; base-uri 'none'; form-action 'none'"},
			}, Body: []byte(resources.LoginPage),
		}), nil
	}
	if req.Path == "/v0/management/plugins/"+pluginName+"/auths" {
		ctx := context.WithValue(context.Background(), quotaCallbackContextKey{}, req.HostCallbackID)
		if req.Method == http.MethodGet {
			return okEnvelope(m.listAuths(ctx)), nil
		}
		if req.Method == http.MethodPost {
			return okEnvelope(m.addAuth(ctx, req.Body)), nil
		}
		if req.Method == http.MethodPatch {
			return okEnvelope(m.editAuth(ctx, req.Body)), nil
		}
	}
	return okEnvelope(loginJSON(404, "error", "未找到入口")), nil
}

// managementJSONHeaders 禁止管理数据缓存及内容嗅探。
func managementJSONHeaders() http.Header {
	return http.Header{"Content-Type": {"application/json; charset=utf-8"}, "Cache-Control": {"no-store"}, "X-Content-Type-Options": {"nosniff"}}
}

// loginJSON 返回固定脱敏的管理响应，不回显任何认证材料。
func loginJSON(status int, phase, message string) pluginapi.ManagementResponse {
	body, _ := json.Marshal(map[string]string{"status": phase, "message": message})
	return pluginapi.ManagementResponse{StatusCode: status, Headers: managementJSONHeaders(), Body: body}
}
