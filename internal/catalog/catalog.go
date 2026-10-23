// Package catalog 获取上游模型目录、规范能力元数据并维护最近成功的快照。
// 所有模型仅由全局 upstream-format 选择协议，supported_endpoints 不参与解码或路由。
// 刷新调度由调用方负责，缺失或空目录均为合法快照。
package catalog

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"sync"

	"github.com/router-for-me/CLIProxyAPI/v8/sdk/pluginapi"

	"newapi-provider/internal/config"
)

// catalogBudgetFloor 为目录保留至少 16 MiB 的响应预算，避免小响应限制阻断刷新。
const catalogBudgetFloor = 16 << 20

// Route 表示所有模型共用的上游请求协议。
type Route string

const (
	// RouteChatCompletions 表示 OpenAI Chat Completions 协议。
	RouteChatCompletions Route = "chat-completions"
	// RouteMessages 表示 Claude Messages 协议。
	RouteMessages Route = "messages"
	// RouteResponses 表示 OpenAI Responses 协议。
	RouteResponses Route = "responses"
)

// routeEndpoints 固定可信协议对应的版本化端点，不接受目录提供的任意地址。
var routeEndpoints = map[Route]string{
	RouteChatCompletions: "/v1/chat/completions",
	RouteMessages:        "/v1/messages",
	RouteResponses:       "/v1/responses",
}

// EndpointPath 返回协议的上游端点路径；未知协议返回空字符串。
func (r Route) EndpointPath() string {
	return routeEndpoints[r]
}

// ModelRecord 保存一个可路由模型的身份、统一路由与能力元数据。
type ModelRecord struct {
	// PublicID 是面向客户端的模型 ID。
	PublicID string
	// UpstreamID 是发送给上游的原始模型 ID。
	UpstreamID string
	// DisplayName 是目录提供的名称，缺省时使用上游 ID。
	DisplayName string
	// Protocol 是根据当前认证格式选出的协议。
	Protocol Route
	// EndpointPath 是该协议固定的版本化请求路径。
	EndpointPath string
	// ContextLimit 是目录声明的上下文 token 上限。
	ContextLimit int64
	// OutputLimit 是目录声明的输出 token 上限。
	OutputLimit int64
	// InputModes 是目录声明的输入模态。
	InputModes []string
	// OutputModes 是目录声明的输出模态。
	OutputModes []string
	// Thinking 是可选的思考能力描述。
	Thinking *pluginapi.ThinkingSupport
}

// UnsupportedModel 保存因身份冲突、缺失 ID 或配置无效而排除的模型诊断。
type UnsupportedModel struct {
	// UpstreamID 标识被排除的上游模型。
	UpstreamID string
	// Reason 说明排除原因，不包含认证凭据。
	Reason string
}

// HostClient 通过宿主发送 HTTP 请求，遵循 pluginapi.HTTPDo 的语义。
type HostClient interface {
	// Do 使用调用方上下文发送宿主 HTTP 请求。
	Do(ctx context.Context, req pluginapi.HTTPRequest) (pluginapi.HTTPResponse, error)
}

// rawModel 只解码身份和能力；未知字段（包括 supported_endpoints）完全忽略。
type rawModel struct {
	// ID 接收上游模型标识。
	ID string `json:"id"`
	// DisplayName 接收可选展示名称。
	DisplayName string `json:"display_name"`
	// ContextLength 接收上下文 token 上限。
	ContextLength int64 `json:"context_length"`
	// MaxOutputTokens 接收输出 token 上限。
	MaxOutputTokens int64 `json:"max_output_tokens"`
	// InputModes 接收首选输入模态字段。
	InputModes []string `json:"input_modes"`
	// OutputModes 接收首选输出模态字段。
	OutputModes []string `json:"output_modes"`
	// InputModalities 接收兼容的输入模态字段。
	InputModalities []string `json:"input_modalities"`
	// OutputModalities 接收兼容的输出模态字段。
	OutputModalities []string `json:"output_modalities"`
	// Thinking 接收可选思考能力对象。
	Thinking *rawThinking `json:"thinking"`
}

// rawThinking 兼容上游 snake_case 字段，保留可选字段是否存在的信息。
type rawThinking struct {
	// Min 接收最小思考预算。
	Min *int `json:"min"`
	// Max 接收最大思考预算。
	Max *int `json:"max"`
	// ZeroAllowed 接收是否允许零思考预算。
	ZeroAllowed *bool `json:"zero_allowed"`
	// DynamicAllowed 接收是否允许动态思考预算。
	DynamicAllowed *bool `json:"dynamic_allowed"`
	// Levels 接收上游支持的思考等级。
	Levels []string `json:"levels"`
}

// DefaultRoute 对应默认 upstream-format=openai，与目录字段无关。
const DefaultRoute = RouteChatCompletions

// Manager 持有目录快照，所有访问方法均可与 Refresh 并发调用。
type Manager struct {
	// cfg 保存本管理器的配置，构造后不再修改。
	cfg config.Config
	// client 通过宿主执行目录请求。
	client HostClient
	// mu 保护快照与索引的一致性。
	mu sync.Mutex
	// raw 保留本认证版本最近一次成功解析的原始目录条目。
	raw []rawModel
	// models 保存当前可路由模型。
	models []ModelRecord
	// index 同时以公开 ID 和上游 ID 索引模型。
	index map[string]ModelRecord
	// unsup 保存当前快照的排除诊断。
	unsup []UnsupportedModel
	// warns 保存重复 ID 与索引冲突的诊断。
	warns []string
}

// New 使用给定配置与宿主客户端创建目录管理器；配置应由 config.Load 加载。
func New(cfg config.Config, client HostClient) *Manager {
	return &Manager{cfg: cfg, client: client}
}

// Refresh 使用 Bearer GET 获取目录并原子替换快照；缺失 data、null 与空数组均合法。
// 失败时返回不含密钥的分类错误，并根据 stale-while-unavailable 决定是否保留旧快照。
func (m *Manager) Refresh(ctx context.Context, apiKey string) error {
	if strings.TrimSpace(apiKey) == "" {
		return m.fail("no enabled newapi auth files")
	}
	if m.client == nil {
		return m.fail("host client unavailable")
	}
	req := pluginapi.HTTPRequest{
		Method: http.MethodGet,
		URL:    m.cfg.CatalogURL,
		Headers: http.Header{
			"Authorization": []string{"Bearer " + apiKey},
			"Accept":        []string{"application/json"},
		},
	}
	resp, err := m.client.Do(ctx, req)
	if err != nil {
		return m.fail("network error")
	}
	budget := max(m.cfg.MaxResponseBytes, catalogBudgetFloor)
	if int64(len(resp.Body)) > budget {
		return m.fail("response exceeds max-response-bytes")
	}
	if resp.StatusCode != http.StatusOK {
		return m.fail(fmt.Sprintf("http %d", resp.StatusCode))
	}
	var env struct {
		// Data 接收可选模型列表，缺失时构建空快照。
		Data json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(resp.Body, &env); err != nil {
		return m.fail("invalid json")
	}
	var entries []rawModel
	if env.Data != nil {
		if err := json.Unmarshal(env.Data, &entries); err != nil {
			return m.fail("invalid json")
		}
	}
	m.swap(entries)
	return nil
}

// fail 应用失败时的快照策略，并返回不包含上游响应或密钥的分类错误。
func (m *Manager) fail(category string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.cfg.Catalog.StaleWhileUnavailable {
		m.raw, m.models, m.index, m.unsup, m.warns = nil, nil, nil, nil, nil
	}
	return fmt.Errorf("catalog refresh failed: %s", category)
}

// swap 根据当前当前认证格式构建新快照，检查身份冲突并在锁内一次性发布。
func (m *Manager) swap(entries []rawModel, extraWarns ...string) {
	models := make([]ModelRecord, 0, len(entries))
	index := make(map[string]ModelRecord, len(entries)*2)
	var unsup []UnsupportedModel
	var warns []string
	warns = append(warns, extraWarns...)
	seen := make(map[string]bool, len(entries))
	route := m.globalRoute()
	endpoint := route.EndpointPath()
	for _, e := range entries {
		if e.ID == "" {
			unsup = append(unsup, UnsupportedModel{Reason: "missing id"})
			continue
		}
		if seen[e.ID] {
			warns = append(warns, fmt.Sprintf("duplicate model %q ignored; keeping first occurrence", e.ID))
			continue
		}
		seen[e.ID] = true
		// 正常配置已由 Load 校验；直接构造无效配置时不推断或回退其他协议。
		if endpoint == "" {
			unsup = append(unsup, UnsupportedModel{UpstreamID: e.ID, Reason: "invalid upstream-format"})
			continue
		}
		if JoinUpstreamURL(m.cfg.BaseURL, endpoint) == "" {
			unsup = append(unsup, UnsupportedModel{UpstreamID: e.ID, Reason: "invalid upstream endpoint or base URL"})
			continue
		}
		display := e.DisplayName
		if display == "" {
			display = e.ID
		}
		rec := ModelRecord{
			PublicID:     config.PublicID(m.cfg, e.ID),
			UpstreamID:   e.ID,
			DisplayName:  display,
			Protocol:     route,
			EndpointPath: endpoint,
			ContextLimit: e.ContextLength,
			OutputLimit:  e.MaxOutputTokens,
			InputModes:   firstNonEmpty(e.InputModes, e.InputModalities),
			OutputModes:  firstNonEmpty(e.OutputModes, e.OutputModalities),
			Thinking:     normalizeThinking(e.Thinking),
		}
		// 公开 ID 可能与其他模型的上游 ID 相同，按目录顺序保留首个模型以防静默误路由。
		if other, ok := index[rec.PublicID]; ok {
			reason := fmt.Sprintf("public id %q collides with upstream id %q", rec.PublicID, other.UpstreamID)
			warns = append(warns, reason)
			unsup = append(unsup, UnsupportedModel{UpstreamID: e.ID, Reason: reason})
			continue
		}
		if other, ok := index[rec.UpstreamID]; ok {
			reason := fmt.Sprintf("public id %q collides with upstream id %q", other.PublicID, rec.UpstreamID)
			warns = append(warns, reason)
			unsup = append(unsup, UnsupportedModel{UpstreamID: e.ID, Reason: reason})
			continue
		}
		models = append(models, rec)
		index[rec.PublicID] = rec
		index[rec.UpstreamID] = rec
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.raw, m.models, m.index, m.unsup, m.warns = entries, models, index, unsup, warns
}

// globalRoute 仅根据当前认证格式选择协议；空值兼容直接构造配置时的默认格式。
func (m *Manager) globalRoute() Route {
	switch m.cfg.UpstreamFormat {
	case "", "openai":
		return RouteChatCompletions
	case "claude":
		return RouteMessages
	case "openai-response":
		return RouteResponses
	default:
		return ""
	}
}

// JoinUpstreamURL 在可信基础路径下拼接端点，统一补齐一个末尾 v1 版本段。
// 例如域名基础地址与 /models 拼为 /v1/models，/proxy/v1 与 /v1/messages
// 拼为 /proxy/v1/messages；端点必须是路径，不允许 URL、查询、片段或路径遍历。
// 无效或可能逃逸基础路径的输入返回空字符串，调用方不得继续发送请求。
func JoinUpstreamURL(baseURL, endpoint string) string {
	base, err := url.Parse(baseURL)
	if err != nil || base.Host == "" || base.Hostname() == "" || base.User != nil || base.Opaque != "" {
		return ""
	}
	if base.Scheme != "https" && base.Scheme != "http" {
		return ""
	}
	if base.RawQuery != "" || base.ForceQuery || base.Fragment != "" || strings.Contains(baseURL, "#") {
		return ""
	}
	ep, err := url.Parse(endpoint)
	if err != nil || ep.IsAbs() || ep.Host != "" || ep.User != nil || ep.Opaque != "" {
		return ""
	}
	if ep.RawQuery != "" || ep.ForceQuery || ep.Fragment != "" || strings.Contains(endpoint, "#") || ep.Path == "" {
		return ""
	}
	if !safePath(base.Path) || !safePath(ep.Path) {
		return ""
	}
	// 使用 URL.Path 赋值而非引用解析，端点始终附加于基础路径，不会覆盖主机或路径前缀。
	basePath := strings.TrimRight(base.Path, "/")
	if !strings.HasSuffix(basePath, "/v1") {
		basePath += "/v1"
	}
	endpointPath := strings.TrimPrefix(ep.Path, "/")
	if endpointPath == "v1" {
		endpointPath = ""
	} else {
		endpointPath = strings.TrimPrefix(endpointPath, "v1/")
	}
	base.Path = basePath
	if endpointPath != "" {
		base.Path += "/" + endpointPath
	}
	base.RawPath = ""
	return base.String()
}

// safePath 拒绝解码后的遍历段、反斜杠、重复分隔符与可被二次解码的残留转义。
func safePath(path string) bool {
	if strings.ContainsAny(path, "\\%") || strings.Contains(path, "//") {
		return false
	}
	for _, segment := range strings.Split(path, "/") {
		if segment == "." || segment == ".." {
			return false
		}
	}
	return true
}

// firstNonEmpty 优先保留首选模态字段，兼容目录的另一种字段名称。
func firstNonEmpty(a, b []string) []string {
	if len(a) > 0 {
		return a
	}
	return b
}

// normalizeThinking 将可选的上游思考能力转换为宿主类型，缺失对象保持 nil。
func normalizeThinking(rt *rawThinking) *pluginapi.ThinkingSupport {
	if rt == nil {
		return nil
	}
	t := &pluginapi.ThinkingSupport{}
	if rt.Min != nil {
		t.Min = *rt.Min
	}
	if rt.Max != nil {
		t.Max = *rt.Max
	}
	if rt.ZeroAllowed != nil {
		t.ZeroAllowed = *rt.ZeroAllowed
	}
	if rt.DynamicAllowed != nil {
		t.DynamicAllowed = *rt.DynamicAllowed
	}
	if len(rt.Levels) > 0 {
		t.Levels = append([]string(nil), rt.Levels...)
	}
	return t
}

// Models 返回当前可路由快照的浅拷贝；嵌套切片和 Thinking 由调用方只读使用。
func (m *Manager) Models() []ModelRecord {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]ModelRecord, len(m.models))
	copy(out, m.models)
	return out
}

// Lookup 按公开 ID 或原始上游 ID 查找模型；身份冲突已在快照构建时排除。
func (m *Manager) Lookup(id string) (ModelRecord, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	rec, ok := m.index[id]
	return rec, ok
}

// Unsupported 返回被排除模型及其原因的副本，不依据 supported_endpoints 排除模型。
func (m *Manager) Unsupported() []UnsupportedModel {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]UnsupportedModel, len(m.unsup))
	copy(out, m.unsup)
	return out
}

// Warnings 返回重复模型和身份冲突的诊断副本。
func (m *Manager) Warnings() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]string, len(m.warns))
	copy(out, m.warns)
	return out
}
