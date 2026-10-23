package plugin

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v8/sdk/pluginabi"
	"github.com/router-for-me/CLIProxyAPI/v8/sdk/pluginapi"

	"newapi-provider/internal/catalog"
	"newapi-provider/internal/config"
)

// ProviderID is the single provider key served by this plugin (FR-001).
// ProviderID 是本插件唯一的认证和模型归属标识。
const ProviderID = "newapi"

// pluginName / pluginVersion are reported in registration metadata.
const (
	// pluginName 是宿主注册使用的稳定插件名称。
	pluginName = "newapi-provider"
	// pluginVersion 标记当前插件实现版本。
	pluginVersion = "0.1.0"
)

// githubRepoURL 指向本插件的源码及 Release 发布仓库，供宿主识别更新来源。
const githubRepoURL = "https://github.com/careerisx/cpa-plusins"

// registerRefreshTimeout 限制按需目录发现的同步等待时间。
const registerRefreshTimeout = 10 * time.Second

// Manager 管理逐认证目录、公开管理资源、执行器及额度缓存，支持并发 ABI 调用。
type Manager struct {
	// bridge 宿主回调桥接器，构造后不替换。
	bridge *HostBridge // immutable after NewManager

	// lifeMu 串行化注册、重配置和关闭过程。
	lifeMu sync.Mutex

	// mu 保护目录索引和额度缓存的生命周期。
	mu sync.RWMutex
	// catalogs 按宿主身份、认证配置及密钥版本隔离目录，最多保留 256 项。
	catalogs map[[32]byte]*authCatalog
	// quotaStatusCache 按公开状态地址保存换算参数，受 mu 保护。
	quotaStatusCache map[string]quotaStatusEntry
	// quotaGeneration 隔离重配置和关闭前后的在途状态请求，受 mu 保护。
	quotaGeneration uint64
}

// NewManager returns a dispatcher whose outbound traffic flows through bridge.
// NewManager 创建通过宿主桥接器发起回调的并发安全分发器。
func NewManager(bridge *HostBridge) *Manager {
	return &Manager{bridge: bridge}
}

// HandleCall dispatches one RPC method and returns envelope bytes. Handler
// failures travel inside the envelope; a recovered panic becomes a
// "plugin_error" envelope so the host process never dies with us.
// HandleCall 分发已启用的 ABI 方法，并将处理异常封装为错误响应。
func (m *Manager) HandleCall(method string, request []byte) (resp []byte, err error) {
	debugTrace("handle method=%s request_bytes=%d", method, len(request))
	defer func() {
		// Defensive: host-boundary panics are goroutine-contained (see
		// HostBridge.callWithTimeout); this guards future handler bugs.
		if r := recover(); r != nil {
			resp = ErrEnvelope("plugin_error", fmt.Sprintf("internal error handling %s", method))
			err = nil
		}
	}()
	switch method {
	case pluginabi.MethodPluginRegister, pluginabi.MethodPluginReconfigure:
		return m.handleLifecycle(request)
	case pluginabi.MethodModelStatic:
		return m.handleModels()
	case pluginabi.MethodModelForAuth:
		return m.handleModelsForAuth(request)
	case pluginabi.MethodPluginShutdown:
		return m.handleShutdown()
	case pluginabi.MethodExecutorExecute:
		return m.handleExecute(request)
	case pluginabi.MethodExecutorExecuteStream:
		return m.handleExecuteStream(request)
	case pluginabi.MethodAuthIdentifier:
		return okEnvelope(map[string]string{"identifier": ProviderID}), nil
	case pluginabi.MethodAuthParse:
		var req pluginapi.AuthParseRequest
		if json.Unmarshal(request, &req) != nil {
			return ErrEnvelope("invalid_request", "malformed auth parse request body"), nil
		}
		resp, err := (authProvider{}).ParseAuth(context.Background(), req)
		if err != nil {
			return ErrEnvelope("auth_failure", err.Error()), nil
		}
		debugTrace("auth parse response provider=%s id=%s attr_api_key_present=%t storage_json_bytes=%d", resp.Auth.Provider, resp.Auth.ID, strings.TrimSpace(resp.Auth.Attributes["api_key"]) != "", len(resp.Auth.StorageJSON))
		return okEnvelope(resp), nil
	case pluginabi.MethodAuthLoginStart, pluginabi.MethodAuthLoginPoll:
		return ErrEnvelope("auth_failure", "请使用插件菜单中的 New API 认证管理页面添加认证；旧登录和轮询入口已停用"), nil
	case pluginabi.MethodManagementRegister:
		return m.registerManagement(request)
	case pluginabi.MethodManagementHandle:
		return m.handleManagement(request)
	case pluginabi.MethodAuthRefresh:
		var req struct{ pluginapi.AuthRefreshRequest }
		if decodeAuthRequest(request, &req) != nil {
			return ErrEnvelope("invalid_request", "malformed auth refresh request body"), nil
		}
		resp, err := (authProvider{}).RefreshAuth(context.Background(), req.AuthRefreshRequest)
		if err != nil {
			return ErrEnvelope("auth_failure", err.Error()), nil
		}
		return okEnvelope(resp), nil
	case pluginabi.MethodQuotaIdentifier:
		return okEnvelope(map[string]string{"identifier": ProviderID}), nil
	case pluginabi.MethodQuotaDescribe:
		return okEnvelope(pluginapi.QuotaDescribeResponse{
			SupportedProviders: []string{ProviderID},
			DisplayName:        "New API 账号与令牌额度",
			SupportsReset:      false,
		}), nil
	case pluginabi.MethodQuotaFetch:
		return m.handleQuotaFetch(request)
	case pluginabi.MethodQuotaReset:
		return okEnvelope(pluginapi.QuotaResetResponse{Success: false, Message: "不支持重置 API 令牌额度"}), nil
	case pluginabi.MethodExecutorIdentifier:
		return okEnvelope(map[string]string{"identifier": ProviderID}), nil
	default:
		return ErrEnvelope("unknown_method", "unknown method: "+method), nil
	}
}

// lifecycleRequest mirrors rpcLifecycleRequest: config_yaml is base64.
// The request's schema_version is decoded-and-ignored; registration echoes
// pluginabi.SchemaVersion.
type lifecycleRequest struct {
	// ConfigYAML 宿主传入的配置内容。
	ConfigYAML []byte `json:"config_yaml"`
}

type capabilities struct {
	// ModelProvider 是否启用模型发现。
	ModelProvider bool `json:"model_provider"`
	// AuthProvider 是否启用认证解析。
	AuthProvider bool `json:"auth_provider"`
	// Executor 是否启用推理执行。
	Executor bool `json:"executor"`
	// ExecutorModelScope 执行器支持的模型注册范围。
	ExecutorModelScope pluginapi.ExecutorModelScope `json:"executor_model_scope,omitempty"`
	// ExecutorInputFormats 执行器接受的客户端协议。
	ExecutorInputFormats []string `json:"executor_input_formats,omitempty"`
	// ExecutorOutputFormats 执行器返回的客户端协议。
	ExecutorOutputFormats []string `json:"executor_output_formats,omitempty"`
	// ManagementAPI 启用公开管理页与受宿主管理认证保护的列表和保存入口。
	ManagementAPI bool `json:"management_api"`
	// QuotaProvider 启用宿主原生 API 令牌额度查询。
	QuotaProvider bool `json:"quota_provider"`
}

type registrationResult struct {
	// SchemaVersion 宿主 ABI 结构版本。
	SchemaVersion uint32 `json:"schema_version"`
	// Metadata 插件注册元数据。
	Metadata pluginapi.Metadata `json:"metadata"`
	// Capabilities 已启用的插件能力。
	Capabilities capabilities `json:"capabilities"`
}

// registrationEnvelope 声明逐认证模型、认证解析、执行、管理及只读额度能力。
func registrationEnvelope() []byte {
	formats := []string{"openai", "claude", "openai-response"}
	return okEnvelope(registrationResult{
		SchemaVersion: pluginabi.SchemaVersion,
		Metadata: pluginapi.Metadata{
			Name:             pluginName,
			Version:          pluginVersion,
			Author:           pluginName,
			GitHubRepository: githubRepoURL,
			ConfigFields:     []pluginapi.ConfigField{},
		},
		Capabilities: capabilities{
			ModelProvider: true,
			AuthProvider:  true,
			Executor:      true,
			// 宿主将认证绑定模型归类为 OAuth 范围，添加认证使用插件菜单管理页。
			ExecutorModelScope:    pluginapi.ExecutorModelScopeOAuth,
			ExecutorInputFormats:  formats,
			ExecutorOutputFormats: formats,
			ManagementAPI:         true,
			QuotaProvider:         true,
		},
	})
}

// handleLifecycle 校验全局结构并重置缓存，不借用认证在启动时发现模型。
func (m *Manager) handleLifecycle(request []byte) ([]byte, error) {
	var req lifecycleRequest
	if json.Unmarshal(request, &req) != nil {
		return ErrEnvelope("invalid_request", "插件配置请求无效"), nil
	}
	if _, err := config.Load(req.ConfigYAML); err != nil {
		return ErrEnvelope("invalid_config", err.Error()), nil
	}
	m.lifeMu.Lock()
	defer m.lifeMu.Unlock()
	m.mu.Lock()
	m.catalogs = nil
	m.quotaGeneration++
	m.quotaStatusCache = nil
	m.mu.Unlock()
	return registrationEnvelope(), nil
}

// authCatalog 只持有同一认证版本目录，独立锁合并并发发现。
type authCatalog struct {
	// mu 串行化本版本刷新与重试退避。
	mu sync.Mutex
	// manager 保存当前版本快照，绝不跨版本 SeedFrom。
	manager *catalog.Manager
	// nextRefresh 是下次按需刷新的最早时间，受 mu 保护。
	nextRefresh time.Time
	// touched 是最近访问时间，受 Manager.mu 保护，用于有界清理。
	touched time.Time
}

// catalogForAuth 按真实 AuthID、路由配置和密钥摘要标识版本，摘要不写日志。
func (m *Manager) catalogForAuth(parent context.Context, authID string, auth resolvedAuth) (*catalog.Manager, error) {
	if strings.TrimSpace(authID) == "" {
		return nil, fmt.Errorf("宿主未提供真实 AuthID，无法隔离认证目录")
	}
	version, _ := json.Marshal(struct {
		// AuthID 来自宿主请求而不是文件 JSON id。
		AuthID string
		// Config 包含目录地址、公开模型 ID 和协议配置。
		Config config.Config
		// KeyDigest 防止密钥轮换后复用旧目录。
		KeyDigest [32]byte
	}{authID, auth.cfg, sha256.Sum256([]byte(auth.key))})
	key := sha256.Sum256(version)
	now := time.Now()
	m.mu.Lock()
	if m.catalogs == nil {
		m.catalogs = make(map[[32]byte]*authCatalog)
	}
	for version, entry := range m.catalogs {
		if now.Sub(entry.touched) > time.Hour {
			delete(m.catalogs, version)
		}
	}
	entry := m.catalogs[key]
	if entry == nil {
		if len(m.catalogs) >= 256 {
			var oldest [32]byte
			var oldestTime time.Time
			for version, candidate := range m.catalogs {
				if oldestTime.IsZero() || candidate.touched.Before(oldestTime) {
					oldest, oldestTime = version, candidate.touched
				}
			}
			delete(m.catalogs, oldest)
		}
		var client catalog.HostClient
		if m.bridge != nil {
			client = m.bridge
		}
		entry = &authCatalog{manager: catalog.New(auth.cfg, client)}
		m.catalogs[key] = entry
	}
	entry.touched = now
	m.mu.Unlock()
	entry.mu.Lock()
	defer entry.mu.Unlock()
	if time.Now().Before(entry.nextRefresh) {
		return entry.manager, nil
	}
	ctx, cancel := context.WithTimeout(parent, registerRefreshTimeout)
	defer cancel()
	err := entry.manager.Refresh(ctx, auth.key)
	interval := auth.cfg.Catalog.RefreshInterval
	if err != nil {
		interval = 30 * time.Second
	}
	entry.nextRefresh = time.Now().Add(interval)
	// 失败仅沿用完全相同认证版本的快照，新版本自然为空。
	return entry.manager, nil
}

// handleModels 返回空静态目录，全部模型由具体认证贡献。
func (m *Manager) handleModels() ([]byte, error) { return modelEnvelope(nil), nil }

// modelEnvelope 将当前认证目录映射为宿主模型，不合并其他认证。
func modelEnvelope(manager *catalog.Manager) []byte {
	models := make([]pluginapi.ModelInfo, 0)
	if manager != nil {
		for _, rec := range manager.Models() {
			models = append(models, pluginapi.ModelInfo{
				ID: rec.PublicID, Object: "model", OwnedBy: ProviderID, DisplayName: rec.DisplayName,
				ContextLength: rec.ContextLimit, MaxCompletionTokens: rec.OutputLimit,
				SupportedInputModalities: rec.InputModes, SupportedOutputModalities: rec.OutputModes, Thinking: rec.Thinking,
			})
		}
	}
	return okEnvelope(pluginapi.ModelResponse{Provider: ProviderID, Models: models})
}

// handleModelsForAuth 使用本次认证内容和宿主回调上下文发现模型。
func (m *Manager) handleModelsForAuth(request []byte) ([]byte, error) {
	var req struct {
		// AuthModelRequest 接收 SDK 的逐认证上下文。
		pluginapi.AuthModelRequest
		// HostCallbackID 绑定宿主为当前认证准备的 HTTP 客户端。
		HostCallbackID string `json:"host_callback_id,omitempty"`
	}
	if decodeAuthRequest(request, &req) != nil {
		return ErrEnvelope("invalid_request", "模型请求无效"), nil
	}
	if req.AuthProvider != ProviderID {
		return m.handleModels()
	}
	auth, err := resolveAuth(req.StorageJSON, req.Metadata, req.Attributes)
	if err != nil {
		return ErrEnvelope("auth_failure", err.Error()), nil
	}
	ctx := context.WithValue(context.Background(), quotaCallbackContextKey{}, req.HostCallbackID)
	manager, err := m.catalogForAuth(ctx, req.AuthID, auth)
	if err != nil {
		return ErrEnvelope("auth_failure", err.Error()), nil
	}
	return modelEnvelope(manager), nil
}

// shutdownDrainTimeout bounds how long handleShutdown waits for orphaned
// host-callback goroutines (timed-out callbacks and their abandon/drain
// cleanup) to finish before returning. Package var so tests can shrink it.
// shutdownDrainTimeout 限制关闭时等待在途宿主回调的时间。
var shutdownDrainTimeout = 15 * time.Second

// handleShutdown stops the refresh loop, drains orphaned host callbacks,
// and clears state. The drain matters on Unix: the SDK loader frees host_api
// and dlclose's the plugin immediately after this export returns
// (loader_unix.go), so a goroutine still calling into the host would crash
// the process — see COMPATIBILITY.md limitations.
// handleShutdown 停止刷新并等待在途宿主回调，随后清空运行状态。
func (m *Manager) handleShutdown() ([]byte, error) {
	m.lifeMu.Lock()
	defer m.lifeMu.Unlock()
	if m.bridge != nil && !m.bridge.WaitForInFlight(shutdownDrainTimeout) {
		// Non-Windows unload during this residual window can crash the
		// host; the warn makes the stall visible without blocking forever.
		_ = m.bridge.Log("warn", "shutdown proceeding with host callbacks still in flight", nil)
	}
	m.mu.Lock()
	m.catalogs = nil
	// 关闭后丢弃换算缓存，并阻止旧请求重新填充。
	m.quotaGeneration++
	m.quotaStatusCache = nil
	m.mu.Unlock()
	return okEnvelope(struct{}{}), nil
}

// okEnvelope 将成功结果编码为统一 ABI 信封。
func okEnvelope(result any) []byte {
	raw, _ := json.Marshal(result)
	out, _ := json.Marshal(pluginabi.Envelope{OK: true, Result: raw})
	return out
}

// ErrEnvelope 将错误码和已脱敏消息编码为统一 ABI 错误信封。
func ErrEnvelope(code, message string) []byte {
	out, _ := json.Marshal(pluginabi.Envelope{OK: false, Error: &pluginabi.Error{Code: code, Message: message}})
	return out
}
