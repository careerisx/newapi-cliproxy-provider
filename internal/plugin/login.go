package plugin

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v8/sdk/pluginapi"
)

// authAddRequest 是管理页可提交字段白名单，不接受文件名、路径、provider 或宿主运行期属性。
type authAddRequest struct {
	// Label 是宿主显示名称。
	Label string `json:"label"`
	// BaseURL 是必填的当前认证上游地址。
	BaseURL string `json:"base_url"`
	// APIKey 是必填的推理密钥，不记录或回显。
	APIKey string `json:"api_key"`
	// UpstreamFormat 是当前认证协议，空指针使用默认协议。
	UpstreamFormat *string `json:"upstream-format"`
	// QuotaURL 是可选额度地址。
	QuotaURL string `json:"quota-url"`
	// CatalogURL 是可选目录地址，仅属于当前认证。
	CatalogURL string `json:"catalog-url"`
	// ModelPrefix 保留对象形式，类型和默认值交由统一配置解析验证。
	ModelPrefix json.RawMessage `json:"model-prefix"`
	// AllowHTTP 仅对当前认证生效，默认禁止 HTTP。
	AllowHTTP bool `json:"allow-http"`
	// SystemAccessToken 仅用于账号余额，不发送到推理端点。
	SystemAccessToken string `json:"system_access_token"`
	// UserID 按 JSON 原值交给统一整数解析，避免浮点精度损失。
	UserID json.RawMessage `json:"user_id"`
}

// addAuth 白名单构造并校验 JSON，通过宿主写盘和 upsert，不调用上游验证。
func (m *Manager) addAuth(ctx context.Context, body []byte) pluginapi.ManagementResponse {
	var req authAddRequest
	if len(body) > 64<<10 || json.Unmarshal(body, &req) != nil {
		return loginJSON(http.StatusBadRequest, "error", "认证提交字段格式无效")
	}
	label := strings.TrimSpace(req.Label)
	if len(label) > 256 || strings.ContainsAny(label, "\r\n") {
		return loginJSON(http.StatusBadRequest, "error", "名称过长或包含换行")
	}
	record := map[string]any{
		"type": ProviderID, "label": label, "email": label, "disabled": false,
		"base_url": strings.TrimSpace(req.BaseURL), "api_key": strings.TrimSpace(req.APIKey),
		"allow-http": req.AllowHTTP,
	}
	if req.UpstreamFormat != nil {
		record["upstream-format"] = *req.UpstreamFormat
	}
	if req.QuotaURL != "" {
		record["quota-url"] = req.QuotaURL
	}
	if req.CatalogURL != "" {
		record["catalog-url"] = req.CatalogURL
	}
	if len(req.ModelPrefix) > 0 {
		record["model-prefix"] = req.ModelPrefix
	}
	if req.SystemAccessToken != "" {
		record["system_access_token"] = req.SystemAccessToken
	}
	if len(req.UserID) > 0 {
		record["user_id"] = req.UserID
	}
	raw, err := json.Marshal(record)
	if err != nil {
		return loginJSON(400, "error", "认证 JSON 无效")
	}
	auth, err := resolveAuth(raw, nil, nil)
	if err != nil {
		return loginJSON(400, "error", err.Error())
	}
	// 仅保存规范化后的白名单数据，不保留浏览器传入的任意扩展属性。
	record["base_url"], record["api_key"] = auth.cfg.BaseURL, auth.key
	record["upstream-format"] = auth.cfg.UpstreamFormat
	record["model-prefix"] = map[string]any{"enabled": auth.cfg.ModelPrefix.Enabled, "value": auth.cfg.ModelPrefix.Value}
	if auth.systemToken != "" {
		record["system_access_token"], record["user_id"] = auth.systemToken, auth.userID
	}
	var random [24]byte
	if _, err := rand.Read(random[:]); err != nil {
		return loginJSON(500, "error", "无法创建安全随机文件名")
	}
	name := ProviderID + "-" + hex.EncodeToString(random[:]) + ".json"
	raw, _ = json.Marshal(record)
	if m.bridge == nil {
		return loginJSON(503, "error", "宿主保存入口不可用")
	}
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	if err := m.bridge.AuthSave(ctx, pluginapi.HostAuthSaveRequest{Name: name, JSON: raw}); err != nil {
		return loginJSON(502, "unknown", "保存结果未知，请先刷新认证列表确认，勿自动或重复提交")
	}
	return loginJSON(201, "saved", "已保存认证；未验证上游权限，请刷新列表查看")
}

// safeAuthFileName 只接受可原样交还宿主的单一 JSON 文件名，拒绝目录及跨平台歧义。
func safeAuthFileName(name string) bool {
	if name == "" || strings.TrimSpace(name) != name || !strings.HasSuffix(strings.ToLower(name), ".json") || strings.ContainsAny(name, "/\\:<>\"|?*") {
		return false
	}
	for _, char := range name {
		if char < 32 || char == 127 {
			return false
		}
	}
	stem := strings.ToUpper(strings.SplitN(name, ".", 2)[0])
	if stem == "" || stem == "CON" || stem == "PRN" || stem == "AUX" || stem == "NUL" || (len(stem) == 4 && (strings.HasPrefix(stem, "COM") || strings.HasPrefix(stem, "LPT")) && stem[3] >= '1' && stem[3] <= '9') {
		return false
	}
	return true
}

// authRecordMatchesProvider 不接受文件标识与宿主列表不同的认证，也拒绝冲突的双标识。
func authRecordMatchesProvider(values map[string]any) bool {
	matched := false
	for _, field := range []string{"type", "provider"} {
		value, exists := values[field]
		if !exists {
			continue
		}
		provider, ok := value.(string)
		if !ok || provider != ProviderID {
			return false
		}
		matched = true
	}
	return matched
}

// editAuth 每次重新读取宿主最新文件，只合并白名单字段，保留未提交的秘密和扩展。
func (m *Manager) editAuth(ctx context.Context, body []byte) pluginapi.ManagementResponse {
	var req map[string]any
	if len(body) > 64<<10 || decodeAuthRequest(body, &req) != nil || req == nil {
		return loginJSON(400, "error", "认证提交字段格式无效")
	}
	name, ok := req["name"].(string)
	if !ok || !safeAuthFileName(name) {
		return loginJSON(400, "error", "认证文件名无效，请刷新列表后重新选择")
	}
	clearBalance := false
	if value, exists := req["clear_balance_credentials"]; exists {
		clearBalance, ok = value.(bool)
		if !ok {
			return loginJSON(400, "error", "清除账号余额凭据标志必须为布尔值")
		}
	}
	// 明确区分控制字段、配置字段与不可写字段，避免客户端修改宿主身份或状态。
	for field, value := range req {
		switch field {
		case "name", "clear_balance_credentials":
		case "label", "base_url", "api_key", "upstream-format", "quota-url", "catalog-url", "system_access_token":
			if _, ok := value.(string); !ok {
				return loginJSON(400, "error", "文本配置字段类型无效")
			}
		case "allow-http":
			if _, ok := value.(bool); !ok {
				return loginJSON(400, "error", "allow-http 必须为布尔值")
			}
		case "model-prefix":
			if _, ok := value.(map[string]any); !ok {
				return loginJSON(400, "error", "model-prefix 必须为对象")
			}
		case "user_id":
			if text, ok := value.(string); ok && strings.TrimSpace(text) == "" {
				continue
			}
			if _, err := normalizeUserID(value); err != nil || value == nil {
				return loginJSON(400, "error", "用户 ID 必须为正十进制整数")
			}
		default:
			return loginJSON(400, "error", "包含不可编辑字段")
		}
	}
	token, _ := req["system_access_token"].(string)
	user, userSubmitted := req["user_id"]
	userReplaced := userSubmitted
	if text, ok := user.(string); ok && strings.TrimSpace(text) == "" {
		userReplaced = false
	}
	if clearBalance && (strings.TrimSpace(token) != "" || userReplaced) {
		return loginJSON(400, "error", "清除账号余额凭据不能与替换系统令牌或用户 ID 同时提交")
	}
	if m.bridge == nil {
		return loginJSON(503, "error", "宿主认证入口不可用")
	}
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	entries, err := m.bridge.AuthList(ctx)
	if err != nil {
		return loginJSON(502, "error", "读取认证列表失败，未提交保存")
	}
	index, matches, disabled := "", 0, false
	for _, entry := range entries {
		if isNewapiAuthFile(entry) && entry.Name == name && safeAuthFileName(entry.Name) {
			matches++
			index = entry.AuthIndex
			disabled = entry.Disabled || strings.EqualFold(entry.Status, "disabled")
		}
	}
	if matches == 0 {
		return loginJSON(404, "error", "认证已不存在或不属于当前插件，请刷新列表")
	}
	if matches != 1 || index == "" {
		return loginJSON(409, "error", "认证目标存在歧义，拒绝保存")
	}
	if disabled {
		return loginJSON(409, "error", "禁用认证不能直接编辑，请先在 CPA 启用后再编辑")
	}
	file, err := m.bridge.AuthGet(ctx, index)
	if err != nil {
		return loginJSON(502, "error", "读取认证文件失败，未提交保存")
	}
	if file.Name != name || (file.AuthIndex != "" && file.AuthIndex != index) {
		return loginJSON(409, "error", "认证文件身份不一致，拒绝保存")
	}
	var record map[string]any
	if decodeAuthRequest(file.JSON, &record) != nil || record == nil || !authRecordMatchesProvider(record) {
		return loginJSON(409, "error", "认证文件无效或 provider 不匹配，拒绝保存")
	}
	// SDK 保存只从 type 构造运行期 provider；不能为兼容文件擅自改写身份标识。
	if record["type"] != ProviderID {
		return loginJSON(409, "error", "当前 SDK 保存要求 type 为 newapi，请先通过 CPA 文件管理补齐 type 后再编辑")
	}
	if value, exists := record["disabled"]; exists {
		value, ok := value.(bool)
		if !ok || value {
			return loginJSON(409, "error", "认证禁用或禁用状态无效，请先在 CPA 确认启用后再编辑")
		}
	}
	// 空秘密始终保留最新文件中的值；普通配置允许显式空值以恢复默认行为。
	for field, value := range req {
		switch field {
		case "name", "clear_balance_credentials":
			continue
		case "api_key", "system_access_token":
			if strings.TrimSpace(value.(string)) == "" {
				continue
			}
		case "user_id":
			if !userReplaced {
				continue
			}
		}
		record[field] = value
	}
	if clearBalance {
		delete(record, "system_access_token")
		delete(record, "user_id")
	}
	if label, ok := record["label"].(string); ok {
		label = strings.TrimSpace(label)
		if len(label) > 256 || strings.ContainsAny(label, "\r\n") {
			return loginJSON(400, "error", "名称过长或包含换行")
		}
		// email 是获准随名称同步的显示别名，不接受客户端单独编辑，也不代表真实邮箱。
		record["label"], record["email"] = label, label
	}
	raw, err := json.Marshal(record)
	if err != nil {
		return loginJSON(400, "error", "认证 JSON 无效")
	}
	if _, err := resolveAuth(raw, nil, nil); err != nil {
		return loginJSON(400, "error", err.Error())
	}
	// AuthSave 会直接 upsert 为 active，因此上面必须拒绝禁用项，且始终原名覆盖。
	if err := m.bridge.AuthSave(ctx, pluginapi.HostAuthSaveRequest{Name: name, JSON: raw}); err != nil {
		return loginJSON(502, "unknown", "保存结果未知，请先刷新认证列表确认，勿自动或重复提交")
	}
	return loginJSON(200, "saved", "已保存修改；未验证上游权限，请刷新列表查看")
}

// authListItem 严格限定可返回页面的字段，不含密钥、完整 JSON、宿主索引或物理路径。
type authListItem struct {
	// Name 是不含目录的文件名。
	Name string `json:"name"`
	// Label 是显示名称。
	Label string `json:"label"`
	// Status 是宿主状态或读取失败提示，不包含宿主错误详情。
	Status string `json:"status"`
	// Disabled 保留宿主禁用状态。
	Disabled bool `json:"disabled"`
	// BaseURL 是经过校验的认证上游地址。
	BaseURL string `json:"base_url"`
	// UpstreamFormat 是当前认证协议。
	UpstreamFormat string `json:"upstream-format"`
	// PrefixEnabled 表示是否启用插件模型前缀。
	PrefixEnabled bool `json:"prefix_enabled"`
	// PrefixValue 是插件前缀值。
	PrefixValue string `json:"prefix_value"`
	// QuotaURL 是白名单中的非敏感额度地址，配置无效时也保留可修复值。
	QuotaURL string `json:"quota-url"`
	// CatalogURL 是当前认证的目录地址。
	CatalogURL string `json:"catalog-url"`
	// AllowHTTP 是当前认证的明文传输开关。
	AllowHTTP bool `json:"allow-http"`
	// HasAPIKey 只表示推理密钥是否存在。
	HasAPIKey bool `json:"has_api_key"`
	// HasSystemToken 只表示系统令牌是否存在。
	HasSystemToken bool `json:"has_system_token"`
	// HasUserID 只表示账号标识是否存在。
	HasUserID bool `json:"has_user_id"`
}

// listAuths 逐个读取真实宿主文件，包含禁用认证并排除内存运行期认证。
func (m *Manager) listAuths(ctx context.Context) pluginapi.ManagementResponse {
	if m.bridge == nil {
		return loginJSON(503, "error", "宿主认证入口不可用")
	}
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	entries, err := m.bridge.AuthList(ctx)
	if err != nil {
		return loginJSON(502, "error", "读取认证列表失败")
	}
	items := make([]authListItem, 0)
	for _, entry := range entries {
		if !isNewapiAuthFile(entry) || !safeAuthFileName(entry.Name) {
			continue
		}
		item := authListItem{Name: entry.Name, Label: entry.Label,
			Status: "可用", Disabled: entry.Disabled || strings.EqualFold(entry.Status, "disabled")}
		if item.Disabled {
			item.Status = "已禁用"
		} else if entry.Unavailable {
			item.Status = "暂不可用"
		}
		file, err := m.bridge.AuthGet(ctx, entry.AuthIndex)
		if err != nil || file.Name != entry.Name || (file.AuthIndex != "" && file.AuthIndex != entry.AuthIndex) {
			item.Status = "文件不可读取"
			items = append(items, item)
			continue
		}
		var values map[string]any
		if decodeAuthRequest(file.JSON, &values) != nil || values == nil {
			item.Status = "认证 JSON 无效"
			items = append(items, item)
			continue
		}
		if !authRecordMatchesProvider(values) {
			continue
		}
		// 先提取类型安全的非敏感原值，统一校验失败时仍可在页面修复。
		item.BaseURL, _ = values["base_url"].(string)
		item.UpstreamFormat, _ = values["upstream-format"].(string)
		item.QuotaURL, _ = values["quota-url"].(string)
		item.CatalogURL, _ = values["catalog-url"].(string)
		item.AllowHTTP, _ = values["allow-http"].(bool)
		item.PrefixEnabled, item.PrefixValue = true, "newapi"
		if prefix, ok := values["model-prefix"].(map[string]any); ok {
			if enabled, ok := prefix["enabled"].(bool); ok {
				item.PrefixEnabled = enabled
			}
			if value, ok := prefix["value"].(string); ok {
				item.PrefixValue = value
			}
		}
		if disabled, ok := values["disabled"].(bool); ok && disabled {
			item.Disabled, item.Status = true, "已禁用"
		}
		if label, ok := values["label"].(string); ok {
			item.Label = label
		}
		key, _ := values["api_key"].(string)
		token, _ := values["system_access_token"].(string)
		item.HasAPIKey, item.HasSystemToken = strings.TrimSpace(key) != "", strings.TrimSpace(token) != ""
		userID, _ := normalizeUserID(values["user_id"])
		item.HasUserID = userID != ""
		auth, err := resolveAuth(file.JSON, nil, nil)
		if err != nil {
			item.Status = "配置无效，请检查必填字段及迁移 base_url"
		} else {
			item.BaseURL, item.UpstreamFormat = auth.cfg.BaseURL, auth.cfg.UpstreamFormat
			item.PrefixEnabled, item.PrefixValue = auth.cfg.ModelPrefix.Enabled, auth.cfg.ModelPrefix.Value
		}
		items = append(items, item)
	}
	body, _ := json.Marshal(map[string]any{"items": items})
	return pluginapi.ManagementResponse{StatusCode: 200, Headers: managementJSONHeaders(), Body: body}
}
