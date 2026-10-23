package plugin

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"strconv"
	"strings"

	"github.com/router-for-me/CLIProxyAPI/v8/sdk/pluginapi"

	"newapi-provider/internal/config"
)

// authProvider 只负责静态 API 密钥文件的解析与无副作用回传。
type authProvider struct{}

// Identifier 返回宿主使用的唯一提供方标识。
func (authProvider) Identifier() string { return ProviderID }

// ParseAuth 解析推理密钥及可选账号余额凭据，并保留宿主管理的元数据。
func (authProvider) ParseAuth(_ context.Context, req pluginapi.AuthParseRequest) (pluginapi.AuthParseResponse, error) {
	debugTrace("auth parse request provider=%s file=%s raw_bytes=%d", req.Provider, req.FileName, len(req.RawJSON))
	var raw struct {
		// Type 认证文件声明的提供方类型。
		Type string `json:"type"`
		// Provider 认证文件声明的提供方标识。
		Provider string `json:"provider"`
		// ID 仅保留文件内容，不用作宿主运行期身份。
		ID string `json:"id"`
		// Label 宿主管理的显示名称。
		Label string `json:"label"`
		// APIKey 认证文件提供的静态 API 密钥。
		APIKey string `json:"api_key"`
		// Disabled 认证文件是否被禁用。
		Disabled bool `json:"disabled"`
	}
	if err := json.Unmarshal(req.RawJSON, &raw); err != nil {
		if req.Provider == ProviderID {
			return pluginapi.AuthParseResponse{}, fmt.Errorf("newapi auth record has invalid JSON")
		}
		return pluginapi.AuthParseResponse{}, nil
	}
	if (req.Provider != "" && req.Provider != ProviderID) || (raw.Type != ProviderID && raw.Provider != ProviderID) {
		return pluginapi.AuthParseResponse{Handled: false}, nil
	}
	selected, err := resolveAuth(req.RawJSON, nil, nil)
	if err != nil {
		return pluginapi.AuthParseResponse{}, err
	}
	// JSON id 不充当宿主运行期身份，且不设置 AuthData.Prefix。
	prefix, _ := json.Marshal(map[string]any{"enabled": selected.cfg.ModelPrefix.Enabled, "value": selected.cfg.ModelPrefix.Value})
	// base_url 仅保留在认证文件和 Metadata；Attributes 中的同名键会触发宿主原生兼容执行器。
	attributes := map[string]string{
		"api_key": selected.key, "catalog-url": selected.cfg.CatalogURL,
		"quota-url": selected.cfg.QuotaURL, "upstream-format": selected.cfg.UpstreamFormat,
		"allow-http": strconv.FormatBool(selected.cfg.AllowHTTP), "model-prefix": string(prefix),
	}
	metadata := selected.values
	// email 仅作为宿主认证列表的显示别名，不代表上游真实邮箱。
	syncAuthDisplayAlias(metadata)
	if selected.systemToken != "" {
		metadata["system_access_token"], metadata["user_id"] = selected.systemToken, selected.userID
		attributes["system_access_token"], attributes["user_id"] = selected.systemToken, selected.userID
	}
	auth := pluginapi.AuthData{
		Provider: ProviderID, ID: req.FileName, FileName: req.FileName, Label: raw.Label,
		Disabled: raw.Disabled, StorageJSON: req.RawJSON, Metadata: metadata,
		Attributes: attributes,
	}
	return pluginapi.AuthParseResponse{Handled: true, Auth: auth}, nil
}

// selectedSystemAuth 仅从选中认证读取成对的账号凭据，不向全局配置或其他认证回退。
func selectedSystemAuth(attributes map[string]string, metadata map[string]any) (string, string, error) {
	token, tokenPresent := attributes["system_access_token"]
	if !tokenPresent {
		if value, present := metadata["system_access_token"]; present && value != nil {
			var ok bool
			token, ok = value.(string)
			if !ok {
				return "", "", fmt.Errorf("系统访问令牌必须为字符串")
			}
		}
	}
	token = strings.TrimSpace(token)
	var value any
	if id, present := attributes["user_id"]; present {
		value = id
	} else {
		value = metadata["user_id"]
	}
	userID, err := normalizeUserID(value)
	if err != nil {
		return "", "", err
	}
	if (token == "") != (userID == "") {
		return "", "", fmt.Errorf("系统访问令牌与用户 ID 必须同时提供；不会回退 API key 查询账号余额")
	}
	if strings.ContainsAny(token, "\r\n") {
		return "", "", fmt.Errorf("系统访问令牌格式无效")
	}
	return token, userID, nil
}

// normalizeUserID 接受正十进制 int64 或字符串；浮点元数据仅接受可精确表达的安全整数。
func normalizeUserID(value any) (string, error) {
	var text string
	switch id := value.(type) {
	case nil:
		return "", nil
	case string:
		text = strings.TrimSpace(id)
	case json.Number:
		text = id.String()
	case float64:
		if math.IsNaN(id) || math.IsInf(id, 0) || id <= 0 || id > 9007199254740991 || math.Trunc(id) != id {
			return "", fmt.Errorf("用户 ID 必须为可精确表示的正十进制整数；大整数请使用字符串")
		}
		text = strconv.FormatFloat(id, 'f', 0, 64)
	default:
		return "", fmt.Errorf("用户 ID 必须为正十进制整数或字符串")
	}
	if text == "" {
		return "", nil
	}
	for _, digit := range text {
		if digit < '0' || digit > '9' {
			return "", fmt.Errorf("用户 ID 必须为正十进制整数")
		}
	}
	id, err := strconv.ParseInt(text, 10, 64)
	if err != nil || id <= 0 {
		return "", fmt.Errorf("用户 ID 必须为 int64 范围内的正十进制整数")
	}
	return strconv.FormatInt(id, 10), nil
}

// RefreshAuth 保留静态凭据，移除会触发宿主原生执行器的运行期地址标记，不登录或保存文件。
func (authProvider) RefreshAuth(_ context.Context, req pluginapi.AuthRefreshRequest) (pluginapi.AuthRefreshResponse, error) {
	if req.AuthProvider != ProviderID {
		return pluginapi.AuthRefreshResponse{}, fmt.Errorf("认证提供方不匹配")
	}
	auth, err := resolveAuth(req.StorageJSON, req.Metadata, req.Attributes)
	if err != nil {
		return pluginapi.AuthRefreshResponse{}, err
	}
	if auth.systemToken != "" {
		auth.values["system_access_token"], auth.values["user_id"] = auth.systemToken, auth.userID
	}
	// 复制而不修改请求映射；旧运行期地址迁回 Metadata，避免刷新时重新绑定原生执行器。
	auth.values["base_url"] = auth.cfg.BaseURL
	attributes := make(map[string]string, len(req.Attributes))
	for key, value := range req.Attributes {
		if key != "base_url" {
			attributes[key] = value
		}
	}
	label := syncAuthDisplayAlias(auth.values)
	return pluginapi.AuthRefreshResponse{Auth: pluginapi.AuthData{Provider: req.AuthProvider, ID: req.AuthID, Label: label, StorageJSON: req.StorageJSON, Metadata: auth.values, Attributes: attributes}}, nil
}

// syncAuthDisplayAlias 用已有非空 label 同步宿主显示别名；缺少名称时不伪造邮箱。
func syncAuthDisplayAlias(metadata map[string]any) string {
	label, _ := metadata["label"].(string)
	if label != "" {
		metadata["email"] = label
	}
	return label
}

// mapKeys 提取字段名称用于诊断，避免记录认证字段值。
func mapKeys[T any](values map[string]T) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	return keys
}

// resolvedAuth 保存当前认证解析后的配置与凭据，不从全局或其他认证补值。
type resolvedAuth struct {
	// cfg 是当前认证站点配置。
	cfg config.Config
	// key 只用于目录、令牌额度和推理。
	key string
	// systemToken 只用于账号余额。
	systemToken string
	// userID 是规范化账号标识。
	userID string
	// values 保留宿主管理元数据。
	values map[string]any
}

// resolveAuth 按 storage、metadata、attributes 顺序合并，兼容保存后只有 metadata 的认证。
func resolveAuth(storage []byte, metadata map[string]any, attributes map[string]string) (resolvedAuth, error) {
	values := make(map[string]any)
	if len(storage) > 0 {
		if decodeAuthRequest(storage, &values) != nil || values == nil {
			return resolvedAuth{}, fmt.Errorf("认证 JSON 无效")
		}
	}
	for key, value := range metadata {
		values[key] = value
	}
	for key, value := range attributes {
		switch key {
		case "base_url", "api_key", "catalog-url", "quota-url", "upstream-format", "system_access_token", "user_id":
			values[key] = value
		case "model-prefix", "allow-http":
			var decoded any
			if decodeAuthRequest([]byte(value), &decoded) != nil {
				return resolvedAuth{}, fmt.Errorf("认证属性 %s 格式无效", key)
			}
			values[key] = decoded
		}
	}
	cfg, err := config.FromAuth(values)
	if err != nil {
		return resolvedAuth{}, err
	}
	key, ok := values["api_key"].(string)
	key = strings.TrimSpace(key)
	if !ok || key == "" || strings.ContainsAny(key, "\r\n") {
		return resolvedAuth{}, fmt.Errorf("认证 api_key 必须为非空且有效的字符串")
	}
	token, userID, err := selectedSystemAuth(nil, values)
	if err != nil {
		return resolvedAuth{}, err
	}
	return resolvedAuth{cfg: cfg, key: key, systemToken: token, userID: userID, values: values}, nil
}

// isNewapiAuthFile 包含禁用文件，排除其他提供方、运行期记录和缺少索引的条目。
func isNewapiAuthFile(entry pluginapi.HostAuthFileEntry) bool {
	provider := entry.Provider
	if provider == "" {
		provider = entry.Type
	}
	return provider == ProviderID && entry.Source == "file" && !entry.RuntimeOnly && entry.AuthIndex != ""
}

// decodeAuthRequest 保留宿主认证元数据中的十进制整数文本。
func decodeAuthRequest(raw []byte, target any) error {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	var extra any
	if decoder.Decode(&extra) != io.EOF {
		return fmt.Errorf("JSON 必须仅包含一个完整值")
	}
	return nil
}
