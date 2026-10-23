package plugin

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v8/sdk/pluginapi"
)

// quotaCallbackContextKey 在目录、执行、管理和额度调用中透传宿主临时 HTTP 回调标识。
type quotaCallbackContextKey struct{}

// quotaFetchRequest 保留 SDK 认证上下文及 ABI 额外传入的回调标识。
type quotaFetchRequest struct {
	pluginapi.QuotaFetchRequest
	// HostCallbackID 绑定本次查询的宿主 HTTP 客户端，不用于选择密钥。
	HostCallbackID string `json:"host_callback_id"`
}

// tokenUsageEnvelope 仅解码已知成功标记及令牌额度，不传播上游消息。
type tokenUsageEnvelope struct {
	// Code 是 New API 令牌查询的布尔成功标记。
	Code *bool `json:"code"`
	// Success 兼容另一种成功信封；任一显式 false 都表示失败。
	Success *bool `json:"success"`
	// Data 保存本次选中令牌的原始额度信息。
	Data *tokenUsageData `json:"data"`
}

// tokenUsageData 使用指针区分缺失字段与真实零额度。
type tokenUsageData struct {
	// TotalGranted 是原始总额度，不是金额或模型 token 数。
	TotalGranted *float64 `json:"total_granted"`
	// TotalUsed 是原始已用额度。
	TotalUsed *float64 `json:"total_used"`
	// TotalAvailable 是原始剩余额度，零表示确实耗尽。
	TotalAvailable *float64 `json:"total_available"`
	// UnlimitedQuota 指明是否不受有限总额度约束。
	UnlimitedQuota *bool `json:"unlimited_quota"`
	// ExpiresAt 是令牌到期的 Unix 秒，零表示无到期时间。
	ExpiresAt *int64 `json:"expires_at"`
}

// accountUsageEnvelope 仅解析账号接口的成功标记与必要额度字段。
type accountUsageEnvelope struct {
	// Success 必须显式为 true，账号接口不接受令牌接口的 code 标记。
	Success *bool `json:"success"`
	// Data 保存账号余额与使用统计。
	Data *accountUsageData `json:"data"`
}

// accountUsageData 不推算账号总额、重置时间或无限状态。
type accountUsageData struct {
	// Quota 是账号剩余原始额度，允许负余额。
	Quota *float64 `json:"quota"`
	// UsedQuota 是账号已用原始额度。
	UsedQuota *float64 `json:"used_quota"`
	// RequestCount 是账号累计请求次数。
	RequestCount *float64 `json:"request_count"`
}

// quotaStatusEntry 保存公开换算参数；零值参数表示短期失败或正在查询。
type quotaStatusEntry struct {
	// quotaPerUnit 是每美元对应的原始额度，零表示不可用。
	quotaPerUnit float64
	// expiresAt 控制成功缓存和失败退避的有效期。
	expiresAt time.Time
	// done 合并同 URL 的在途查询；完成后置空，等待者沿用各自回调上下文期限。
	done chan struct{}
}

// quotaStatusEnvelope 只接受公开状态明确成功且有效的换算参数。
type quotaStatusEnvelope struct {
	// Success 必须显式为 true。
	Success bool `json:"success"`
	// Data 仅包含美元换算需要的字段，不使用汇率字段。
	Data struct {
		// QuotaPerUnit 是上游声明的每美元原始额度。
		QuotaPerUnit float64 `json:"quota_per_unit"`
	} `json:"data"`
}

// handleQuotaFetch 严格使用宿主选中的认证，按成对账号凭据决定额度模式。
func (m *Manager) handleQuotaFetch(request []byte) ([]byte, error) {
	var req quotaFetchRequest
	decoder := json.NewDecoder(bytes.NewReader(request))
	decoder.UseNumber()
	if decoder.Decode(&req) != nil || req.Provider != ProviderID {
		return ErrEnvelope("invalid_request", "额度查询请求无效或提供方不匹配"), nil
	}
	auth, err := resolveAuth(req.StorageJSON, req.Metadata, req.Attributes)
	if err != nil {
		return ErrEnvelope("auth_failure", err.Error()), nil
	}
	key, systemToken, userID, cfg := auth.key, auth.systemToken, auth.userID, auth.cfg
	m.mu.RLock()
	generation := m.quotaGeneration
	m.mu.RUnlock()
	if m.bridge == nil || cfg.BaseURL == "" || cfg.RequestTimeout <= 0 || cfg.MaxResponseBytes <= 0 {
		return ErrEnvelope("quota_failure", "额度查询尚未就绪"), nil
	}
	quotaURL := cfg.QuotaURL
	if quotaURL == "" {
		u, parseErr := url.Parse(cfg.BaseURL)
		if parseErr != nil {
			return ErrEnvelope("quota_failure", "额度查询地址无效"), nil
		}
		endpoint := "/api/usage/token/"
		if systemToken != "" {
			endpoint = "/api/user/self"
		}
		u.Path = strings.TrimSuffix(strings.TrimRight(u.Path, "/"), "/v1") + endpoint
		u.RawPath = ""
		quotaURL = u.String()
	}
	timeout := cfg.RequestTimeout
	if timeout > 30*time.Second {
		timeout = 30 * time.Second
	}
	// 每个接口拥有独立期限，双接口并行且互不传播上游失败。
	ctx := context.WithValue(context.Background(), quotaCallbackContextKey{}, req.HostCallbackID)
	limit := cfg.MaxResponseBytes
	if limit > 1<<20 {
		limit = 1 << 20
	}
	tokenURL, accountURL := "", ""
	switch quotaEndpoint(quotaURL) {
	case "/api/usage/token":
		tokenURL = quotaURL
		if systemToken != "" {
			accountURL = replaceQuotaEndpoint(quotaURL, "/api/user/self")
		}
	case "/api/user/self":
		// 即使未配置系统令牌，也只将 API key 发往同站点令牌接口。
		tokenURL = replaceQuotaEndpoint(quotaURL, "/api/usage/token/")
		if systemToken != "" {
			accountURL = quotaURL
		}
	default:
		// 自定义路径只用于明确选定的凭据用途，不猜其他路径或跨域站点。
		if systemToken != "" {
			accountURL = quotaURL
		} else {
			tokenURL = quotaURL
		}
	}
	// 有界缓冲确保两次查询均能完成；按固定顺序合并，金额不相加。
	results := make([]chan pluginapi.QuotaFetchResponse, 0, 2)
	for _, query := range []struct {
		// address 是实际查询地址，空值表示该接口不可安全推导。
		address string
		// account 标记账号接口，避免混用系统令牌与推理密钥。
		account bool
	}{{accountURL, true}, {tokenURL, false}} {
		if query.address == "" {
			continue
		}
		ch := make(chan pluginapi.QuotaFetchResponse, 1)
		results = append(results, ch)
		go func(address string, account bool) {
			credential := key
			if account {
				credential = systemToken
			}
			ch <- m.fetchQuotaUsage(ctx, address, credential, userID, account, timeout, limit, generation)
		}(query.address, query.account)
	}
	result := pluginapi.QuotaFetchResponse{}
	for _, ch := range results {
		part := <-ch
		result.Summary = append(result.Summary, part.Summary...)
		result.Groups = append(result.Groups, part.Groups...)
	}
	return okEnvelope(result), nil
}

// quotaEndpoint 只识别已知接口尾路径，不对未知路径进行探测。
func quotaEndpoint(address string) string {
	u, err := url.Parse(address)
	if err != nil {
		return ""
	}
	path := strings.TrimSuffix(u.Path, "/")
	for _, endpoint := range []string{"/api/user/self", "/api/usage/token"} {
		if strings.HasSuffix(path, endpoint) {
			return endpoint
		}
	}
	return ""
}

// replaceQuotaEndpoint 从实际额度地址替换已知尾路径，保留同源部署前缀。
func replaceQuotaEndpoint(address, endpoint string) string {
	known := quotaEndpoint(address)
	if known == "" {
		return ""
	}
	u, err := url.Parse(address)
	if err != nil {
		return ""
	}
	u.Path = strings.TrimSuffix(strings.TrimSuffix(u.Path, "/"), known) + endpoint
	u.RawPath = ""
	return u.String()
}

// fetchQuotaUsage 查询单个接口，失败时不产生伪造的零额度或影响另一接口。
func (m *Manager) fetchQuotaUsage(parent context.Context, address, credential, userID string, account bool, timeout time.Duration, limit int64, generation uint64) pluginapi.QuotaFetchResponse {
	ctx, cancel := context.WithTimeout(parent, timeout)
	defer cancel()
	headers := http.Header{"Authorization": []string{"Bearer " + credential}, "Accept": []string{"application/json"}}
	if account {
		headers.Set("New-Api-User", userID)
	}
	resp, err := m.bridge.Do(ctx, pluginapi.HTTPRequest{Method: http.MethodGet, URL: address, Headers: headers})
	if err != nil || resp.StatusCode < 200 || resp.StatusCode >= 300 || int64(len(resp.Body)) > limit {
		return pluginapi.QuotaFetchResponse{}
	}
	var result pluginapi.QuotaFetchResponse
	if account {
		var envelope accountUsageEnvelope
		if json.Unmarshal(resp.Body, &envelope) != nil || envelope.Success == nil || !*envelope.Success || envelope.Data == nil {
			return result
		}
		data := envelope.Data
		if !validQuotaNumber(data.Quota) || !validQuotaNumber(data.UsedQuota) || *data.UsedQuota < 0 ||
			!validQuotaNumber(data.RequestCount) || *data.RequestCount < 0 || math.Trunc(*data.RequestCount) != *data.RequestCount {
			return result
		}
		result.Summary = []pluginapi.QuotaMetric{
			{Key: "account_quota", Label: "账号剩余原始额度", Value: *data.Quota, Unit: "quota", Format: "number"},
			{Key: "account_used_quota", Label: "账号已用原始额度", Value: *data.UsedQuota, Unit: "quota", Format: "number"},
			{Key: "account_request_count", Label: "账号请求次数", Value: *data.RequestCount, Unit: "requests", Format: "number"},
		}
	} else {
		var envelope tokenUsageEnvelope
		if json.Unmarshal(resp.Body, &envelope) != nil || (envelope.Code == nil && envelope.Success == nil) ||
			(envelope.Code != nil && !*envelope.Code) || (envelope.Success != nil && !*envelope.Success) || envelope.Data == nil {
			return result
		}
		result, err = normalizeTokenUsage(envelope.Data)
		if err != nil {
			return pluginapi.QuotaFetchResponse{}
		}
	}
	// 状态不可用时保留明确标注的原始 quota，不套用常量或汇率。
	if perUnit := m.quotaPerUnit(ctx, address, limit, generation); perUnit > 0 {
		for i := range result.Summary {
			metric := &result.Summary[i]
			if metric.Unit != "quota" || math.IsInf(metric.Value/perUnit, 0) || math.IsNaN(metric.Value/perUnit) {
				continue
			}
			metric.Value /= perUnit
			metric.Unit, metric.Format, metric.Currency = "", "currency", "USD"
			metric.Label = strings.ReplaceAll(metric.Label, "原始额度", "金额")
		}
	}
	return result
}

// validQuotaNumber 排除缺失和非有限数值，避免无效金额进入 ABI 响应。
func validQuotaNumber(value *float64) bool {
	return value != nil && !math.IsNaN(*value) && !math.IsInf(*value, 0)
}

// quotaPerUnit 通过同一宿主桥与回调上下文查询公开换算参数，并按生命周期缓存。
func (m *Manager) quotaPerUnit(parent context.Context, address string, limit int64, generation uint64) float64 {
	statusURL := replaceQuotaEndpoint(address, "/api/status")
	if statusURL == "" {
		return 0
	}
	ctx, cancel := context.WithTimeout(parent, 3*time.Second)
	defer cancel()
	m.mu.Lock()
	if generation != m.quotaGeneration {
		m.mu.Unlock()
		return 0
	}
	if entry, ok := m.quotaStatusCache[statusURL]; ok && (entry.done != nil || time.Now().Before(entry.expiresAt)) {
		m.mu.Unlock()
		if entry.done == nil {
			return entry.quotaPerUnit
		}
		select {
		case <-ctx.Done():
			return 0
		case <-entry.done:
			m.mu.RLock()
			defer m.mu.RUnlock()
			if generation == m.quotaGeneration {
				return m.quotaStatusCache[statusURL].quotaPerUnit
			}
			return 0
		}
	}
	if m.quotaStatusCache == nil {
		m.quotaStatusCache = make(map[string]quotaStatusEntry)
	}
	// 只清理已结束的过期缓存，不移除等待者仍依赖的在途条目。
	for address, entry := range m.quotaStatusCache {
		if entry.done == nil && time.Now().After(entry.expiresAt) {
			delete(m.quotaStatusCache, address)
		}
	}
	if len(m.quotaStatusCache) >= 256 {
		m.mu.Unlock()
		return 0
	}
	done := make(chan struct{})
	m.quotaStatusCache[statusURL] = quotaStatusEntry{done: done}
	m.mu.Unlock()
	// 公开状态只发送 Accept，不携带任何账号、用户 ID 或推理认证头。
	resp, err := m.bridge.Do(ctx, pluginapi.HTTPRequest{
		Method: http.MethodGet, URL: statusURL, Headers: http.Header{"Accept": []string{"application/json"}},
	})
	perUnit := float64(0)
	if limit > 64<<10 {
		limit = 64 << 10
	}
	if err == nil && resp.StatusCode >= 200 && resp.StatusCode < 300 && int64(len(resp.Body)) <= limit {
		var envelope quotaStatusEnvelope
		if json.Unmarshal(resp.Body, &envelope) == nil && envelope.Success &&
			envelope.Data.QuotaPerUnit > 0 && !math.IsInf(envelope.Data.QuotaPerUnit, 0) && !math.IsNaN(envelope.Data.QuotaPerUnit) {
			perUnit = envelope.Data.QuotaPerUnit
		}
	}
	ttl := 30 * time.Second
	if perUnit > 0 {
		ttl = 15 * time.Minute
	}
	m.mu.Lock()
	if generation == m.quotaGeneration {
		m.quotaStatusCache[statusURL] = quotaStatusEntry{quotaPerUnit: perUnit, expiresAt: time.Now().Add(ttl)}
	} else {
		perUnit = 0
	}
	close(done)
	m.mu.Unlock()
	return perUnit
}

// normalizeTokenUsage 将原始额度映射为宿主数值摘要，不虚构金额、套餐或重置时间。
func normalizeTokenUsage(data *tokenUsageData) (pluginapi.QuotaFetchResponse, error) {
	if data == nil || data.UnlimitedQuota == nil || !validQuotaNumber(data.TotalUsed) || *data.TotalUsed < 0 ||
		data.ExpiresAt == nil || *data.ExpiresAt < 0 {
		return pluginapi.QuotaFetchResponse{}, fmt.Errorf("invalid token usage")
	}
	description := "令牌无到期时间；不支持重置"
	if *data.ExpiresAt > 0 {
		description = "令牌到期时间：" + time.Unix(*data.ExpiresAt, 0).UTC().Format(time.RFC3339) + "；不是额度重置时间"
	}
	result := pluginapi.QuotaFetchResponse{
		Summary: []pluginapi.QuotaMetric{
			{Key: "total_used", Label: "令牌已用原始额度", Value: *data.TotalUsed, Unit: "quota", Format: "number"},
		},
	}
	if *data.UnlimitedQuota {
		// SDK 无无限字段，以明确文案和布尔数值标记表达，不创建虚假的百分比桶。
		result.Summary = append(result.Summary, pluginapi.QuotaMetric{
			Key: "unlimited_quota", Label: "无限额度（1 表示启用）", Value: 1, Format: "number",
		})
		result.Groups = []pluginapi.QuotaGroup{{DisplayName: "无限额度；" + description}}
		return result, nil
	}
	if !validQuotaNumber(data.TotalGranted) || !validQuotaNumber(data.TotalAvailable) || *data.TotalGranted < 0 {
		return pluginapi.QuotaFetchResponse{}, fmt.Errorf("missing finite quota")
	}
	result.Summary = append(result.Summary,
		pluginapi.QuotaMetric{Key: "total_available", Label: "令牌剩余原始额度", Value: *data.TotalAvailable, Unit: "quota", Format: "number"},
		pluginapi.QuotaMetric{Key: "total_granted", Label: "令牌总原始额度", Value: *data.TotalGranted, Unit: "quota", Format: "number"},
	)
	group := pluginapi.QuotaGroup{DisplayName: "API 令牌额度；" + description}
	if *data.TotalGranted > 0 {
		fraction := *data.TotalAvailable / *data.TotalGranted
		if fraction < 0 {
			fraction = 0
		} else if fraction > 1 {
			fraction = 1
		}
		group.Buckets = []pluginapi.QuotaBucket{{
			Window: "令牌总额度", RemainingFraction: fraction, Description: description,
		}}
	}
	result.Groups = []pluginapi.QuotaGroup{group}
	return result, nil
}
