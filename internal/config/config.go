// Package config 加载并校验 newapi-provider 的配置，统一管理上游格式与请求策略。
package config

import (
	"encoding/json"
	"fmt"
	"net/url"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

const (
	// DefaultModelPrefix 是面向客户端的默认模型前缀。
	DefaultModelPrefix = "newapi"
	// DefaultUpstreamFormat 是每认证缺省采用的上游格式。
	DefaultUpstreamFormat = "openai"
	// DefaultRefreshInterval 是目录的默认刷新间隔。
	DefaultRefreshInterval = 15 * time.Minute
	// DefaultRequestTimeout 是单次上游请求的默认超时。
	DefaultRequestTimeout = 5 * time.Minute
	// DefaultMaxResponseBytes 是默认响应大小上限，单位为字节。
	DefaultMaxResponseBytes = int64(67108864)
)

// ModelPrefix 控制客户端模型 ID 的提供商前缀。
type ModelPrefix struct {
	// Enabled 决定是否为上游模型 ID 添加前缀。
	Enabled bool
	// Value 保存不含斜杠的提供商标识。
	Value string
}

// Catalog 保存目录刷新与不可用时的快照策略。
type Catalog struct {
	// RefreshInterval 指定目录刷新周期，最短为一分钟。
	RefreshInterval time.Duration
	// StaleWhileUnavailable 决定刷新失败时是否继续提供最近成功的快照。
	StaleWhileUnavailable bool
}

// Config 保存经过默认值填充和校验的运行配置。
type Config struct {
	// BaseURL 是上游 API 的可信基础地址，可包含自定义路径。
	BaseURL string
	// CatalogURL 是携带 Bearer 凭据获取模型目录的完整地址。
	CatalogURL string
	// QuotaURL 是可选的完整令牌额度地址；为空时从 BaseURL 推导。
	QuotaURL string
	// ModelPrefix 保存客户端模型 ID 的前缀规则。
	ModelPrefix ModelPrefix
	// UpstreamFormat 指定当前认证目录内全部模型的上游格式。
	UpstreamFormat string `yaml:"upstream-format"`
	// Catalog 保存目录刷新策略。
	Catalog Catalog
	// AllowHTTP 显式允许未加密的 HTTP 上游地址。
	AllowHTTP bool
	// RequestTimeout 限制单次上游请求耗时。
	RequestTimeout time.Duration
	// MaxResponseBytes 限制普通上游响应的字节数。
	MaxResponseBytes int64
}

// authConfig 只接收每认证站点字段，运行策略固定使用原默认值。
type authConfig struct {
	// BaseURL 是必填的当前认证上游地址。
	BaseURL string `json:"base_url"`
	// CatalogURL 是可选的当前认证目录地址。
	CatalogURL string `json:"catalog-url"`
	// QuotaURL 是可选的当前认证额度地址。
	QuotaURL string `json:"quota-url"`
	// UpstreamFormat 指定当前认证协议。
	UpstreamFormat *string `json:"upstream-format"`
	// ModelPrefix 接收对象形式的前缀配置。
	ModelPrefix struct {
		// Enabled 区分缺省和显式关闭。
		Enabled *bool `json:"enabled"`
		// Value 区分缺省和显式空值。
		Value *string `json:"value"`
	} `json:"model-prefix"`
	// AllowHTTP 仅对当前认证放行 HTTP。
	AllowHTTP bool `json:"allow-http"`
}

// defaults 固定运行策略，不读取全局上游或策略配置。
func defaults() Config {
	return Config{
		ModelPrefix:    ModelPrefix{Enabled: true, Value: DefaultModelPrefix},
		UpstreamFormat: DefaultUpstreamFormat,
		Catalog:        Catalog{RefreshInterval: DefaultRefreshInterval, StaleWhileUnavailable: true},
		RequestTimeout: DefaultRequestTimeout, MaxResponseBytes: DefaultMaxResponseBytes,
	}
}

// Load 仅验证宿主 YAML 结构，站点和运行策略不从全局读取。
func Load(yamlBytes []byte) (Config, error) {
	var raw map[string]any
	if yaml.Unmarshal(yamlBytes, &raw) != nil {
		return Config{}, fmt.Errorf("插件配置 YAML 结构无效")
	}
	return defaults(), nil
}

// FromAuth 校验当前认证，缺失站点时明确要求迁移且绝不回退全局站点。
func FromAuth(values map[string]any) (Config, error) {
	encoded, err := json.Marshal(values)
	if err != nil {
		return Config{}, fmt.Errorf("认证配置结构无效")
	}
	if prefix, present := values["model-prefix"]; present {
		object, ok := prefix.(map[string]any)
		if !ok || object == nil {
			return Config{}, fmt.Errorf("model-prefix 必须为 enabled/value 对象")
		}
	}
	var raw authConfig
	if json.Unmarshal(encoded, &raw) != nil {
		return Config{}, fmt.Errorf("认证配置字段类型无效，model-prefix 必须为对象")
	}
	if strings.TrimSpace(raw.BaseURL) == "" {
		return Config{}, fmt.Errorf("认证缺少必填 base_url：请将旧全局站点配置迁移到每份认证；不会回退全局站点")
	}
	c := defaults()
	c.BaseURL, c.CatalogURL, c.QuotaURL = strings.TrimSpace(raw.BaseURL), strings.TrimSpace(raw.CatalogURL), strings.TrimSpace(raw.QuotaURL)
	c.UpstreamFormat = orDefault(raw.UpstreamFormat, DefaultUpstreamFormat)
	c.ModelPrefix.Enabled = orDefault(raw.ModelPrefix.Enabled, true)
	c.ModelPrefix.Value = orDefault(raw.ModelPrefix.Value, DefaultModelPrefix)
	c.AllowHTTP = raw.AllowHTTP
	if err := validateURL("base_url", c.BaseURL, c.AllowHTTP); err != nil {
		return Config{}, err
	}
	if c.CatalogURL == "" {
		u, _ := url.Parse(c.BaseURL)
		u.Path = strings.TrimRight(u.Path, "/")
		if !strings.HasSuffix(u.Path, "/v1") {
			u.Path += "/v1"
		}
		u.Path += "/models"
		u.RawPath = ""
		c.CatalogURL = u.String()
	}
	if err := c.validate(); err != nil {
		return Config{}, err
	}
	return c, nil
}

// PublicID 根据前缀配置返回客户端可见的模型 ID。
func PublicID(c Config, upstreamID string) string {
	if c.ModelPrefix.Enabled {
		return c.ModelPrefix.Value + "/" + upstreamID
	}
	return upstreamID
}

// validate 校验运行配置的地址、格式、刷新周期和大小边界。
func (c Config) validate() error {
	if err := validateURL("base_url", c.BaseURL, c.AllowHTTP); err != nil {
		return err
	}
	if err := validateURL("catalog-url", c.CatalogURL, c.AllowHTTP); err != nil {
		return err
	}
	if c.QuotaURL != "" {
		if err := validateURL("quota-url", c.QuotaURL, c.AllowHTTP); err != nil {
			return err
		}
	}
	if c.Catalog.RefreshInterval < time.Minute {
		return fmt.Errorf("catalog.refresh-interval: must be at least 1m")
	}
	if c.MaxResponseBytes <= 0 {
		return fmt.Errorf("max-response-bytes: must be positive")
	}
	switch c.UpstreamFormat {
	case "openai", "claude", "openai-response":
	default:
		return fmt.Errorf("upstream-format: must be openai, claude, or openai-response")
	}
	if c.ModelPrefix.Enabled && !validPrefix(c.ModelPrefix.Value) {
		return fmt.Errorf("model-prefix.value: invalid provider-ID characters")
	}
	return nil
}

// validateURL 限定可信 HTTP(S) 地址，拒绝凭据、查询、片段和歧义路径。
func validateURL(name, raw string, allowHTTP bool) error {
	if raw == "" {
		return fmt.Errorf("%s: must not be empty", name)
	}
	u, err := url.Parse(raw)
	if err != nil || u.Host == "" || u.Hostname() == "" || u.Opaque != "" {
		return fmt.Errorf("%s: invalid URL", name)
	}
	if u.RawQuery != "" || u.ForceQuery || u.Fragment != "" || strings.Contains(raw, "#") || u.User != nil {
		return fmt.Errorf("%s: must not contain query, fragment, or userinfo", name)
	}
	switch u.Scheme {
	case "https":
	case "http":
		if !allowHTTP {
			return fmt.Errorf("%s: http scheme requires allow-http", name)
		}
	default:
		return fmt.Errorf("%s: unsupported scheme; use https", name)
	}
	// 解码后的路径禁止遍历、反斜杠与残留转义，避免代理多次解码改变目标。
	if strings.ContainsAny(u.Path, "\\%") || strings.Contains(u.Path, "//") {
		return fmt.Errorf("%s: invalid path", name)
	}
	for _, segment := range strings.Split(u.Path, "/") {
		if segment == "." || segment == ".." {
			return fmt.Errorf("%s: invalid path", name)
		}
	}
	return nil
}

// validPrefix 校验提供商标识，首字符必须为字母或数字。
func validPrefix(s string) bool {
	if s == "" {
		return false
	}
	for i, r := range s {
		alnum := r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9'
		if i == 0 && !alnum {
			return false
		}
		if !alnum && r != '.' && r != '_' && r != '-' {
			return false
		}
	}
	return true
}

// orDefault 仅在未显式提供值时使用默认值。
func orDefault[T any](p *T, def T) T {
	if p != nil {
		return *p
	}
	return def
}
