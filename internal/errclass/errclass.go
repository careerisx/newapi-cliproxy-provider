// Package errclass 提供统一错误分类、重试语义及凭证脱敏。
package errclass

import (
	"regexp"
	"strings"

	"github.com/router-for-me/CLIProxyAPI/v8/sdk/pluginabi"
)

// Class is a stable error-category identifier (FR-009) used as the
// envelope error Code.
type Class string

const (
	ClassAuth         Class = "auth_failure"
	ClassInvalidModel Class = "invalid_model"
	ClassUnsupported  Class = "unsupported_protocol_or_parameter"
	ClassRateLimit    Class = "rate_limit"
	ClassQuota        Class = "quota_exhaustion"
	ClassBilling      Class = "billing"
	ClassUpstream     Class = "upstream_server_failure"
	ClassNetwork      Class = "timeout_or_network_failure"
	ClassTranslation  Class = "translation_failure"
)

// Error describes classified request errors and retryability for CPA execution.
type Error struct {
	// Class 稳定错误分类标识。
	Class      Class
	// Message 响应消息或已脱敏错误说明。
	Message    string
	// StatusCode HTTP 状态码，零表示未提供。
	StatusCode int
	// Retryable 是否允许执行器重试错误。
	Retryable  bool
}

// Error 返回稳定错误分类与已处理的错误消息。
func (e *Error) Error() string {
	return string(e.Class) + ": " + e.Message
}

// FromStatus classifies an upstream HTTP status per resolved §7.
// Network/timeout failures (no status) use FromNetwork instead. A redacted
// message containing "quota" (case-insensitive) upgrades 429/402/403 to
// ClassQuota — a bounded heuristic given no documented upstream quota
// taxonomy.
// FromStatus 根据上游状态和配额提示生成脱敏错误及重试策略。
func FromStatus(status int, message string) *Error {
	msg := Redact(message)
	lowerMsg := strings.ToLower(msg)
	quota := strings.Contains(lowerMsg, "quota")
	quotaClass := func(base Class) Class {
		if quota {
			return ClassQuota
		}
		return base
	}
	switch status {
	case 401:
		return &Error{Class: ClassAuth, Message: msg, StatusCode: status, Retryable: true}
	case 403:
		return &Error{Class: quotaClass(ClassAuth), Message: msg, StatusCode: status, Retryable: true}
	case 402:
		return &Error{Class: quotaClass(ClassBilling), Message: msg, StatusCode: status, Retryable: true}
	case 404:
		return &Error{Class: ClassInvalidModel, Message: msg, StatusCode: status}
	case 429:
		return &Error{Class: quotaClass(ClassRateLimit), Message: msg, StatusCode: status, Retryable: true}
	}
	if status >= 500 {
		return &Error{Class: ClassUpstream, Message: msg, StatusCode: status, Retryable: true}
	}
	return &Error{Class: ClassUnsupported, Message: msg, StatusCode: status}
}

// FromNetwork classifies a PRE-first-byte network/timeout failure; always
// retryable (§7). Failures observed after the first upstream byte must use
// PostFirstByteNetwork instead.
// FromNetwork 分类首字节到达前的网络错误，允许执行器重试。
func FromNetwork(err error) *Error {
	msg := ""
	if err != nil {
		msg = Redact(err.Error())
	}
	return &Error{Class: ClassNetwork, Message: msg, Retryable: true}
}

// PostFirstByteNetwork classifies a network/timeout failure seen AFTER the
// first upstream byte was received: part of the response may already be
// delivered downstream, so a retry would risk duplicate delivery of a half
// stream — never retryable (§7).
// PostFirstByteNetwork 分类首字节到达后的网络错误，禁止重试以避免重复输出。
func PostFirstByteNetwork(msg string) *Error {
	return &Error{Class: ClassNetwork, Message: Redact(msg), Retryable: false}
}

// Translation reports an FR-005/FR-006 translation failure; never retryable.
// Translation 生成不可重试的协议转换错误并脱敏消息。
func Translation(msg string) *Error {
	return &Error{Class: ClassTranslation, Message: Redact(msg)}
}

// UpstreamFallback builds a ClassUpstream error for the adapter fallback
// sites (chunk/failure errors) that have no upstream status code. Resolved
// §7 makes upstream server failures retryable; the message is redacted at
// construction.
// UpstreamFallback 为缺少 HTTP 状态的上游失败生成可重试错误。
func UpstreamFallback(msg string) *Error {
	return &Error{Class: ClassUpstream, Message: Redact(msg), Retryable: true}
}

// ToEnvelopeError converts to the shared SDK wire type with a redacted
// message. Re-redacting at the envelope edge is INTENTIONAL defense-in-depth:
// constructors already redact (so non-envelope consumers — logs, executor
// fail paths reading .Message directly) are safe, and Redact is idempotent.
// ToEnvelopeError 转换为 SDK 错误外壳，并在输出边界再次脱敏。
func ToEnvelopeError(e *Error) pluginabi.Error {
	return pluginabi.Error{
		Code:       string(e.Class),
		Message:    Redact(e.Message),
		Retryable:  e.Retryable,
		HTTPStatus: e.StatusCode,
	}
}

// bearerRe matches a bearer token (case-insensitive scheme, optional
// space/tab separator, token terminated by whitespace, comma, or quote).
// The separator is optional so a bare "bearer" word is redacted exactly as
// the previous hand-rolled scanner did. Redact is idempotent — applying it
// twice yields the same output — which is what makes edge re-redaction safe.
// bearerRe 识别错误文本中需要脱敏的 Bearer 凭证。
var bearerRe = regexp.MustCompile(`(?i)bearer[ \t]*[^\s,"]*`)

// apiKeyRe finds x-api-key header echoes (Messages-route auth, FR-007):
// case-insensitive name, then a run of quote/colon/equals/space separators,
// then the value run. The value class spans hex/base62 plus the base64
// alphabet ("+", "/", "="), matching bearerRe/queryKeyRe sibling classes.
// The candidate is only replaced when the value looks
// like a key (contains a digit) so ordinary prose after the header name —
// "x-api-key authentication failed" — passes untouched; every real key
// alphabet (hex, base64, base62) carries digits. Idempotent: "[redacted]"
// starts with '[' which is outside the value class, so it never re-matches.
// apiKeyRe 识别错误文本中的 API 密钥请求头回显。
var apiKeyRe = regexp.MustCompile(`(?i)x-api-key["':= ]*[A-Za-z0-9_\-.+/=]+`)

// redactAPIKey 只替换符合密钥特征的请求头值，避免误伤普通说明文字。
func redactAPIKey(match string) string {
	value := strings.TrimLeft(match[len("x-api-key"):], "\"':= ")
	hasDigit := false
	for _, r := range value {
		if r >= '0' && r <= '9' {
			hasDigit = true
			break
		}
	}
	if !hasDigit || len(value) < 8 {
		return match
	}
	return match[:len(match)-len(value)] + "[redacted]"
}

// queryKeyRe redacts secrets carried in URL query strings (FR-011): transport
// failures conventionally echo the full URL, so base_url/catalog-url keys
// passed as ?api_key=… / ?token=… must not survive into envelopes or logs.
// The value run stops at whitespace, '&', and quotes so consecutive params
// survive; the replacement keeps the parameter name. Idempotent: the
// redacted value "[redacted]" still matches the value class, so re-running
// yields the same text.
// queryKeyRe 识别 URL 查询参数中的访问密钥或令牌。
var queryKeyRe = regexp.MustCompile(`(?i)([?&](?:api[_-]?key|key|access[_-]?token|token)=)[^\s&"']+`)

// Redact strips bearer tokens, x-api-key echoes, and query-string key
// parameters from a message so keys never reach logs or clients.
// Redact 移除消息中的 Bearer 凭证、API 密钥头及查询参数密钥。
func Redact(msg string) string {
	msg = bearerRe.ReplaceAllString(msg, "Bearer [redacted]")
	msg = apiKeyRe.ReplaceAllStringFunc(msg, redactAPIKey)
	return queryKeyRe.ReplaceAllString(msg, "${1}[redacted]")
}
