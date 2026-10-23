// Non-stream response conversion for the Messages adapter (FR-006,
// AC §C): an upstream Anthropic Messages HTTP response is translated back
// into the client protocol — validated passthrough for claude clients,
// Chat Completions / Responses synthesis otherwise. Reasoning content has
// no representation outside Anthropic wire format and is explicitly
// omitted (FR-005 policy).
package messages

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"newapi-provider/internal/adapter/shared"
	"newapi-provider/internal/errclass"
)

// ---- upstream Anthropic Messages response shapes (decode-only) ----

type claudeBlockIn struct {
	// Type 协议对象或内容分部类型。
	Type  string          `json:"type"` // text | thinking | tool_use | ...
	// Text 当前内容分部或摘要文本。
	Text  string          `json:"text"`
	// ID 响应、内容块或工具调用的协议标识。
	ID    string          `json:"id"`
	// Name 工具函数名。
	Name  string          `json:"name"`
	// Input 输入项目或工具参数载荷。
	Input json.RawMessage `json:"input"`
}

type claudeUsageIn struct {
	// InputTokens 输入令牌数。
	InputTokens   int64  `json:"input_tokens"`
	// OutputTokens 输出令牌数。
	OutputTokens  int64  `json:"output_tokens"`
	// CacheRead 可选缓存读取输入令牌数。
	CacheRead     *int64 `json:"cache_read_input_tokens"`
	// CacheCreation 可选缓存创建输入令牌数。
	CacheCreation *int64 `json:"cache_creation_input_tokens"`
}

type claudeResponseIn struct {
	// ID 响应、内容块或工具调用的协议标识。
	ID         string          `json:"id"`
	// Model 上游模型标识。
	Model      string          `json:"model"`
	// Content 原始或归一化的消息内容。
	Content    []claudeBlockIn `json:"content"`
	// StopReason Messages 停止原因。
	StopReason string          `json:"stop_reason"`
	// Usage 令牌用量及可选缓存详情。
	Usage      claudeUsageIn   `json:"usage"`
}

// ConvertNonStreamResponse translates an upstream Anthropic Messages HTTP
// response back into the client protocol sourceFormat (FR-006, AC §C).
// Statuses >= 400 become classified redacted errors (§7); "claude"
// passes through; "openai" and "openai-response" are converted, preserving
// text, tool_use, stop reason, and usage while dropping thinking blocks
// (FR-005 explicit omission). Unknown formats are ClassUnsupported;
// malformed upstream bodies are ClassTranslation.
// ConvertNonStreamResponse 转换非流式上游响应并统一处理错误状态。
func ConvertNonStreamResponse(sourceFormat string, status int, upstreamBody []byte) ([]byte, *errclass.Error) {
	if status >= 400 {
		return nil, shared.UpstreamStatusError(status, upstreamBody)
	}
	switch sourceFormat {
	case "claude":
		if !json.Valid(upstreamBody) {
			return nil, errclass.Translation("malformed Messages response JSON")
		}
		return upstreamBody, nil
	case "openai":
		return claudeToChat(upstreamBody)
	case "openai-response":
		return claudeToResponses(upstreamBody)
	default:
		return nil, shared.UnsupportedFormat(sourceFormat, EndpointPath)
	}
}

// decodeClaude validates and decodes an upstream non-stream Messages body;
// a missing content array is a translation failure, never silently empty
// (FR-006).
// decodeClaude 校验并解码 Messages 响应，拒绝缺失内容数组。
func decodeClaude(body []byte) (*claudeResponseIn, *errclass.Error) {
	var resp claudeResponseIn
	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, errclass.Translation("malformed Messages response JSON: " + err.Error())
	}
	if resp.Content == nil {
		return nil, errclass.Translation("Messages response carries no content blocks")
	}
	return &resp, nil
}

// claudeToChat converts a Messages response into a Chat Completions
// response: text blocks concatenate into message.content, tool_use blocks
// become indexed tool_calls with JSON-encoded arguments, stop_reason maps
// to finish_reason, and input/output tokens map to prompt/completion
// usage (FR-006, AC §C). Thinking and redacted_thinking blocks are
// omitted — Chat Completions has no standard reasoning-delta field
// (FR-005 explicit omission).
// claudeToChat 将 Messages 消息或响应转换为 Chat Completions 表达。
func claudeToChat(body []byte) ([]byte, *errclass.Error) {
	resp, eErr := decodeClaude(body)
	if eErr != nil {
		return nil, eErr
	}
	var text strings.Builder
	msg := shared.CCMessage{Role: "assistant"}
	for _, blk := range resp.Content {
		switch blk.Type {
		case "text":
			text.WriteString(blk.Text)
		case "tool_use":
			args, _ := json.Marshal(decodeBlockInput(blk.Input))
			tc := shared.CCToolCall{ID: blk.ID, Type: "function"}
			tc.Function.Name = blk.Name
			tc.Function.Arguments = string(args)
			msg.ToolCalls = append(msg.ToolCalls, tc)
		case "thinking", "redacted_thinking":
			// FR-005 explicit omission policy: no standard Chat
			// Completions reasoning field; dropped rather than put in a
			// non-standard one. redacted_thinking is pure encrypted
			// reasoning metadata, omitted like the stream path instead of
			// failing the conversion.
		default:
			return nil, errclass.Translation(fmt.Sprintf(
				"%s output cannot represent %q content blocks", EndpointPath, blk.Type))
		}
	}
	// Aggregated text ships verbatim (repo-wide verbatim policy, matching
	// OutputAssembler.Render and the streaming legs): no trimming here.
	if text.Len() > 0 {
		msg.Content, _ = json.Marshal(text.String())
	}
	// Observed tool calls outrank the status-derived reason: a terminal
	// max_tokens cannot downgrade tool_calls to length (FR-006).
	finish := shared.TerminalReason(len(msg.ToolCalls) > 0, "tool_calls",
		shared.ClaudeStopToFinish(resp.StopReason))
	out := shared.CompletionEnvelope(resp.ID, resp.Model, time.Now().Unix(),
		[]map[string]any{{
			"index":         0,
			"message":       msg,
			"finish_reason": finish,
		}},
		shared.CCUsageFrom(resp.Usage.InputTokens+valueOrZero(resp.Usage.CacheRead)+valueOrZero(resp.Usage.CacheCreation), resp.Usage.OutputTokens, shared.UsageDetails{CachedTokens: resp.Usage.CacheRead}))
	b, _ := json.Marshal(out) // composed marshallable types only; cannot fail
	return b, nil
}

// decodeBlockInput normalizes a tool_use input payload: absent/empty
// decodes to an empty object so every tool_call carries valid arguments.
// The raw bytes come from an already-validated outer document, so they are
// always valid JSON.
// decodeBlockInput 将缺省工具输入归一化为空对象，保留已有 JSON 输入。
func decodeBlockInput(raw json.RawMessage) any {
	if !shared.HasContent(raw) {
		return map[string]any{}
	}
	var v any
	json.Unmarshal(raw, &v)
	return v
}

// ---- Messages -> openai-response (Responses) (FR-006) ----

// claudeToResponses converts a Messages response into a Responses result:
// text blocks aggregate into an assistant output_text message item,
// tool_use blocks become function_call items, stop_reason end_turn maps to
// status completed, and input/output tokens map to input/output usage
// (FR-006). Thinking and redacted_thinking blocks are omitted — Responses
// has no standard reasoning-summary equivalent without signatures (FR-005
// policy).
// claudeToResponses 将 Messages 内容和工具使用块转换为 Responses 输出项。
func claudeToResponses(body []byte) ([]byte, *errclass.Error) {
	resp, eErr := decodeClaude(body)
	if eErr != nil {
		return nil, eErr
	}
	// Stop reason maps straight onto the Responses status via the shared
	// table (max_tokens -> incomplete, end_turn -> completed); function_call
	// items already carry the tool signal (FR-006).
	status := shared.ResponseStatusFromClaudeStop(resp.StopReason)
	out := shared.ResponsesResult{
		ID:     resp.ID,
		Object: "response",
		Model:  resp.Model,
		Status: status,
		Output: []any{},
		Usage:  shared.NewResponsesUsageFrom(resp.Usage.InputTokens+valueOrZero(resp.Usage.CacheRead)+valueOrZero(resp.Usage.CacheCreation), resp.Usage.OutputTokens, shared.UsageDetails{CachedTokens: resp.Usage.CacheRead, CacheWriteTokens: resp.Usage.CacheCreation}),
	}
	oa := shared.NewOutputAssembler(resp.ID)
	for _, blk := range resp.Content {
		switch blk.Type {
		case "text":
			// The shared assembler reserves the first text block's slot so
			// the output array matches the streaming synthesizer's
			// upstream-order emission (stream.go outputItems): identical
			// input must not reorder by mode (FR-006).
			oa.ReserveTextSlot()
			oa.AddText(blk.Text)
		case "tool_use":
			args, _ := json.Marshal(decodeBlockInput(blk.Input))
			oa.AppendFunctionCall(blk.ID, blk.Name, string(args))
		case "thinking", "redacted_thinking":
			// FR-005 explicit omission policy (see claudeToChat).
		default:
			return nil, errclass.Translation(fmt.Sprintf(
				"%s output cannot represent %q content blocks", EndpointPath, blk.Type))
		}
	}
	out.Output = oa.Render()
	b, _ := json.Marshal(out) // only marshallable composed types; cannot fail
	return b, nil
}

// valueOrZero 读取可选计数，缺省字段按零参与用量计算。
func valueOrZero(v *int64) int64 {
	if v != nil {
		return *v
	}
	return 0
}
