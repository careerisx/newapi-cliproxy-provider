package chatcompletions

import (
	"encoding/json"
	"fmt"

	"newapi-provider/internal/adapter/shared"
	"newapi-provider/internal/errclass"
)

// ---- upstream Chat Completions response shapes (FR-006) ----

type ccRespMessage struct {
	// Content 原始或归一化的消息内容。
	Content   json.RawMessage     `json:"content"` // JSON string or part array
	// ToolCalls 消息携带的函数调用列表。
	ToolCalls []shared.CCToolCall `json:"tool_calls"`
	// Reasoning carries the vendor thinking spellings (reasoning /
	// reasoning_details[].text / reasoning_content); embedded untagged so
	// the shared kernel decodes all three at once.
	// ReasoningFields 统一解码上游兼容推理文本字段。
	shared.ReasoningFields
}

type ccChoice struct {
	// Message 响应消息或已脱敏错误说明。
	Message      ccRespMessage `json:"message"`
	// FinishReason Chat Completions 完成原因。
	FinishReason string        `json:"finish_reason"`
}

type ccUsage struct {
	// PromptTokens Chat Completions 输入令牌数。
	PromptTokens     int64 `json:"prompt_tokens"`
	// CompletionTokens Chat Completions 输出令牌数。
	CompletionTokens int64 `json:"completion_tokens"`
	// PromptDetails 可选输入令牌详情。
	PromptDetails    *struct {
		// CachedTokens 可选缓存读取令牌数。
		CachedTokens *int64 `json:"cached_tokens"`
	} `json:"prompt_tokens_details"`
	// CompletionDetails 可选输出令牌详情。
	CompletionDetails *struct {
		// ReasoningTokens 可选推理令牌数。
		ReasoningTokens *int64 `json:"reasoning_tokens"`
	} `json:"completion_tokens_details"`
}

type ccResponse struct {
	// ID 响应、内容块或工具调用的协议标识。
	ID      string     `json:"id"`
	// Model 上游模型标识。
	Model   string     `json:"model"`
	// Choices 上游候选结果列表。
	Choices []ccChoice `json:"choices"`
	// Usage 令牌用量及可选缓存详情。
	Usage   *ccUsage   `json:"usage"`
}

// ConvertNonStreamResponse translates an upstream Chat Completions HTTP
// response back into the client protocol sourceFormat (FR-006, AC §B).
// Statuses >= 400 become classified redacted errors (§7); "openai"
// passes through with reasoning text mirrored onto the standard
// reasoning_content member; "claude" and "openai-response" are converted,
// preserving text, tool calls, finish reason, usage, and vendor thinking
// text (thinking block / reasoning item). Unknown formats are
// ClassUnsupported; malformed upstream bodies are ClassTranslation.
// ConvertNonStreamResponse 转换非流式上游响应并统一处理错误状态。
func ConvertNonStreamResponse(sourceFormat string, status int, upstreamBody []byte) ([]byte, *errclass.Error) {
	if status >= 400 {
		return nil, shared.UpstreamStatusError(status, upstreamBody)
	}
	switch sourceFormat {
	case "openai":
		return passthroughResponse(upstreamBody)
	case "claude":
		return chatToClaude(upstreamBody)
	case "openai-response":
		return chatToResponses(upstreamBody)
	default:
		return nil, shared.UnsupportedFormat(sourceFormat, EndpointPath)
	}
}

// decodeCC validates and decodes an upstream non-stream response body;
// a body without choices is a translation failure, never silently empty
// (FR-006).
// decodeCC 校验并解码 Chat Completions 响应，拒绝缺失候选结果。
func decodeCC(body []byte) (*ccResponse, *errclass.Error) {
	var resp ccResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, errclass.Translation("malformed Chat Completions response JSON: " + err.Error())
	}
	if len(resp.Choices) == 0 {
		return nil, errclass.Translation("Chat Completions response carries no choices")
	}
	return &resp, nil
}

// passthroughResponse validates the Chat Completions body decodes as
// JSON and forwards it unchanged except for the reasoning_content
// backfill, which mirrors vendor thinking spellings onto the standard
// member consumers read (FR-003).
// passthroughResponse 校验原生响应并按需补齐通用推理文本字段。
func passthroughResponse(body []byte) ([]byte, *errclass.Error) {
	if !json.Valid(body) {
		return nil, errclass.Translation("malformed openai response JSON")
	}
	if fixed := shared.BackfillReasoningContent(body, "message"); fixed != nil {
		return fixed, nil
	}
	return body, nil
}

// ---- Chat Completions -> claude (Anthropic Messages) (FR-006) ----

// chatToClaude converts a Chat Completions result into a Messages
// response via the shared Claude-result kernel: content becomes text or
// image blocks, tool_calls become tool_use blocks, finish_reason maps to
// stop_reason, and prompt/completion token counts map to input/output
// usage (FR-006, AC §B). Vendor thinking text leads as a thinking block,
// the position and shape the streaming synthesizer emits.
// chatToClaude 将 Chat Completions 内容和工具调用转换为 Messages 响应。
func chatToClaude(body []byte) ([]byte, *errclass.Error) {
	resp, eErr := decodeCC(body)
	if eErr != nil {
		return nil, eErr
	}
	choice := resp.Choices[0]
	blocks, eErr := ccContentToBlocks(choice.Message.Content)
	if eErr != nil {
		return nil, eErr
	}
	toolUse, eErr := claudeToolUseBlocks(choice.Message.ToolCalls)
	if eErr != nil {
		return nil, eErr
	}
	// Thinking precedes text and tool_use, matching the stream leg's block
	// order (mode parity). The signature member is deliberately absent:
	// the vendor stream carries none.
	if thinking, ok := choice.Message.ReasoningText(); ok {
		blocks = append([]claudeBlock{shared.ClaudeThinkingBlock(thinking)}, blocks...)
	}
	var inputTokens, outputTokens int64
	var cacheRead *int64
	if resp.Usage != nil {
		inputTokens, outputTokens = resp.Usage.PromptTokens, resp.Usage.CompletionTokens
		if resp.Usage.PromptDetails != nil {
			cacheRead = resp.Usage.PromptDetails.CachedTokens
		}
	}
	// Observed tool calls outrank the status-derived reason: a terminal
	// length cannot downgrade tool_calls to max_tokens.
	stop := shared.TerminalReason(len(choice.Message.ToolCalls) > 0,
		"tool_use", shared.FinishToClaudeStop(choice.FinishReason))
	b, _ := json.Marshal(shared.NewClaudeResult(resp.ID, resp.Model, stop,
		shared.ClampSubtract(inputTokens, cacheRead), outputTokens, cacheRead, nil, append(blocks, toolUse...)))
	return b, nil // only marshallable composed types; cannot fail
}

// claudeStopReason was replaced by shared.FinishToClaudeStop, which adds
// the missing content_filter→refusal leg so stop reasons round-trip.

// ccContentToBlocks normalizes response content into Anthropic blocks
// via the shared kernel, which owns validation and the empty-input
// policy (FR-006 multimodal preservation). Responses-flavored alias part
// names in upstream CC bodies therefore behave identically to every
// other route (F5 parity).
// ccContentToBlocks 通过共享转换核保留文本和图片内容块。
func ccContentToBlocks(raw json.RawMessage) ([]claudeBlock, *errclass.Error) {
	return shared.ClaudeBlocksFromOpenAI(raw, EndpointPath)
}

// claudeToolUseBlocks converts tool calls into tool_use blocks; malformed
// arguments are a translation failure, never dropped (FR-006).
// claudeToolUseBlocks 转换工具调用块并校验参数 JSON。
func claudeToolUseBlocks(calls []shared.CCToolCall) ([]claudeBlock, *errclass.Error) {
	var blocks []claudeBlock
	for _, tc := range calls {
		input, eErr := shared.DecodeArgs(tc.Function.Arguments)
		if eErr != nil {
			return nil, eErr
		}
		blocks = append(blocks, claudeBlock{
			"type": "tool_use", "id": tc.ID, "name": tc.Function.Name, "input": input,
		})
	}
	return blocks, nil
}

// ---- Chat Completions -> openai-response (Responses) (FR-006) ----

// chatToResponses converts a Chat Completions result into Responses
// output items: text content becomes an assistant output_text message
// item, tool_calls become function_call items, finish_reason length maps
// to status incomplete, and prompt/completion tokens map to input/output
// usage (FR-006). Vendor thinking text leads as a reasoning item carrying
// one summary_text part, the shape and position the streaming synthesizer
// emits (mode parity).
// chatToResponses 将 Chat Completions 响应转换为 Responses 输出项及用量。
func chatToResponses(body []byte) ([]byte, *errclass.Error) {
	resp, eErr := decodeCC(body)
	if eErr != nil {
		return nil, eErr
	}
	choice := resp.Choices[0]
	// Status derives positionally from finish alone (Responses-status
	// exemption): length→incomplete, else completed — matching the stream
	// sibling; tool calls are represented by output items, not status.
	status := shared.ResponseStatusFromCCFinish(choice.FinishReason)
	out := shared.ResponsesResult{
		ID:     resp.ID,
		Object: "response",
		Model:  resp.Model,
		Status: status,
		Output: []any{},
	}
	blocks, eErr := ccContentToBlocks(choice.Message.Content)
	if eErr != nil {
		return nil, eErr
	}
	// The shared assembler gives Chat Completions placement (no reserved
	// slot): the message leads and appears only when text is non-empty,
	// matching the streaming terminal (FR-006 sibling parity). A reasoning
	// item, when present, occupies the first slot because upstream thinking
	// precedes text — the same position the stream announces for it, so the
	// message is pinned behind it.
	oa := shared.NewOutputAssembler(resp.ID)
	if thinking, ok := choice.Message.ReasoningText(); ok {
		oa.AppendReasoning(shared.NewRespReasoningItem(shared.ReasoningItemID(resp.ID, 0), thinking))
	}
	sawText := false
	for _, b := range blocks {
		if b["type"] != "text" {
			return nil, errclass.Translation(fmt.Sprintf(
				"%s output cannot represent %q content blocks", EndpointPath, b["type"]))
		}
		oa.AddText(b["text"].(string))
		sawText = true
	}
	if sawText {
		oa.ReserveTextSlot()
	}
	for _, tc := range choice.Message.ToolCalls {
		oa.AppendFunctionCall(tc.ID, tc.Function.Name, shared.DefaultArgs(tc.Function.Arguments))
	}
	out.Output = oa.Render()
	if resp.Usage != nil {
		var details shared.UsageDetails
		if resp.Usage.PromptDetails != nil {
			details.CachedTokens = resp.Usage.PromptDetails.CachedTokens
		}
		if resp.Usage.CompletionDetails != nil {
			details.ReasoningTokens = resp.Usage.CompletionDetails.ReasoningTokens
		}
		out.Usage = shared.NewResponsesUsageFrom(resp.Usage.PromptTokens, resp.Usage.CompletionTokens, details)
	}
	b, _ := json.Marshal(out) // only marshallable composed types; cannot fail
	return b, nil
}
