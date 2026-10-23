// Package shared 提供共用的 JSON、工具、图片、SSE 和错误脱敏能力。
package shared

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"

	"newapi-provider/internal/errclass"
)

// SSEFramer buffers SSE bytes across chunks and yields complete frames.
// Multiple data: lines within one frame join with "\n" per the SSE spec;
// comments, heartbeats, and stray blank lines are skipped; a frame is
// emitted only when at least one event:/data: line was seen. Raw blocks
// are captured verbatim (including line terminators) only when wantRaw is
// set — pass false when no consumer reads them.
type SSEFramer struct {
	// buf 跨输入分片保留的待处理字节。
	buf       []byte          // partial line carried across Push calls
	// eventType 当前 SSE 事件名。
	eventType string          // current event: value
	// data 当前帧合并后的数据行载荷。
	data      strings.Builder // joined data: payloads of the current frame
	// sawFrame 是否遇到有效事件或数据行。
	sawFrame  bool            // event:/data: seen since last emit
	// wantRaw 是否保留原始 SSE 帧字节。
	wantRaw   bool
	// raw 透传所需的原始帧字节。
	raw       []byte // verbatim current block when wantRaw
}

// NewSSEFramer prepares a framer; wantRaw enables verbatim block capture.
// NewSSEFramer 创建增量 SSE 分帧器，仅在透传需要时保留原始帧字节。
func NewSSEFramer(wantRaw bool) *SSEFramer {
	return &SSEFramer{wantRaw: wantRaw}
}

// Push appends one network chunk to the framer buffer.
// Push 追加网络分片，供后续逐帧读取。
func (f *SSEFramer) Push(chunk []byte) {
	f.buf = append(f.buf, chunk...)
}

// Next returns the next complete frame (eventType, joined data, raw block).
// ok=false means the buffer needs more input.
// Next 提取下一完整 SSE 帧，保留跨分片状态并合并多条数据行。
func (f *SSEFramer) Next() (eventType, data string, raw []byte, ok bool) {
	for {
		i := bytes.IndexByte(f.buf, '\n')
		if i < 0 {
			return "", "", nil, false
		}
		line := f.buf[:i] // excludes \n, may include \r
		f.buf = f.buf[i+1:]
		if f.wantRaw {
			f.raw = append(f.raw, line...)
			f.raw = append(f.raw, '\n')
		}
		text := strings.TrimSuffix(string(line), "\r")
		switch {
		case text == "":
			if !f.sawFrame {
				continue // stray blank line between frames
			}
			eventType, data, raw = f.eventType, f.data.String(), f.raw
			f.eventType = ""
			f.data.Reset()
			f.sawFrame = false
			f.raw = nil
			return eventType, data, raw, true
		case strings.HasPrefix(text, ":"):
			// SSE comment/heartbeat: raw capture only.
		case strings.HasPrefix(text, "event:"):
			f.eventType = strings.TrimSpace(strings.TrimPrefix(text, "event:"))
			f.sawFrame = true
		case strings.HasPrefix(text, "data:"):
			if f.data.Len() > 0 {
				f.data.WriteByte('\n')
			}
			f.data.WriteString(strings.TrimPrefix(strings.TrimPrefix(text, "data:"), " "))
			f.sawFrame = true
		default:
			// Unknown lines are ignored outside raw capture.
		}
	}
}

// RewriteModelID rewrites the top-level model field of a native-format
// request body to the bare upstream ID. The body decodes ONCE one level
// into json.RawMessage values, so every nested value passes through
// byte-exact — notably large integers that a float64 roundtrip would
// mangle (FR-005 fidelity). When the decoded model field's raw bytes
// already equal the quoted upstream ID (the common bare-ID/prefix client
// case), the original body is returned untouched before any mutation —
// a pure no-op that skips the remarshal. The comparison target mirrors
// the rewrite quoting below, and a decoded RawMessage always holds a
// valid JSON value, so an upstream id that cannot be represented
// (quotes/backslashes/control characters) can never match here and still
// reaches the rewrite's error. Duplicate keys follow encoding/json
// semantics: the LAST occurrence wins. formatLabel names the source
// protocol in the malformed-JSON error.
// RewriteModelID 仅替换顶层模型标识，保留嵌套 JSON 精度并避免无意义重编码。
func RewriteModelID(upstreamModel string, body []byte, formatLabel string) ([]byte, *errclass.Error) {
	var req map[string]json.RawMessage
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, errclass.Translation("malformed " + formatLabel + " request JSON: " + err.Error())
	}
	// A literal JSON null body unmarshals without error into a NIL map;
	// assigning the model field would panic, so reject it as malformed.
	if req == nil {
		return nil, errclass.Translation("malformed request body: JSON null is not a valid request")
	}
	if raw, ok := req["model"]; ok && string(raw) == `"`+upstreamModel+`"` {
		return body, nil
	}
	req["model"] = json.RawMessage(`"` + upstreamModel + `"`)
	b, err := json.Marshal(req)
	if err != nil {
		// A catalog model id containing quotes/backslashes/control
		// characters cannot be represented as a JSON string value.
		return nil, errclass.Translation("model id cannot be represented as JSON")
	}
	return b, nil
}

// HasContent reports whether a raw content/instructions/arguments field is
// present (non-empty and not JSON null); it is the presence test used
// before string-or-parts decoding.
// HasContent 区分缺失或 JSON null 与实际存在的字段载荷。
func HasContent(raw json.RawMessage) bool {
	return len(raw) > 0 && string(raw) != "null"
}

// DefaultArgs applies the FR-005/FR-006 absent-tool-arguments policy:
// whitespace-only or JSON-null arguments become an empty JSON object so
// every tool call carries valid arguments.
// DefaultArgs 将缺省、空白和 null 工具参数归一化为空 JSON 对象。
func DefaultArgs(args string) string {
	s := strings.TrimSpace(args)
	if s == "" || s == "null" {
		return "{}"
	}
	return args
}

// emptyObjectSchema is the normalized parameter schema for tools declared
// without one.
// emptyObjectSchema 缺省工具参数使用的空对象模式。
var emptyObjectSchema = json.RawMessage(`{"type":"object","properties":{}}`)

// ObjectSchema normalizes a tool parameter schema for generic
// function-calling validators that require type:object.
//
// Absent or null schemas become the empty object schema. Object schemas that
// omit type (for example a top-level anyOf) get type:object injected. Schemas
// that already declare type, or that are not JSON objects, are returned
// untouched.
// ObjectSchema 归一化缺省工具参数模式，为省略类型的对象补充 object 类型。
func ObjectSchema(raw json.RawMessage) json.RawMessage {
	if !HasContent(raw) {
		return emptyObjectSchema
	}
	return ensureObjectType(raw)
}

// ensureObjectType 仅为未声明类型的对象模式补充类型，其他模式保持原样。
func ensureObjectType(raw json.RawMessage) json.RawMessage {
	var obj map[string]json.RawMessage
	if err := json.Unmarshal(raw, &obj); err != nil || obj == nil {
		return raw
	}
	if _, ok := obj["type"]; ok {
		return raw
	}
	obj["type"] = json.RawMessage(`"object"`)
	out, err := json.Marshal(obj)
	if err != nil {
		return raw
	}
	return out
}

// ToolChoice kinds returned by DecodeToolChoice: "absent" (field missing
// or null), "auto", "any" (any/required), "none", "named" (forced single
// tool, name carries the tool name).
const (
	ToolChoiceAbsent = "absent"
	ToolChoiceAuto   = "auto"
	ToolChoiceAny    = "any"
	ToolChoiceNone   = "none"
	ToolChoiceNamed  = "named"
)

// AllToolChoiceKinds enumerates every ToolChoice* kind DecodeToolChoice
// returns, in kind order (absent/auto/any/none/named). Consumers'
// exhaustiveness tests must iterate this slice; add a kind here first.
// AllToolChoiceKinds 工具选择分类的完整集合，供转换器共享封闭策略集合。
var AllToolChoiceKinds = []string{
	ToolChoiceAbsent,
	ToolChoiceAuto,
	ToolChoiceAny,
	ToolChoiceNone,
	ToolChoiceNamed,
}

// DecodeToolChoice classifies a tool_choice value across the Chat
// Completions ({type:function,function:{name}}), Anthropic
// ({type:tool,name}), and Responses (string or {type:function,name})
// shapes into a normalized kind plus forced-tool name. Unknown types are
// a translation failure — tool_choice is never silently dropped (FR-005).
// DecodeToolChoice 统一解析三种协议的工具选择表达，未知类型明确返回错误。
func DecodeToolChoice(raw json.RawMessage) (kind, name string, eErr *errclass.Error) {
	if !HasContent(raw) {
		return ToolChoiceAbsent, "", nil
	}
	var tc struct {
		// Type 协议对象或内容分部类型。
		Type     string `json:"type"`
		// Name 工具函数名。
		Name     string `json:"name"`
		// Function 工具函数定义或调用内容。
		Function struct {
			// Name 工具函数名。
			Name string `json:"name"`
		} `json:"function"`
	}
	if err := json.Unmarshal(raw, &tc); err != nil {
		if s := strings.TrimSpace(string(raw)); s == `"auto"` || s == `"any"` ||
			s == `"required"` || s == `"none"` {
			switch strings.Trim(s, `"`) {
			case "auto":
				return ToolChoiceAuto, "", nil
			case "any", "required":
				return ToolChoiceAny, "", nil
			default:
				return ToolChoiceNone, "", nil
			}
		}
		return "", "", errclass.Translation("malformed tool_choice: " + err.Error())
	}
	name = tc.Function.Name
	if name == "" {
		name = tc.Name
	}
	switch tc.Type {
	case "auto":
		return ToolChoiceAuto, "", nil
	case "any", "required":
		return ToolChoiceAny, "", nil
	case "none":
		return ToolChoiceNone, "", nil
	case "tool", "function":
		if name == "" {
			return "", "", errclass.Translation("malformed tool_choice: named tool requires a name")
		}
		return ToolChoiceNamed, name, nil
	}
	return "", "", errclass.Translation(fmt.Sprintf("unsupported tool_choice type %q", tc.Type))
}

// DecodeArgs parses tool-call arguments JSON; empty arguments decode to an
// empty object so every tool call carries valid input (FR-005/FR-006:
// tool calls are never silently dropped).
//
// Accepted ceiling: cross-format legs round-trip args through float64;
// integers > 2^53 lose precision — Responses passthrough preserves bytes.
// Upgrade path: carry json.RawMessage end-to-end.
// DecodeArgs 校验并解析工具参数，缺省参数归一化为空对象。
func DecodeArgs(args string) (any, *errclass.Error) {
	args = DefaultArgs(args)
	var v any
	if err := json.Unmarshal([]byte(args), &v); err != nil {
		return nil, errclass.Translation("malformed tool call arguments JSON: " + err.Error())
	}
	return v, nil
}

// ImageSource converts an image URL into the Anthropic image block
// source. Only data URLs whose media suffix ends in ";base64" decode to
// base64 sources; every other URL — including percent-encoded or plain
// data URLs — passes through verbatim as a url source so the encoding
// stays faithful and an upstream that cannot render it errors descriptively.
// ImageSource 转换图片 URL 为 Messages 图片源，仅解包明确声明的 base64 数据 URL。
func ImageSource(url string) (map[string]any, *errclass.Error) {
	if rest, ok := strings.CutPrefix(url, "data:"); ok {
		comma := strings.Index(rest, ",")
		if comma < 0 {
			return nil, errclass.Translation("malformed data URL: missing ',' separator")
		}
		if strings.HasSuffix(rest[:comma], ";base64") {
			media := strings.TrimSuffix(rest[:comma], ";base64")
			if media == "" {
				media = "application/octet-stream"
			}
			return map[string]any{
				"type": "image",
				"source": map[string]any{
					"type": "base64", "media_type": media, "data": rest[comma+1:],
				},
			}, nil
		}
	}
	return map[string]any{
		"type":   "image",
		"source": map[string]any{"type": "url", "url": url},
	}, nil
}

// ClaudeStopToFinish maps Anthropic Messages stop reasons to Chat
// Completions finish reasons (FR-006). One table serves the messages
// adapter's streaming and non-stream paths so they cannot diverge:
// refusal maps to content_filter, and unmapped reasons fall back to stop
// by the FR-006 best-effort policy. The pair with FinishToClaudeStop is
// a symmetric round-trip (refusal ↔ content_filter).
// ClaudeStopToFinish 将 Messages 停止原因映射为 Chat Completions 完成原因。
func ClaudeStopToFinish(stop string) string {
	switch stop {
	case "tool_use":
		return "tool_calls"
	case "max_tokens":
		return "length"
	case "refusal":
		return "content_filter"
	default: // end_turn, stop_sequence, unknown
		return "stop"
	}
}

// FinishToClaudeStop is the inverse of ClaudeStopToFinish: Chat
// Completions finish reasons to Messages stop reasons, served from this
// one shared table so the chatcompletions adapter's streaming and
// non-stream paths match it exactly. content_filter maps to refusal so a
// mapped reason round-trips; unmapped reasons fall back to end_turn by
// the FR-006 best-effort policy.
// FinishToClaudeStop 将 Chat Completions 完成原因映射为 Messages 停止原因。
func FinishToClaudeStop(finish string) string {
	switch finish {
	case "tool_calls":
		return "tool_use"
	case "length":
		return "max_tokens"
	case "content_filter":
		return "refusal"
	default: // stop, stop_sequence, unknown
		return "end_turn"
	}
}

// TerminalReason resolves a converted result's terminal stop/finish
// reason when two signals exist (FR-006): observed tool calls outrank the
// status-derived reason, so a terminal "incomplete" cannot downgrade
// tool_calls to length. toolReason/statusReason carry the target
// protocol's vocabulary ("tool_calls"/"length" for Chat Completions,
// "tool_use"/"max_tokens" for Messages). Responses-target STATUS fields
// are exempt — status derives positionally (truncation→incomplete) because
// tool calls are represented by output items, not status vocabulary;
// applies to CC finish_reason and Claude stop_reason vocabularies only.
// TerminalReason 优先保留工具调用信号，避免截断状态覆盖工具调用完成原因。
func TerminalReason(toolCallsSeen bool, toolReason, statusReason string) string {
	if toolCallsSeen {
		return toolReason
	}
	return statusReason
}

// ResponseStatusFromCCFinish maps a Chat Completions finish_reason into
// the Responses status vocabulary (FR-006): only "length" is truncation
// and maps to incomplete; every other reason — including unknown or
// absent values — completes the response. Shared by the non-streaming
// converter and the stream terminal so the mapping cannot diverge across
// the sibling routes (FR-005/FR-006 consistency).
// ResponseStatusFromCCFinish 将 Chat Completions 截断信号映射为 Responses 状态。
func ResponseStatusFromCCFinish(finish string) string {
	if finish == "length" {
		return "incomplete"
	}
	return "completed"
}

// ResponseStatusFromClaudeStop maps a Messages stop_reason into the
// Responses status vocabulary (FR-006): only "max_tokens" is truncation
// and maps to incomplete; every other reason — including unknown or
// absent values — completes the response. Shared by the non-streaming
// converter and the stream message_stop handler so the mapping cannot
// diverge across the sibling routes (FR-005/FR-006 consistency).
// ResponseStatusFromClaudeStop 将 Messages 截断信号映射为 Responses 状态。
func ResponseStatusFromClaudeStop(stop string) string {
	if stop == "max_tokens" {
		return "incomplete"
	}
	return "completed"
}

// CCFinishFromResponseStatus maps a Responses result status back to the
// Chat Completions finish_reason vocabulary (inverse of
// ResponseStatusFromCCFinish) so synthesized non-Responses output cannot
// drift from the forward mapping.
// CCFinishFromResponseStatus 将 Responses 状态反向映射为 Chat Completions 完成原因。
func CCFinishFromResponseStatus(status string) string {
	if status == "incomplete" {
		return "length"
	}
	return "stop"
}

// ClaudeStopFromResponseStatus maps a Responses result status back to the
// Anthropic stop_reason vocabulary (inverse of
// ResponseStatusFromClaudeStop) so synthesized non-Responses output cannot
// drift from the forward mapping.
// ClaudeStopFromResponseStatus 将 Responses 状态反向映射为 Messages 停止原因。
func ClaudeStopFromResponseStatus(status string) string {
	if status == "incomplete" {
		return "max_tokens"
	}
	return "end_turn"
}

// ClaudeMaxTokens applies the FR-005 max_tokens policy shared by every
// Claude-source converter: a positive value passes through, and an
// absent, zero, or negative value defaults to 4096 instead of failing
// translation or silently dropping the cap. One kernel serves all legs
// so identical Anthropic requests cannot diverge between rejecting,
// unbounding, and defaulting depending on the routed model.
// ClaudeMaxTokens 保留有效正数输出上限，缺省或无效值统一使用兼容默认上限。
func ClaudeMaxTokens(maxTokens int64) int64 {
	if maxTokens > 0 {
		return maxTokens
	}
	return 4096
}

// ClaudeSystemText renders the Anthropic Messages top-level system field
// (JSON string or text-block array) as instruction text (FR-005). Text
// blocks join with blank lines; non-text blocks are rejected descriptively
// rather than dropped. One kernel serves every adapter translating a
// Claude system field so separator and rejection policy cannot diverge.
// ClaudeSystemText 按顺序合并 Messages 系统文本，拒绝无法表达的非文本系统内容。
func ClaudeSystemText(raw json.RawMessage) (string, *errclass.Error) {
	if !HasContent(raw) {
		return "", nil
	}
	var s string
	if err := json.Unmarshal(raw, &s); err == nil {
		return s, nil
	}
	var blocks []struct {
		// Type 协议对象或内容分部类型。
		Type string `json:"type"`
		// Text 当前内容分部或摘要文本。
		Text string `json:"text"`
	}
	if err := json.Unmarshal(raw, &blocks); err != nil {
		return "", errclass.Translation("malformed system field")
	}
	var b strings.Builder
	for _, blk := range blocks {
		if blk.Type != "text" {
			return "", errclass.Translation(fmt.Sprintf("unsupported system block type %q", blk.Type))
		}
		if b.Len() > 0 {
			b.WriteString("\n\n")
		}
		b.WriteString(blk.Text)
	}
	return b.String(), nil
}

// FunctionCallOutputText flattens a Responses function_call_output.output
// field into the string both Chat Completions and Messages tool results
// already send upstream. The field is a JSON string or an array of text
// parts (input_text, output_text, or text). Non-text parts are rejected
// rather than dropped: function_call_output carries text only.
// FunctionCallOutputText 提取 Responses 工具结果的字符串或文本分部，拒绝非文本内容。
func FunctionCallOutputText(raw json.RawMessage) (string, *errclass.Error) {
	if !HasContent(raw) {
		return "", nil
	}
	var s string
	if err := json.Unmarshal(raw, &s); err == nil {
		return s, nil
	}
	var parts []struct {
		// Type 协议对象或内容分部类型。
		Type string `json:"type"`
		// Text 当前内容分部或摘要文本。
		Text string `json:"text"`
	}
	if err := json.Unmarshal(raw, &parts); err != nil {
		return "", errclass.Translation("function_call_output.output must be a string or an array of text parts: " + err.Error())
	}
	var b strings.Builder
	for _, p := range parts {
		switch p.Type {
		case "text", "input_text", "output_text":
			b.WriteString(p.Text)
		default:
			return "", errclass.Translation(fmt.Sprintf(
				"unsupported tool content part type %q; function_call_output carries text only", p.Type))
		}
	}
	return b.String(), nil
}

// ToolResultText flattens Claude tool_result content (JSON string or text
// block array) into one text string for the target protocol's tool-result
// field. Non-text blocks have no textual representation and are rejected
// descriptively rather than dropped (FR-005); targetNoun names the target
// wire vocabulary so each serving route keeps its own wording ("tool
// messages carry text only", "function_call_output carries text only").
// One kernel serves both adapters so the flatten and rejection policy
// cannot diverge again.
// ToolResultText 按目标工具结果字段提取文本，明确拒绝无法表示的非文本块。
func ToolResultText(raw json.RawMessage, targetNoun string) (string, *errclass.Error) {
	if !HasContent(raw) {
		return "", nil
	}
	var s string
	if err := json.Unmarshal(raw, &s); err == nil {
		return s, nil
	}
	var blocks []struct {
		// Type 协议对象或内容分部类型。
		Type string `json:"type"`
		// Text 当前内容分部或摘要文本。
		Text string `json:"text"`
	}
	if err := json.Unmarshal(raw, &blocks); err != nil {
		return "", errclass.Translation("tool_result content must be a string or an array of blocks")
	}
	var b strings.Builder
	for _, blk := range blocks {
		if blk.Type != "text" {
			return "", errclass.Translation(fmt.Sprintf(
				"unsupported tool_result block type %q; %s", blk.Type, targetNoun))
		}
		b.WriteString(blk.Text)
	}
	return b.String(), nil
}

// ClaudeImageURL converts an Anthropic image block source into an image
// URL for OpenAI-style targets, encoding base64 sources as data URLs
// (FR-005 multimodal preservation). Strict validation shared by every
// call site: url sources need a url, base64 sources need data, and
// unknown source types are named descriptively — never a silently
// corrupt data: URL.
// ClaudeImageURL 校验 Messages 图片源并转换为 URL 或 base64 数据 URL。
func ClaudeImageURL(srcType, url, mediaType, data string) (string, *errclass.Error) {
	switch srcType {
	case "url":
		if url == "" {
			return "", errclass.Translation("image source missing url")
		}
		return url, nil
	case "base64":
		if data == "" {
			return "", errclass.Translation("base64 image source missing data")
		}
		if mediaType == "" {
			mediaType = "application/octet-stream"
		}
		return "data:" + mediaType + ";base64," + data, nil
	default:
		return "", errclass.Translation(fmt.Sprintf("unsupported image source type %q", srcType))
	}
}

// SSEEvent renders one named SSE frame.
// SSEEvent 编码具名 SSE 事件，统一事件名和数据帧边界。
func SSEEvent(name string, v any) []byte {
	b, _ := json.Marshal(v) // composed marshallable types only; cannot fail
	out := make([]byte, 0, len("event: ")+len(name)+len("\ndata: ")+len(b)+2)
	out = append(out, "event: "...)
	out = append(out, name...)
	out = append(out, "\ndata: "...)
	out = append(out, b...)
	return append(out, '\n', '\n')
}

// IsSSEDone reports whether a framer-stripped SSE data payload is the
// terminal [DONE] sentinel; surrounding whitespace and CR are ignored.
// IsSSEDone 忽略外围空白后识别 SSE 终止标记。
func IsSSEDone(payload string) bool {
	return strings.TrimSpace(payload) == "[DONE]"
}

// ---- Chat Completions reasoning (thinking) normalization ----

// ReasoningDetail is one entry of a Chat Completions "reasoning_details"
// array — the vendor-specific structured form of thinking text.
type ReasoningDetail struct {
	// Text 当前内容分部或摘要文本。
	Text string `json:"text"`
}

// ReasoningFields decodes the spellings upstreams use for Chat Completions
// thinking text, embedded untagged so one decode serves the streaming delta
// and the non-streaming message. Some compatible upstreams return
// "reasoning" (string) plus "reasoning_details" (array of {text,...}) and
// never the standard "reasoning_content" that the host's OpenAI-to-Claude
// translator and most OpenAI clients read.
type ReasoningFields struct {
	// Reasoning 上游推理文本或请求推理控制。
	Reasoning        string            `json:"reasoning"`
	// ReasoningContent 供兼容客户端读取的推理文本字段。
	ReasoningContent string            `json:"reasoning_content"`
	// ReasoningDetails 上游结构化推理文本详情。
	ReasoningDetails []ReasoningDetail `json:"reasoning_details"`
}

// ReasoningText returns the thinking text carried by one message/delta and
// whether any is present. Priority: reasoning_details[].text (the
// authoritative per-token text; vendors mirror the plain string in it when
// both are present, so details-only avoids duplicating content), then
// "reasoning", then an already-standard "reasoning_content".
// ReasoningText 按详情、reasoning、reasoning_content 优先级提取推理文本并避免重复。
func (f *ReasoningFields) ReasoningText() (string, bool) {
	var parts []string
	for _, d := range f.ReasoningDetails {
		if strings.TrimSpace(d.Text) != "" {
			parts = append(parts, d.Text)
		}
	}
	if len(parts) > 0 {
		return strings.Join(parts, ""), true
	}
	if strings.TrimSpace(f.Reasoning) != "" {
		return f.Reasoning, true
	}
	if strings.TrimSpace(f.ReasoningContent) != "" {
		return f.ReasoningContent, true
	}
	return "", false
}

// BackfillReasoningContent copies vendor reasoning text onto the standard
// "reasoning_content" member of choices[].<field> (field is "delta" for
// streaming chunks, "message" for a full response) so consumers that only
// read the standard spelling still receive thinking text. It is the
// pass-through sibling of the adapters' own reasoning rendering, and the
// compatibility normalization applied to upstream responses.
//
// nil means "unchanged": bodies without any reasoning field, members that
// already carry non-empty reasoning_content, non-array choices, and invalid
// JSON are all reported as unchanged so callers forward the original bytes.
// BackfillReasoningContent 按需补齐通用推理字段，未变更时返回空值以保留原载荷。
func BackfillReasoningContent(body []byte, field string) []byte {
	if len(body) == 0 || !bytes.Contains(body, []byte("reasoning")) {
		return nil
	}
	if !gjson.ValidBytes(body) {
		return nil
	}
	choices := gjson.GetBytes(body, "choices")
	if !choices.IsArray() {
		return nil
	}
	out := body
	changed := false
	idx := -1
	choices.ForEach(func(_, choice gjson.Result) bool {
		idx++
		msg := choice.Get(field)
		if !msg.Exists() || !msg.IsObject() {
			return true
		}
		var fields ReasoningFields
		if err := json.Unmarshal([]byte(msg.Raw), &fields); err != nil {
			return true
		}
		text, ok := fields.ReasoningText()
		if !ok || strings.TrimSpace(fields.ReasoningContent) != "" {
			return true
		}
		updated, err := sjson.SetBytes(out, "choices."+strconv.Itoa(idx)+"."+field+".reasoning_content", text)
		if err != nil {
			return true
		}
		out = updated
		changed = true
		return true
	})
	if !changed {
		return nil
	}
	return out
}

// RespReasoningPart is one summary part of a Responses reasoning item.
type RespReasoningPart struct {
	// Type 协议对象或内容分部类型。
	Type string `json:"type"` // always "summary_text"
	// Text 当前内容分部或摘要文本。
	Text string `json:"text"`
}

// RespReasoningItem is the reasoning output item shape shared by the
// non-stream result and the stream terminal payload: encrypted_content is
// always present (host parity) and summary carries the observed
// summary_text part.
type RespReasoningItem struct {
	// ID 响应、内容块或工具调用的协议标识。
	ID               string              `json:"id"`
	// Type 协议对象或内容分部类型。
	Type             string              `json:"type"` // always "reasoning"
	// EncryptedContent 加密推理载荷，未提供时保持空值。
	EncryptedContent string              `json:"encrypted_content"`
	// Summary 推理摘要分部列表。
	Summary          []RespReasoningPart `json:"summary"`
}

// NewRespReasoningItem renders one reasoning output item carrying text as
// its single summary_text summary part.
// NewRespReasoningItem 构造带有单个摘要文本分部的 Responses 推理项。
func NewRespReasoningItem(id, text string) RespReasoningItem {
	return RespReasoningItem{
		ID:      id,
		Type:    "reasoning",
		Summary: []RespReasoningPart{{Type: "summary_text", Text: text}},
	}
}

// ReasoningItemInProgress renders the reasoning item announcement: the same
// identity and type as the terminal item, status in_progress, and no summary
// content yet (the part-added event follows).
// ReasoningItemInProgress 构造尚无摘要内容的推理项公布对象。
func ReasoningItemInProgress(id string) map[string]any {
	return map[string]any{"id": id, "type": "reasoning", "status": "in_progress", "summary": []any{}}
}

// ReasoningItemID derives a reasoning item identity from the response
// identity and choice index (host convention: rs_<response>_<choice>).
// ReasoningItemID 根据响应标识及候选索引生成稳定的推理项标识。
func ReasoningItemID(responseID string, choice int) string {
	return "rs_" + responseID + "_" + strconv.Itoa(choice)
}

// ClaudeThinkingBlock renders one Anthropic extended-thinking block. The
// signature member is intentionally omitted: vendor streams carry none, and
// historical thinking blocks are dropped by the claude->Chat Completions
// request policy, so nothing downstream validates it.
// ClaudeThinkingBlock 构造未签名思考块，不伪造上游未提供的签名。
func ClaudeThinkingBlock(text string) map[string]any {
	return map[string]any{"type": "thinking", "thinking": text}
}

// ChatChunkBuilder synthesizes chat.completion.chunk frames with one
// shared shape so every openai-target stream synthesizer renders
// identical envelopes (FR-006): role chunks carry role plus empty
// content, an empty finish renders null, and usage is attached only when
// provided. A zero Created defaults to render-time now.
type ChatChunkBuilder struct {
	// ID 响应、内容块或工具调用的协议标识。
	ID      string
	// Model 上游模型标识。
	Model   string
	// Created 响应创建时间戳，零值使用生成器当前时间。
	Created int64
}

// RoleChunk renders the leading assistant-role chunk.
// RoleChunk 构造助手角色前导增量。
func (b ChatChunkBuilder) RoleChunk() []byte {
	return b.frame(map[string]any{"role": "assistant", "content": ""}, nil, nil)
}

// Delta renders an intermediate delta chunk (null finish_reason).
// Delta 构造尚未终止的中间增量。
func (b ChatChunkBuilder) Delta(delta any) []byte {
	return b.frame(delta, nil, nil)
}

// Finish renders the terminal chunk; an empty finish renders null.
// Finish 构造最终完成增量并按需附加用量。
func (b ChatChunkBuilder) Finish(finish string, usage map[string]any) []byte {
	var fr any
	if finish != "" {
		fr = finish
	}
	return b.frame(map[string]any{}, fr, usage)
}

// frame 使用统一标识、时间和候选结构编码 Chat Completions 增量。
func (b ChatChunkBuilder) frame(delta, finish any, usage map[string]any) []byte {
	created := b.Created
	if created == 0 {
		created = time.Now().Unix()
	}
	frame := map[string]any{
		"id":      b.ID,
		"object":  "chat.completion.chunk",
		"created": created,
		"model":   b.Model,
		"choices": []any{map[string]any{"index": 0, "delta": delta, "finish_reason": finish}},
	}
	if usage != nil {
		frame["usage"] = usage
	}
	raw, _ := json.Marshal(frame)
	return raw
}

// ClaudeEventEmitter renders canonical Anthropic Messages SSE frames so
// every claude-target stream synthesizer emits byte-identical shapes
// regardless of serving route (FR-006): every frame carries a named
// event: line, message_start usage always includes output_tokens:0
// beside input_tokens, and message_delta omits stop_sequence entirely
// (a null member breaks event-line-dispatching clients). Native
// Anthropic frames carry no created/timestamp value, so none is stored.
type ClaudeEventEmitter struct {
	// id 响应或工具调用的稳定标识。
	id    string
	// model 上游模型标识。
	model string
}

// NewClaudeEventEmitter binds the emitter to the stream identity.
// NewClaudeEventEmitter 将 Messages 事件生成器绑定到流标识和模型。
func NewClaudeEventEmitter(id, model string) ClaudeEventEmitter {
	return ClaudeEventEmitter{id: id, model: model}
}

// MessageStart opens the stream with an empty content list.
// MessageStart 生成带空内容数组和初始输入用量的消息开始事件。
func (e ClaudeEventEmitter) MessageStart(inputTokens int64) []byte {
	return SSEEvent("message_start", map[string]any{
		"type": "message_start",
		"message": map[string]any{
			"id": e.id, "type": "message", "role": "assistant",
			"model": e.model, "content": []any{},
			"usage": map[string]any{"input_tokens": inputTokens, "output_tokens": 0},
		},
	})
}

// ContentBlockStart announces one content block; extra carries the
// block-type-specific fields (text, or id/name/input for tool_use).
// ContentBlockStart 公布内容块并合并块类型特有字段。
func (e ClaudeEventEmitter) ContentBlockStart(index int, blockType string, extra map[string]any) []byte {
	block := make(map[string]any, len(extra)+1)
	block["type"] = blockType
	for k, v := range extra {
		block[k] = v
	}
	return SSEEvent("content_block_start", map[string]any{
		"type": "content_block_start", "index": index, "content_block": block,
	})
}

// ContentBlockDelta streams one partial delta (text_delta or
// input_json_delta).
// ContentBlockDelta 生成指定内容块的文本或工具参数增量事件。
func (e ClaudeEventEmitter) ContentBlockDelta(index int, delta map[string]any) []byte {
	return SSEEvent("content_block_delta", map[string]any{
		"type": "content_block_delta", "index": index, "delta": delta,
	})
}

// ContentBlockStop closes one content block.
// ContentBlockStop 生成与已公布内容块对应的关闭事件。
func (e ClaudeEventEmitter) ContentBlockStop(index int) []byte {
	return SSEEvent("content_block_stop", map[string]any{
		"type": "content_block_stop", "index": index,
	})
}

// MessageDelta renders the terminal delta; a nil stopReason omits
// stop_reason, and stop_sequence is never emitted. A nil usage map omits
// usage; both serving routes always attach it.
// MessageDelta 生成终止增量，仅输出实际提供的停止原因和用量。
func (e ClaudeEventEmitter) MessageDelta(stopReason *string, usage map[string]any) []byte {
	delta := map[string]any{}
	if stopReason != nil {
		delta["stop_reason"] = *stopReason
	}
	payload := map[string]any{"type": "message_delta", "delta": delta}
	if usage != nil {
		payload["usage"] = usage
	}
	return SSEEvent("message_delta", payload)
}

// MessageStop terminates the stream.
// MessageStop 生成 Messages 消息终止事件。
func (e ClaudeEventEmitter) MessageStop() []byte {
	return SSEEvent("message_stop", map[string]any{"type": "message_stop"})
}

// ThinkingBlockStart opens the extended-thinking content block; the block
// carries an empty thinking string like the host's own OpenAI-to-Claude
// translator, and no signature (vendor thinking text is unsigned).
// ThinkingBlockStart 公布未签名且初始文本为空的思考内容块。
func (e ClaudeEventEmitter) ThinkingBlockStart(index int) []byte {
	return e.ContentBlockStart(index, "thinking", map[string]any{"thinking": ""})
}

// ThinkingDelta streams one partial thinking fragment.
// ThinkingDelta 生成指定思考内容块的文本增量。
func (e ClaudeEventEmitter) ThinkingDelta(index int, text string) []byte {
	return e.ContentBlockDelta(index, map[string]any{"type": "thinking_delta", "thinking": text})
}

// ResponsesEventEmitter renders canonical OpenAI Responses SSE frames so
// every openai-response stream synthesizer emits byte-identical shapes
// regardless of serving route (FR-006) — the sibling of
// ClaudeEventEmitter/ChatChunkBuilder. Native Responses responses carry a
// creation timestamp synthesizers never observe, so Created falls back to
// render-time now (same policy as ChatChunkBuilder.Created); the terminal
// completed payload always carries the model, matching the non-stream
// ResponsesResult shape.
type ResponsesEventEmitter struct {
	// ID 响应、内容块或工具调用的协议标识。
	ID    string // response identity carried by every event
	// Model 上游模型标识。
	Model string // model rendered on the terminal completed payload
}

// Created renders the leading response.created announcement.
// Created 生成 Responses 创建事件，先于项目增量公布响应标识。
func (e ResponsesEventEmitter) Created() []byte {
	return SSEEvent("response.created", map[string]any{
		"type": "response.created",
		"response": map[string]any{
			"id": e.ID, "object": "response", "created_at": time.Now().Unix(), "status": "in_progress",
		},
	})
}

// ItemAdded announces one output item before its first delta; item carries
// the block-type-specific fields (message id/role/content, or function_call
// id/call_id/name/arguments).
// ItemAdded 公布指定输出索引的项目，供后续增量稳定引用。
func (e ResponsesEventEmitter) ItemAdded(outputIndex int, item map[string]any) []byte {
	return SSEEvent("response.output_item.added", map[string]any{
		"type": "response.output_item.added", "output_index": outputIndex, "item": item,
	})
}

// TextDelta streams one partial output_text delta referencing the
// announced message item by item_id and output_index.
// TextDelta 生成引用已公布消息项的输出文本增量。
func (e ResponsesEventEmitter) TextDelta(itemID string, outputIndex int, delta string) []byte {
	return SSEEvent("response.output_text.delta", map[string]any{
		"type": "response.output_text.delta", "item_id": itemID, "output_index": outputIndex, "delta": delta,
	})
}

// ArgsDelta streams one partial function_call_arguments delta referencing
// the announced function_call item by item_id and output_index.
// ArgsDelta 生成引用已公布工具调用项的参数增量。
func (e ResponsesEventEmitter) ArgsDelta(itemID string, outputIndex int, delta string) []byte {
	return SSEEvent("response.function_call_arguments.delta", map[string]any{
		"type": "response.function_call_arguments.delta", "item_id": itemID, "output_index": outputIndex, "delta": delta,
	})
}

// ArgsDone closes one function_call argument stream with the complete
// arguments. Clients that finish a call only after this event otherwise keep
// the streamed fragments open.
// ArgsDone 使用完整参数关闭指定工具参数流。
func (e ResponsesEventEmitter) ArgsDone(itemID string, outputIndex int, arguments string) []byte {
	return SSEEvent("response.function_call_arguments.done", map[string]any{
		"type": "response.function_call_arguments.done", "item_id": itemID, "output_index": outputIndex, "arguments": arguments,
	})
}

// FunctionCallItemDone closes the announced function_call at the same
// output_index. The item id must use an fc_ prefix: clients that drop any
// other id otherwise leave the call unfinished or unbound.
// FunctionCallItemDone 在原输出索引关闭已公布的函数调用项。
func (e ResponsesEventEmitter) FunctionCallItemDone(outputIndex int, item RespItem) []byte {
	return SSEEvent("response.output_item.done", map[string]any{
		"type": "response.output_item.done", "output_index": outputIndex, "item": item,
	})
}

// CloseFunctionCall emits the argument-done and item-done pair that finishes
// one function_call before response.completed.
// CloseFunctionCall 依次生成工具参数完成和工具项目完成事件。
func (e ResponsesEventEmitter) CloseFunctionCall(outputIndex int, item RespItem) [][]byte {
	return [][]byte{
		e.ArgsDone(item.ID, outputIndex, item.Arguments),
		e.FunctionCallItemDone(outputIndex, item),
	}
}

// ReasoningItemAdded announces the reasoning item before any of its summary
// deltas, mirroring the host translator's lifecycle so clients that key off
// output_item transitions see one item per thinking run.
// ReasoningItemAdded 在摘要增量前公布稳定的推理项标识和索引。
func (e ResponsesEventEmitter) ReasoningItemAdded(itemID string, outputIndex int) []byte {
	return e.ItemAdded(outputIndex, ReasoningItemInProgress(itemID))
}

// ReasoningPartAdded opens the item's single summary part.
// ReasoningPartAdded 打开推理项的单个摘要分部。
func (e ResponsesEventEmitter) ReasoningPartAdded(itemID string, outputIndex int) []byte {
	return SSEEvent("response.reasoning_summary_part.added", map[string]any{
		"type":    "response.reasoning_summary_part.added",
		"item_id": itemID, "output_index": outputIndex, "summary_index": 0,
		"part": RespReasoningPart{Type: "summary_text"},
	})
}

// ReasoningSummaryDelta streams one partial reasoning summary fragment.
// ReasoningSummaryDelta 生成推理摘要文本分片。
func (e ResponsesEventEmitter) ReasoningSummaryDelta(itemID string, outputIndex int, delta string) []byte {
	return SSEEvent("response.reasoning_summary_text.delta", map[string]any{
		"type":    "response.reasoning_summary_text.delta",
		"item_id": itemID, "output_index": outputIndex, "summary_index": 0, "delta": delta,
	})
}

// ReasoningSummaryDone closes the summary text, ReasoningPartDone closes the
// summary part, and ReasoningItemDone closes the announced reasoning item
// with the aggregated text — the three terminal transitions of one thinking
// run, emitted together when text, tool calls, or the stream end arrive.
// ReasoningSummaryDone 使用累计文本关闭推理摘要文本流。
func (e ResponsesEventEmitter) ReasoningSummaryDone(itemID string, outputIndex int, text string) []byte {
	return SSEEvent("response.reasoning_summary_text.done", map[string]any{
		"type":    "response.reasoning_summary_text.done",
		"item_id": itemID, "output_index": outputIndex, "summary_index": 0, "text": text,
	})
}

// ReasoningPartDone closes the summary part with its aggregated text.
// ReasoningPartDone 使用累计摘要文本关闭摘要分部。
func (e ResponsesEventEmitter) ReasoningPartDone(itemID string, outputIndex int, text string) []byte {
	return SSEEvent("response.reasoning_summary_part.done", map[string]any{
		"type":    "response.reasoning_summary_part.done",
		"item_id": itemID, "output_index": outputIndex, "summary_index": 0,
		"part": RespReasoningPart{Type: "summary_text", Text: text},
	})
}

// ReasoningItemDone closes the reasoning item; the payload matches the
// terminal response.completed item (encrypted_content present, one
// summary_text part).
// ReasoningItemDone 以与最终输出一致的累计摘要关闭推理项。
func (e ResponsesEventEmitter) ReasoningItemDone(itemID string, outputIndex int, text string) []byte {
	return SSEEvent("response.output_item.done", map[string]any{
		"type": "response.output_item.done", "output_index": outputIndex,
		"item": NewRespReasoningItem(itemID, text),
	})
}

// Completed renders the terminal response.completed event: status from the
// route's status mapping, usage always attached (F-R6), and output items
// rendered by the caller through OutputAssembler.
// Completed 生成带最终状态、用量和有序输出项的完成事件。
func (e ResponsesEventEmitter) Completed(status string, usage ResponsesUsage, output []any) []byte {
	return SSEEvent("response.completed", map[string]any{
		"type": "response.completed",
		"response": map[string]any{
			"id": e.ID, "object": "response", "model": e.Model, "status": status,
			"usage": usage, "output": output,
		},
	})
}

// RedactedSnippet 先脱敏再按字符限制错误长度，保留常规上游错误说明且不切断中文。
func RedactedSnippet(s string) string {
	s = errclass.Redact(strings.ToValidUTF8(s, "�"))
	characters := []rune(s)
	if len(characters) > snippetBound {
		return string(characters[:snippetBound]) + "...（错误信息过长，已截断）"
	}
	return s
}

// snippetBound 限制上游错误正文的处理字节数和脱敏后展示字符数，避免无限回显。
const snippetBound = 4096

// UpstreamStatusError 限制错误正文处理长度后统一脱敏并分类，超长正文明确标注截断。
func UpstreamStatusError(status int, body []byte) *errclass.Error {
	truncated := len(body) > snippetBound
	if truncated {
		body = body[:snippetBound]
	}
	message := RedactedSnippet(string(body))
	if truncated {
		message += "...（错误信息过长，已截断）"
	}
	return errclass.FromStatus(status, message)
}

// ResponsesUsage is the token-usage block of synthesized Responses
// results and terminal response.completed payloads.
type ResponsesUsage struct {
	// InputTokens 输入令牌数。
	InputTokens   int64                   `json:"input_tokens"`
	// OutputTokens 输出令牌数。
	OutputTokens  int64                   `json:"output_tokens"`
	// TotalTokens 输入与输出令牌之和。
	TotalTokens   int64                   `json:"total_tokens"`
	// InputDetails 可选输入令牌详情。
	InputDetails  *ResponsesInputDetails  `json:"input_tokens_details,omitempty"`
	// OutputDetails 可选输出令牌详情。
	OutputDetails *ResponsesOutputDetails `json:"output_tokens_details,omitempty"`
}

type ResponsesInputDetails struct {
	// CachedTokens 可选缓存读取令牌数。
	CachedTokens     *int64 `json:"cached_tokens,omitempty"`
	// CacheWriteTokens 可选缓存写入令牌数。
	CacheWriteTokens *int64 `json:"cache_write_tokens,omitempty"`
}

type ResponsesOutputDetails struct {
	// ReasoningTokens 可选推理令牌数。
	ReasoningTokens *int64 `json:"reasoning_tokens,omitempty"`
}

// UsageDetails carries only fields that were present in the source usage.
// Pointer values preserve an explicit zero from an absent field.
type UsageDetails struct {
	// CachedTokens 可选缓存读取令牌数。
	CachedTokens     *int64
	// CacheWriteTokens 可选缓存写入令牌数。
	CacheWriteTokens *int64
	// ReasoningTokens 可选推理令牌数。
	ReasoningTokens  *int64
}

// ClampSubtract 扣除实际存在的缓存计数，并将结果下限限制为零。
func ClampSubtract(value int64, parts ...*int64) int64 {
	for _, part := range parts {
		if part != nil {
			value -= *part
		}
	}
	if value < 0 {
		return 0
	}
	return value
}

// NewResponsesUsageFrom builds the struct form of the Responses usage
// block with the majority sum rule: total_tokens is always input+output
// (FR-005/FR-006 consistency), never taken from or left zero beside
// nonzero counts. Shared by every adapter emitting a ResponsesUsage struct
// so the computation cannot diverge.
// NewResponsesUsageFrom 构造 Responses 用量，保持总量等于输入与输出之和。
func NewResponsesUsageFrom(inputTokens, outputTokens int64, details UsageDetails) ResponsesUsage {
	usage := ResponsesUsage{
		InputTokens:  inputTokens,
		OutputTokens: outputTokens,
		TotalTokens:  inputTokens + outputTokens,
	}
	if details.CachedTokens != nil || details.CacheWriteTokens != nil {
		usage.InputDetails = &ResponsesInputDetails{
			CachedTokens: details.CachedTokens, CacheWriteTokens: details.CacheWriteTokens,
		}
	}
	if details.ReasoningTokens != nil {
		usage.OutputDetails = &ResponsesOutputDetails{ReasoningTokens: details.ReasoningTokens}
	}
	return usage
}

// CCUsageFrom renders the Chat Completions usage map with total_tokens
// computed as prompt+completion (FR-005/FR-006 consistency): the same
// majority sum rule as NewResponsesUsageFrom applied to the Chat
// Completions vocabulary. Shared by every adapter emitting a Chat
// Completions usage block so the computation cannot diverge.
// CCUsageFrom 构造 Chat Completions 用量，仅保留上游实际提供的详情。
func CCUsageFrom(promptTokens, completionTokens int64, details UsageDetails) map[string]any {
	usage := map[string]any{
		"prompt_tokens":     promptTokens,
		"completion_tokens": completionTokens,
		"total_tokens":      promptTokens + completionTokens,
	}
	if details.CachedTokens != nil {
		usage["prompt_tokens_details"] = map[string]any{"cached_tokens": *details.CachedTokens}
	}
	if details.ReasoningTokens != nil {
		usage["completion_tokens_details"] = map[string]any{"reasoning_tokens": *details.ReasoningTokens}
	}
	return usage
}

// CCToolCallOpeningEntry renders the canonical OPENING tool_calls delta
// entry for a Chat Completions stream: OpenAI puts type on the FIRST
// delta entry only, so it carries index/id/type plus the function name
// and initial arguments fragment; continuation deltas for the same call
// are minimal ({index, function:{arguments}}) and must be built by the
// caller, not through this kernel.
// CCToolCallOpeningEntry 构造首次工具增量，携带类型、标识、函数名及初始参数。
func CCToolCallOpeningEntry(index int, callID, name, argsDelta string) map[string]any {
	return map[string]any{
		"index": index,
		"id":    callID,
		"type":  "function",
		"function": map[string]any{
			"name":      name,
			"arguments": argsDelta,
		},
	}
}

// CompletionEnvelope renders the top-level chat.completion envelope so
// every cross-format converter emits an identical shape (FR-006): one
// choice entry plus prompt/completion usage.
// CompletionEnvelope 构造共用的非流式 Chat Completions 响应外壳。
func CompletionEnvelope(id, model string, created int64, choices []map[string]any, usage map[string]any) map[string]any {
	return map[string]any{
		"id":      id,
		"object":  "chat.completion",
		"created": created,
		"model":   model,
		"choices": choices,
		"usage":   usage,
	}
}

// NewClaudeResult renders the non-stream Anthropic Messages response
// envelope so every claude-target converter emits an identical shape
// regardless of serving route (FR-006) — the Messages sibling of
// CompletionEnvelope/ResponsesResult. One policy per field: content is
// always a JSON array (nil normalizes to []), usage always carries both
// counters, and stop_sequence is never emitted — neither cross-format
// source vocabulary (CC finish_reason, Responses status) carries a stop
// sequence, so the key stays absent instead of null, matching the
// streaming message_delta sibling that omits it entirely. Callers pass
// blocks whose builders already drop empty text parts, so no dead text
// block ships.
// NewClaudeResult 构造统一 Messages 响应，归一化空内容且不伪造停止序列。
func NewClaudeResult(id, model, stopReason string, inputTokens, outputTokens int64, cacheRead, cacheCreation *int64, content []map[string]any) map[string]any {
	if content == nil {
		content = []map[string]any{}
	}
	return map[string]any{
		"id":          id,
		"type":        "message",
		"role":        "assistant",
		"model":       model,
		"content":     content,
		"stop_reason": stopReason,
		"usage":       ClaudeUsage(inputTokens, outputTokens, cacheRead, cacheCreation),
	}
}

// ClaudeUsage 构造 Messages 用量，仅在源数据存在时附加缓存计数。
func ClaudeUsage(input, output int64, cacheRead, cacheWrite *int64) map[string]any {
	usage := map[string]any{"input_tokens": input, "output_tokens": output}
	if cacheRead != nil {
		usage["cache_read_input_tokens"] = *cacheRead
	}
	if cacheWrite != nil {
		usage["cache_creation_input_tokens"] = *cacheWrite
	}
	return usage
}

// ResponsesResult is the non-stream Responses result body synthesized
// for openai-response clients; one shared shape keeps the Chat
// Completions and Messages routes byte-identical in structure
// (including the model field).
type ResponsesResult struct {
	// ID 响应、内容块或工具调用的协议标识。
	ID     string         `json:"id"`
	// Object 协议对象名称。
	Object string         `json:"object"`
	// Model 上游模型标识。
	Model  string         `json:"model"`
	// Status 响应完成或截断状态。
	Status string         `json:"status"`
	// Output 输出项列表或工具结果原始载荷。
	Output []any          `json:"output"`
	// Usage 令牌用量及可选缓存详情。
	Usage  ResponsesUsage `json:"usage"`
}

// RespTool is one Responses function tool.
type RespTool struct {
	// Type 协议对象或内容分部类型。
	Type        string          `json:"type"` // always "function"
	// Name 工具函数名。
	Name        string          `json:"name"`
	// Description 工具功能说明。
	Description string          `json:"description,omitempty"`
	// Parameters 工具参数的 JSON Schema。
	Parameters  json.RawMessage `json:"parameters,omitempty"`
}

// ResponsesRequest decodes an inbound OpenAI Responses request body
// (superset of what each adapter reads). Instructions is a JSON string,
// Input a JSON string or item array.
type ResponsesRequest struct {
	// Instructions Responses 系统指令原始载荷。
	Instructions      json.RawMessage `json:"instructions"`
	// Input 输入项目或工具参数载荷。
	Input             json.RawMessage `json:"input"`
	// MaxOutputTokens Responses 输出令牌上限。
	MaxOutputTokens   *int64          `json:"max_output_tokens"`
	// Tools 可调用的函数工具定义。
	Tools             []RespTool      `json:"tools"`
	// ToolChoice 工具选择策略的协议表达。
	ToolChoice        json.RawMessage `json:"tool_choice"`
	// ParallelToolCalls 可选并行工具调用约束。
	ParallelToolCalls *bool           `json:"parallel_tool_calls"`
	// Stream 是否请求流式输出。
	Stream            bool            `json:"stream"`
	// Temperature 可选温度采样控制。
	Temperature       *float64        `json:"temperature"`
	// TopP 可选核采样控制。
	TopP              *float64        `json:"top_p"`
	// Reasoning 上游推理文本或请求推理控制。
	Reasoning         *struct {
		// Effort 推理强度等级。
		Effort string `json:"effort"`
	} `json:"reasoning"`
}

// DecodeInstructions resolves the instructions field (FR-005): absent or
// null yields "", a JSON string decodes to its value, and any other
// shape is a translation failure. One kernel serves both Responses-source
// builders so the wording and policy cannot diverge again.
// DecodeInstructions 解析可选指令字符串，其他形状返回转换错误。
func (r *ResponsesRequest) DecodeInstructions() (string, *errclass.Error) {
	if !HasContent(r.Instructions) {
		return "", nil
	}
	var instr string
	if err := json.Unmarshal(r.Instructions, &instr); err != nil {
		return "", errclass.Translation("instructions must be a string")
	}
	return instr, nil
}

// DecodeInputItems decodes the input field into item form: an item array
// passes through, a plain JSON string surfaces as one synthesized
// user-message item carrying the raw string bytes — each builder's
// message pipeline renders it exactly as its former inline string branch
// did, including dropping the empty string — and absent or null decodes
// to zero items. Any other shape is a translation failure. One kernel owns
// the string-or-items discrimination for both Responses-source builders so
// it cannot diverge again.
// DecodeInputItems 将字符串输入或项目数组统一解析为 Responses 项目列表。
func (r *ResponsesRequest) DecodeInputItems() ([]RespItem, *errclass.Error) {
	if !HasContent(r.Input) {
		return nil, nil
	}
	var s string
	if err := json.Unmarshal(r.Input, &s); err == nil {
		return []RespItem{{Type: "message", Role: "user", Content: r.Input}}, nil
	}
	var items []RespItem
	if err := json.Unmarshal(r.Input, &items); err != nil {
		return nil, errclass.Translation("input must be a string or an array of items: " + err.Error())
	}
	return items, nil
}

// RespItem is one Responses input/output item (message, function_call,
// or function_call_output). ID carries the item identity of synthesized
// terminal message items (FR-006 sibling parity: the Chat Completions
// and Messages routes emit the same "id" the streaming
// response.output_item.added announcement used). Function-call items also
// carry an fc_-prefixed id so clients that drop any other id can still bind
// the call.
type RespItem struct {
	// Type 协议对象或内容分部类型。
	Type      string          `json:"type"`
	// ID 响应、内容块或工具调用的协议标识。
	ID        string          `json:"id,omitempty"`
	// Role 消息角色。
	Role      string          `json:"role,omitempty"`
	// Content 原始或归一化的消息内容。
	Content   json.RawMessage `json:"content,omitempty"`
	// CallID 函数调用标识，用于关联工具结果。
	CallID    string          `json:"call_id,omitempty"`
	// Name 工具函数名。
	Name      string          `json:"name,omitempty"`
	// Arguments 函数调用参数的 JSON 字符串。
	Arguments string          `json:"arguments,omitempty"`
	// Output is a JSON string or an array of text parts. Kept raw so either
	// shape decodes; FunctionCallOutputText flattens it for upstream.
	// Output 输出项列表或工具结果原始载荷。
	Output  json.RawMessage `json:"output,omitempty"`
	// Summary 推理摘要分部列表。
	Summary []struct {
		// Text 当前内容分部或摘要文本。
		Text string `json:"text"`
	} `json:"summary,omitempty"`
}

// CCFunction is one Chat Completions tool function definition (decode and
// emit; omitempty keeps absent descriptions/parameters off the wire).
type CCFunction struct {
	// Name 工具函数名。
	Name        string          `json:"name"`
	// Description 工具功能说明。
	Description string          `json:"description,omitempty"`
	// Parameters 工具参数的 JSON Schema。
	Parameters  json.RawMessage `json:"parameters,omitempty"`
}

// CCToolCall is one Chat Completions tool call (decode and emit).
type CCToolCall struct {
	// ID 响应、内容块或工具调用的协议标识。
	ID       string `json:"id"`
	// Type 协议对象或内容分部类型。
	Type     string `json:"type"`
	// Function 工具函数定义或调用内容。
	Function struct {
		// Name 工具函数名。
		Name      string `json:"name"`
		// Arguments 函数调用参数的 JSON 字符串。
		Arguments string `json:"arguments"`
	} `json:"function"`
}

// CCMessage is one Chat Completions message.
type CCMessage struct {
	// Role 消息角色。
	Role       string          `json:"role"`
	// Content 原始或归一化的消息内容。
	Content    json.RawMessage `json:"content"` // string or typed parts
	// ToolCalls 消息携带的函数调用列表。
	ToolCalls  []CCToolCall    `json:"tool_calls"`
	// ToolCallID 工具结果关联的调用标识。
	ToolCallID string          `json:"tool_call_id"`
}

// CCTool is one Chat Completions tool definition.
type CCTool struct {
	// Type 协议对象或内容分部类型。
	Type     string     `json:"type"`
	// Function 工具函数定义或调用内容。
	Function CCFunction `json:"function"`
}

// ChatCompletionsRequest decodes an inbound Chat Completions request
// (superset of what each adapter reads).
type ChatCompletionsRequest struct {
	// Messages 按原顺序保存的消息列表。
	Messages            []CCMessage     `json:"messages"`
	// MaxTokens 输出令牌上限。
	MaxTokens           *int64          `json:"max_tokens"`
	// MaxCompletionTokens Chat Completions 新版输出令牌上限。
	MaxCompletionTokens *int64          `json:"max_completion_tokens"`
	// Stop 停止字符串或停止序列。
	Stop                json.RawMessage `json:"stop"`
	// Tools 可调用的函数工具定义。
	Tools               []CCTool        `json:"tools"`
	// ToolChoice 工具选择策略的协议表达。
	ToolChoice          json.RawMessage `json:"tool_choice"`
	// ParallelToolCalls 可选并行工具调用约束。
	ParallelToolCalls   *bool           `json:"parallel_tool_calls"`
	// Stream 是否请求流式输出。
	Stream              bool            `json:"stream"`
	// Temperature 可选温度采样控制。
	Temperature         *float64        `json:"temperature"`
	// TopP 可选核采样控制。
	TopP                *float64        `json:"top_p"`
	// ReasoningEffort 推理强度控制。
	ReasoningEffort     string          `json:"reasoning_effort"`
}

// JoinTexts concatenates the string values stored under "text" across
// content parts/blocks in order, skipping non-string members — one kernel
// for every adapter that flattens typed parts into plain text (FR-005).
// JoinTexts 按内容顺序连接文本字段，跳过非字符串值。
func JoinTexts(parts []map[string]any) string {
	var b strings.Builder
	for _, p := range parts {
		if t, ok := p["text"].(string); ok {
			b.WriteString(t)
		}
	}
	return b.String()
}

// ContentPart is one OpenAI-style content part decoded by
// DecodeStringOrParts: a text part (ImageURL empty) or an image part
// carrying its resolved URL.
type ContentPart struct {
	// Text 当前内容分部或摘要文本。
	Text     string
	// ImageURL 图片地址或图片地址对象。
	ImageURL string // resolved image URL; empty for text parts
}

// DecodeStringOrParts is THE shared string-or-parts kernel owning the
// empty-input policy for every target route (F5 route parity): an absent
// or null field, an empty JSON string, and empty-text parts all decode to
// zero parts, so identical inbound bodies drop identically instead of one
// route shipping dead parts upstream while siblings drop the message.
// Unknown part types are ClassUnsupported naming targetLabel; malformed
// shapes are ClassTranslation. One kernel serves responses/contentParts,
// messages/contentParts, and chatcompletions/respContent so the policy
// cannot diverge again.
// DecodeStringOrParts 统一解析字符串或文本图片分部，并应用一致的空内容策略。
func DecodeStringOrParts(raw json.RawMessage, targetLabel string) ([]ContentPart, *errclass.Error) {
	if !HasContent(raw) {
		return nil, nil
	}
	var s string
	if err := json.Unmarshal(raw, &s); err == nil {
		if s == "" {
			return nil, nil
		}
		return []ContentPart{{Text: s}}, nil
	}
	var parts []struct {
		// Type 协议对象或内容分部类型。
		Type     string          `json:"type"`
		// Text 当前内容分部或摘要文本。
		Text     string          `json:"text"`
		// ImageURL 图片地址或图片地址对象。
		ImageURL json.RawMessage `json:"image_url"`
	}
	if err := json.Unmarshal(raw, &parts); err != nil {
		return nil, errclass.Translation("malformed message content: " + err.Error())
	}
	var out []ContentPart
	for _, p := range parts {
		if !IsContentPartType(p.Type) {
			return nil, ValidateContentPart(p.Type, targetLabel)
		}
		if p.Type == "image_url" || p.Type == "input_image" {
			url, eErr := OpenAIImageURL(p.ImageURL)
			if eErr != nil {
				return nil, eErr
			}
			out = append(out, ContentPart{ImageURL: url})
			continue
		}
		if p.Text != "" { // uniform DROP policy: no dead text parts upstream
			out = append(out, ContentPart{Text: p.Text})
		}
	}
	return out, nil
}

// ClaudeBlocksFromOpenAI normalizes an OpenAI-style content field (JSON
// string or typed parts) into Anthropic content blocks preserving
// per-part order (FR-005 multimodal preservation): text parts become
// text blocks, image parts image sources. THE Claude-blocks kernel for
// every route emitting Anthropic blocks from openai-style content: it
// owns validation and the empty-input policy — empties dropped uniformly
// on every route (F5 parity), unknown types rejected descriptively
// naming target. One kernel serves messages/contentParts and
// chatcompletions/ccContentToBlocks so the policy cannot diverge again.
// ClaudeBlocksFromOpenAI 按原顺序将 OpenAI 风格文本图片内容转换为 Messages 块。
func ClaudeBlocksFromOpenAI(raw json.RawMessage, target string) ([]map[string]any, *errclass.Error) {
	parts, eErr := DecodeStringOrParts(raw, target)
	if eErr != nil {
		return nil, eErr
	}
	var blocks []map[string]any
	for _, p := range parts {
		if p.ImageURL != "" {
			src, eErr := ImageSource(p.ImageURL)
			if eErr != nil {
				return nil, eErr
			}
			blocks = append(blocks, src)
			continue
		}
		blocks = append(blocks, map[string]any{"type": "text", "text": p.Text})
	}
	return blocks, nil
}

// ToolResultErrorPrefix marks tool results that arrived with is_error set;
// every Claude-source translator applies it so the flag is never silently
// erased (FR-005).
const ToolResultErrorPrefix = "[error] "

// ClaudeTool is one Anthropic Messages tool definition (decode-only).
type ClaudeTool struct {
	// Name 工具函数名。
	Name        string          `json:"name"`
	// Description 工具功能说明。
	Description string          `json:"description"`
	// InputSchema 工具参数的 JSON Schema。
	InputSchema json.RawMessage `json:"input_schema"`
}

// ClaudeThinking is the Anthropic extended-thinking request control
// (decode-only).
type ClaudeThinking struct {
	// Type 协议对象或内容分部类型。
	Type         string `json:"type"`
	// BudgetTokens 扩展思考预算令牌数。
	BudgetTokens int64  `json:"budget_tokens"`
}

// ThinkingEnabled reports whether an Anthropic thinking control requests
// extended thinking: present with type "enabled" (FR-005). One kernel
// serves every adapter gating on the control so the check cannot diverge.
// ThinkingEnabled 判断推理控制是否显式启用扩展思考。
func ThinkingEnabled(t *ClaudeThinking) bool {
	return t != nil && t.Type == "enabled"
}

// ClaudeBlock is one normalized Anthropic Messages content block produced
// by DecodeClaudeMessages: Kind carries the wire "type" and only the
// fields that kind uses are populated. Image URLs arrive pre-resolved
// through ClaudeImageURL; tool_use Input and tool_result Result stay raw
// so each target renderer applies its own vocabulary and policies.
type ClaudeBlock struct {
	// Kind 归一化内容块类型。
	Kind    string          // text, image, tool_use, tool_result, thinking, redacted_thinking, or an unknown wire type
	// Text 当前内容分部或摘要文本。
	Text    string          // text blocks
	// URL 图片地址或数据 URL。
	URL     string          // image blocks: resolved http(s)/data URL
	// CallID 函数调用标识，用于关联工具结果。
	CallID  string          // tool_use "id" / tool_result "tool_use_id"
	// Name 工具函数名。
	Name    string          // tool_use name
	// Input 输入项目或工具参数载荷。
	Input   json.RawMessage // tool_use arguments (absent → nil)
	// Result 工具结果原始载荷。
	Result  json.RawMessage // tool_result payload (string or block array)
	// IsError 工具结果是否显式标记失败。
	IsError bool            // tool_result is_error flag
}

// ClaudeMessageRecord is one Anthropic Messages message turn: Content is
// set when the wire content was a JSON string, Blocks when it was a block
// array; absent/null content sets neither.
type ClaudeMessageRecord struct {
	// Role 消息角色。
	Role    string
	// Content 原始或归一化的消息内容。
	Content string
	// Blocks 归一化消息内容块。
	Blocks  []ClaudeBlock
}

// ClaudeRequestRecord is the normalized decode of an inbound Anthropic
// Messages request body (FR-005): envelope fields resolved once — max
// tokens defaulted per ClaudeMaxTokens, system flattened per
// ClaudeSystemText, tool_choice classified per DecodeToolChoice — so
// every Claude-source translator decodes identically and keeps only
// target-shape rendering local.
type ClaudeRequestRecord struct {
	// MaxTokens 输出令牌上限。
	MaxTokens      int64
	// System 系统指令内容。
	System         string
	// Messages 按原顺序保存的消息列表。
	Messages       []ClaudeMessageRecord
	// StopSequences 停止序列列表。
	StopSequences  []string
	// Tools 可调用的函数工具定义。
	Tools          []ClaudeTool
	// ToolChoiceKind 标准化工具选择策略。
	ToolChoiceKind string
	// ToolChoiceName 强制选择的工具函数名。
	ToolChoiceName string
	// Thinking 推理控制或思考内容。
	Thinking       *ClaudeThinking
	// Stream 是否请求流式输出。
	Stream         bool
	// Temperature 可选温度采样控制。
	Temperature    *float64
	// TopP 可选核采样控制。
	TopP           *float64
}

// claudeWireMessage mirrors the inbound Anthropic message shape (decode-only).
type claudeWireMessage struct {
	// Role 消息角色。
	Role    string          `json:"role"`
	// Content 原始或归一化的消息内容。
	Content json.RawMessage `json:"content"` // string or block array
}

// claudeWireBlock mirrors the inbound Anthropic block shape (decode-only).
type claudeWireBlock struct {
	// Type 协议对象或内容分部类型。
	Type   string `json:"type"`
	// Text 当前内容分部或摘要文本。
	Text   string `json:"text"`
	// Source Messages 图片源描述。
	Source *struct {
		// Type 协议对象或内容分部类型。
		Type      string `json:"type"` // base64 | url
		// MediaType 图片媒体类型。
		MediaType string `json:"media_type"`
		// Data base64 图片数据。
		Data      string `json:"data"`
		// URL 图片地址或数据 URL。
		URL       string `json:"url"`
	} `json:"source"`
	// ID 响应、内容块或工具调用的协议标识。
	ID        string          `json:"id"`
	// Name 工具函数名。
	Name      string          `json:"name"`
	// Input 输入项目或工具参数载荷。
	Input     json.RawMessage `json:"input"`
	// ToolUseID 工具结果引用的工具使用标识。
	ToolUseID string          `json:"tool_use_id"`
	// Content 原始或归一化的消息内容。
	Content   json.RawMessage `json:"content"` // tool_result payload: string or blocks
	// IsError 工具结果是否显式标记失败。
	IsError   bool            `json:"is_error"`
}

// DecodeClaudeMessages decodes an inbound Anthropic Messages request body
// into a ClaudeRequestRecord. It is THE Claude-request kernel for every
// adapter translating claude source (FR-005): one decoder owns malformed-
// input classification (all ClassTranslation) and wording — broken JSON,
// non-string/non-array message content, undecodable block elements, image
// sources (via ClaudeImageURL validation) — so the sibling routes cannot
// diverge again. Unknown block types and roles are recorded verbatim:
// rejecting them names the TARGET endpoint and stays with the renderer.
// DecodeClaudeMessages 统一解析 Messages 请求、工具选择、系统字段及内容块。
func DecodeClaudeMessages(body json.RawMessage) (*ClaudeRequestRecord, *errclass.Error) {
	var env struct {
		// MaxTokens 输出令牌上限。
		MaxTokens     int64               `json:"max_tokens"`
		// System 系统指令内容。
		System        json.RawMessage     `json:"system"`
		// Messages 按原顺序保存的消息列表。
		Messages      []claudeWireMessage `json:"messages"`
		// StopSequences 停止序列列表。
		StopSequences []string            `json:"stop_sequences"`
		// Tools 可调用的函数工具定义。
		Tools         []ClaudeTool        `json:"tools"`
		// ToolChoice 工具选择策略的协议表达。
		ToolChoice    json.RawMessage     `json:"tool_choice"`
		// Thinking 推理控制或思考内容。
		Thinking      *ClaudeThinking     `json:"thinking"`
		// Stream 是否请求流式输出。
		Stream        bool                `json:"stream"`
		// Temperature 可选温度采样控制。
		Temperature   *float64            `json:"temperature"`
		// TopP 可选核采样控制。
		TopP          *float64            `json:"top_p"`
	}
	if err := json.Unmarshal(body, &env); err != nil {
		return nil, errclass.Translation("malformed claude request JSON: " + err.Error())
	}
	kind, name, eErr := DecodeToolChoice(env.ToolChoice)
	if eErr != nil {
		return nil, eErr
	}
	system, eErr := ClaudeSystemText(env.System)
	if eErr != nil {
		return nil, eErr
	}
	rec := &ClaudeRequestRecord{
		MaxTokens:      ClaudeMaxTokens(env.MaxTokens),
		System:         system,
		StopSequences:  env.StopSequences,
		Tools:          env.Tools,
		ToolChoiceKind: kind,
		ToolChoiceName: name,
		Thinking:       env.Thinking,
		Stream:         env.Stream,
		Temperature:    env.Temperature,
		TopP:           env.TopP,
	}
	for _, wm := range env.Messages {
		m := ClaudeMessageRecord{Role: wm.Role}
		if HasContent(wm.Content) {
			var s string
			if json.Unmarshal(wm.Content, &s) == nil {
				m.Content = s
			} else {
				blocks, eErr := decodeClaudeBlocks(wm.Content)
				if eErr != nil {
					return nil, eErr
				}
				m.Blocks = blocks
			}
		}
		rec.Messages = append(rec.Messages, m)
	}
	return rec, nil
}

// decodeClaudeBlocks normalizes one block-array message content field.
// A well-formed JSON value that is neither string nor array fails with
// the canonical shape error; an undecodable element fails with the
// canonical block error — never silently dropped (FR-005).
// decodeClaudeBlocks 校验并归一化内容块，保留未知类型交由目标渲染器处理。
func decodeClaudeBlocks(raw json.RawMessage) ([]ClaudeBlock, *errclass.Error) {
	var elems []json.RawMessage
	if err := json.Unmarshal(raw, &elems); err != nil {
		return nil, errclass.Translation("message content must be a string or an array of blocks")
	}
	blocks := make([]ClaudeBlock, 0, len(elems))
	for _, elem := range elems {
		var wb claudeWireBlock
		if err := json.Unmarshal(elem, &wb); err != nil {
			return nil, errclass.Translation("malformed content block")
		}
		blk := ClaudeBlock{
			Kind: wb.Type, Text: wb.Text,
			Name: wb.Name, Input: wb.Input,
			Result: wb.Content, IsError: wb.IsError,
		}
		switch wb.Type {
		case "image":
			if wb.Source == nil {
				return nil, errclass.Translation("image block missing source")
			}
			url, eErr := ClaudeImageURL(wb.Source.Type, wb.Source.URL, wb.Source.MediaType, wb.Source.Data)
			if eErr != nil {
				return nil, eErr
			}
			blk.URL = url
		case "tool_use":
			blk.CallID = wb.ID
		case "tool_result":
			blk.CallID = wb.ToolUseID
		}
		blocks = append(blocks, blk)
	}
	return blocks, nil
}

// OpenAIImageURL extracts the image URL from an OpenAI-style content part:
// {"url": ...} object form or plain string form. An empty or absent URL is
// a translation failure.
// OpenAIImageURL 从字符串或对象形式提取图片 URL，并拒绝缺失或空 URL。
func OpenAIImageURL(raw json.RawMessage) (string, *errclass.Error) {
	var url string
	if len(raw) > 0 {
		var obj struct {
			// URL 图片地址或数据 URL。
			URL string `json:"url"`
		}
		if json.Unmarshal(raw, &obj) == nil && obj.URL != "" {
			return obj.URL, nil
		}
		json.Unmarshal(raw, &url)
	}
	if url == "" {
		return "", errclass.Translation("image content part missing url")
	}
	return url, nil
}

// UnsupportedFormat reports a source protocol no translator in the calling
// adapter handles, naming the target endpoint.
// UnsupportedFormat 构造带目标端点信息的不支持来源协议错误。
func UnsupportedFormat(format, target string) *errclass.Error {
	return &errclass.Error{
		Class:   errclass.ClassUnsupported,
		Message: fmt.Sprintf("source protocol %q cannot be translated to %s", format, target),
	}
}

// UnsupportedPartType reports a client content-block/part type the target
// endpoint cannot represent (ClassUnsupported; FR-005 explicit policy).
// UnsupportedPartType 明确报告目标端点不能表示的内容块类型。
func UnsupportedPartType(partType, target string) *errclass.Error {
	return &errclass.Error{
		Class:   errclass.ClassUnsupported,
		Message: fmt.Sprintf("unsupported content block type %q for %s", partType, target),
	}
}

// NormalizeResponsesItemType implements the OpenAI Responses compact-item
// default: an item with no type but a role or content is a message.
// Completely empty items (no type, role, or content) return "" so callers
// can skip them instead of failing the request.
// NormalizeResponsesItemType 为紧凑输入项推导消息类型，空项可忽略。
func NormalizeResponsesItemType(item RespItem) string {
	t := strings.TrimSpace(item.Type)
	if t != "" {
		return t
	}
	if item.Role != "" || HasContent(item.Content) {
		return "message"
	}
	return ""
}

// UnsupportedInputItemType reports an unrecognized Responses input item type
// (ClassUnsupported; FR-005 explicit policy). HTTP 400 so the host does not
// treat a client-body problem as a retryable upstream failure and burn the
// rest of the key pool.
// UnsupportedInputItemType 报告不支持的 Responses 输入项并标记为客户端错误。
func UnsupportedInputItemType(itemType string) *errclass.Error {
	return &errclass.Error{
		Class:      errclass.ClassUnsupported,
		Message:    fmt.Sprintf("unsupported Responses input item type %q", itemType),
		StatusCode: 400,
	}
}

// IsContentPartType reports whether an OpenAI-style content part type is
// accepted, covering both Chat Completions names (text, image_url) and
// their Responses-flavored aliases (input_text, output_text, input_image)
// so identical client bodies succeed on every route.
// IsContentPartType 判断是否为受支持的文本、图片分部或协议别名。
func IsContentPartType(partType string) bool {
	switch partType {
	case "text", "input_text", "output_text", "image_url", "input_image":
		return true
	}
	return false
}

// ValidateContentPart rejects an unknown OpenAI-style content part type
// as unsupported_protocol_or_parameter (FR-009; round-9 precedent:
// unsupported parameter values are unsupported, not malformed JSON),
// naming the target endpoint. One kernel serves all three adapters so
// the error class cannot diverge again.
// ValidateContentPart 为未知内容分部生成统一不支持参数错误。
func ValidateContentPart(partType, target string) *errclass.Error {
	return &errclass.Error{
		Class:   errclass.ClassUnsupported,
		Message: fmt.Sprintf("unsupported content part type %q for %s", partType, target),
	}
}

// ValidateRole rejects a message role no translator in the calling
// adapter handles, same FR-009 class and rationale as ValidateContentPart.
// ValidateRole 为目标端点不能处理的角色生成明确错误。
func ValidateRole(role, target string) *errclass.Error {
	return &errclass.Error{
		Class:   errclass.ClassUnsupported,
		Message: fmt.Sprintf("unsupported message role %q for %s", role, target),
	}
}

// SystemImageRejected reports image content inside a system/instruction
// message, which no target protocol can represent (FR-005). One kernel
// serves every adapter so the wording cannot diverge again.
// SystemImageRejected 统一报告系统指令不能携带图片的转换限制。
func SystemImageRejected() *errclass.Error {
	return errclass.Translation("system messages cannot carry image content")
}

// outputTextPart is one content part of a synthesized assistant message
// item.
type outputTextPart struct {
	// Type 协议对象或内容分部类型。
	Type string `json:"type"` // always "output_text"
	// Text 当前内容分部或摘要文本。
	Text string `json:"text"`
}

// OutputAssembler aggregates Responses output items — ONE assistant
// message item carrying all observed text plus function_call items in
// arrival order — for every Responses-output synthesis site, streaming and
// non-streaming, Messages-source and Chat-Completions-source alike
// (FR-006 mode/route parity). Placement collapses two former per-site
// mechanics into one rule: a site that called ReserveTextSlot gets the
// message at the first text observation's slot (filled even when the
// aggregated text is empty — Messages-source semantics); a site that
// never reserves gets the Chat Completions placement (message leads, and
// only when text is non-empty). Render materializes the array once, at
// terminal time.
type OutputAssembler struct {
	// messageID 聚合消息项的稳定标识。
	messageID string          // identity carried by the message item
	// items 按插入顺序保存的输出项。
	items     []any           // rendered items in insertion order
	// textSlot 预留消息位置，负数表示未预留。
	textSlot  int             // reserved message position, -1 until reserved
	// text 按顺序累计的文本。
	text      strings.Builder // aggregated message text
}

// NewOutputAssembler binds an assembler to the response identity the
// synthesized message item carries.
// NewOutputAssembler 创建绑定消息标识的输出聚合器，保证项目顺序稳定。
func NewOutputAssembler(messageID string) *OutputAssembler {
	return &OutputAssembler{messageID: messageID, textSlot: -1, items: make([]any, 0)}
}

// ReserveTextSlot pins the message item's position at the current end of
// the output array so later function_call items cannot displace it;
// idempotent — only the first call reserves.
// ReserveTextSlot 仅首次预留消息位置，防止后续工具调用挤占输出索引。
func (a *OutputAssembler) ReserveTextSlot() {
	if a.textSlot >= 0 {
		return
	}
	a.textSlot = len(a.items)
	a.items = append(a.items, nil)
}

// AddText appends one text fragment to the aggregated message.
// AddText 追加消息文本片段，保留原有空白和顺序。
func (a *OutputAssembler) AddText(fragment string) {
	a.text.WriteString(fragment)
}

// FunctionCallItemID returns the Responses item id for a synthesized
// function_call. Clients such as Pi keep only ids with an fc_ prefix and
// otherwise record the item id as missing, so every synthesized call gets
// that prefix. The result depends only on callID so stream announcements,
// terminal output, and the non-stream converter stay identical.
// FunctionCallItemID 生成具有 fc_ 前缀的稳定工具项目标识，兼容客户端关联逻辑。
func FunctionCallItemID(callID string) string {
	if strings.HasPrefix(callID, "fc_") {
		return callID
	}
	if callID == "" {
		return "fc_0"
	}
	return "fc_" + callID
}

// AppendFunctionCall adds one function_call item in arrival order;
// arguments pass through verbatim — callers apply the absent-arguments
// policy themselves.
// AppendFunctionCall 按到达顺序追加工具调用项，保留调用方处理后的参数。
func (a *OutputAssembler) AppendFunctionCall(callID, name, args string) {
	a.items = append(a.items, RespItem{
		Type: "function_call", ID: FunctionCallItemID(callID), CallID: callID, Name: name, Arguments: args,
	})
}

// AppendReasoning adds one reasoning item in arrival order. Upstream
// thinking always precedes text and tool calls, so it lands first and the
// item positions keep matching the output_index values the stream announced.
// AppendReasoning 按到达顺序追加推理项，保持与流内输出索引一致。
func (a *OutputAssembler) AppendReasoning(item RespReasoningItem) {
	a.items = append(a.items, item)
}

// Render fills the reserved slot (or leads with the message for sites
// that never reserved but observed non-empty text) and returns the
// output array.
// Render 填充预留消息位置或前置非空文本，生成最终有序输出数组。
func (a *OutputAssembler) Render() []any {
	t := a.text.String()
	if t != "" || a.textSlot >= 0 {
		content, _ := json.Marshal([]outputTextPart{{Type: "output_text", Text: t}})
		msg := RespItem{Type: "message", ID: a.messageID, Role: "assistant", Content: content}
		if a.textSlot >= 0 {
			a.items[a.textSlot] = msg
		} else {
			rest := a.items
			a.items = make([]any, 0, len(rest)+1)
			a.items = append(a.items, msg)
			a.items = append(a.items, rest...)
		}
	}
	return a.items
}

// FunctionTool validates one tool definition for translation. toolType
// must be "function" — anything else is unsupported_protocol_or_parameter
// (FR-009: a non-function tool type is an unsupported parameter) naming
// the target endpoint. Callers normalize or preserve the parameter schema
// themselves from their own raw field.
// FunctionTool 校验工具类型必须为函数，避免静默丢弃不可转换工具。
func FunctionTool(toolType, targetLabel string) *errclass.Error {
	if toolType != "function" {
		return &errclass.Error{
			Class:   errclass.ClassUnsupported,
			Message: fmt.Sprintf("unsupported tool type %q; only function tools translate to %s", toolType, targetLabel),
		}
	}
	return nil
}
