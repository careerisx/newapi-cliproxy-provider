// 流转换按接收顺序维护协议状态；事件公布、内容增量和收尾使用稳定索引。
package messages

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"newapi-provider/internal/adapter/shared"
	"newapi-provider/internal/errclass"
)

// StreamConverter converts an upstream Anthropic Messages SSE stream
// (typed `event:`/`data:` pairs) into the client protocol incrementally
// (FR-006, AC §C streaming). It is stateful and sequential-use only:
// one instance per upstream response, Feed called with consecutive
// network chunks in order.
type StreamConverter struct {
	// framer 跨分片 SSE 分帧器。
	framer           *shared.SSEFramer
	// sourceFormat 客户端来源协议，决定输出格式。
	sourceFormat     string
	// created 响应创建时间戳。
	created          int64
	// blocks 按上游索引保存的内容块状态。
	blocks           map[int]*blockState
	// toolOrdinal 下一个紧凑工具调用序号。
	toolOrdinal      int
	// msgID 当前消息标识。
	msgID            string
	// model 上游模型标识。
	model            string
	// promptTokens 累计输入令牌数。
	promptTokens     int64
	// completionTokens 累计输出令牌数。
	completionTokens int64
	// cacheRead 可选缓存读取令牌数，区分缺失与零值。
	cacheRead        *int64
	// cacheCreation 可选缓存创建令牌数。
	cacheCreation    *int64
	// stopReason 最近观察到的停止原因。
	stopReason       string // last observed message_delta stop_reason (responses terminal status)
	// toolsSeen 是否观察到工具调用，用于停止原因优先级。
	toolsSeen        bool   // any tool_use block observed (openai finish-reason precedence)
	// roleSent 是否已输出助手角色增量。
	roleSent         bool
	// msgIdx 聚合消息输出索引，负数表示未公布。
	msgIdx           int  // responses: compacted output_index of the ONE announced message item (-1 until then)
	// outCount 下一个紧凑输出位置。
	outCount         int  // responses: next compacted output position; thinking blocks consume none (FR-005 omission)
	// emitted 是否已交付事件或调用参数。
	emitted          bool // any client event emitted (Flush eligibility)
	// flushed 是否已执行提前关闭收尾。
	flushed          bool // one-shot guard for Flush
}

// blockState tracks one open upstream content block by index.
type blockState struct {
	// kind 归一化内容块类型。
	kind    string
	// id 响应或工具调用的稳定标识。
	id      string
	// itemID 用于关联 Responses 项目事件的标识。
	itemID  string // fc_-prefixed Responses item id shared by deltas and done events
	// name 工具函数名。
	name    string
	// ordinal 紧凑工具调用序号。
	ordinal int             // compact tool_calls index for openai chunks
	// outIdx 已公布的 Responses 输出索引。
	outIdx  int             // responses: compacted output_index assigned at block start
	// emitted 是否已交付事件或调用参数。
	emitted bool            // openai: id/name attached to the first arguments fragment
	// text 按顺序累计的文本。
	text    strings.Builder // accumulated text_delta content (responses target)
	// args 累计工具参数分片。
	args    strings.Builder // accumulated input_json_delta content (responses target)
}

// NewStreamConverter prepares stream conversion for sourceFormat
// ("openai" Chat Completions chunks, "openai-response" Responses events,
// "claude" verbatim passthrough).
// NewStreamConverter 创建每条上游流独享的转换器，仅允许顺序使用。
func NewStreamConverter(sourceFormat string) *StreamConverter {
	return &StreamConverter{
		framer:       shared.NewSSEFramer(sourceFormat == "claude"),
		sourceFormat: sourceFormat,
		created:      time.Now().Unix(),
		blocks:       map[int]*blockState{},
		msgIdx:       -1,
	}
}

// Feed consumes one network chunk and returns fully framed client events,
// whether the upstream stream is finished (message_stop seen), and a
// classified error. Partial lines are buffered until complete.
// Feed 按字节顺序接收网络分片，输出完整事件、终止状态或分类错误。
func (sc *StreamConverter) Feed(chunk []byte) (events [][]byte, done bool, eErr *errclass.Error) {
	sc.framer.Push(chunk)
	for {
		etype, data, rawBlock, ok := sc.framer.Next()
		if !ok {
			return events, false, nil
		}
		d, eErr := sc.dispatch(etype, data, rawBlock, &events)
		if len(events) > 0 {
			sc.emitted = true
		}
		if eErr != nil {
			return events, false, eErr
		}
		if d {
			return events, true, nil
		}
	}
}

// dispatch routes one complete SSE frame. Conversion targets parse the
// data payload eagerly into typed per-event structs (with the
// type-from-payload fallback when the event: line was absent); claude
// passthrough forwards the verbatim raw block and parses only what error
// handling and done-detection require.
// dispatch 分派完整 SSE 帧，转换目标解析载荷而原生目标保留原始字节。
func (sc *StreamConverter) dispatch(etype, data string, rawBlock []byte, events *[][]byte) (bool, *errclass.Error) {
	if sc.sourceFormat == "claude" {
		return sc.dispatchClaude(etype, data, rawBlock, events)
	}
	var ev sseEvent
	if data != "" {
		if err := json.Unmarshal([]byte(data), &ev); err != nil {
			return false, errclass.Translation(fmt.Sprintf("malformed SSE data JSON: %s", shared.RedactedSnippet(data)))
		}
		if etype == "" {
			etype = ev.Type // tolerate missing event: line
		}
	}
	switch sc.sourceFormat {
	case "openai":
		return sc.dispatchOpenAI(etype, &ev, events)
	case "openai-response":
		return sc.dispatchResponses(etype, &ev, events)
	default:
		return false, shared.UnsupportedFormat(sc.sourceFormat, EndpointPath)
	}
}

// sseEvent is the typed union of Anthropic Messages SSE payload shapes on
// conversion targets. One struct serves every event type because the
// type may arrive only inside the payload (missing event: line), so the
// decode must precede routing; absent members stay zero-valued at no
// cost, and unknown keys are ignored. Error stays a raw map because the
// classifier reads an open-ended upstream object.
type sseEvent struct {
	// Type 协议对象或内容分部类型。
	Type         string         `json:"type"`
	// Index 上游内容块或工具调用索引。
	Index        float64        `json:"index"`
	// Message 响应消息或已脱敏错误说明。
	Message      startMessage   `json:"message"`
	// ContentBlock 内容块开始事件载荷。
	ContentBlock contentBlock   `json:"content_block"`
	// Delta 流式事件增量载荷。
	Delta        deltaBody      `json:"delta"`
	// Usage 令牌用量及可选缓存详情。
	Usage        usageCounts    `json:"usage"`
	// Error 上游错误对象。
	Error        map[string]any `json:"error"`
}

type startMessage struct {
	// ID 响应、内容块或工具调用的协议标识。
	ID    string      `json:"id"`
	// Model 上游模型标识。
	Model string      `json:"model"`
	// Usage 令牌用量及可选缓存详情。
	Usage usageCounts `json:"usage"`
}

type contentBlock struct {
	// Type 协议对象或内容分部类型。
	Type string `json:"type"`
	// ID 响应、内容块或工具调用的协议标识。
	ID   string `json:"id"`
	// Name 工具函数名。
	Name string `json:"name"`
}

type deltaBody struct {
	// Type 协议对象或内容分部类型。
	Type        string `json:"type"`
	// Text 当前内容分部或摘要文本。
	Text        string `json:"text"`
	// PartialJSON 工具参数 JSON 文本分片。
	PartialJSON string `json:"partial_json"`
	// StopReason Messages 停止原因。
	StopReason  string `json:"stop_reason"`
}

type usageCounts struct {
	// InputTokens 输入令牌数。
	InputTokens   float64  `json:"input_tokens"`
	// OutputTokens 输出令牌数。
	OutputTokens  float64  `json:"output_tokens"`
	// CacheRead 可选缓存读取输入令牌数。
	CacheRead     *float64 `json:"cache_read_input_tokens"`
	// CacheCreation 可选缓存创建输入令牌数。
	CacheCreation *float64 `json:"cache_creation_input_tokens"`
}

// dispatchClaude passes event blocks through verbatim; an in-stream error
// event becomes a classified error instead of reaching the client raw.
// The payload is parsed lazily: only when the typed fallback must resolve
// a missing event: line or when the frame is an error event.
// dispatchClaude 原样转发 Messages 帧，并识别错误及终止事件。
func (sc *StreamConverter) dispatchClaude(etype, data string, raw []byte, events *[][]byte) (bool, *errclass.Error) {
	var payload map[string]any
	if etype == "" && data != "" {
		json.Unmarshal([]byte(data), &payload) // best-effort type probe
		etype, _ = payload["type"].(string)
	}
	if etype == "error" {
		if payload == nil && data != "" {
			json.Unmarshal([]byte(data), &payload)
		}
		return false, sseError(mget(payload, "error"))
	}
	if len(raw) > 0 {
		*events = append(*events, raw)
	}
	return etype == "message_stop", nil
}

// dispatchOpenAI synthesizes chat.completion.chunk frames (FR-006).
// dispatchOpenAI 将 Messages 事件合成为 Chat Completions 增量及用量。
func (sc *StreamConverter) dispatchOpenAI(etype string, ev *sseEvent, events *[][]byte) (bool, *errclass.Error) {
	switch etype {
	case "message_start":
		sc.msgID = ev.Message.ID
		sc.model = ev.Message.Model
		sc.promptTokens = int64(ev.Message.Usage.InputTokens)
		sc.captureCache(ev.Message.Usage)
		sc.emitRole(events)
	case "content_block_start":
		bs := &blockState{kind: ev.ContentBlock.Type, id: ev.ContentBlock.ID, name: ev.ContentBlock.Name}
		if bs.kind == "tool_use" {
			bs.ordinal = sc.toolOrdinal
			sc.toolOrdinal++
			sc.toolsSeen = true
		}
		sc.blocks[int(ev.Index)] = bs
	case "content_block_delta":
		bs := sc.blocks[int(ev.Index)]
		switch ev.Delta.Type {
		case "text_delta":
			sc.emitRole(events)
			sc.emitChunk(events, map[string]any{"content": ev.Delta.Text}, "", false)
		case "input_json_delta":
			if bs == nil || bs.kind != "tool_use" {
				return false, errclass.Translation("tool arguments fragment without a tool_use block start")
			}
			var tc map[string]any
			if !bs.emitted {
				tc = shared.CCToolCallOpeningEntry(bs.ordinal, bs.id, bs.name, ev.Delta.PartialJSON)
				bs.emitted = true
			} else {
				tc = map[string]any{
					"index":    bs.ordinal,
					"function": map[string]any{"arguments": ev.Delta.PartialJSON},
				}
			}
			sc.emitRole(events)
			sc.emitChunk(events, map[string]any{"tool_calls": []any{tc}}, "", false)
		case "thinking_delta":
			// FR-005 explicit omission policy: chat.completion.chunk has no
			// standard reasoning-delta field, so thinking text is dropped
			// rather than put in a non-standard field. Signatures have no
			// destination either.
		}
	case "message_delta":
		if ev.Usage.InputTokens != 0 {
			sc.promptTokens = int64(ev.Usage.InputTokens)
		}
		sc.completionTokens = int64(ev.Usage.OutputTokens)
		sc.captureCache(ev.Usage)
		// Observed tool calls outrank the status-derived reason: a
		// terminal max_tokens cannot downgrade tool_calls to length.
		sc.emitChunk(events, map[string]any{},
			shared.TerminalReason(sc.toolsSeen, "tool_calls",
				shared.ClaudeStopToFinish(ev.Delta.StopReason)), true)
	case "message_stop":
		return true, nil
	case "error":
		return false, sseError(ev.Error)
	default: // ping and unknown event types are ignored (forward compat)
	}
	return false, nil
}

// dispatchResponses synthesizes Responses SSE events (FR-006).
// dispatchResponses 将 Messages 事件合成为 Responses 项目及增量。
func (sc *StreamConverter) dispatchResponses(etype string, ev *sseEvent, events *[][]byte) (bool, *errclass.Error) {
	switch etype {
	case "message_start":
		sc.msgID = ev.Message.ID
		sc.model = ev.Message.Model
		sc.promptTokens = int64(ev.Message.Usage.InputTokens)
		sc.captureCache(ev.Message.Usage)
		// Lifecycle parity with the Chat Completions route's synthesizer:
		// announce the response before any deltas reference it (F18).
		*events = append(*events, sc.responsesEm().Created())
	case "content_block_start":
		bs := &blockState{kind: ev.ContentBlock.Type, id: ev.ContentBlock.ID, name: ev.ContentBlock.Name}
		sc.blocks[int(ev.Index)] = bs
		// Announce each translatable item before its first delta (F18
		// lifecycle parity); thinking blocks produce no deltas and are
		// omitted entirely (FR-005). Indexes come from the SAME compacted
		// space outputItems() renders, so streamed output_index values
		// always match terminal response.completed.output positions.
		switch bs.kind {
		case "text":
			if sc.msgIdx >= 0 {
				break // one message item aggregates every text block (canonical shape)
			}
			sc.msgIdx = sc.outCount
			sc.outCount++
			// Native-compatible shape: the announced item carries "id" so
			// downstream output_text.delta item_ids resolve (F-R3 parity).
			*events = append(*events, sc.responsesEm().ItemAdded(sc.msgIdx, map[string]any{
				"type": "message", "id": sc.msgID, "role": "assistant", "content": []any{},
			}))
		case "tool_use":
			sc.toolsSeen = true
			bs.outIdx = sc.outCount
			sc.outCount++
			bs.itemID = shared.FunctionCallItemID(bs.id)
			*events = append(*events, sc.responsesEm().ItemAdded(bs.outIdx, map[string]any{
				"type": "function_call", "id": bs.itemID, "call_id": bs.id, "name": bs.name, "arguments": "",
			}))
		}
	case "content_block_delta":
		idx := int(ev.Index)
		bs := sc.blocks[idx]
		switch ev.Delta.Type {
		case "text_delta":
			if bs != nil {
				bs.text.WriteString(ev.Delta.Text)
			}
			// Deltas target the single aggregated message item, so later
			// text blocks reference its announced position, never their own.
			dIdx := idx
			if sc.msgIdx >= 0 {
				dIdx = sc.msgIdx
			}
			*events = append(*events, sc.responsesEm().TextDelta(sc.msgID, dIdx, ev.Delta.Text))
		case "input_json_delta":
			if bs == nil || bs.kind != "tool_use" {
				return false, errclass.Translation("tool arguments fragment without a tool_use block start")
			}
			bs.args.WriteString(ev.Delta.PartialJSON)
			*events = append(*events, sc.responsesEm().ArgsDelta(bs.itemID, bs.outIdx, ev.Delta.PartialJSON))
		case "thinking_delta":
			// FR-005 explicit omission policy: no standard Responses
			// reasoning-delta event; dropped.
		}
	case "message_delta":
		if ev.Usage.InputTokens != 0 {
			sc.promptTokens = int64(ev.Usage.InputTokens)
		}
		sc.completionTokens = int64(ev.Usage.OutputTokens) // accumulated, reported on completion
		sc.captureCache(ev.Usage)
		sc.stopReason = ev.Delta.StopReason
	case "message_stop":
		*events = append(*events, sc.responsesCompleted()...)
		return true, nil
	case "error":
		return false, sseError(ev.Error)
	default: // ping and unknown event types are ignored (forward compat)
	}
	return false, nil
}

// responsesCompleted builds the terminal response.completed event: shape
// parity with the non-stream claudeToResponses converter — status comes
// from the Responses vocabulary {completed, incomplete} via the observed
// stop_reason (max_tokens truncation → incomplete); tool calls are
// represented by their function_call output items and never override the
// status (FR-006). Each function_call is closed at its announced
// output_index before completed, because clients finish a call only after
// the matching done event. Shared by message_stop and Flush so an early
// upstream close cannot diverge from the normal-path shape.
// responsesCompleted 关闭已公布的工具调用并输出完整结果及最终用量。
func (sc *StreamConverter) responsesCompleted() [][]byte {
	status := shared.ResponseStatusFromClaudeStop(sc.stopReason)
	usage := shared.NewResponsesUsageFrom(sc.promptTokens+valueOrZero(sc.cacheRead)+valueOrZero(sc.cacheCreation), sc.completionTokens,
		shared.UsageDetails{CachedTokens: sc.cacheRead, CacheWriteTokens: sc.cacheCreation})
	em := sc.responsesEm()
	indexes := make([]int, 0, len(sc.blocks))
	for i := range sc.blocks {
		indexes = append(indexes, i)
	}
	sort.Ints(indexes)
	events := make([][]byte, 0, len(indexes)*2+1)
	for _, i := range indexes {
		bs := sc.blocks[i]
		if bs == nil || bs.kind != "tool_use" {
			continue
		}
		args := shared.DefaultArgs(bs.args.String())
		events = append(events, em.CloseFunctionCall(bs.outIdx, shared.RespItem{
			Type: "function_call", ID: bs.itemID, CallID: bs.id, Name: bs.name, Arguments: args,
		})...)
	}
	return append(events, em.Completed(status, usage, sc.outputItems()))
}

// responsesEm binds the shared Responses emitter kernel to the captured
// upstream message identity so this route's frames cannot diverge from the
// sibling Chat-Completions-route synthesizer (FR-006).
// responsesEm 将共享 Responses 事件生成器绑定到当前响应身份。
func (sc *StreamConverter) responsesEm() shared.ResponsesEventEmitter {
	return shared.ResponsesEventEmitter{ID: sc.msgID, Model: sc.model}
}

// Flush terminates a stream whose upstream closed before message_stop:
// without this the client never sees response.completed nor [DONE] (F5,
// FR-006). Responses synthesis emits response.completed from captured
// identity, usage, and accumulated output items; openai targets get the
// missing [DONE]; claude passthrough holds nothing back. One-shot: later
// calls return nothing, as does any call with nothing emitted.
// Flush 在上游提前关闭时释放待处理终止事件，避免重复收尾。
func (sc *StreamConverter) Flush() [][]byte {
	if sc.flushed {
		return nil
	}
	sc.flushed = true
	if !sc.emitted {
		return nil
	}
	switch sc.sourceFormat {
	case "openai-response":
		return sc.responsesCompleted()
	case "openai":
		return nil
	default: // claude passthrough forwards verbatim; nothing deferred
		return nil
	}
}

// outputItems materializes the Responses output array from observed
// content blocks in upstream index order (FR-006) through the shared
// OutputAssembler: ALL text blocks aggregate into ONE message item
// positioned at the first text block's slot while tool_use blocks become
// separate function_call items — the canonical native shape, equal to the
// non-stream claudeToResponses converter (FR-006 mode parity); thinking
// blocks are omitted (FR-005).
// outputItems 按上游内容块索引汇总文本和工具调用，保持输出顺序。
func (sc *StreamConverter) outputItems() []any {
	indexes := make([]int, 0, len(sc.blocks))
	for i := range sc.blocks {
		indexes = append(indexes, i)
	}
	sort.Ints(indexes)
	oa := shared.NewOutputAssembler(sc.msgID)
	for _, i := range indexes {
		bs := sc.blocks[i]
		switch bs.kind {
		case "text":
			oa.ReserveTextSlot()
			oa.AddText(bs.text.String())
		case "tool_use":
			oa.AppendFunctionCall(bs.id, bs.name, shared.DefaultArgs(bs.args.String()))
		}
	}
	return oa.Render()
}

// chatChunks binds the shared chunk kernel to the captured upstream
// message identity so this route's frames cannot diverge from the
// sibling Responses-route synthesizer (FR-006).
// chatChunks 将共享 Chat Completions 增量生成器绑定到当前响应身份。
func (sc *StreamConverter) chatChunks() shared.ChatChunkBuilder {
	return shared.ChatChunkBuilder{ID: sc.msgID, Model: sc.model, Created: sc.created}
}

// emitRole guarantees the leading assistant-role chunk openai clients expect.
// emitRole 确保只输出一次前导助手角色增量。
func (sc *StreamConverter) emitRole(events *[][]byte) {
	if sc.roleSent {
		return
	}
	sc.roleSent = true
	*events = append(*events, sc.chatChunks().RoleChunk())
}

// emitChunk appends one synthesized chat.completion.chunk frame via the
// shared kernel; an empty finish renders null (intermediate chunks) and
// withUsage attaches the accumulated token usage to the terminal frame.
// emitChunk 追加一条合成增量，并按需附加最终用量。
func (sc *StreamConverter) emitChunk(events *[][]byte, delta map[string]any, finish string, withUsage bool) {
	b := sc.chatChunks()
	if finish == "" {
		*events = append(*events, b.Delta(delta))
		return
	}
	var usage map[string]any
	if withUsage {
		usage = shared.CCUsageFrom(sc.promptTokens+valueOrZero(sc.cacheRead)+valueOrZero(sc.cacheCreation), sc.completionTokens,
			shared.UsageDetails{CachedTokens: sc.cacheRead})
	}
	*events = append(*events, b.Finish(finish, usage))
}

// captureCache 捕获实际存在的缓存用量，区分缺失字段与显式零值。
func (sc *StreamConverter) captureCache(u usageCounts) {
	if u.CacheRead != nil {
		v := int64(*u.CacheRead)
		sc.cacheRead = &v
	}
	if u.CacheCreation != nil {
		v := int64(*u.CacheCreation)
		sc.cacheCreation = &v
	}
}

// sseError maps an Anthropic in-stream error event onto the shared
// classification (FR-009) via its documented error-type taxonomy.
// sseError 将 Messages 流内错误按协议类型转换为统一错误分类。
func sseError(e map[string]any) *errclass.Error {
	errorType := sget(e, "type")
	message := sget(e, "message")
	return errclass.FromStatus(anthropicErrorStatus(errorType), message)
}

// anthropicErrorStatus 按 Messages 错误类型映射状态码，未知类型按服务器错误处理。
func anthropicErrorStatus(t string) int {
	switch t {
	case "invalid_request_error":
		return 400
	case "authentication_error":
		return 401
	case "permission_error":
		return 403
	case "not_found_error":
		return 404
	case "request_too_large":
		return 413
	case "rate_limit_error":
		return 429
	case "overloaded_error":
		return 503
	default:
		return 500
	}
}

// mget 安全读取动态对象的嵌套对象，类型不符时返回空值。
func mget(m map[string]any, k string) map[string]any {
	v, _ := m[k].(map[string]any)
	return v
}

// sget 安全读取动态对象的字符串，类型不符时返回空串。
func sget(m map[string]any, k string) string {
	v, _ := m[k].(string)
	return v
}
