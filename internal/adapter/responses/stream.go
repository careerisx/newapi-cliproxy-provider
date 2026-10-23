// 流转换按接收顺序维护协议状态；事件公布、内容增量和收尾使用稳定索引。
package responses

import (
	"encoding/json"
	"fmt"

	"newapi-provider/internal/adapter/shared"
	"newapi-provider/internal/errclass"
)

// StreamConverter converts an upstream /v1/responses SSE stream into the
// client protocol incrementally.
//
// Contract: instances are single-use and strictly sequential — Feed is
// called from one goroutine with consecutive network chunks; there is no
// internal locking. Partial SSE lines are buffered until a blank line
// completes the `event:`/`data:` pair.
type StreamConverter struct {
	// framer 跨分片 SSE 分帧器。
	framer *shared.SSEFramer
	// source 客户端目标协议。
	source string

	// Response identity captured from response.created.
	// id 响应或工具调用的稳定标识。
	id      string
	// model 上游模型标识。
	model   string
	// created 响应创建时间戳。
	created int64

	// openai (Chat Completions) target state.
	// toolCallsSeen 是否观察到函数调用。
	toolCallsSeen bool

	// claude (Messages) target state.
	// textIndex 当前文本内容块索引。
	textIndex  int
	// textOpen 当前文本内容块是否打开。
	textOpen   bool
	// nextIndex 下一个可分配的输出索引。
	nextIndex  int
	// openBlocks 仍需关闭的 Messages 内容块索引。
	openBlocks []int

	// Shared function_call announce-or-replay decision table for both
	// conversion targets; an instance converts to exactly one target,
	// so nextIndex doubles as the tool block index allocator.
	// tracker 工具标识别名、公布与参数交付追踪器。
	tracker *toolCallTracker
}

// NewStreamConverter builds a converter for sourceFormat ("openai",
// "claude", "openai-response"); unknown formats fail on first Feed with
// ClassUnsupported (same classes as BuildRequest).
// NewStreamConverter 创建每条上游流独享的转换器，仅允许顺序使用。
func NewStreamConverter(sourceFormat string) *StreamConverter {
	sc := &StreamConverter{
		// wantRaw only for the openai-response passthrough, which
		// forwards verbatim blocks; a rebuilt single data line would
		// embed raw newlines from legally multi-data-line frames and
		// corrupt native framing. Conversion targets parse the joined
		// payload and never read raw.
		framer:    shared.NewSSEFramer(sourceFormat == "openai-response"),
		source:    sourceFormat,
		id:        "response",
		textIndex: -1,
	}
	sc.tracker = newToolCallTracker(sc.allocIndex)
	return sc
}

// allocIndex hands out the next block/entry index, shared by text and
// tool blocks in Messages vocabulary and by tool entries alone in Chat
// Completions vocabulary (only one target runs per converter instance).
// allocIndex 分配下一个稳定内容块或工具索引。
func (sc *StreamConverter) allocIndex() int {
	i := sc.nextIndex
	sc.nextIndex++
	return i
}

// toolCallTracker is the single announce-or-replay decision table for
// upstream function_call items, keyed by call_id and owned by both
// conversion targets so their delivery decisions cannot diverge:
// announce once per call, deliver complete arguments the moment they are
// observed (at output_item.added or .done alike), and replay at done
// only when nothing was delivered earlier (FR-006).
//
// OpenAI-conformant Responses streams identify one item in TWO distinct
// namespaces: output_item events carry call_id ("call_…") while
// argument-delta events reference the item's own id ("fc_…"). Every
// registration therefore binds BOTH identifiers to the same state and
// lookups resolve through either before creating new state — otherwise
// each delta fragment would fork a second tracker state and clients
// would see a ghost tool entry beside the announced call.
type toolCallTracker struct {
	// calls 调用标识和项目别名到共享状态的关联表。
	calls map[string]*toolCallState
	// alloc 为新工具调用分配稳定索引的回调。
	alloc func() int
}

type toolCallState struct {
	// index 调用或内容块的稳定索引。
	index     int
	// delivered 参数是否已交付，防止完成事件重放。
	delivered bool
}

// newToolCallTracker 创建工具调用追踪器，统一公布与参数重放决策。
func newToolCallTracker(alloc func() int) *toolCallTracker {
	return &toolCallTracker{calls: map[string]*toolCallState{}, alloc: alloc}
}

// Observe records an output_item.added/.done sighting of a function_call
// (callID from item.call_id, itemID from the item's own "id" field) and
// reports whether this is the first sight (the caller must emit its
// announce frame), the call's stable index, and whether complete
// arguments must be delivered with this event.
// Observe 绑定调用标识与项目别名，决定首次公布及完整参数交付。
func (t *toolCallTracker) Observe(callID, itemID string, argsComplete bool) (first bool, index int, deliverArgs bool) {
	first, st := t.state(callID, itemID)
	if argsComplete && !st.delivered {
		st.delivered = true
		deliverArgs = true
	}
	return first, st.index, deliverArgs
}

// StreamArgs records argument fragments streamed outside item events;
// any fragment counts as delivered, so a later done cannot replay
// duplicates. It reports whether the call was first seen here (the
// caller must open its entry/block defensively).
// StreamArgs 记录参数分片已交付，防止完成事件重复重放。
func (t *toolCallTracker) StreamArgs(itemID string) (first bool, index int) {
	first, st := t.state(itemID, "")
	st.delivered = true
	return first, st.index
}

// state resolves key, then alias, binding whichever identifiers are
// present to the shared state so both namespaces stay one call.
// state 通过主标识或别名解析同一调用状态，缺省时分配新状态。
func (t *toolCallTracker) state(key, alias string) (bool, *toolCallState) {
	if st, ok := t.calls[key]; ok {
		t.bind(alias, st)
		return false, st
	}
	if st, ok := t.calls[alias]; ok {
		t.bind(key, st)
		return false, st
	}
	st := &toolCallState{index: t.alloc()}
	t.bind(key, st)
	t.bind(alias, st)
	return true, st
}

// bind 将非空调用标识绑定到已有追踪状态。
func (t *toolCallTracker) bind(key string, st *toolCallState) {
	if key != "" {
		t.calls[key] = st
	}
}

// ---- upstream Responses SSE payload shapes ----
//
// One typed struct per handled event type, keyed off the framer's event
// name; unknown keys are ignored and absent members stay zero-valued at
// no cost. Unknown EVENT types are JSON-validated without decoding and
// ignored.

type usageCounts struct {
	// InputTokens 输入令牌数。
	InputTokens  float64 `json:"input_tokens"`
	// OutputTokens 输出令牌数。
	OutputTokens float64 `json:"output_tokens"`
	// InputDetails 可选输入令牌详情。
	InputDetails *struct {
		// CachedTokens 可选缓存读取令牌数。
		CachedTokens     *float64 `json:"cached_tokens"`
		// CacheWriteTokens 可选缓存写入令牌数。
		CacheWriteTokens *float64 `json:"cache_write_tokens"`
	} `json:"input_tokens_details"`
	// OutputDetails 可选输出令牌详情。
	OutputDetails *struct {
		// ReasoningTokens 可选推理令牌数。
		ReasoningTokens *float64 `json:"reasoning_tokens"`
	} `json:"output_tokens_details"`
}

type errorMessage struct {
	// Message 响应消息或已脱敏错误说明。
	Message string `json:"message"`
}

type responseMeta struct {
	// ID 响应、内容块或工具调用的协议标识。
	ID         string       `json:"id"`
	// Model 上游模型标识。
	Model      string       `json:"model"`
	// CreatedAt 上游响应创建时间戳。
	CreatedAt  float64      `json:"created_at"`
	// StatusCode HTTP 状态码，零表示未提供。
	StatusCode float64      `json:"status_code"`
	// Usage 令牌用量及可选缓存详情。
	Usage      usageCounts  `json:"usage"`
	// Error 上游错误对象。
	Error      errorMessage `json:"error"`
}

type functionCallItem struct {
	// Type 协议对象或内容分部类型。
	Type      string `json:"type"`
	// CallID 函数调用标识，用于关联工具结果。
	CallID    string `json:"call_id"`
	// ID 响应、内容块或工具调用的协议标识。
	ID        string `json:"id"`
	// Name 工具函数名。
	Name      string `json:"name"`
	// Arguments 函数调用参数的 JSON 字符串。
	Arguments string `json:"arguments"`
}

type createdEvent struct {
	// Response 事件携带的响应元信息。
	Response responseMeta `json:"response"`
}

type textDeltaEvent struct {
	// Delta 流式事件增量载荷。
	Delta string `json:"delta"`
}

type itemEvent struct {
	// Item 事件携带的输出项。
	Item functionCallItem `json:"item"`
}

type argsDeltaEvent struct {
	// ItemID Responses 输出项标识，用于关联增量。
	ItemID string `json:"item_id"`
	// Delta 流式事件增量载荷。
	Delta  string `json:"delta"`
}

type terminalEvent struct {
	// Response 事件携带的响应元信息。
	Response responseMeta `json:"response"`
}

type failureEvent struct {
	// StatusCode HTTP 状态码，零表示未提供。
	StatusCode float64      `json:"status_code"`
	// Message 响应消息或已脱敏错误说明。
	Message    string       `json:"message"`
	// Response 事件携带的响应元信息。
	Response   responseMeta `json:"response"`
}

// decodeEvent parses one SSE data payload into its typed shape; malformed
// JSON yields a translation failure carrying only a short redacted
// snippet (FR-006, §5 security: no upstream body echo). An empty payload
// decodes to zero values, matching the legacy empty-map behavior.
// decodeEvent 解析事件载荷，错误只携带脱敏后的短片段。
func decodeEvent[T any](eventType, payload string, v *T) *errclass.Error {
	if payload == "" {
		return nil
	}
	if err := json.Unmarshal([]byte(payload), v); err != nil {
		return errclass.Translation(fmt.Sprintf(
			"malformed %s event payload: %s", eventType, shared.RedactedSnippet(payload)))
	}
	return nil
}

// checkTarget rejects unknown conversion sources after payload decode, so
// malformed-payload translation failures keep their pre-existing
// precedence over ClassUnsupported.
// checkTarget 检查目标协议支持情况，保持解析错误的优先级。
func (sc *StreamConverter) checkTarget() *errclass.Error {
	if sc.source == "openai" || sc.source == "claude" {
		return nil
	}
	return shared.UnsupportedFormat(sc.source, EndpointPath)
}

// Feed consumes one network chunk and returns synthesized client events,
// whether the upstream stream reached a terminal state, and a classified
// error for failed/malformed streams (FR-006). Native passthrough frames
// are forwarded verbatim without JSON validation; conversion targets
// parse eagerly into typed per-event structs.
// Feed 按字节顺序接收网络分片，输出完整事件、终止状态或分类错误。
func (sc *StreamConverter) Feed(chunk []byte) (events [][]byte, done bool, eErr *errclass.Error) {
	sc.framer.Push(chunk)
	for {
		eventType, payload, raw, ok := sc.framer.Next()
		if !ok {
			return events, done, nil
		}
		if sc.source == "openai-response" {
			evs, d, dErr := sc.passthroughEvent(eventType, payload, raw)
			events = append(events, evs...)
			if dErr != nil {
				return events, false, dErr
			}
			done = done || d
			continue
		}
		evs, d, dErr := sc.convertEvent(eventType, payload)
		events = append(events, evs...)
		if dErr != nil {
			return events, false, dErr
		}
		done = done || d
	}
}

// convertEvent routes one complete SSE frame to a conversion target:
// handled event types decode into their typed structs and share the
// field extraction below; unknown types only validate their payload.
// convertEvent 按 Responses 事件类型转换载荷，忽略兼容性信息事件。
func (sc *StreamConverter) convertEvent(eventType, payload string) ([][]byte, bool, *errclass.Error) {
	switch eventType {
	case "response.created":
		var ev createdEvent
		if eErr := decodeEvent(eventType, payload, &ev); eErr != nil {
			return nil, false, eErr
		}
		if eErr := sc.checkTarget(); eErr != nil {
			return nil, false, eErr
		}
		sc.captureResponse(&ev.Response)
		if sc.source == "openai" {
			return [][]byte{sc.chatChunks().RoleChunk()}, false, nil
		}
		// Input token count only becomes known at completion; Claude
		// clients read authoritative usage from message_delta, so
		// starting at zero is lossless here.
		return [][]byte{sc.claudeChunks().MessageStart(0)}, false, nil
	case "response.output_text.delta":
		var ev textDeltaEvent
		if eErr := decodeEvent(eventType, payload, &ev); eErr != nil {
			return nil, false, eErr
		}
		if eErr := sc.checkTarget(); eErr != nil {
			return nil, false, eErr
		}
		return sc.textDelta(ev.Delta)
	case "response.output_item.added", "response.output_item.done":
		var ev itemEvent
		if eErr := decodeEvent(eventType, payload, &ev); eErr != nil {
			return nil, false, eErr
		}
		if eErr := sc.checkTarget(); eErr != nil {
			return nil, false, eErr
		}
		return sc.outputItem(&ev.Item)
	case "response.function_call_arguments.delta":
		var ev argsDeltaEvent
		if eErr := decodeEvent(eventType, payload, &ev); eErr != nil {
			return nil, false, eErr
		}
		if eErr := sc.checkTarget(); eErr != nil {
			return nil, false, eErr
		}
		return sc.argsFragment(ev.ItemID, ev.Delta)
	case "response.completed", "response.incomplete":
		var ev terminalEvent
		if eErr := decodeEvent(eventType, payload, &ev); eErr != nil {
			return nil, false, eErr
		}
		if eErr := sc.checkTarget(); eErr != nil {
			return nil, false, eErr
		}
		details := shared.UsageDetails{}
		if ev.Response.Usage.InputDetails != nil {
			if ev.Response.Usage.InputDetails.CachedTokens != nil {
				v := int64(*ev.Response.Usage.InputDetails.CachedTokens)
				details.CachedTokens = &v
			}
			if ev.Response.Usage.InputDetails.CacheWriteTokens != nil {
				v := int64(*ev.Response.Usage.InputDetails.CacheWriteTokens)
				details.CacheWriteTokens = &v
			}
		}
		if ev.Response.Usage.OutputDetails != nil && ev.Response.Usage.OutputDetails.ReasoningTokens != nil {
			v := int64(*ev.Response.Usage.OutputDetails.ReasoningTokens)
			details.ReasoningTokens = &v
		}
		return sc.terminal(eventType == "response.incomplete", int(ev.Response.Usage.InputTokens), int(ev.Response.Usage.OutputTokens), details)
	case "response.failed", "error":
		var ev failureEvent
		if eErr := decodeEvent(eventType, payload, &ev); eErr != nil {
			return nil, false, eErr
		}
		if eErr := sc.checkTarget(); eErr != nil {
			return nil, false, eErr
		}
		return nil, false, failureError(&ev)
	default:
		// Informational events (response.in_progress, annotations,
		// reasoning summaries) have no client equivalent and are omitted
		// per the FR-005/FR-006 compatibility policy; the payload is
		// validated without materializing a discarded generic graph.
		if payload != "" && !json.Valid([]byte(payload)) {
			return nil, false, errclass.Translation(fmt.Sprintf(
				"malformed %s event payload: %s", eventType, shared.RedactedSnippet(payload)))
		}
		return nil, false, nil
	}
}

// passthroughEvent echoes native Responses frames verbatim (the framer's
// raw block, byte-identical to the upstream bytes); terminal events flip
// done. Failure/error payloads are the only ones parsed — best-effort, so
// an unparseable failure degrades to a retryable upstream error rather
// than failing the passthrough contract.
// passthroughEvent 原样转发 Responses 帧，同时识别终止与失败事件。
func (sc *StreamConverter) passthroughEvent(eventType, payload string, raw []byte) ([][]byte, bool, *errclass.Error) {
	switch eventType {
	case "response.completed", "response.incomplete":
		return [][]byte{raw}, true, nil
	case "response.failed", "error":
		var ev failureEvent
		json.Unmarshal([]byte(payload), &ev)
		return nil, false, failureError(&ev)
	default:
		return [][]byte{raw}, false, nil
	}
}

// textDelta emits one output text delta: a plain Chat Completions content
// chunk, or a Messages content_block_delta that auto-opens (and may
// reopen) the text block.
// textDelta 输出文本增量，并在 Messages 目标中自动打开或重开文本块。
func (sc *StreamConverter) textDelta(delta string) ([][]byte, bool, *errclass.Error) {
	if sc.source == "openai" {
		return [][]byte{sc.chatChunks().Delta(map[string]any{"content": delta})}, false, nil
	}
	var out [][]byte
	if !sc.textOpen {
		// A tool_use block start closed the previous text block; a
		// later text delta (legal Responses interleaving) reopens a
		// fresh text block at the next free index to keep the
		// mandated start/delta/stop pairing per block index (same
		// stop/reopen pattern as the Chat Completions route). Any
		// still-open block stops first: close-before-next-start.
		out = append(out, sc.closeOpenBlocks()...)
		sc.textIndex = sc.allocIndex()
		sc.textOpen = true
		sc.openBlocks = append(sc.openBlocks, sc.textIndex)
		out = append(out, sc.claudeChunks().ContentBlockStart(sc.textIndex, "text", map[string]any{"text": ""}))
	}
	out = append(out, sc.claudeChunks().ContentBlockDelta(sc.textIndex,
		map[string]any{"type": "text_delta", "text": delta}))
	return out, false, nil
}

// outputItem handles output_item.added/.done through the shared tracker:
// Chat Completions announces/replays tool_calls entries, Messages opens
// tool_use blocks with mandated pairing; non-function items are ignored.
// outputItem 追踪函数调用公布及完成事件，按需交付参数且避免重复。
func (sc *StreamConverter) outputItem(item *functionCallItem) ([][]byte, bool, *errclass.Error) {
	if item.Type != "function_call" {
		return nil, false, nil
	}
	sc.toolCallsSeen = true
	first, idx, deliver := sc.tracker.Observe(item.CallID, item.ID, item.Arguments != "")
	if sc.source == "openai" {
		switch {
		case first:
			// The announcement embeds complete arguments when the item
			// event already carries them.
			entryArgs := ""
			if deliver {
				entryArgs = item.Arguments
			}
			return [][]byte{sc.chatChunks().Delta(map[string]any{"tool_calls": []any{
				shared.CCToolCallOpeningEntry(idx, item.CallID, item.Name, entryArgs),
			}})}, false, nil
		case deliver:
			// Done for an already-announced call with no streamed arguments:
			// replay the complete arguments so none are lost (FR-006).
			return [][]byte{sc.chatChunks().Delta(map[string]any{"tool_calls": []any{map[string]any{
				"index": idx, "function": map[string]any{"arguments": item.Arguments},
			}}})}, false, nil
		}
		return nil, false, nil
	}
	var out [][]byte
	if first {
		out = append(out, sc.closeOpenBlocks()...)
		sc.openBlocks = append(sc.openBlocks, idx)
		out = append(out, sc.claudeChunks().ContentBlockStart(idx, "tool_use",
			map[string]any{"id": item.CallID, "name": item.Name, "input": map[string]any{}}))
	}
	if deliver {
		// Complete arguments are delivered the moment they are seen,
		// whether they arrive at added or at done (FR-006) — the
		// same decision table as the Chat Completions route.
		out = append(out, sc.argsDelta(idx, item.Arguments))
	}
	if len(out) == 0 {
		return nil, false, nil
	}
	return out, false, nil
}

// argsFragment streams one function_call_arguments.delta: Chat
// Completions feeds its accumulated tool_calls entry (defensively opening
// it when arguments precede the announcement), Messages mirrors that with
// input_json_delta blocks.
// argsFragment 转换参数分片，参数先到达时防御性创建工具调用。
func (sc *StreamConverter) argsFragment(itemID, delta string) ([][]byte, bool, *errclass.Error) {
	first, idx := sc.tracker.StreamArgs(itemID)
	if sc.source == "openai" {
		sc.toolCallsSeen = true
		if first {
			// Arguments before the announcement: open the entry so no
			// partial JSON is lost.
			return [][]byte{sc.chatChunks().Delta(map[string]any{"tool_calls": []any{
				shared.CCToolCallOpeningEntry(idx, itemID, "", delta),
			}})}, false, nil
		}
		return [][]byte{sc.chatChunks().Delta(map[string]any{"tool_calls": []any{map[string]any{
			"index": idx, "function": map[string]any{"arguments": delta},
		}}})}, false, nil
	}
	var out [][]byte
	if first {
		// Arguments before the item announcement: open the block with
		// the identifiers available so no partial JSON is lost.
		out = append(out, sc.closeOpenBlocks()...)
		sc.openBlocks = append(sc.openBlocks, idx)
		out = append(out, sc.claudeChunks().ContentBlockStart(idx, "tool_use",
			map[string]any{"id": itemID, "name": "", "input": map[string]any{}}))
	}
	out = append(out, sc.argsDelta(idx, delta))
	return out, false, nil
}

// terminal renders the completed/incomplete tail: Chat Completions gets a
// finish_reason chunk plus [DONE]; Messages gets close-before-stop block
// pairing, message_delta (stop_sequence omitted entirely) and
// message_stop. Shared precedence: tool calls outrank the status-derived
// reason, so response.incomplete cannot downgrade them.
// terminal 按目标协议关闭内容块，输出完成原因、用量及终止事件。
func (sc *StreamConverter) terminal(incomplete bool, in, out int, details shared.UsageDetails) ([][]byte, bool, *errclass.Error) {
	st := "completed"
	if incomplete {
		st = "incomplete"
	}
	if sc.source == "openai" {
		finish := shared.TerminalReason(sc.toolCallsSeen, "tool_calls", shared.CCFinishFromResponseStatus(st))
		// Always attached (F-R6 parity with the Messages route's terminal
		// chunk): typed clients prefer a stable schema, zero-valued fields
		// when upstream reported none. Shared kernel keeps total_tokens
		// consistent with the non-stream Chat Completions mapper.
		chunk := sc.chatChunks().Finish(finish, shared.CCUsageFrom(int64(in), int64(out), details))
		return [][]byte{chunk}, true, nil
	}
	statusStop := shared.ClaudeStopFromResponseStatus(st)
	stop := shared.TerminalReason(sc.toolCallsSeen, "tool_use", statusStop)
	cacheRead, cacheWrite := details.CachedTokens, details.CacheWriteTokens
	em := sc.claudeChunks()
	var events [][]byte
	for _, idx := range sc.openBlocks {
		events = append(events, em.ContentBlockStop(idx))
	}
	sc.openBlocks = nil
	events = append(events,
		em.MessageDelta(&stop, shared.ClaudeUsage(shared.ClampSubtract(int64(in), cacheRead, cacheWrite), int64(out), cacheRead, cacheWrite)),
		em.MessageStop(),
	)
	return events, true, nil
}

// chatChunks binds the shared chunk kernel to the captured upstream
// response identity so this route's frames cannot diverge from the
// sibling Messages-route synthesizer (FR-006); a zero Created defaults
// to render-time now inside the kernel.
// chatChunks 将共享 Chat Completions 增量生成器绑定到当前响应身份。
func (sc *StreamConverter) chatChunks() shared.ChatChunkBuilder {
	return shared.ChatChunkBuilder{ID: sc.id, Model: sc.model, Created: sc.created}
}

// claudeChunks binds the shared Claude emitter kernel to the captured
// upstream response identity so this route's Messages frames cannot
// diverge from the sibling Chat Completions-route synthesizer (FR-006).
// claudeChunks 将共享 Messages 事件生成器绑定到当前响应身份。
func (sc *StreamConverter) claudeChunks() shared.ClaudeEventEmitter {
	return shared.NewClaudeEventEmitter(sc.id, sc.model)
}

// closeOpenBlocks emits content_block_stop for every currently open
// content block and resets open-block tracking, so the next
// content_block_start obeys Anthropic's close-before-next-start rule;
// the text block is marked closed so a later text delta reopens a fresh
// block (stop/reopen parity with the Chat Completions route).
// closeOpenBlocks 关闭所有打开的内容块，确保下一内容块开始前完成配对。
func (sc *StreamConverter) closeOpenBlocks() [][]byte {
	if len(sc.openBlocks) == 0 {
		return nil
	}
	em := sc.claudeChunks()
	var out [][]byte
	for _, idx := range sc.openBlocks {
		out = append(out, em.ContentBlockStop(idx))
	}
	sc.openBlocks = nil
	sc.textOpen = false
	return out
}

// argsDelta renders one input_json_delta content block delta.
// argsDelta 构造工具参数 JSON 分片事件。
func (sc *StreamConverter) argsDelta(index int, partial string) []byte {
	return sc.claudeChunks().ContentBlockDelta(index,
		map[string]any{"type": "input_json_delta", "partial_json": partial})
}

// captureResponse records stream identity from response.created.
// captureResponse 捕获响应创建事件中的响应标识、模型及创建时间。
func (sc *StreamConverter) captureResponse(resp *responseMeta) {
	if resp.ID != "" {
		sc.id = resp.ID
	}
	if resp.Model != "" {
		sc.model = resp.Model
	}
	sc.created = int64(resp.CreatedAt)
}

// failureError maps response.failed / error events onto FR-009 classes,
// honoring an upstream status_code when present (§7 semantics); without
// one the failure is treated as a retryable upstream server failure.
// failureError 依据上游状态分类失败，缺省时按可重试服务器错误处理。
func failureError(ev *failureEvent) *errclass.Error {
	status := int(ev.StatusCode)
	if status == 0 {
		status = int(ev.Response.StatusCode)
	}
	msg := ev.Message
	if msg == "" {
		msg = ev.Response.Error.Message
	}
	if status > 0 {
		return errclass.FromStatus(status, msg)
	}
	return errclass.UpstreamFallback(msg)
}
