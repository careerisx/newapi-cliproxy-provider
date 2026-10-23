// 流转换按接收顺序维护协议状态；事件公布、内容增量和收尾使用稳定索引。
package chatcompletions

import (
	"bytes"
	"encoding/json"
	"strconv"
	"strings"

	"newapi-provider/internal/adapter/shared"
	"newapi-provider/internal/errclass"
)

// StreamConverter incrementally converts an upstream Chat Completions
// SSE stream into the client protocol's stream (FR-006, AC §B): "openai"
// frames pass through verbatim (with reasoning_content backfilled from
// vendor reasoning spellings), "claude" synthesizes Anthropic Messages
// events, "openai-response" synthesizes Responses events. Partial SSE
// lines are buffered across Feed calls; tool-call argument fragments are
// accumulated per tool_calls index until the stream terminates.
//
// A converter is safe for SEQUENTIAL Feed calls only — it is not
// goroutine-safe; use one converter per upstream stream. Synthesis
// follows choices[0] (upstream serves n=1 routes).
type StreamConverter struct {
	// sourceFormat 客户端来源协议，决定输出格式。
	sourceFormat string
	// lineBuf 跨分片保留的未完成 SSE 行。
	lineBuf      []byte // partial SSE line carried across Feed calls
	// done 是否收到上游终止标记。
	done         bool

	// started 是否已公布流开始事件。
	started      bool // message_start / first chunk seen
	// id 响应或工具调用的稳定标识。
	id           string
	// model 上游模型标识。
	model        string
	// claudeEm 绑定响应身份的 Messages 事件生成器。
	claudeEm     shared.ClaudeEventEmitter // canonical Messages frames bound to first-chunk identity
	// textOpen 当前文本内容块是否打开。
	textOpen     bool                      // claude text content_block open
	// textIndex 当前文本内容块索引。
	textIndex    int                       // claude index of the currently-open text block
	// thinkOpen 当前思考内容块是否打开。
	thinkOpen    bool                      // claude thinking content_block open
	// thinkIndex 当前思考内容块索引。
	thinkIndex   int                       // claude index of the currently-open thinking block
	// nextIndex 下一个可分配的输出索引。
	nextIndex    int                       // next output/block index
	// msgIndex 聚合消息输出索引，负数表示未公布。
	msgIndex     int                       // announced assistant message item index (-1 until text)
	// thinkItemID 已公布的推理项标识。
	thinkItemID  string                    // announced reasoning item identity ("" until reasoning)
	// thinkIndexIn 推理项输出索引，负数表示未公布。
	thinkIndexIn int                       // announced reasoning output index (-1 until reasoning)
	// thinkStopped 是否已发送推理项收尾事件。
	thinkStopped bool                      // reasoning done transitions already emitted
	// thinkSeen 是否观察到推理文本。
	thinkSeen    bool                      // any reasoning text observed (terminal item inclusion)
	// thinkBuf 累计推理摘要文本。
	thinkBuf     strings.Builder           // aggregated reasoning summary text
	// tools 按上游索引保存的工具调用状态。
	tools        map[int64]*streamTool
	// toolOrder 工具调用首次出现的顺序。
	toolOrder    []int64 // upstream tool_call indices in first-arrival order
	// toolsSeen 是否观察到工具调用，用于停止原因优先级。
	toolsSeen    bool    // any tool_calls entry observed (terminal-reason precedence)
	// usage 最新捕获的上游用量。
	usage        *ccUsage
	// finished 是否已处理完成原因。
	finished     bool            // finish_reason processed
	// heldFinish 为等待用量尾帧而暂存的完成原因。
	heldFinish   string          // finish_reason awaiting terminal emission (flushed on the next data line or [DONE], so the standard include_usage trailer lands in the terminal event; Flush covers close-without-[DONE])
	// terminalSent 是否已输出终止增量。
	terminalSent bool            // claudeTerminal already emitted message_delta (Flush must still close with message_stop)
	// flushed 是否已执行提前关闭收尾。
	flushed      bool            // Flush already ran (one-shot guard)
	// respText 累计 Responses 输出文本。
	respText     strings.Builder // openai-response accumulated output_text
}

// streamTool accumulates one upstream tool_calls index; args collects
// argument fragments so the terminal response.completed output carries the
// complete call (FR-006).
type streamTool struct {
	// blockIndex 工具内容块或输出项的稳定索引。
	blockIndex int
	// id 响应或工具调用的稳定标识。
	id         string
	// itemID 用于关联 Responses 项目事件的标识。
	itemID     string // fc_-prefixed Responses item id shared by deltas and done events
	// name 工具函数名。
	name       string
	// args 累计工具参数分片。
	args       strings.Builder
	// stopped 内容块关闭事件是否已发送。
	stopped    bool // content_block_stop emitted
}

// NewStreamConverter returns a converter translating Chat Completions
// SSE into sourceFormat's stream shape.
// NewStreamConverter 创建每条上游流独享的转换器，仅允许顺序使用。
func NewStreamConverter(sourceFormat string) *StreamConverter {
	return &StreamConverter{
		sourceFormat: sourceFormat,
		msgIndex:     -1,
		thinkIndexIn: -1,
		tools:        map[int64]*streamTool{},
	}
}

// Feed consumes one upstream chunk (any split of the byte stream),
// returning the client-protocol events completed by this chunk, whether
// the stream is done ([DONE] seen), and a classified error for
// malformed chunks (FR-006). Errors carry at most an 80-character
// redacted snippet — never the full upstream body.
// Feed 按字节顺序接收网络分片，输出完整事件、终止状态或分类错误。
func (sc *StreamConverter) Feed(chunk []byte) (events [][]byte, done bool, eErr *errclass.Error) {
	sc.lineBuf = append(sc.lineBuf, chunk...)
	for {
		i := bytes.IndexByte(sc.lineBuf, '\n')
		if i < 0 {
			break // keep trailing partial line buffered
		}
		line := strings.TrimSuffix(string(sc.lineBuf[:i]), "\r")
		sc.lineBuf = sc.lineBuf[i+1:]
		ev, eErr := sc.handleLine(line)
		if eErr != nil {
			return events, false, eErr
		}
		events = append(events, ev...)
		if sc.done {
			break
		}
	}
	return events, sc.done, nil
}

// handleLine 根据客户端协议分派完整的上游 SSE 行。
func (sc *StreamConverter) handleLine(line string) ([][]byte, *errclass.Error) {
	switch sc.sourceFormat {
	case "openai":
		return sc.passthroughLine(line), nil
	case "claude":
		return sc.claudeLine(line)
	case "openai-response":
		return sc.responsesLine(line)
	default:
		return nil, shared.UnsupportedFormat(sc.sourceFormat, EndpointPath)
	}
}

// Flush terminates a stream whose upstream closed without [DONE]: the
// finish_reason chunk may legally be the last data line before close, and
// without this the client never sees message_delta/message_stop or
// response.completed — nor usage stranded behind the deferred terminal
// (FR-006). Claude synthesis appends message_stop after the held
// message_delta; Responses synthesis emits response.completed. One-shot:
// later calls return nothing, as does any call with no pending terminal.
// Flush 在上游提前关闭时释放待处理终止事件，避免重复收尾。
func (sc *StreamConverter) Flush() [][]byte {
	if sc.flushed {
		return nil
	}
	sc.flushed = true
	switch sc.sourceFormat {
	case "claude":
		events := sc.claudeTerminal()
		if len(events) == 0 {
			if !sc.terminalSent {
				return nil
			}
			// The post-finish include_usage trailer already delivered the
			// terminal via claudeLine; close-without-[DONE] must still emit
			// message_stop or Anthropic clients hang until timeout.
			return [][]byte{sc.claudeEm.MessageStop()}
		}
		return append(events, sc.claudeEm.MessageStop())
	case "openai-response":
		return sc.responsesTerminal()
	default:
		return nil
	}
}

// sseData extracts a data-line payload; ok is false for non-data lines
// (comments, event:, retry:, empty lines), which converters ignore.
//
// KNOWN LIMITATION (audited, accepted): this line-oriented parser treats
// each data: line as one complete payload, so a legal multi-data-line SSE
// frame whose lines form a single JSON document decodes as separate
// fragments and fails unmarshal with a spurious translation error. Real
// Chat Completions upstreams emit single-data-line frames, and the shared
// SSEFramer migration was declined because its frame-granular batching
// conflicts with passthroughLine's line-immediate [DONE] semantics.
// sseData 提取 SSE 数据行，忽略注释、事件名和空行。
func sseData(line string) (payload string, ok bool) {
	rest, ok := strings.CutPrefix(line, "data:")
	if !ok {
		return "", false
	}
	return strings.TrimSpace(rest), true
}

// passthroughLine extracts the bare data payload for openai-target emissions;
// [DONE] terminates the stream without emitting a chunk (CPA's WriteDone
// automatically writes the trailing data: [DONE]). Reasoning text arriving
// under a vendor spelling is mirrored onto the standard reasoning_content
// member so clients that only read that member still see thinking; every
// other payload forwards byte-identical.
// passthroughLine 透传原生数据载荷并兼容推理字段，终止标记交由宿主处理。
func (sc *StreamConverter) passthroughLine(line string) [][]byte {
	data, ok := sseData(line)
	if !ok {
		return nil
	}
	if shared.IsSSEDone(data) {
		sc.done = true
		return nil
	}
	if data == "" {
		return nil
	}
	if fixed := shared.BackfillReasoningContent([]byte(data), "delta"); fixed != nil {
		return [][]byte{fixed}
	}
	return [][]byte{[]byte(data)}
}

// ---- upstream Chat Completions chunk shape ----

type ccToolCallDelta struct {
	// Index 上游内容块或工具调用索引。
	Index    int64  `json:"index"`
	// ID 响应、内容块或工具调用的协议标识。
	ID       string `json:"id"`
	// Function 工具函数定义或调用内容。
	Function struct {
		// Name 工具函数名。
		Name      string `json:"name"`
		// Arguments 函数调用参数的 JSON 字符串。
		Arguments string `json:"arguments"`
	} `json:"function"`
}

type ccDelta struct {
	// Content 原始或归一化的消息内容。
	Content   string            `json:"content"`
	// ToolCalls 消息携带的函数调用列表。
	ToolCalls []ccToolCallDelta `json:"tool_calls"`
	// Reasoning carries the vendor thinking spellings (reasoning /
	// reasoning_details[].text / reasoning_content); embedded untagged so
	// the shared kernel decodes all three at once.
	// ReasoningFields 统一解码上游兼容推理文本字段。
	shared.ReasoningFields
}

type ccChunkChoice struct {
	// Delta 流式事件增量载荷。
	Delta        ccDelta `json:"delta"`
	// FinishReason Chat Completions 完成原因。
	FinishReason string  `json:"finish_reason"`
}

type ccChunk struct {
	// ID 响应、内容块或工具调用的协议标识。
	ID      string          `json:"id"`
	// Model 上游模型标识。
	Model   string          `json:"model"`
	// Choices 上游候选结果列表。
	Choices []ccChunkChoice `json:"choices"`
	// Usage 令牌用量及可选缓存详情。
	Usage   *ccUsage        `json:"usage"`
	// Error 上游错误对象。
	Error   *ccChunkError   `json:"error"`
}

// ccChunkError is an in-stream upstream error object; choice-less chunks
// carrying one are failures, not empty deltas.
type ccChunkError struct {
	// Message 响应消息或已脱敏错误说明。
	Message string `json:"message"`
	// Type 协议对象或内容分部类型。
	Type    string `json:"type"`
	// Code 上游错误码。
	Code    any    `json:"code"`
}

// decodeChunk parses one data payload; malformed JSON yields a
// translation failure carrying only a short redacted snippet (FR-006,
// §5 security: no upstream body echo). A non-empty upstream error object
// fails the conversion with a classified error so the fail path closes
// both streams instead of the chunk being silently dropped as a
// zero-choices delta.
//
// Usage is captured here — before any empty-choices early return in the
// format converters: OpenAI's stream_options.include_usage terminal
// chunk is exactly choice-less-with-usage, and dropping it would lose
// the final counts.
// decodeChunk 解析上游增量及流内错误，并在忽略空候选前捕获用量。
func (sc *StreamConverter) decodeChunk(data string) (*ccChunk, *errclass.Error) {
	var chunk ccChunk
	if err := json.Unmarshal([]byte(data), &chunk); err != nil {
		return nil, errclass.Translation("malformed Chat Completions stream chunk: " + shared.RedactedSnippet(data))
	}
	if chunk.Error != nil {
		return nil, chunkError(chunk.Error)
	}
	if chunk.Usage != nil {
		sc.usage = chunk.Usage
	}
	return &chunk, nil
}

// chunkError classifies an in-stream Chat Completions error object onto
// FR-009 classes, mirroring the sibling adapters' stream-error paths:
// a numeric code (or rate-limit marker) maps through FromStatus so §7
// retryability semantics apply; anything else is a retryable upstream
// server failure.
// chunkError 依据流内状态码或限流标记构造统一错误。
func chunkError(e *ccChunkError) *errclass.Error {
	status := 0
	switch c := e.Code.(type) {
	case float64:
		status = int(c)
	case string:
		if n, err := strconv.Atoi(c); err == nil {
			status = n
		} else if strings.Contains(c, "rate_limit") {
			status = 429
		}
	}
	if status == 0 && strings.Contains(e.Type, "rate_limit") {
		status = 429
	}
	if status > 0 {
		return errclass.FromStatus(status, e.Message)
	}
	return errclass.UpstreamFallback(e.Message)
}

// ---- Chat Completions -> claude (Anthropic Messages) stream (FR-006) ----

// claudeLine synthesizes Messages events from one chunk: the first
// chunk opens message_start, delta.content opens the text content block
// lazily on first non-empty delta (tool-only streams therefore carry
// tool_use from index 0 like non-stream chatToClaude) and becomes
// text_delta (reopening a new text block if tool_use starts
// closed it), tool_calls argument fragments open tool_use
// blocks and stream input_json_delta, finish_reason closes blocks and
// holds message_delta with the mapped stop reason until the next data
// line or [DONE] (OpenAI's standard include_usage trailer follows the
// finish_reason chunk and must land in the terminal event), and [DONE]
// emits message_stop. Vendor thinking text opens the leading thinking
// block, which every later block start closes — Anthropic's block order is
// thinking, text, tool_use, one block open at a time.
// claudeLine 按内容块生命周期合成 Messages 增量，并延迟终止以接收用量。
func (sc *StreamConverter) claudeLine(line string) ([][]byte, *errclass.Error) {
	data, ok := sseData(line)
	if !ok || data == "" {
		return nil, nil
	}
	if shared.IsSSEDone(data) {
		sc.done = true
		events := sc.stopBlocks()
		events = append(events, sc.claudeTerminal()...)
		return append(events, sc.claudeEm.MessageStop()), nil
	}
	chunk, eErr := sc.decodeChunk(data)
	if eErr != nil {
		return nil, eErr
	}
	var events [][]byte
	if sc.heldFinish != "" {
		// A post-finish chunk (typically the include_usage trailer)
		// flushes the deferred terminal so it carries the final counts.
		events = append(events, sc.claudeTerminal()...)
	}
	if !sc.started && len(chunk.Choices) > 0 {
		sc.started = true
		input := int64(0)
		if chunk.Usage != nil {
			input = chunk.Usage.PromptTokens
		}
		sc.claudeEm = shared.NewClaudeEventEmitter(chunk.ID, chunk.Model)
		events = append(events, sc.claudeEm.MessageStart(input))
	}
	if len(chunk.Choices) == 0 {
		return events, nil
	}
	choice := chunk.Choices[0]
	if thinking, ok := choice.Delta.ReasoningText(); ok {
		// Vendor thinking spellings become the leading thinking block. A
		// reopen (thinking after text or tools) closes every open block
		// first, the same one-open-block rule the text path follows.
		if !sc.thinkOpen {
			events = append(events, sc.stopBlocks()...)
			sc.thinkIndex = sc.nextIndex
			sc.nextIndex++
			sc.thinkOpen = true
			events = append(events, sc.claudeEm.ThinkingBlockStart(sc.thinkIndex))
		}
		events = append(events, sc.claudeEm.ThinkingDelta(sc.thinkIndex, thinking))
	}
	if choice.Delta.Content != "" {
		// The text block opens lazily on first non-empty content so pure
		// tool-call streams carry no phantom empty text block, matching
		// non-stream chatToClaude's block shape. Every content_block_start
		// (this reopen and a first-of-a-new-index tool_use start below)
		// closes ALL currently-open blocks first — open text via stopText
		// and sibling tool_use blocks ascending — mirroring responses'
		// closeOpenBlocks discipline, because Anthropic forbids two
		// concurrently-open blocks and legal Chat Completions interleaving
		// would otherwise hold both open.
		if !sc.textOpen {
			events = append(events, sc.stopBlocks()...)
			sc.textIndex = sc.nextIndex
			sc.nextIndex++
			sc.textOpen = true
			events = append(events, sc.claudeEm.ContentBlockStart(sc.textIndex, "text", map[string]any{"text": ""}))
		}
		events = append(events, sc.claudeEm.ContentBlockDelta(sc.textIndex,
			map[string]any{"type": "text_delta", "text": choice.Delta.Content}))
	}
	for _, tc := range choice.Delta.ToolCalls {
		t := sc.tools[tc.Index]
		if t == nil {
			events = append(events, sc.stopBlocks()...)
			t = &streamTool{blockIndex: sc.nextIndex, id: tc.ID, name: tc.Function.Name}
			sc.nextIndex++
			sc.tools[tc.Index] = t
			sc.toolsSeen = true
			sc.toolOrder = append(sc.toolOrder, tc.Index)
			events = append(events, sc.claudeEm.ContentBlockStart(t.blockIndex, "tool_use",
				map[string]any{"id": t.id, "name": t.name, "input": map[string]any{}}))
		}
		if tc.Function.Arguments != "" {
			events = append(events, sc.claudeEm.ContentBlockDelta(t.blockIndex,
				map[string]any{"type": "input_json_delta", "partial_json": tc.Function.Arguments}))
		}
	}
	if choice.FinishReason != "" && !sc.finished {
		sc.finished = true
		sc.heldFinish = choice.FinishReason
		events = append(events, sc.stopBlocks()...)
	}
	return events, nil
}

// claudeTerminal renders the held message_delta exactly once, carrying
// the captured usage when any arrived before emission.
// claudeTerminal 仅输出一次延迟终止增量，并附加最终捕获的用量。
func (sc *StreamConverter) claudeTerminal() [][]byte {
	finish := sc.heldFinish
	sc.heldFinish = ""
	if finish == "" {
		return nil
	}
	sc.terminalSent = true
	// Observed tool calls outrank the status-derived reason, mirroring
	// non-stream chatToClaude (FR-006).
	reason := shared.TerminalReason(sc.toolsSeen, "tool_use", shared.FinishToClaudeStop(finish))
	// Always attach: sibling terminals never omit usage; fields zero when
	// upstream reported none (F-R6).
	input, output := int64(0), int64(0)
	var cacheRead *int64
	if sc.usage != nil {
		input = sc.usage.PromptTokens
		output = sc.usage.CompletionTokens
		if sc.usage.PromptDetails != nil {
			cacheRead = sc.usage.PromptDetails.CachedTokens
		}
	}
	return [][]byte{sc.claudeEm.MessageDelta(&reason,
		shared.ClaudeUsage(shared.ClampSubtract(input, cacheRead), output, cacheRead, nil))}
}

// stopThinking closes the open thinking content block, if any.
// stopThinking 关闭当前思考块，保证开始与结束配对。
func (sc *StreamConverter) stopThinking() [][]byte {
	if !sc.thinkOpen {
		return nil
	}
	sc.thinkOpen = false
	return [][]byte{sc.claudeEm.ContentBlockStop(sc.thinkIndex)}
}

// stopText closes the open text content block, if any.
// stopText 关闭当前文本块，允许后续文本重新打开新块。
func (sc *StreamConverter) stopText() [][]byte {
	if !sc.textOpen {
		return nil
	}
	sc.textOpen = false
	return [][]byte{sc.claudeEm.ContentBlockStop(sc.textIndex)}
}

// stopBlocks closes the thinking, text, and every open tool_use block
// exactly once in Anthropic block order; called before every
// content_block_start (so blocks never overlap) and again (as a no-op) on
// finish_reason and [DONE].
// stopBlocks 依次关闭所有打开的内容块，避免块重叠和重复结束。
func (sc *StreamConverter) stopBlocks() [][]byte {
	var events [][]byte
	events = append(events, sc.stopThinking()...)
	events = append(events, sc.stopText()...)
	for _, idx := range sc.toolOrder {
		t := sc.tools[idx]
		if t != nil && !t.stopped {
			t.stopped = true
			events = append(events, sc.claudeEm.ContentBlockStop(t.blockIndex))
		}
	}
	return events
}

// ---- Chat Completions -> openai-response (Responses) stream (FR-006) ----

// responsesLine synthesizes Responses events: delta.content becomes
// response.output_text.delta, tool_calls argument fragments become
// response.function_call_arguments.delta keyed by item_id, and
// finish_reason holds response.completed — emitted at the next data
// line or [DONE] with prompt/completion tokens mapped to input/output
// usage (the standard include_usage trailer follows the finish_reason
// chunk); [DONE] terminates the stream. Vendor thinking text becomes the
// leading reasoning item with its summary events (host-translator parity),
// closed before the first text or function_call item is announced.
// responsesLine 合成 Responses 输出项生命周期及增量事件。
func (sc *StreamConverter) responsesLine(line string) ([][]byte, *errclass.Error) {
	data, ok := sseData(line)
	if !ok || data == "" {
		return nil, nil
	}
	if shared.IsSSEDone(data) {
		sc.done = true
		events := sc.stopReasoning()
		return append(events, sc.responsesTerminal()...), nil
	}
	chunk, eErr := sc.decodeChunk(data)
	if eErr != nil {
		return nil, eErr
	}
	var events [][]byte
	if sc.heldFinish != "" {
		// A post-finish chunk (typically the include_usage trailer)
		// flushes the deferred terminal so it carries the final counts.
		events = append(events, sc.responsesTerminal()...)
	}
	if !sc.started && len(chunk.Choices) > 0 {
		sc.started = true
		sc.id = chunk.ID
		sc.model = chunk.Model
		// Lifecycle parity with the Responses protocol: announce the
		// response before any deltas reference it (F18).
		events = append(events, sc.responsesEm().Created())
	}
	if len(chunk.Choices) == 0 {
		return events, nil
	}
	choice := chunk.Choices[0]
	if thinking, ok := choice.Delta.ReasoningText(); ok {
		if !sc.thinkSeen {
			sc.thinkSeen = true
			sc.thinkIndexIn = sc.nextIndex
			sc.nextIndex++
			sc.thinkItemID = shared.ReasoningItemID(sc.id, 0)
			events = append(events, sc.responsesEm().ReasoningItemAdded(sc.thinkItemID, sc.thinkIndexIn))
			events = append(events, sc.responsesEm().ReasoningPartAdded(sc.thinkItemID, sc.thinkIndexIn))
		}
		sc.thinkBuf.WriteString(thinking)
		events = append(events, sc.responsesEm().ReasoningSummaryDelta(sc.thinkItemID, sc.thinkIndexIn, thinking))
	}
	if choice.Delta.Content != "" {
		events = append(events, sc.stopReasoning()...)
		if sc.msgIndex < 0 {
			sc.msgIndex = sc.nextIndex
			sc.nextIndex++
			events = append(events, sc.responsesEm().ItemAdded(sc.msgIndex, map[string]any{
				"type": "message", "role": "assistant", "id": sc.id, "content": []any{},
			}))
		}
		sc.respText.WriteString(choice.Delta.Content)
		events = append(events, sc.responsesEm().TextDelta(sc.id, sc.msgIndex, choice.Delta.Content))
	}
	for _, tc := range choice.Delta.ToolCalls {
		t := sc.tools[tc.Index]
		if t == nil {
			events = append(events, sc.stopReasoning()...)
			t = &streamTool{blockIndex: sc.nextIndex, id: tc.ID, itemID: shared.FunctionCallItemID(tc.ID), name: tc.Function.Name}
			sc.nextIndex++
			sc.tools[tc.Index] = t
			sc.toolsSeen = true
			sc.toolOrder = append(sc.toolOrder, tc.Index)
			// Announce the item before any arguments delta references it.
			// The fc_ item id is what clients use to bind the later done event.
			events = append(events, sc.responsesEm().ItemAdded(t.blockIndex, map[string]any{
				"type": "function_call", "id": t.itemID, "call_id": t.id,
				"name": t.name, "arguments": "",
			}))
		}
		if tc.Function.Arguments != "" {
			t.args.WriteString(tc.Function.Arguments)
			events = append(events, sc.responsesEm().ArgsDelta(t.itemID, t.blockIndex, tc.Function.Arguments))
		}
	}
	if choice.FinishReason != "" && !sc.finished {
		sc.finished = true
		sc.heldFinish = choice.FinishReason
		events = append(events, sc.stopReasoning()...)
	}
	return events, nil
}

// stopReasoning emits the reasoning item's closing transitions exactly
// once, after the summary text is complete: text done, part done, item
// done. Text, tool calls, finish_reason, and [DONE] all trigger it, so a
// thinking run always closes before the next item is announced
// (host-translator parity).
// stopReasoning 按摘要文本、摘要分部及推理项顺序发送一次收尾事件。
func (sc *StreamConverter) stopReasoning() [][]byte {
	if !sc.thinkSeen || sc.thinkStopped {
		return nil
	}
	sc.thinkStopped = true
	text := sc.thinkBuf.String()
	em := sc.responsesEm()
	return [][]byte{
		em.ReasoningSummaryDone(sc.thinkItemID, sc.thinkIndexIn, text),
		em.ReasoningPartDone(sc.thinkItemID, sc.thinkIndexIn, text),
		em.ReasoningItemDone(sc.thinkItemID, sc.thinkIndexIn, text),
	}
}

// responsesEm binds the shared Responses emitter kernel to the captured
// upstream chunk identity so this route's frames cannot diverge from the
// sibling Messages-route synthesizer (FR-006).
// responsesEm 将共享 Responses 事件生成器绑定到当前响应身份。
func (sc *StreamConverter) responsesEm() shared.ResponsesEventEmitter {
	return shared.ResponsesEventEmitter{ID: sc.id, Model: sc.model}
}

// responsesTerminal renders the held response.completed exactly once,
// carrying the captured usage when any arrived before emission.
// responsesTerminal 输出延迟的 Responses 完成事件，保留项目顺序和最终用量。
func (sc *StreamConverter) responsesTerminal() [][]byte {
	finish := sc.heldFinish
	sc.heldFinish = ""
	if finish == "" {
		return nil
	}
	// Status derives positionally from finish alone: length→incomplete,
	// else completed. Tool calls are represented by output items, not
	// status vocabulary (F-R2, Messages-route parity).
	status := shared.ResponseStatusFromCCFinish(finish)
	// Shared assembler (FR-006 sibling parity): the terminal payload
	// carries the output items fed in ARRIVAL order so Render() reproduces
	// the streamed output_item.added indexes exactly — the message slot is
	// pinned at sc.msgIndex (assigned at the first text delta, after any
	// tools-first announcements), never displacing already-announced
	// function_call items; this mirrors the Messages-route invariant that
	// terminal output order equals announcement order (W4 pin).
	oa := shared.NewOutputAssembler(sc.id)
	// The reasoning item renders first: it was announced before the message
	// and function_call items, so its array position keeps the streamed
	// output_index values valid.
	if sc.thinkSeen {
		oa.AppendReasoning(shared.NewRespReasoningItem(sc.thinkItemID, sc.thinkBuf.String()))
	}
	reserved := false
	for _, idx := range sc.toolOrder {
		t := sc.tools[idx]
		if sc.msgIndex >= 0 && !reserved && t.blockIndex > sc.msgIndex {
			oa.ReserveTextSlot()
			reserved = true
		}
		oa.AppendFunctionCall(t.id, t.name, shared.DefaultArgs(t.args.String()))
	}
	if sc.msgIndex >= 0 && !reserved {
		oa.ReserveTextSlot()
	}
	oa.AddText(sc.respText.String())
	// Always attach (F-R6): zero-valued fields when upstream sent none.
	input, outputTokens := int64(0), int64(0)
	if sc.usage != nil {
		input = sc.usage.PromptTokens
		outputTokens = sc.usage.CompletionTokens
	}
	var details shared.UsageDetails
	if sc.usage != nil {
		if sc.usage.PromptDetails != nil {
			details.CachedTokens = sc.usage.PromptDetails.CachedTokens
		}
		if sc.usage.CompletionDetails != nil {
			details.ReasoningTokens = sc.usage.CompletionDetails.ReasoningTokens
		}
	}
	usage := shared.NewResponsesUsageFrom(input, outputTokens, details)
	em := sc.responsesEm()
	events := make([][]byte, 0, len(sc.toolOrder)*2+1)
	for _, idx := range sc.toolOrder {
		t := sc.tools[idx]
		if t == nil {
			continue
		}
		args := shared.DefaultArgs(t.args.String())
		events = append(events, em.CloseFunctionCall(t.blockIndex, shared.RespItem{
			Type: "function_call", ID: t.itemID, CallID: t.id, Name: t.name, Arguments: args,
		})...)
	}
	return append(events, em.Completed(status, usage, oa.Render()))
}
