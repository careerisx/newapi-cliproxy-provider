// Non-stream and stream execution paths (FR-005/FR-006/FR-007, arch §4/§5
// steps 5-8): resolve the public model ID against the catalog snapshot,
// translate the inbound payload to the record's upstream protocol, call
// upstream through the host HTTP callbacks, and translate back to the
// client protocol. Request authentication is selected by CPA.

package plugin

import (
  "context"
  "encoding/json"
  "net/http"
  "strings"
  "sync"
  "sync/atomic"
  "time"

  "github.com/router-for-me/CLIProxyAPI/v8/sdk/pluginabi"
  "github.com/router-for-me/CLIProxyAPI/v8/sdk/pluginapi"

  "newapi-provider/internal/adapter/chatcompletions"
  "newapi-provider/internal/adapter/messages"
  "newapi-provider/internal/adapter/responses"
  "newapi-provider/internal/adapter/shared"
  "newapi-provider/internal/catalog"
  "newapi-provider/internal/config"
  "newapi-provider/internal/errclass"
)

// executorRequest mirrors rpcExecutorRequest: the SDK embeds
// pluginapi.ExecutorRequest untagged, so its fields marshal under Go field
// names ("Model", "SourceFormat", "OriginalRequest", "Stream"). StreamID is
// the DOWNSTREAM host-allocated id for executor.execute_stream emissions;
// ids returned by DoStream are UPSTREAM and never interchangeable (§4).
type executorRequest struct {
  // ExecutorRequest 保留宿主协议、选定认证与原始请求信息。
  pluginapi.ExecutorRequest
  // StreamID 宿主管理的流标识，上下游不可混用。
  StreamID string `json:"stream_id,omitempty"`
  // HostCallbackID 透传本次执行的宿主 HTTP 上下文。
  HostCallbackID string `json:"host_callback_id,omitempty"`
}

// resolvedExecution carries everything both execution paths need after
// model/key resolution succeeded.
type resolvedExecution struct {
  // cfg 是宿主本次选中认证的配置快照。
  cfg config.Config
  // rec 本次执行使用的目录模型快照。
  rec catalog.ModelRecord
  // key 宿主为本次请求选中的 API 密钥，禁止记录。
  key string
}

// resolveExecution resolves the requested model against the snapshot and
// extracts the key selected by CPA. A non-nil second return is a ready-made
// failure envelope.
// resolveExecution 按本次认证版本解析配置、凭据和当前认证的模型映射。
func (m *Manager) resolveExecution(req executorRequest) (*resolvedExecution, []byte) {
  if req.AuthProvider != ProviderID {
    return nil, classEnvelope(&errclass.Error{Class: errclass.ClassAuth, Message: "selected auth provider is not newapi"})
  }
  auth, err := resolveAuth(req.StorageJSON, req.AuthMetadata, req.AuthAttributes)
  if err != nil {
    return nil, classEnvelope(&errclass.Error{Class: errclass.ClassAuth, Message: err.Error()})
  }
  ctx := context.WithValue(context.Background(), quotaCallbackContextKey{}, req.HostCallbackID)
  mgr, err := m.catalogForAuth(ctx, req.AuthID, auth)
  if err != nil {
    return nil, classEnvelope(&errclass.Error{Class: errclass.ClassAuth, Message: err.Error()})
  }
  cfg, key := auth.cfg, auth.key
  var rec catalog.ModelRecord
  var found bool
  if mgr != nil && req.Model != "" {
    rec, found = mgr.Lookup(req.Model)
  }
  if !found {
    return nil, classEnvelope(&errclass.Error{
      Class:      errclass.ClassInvalidModel,
      Message:    "model not in routable catalog",
      StatusCode: http.StatusNotFound,
    })
  }
  return &resolvedExecution{cfg: cfg, rec: rec, key: key}, nil
}

// handleExecute implements executor.execute (non-stream). Stream-flagged
// requests are routed to the stream path instead of rejected.
// handleExecute 执行非流式推理并转换响应，流式标记请求转交流式入口。
func (m *Manager) handleExecute(request []byte) ([]byte, error) {
  var req executorRequest
  if err := decodeAuthRequest(request, &req); err != nil {
    return ErrEnvelope("invalid_request", "malformed executor request body"), nil
  }
  debugTrace("executor invoked model=%s source_format=%s stream=%t original_body_%s payload_%s", req.Model, req.SourceFormat, req.Stream, debugBodyMeta(req.OriginalRequest), debugBodyMeta(req.Payload))
  if req.Stream {
    return m.executeStream(req)
  }
  debugBody("request-original", req.Model, req.OriginalRequest)
  res, failEnv := m.resolveExecution(req)
  if res == nil {
    return failEnv, nil
  }
  upstreamBody, eErr := buildUpstreamRequest(res.cfg, res.rec, req.SourceFormat, req.OriginalRequest)
  if eErr != nil {
    return classEnvelope(eErr), nil
  }

  url := catalog.JoinUpstreamURL(res.cfg.BaseURL, res.rec.EndpointPath)
  // URL 拼接拒绝非法基础地址或端点时立即失败，不向宿主发送空地址。
  if url == "" {
    return classEnvelope(errclass.Translation("invalid upstream endpoint URL")), nil
  }
  debugTrace("executor resolved public_model=%s upstream_model=%s route=%s url=%s", req.Model, res.rec.UpstreamID, res.rec.Protocol, url)
  debugTrace("executor sending non-stream url=%s body_len=%d", url, len(upstreamBody))
  debugBody("request-upstream", req.Model, upstreamBody)
  m.logUpstreamModel(req.Model, res.rec.UpstreamID, upstreamBody, false)
  ctx, cancel := context.WithTimeout(context.WithValue(context.Background(), quotaCallbackContextKey{}, req.HostCallbackID), res.cfg.RequestTimeout)
  defer cancel()
  resp, err := m.bridge.Do(ctx, pluginapi.HTTPRequest{
    Method:  http.MethodPost,
    URL:     url,
    Headers: upstreamAuthHeaders(res.rec.Protocol, res.key),
    Body:    upstreamBody,
  })
  if err != nil {
    debugTrace("executor non-stream network error: %v", err)
    return classEnvelope(errclass.FromNetwork(err)), nil
  }
  debugTrace("executor received non-stream status=%d body_len=%d", resp.StatusCode, len(resp.Body))
  debugBody("response-upstream", req.Model, resp.Body)
  if resp.StatusCode >= 400 {
    return classEnvelope(shared.UpstreamStatusError(resp.StatusCode, resp.Body)), nil
  }
  // Parse/envelope guard only; true OOM prevention belongs to the host transport's byte cap.
  if int64(len(resp.Body)) > res.cfg.MaxResponseBytes {
    return classEnvelope(errclass.Translation("response exceeds max-response-bytes")), nil
  }
  converted, eErr := convertNonStream(res.rec.Protocol, req.SourceFormat, resp.StatusCode, resp.Body)
  if eErr != nil {
    return classEnvelope(eErr), nil
  }
  debugBody("response-downstream", req.Model, converted)
  return okEnvelope(pluginapi.ExecutorResponse{Payload: converted, Headers: resp.Headers}), nil
}

// buildUpstreamRequest 在统一发送边界校验当前认证的模型映射，并确保请求仅含一个可信 model。
func buildUpstreamRequest(cfg config.Config, rec catalog.ModelRecord, sourceFormat string, sourceBody []byte) ([]byte, *errclass.Error) {
  upstreamModel := rec.PublicID
  if cfg.ModelPrefix.Enabled {
    prefix := cfg.ModelPrefix.Value + "/"
    if !strings.HasPrefix(upstreamModel, prefix) {
      return nil, errclass.Translation("model prefix does not match selected auth")
    }
    // 仅剥离当前认证添加的一次前缀，保留上游自身的完整命名空间与原始目录 ID。
    upstreamModel = strings.TrimPrefix(upstreamModel, prefix)
  }
  if upstreamModel == "" || upstreamModel != rec.UpstreamID {
    return nil, errclass.Translation("model mapping does not match selected auth catalog")
  }

  var body []byte
  var eErr *errclass.Error
  switch rec.Protocol {
  case catalog.RouteChatCompletions:
    body, eErr = chatcompletions.BuildRequest(upstreamModel, sourceFormat, sourceBody, rec.Thinking)
  case catalog.RouteMessages:
    body, eErr = messages.BuildRequest(upstreamModel, sourceFormat, sourceBody, rec.Thinking)
  case catalog.RouteResponses:
    body, eErr = responses.BuildRequest(upstreamModel, sourceFormat, sourceBody, rec.Thinking)
  default:
    return nil, errclass.Translation("unsupported route")
  }
  if eErr != nil {
    return nil, eErr
  }
  // 适配器可能原样返回包含重复 model 的 JSON；重新编码顶层对象，消除上下游解析差异。
  var payload map[string]json.RawMessage
  if err := json.Unmarshal(body, &payload); err != nil || payload == nil {
    return nil, errclass.Translation("malformed upstream request JSON")
  }
  modelJSON, err := json.Marshal(upstreamModel)
  if err != nil {
    return nil, errclass.Translation("model id cannot be represented as JSON")
  }
  payload["model"] = modelJSON
  body, err = json.Marshal(payload)
  if err != nil {
    return nil, errclass.Translation("upstream request cannot be represented as JSON")
  }
  return body, nil
}

// logUpstreamModel 记录发送边界的模型映射，不输出凭据、提示词或完整请求体。
func (m *Manager) logUpstreamModel(publicModel, catalogModel string, body []byte, stream bool) {
  if m.bridge == nil {
    return
  }
  var payload struct {
    // Model 是最终交给宿主 HTTP 桥接的请求模型名。
    Model string `json:"model"`
  }
  if json.Unmarshal(body, &payload) != nil {
    return
  }
  _ = m.bridge.Log("info", "newapi-provider upstream model audit v1", map[string]any{
    "public_model": publicModel,
    "catalog_model": catalogModel,
    "body_model": payload.Model,
    "stream": stream,
  })
}

// upstreamAuthHeaders 为所有协议设置统一 Bearer 认证，不传递客户端专有会话头。
// upstreamAuthHeaders 统一设置 Bearer 认证，仅 Messages 附带标准版本头，不添加专有会话头。
func upstreamAuthHeaders(route catalog.Route, key string) http.Header {
  h := make(http.Header)
  h.Set("Content-Type", "application/json")
  h.Set("Authorization", "Bearer "+key)
  if route == catalog.RouteMessages {
    h.Set("anthropic-version", "2023-06-01")
  }
  return h
}

// convertNonStream routes one upstream response to its adapter's uniform
// translator: every adapter owns status classification (>=400 → §7
// classified errors), native passthrough, and cross-format conversion for
// all client formats.
// convertNonStream 根据上游协议调用既有适配器转换非流式响应。
func convertNonStream(route catalog.Route, sourceFormat string, status int, body []byte) ([]byte, *errclass.Error) {
  switch route {
  case catalog.RouteChatCompletions:
    return chatcompletions.ConvertNonStreamResponse(sourceFormat, status, body)
  case catalog.RouteMessages:
    return messages.ConvertNonStreamResponse(sourceFormat, status, body)
  case catalog.RouteResponses:
    return responses.ConvertNonStreamResponse(sourceFormat, status, body)
  }
  return nil, errclass.Translation("unsupported route")
}

// classEnvelope renders a classified failure as the wire error envelope;
// ToEnvelopeError redacts and propagates Retryable/HTTPStatus host-side.
// classEnvelope 将已分类且已脱敏的错误映射为宿主可识别的 ABI 信封。
func classEnvelope(e *errclass.Error) []byte {
  wire := errclass.ToEnvelopeError(e)
  out, _ := json.Marshal(pluginabi.Envelope{OK: false, Error: &wire})
  return out
}

// streamConverter is the common shape of the three adapters' stream
// converters: feed one upstream SSE chunk, get translated client events.
type streamConverter interface {
  // Feed 转换一个上游分片并返回下游事件、结束标志或已分类错误。
  Feed(chunk []byte) (events [][]byte, done bool, eErr *errclass.Error)
}

// Compile-time proof the close-without-terminal Flush seam (F5) picks up
// both converters that can hold a deferred terminal.
var (
  _ interface{ Flush() [][]byte } = (*chatcompletions.StreamConverter)(nil)
  _ interface{ Flush() [][]byte } = (*messages.StreamConverter)(nil)
)

// newStreamConverter 根据目录协议创建既有流转换器。
func newStreamConverter(route catalog.Route, sourceFormat string) streamConverter {
  switch route {
  case catalog.RouteMessages:
    return messages.NewStreamConverter(sourceFormat)
  case catalog.RouteResponses:
    return responses.NewStreamConverter(sourceFormat)
  }
  return chatcompletions.NewStreamConverter(sourceFormat)
}

// handleExecuteStream implements executor.execute_stream (FR-006, §7).
// It decodes once and delegates to executeStream so a stream-flagged
// request arriving via executor.execute is never parsed twice.
// handleExecuteStream 解析流式 ABI 请求并交给统一执行流程。
func (m *Manager) handleExecuteStream(request []byte) ([]byte, error) {
  var req executorRequest
  if err := decodeAuthRequest(request, &req); err != nil {
    return ErrEnvelope("invalid_request", "malformed executor request body"), nil
  }
  debugTrace("executor stream invoked model=%s source_format=%s stream=%t original_body_%s payload_%s", req.Model, req.SourceFormat, req.Stream, debugBodyMeta(req.OriginalRequest), debugBodyMeta(req.Payload))
  return m.executeStream(req)
}

// executeStream runs the already-decoded stream execution: pre-first-byte errors
// (invalid request, unroutable model, upstream HTTP >=400) return immediately as
// error envelopes so CPA can failover pre-emission.
// On success (upstream HTTP 200 OK), the reading and emitting pump loop runs in
// a background goroutine and executeStream returns okEnvelope immediately so the
// host can start draining chunks to the downstream client without buffer deadlocks.
// executeStream 在首字节前返回可重试错误，成功后启动后台读写泵避免宿主死锁。
func (m *Manager) executeStream(req executorRequest) ([]byte, error) {
  debugBody("request-original-stream", req.StreamID, req.OriginalRequest)
  res, failEnv := m.resolveExecution(req)
  if res == nil {
    return failEnv, nil
  }
  upstreamBody, eErr := buildUpstreamRequest(res.cfg, res.rec, req.SourceFormat, req.OriginalRequest)
  if eErr != nil {
    return classEnvelope(eErr), nil
  }

  url := catalog.JoinUpstreamURL(res.cfg.BaseURL, res.rec.EndpointPath)
  // URL 拼接拒绝非法基础地址或端点时立即失败，不向宿主发送空地址。
  if url == "" {
    return classEnvelope(errclass.Translation("invalid upstream endpoint URL")), nil
  }
  debugTrace("executor sending stream url=%s body_len=%d", url, len(upstreamBody))
  debugBody("request-upstream-stream", req.Model, upstreamBody)
  m.logUpstreamModel(req.Model, res.rec.UpstreamID, upstreamBody, true)
  ctx, cancel := context.WithTimeout(context.WithValue(context.Background(), quotaCallbackContextKey{}, req.HostCallbackID), res.cfg.RequestTimeout)
  defer cancel()
  st, _, id, err := m.bridge.DoStream(ctx, pluginapi.HTTPRequest{
    Method:  http.MethodPost,
    URL:     url,
    Headers: upstreamAuthHeaders(res.rec.Protocol, res.key),
    Body:    upstreamBody,
  })
  debugTrace("executor stream DoStream status=%d upstreamID=%s err=%v", st, id, err)
  if err != nil {
    return classEnvelope(errclass.FromNetwork(err)), nil
  }
  if st >= 400 {
    var body []byte
    if id != "" {
      payload, errMsg, _, readErr := m.bridge.StreamRead(id)
      debugBody("response-upstream-error-stream", id, payload)
      debugTrace("executor stream error response upstreamID=%s readErrMsg=%q err=%v", id, errMsg, readErr)
      if readErr == nil {
        if len(payload) > 0 {
          body = payload
        } else if errMsg != "" {
          body = []byte(errMsg)
        }
      }
      _ = m.bridge.StreamClose(id)
    }
    return classEnvelope(shared.UpstreamStatusError(st, body)), nil
  }

  downID := req.StreamID
  if m.bridge != nil {
    m.bridge.inFlight.Add(1)
  }
  go func() {
    if m.bridge != nil {
      defer m.bridge.inFlight.Done()
    }
    m.pumpStream(downID, id, res, req.SourceFormat)
  }()
  return okEnvelope(struct{}{}), nil
}

// pumpStream 转换并转发上游流，在超时、限额或异常时关闭上下游资源。
func (m *Manager) pumpStream(downID, upstreamID string, res *resolvedExecution, sourceFormat string) {
  var closeOnce sync.Once
  closeStreams := func(downErrMsg string) {
    closeOnce.Do(func() {
      debugTrace("executor stream closing upstreamID=%s downstreamID=%s error=%q", upstreamID, downID, errclass.Redact(downErrMsg))
      _ = m.bridge.StreamClose(upstreamID)
      _ = m.bridge.StreamCloseDownstream(downID, downErrMsg)
    })
  }
  defer closeStreams("")

  var aborted atomic.Bool
  watchdog := time.AfterFunc(res.cfg.RequestTimeout, func() {
    defer func() {
      if r := recover(); r != nil && m.bridge != nil {
        _ = m.bridge.Log("error", "stream watchdog panicked", nil)
      }
    }()
    aborted.Store(true)
    _ = m.bridge.StreamClose(upstreamID)
  })
  defer watchdog.Stop()

  conv := newStreamConverter(res.rec.Protocol, sourceFormat)
  var (
    total          int64
    upstreamClosed bool
    convDone       bool
  )
  for {
    payload, readErrMsg, closed, err := m.bridge.StreamRead(upstreamID)
    debugTrace("executor stream read chunk_len=%d closed=%t readErrMsg=%q err=%v", len(payload), closed, readErrMsg, err)
    debugBody("response-upstream-stream", upstreamID, payload)
    upstreamClosed = closed
    if aborted.Load() {
      closeStreams(errclass.Redact("stream exceeded request-timeout"))
      return
    }
    if err != nil {
      closeStreams(errclass.Redact(err.Error()))
      return
    }
    if readErrMsg != "" {
      closeStreams(errclass.Redact(readErrMsg))
      return
    }
    total += int64(len(payload))
    if total > res.cfg.MaxResponseBytes {
      closeStreams(errclass.Redact("stream exceeded max-response-bytes"))
      return
    }
    events, done, convErr := conv.Feed(payload)
    if convErr != nil {
      closeStreams(errclass.Redact(convErr.Message))
      return
    }
    if emitErr := m.emitAll(downID, events); emitErr != nil {
      closeStreams(errclass.Redact(emitErr.Error()))
      return
    }
    convDone = done
    if convDone || upstreamClosed {
      break
    }
  }
  debugTrace("executor stream loop end upstreamID=%s total_bytes=%d convDone=%t upstreamClosed=%t", upstreamID, total, convDone, upstreamClosed)
  if !convDone && upstreamClosed {
    if flusher, ok := conv.(interface{ Flush() [][]byte }); ok {
      flushed := flusher.Flush()
      if emitErr := m.emitAll(downID, flushed); emitErr != nil {
        closeStreams(errclass.Redact(emitErr.Error()))
        return
      }
    }
  }
}

// emitAll feeds converted events downstream in order. StreamEmit is
// deadline-bounded (emitTimeout), so a host that stops draining the
// downstream stream surfaces here as an error; the pump loop treats that
// as a post-first-byte stream-fatal failure and runs fail(), whose
// Once-closer releases both streams.
// emitAll 按顺序发送转换后的事件，下游背压超时立即结束流。
func (m *Manager) emitAll(downStreamID string, events [][]byte) error {
  for _, evt := range events {
    if err := m.bridge.StreamEmit(downStreamID, evt); err != nil {
      debugTrace("executor stream emit failed downstreamID=%s err=%v", downStreamID, err)
      return err
    }
    debugBody("response-downstream-stream", downStreamID, evt)
  }
  return nil
}
