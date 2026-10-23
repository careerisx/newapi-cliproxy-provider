//go:build debug

package plugin

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"newapi-provider/internal/errclass"
)

// debugBodyMeta describes request encoding without embedding the body.
// debugBodyMeta 提取请求编码诊断信息，发布版本不返回内容。
func debugBodyMeta(body []byte) string {
	if len(body) == 0 {
		return "len=0 json=false first=- last=-"
	}
	var value any
	if err := json.Unmarshal(body, &value); err != nil {
		var syntaxErr *json.SyntaxError
		if errors.As(err, &syntaxErr) {
			offset := int(syntaxErr.Offset)
			if offset > 0 && offset <= len(body) {
				return fmt.Sprintf("len=%d json=false error_offset=%d error_byte=%q first=%q last=%q", len(body), offset, body[offset-1], body[0], body[len(body)-1])
			}
			return fmt.Sprintf("len=%d json=false error_offset=%d first=%q last=%q", len(body), offset, body[0], body[len(body)-1])
		}
		return fmt.Sprintf("len=%d json=false error=%T first=%q last=%q", len(body), err, body[0], body[len(body)-1])
	}
	return fmt.Sprintf("len=%d json=true first=%q last=%q", len(body), body[0], body[len(body)-1])
}

// debugMu prevents concurrent records from interleaving in the log file.
// debugMu 防止并发调试记录在日志中交错。
var debugMu sync.Mutex

// 注意：调试 body 可能包含敏感提示词、工具参数与未识别凭据，仅应在受控环境启用。
// debugBody records all received body bytes without length truncation. Quoting
// keeps each chunk on one line, including partial JSON and SSE frames, while
// existing redaction removes recognized credential patterns.
// debugBody 仅调试版本记录正文；正文可能包含敏感提示词、工具参数和未识别凭据。
func debugBody(stage, id string, body []byte) {
	debugTrace("body stage=%s id=%q body_len=%d body=%q", stage, id, len(body), errclass.Redact(string(body)))
}

// debugTrace appends diagnostic records to the executable's log directory.
// Body logging is explicit through debugBody; auth headers are never logged.
// debugTrace 仅调试版本将诊断信息写入专用日志，发布版本为空操作。
func debugTrace(format string, args ...any) {
	debugMu.Lock()
	defer debugMu.Unlock()

	path := filepath.Join(filepath.Dir(os.Args[0]), "newapi-provider-debug.log")
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0600)
	if err != nil {
		return
	}
	defer f.Close()
	values := append([]any{time.Now().Format(time.RFC3339Nano)}, args...)
	_, _ = fmt.Fprintf(f, "%s "+format+"\n", values...)
}
