//go:build !debug

package plugin

// debugTrace is compiled as an empty no-op stub for release/production builds.
// debugTrace 仅调试版本将诊断信息写入专用日志，发布版本为空操作。
func debugTrace(string, ...any) {}

// debugBodyMeta returns an empty string in release/production builds.
// debugBodyMeta 提取请求编码诊断信息，发布版本不返回内容。
func debugBodyMeta([]byte) string { return "" }

// debugBody omits request and response bodies in release/production builds.
// debugBody 仅调试版本记录正文；正文可能包含敏感提示词、工具参数和未识别凭据。
func debugBody(string, string, []byte) {}
