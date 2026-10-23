// Package thinking 根据模型能力在推理预算与标准推理等级之间转换。
package thinking

import (
	"fmt"
	"slices"
	"strings"

	"github.com/router-for-me/CLIProxyAPI/v8/sdk/pluginapi"

	"newapi-provider/internal/errclass"
)

// CanonicalLevels is the ordered effort space, weakest to strongest.
// "auto" is deliberately absent: it is a dynamic sentinel, not a rankable level.
// CanonicalLevels 从弱到强的标准推理等级序列，不含动态模式哨兵。
var CanonicalLevels = []string{"none", "minimal", "low", "medium", "high", "xhigh", "max"}

// levelBudgetTable ports the host's levelToBudgetMap verbatim
// (SDK internal/thinking/convert.go @ v8.0.0): exact per-level budgets,
// no interpolation.
// levelBudgetTable 宿主兼容的推理等级到固定令牌预算的映射表。
var levelBudgetTable = map[string]int64{
	"minimal": 512,
	"low":     1024,
	"medium":  8192,
	"high":    24576,
	"xhigh":   32768,
	"max":     128000, // Claude adaptive ceiling; clamped to model Max below
}

// budgetToLevel ports the host's ConvertBudgetToLevel fixed thresholds
// (SDK internal/thinking/convert.go @ v8.0.0: ThresholdMinimal/Low/Medium/
// High = 512/1024/8192/24576): <=512 minimal, <=1024 low, <=8192 medium,
// <=24576 high, else xhigh. Callers handle the -1→auto and 0→none sentinels;
// "max" is never derived from a budget.
// budgetToLevel 按宿主固定阈值将正数预算映射为标准推理等级。
func budgetToLevel(budget int64) string {
	switch {
	case budget <= 512:
		return "minimal"
	case budget <= 1024:
		return "low"
	case budget <= 8192:
		return "medium"
	case budget <= 24576:
		return "high"
	default:
		return "xhigh"
	}
}

// clampToSupported returns level unchanged when supported; otherwise the
// nearest supported level by index distance in CanonicalLevels order,
// preferring the weaker level on ties (mirrors SDK validate.go clampLevel).
//
// Domain contract: level is always canonical — a budgetToLevel result or
// the literal "medium" — and supported holds only CanonicalLevels members
// and is never empty (it is always a SupportedLevels output), so every
// index lookup below hits.
// clampToSupported 选择最近的受支持等级，距离相同时优先较弱等级。
func clampToSupported(level string, supported []string) string {
	if slices.Contains(supported, level) {
		return level
	}
	pos := slices.Index(CanonicalLevels, level)
	bestIdx, bestDist := -1, len(CanonicalLevels)+1
	for _, s := range supported {
		idx := slices.Index(CanonicalLevels, s)
		dist := pos - idx
		if dist < 0 {
			dist = -dist
		}
		if dist < bestDist || (dist == bestDist && idx < bestIdx) {
			bestIdx, bestDist = idx, dist
		}
	}
	return CanonicalLevels[bestIdx]
}

// EffortFromBudget converts an Anthropic-style thinking budget into the best
// matching effort level for ts (FR-005).
//
//   - Undeclared capability (Declared false or capability metadata absent):
//     the host threshold mapping is applied unclamped, so the strongest
//     budgets reach "xhigh"; budget <= 0 yields "" so callers omit the
//     effort field instead of fabricating a weakest level.
//   - The model's declared Levels are filtered to canonical members and
//     reordered canonically; if nothing survives, {"low","medium","high"} is
//     assumed so an unknown or nil capability still yields a usable mapping.
//   - budget <= 0 (reasoning off): ZeroAllowed with "none" supported → "none";
//     otherwise DynamicAllowed → "auto" (targets clamp); otherwise lowest.
//   - budget > 0: derive the standard level via the host threshold table
//     (budgetToLevel), then clamp it to the nearest supported level — index
//     distance in CanonicalLevels order, weaker side wins ties. E.g.
//     budget 8192 derives "medium"; against Levels {low,high} the distances
//     are equal, so the weaker "low" is returned.
//
// The result is always a member of the supported set, except "auto".
// EffortFromBudget 结合模型能力将预算映射为等级，保留关闭和动态模式语义。
func EffortFromBudget(budget int64, ts *pluginapi.ThinkingSupport) string {
	if !Declared(ts) {
		if budget <= 0 {
			return ""
		}
		return budgetToLevel(budget)
	}
	supported := SupportedLevels(ts)
	if budget <= 0 {
		if ts != nil && ts.ZeroAllowed && slices.Contains(supported, "none") {
			return "none"
		}
		if ts != nil && ts.DynamicAllowed {
			return "auto"
		}
		return supported[0]
	}
	return clampToSupported(budgetToLevel(budget), supported)
}

// BudgetFromEffort maps a client-declared effort level to an Anthropic-style
// budget_tokens value for models whose upstream protocol takes budgets
// (FR-005), using the host levelToBudgetMap exact values (SDK
// internal/thinking/convert.go @ v8.0.0) instead of interpolation. ok is
// false when the effort cannot be represented for ts so callers raise an
// explicit unsupported-class error instead of silently dropping the control.
//
// Sentinels: "auto" yields the -1 dynamic sentinel when DynamicAllowed, else
// falls back to the supported level nearest "medium"; "none"/"minimal" yield
// their table values when ZeroAllowed, else the weakest supported level.
//
// Divergence from the host: the host returns table values as-is, we clamp
// into [Min,Max] because our callers forward the budget directly to upstream
// providers whose declared bounds are authoritative — except the off state:
// when "none" resolves under ZeroAllowed its zero budget bypasses the Min
// clamp, since clamping it up would silently re-enable thinking against an
// explicit client opt-out (callers treat budget <= 0 as reasoning-off).
// BudgetFromEffort 按固定等级表解析预算，并对有效预算应用模型能力边界。
func BudgetFromEffort(effort string, ts *pluginapi.ThinkingSupport) (int64, bool) {
	effort = strings.ToLower(strings.TrimSpace(effort))
	if effort == "" {
		return 0, false
	}
	if !Declared(ts) {
		// Undeclared capability: table values are forwarded unclamped (no
		// Min/Max to clamp into) and the sentinels keep their off-state
		// meaning for the targets that take budgets.
		switch effort {
		case "auto":
			return -1, true
		case "none":
			return 0, true
		}
		budget, ok := levelBudgetTable[effort]
		return budget, ok
	}
	supported := SupportedLevels(ts)
	switch {
	case effort == "auto":
		if ts != nil && ts.DynamicAllowed {
			return -1, true
		}
		effort = clampToSupported("medium", supported)
	case effort == "none" || effort == "minimal":
		if ts == nil || !ts.ZeroAllowed {
			effort = supported[0] // off-state unrepresentable: weakest enabled level
		} else if effort == "none" {
			return 0, true // off-state sentinel: never clamped up to Min
		}
	default:
		if !slices.Contains(supported, effort) {
			return 0, false
		}
	}
	budget := levelBudgetTable[effort]
	if ts != nil {
		if ts.Min > 0 && budget < int64(ts.Min) {
			budget = int64(ts.Min)
		}
		if ts.Max > 0 && budget > int64(ts.Max) {
			budget = int64(ts.Max)
		}
	}
	return budget, true
}

// ValidateEffort is the shared reject kernel for client-declared efforts
// (FR-005): nil when the value is representable for ts, otherwise a
// ClassUnsupported error quoting the value exactly as the client declared
// it. Canonical declared levels are admitted via SupportedLevels; the
// non-canonical sentinels are capability-aware, matching BudgetFromEffort:
// "auto" only under DynamicAllowed (declared Levels never admit it — they
// are canon-filtered) and "none" under ZeroAllowed or explicit declaration.
//
// Undeclared capability admits every non-empty value: with no declaration
// the upstream is the only authority on which efforts it accepts, so the
// client's value is forwarded verbatim and an upstream rejection surfaces
// unchanged when the upstream publishes no thinking metadata.
// ValidateEffort 校验已声明能力是否支持客户端等级，未声明时交由上游决定。
func ValidateEffort(effort string, ts *pluginapi.ThinkingSupport) *errclass.Error {
	e := strings.ToLower(strings.TrimSpace(effort))
	if e == "" {
		return UnsupportedEffort(effort)
	}
	if !Declared(ts) {
		return nil
	}
	supported := SupportedLevels(ts)
	switch {
	case e == "auto":
		if ts != nil && ts.DynamicAllowed {
			return nil
		}
	case e == "none":
		if (ts != nil && ts.ZeroAllowed) || slices.Contains(supported, "none") {
			return nil
		}
	default:
		if slices.Contains(supported, e) {
			return nil
		}
	}
	return UnsupportedEffort(effort)
}

// UnsupportedEffort is the single reject shape for a client-declared effort
// the target cannot represent (FR-005): ClassUnsupported and a message quoting
// the value exactly as the client wrote it. Shared by ValidateEffort and by
// the budget-converting legs, which must refuse a label with no table value
// rather than silently dropping the reasoning control.
// UnsupportedEffort 统一构造不支持推理等级错误，保留客户端原始值。
func UnsupportedEffort(effort string) *errclass.Error {
	return &errclass.Error{
		Class:   errclass.ClassUnsupported,
		Message: fmt.Sprintf("reasoning_effort %q is not supported for this model", effort),
	}
}

// SupportedLevels filters ts.Levels to canonical members in CanonicalLevels
// order, falling back to {"low","medium","high"} when empty or unset.
// SupportedLevels 按标准顺序筛选模型等级，缺省时返回兼容集合。
func SupportedLevels(ts *pluginapi.ThinkingSupport) []string {
	if ts == nil {
		return defaultLevels()
	}
	var out []string
	for _, canon := range CanonicalLevels {
		for _, l := range ts.Levels {
			if strings.EqualFold(strings.TrimSpace(l), canon) {
				out = append(out, canon)
				break
			}
		}
	}
	if len(out) == 0 {
		return defaultLevels()
	}
	return out
}

// defaultLevels 返回缺省等级集合，供没有有效等级声明的模型映射使用。
func defaultLevels() []string {
	return []string{"low", "medium", "high"}
}

// Declared reports whether the model carries an authoritative thinking
// declaration: any canonical level in Levels, or any capability bound/flag.
//
// Without authoritative thinking metadata, the upstream decides accepted
// effort levels and budgets. A model whose
// entry declares even one level or bound opts back into eager local
// validation; the {low,medium,high} fallback in SupportedLevels remains the
// mapping target only for declared-but-empty capability objects.
// Declared 判断是否存在权威推理等级、边界或开关声明，以决定本地约束。
func Declared(ts *pluginapi.ThinkingSupport) bool {
	if ts == nil {
		return false
	}
	if ts.ZeroAllowed || ts.DynamicAllowed || ts.Min > 0 || ts.Max > 0 {
		return true
	}
	for _, l := range ts.Levels {
		if slices.Contains(CanonicalLevels, strings.ToLower(strings.TrimSpace(l))) {
			return true
		}
	}
	return false
}
