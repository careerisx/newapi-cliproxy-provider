// Package resources 提供插件内嵌的公开认证管理页。
package resources

import _ "embed"

// LoginPage 是不包含认证材料的静态认证管理页面。
//go:embed login_page.html
var LoginPage string
