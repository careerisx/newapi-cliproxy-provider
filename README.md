# newapi-provider

CLIProxyAPI v8 原生插件。每份认证独立配置上游站点、协议、模型前缀和额度地址，支持 Chat Completions、Messages、Responses 的流式及非流式适配。认证通过插件菜单内置页面添加和编辑，也可使用宿主既有认证文件上传功能。

插件名、Go module/import、配置 key、日志标记及构建产物 `newapi-provider.so` 仍为 `newapi-provider`；认证 ProviderID、认证/执行器/额度标识和模型归属为 `newapi`。资源及管理 URL 仍使用 `newapi-provider`，不要全局替换。

## 全局配置

全局只保留插件启用和目录，实例中只配置启用：

```yaml
plugins:
  enabled: true
  dir: "./plugins"
  configs:
    newapi-provider:
      enabled: true
```

不再从全局读取 `base_url`、`catalog-url`、`quota-url`、`upstream-format`、`model-prefix`、`allow-http` 或 `public-url`。旧全局字段不会补齐认证，必须将相应站点配置迁移到每份认证。没有默认站点，也不会回退到 `cf.api.fan` 或其他认证。

| 固定运行策略 | 值 |
|---|---|
| 上游请求超时 | 5 分钟 |
| 普通响应和流累计大小上限 | 64 MiB；网络接收限制仍由宿主负责 |
| 目录读取预算 | 至少 16 MiB |
| 目录刷新 | 按认证请求触发，成功后 15 分钟再刷新，失败退避 30 秒 |
| 同步目录发现等待 | 最多 10 秒 |
| 失败目录快照 | 仅同一认证、同一配置和同一 key 版本保留成功快照 |
| 目录缓存清理 | 最多 256 个版本，按需清理超过 1 小时未访问的版本 |

没有全局周期刷新，也不调用或猜测宿主推送模型接口。缓存到期由后续模型发现或执行请求触发刷新，宿主界面的模型展示时机由宿主决定。

## 认证管理页面

在宿主管理界面的插件菜单中打开 **New API 认证管理**。正式菜单入口来自 `Resources.Menu`，保留资源路径及内嵌文件名 `login` / `resources/login_page.html`。

| 入口 | 权限与职责 |
|---|---|
| `GET /v0/resource/plugins/newapi-provider/login` | 公开静态页面，不包含任何认证数据或秘密 |
| `GET /v0/management/plugins/newapi-provider/auths` | 受宿主管理认证保护，真实 AuthList + AuthGet 列表 |
| `POST /v0/management/plugins/newapi-provider/auths` | 受宿主管理认证保护，校验后通过 HostBridge.AuthSave 新增 |
| `PATCH /v0/management/plugins/newapi-provider/auths` | 受宿主管理认证保护，以列表文件名定位真实文件，读取最新 JSON 后合并白名单修改并原名保存 |

敏感管理路由没有 `Menu`，避免 SDK 将其注册为公开资源。页面优先使用手动输入的 CPA 管理密码，使用 `password` 控件；API key 与系统访问令牌也使用 `password` 控件。管理密码仅通过 Authorization 请求头发送，不写入认证 JSON。

页面启动时会**兼容尝试**读取同源 CPA 前端已保存的 `localStorage['cli-proxy-auth']` 中 `state.managementKey`，兼容明文 JSON 及参考页面的 `enc::v1::` Base64/XOR 解码。这不是浏览器密码库读取，也不保证当前宿主管理前端采用此规则：尚未定位并核实其真实实现，未保存登录、不同源、存储受限、格式损坏或未知版本都会回退手动输入。候选密钥首先仅通过 GET 列表验证，成功后明确显示已使用保存登录，并可切换手动输入；不会自动保存认证。401/403 停用自动候选并聚焦密码框，不删除宿主存储、不循环重试；网络错误单独提示，由用户手动刷新。

页面根据当前 `location.pathname` 派生同源管理地址，保留资源路径之前的反向代理前缀。例如 `/proxy/v0/resource/plugins/newapi-provider/login` 请求 `/proxy/v0/management/plugins/newapi-provider/auths`，不依赖 `public-url`。反向代理需将此前缀映射到宿主对应路由。

页面只读取上述兼容存储，不写入 localStorage、不复制持久化管理密码；自动候选不填入可显示控件。管理密码或 key 不放入 URL、日志或新增持久存储，请求使用 `redirect: 'error'` 拒绝重定向，保留同源前缀与安全 CSP。请使用 HTTPS 或可信网络。插件不能控制宿主和反向代理自身的日志策略，应避免记录认证头和提交正文。

| 页面能力 | 行为 |
|---|---|
| 添加 | 名称、base_url、API key、三值协议选择、可选 quota-url/catalog-url、模型前缀对象、可选成对系统令牌及用户 ID、当前认证 allow-http |
| 新增保存 | 后台只接收白名单字段，生成安全随机文件名，统一校验后调用宿主保存；不会使用浏览器指定的路径、文件名或 provider |
| 编辑 | 回填名称、base_url、协议、额度/目录地址、模型前缀和 allow-http；密钥、系统令牌和用户 ID 不回显，只显示存在标记；支持取消编辑 |
| 秘密留空 | 编辑时 API key、系统令牌及用户 ID 留空或省略均保留最新原值，非空才替换；系统凭据与旧值合并后成对校验，可只替换其中一项 |
| 显式清除 | 独立勾选清除账号余额凭据，同时删除 system_access_token 和 user_id；不能同时输入替换值，空值不等于清除 |
| 编辑保留 | 服务端使用真实 AuthList 条目和 AuthIndex 读取最新 JSON，核对 provider 与安全单一文件名，再原名覆盖；保留 id/type/provider/disabled/priority/note 及未知扩展，但 email 明确随校验后的 label 同步；拒绝歧义、路径或跨 provider 目标 |
| 成功 | 只显示“已保存”，不声称已验证上游权限；用户手动刷新列表 |
| 未知结果 | 提示先刷新确认，锁定再次保存，不自动重试；刷新后仍需用户确认是否已经保存 |
| 列表 | 包含禁用认证，排除 runtime-only；返回类型安全的非敏感配置和凭据存在标记供编辑；统一校验失败仍尽量保留原始白名单配置以便修复 |
| 数据展示 | 使用 textContent，不解释认证数据中的 HTML，不返回完整 JSON、key、宿主索引或物理路径 |

页面提供添加、编辑、取消编辑、显示与刷新，不提供删除、启停或额度页。SDK v8.0.0 的保存回调直接构造 StatusActive，未继承 Disabled，因此**拒绝编辑禁用认证**，必须先在 CPA 启用后编辑，避免磁盘禁用状态与运行期状态不一致。兼容的仅 provider、缺少 type 的文件仍可列出，但 SDK 保存只读取 type，会导致运行期 provider 变成 unknown；为保留原身份字段，此类编辑明确拒绝，需先在 CPA 文件管理补齐 `type: "newapi"`。

编辑每次读取最新文件再合并，留空秘密不会使用客户端旧值覆盖新 key；未实现跨编辑者完整版本锁，普通配置同时编辑仍可能后写覆盖。保存超时或错误可能发生在文件落盘之后，保留 unknown 语义，不自动重试。保存后宿主负责写盘及运行期 upsert；该路径不保证再次调用插件 ParseAuth，因此所有调用都兼容只有 Metadata、没有 Attributes 的认证。

旧 StartLogin/PollLogin 入口明确提示使用插件认证管理页面。原 state、一次性交付会话、get-auth-status 轮询与会话清理链路已移除。AuthProvider 保留解析和静态凭据刷新能力；SDK 可能仍因 AuthProvider 显示 `supports_oauth` 按钮，本插件不保证隐藏该按钮。

## 每认证 JSON

可通过宿主现有认证文件上传入口提交以下格式，也可在宿主 auth-dir 中维护文件：

```json
{
  "type": "newapi",
  "provider": "newapi",
  "label": "站点 A 推理令牌",
  "email": "站点 A 推理令牌",
  "base_url": "https://api.example.com/v1",
  "api_key": "替换为推理 API key",
  "upstream-format": "openai",
  "quota-url": "https://site.example.com/api/user/self",
  "model-prefix": {
    "enabled": true,
    "value": "newapi"
  },
  "system_access_token": "替换为系统访问令牌",
  "user_id": "174339",
  "allow-http": false,
  "disabled": false
}
```

不需要账号余额时，同时省略 `system_access_token` 和 `user_id`。不需要自定义额度站点时省略 `quota-url`；示例地址仅演示结构，不是内置站点。

SDK v8.0.0 的 AuthSave 直接保存路径不读取 label，而使用非空 email 覆盖运行期显示名称。因此页面新增及编辑保留 label，同时把规范化名称同步到保存 JSON 的 email；ParseAuth 和 RefreshAuth 在 label 非空时同步 Metadata.email，并返回一致的 Label。此 email **只是认证名称的显示兼容别名，不是上游真实邮箱**，不用于登录、邮箱验证或上游请求。缺少 label 时不会凭空生成名称或邮箱。新增文件使用 `newapi-随机值.json`，编辑始终保留原文件名，不修改文件 ID 来伪装显示名称。

| 字段 | 必要性与默认值 | 含义 |
|---|---|---|
| `type` | 必填，`newapi` | 解析也接受同值 `provider` 标识；页面编辑要求存在 type |
| `provider` | 可选，存在时为 `newapi` | 不要保留旧值或与 type 冲突 |
| `label` | 可选；页面要求名称 | 认证显示名称，保留此字段 |
| `email` | 页面保存时与规范化 label 同步 | 仅供宿主显示的兼容别名，**不是上游真实邮箱**；不能由客户端单独编辑 |
| `base_url` | **每份认证必填** | HTTPS 上游基础地址，可保留自定义路径；仅接受此字段名，缺失时明确报迁移错误 |
| `api_key` | **必填且非空** | 令牌本体，不加 Bearer；仅用于模型目录、推理及令牌额度 |
| `upstream-format` | 默认 `openai` | 只接受 `openai`、`claude`、`openai-response` |
| `model-prefix` | 对象；默认 `enabled: true, value: newapi` | 不接受字符串前缀；启用时公开模型 ID 为 `value/上游ID` |
| `quota-url` | 可选 | 当前认证的完整额度地址，空值按 base_url 派生 |
| `catalog-url` | 可选 | 当前认证的完整目录地址，空值按 base_url 补齐 `/v1/models`；会收到 API key |
| `allow-http` | 当前认证默认 `false` | 明确设置 true 才允许该认证使用 HTTP 地址 |
| `system_access_token` | 与 user_id 同时提供或省略 | 仅用于账号余额，不用于目录、推理或令牌额度 |
| `user_id` | 与系统令牌成对 | int64 范围内正十进制整数，推荐字符串；不安全的浮点元数据会被拒绝 |
| `disabled` | 默认 `false` | 禁用状态由宿主管理；页面不提供切换 |
| `id` | 可选兼容内容字段 | **不等于宿主真实 AuthID**，不用于跨请求目录缓存身份 |

URL 拒绝 userinfo、query、fragment、路径遍历及歧义路径。所有可配置地址都必须可信；显式 catalog-url 和 quota-url 可以指向独立可信站点，插件不会探测另一个域名或借用全局地址。

所有认证消费入口统一按 StorageJSON → Metadata → Attributes 合并，后者覆盖前者；不会在缺少某个认证字段时借用全局配置或另一认证。整数使用保留十进制文本的解码方式；已有 float64 用户 ID 仅接受不超过 `2^53 - 1` 的安全正整数。系统令牌和用户 ID 缺一项会明确报错，不回退 API key 查询账号。

## 迁移和模型隔离

| 旧用法 | 新用法 |
|---|---|
| 认证 type/provider 为 newapi-provider | 手动将 type 改为 newapi，且已存在的 provider 也改为 newapi；不提供旧认证标识兼容或自动迁移 |
| 宿主直接保存或读取时名称未展示 | 保留 label，并补 email 与 label 相同，作为显示别名而非真实邮箱 |
| 全局配置上游站点 | 将 base_url 等字段分别放到各认证文件；全局删除上游与 public-url |
| 只有 api_key 的认证 | 必须补 base_url，否则提示明确迁移错误 |
| 依赖固定默认站点 | 必须为认证显式填写地址 |
| 通过 OAuth 按钮创建 state | 使用插件菜单认证管理页，不再轮询交付 |
| 多个 key 共用首个认证的模型目录 | 每认证单独请求目录，不借用其他认证目录 |
| JSON id 充当缓存身份 | 使用本次 SDK 请求真实 AuthID |

迁移时保留原认证文件名和 JSON id，不需要把旧的 `newapi-provider-*.json` 重命名。插件不会自动改写磁盘上已有文件；手动更新 type、已存在的 provider 及显示别名后，重启宿主或通过宿主重载认证，使运行期记录使用新标识。只更新插件二进制而不迁移旧认证不能继续使用；插件名、配置 key、资源/管理 URL 和日志标记仍为 `newapi-provider`。

`model.static` 返回空列表。`model.for_auth` 读取 SDK 的 AuthID、AuthProvider、StorageJSON、Metadata、Attributes，且通过额外 `host_callback_id` 使用宿主为当前认证设置的 HTTP 客户端。

每个 catalog.Manager 只属于一个认证版本；版本包含真实 AuthID、完整地址/目录/协议/前缀等配置及 API key 摘要。摘要不记录日志；新 key、新地址或新认证不会 SeedFrom 旧快照。相同版本的并发发现合并刷新，失败只能保留该版本自身快照。

流式与非流式执行均从宿主选中认证重新解析配置和 key，查找准确版本的目录，将公开模型 ID 映射为当前认证的上游模型 ID，再调用该认证的站点与协议。不会维护私有轮换 key 池。插件只在模型 ID 上添加一次前缀，不写 AuthData.Prefix；同名模型由宿主调度认证。

| 上游协议 | 规范化后的端点 |
|---|---|
| `openai` | `/v1/chat/completions` |
| `claude` | `/v1/messages` |
| `openai-response` | `/v1/responses` |

base_url 可以是 `https://example.com`、`https://example.com/v1` 或 `https://example.com/proxy/v1/`；保留部署路径，只在缺少末尾版本段时补 `/v1`，不要填写完整推理端点。

适配器保持现有实现：支持三种客户端格式的普通响应与 SSE，覆盖文本、图片和函数工具调用；不保证音频、文件、视频、非函数工具、Responses 状态续接或跨协议推理内容无损。Messages 的 tool_choice:none、Responses 的 stop 映射及超大整数等原有转换边界仍存在。需要协议扩展时优先使用与客户端一致的上游协议。

## 额度与安全边界

额度只读取宿主当前认证配置。账号余额和令牌额度保留两次独立并行查询，结果分别显示，不合并金额。每次请求超时最多 30 秒，解析预算最多 1 MiB。

| 查询 | 凭据 | 行为 |
|---|---|---|
| `/api/usage/token/` | 仅 API key | 有限令牌保留已用、剩余、总额和可靠比例；无限令牌保留无限标记及已用，不伪造有限剩余 |
| `/api/user/self` | 仅系统访问令牌 + New-Api-User | 成对系统凭据存在才查询；保留剩余、已用和请求次数，不编造总额或重置 |
| 同站点 `/api/status` | 无认证头，仅 Accept | 读取公开 quota_per_unit，用于 USD 换算 |

成功的部分显示，失败的部分隐藏；全失败返回空额度，不伪造零余额。有效零额度正常展示。系统令牌失败不会换用 API key。expires_at 是令牌到期时间，不是重置时间；不支持额度重置。

| 地址场景 | 派生规则 |
|---|---|
| 未提供 quota-url | 从当前 base_url 去掉末尾 `/v1` 后追加标准额度路径，保留部署前缀 |
| quota-url 为 `/api/user/self` | 仅用系统凭据查询该地址，API key 请求同站点派生的令牌路径；无系统凭据也不把 API key 发到账号接口 |
| quota-url 为 `/api/usage/token[/]` | API key 查询该地址，有系统凭据时派生同站点账号路径 |
| 未知自定义 quota-url | 有系统凭据只作账号用途，无系统凭据只作令牌用途；不猜其他接口 |

只从**实际额度 URL** 的标准尾路径派生同站点 `/api/status`，保留部署前缀，不自动跨域。未知自定义路径不探测状态接口。请求不发送 Authorization、New-Api-User 或其他秘密；最多等待 3 秒，解析最多 64 KiB。

仅接受 success:true 且有限正数 quota_per_unit，金额按 `原始 quota / quota_per_unit` 显示 USD，不硬编码比例，也不再乘汇率。状态失败不影响额度结果，退回有明确 quota 单位的原始值。

公开比例缓存按完整 status URL 隔离站点和部署前缀，成功缓存 15 分钟，失败缓存 30 秒；并发请求合并，最多 256 项，不移除仍有等待者的在途条目。重配置及关闭清空缓存，并防止旧请求回填。

所有网络请求经现有 HostBridge，代理、TLS、传输和重定向政策由宿主控制；跨域重定向的凭据保护也依赖宿主。发布构建不记录请求正文；现有 debug 构建可能记录提示词和响应，不应在不受控环境启用或公开日志。认证管理入口不记录提交正文、密码或 key。

## 运行兼容与验证

依赖继续使用现有 Go 1.26.7、CLIProxyAPI SDK v8.0.0 及原有依赖；Makefile、主 ABI 和协议适配器保持原实现。SDK HostBridge 接口未暴露 Save 方法，本插件按已有 host.auth.save ABI、HostAuthSaveRequest{Name,JSON} 和响应{Name,Path} 实现保存桥接，响应物理路径不返回页面。

AuthSave 依赖宿主启用认证管理及可写 auth-dir。模型菜单是否立即更新、同名模型调度及 OAuth 按钮展示由宿主决定；不能仅凭保存成功判断上游权限或模型可用。现有动态库未重新生成，需要另行构建并由宿主加载后才会生效。

按本次实施约束，未运行构建、编译、测试、安装、部署、真实上游请求或代码审核；未新增测试或依赖，也未执行 Git 写操作。

后续在允许验证并配置好 Go/CGO、Docker 工具链后，建议使用现有 `make linux-amd64`；其他构建入口仍保留 `make build` 和 `make debug`。这些仅为建议命令，本次未执行。
