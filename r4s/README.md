# R4S Telegram update 桥接

R4S 的 CC98 poller 统一读取共享 bot 的 Telegram updates。桥接模块先把个人私聊和 X 复核按钮回执写入本机 SQLite，再允许 CC98 推进 Telegram offset。BWG 通过现有 SSH 连接读取 outbox，执行 X 复核和个人消息转交。

## 文件与职责

- x_review_bridge.py：配置、输入筛选、整批持久化，以及只读 export 命令。
- cc98_telegram_bot.py.patch：在 get_updates 增加已启用时的 callback 订阅，在 run_once 的原处理逻辑前提交整批 outbox。
- test_x_review_bridge.py：模块与原 poller 函数的集成测试。
- cc98_telegram_bot.py.baseline：补丁对应的 R4S 源码快照，SHA-256 为 0302e6ac7cfaadb8d93ae5ae166e739b915dcdbfcab8e10578be63ba9588a124。

模块仅使用 Python 标准库。R4S 当前 Python 3.11 可用，无新增第三方依赖。

## 启用配置

在 CC98 已加载的 runtime env 中设置两项；也可以在 CC98 配置中使用 x_review_outbox 和 x_review_owner_id：

    CC98_XREVIEW_OUTBOX=/opt/r4sbot/state/x-review-outbox.sqlite3
    CC98_XREVIEW_OWNER_ID=<owner-user-id>

未配置两项时桥接关闭，原订阅仍为 message。仅配置一项、owner 非正整数或 outbox 使用相对路径时返回错误，避免以不完整配置继续消费 updates。

启用后的订阅为 message 和 callback_query。仅保存 owner 在同一 private chat 发出的普通消息，以及 owner 对该 private chat 内具有正整数 message ID 的 xreview:Rxx:keep|merge|uncertain 按钮回执。服务消息、其他用户和群消息不写入 outbox。BWG 再对复核包、case、chat、message 和 owner 做精确映射校验。

## 持久化与消费

匹配 update 保存完整 JSON，主键为 update_id。相同内容重试不增加记录；既有 ID 的 owner、类型或 JSON 内容发生冲突时回滚整批。SQLite 使用 journal_mode=DELETE、synchronous=FULL，整批提交后 run_once 才继续原消息处理和 offset 保存；提交失败直接返回到原 poller 的失败重试路径，offset 保持不变。数据库新建权限为 600，新父目录权限为 700。

个人普通消息持久化后仍进入原 CC98 处理；已转交的 X 复核 callback 跳过原命令处理。outbox 不删除记录、不修改消费 cursor。BWG 的 cursor 由 BWG 保存。

先初始化空库，再启动 BWG 消费端：

    python3 /opt/r4sbot/x_review_bridge.py --state /opt/r4sbot/state/x-review-outbox.sqlite3 init

BWG 使用下面的只读命令取得完整 updates 数组。--after 为最后已确认 update ID；若 BWG 保存的是 next offset，传入 offset - 1。第一次可传 -1。每批最多 100 条。

    python3 /opt/r4sbot/x_review_bridge.py --state /opt/r4sbot/state/x-review-outbox.sqlite3 export --after -1 --owner <owner-user-id> --limit 100

export 以 SQLite mode=ro 和 query_only 打开数据库，不创建库、表或游标。缺库、坏库和非法记录均以非零状态退出，消费端保留原 cursor 后重试。

## 安装与回退

安装前核对 CC98 源文件与基线哈希，备份源码和 runtime env，检查旧 poller 的当前 offset。将模块与补丁作为同一次更新部署，初始化 outbox，设置配置后只重启一次 /etc/init.d/r4sbot。保留已有 offset，避免重读历史消息。BWG 保留 direct getUpdates 禁用状态，改为轮询 outbox。

回退时停用 BWG relay，再恢复 CC98 源码和启用前 runtime env，并重启 CC98。outbox 与原始备份保留，不回退 Telegram offset。

## 验证

在本目录运行：

    python3 -m unittest -v test_x_review_bridge.py

2026-09-24 本地 14 项测试通过，包括补丁在原基线应用、整批提交失败时不推进 offset、原 handler 调用前数据已持久化、X callback 跳过旧处理、重复 ID 幂等、payload 冲突整批回滚、owner/chat 校验和只读导出。远端部署与 Telegram 按钮回执由生产交接另行记录。
