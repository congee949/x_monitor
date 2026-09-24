# Telegram 人工复核与普通消息转交

r4s 上的 CC98 poller 是共享 bot 的唯一 getUpdates 消费者。它将 owner 私聊和 X 复核 callback 整批写入 outbox；BWG 上的 x_review_relay.py 通过 SSH 读取 outbox，保存到 state/x-review.sqlite3 后处理复核。

## 消费者交接

启动 BWG 服务前，先确认 r4s CC98 bridge 已启用，并把 Mac growth 采集切换到 relay。BWG 服务使用 x_review_relay.py，不调用 Telegram getUpdates；不要使用负 offset 或清空待处理队列。服务不删除 webhook。

## 运行文件

凭据和 owner 只从现有 config.json 的 telegram_bot_token、telegram_chat_id 读取。state/x-event-review-20.json、state/telegram-review-receipt.json 和 state/x-review.sqlite3 都是运行数据，不入仓库。部署模板是 [x-review-bot.service](../deploy/x-review-bot.service)。

    python3 x_review_relay.py --state /root/x_monitor/state/x-review.sqlite3 --packet /root/x_monitor/state/x-event-review-20.json --receipt /root/x_monitor/state/telegram-review-receipt.json

## 复核规则

按钮必须同时匹配 owner 的 from.id、私聊 chat.id、receipt 的 message_id 和 case ID。每次真实点击保存到 callbacks 审计表；重复 callback ID 只记录一次，新点击可以改判。原消息显示最新状态和选中按钮。

只有 gate_eligible=true、source_kind=online_observation、machine_decision=would_suppress 的案例可以写事件账本。写入前验证 observation ID、event key、candidate tweet ID、candidate username 和实际 decision。历史补充案例只写 sidecar。

| 按钮 | reviewed | false_positive |
|---|---|---|
| keep 保留两条 | 1 | 1 |
| merge 可以合并 | 1 | 0 |
| uncertain 不确定 | 0 | NULL |

服务不修改 event_dedup_mode，不会开启 enforce。账本或 Telegram 编辑失败时保留更新与点击，不推进 offset；重启后恢复本地未完成记录。

## Mac relay

    python3 x_review_bot.py --state /root/x_monitor/state/x-review.sqlite3 relay --after 0 --owner OWNER_ID

--after 是最后已接收的 Telegram update_id。输出为 JSON 数组，元素含 id（message_id）、update_id、date、text。relay 同时要求 sender 和 private chat 都等于 owner。caption 投影到 text，其他非文本消息的原始 payload 保存在 SQLite。调用方成功保存消息后再推进自身游标；旧 Bot API offset 若存的是 next offset，转为 --after 时应减一。relay 只读打开数据库，不消费或删除消息。

针对测试：python3 -m unittest -q test_x_review_bot.py。API 行为参考 [Telegram Bot API](https://core.telegram.org/bots/api#getupdates)。

## 富媒体复核卡

回调中的 rich_message 对象可作为可访问原卡。富媒体卡只调用 editMessageReplyMarkup 更新选中按钮，不编辑正文，因此图片、视频和原生富文本保持原样。answerCallbackQuery 显示“已选择 20/20”和“门槛有效标注 8/8”等进度；前者包含历史案例和 uncertain，后者只计 gate eligible 案例中的 keep/merge。纯文本卡继续更新正文状态。已有 SQLite 会自动补上 rich_message 标记列。

## 复核卡媒体回填

`x_review_cards.py` 根据固定 packet、Telegram receipt 和只读 X 详情缓存生成 `editMessageText` 的 `rich_message`。`tools_x_review_media_fetch.py` 只读抓取固定 tweet ID，`tools_x_review_media_cards.py preview` 先生成 HTML 与 payload，`apply` 在 review consumer lock 下原地更新既有消息，并保留当前选择。运行数据、详情缓存和 Telegram receipt 放在 `state/`，不提交 Git。

## 共享 Telegram 接收器

r4s 上的 CC98 poller 是该 bot 的唯一 `getUpdates` 消费者。`x_review_relay.py` 通过 SSH 读取 r4s bridge 的只读导出，向 BWG 的复核处理器交付 callback；BWG 不再直接调用 `getUpdates`。服务交接前必须确认 CC98 bridge 已启用且 outbox 可读。
