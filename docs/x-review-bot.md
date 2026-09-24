# Telegram 人工复核与普通消息转交

x_review_bot.py 是 bot 的唯一 getUpdates 消费者。它接收 callback_query 和 message，先把整批更新保存到 state/x-review.sqlite3，再处理复核和确认 offset。

## 消费者交接

启动 BWG 服务前，先把 Mac growth 采集切换到 relay，停止它对该 bot 的 getUpdates 调用。不要使用负 offset 或清空待处理队列。HTTP 409 会使服务以状态 75 退出，模板禁止自动重启此状态，避免与其他消费者争抢。服务不删除 webhook。

## 运行文件

凭据和 owner 只从现有 config.json 的 telegram_bot_token、telegram_chat_id 读取。state/x-event-review-20.json、state/telegram-review-receipt.json 和 state/x-review.sqlite3 都是运行数据，不入仓库。部署模板是 [x-review-bot.service](../deploy/x-review-bot.service)。

    python3 x_review_bot.py --state /root/x_monitor/state/x-review.sqlite3 run --packet /root/x_monitor/state/x-event-review-20.json --receipt /root/x_monitor/state/telegram-review-receipt.json

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
