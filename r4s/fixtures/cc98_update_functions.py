"""Original CC98 update functions used only as isolated patch fixtures."""


def get_updates(config, offset, timeout=25):
    payload = {"timeout": timeout, "allowed_updates": ["message"]}
    if offset:
        payload["offset"] = offset
    # socket 超时只比 long-poll 多 5s：连接被静默切断时最多空转 timeout+5，
    # 而不是所有调用共用的 35s（其它调用仍用默认 35s）。
    return telegram_request(config, "getUpdates", payload, timeout=timeout + 5)


def run_once(config, profile_key):
    state = load_state()
    offset = state.get("last_update_id", 0) + 1 if state.get("last_update_id", 0) else 0
    updates = get_updates(config, offset=offset, timeout=25)
    for update in updates:
        try:
            process_update(config, profile_key, update)
        except Exception as e:
            traceback.print_exc(file=sys.stderr)
            try:
                # 仅向白名单 chat 回报错误，绝不把异常串发给陌生/未授权 chat。
                chat_id = str((update.get("message") or {}).get("chat", {}).get("id", "")).strip()
                if chat_id and chat_id in allowed_chat_ids(config):
                    send_message(config, chat_id, f"处理失败：{type(e).__name__}: {e}")
            except Exception:
                pass
        # offset 必须无论如何前进，否则同一 update 会被无限重试
        state["last_update_id"] = update["update_id"]
        save_state(state)
    return len(updates)
