from __future__ import annotations
import ast
import io
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import tempfile
import unittest
from unittest import mock
import x_review_bridge as bridge
OWNER = 123456

def message(uid=1, owner=OWNER):
    return {"update_id": uid, "message": {"message_id": 8, "date": 1,
        "from": {"id": owner, "is_bot": False},
        "chat": {"id": owner, "type": "private"}, "text": "hello"}}

def callback(uid=2, owner=OWNER):
    return {"update_id": uid, "callback_query": {"id": "cb-1",
        "from": {"id": owner, "is_bot": False}, "data": "xreview:R01:merge",
        "message": {"message_id": 10, "chat": {"id": owner, "type": "private"}}}}

class BridgeTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.path = Path(self.temp.name) / "private" / "outbox.sqlite3"
        self.env = mock.patch.dict(os.environ, {}, clear=True); self.env.start()
    def tearDown(self):
        self.env.stop(); self.temp.cleanup()
    def read(self, after=-1, owner=OWNER, limit=100):
        return bridge.export_updates(path=self.path, after=after, owner=owner, limit=limit)
    def test_default_disabled_no_file_or_writes(self):
        self.assertIsNone(bridge.bridge_settings({}))
        self.assertEqual(bridge.allowed_updates({}), ["message"])
        self.assertEqual(bridge.persist_configured_batch({}, [message(), callback()]), set())
        self.assertFalse(self.path.exists())
    def test_config_requires_both_and_absolute_path(self):
        for cfg in [{"x_review_outbox": str(self.path)}, {"x_review_owner_id": OWNER},
                    {"x_review_outbox": "relative", "x_review_owner_id": OWNER},
                    {"x_review_outbox": str(self.path), "x_review_owner_id": True}]:
            with self.assertRaises(ValueError): bridge.bridge_settings(cfg)
    def test_environment_config(self):
        with mock.patch.dict(os.environ, {"CC98_XREVIEW_OUTBOX":str(self.path), "CC98_XREVIEW_OWNER_ID":str(OWNER)}):
            self.assertEqual(bridge.bridge_settings({}), (self.path, OWNER))
            self.assertEqual(bridge.allowed_updates({}), ["message", "callback_query"])
    def test_store_complete_payload_order_and_owner_filter(self):
        msg = message(2); msg["message"]["entities"] = [{"type": "url", "offset":0,"length":5}]
        cb = callback(1)
        self.assertEqual(bridge.persist_batch([msg,cb],path=self.path,owner=OWNER), {1})
        self.assertEqual(self.read(), [cb,msg]); self.assertEqual(self.read(after=1), [msg])
        self.assertEqual(self.read(owner=OWNER+1), [])
        self.assertEqual(self.path.stat().st_mode & 0o777, 0o600)
    def test_empty_batch_initializes_readable_outbox(self):
        bridge.persist_batch([], path=self.path, owner=OWNER); self.assertEqual(self.read(), [])
    def test_idempotency_and_conflict_rollback_whole_batch(self):
        a=message(1);bridge.persist_batch([a,a],path=self.path,owner=OWNER)
        self.assertEqual(self.read(), [a]);changed=message(1);changed["message"]["text"]="changed"
        with self.assertRaises(ValueError):bridge.persist_batch([message(2),changed],path=self.path,owner=OWNER)
        self.assertEqual(self.read(), [a])
    def test_rejects_callback_impostors_and_messages_outside_owner_dm(self):
        bad=[]
        for field,value in [("id", OWNER+1),("is_bot", True),("is_bot", None)]:
            u=callback();u["callback_query"]["from"][field]=value;bad.append(u)
        for field,value in [("id", OWNER+1),("type","group")]:
            u=callback();u["callback_query"]["message"]["chat"][field]=value;bad.append(u)
        for data in ["xreview:R01:evil","xreview:R1:merge","other:R01:merge","xreview:R01:merge\n"]:
            u=callback();u["callback_query"]["data"]=data;bad.append(u)
        bad.extend([message(owner=OWNER+1),{"update_id":3,"edited_message":message()["message"]}])
        u=message();u["message"]["chat"]["type"]="group";bad.append(u)
        u=message();u["message"].pop("text");u["message"]["new_chat_title"]="title";bad.append(u)
        u=callback();u["callback_query"]["message"]["message_id"]=True;bad.append(u)
        bridge.persist_batch(bad,path=self.path,owner=OWNER);self.assertEqual(self.read(), [])
    def test_invalid_export_bounds_and_missing_file_are_read_only(self):
        for limit in [0,101,-1,True]:
            with self.assertRaises(ValueError):self.read(limit=limit)
        with self.assertRaises(sqlite3.OperationalError):self.read()
        self.assertFalse(self.path.exists())
    def test_export_does_not_change_database_bytes(self):
        bridge.persist_batch([message()],path=self.path,owner=OWNER)
        before=self.path.read_bytes();before_stat=self.path.stat()
        self.assertEqual(self.read(limit=1), [message()]);self.assertEqual(self.path.read_bytes(),before)
        self.assertEqual(self.path.stat().st_mtime_ns,before_stat.st_mtime_ns)
        self.assertFalse(self.path.with_name(self.path.name+"-journal").exists())
    def test_durable_writer_pragmas(self):
        db=bridge._open_writer(self.path)
        self.assertEqual(db.execute("PRAGMA synchronous").fetchone()[0], 2)
        self.assertEqual(db.execute("PRAGMA journal_mode").fetchone()[0], "delete");db.close()
    def test_cli_init_and_export_contract(self):
        with mock.patch("sys.stdout",new_callable=io.StringIO):
            self.assertEqual(bridge.main(["--state",str(self.path),"init"]), 0)
        bridge.persist_batch([message()],path=self.path,owner=OWNER)
        with mock.patch("sys.stdout",new_callable=io.StringIO) as out:
            bridge.main(["--state",str(self.path),"export","--after","-1","--owner",str(OWNER),"--limit","100"])
            self.assertEqual(json.loads(out.getvalue()), [message()])
    def _patched_functions(self):
        root=Path(__file__).parent
        source=(root/"fixtures/cc98_update_functions.py").read_text()
        target=Path(self.temp.name)/"cc98_telegram_bot.py";target.write_text(source)
        subprocess.run(["patch","--batch","--silent","-p1","-i",str((root/"cc98_telegram_bot.py.patch").resolve())],cwd=self.temp.name,check=True)
        parsed=ast.parse(target.read_text())
        selected=[n for n in parsed.body if isinstance(n,ast.FunctionDef) and n.name in {"run_once","get_updates"}]
        ns={"load_state":lambda:{"last_update_id":0},"process_update":mock.Mock(),"save_state":mock.Mock(),"send_message":mock.Mock(),"allowed_chat_ids":lambda c:[],"traceback":mock.Mock(),"sys":mock.Mock()}
        exec(compile(ast.Module(body=selected,type_ignores=[]),str(target),"exec"),ns);return ns
    def test_patch_failure_cannot_advance_offset_or_call_old_handler(self):
        ns=self._patched_functions();ns["get_updates"]=lambda *a,**kw:[message(),callback()]
        with mock.patch.object(bridge,"persist_configured_batch",side_effect=OSError("disk full")):
            with self.assertRaises(OSError):ns["run_once"]({},None)
        ns["save_state"].assert_not_called();ns["process_update"].assert_not_called()
    def test_patch_persists_batch_before_old_handler_and_skips_xreview(self):
        ns=self._patched_functions();updates=[message(),callback()];ns["get_updates"]=lambda *a,**kw:updates
        seen=[];ns["process_update"].side_effect=lambda *a:seen.append(self.read())
        cfg={"x_review_outbox":str(self.path),"x_review_owner_id":OWNER}
        self.assertEqual(ns["run_once"](cfg,None),2)
        self.assertEqual(seen,[updates]);self.assertEqual(ns["save_state"].call_count,2)
        ns["process_update"].assert_called_once_with(cfg,None,updates[0])
    def test_patch_disabled_preserves_old_message_subscription(self):
        ns=self._patched_functions();api=mock.Mock(return_value=[]);ns["telegram_request"]=api
        ns["get_updates"]({},0);self.assertEqual(api.call_args.args[2]["allowed_updates"],["message"])
        ns["get_updates"]({"x_review_outbox":str(self.path),"x_review_owner_id":OWNER},5)
        self.assertEqual(api.call_args.args[2]["allowed_updates"],["message","callback_query"])
        self.assertEqual(api.call_args.args[2]["offset"],5)

if __name__ == "__main__": unittest.main()
