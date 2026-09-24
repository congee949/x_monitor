import copy
import json
from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest.mock import patch

import x_review_bot as xr


class FakeAPI:
    def __init__(self):
        self.calls = []
        self.updates = []
        self.fail_method = None
        self.failure = xr.BotError(500)

    def call(self, method, payload):
        self.calls.append((method, payload))
        if method == self.fail_method:
            raise self.failure
        return self.updates if method == 'getUpdates' else True


class ReviewTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.state = self.root/'state.sqlite3'
        self.ledger = self.root/'event.sqlite3'
        db=sqlite3.connect(self.ledger)
        db.executescript("""CREATE TABLE event_observations(id INTEGER PRIMARY KEY,event_key TEXT,
            candidate_tweet_id TEXT,candidate_username TEXT,decision TEXT,reviewed INTEGER,
            false_positive INTEGER,note TEXT);
            INSERT INTO event_observations VALUES (7,'event:a','200','claudeai','would_suppress',0,NULL,NULL);
            INSERT INTO event_observations VALUES (8,'event:b','201','OpenAI','material_update',0,NULL,NULL);
        """)
        db.close()
        self.packet={'cases':[
            {'case_id':'R01','gate_eligible':True,'source_kind':'online_observation',
             'observation_id':7,'machine_decision':'would_suppress','event_key':'event:a',
             'candidate':{'tweet_id':'200','username':'claudeai','url':'https://x.com/claudeai/status/200'},
             'prior':{'url':'https://x.com/claudeai/status/100'}},
            {'case_id':'R02','gate_eligible':False,'source_kind':'historical_pair',
             'observation_id':8,'candidate':{'tweet_id':'201','username':'OpenAI'}}]}
        self.receipt={'chat_id':'42','receipts':[{'case_id':'R01','message_id':301,'ok':True},
                                               {'case_id':'R02','message_id':302,'ok':True}]}
        self.api=FakeAPI()
        self.store=xr.ReviewStore(self.state)
        self.bot=xr.ReviewBot(self.api,self.store,self.packet,self.receipt,42,self.ledger)

    def tearDown(self):
        self.store.close();self.tmp.cleanup()

    def update(self, update_id=1, verdict='keep', case='R01', actor=42, chat=42, message=301):
        return {'update_id':update_id,'callback_query':{'id':f'cb{update_id}',
            'from':{'id':actor,'is_bot':False},'data':f'xreview:{case}:{verdict}',
            'message':{'message_id':message,'chat':{'id':chat,'type':'private'},
                       'text':'Review pair <plain>','entities':[{'type':'bold','offset':0,'length':6}]}}}

    def gate(self, observation=7):
        db=sqlite3.connect(self.ledger)
        row=db.execute('SELECT reviewed,false_positive FROM event_observations WHERE id=?',(observation,)).fetchone()
        db.close();return row

    def audit(self):
        return self.store.db.execute('SELECT * FROM callbacks ORDER BY seq').fetchall()

    def test_owner_click_updates_gate_and_preserves_message_entities(self):
        self.bot.handle(self.update())
        self.assertEqual(self.gate(),(1,1));self.assertEqual(self.store.offset(),2)
        self.assertEqual(len(self.audit()),1);self.assertEqual(self.audit()[0]['actor_id'],'42')
        edit=next(payload for method,payload in self.api.calls if method=='editMessageText')
        self.assertIn('复核状态：保留两条',edit['text'])
        self.assertEqual(edit['entities'][0]['type'],'bold')
        self.assertTrue(edit['reply_markup']['inline_keyboard'][-1][0]['text'].startswith('✓'))
        self.assertIn('answerCallbackQuery',[method for method,_ in self.api.calls])

    def test_other_actor_chat_and_cross_message_are_rejected(self):
        variants=[{'actor':99},{'chat':99},{'message':302},{'case':'R99'}]
        for number, variant in enumerate(variants,1):
            self.bot.handle(self.update(number,**variant))
        self.assertEqual(self.gate(),(0,None))
        self.assertTrue(all(row['accepted']==0 for row in self.audit()))
        self.assertNotIn('editMessageText',[method for method,_ in self.api.calls])

    def test_duplicate_update_and_callback_are_idempotent(self):
        first=self.update();self.bot.handle(first);self.bot.handle(first)
        self.bot.handle(self.update(2,'merge'))
        duplicate=copy.deepcopy(first);duplicate['update_id']=3
        self.bot.handle(duplicate)
        self.assertEqual(self.gate(),(1,0))
        self.assertEqual(len(self.audit()),2)
        self.assertEqual(self.store.offset(),4)

    def test_changed_verdict_and_uncertain_remove_gate_label(self):
        self.bot.handle(self.update(1,'merge'))
        self.assertEqual(self.gate(),(1,0))
        self.bot.handle(self.update(2,'keep'))
        self.assertEqual(self.gate(),(1,1))
        self.bot.handle(self.update(3,'uncertain'))
        self.assertEqual(self.gate(),(0,None));self.assertEqual(len(self.audit()),3)
        edits=[p for m,p in self.api.calls if m=='editMessageText']
        self.assertEqual(edits[-1]['text'].count('复核状态：'),1)

    def test_historical_case_records_click_without_touching_gate(self):
        before=self.ledger.read_bytes()
        self.bot.handle(self.update(case='R02',message=302,verdict='merge'))
        self.assertEqual(self.ledger.read_bytes(),before)
        self.assertEqual(self.audit()[0]['accepted'],1)
        self.assertEqual(self.gate(8),(0,None))

    def test_observation_identity_must_match_packet(self):
        for column,value in [('event_key','wrong'),('candidate_tweet_id','999'),
                             ('candidate_username','wrong'),('decision','material_update')]:
            with self.subTest(column=column):
                db=sqlite3.connect(self.ledger)
                original=db.execute(f'SELECT {column} FROM event_observations WHERE id=7').fetchone()[0]
                db.execute(f'UPDATE event_observations SET {column}=? WHERE id=7',(value,));db.commit();db.close()
                update_id=len(self.audit())+1
                self.bot.handle(self.update(update_id))
                self.assertEqual(self.gate(),(0,None));self.assertEqual(self.audit()[-1]['accepted'],0)
                db=sqlite3.connect(self.ledger);db.execute(f'UPDATE event_observations SET {column}=? WHERE id=7',(original,));db.commit();db.close()

    def test_regular_messages_are_retained_and_relay_is_owner_only_read_only(self):
        for update_id,owner in [(1,42),(2,99),(3,42)]:
            self.bot.handle({'update_id':update_id,'message':{'message_id':500+update_id,
                'from':{'id':owner},'chat':{'id':owner,'type':'private'},'date':100+update_id,
                'text':f'message{update_id}'}})
        before=self.state.read_bytes()
        rows=xr.relay(self.state,1,42)
        self.assertEqual(rows,[{'id':503,'update_id':3,'date':103,'text':'message3'}])
        self.assertEqual(self.state.read_bytes(),before)
        self.assertEqual(self.store.db.execute('SELECT count(*) FROM messages').fetchone()[0],3)

    def test_edit_failure_does_not_advance_offset_and_retry_is_single_audit(self):
        self.api.fail_method='editMessageText'
        with self.assertRaises(xr.BotError):self.bot.handle(self.update())
        self.assertEqual(self.store.offset(),0);self.assertEqual(len(self.audit()),1)
        self.api.fail_method=None;self.bot.handle(self.update())
        self.assertEqual(self.store.offset(),2);self.assertEqual(len(self.audit()),1)

    def test_ledger_failure_does_not_advance_offset_or_acknowledge(self):
        self.bot.ledger=self.root/'absent.sqlite3'
        with self.assertRaises(sqlite3.OperationalError):self.bot.handle(self.update())
        self.assertEqual(self.store.offset(),0);self.assertEqual(len(self.audit()),1)
        self.assertEqual(self.api.calls,[])

    def test_poll_failure_stops_before_later_message_and_getupdates_retains_types(self):
        self.api.updates=[self.update(),{'update_id':2,'message':{'message_id':501}}]
        self.api.fail_method='editMessageText'
        with self.assertRaises(xr.BotError):self.bot.poll()
        self.assertEqual(self.store.offset(),0)
        self.assertEqual(self.store.db.execute('SELECT count(*) FROM messages').fetchone()[0],1)
        get=self.api.calls[0][1]
        self.assertEqual(get['allowed_updates'],['callback_query','message'])
        self.assertEqual(get['offset'],0)

    def test_conflict_is_surfaced_without_deleting_webhook_or_advancing_offset(self):
        self.api.fail_method='getUpdates';self.api.failure=xr.BotError(409)
        with self.assertRaises(xr.BotError) as caught:self.bot.poll()
        self.assertEqual(caught.exception.code,409)
        self.assertEqual(self.store.offset(),0)
        self.assertEqual([name for name,_ in self.api.calls],['getUpdates'])

    def test_restart_recovers_durable_callback_and_later_message_without_new_updates(self):
        self.api.updates=[self.update(), {'update_id':2,'message':{'message_id':502,
            'from':{'id':42},'chat':{'id':42,'type':'private'},'date':100,'text':'keep this DM'}}]
        self.api.fail_method='editMessageText'
        with self.assertRaises(xr.BotError):self.bot.poll()
        self.store.close()
        self.store=xr.ReviewStore(self.state)
        self.bot=xr.ReviewBot(self.api,self.store,self.packet,self.receipt,42,self.ledger)
        self.api.fail_method=None;self.api.updates=[]
        self.bot.poll()
        self.assertEqual(self.store.offset(),3)
        self.assertEqual(len(self.audit()),1)
        self.assertEqual(xr.relay(self.state,0,42)[0]['text'],'keep this DM')

    def test_run_exits_75_on_consumer_conflict(self):
        config=self.root/'config.json'; packet=self.root/'packet.json'; receipt=self.root/'receipt.json'
        config.write_text(json.dumps({'telegram_bot_token':'test-token','telegram_chat_id':42}))
        packet.write_text(json.dumps(self.packet));receipt.write_text(json.dumps(self.receipt))
        self.api.fail_method='getUpdates';self.api.failure=xr.BotError(409)
        with patch.object(xr,'Telegram',return_value=self.api):
            result=xr.main(['--state',str(self.state),'run','--config',str(config),
                '--packet',str(packet),'--receipt',str(receipt),'--ledger',str(self.ledger)])
        self.assertEqual(result,75)

    def test_caption_and_nontext_messages_are_retained(self):
        self.bot.handle({'update_id':1,'message':{'message_id':601,'date':100,
            'from':{'id':42},'chat':{'id':42,'type':'private'},'caption':'photo caption','photo':[{'file_id':'x'}]}})
        self.bot.handle({'update_id':2,'message':{'message_id':602,'date':101,
            'from':{'id':42},'chat':{'id':42,'type':'private'},'sticker':{'file_id':'y'}}})
        self.assertEqual([row['text'] for row in xr.relay(self.state,0,42)],['photo caption',''])
        raw=self.store.db.execute('SELECT payload_json FROM messages WHERE update_id=2').fetchone()[0]
        self.assertIn('sticker',json.loads(raw))

    def test_answer_failure_retries_without_losing_click_or_advancing_offset(self):
        self.api.fail_method='answerCallbackQuery'
        with self.assertRaises(xr.BotError):self.bot.handle(self.update())
        self.assertEqual(self.store.offset(),0);self.assertEqual(len(self.audit()),1)
        self.api.fail_method=None;self.bot.handle(self.update())
        self.assertEqual(self.store.offset(),2);self.assertEqual(len(self.audit()),1)

    def test_packet_owner_mapping_and_binding_validation(self):
        bad=copy.deepcopy(self.receipt);bad['chat_id']='99'
        with self.assertRaises(ValueError):xr.ReviewBot(self.api,self.store,self.packet,bad,42,self.ledger)
        bad=copy.deepcopy(self.receipt);bad['receipts'][1]['message_id']=301
        with self.assertRaises(ValueError):xr.ReviewBot(self.api,self.store,self.packet,bad,42,self.ledger)
        packet=copy.deepcopy(self.packet);packet['created_at']='different'
        with self.assertRaises(ValueError):xr.ReviewBot(self.api,self.store,packet,self.receipt,42,self.ledger)


if __name__=='__main__':unittest.main()
