import json
import sqlite3
import tempfile
import unittest
from pathlib import Path
from datetime import datetime, timedelta, timezone
import quote_fold
import thread_merge

class FoldTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.path=Path(self.tmp.name)/'ledger.db'
        db=sqlite3.connect(self.path)
        db.executescript('''CREATE TABLE deliveries (message_id TEXT,facts_json TEXT,username TEXT,tweet_id TEXT,target_chat_id TEXT,target_thread_id TEXT,state TEXT,updated_at TEXT);
        CREATE TABLE tweet_anchors (message_id TEXT,target_chat_id TEXT,target_thread_id TEXT,tweet_id TEXT,updated_at TEXT);''')
        db.execute('INSERT INTO deliveries VALUES (?,?,?,?,?,?,?,?)',('50','["opus 5.5", "price"]','claudeai','100','-1001','19','confirmed',datetime.now(timezone.utc).isoformat()))
        db.commit();db.close()
    def tearDown(self):self.tmp.cleanup()
    def plan(self,tweet,**kw):
        args=dict(username='bcherny',chat_id='-1001',thread_id=19,ledger_path=self.path,
                  translation_reply=True,groups=quote_fold.normalize_groups({'a':['claudeai','bcherny']}),facts=['opus 5.5'])
        args.update(kw);return quote_fold.plan(tweet,**args)
    def quote(self):return {'id':'200','quoted_status':{'id':'100','screen_name':'claudeai'}}
    def test_official_no_facts_skips_only_same_confirmed_target(self):
        self.assertEqual(self.plan(self.quote())['action'],'skip')
        self.assertEqual(self.plan(self.quote(),thread_id=41)['action'],'deliver')
        self.assertEqual(self.plan(self.quote(),username='dotey')['action'],'deliver')
    def test_increment_and_image_remain_reply(self):
        self.assertEqual(self.plan(self.quote(),facts=['new-fact'])['action'],'reply')
        t=self.quote();t['media']=[{'url':'photo'}]
        self.assertEqual(self.plan(t)['action'],'reply')
    def test_translation_known_source_reply_unknown_source_delivers(self):
        t={'_quote_translation':{'action':'source_only','source_id':'100'}}
        self.assertEqual(self.plan(t,username='dotey')['action'],'reply')
        t['_quote_translation']['source_id']='404'
        self.assertEqual(self.plan(t)['action'],'deliver')
    def test_ambiguous_never_anchors(self):
        db=sqlite3.connect(self.path);db.execute("UPDATE deliveries SET state='ambiguous'");db.commit();db.close()
        self.assertEqual(self.plan(self.quote())['action'],'deliver')
    def test_off_disables_even_malformed_bundle(self):
        self.assertEqual(self.plan({'semantic_bundle':'bad'},groups=[],translation_reply=False)['action'],'deliver')
    def test_source_only_alias_resolves_confirmed_full_card(self):
        db=sqlite3.connect(self.path);db.execute('INSERT INTO tweet_anchors VALUES (?,?,?,?,?)',('50','-1001','19','300',datetime.now(timezone.utc).isoformat()));db.commit();db.close()
        t={'_quote_translation':{'action':'source_only','source_id':'300'}}
        self.assertEqual(self.plan(t)['message_id'],50)

    def test_same_round_source_later_becomes_foldable_after_confirmation(self):
        # ClaudeDevs may be visited before claudeai. Before the source is confirmed,
        # folding must fail open. A later call sees the confirmed source; account
        # ordering makes that happen before the quote's single delivery pass.
        db=sqlite3.connect(self.path)
        db.execute("DELETE FROM deliveries")
        db.commit();db.close()
        quote = self.quote()
        args = {'username': 'ClaudeDevs',
                'groups': quote_fold.normalize_groups({'a': ['claudeai', 'ClaudeDevs']})}
        self.assertEqual(self.plan(quote, **args)['action'], 'deliver')
        db=sqlite3.connect(self.path)
        db.execute('INSERT INTO deliveries VALUES (?,?,?,?,?,?,?,?)',
                   ('51','["opus 5.5", "price"]','claudeai','100','-1001','19',
                    'confirmed',datetime.now(timezone.utc).isoformat()))
        db.commit();db.close()
        self.assertEqual(self.plan(quote, **args)['action'], 'skip')
        self.assertEqual(self.plan(quote, facts=['new-api-fact'], **args)['action'], 'reply')

    def test_malformed_metadata_fails_open(self):
        for tweet in ({'semantic_bundle': {'context_nodes': 'bad'}},
                      {'_quote_translation': ['bad']},
                      {'quoted_status': 'bad'}):
            self.assertEqual(self.plan(tweet)['action'], 'deliver')

    def test_curator_anchor_in_official_repost_is_preserved(self):
        tweet = {'semantic_bundle': {'anchor': {'author': 'curator'},
                 'context_nodes': [{'tweet_id':'100','author':'claudeai'}],
                 'resolution': {'status':'complete'}}}
        self.assertEqual(self.plan(tweet)['action'],'deliver')

    def test_idle_thread_merge_preserves_member_ids(self):
        now=datetime.now(timezone.utc);stamp=(now-timedelta(minutes=3)).isoformat()
        a={'id':'100','text':'release','conversation_id_str':'100','created_at':stamp,'_push_event_type':'model_launch'}
        b={'id':'101','text':'price','conversation_id_str':'100','created_at':stamp,'in_reply_to_status':{'id':'100','screen_name':'claudeai'},'_push_event_type':'model_launch'}
        result=thread_merge.merge_ready([(a,'a'),(b,'b')],'claudeai',enabled=True,now=now)
        self.assertEqual(len(result),1)
        self.assertEqual([t['id'] for t in result[0][0]['_official_thread_members']],['100','101'])
        b['created_at']=now.isoformat()
        self.assertEqual(len(thread_merge.merge_ready([(a,'a'),(b,'b')],'claudeai',enabled=True,now=now)),2)


class IdleWindowTests(unittest.TestCase):
    def test_only_release_event_types_enter_the_idle_window(self):
        now = datetime.now(timezone.utc)
        for event_type in thread_merge.IDLE_WINDOW_EVENT_TYPES:
            with self.subTest(event_type=event_type):
                tweet = {'id': '100', 'text': 'Announcement',
                         'created_at': now.isoformat(), '_push_event_type': event_type}
                deferred = []
                ready = thread_merge.merge_ready([(tweet, 'test')], 'claudeai',
                    enabled=True, now=now, deferred=deferred)
                self.assertEqual(ready, [])
                self.assertEqual(deferred, [(tweet, 'test')])

    def test_quota_and_entitlement_first_posts_are_immediate(self):
        now = datetime.now(timezone.utc)
        for event_type in ('quota_reset', 'quota_compensation', 'credit_grant',
                           'quota_policy', 'plan_entitlement', 'model_access', None, 'unknown'):
            with self.subTest(event_type=event_type):
                tweet = {'id': '100', 'text': 'Announcement',
                         'created_at': now.isoformat(), '_push_event_type': event_type}
                deferred = []
                ready = thread_merge.merge_ready([(tweet, 'test')], 'claudeai',
                    enabled=True, now=now, deferred=deferred)
                self.assertEqual(ready, [(tweet, 'test')])
                self.assertEqual(deferred, [])

    def test_urgent_member_cannot_inherit_release_delay(self):
        now = datetime.now(timezone.utc)
        root = {'id': '100', 'text': 'Release', 'created_at': now.isoformat(),
                'conversation_id_str': '100', '_push_event_type': 'model_launch'}
        reply = {'id': '101', 'text': 'Quota compensation', 'created_at': now.isoformat(),
                 'conversation_id_str': '100', '_push_event_type': 'quota_compensation',
                 'in_reply_to_status': {'id': '100', 'screen_name': 'claudeai'}}
        deferred = []
        ready = thread_merge.merge_ready([(root, 'a'), (reply, 'b')], 'claudeai',
            enabled=True, now=now, deferred=deferred)
        self.assertEqual([item[0]['id'] for item in deferred], ['100'])
        self.assertEqual([item[0]['id'] for item in ready], ['101'])

    def test_media_first_post_never_enters_idle_window(self):
        now = datetime.now(timezone.utc)
        tweet = {'id': '100', 'text': 'Release', 'created_at': now.isoformat(),
                 '_push_event_type': 'model_launch', 'media': [{'type': 'photo'}]}
        deferred = []
        ready = thread_merge.merge_ready([(tweet, 'test')], 'claudeai',
            enabled=True, now=now, deferred=deferred)
        self.assertEqual(ready, [(tweet, 'test')])
        self.assertEqual(deferred, [])


class ThreadDeliveryTest(unittest.TestCase):
    def process(self, tweets, *, outcome=None, now=None, retry_records=None,
                fold=None, journal_error=None):
        from contextlib import ExitStack
        from unittest.mock import patch, Mock
        import argparse
        import twitter_monitor as tm
        from test_twitter_monitor import FakeAI
        clock = now or datetime.now(timezone.utc)
        class Clock(datetime):
            @classmethod
            def now(cls, tz=None):
                return clock if tz is not None else clock.replace(tzinfo=None)
        with tempfile.TemporaryDirectory() as directory, ExitStack() as stack:
            stack.enter_context(patch.object(tm, 'SEEN_DIR', directory))
            stack.enter_context(patch.object(tm, 'EVENT_LEDGER_PATH', str(Path(directory)/'ledger.db')))
            stack.enter_context(patch.object(tm, 'SEMANTIC_DECISION_JOURNAL', str(Path(directory)/'journal')))
            stack.enter_context(patch.object(tm, '_ACCOUNT_CONFIG_BY_USERNAME',
                                           {'claudeai': {'push_policy': 'test'}}))
            stack.enter_context(patch.object(tm, '_OFFICIAL_THREAD_MERGE_ENABLED', True))
            stack.enter_context(patch.object(tm, '_SENT_CONTENT_LEDGER_ENABLED', False))
            stack.enter_context(patch.object(tm, '_SEMANTIC_BUNDLE_ENABLED', False))
            stack.enter_context(patch.object(tm, '_SEMANTIC_BUNDLE_SHADOW', False))
            stack.enter_context(patch.object(tm, '_EVENT_DEDUP_EFFECTIVE_MODE', 'off'))
            stack.enter_context(patch.object(tm, '_CROSS_DEDUP_ENABLED', False))
            stack.enter_context(patch.object(tm, 'datetime', Clock))
            stack.enter_context(patch.object(tm, 'fetch_tweets', return_value=tweets))
            stack.enter_context(patch.object(tm, 'classify_official_push',
                                           side_effect=lambda _policy, tweet: ('pass', 'test', tweet.get('_push_event_type') or 'model_launch')))
            stack.enter_context(patch.object(tm, '_verify_configured_account_identity'))
            stack.enter_context(patch.object(tm.time, 'sleep'))
            stack.enter_context(patch.object(tm, '_article_queue_time_remaining', return_value=10000))
            if fold is not None:
                stack.enter_context(patch.object(tm.quote_fold, 'plan', return_value=fold))
                stack.enter_context(patch.object(tm, '_journal_quote_fold',
                                               side_effect=journal_error))
            tm.save_seen('claudeai', {'old'})
            if retry_records:
                tm._PUSH_RETRY_STATE_BY_USER['claudeai'] = retry_records
                tm.save_push_retry('claudeai', set(retry_records))
            send=stack.enter_context(patch.object(tm, 'send_tweet',
                        return_value=outcome or {'ok':True,'result':{'message_id':50}}))
            tm.process_user(None, FakeAI(False), 'claudeai', 'test', 'group',
                            argparse.Namespace(test=False, seed=False, dry_run=False,
                                               limit=20, max_push_age_minutes=0))
            seen=tm.load_seen('claudeai')[0]
            retry=tm.load_push_retry('claudeai')
            records=dict(tm._PUSH_RETRY_STATE_BY_USER.get('claudeai') or {})
            return send.call_args_list, seen, retry, records

    def pair(self, seconds=180):
        stamp=(datetime.now(timezone.utc)-timedelta(seconds=seconds)).isoformat()
        return [{'id':'100','text':'A complete launch announcement with enough text.',
                 'created_at':stamp,'conversation_id_str':'100','_push_event_type':'model_launch'},
                {'id':'101','text':'Pricing and API availability details with enough text.',
                 'created_at':stamp,'conversation_id_str':'100',
                 'in_reply_to_status':{'id':'100','screen_name':'claudeai'},'_push_event_type':'model_launch'}]

    def test_merged_success_checkpoints_all_members_and_clears_retry(self):
        tweets=self.pair()
        calls,seen,retry,_=self.process(tweets,retry_records={'100':{},'101':{}})
        self.assertEqual(len(calls),1)
        self.assertEqual({t['id'] for t in calls[0].args[3]['_official_thread_members']},{'100','101'})
        self.assertTrue({'100','101'}<=seen)
        self.assertEqual(retry,set())

    def test_merged_send_failure_keeps_all_members_retryable(self):
        calls,seen,retry,records=self.process(self.pair(),outcome={'ok':False})
        self.assertEqual(len(calls),1)
        self.assertFalse({'100','101'} & seen)
        self.assertEqual(retry,{'100','101'})
        self.assertEqual(records['101']['thread_tweet']['id'],'101')


    def test_failed_terminal_journal_delivers_instead_of_skipping(self):
        calls,seen,retry,_=self.process(self.pair()[:1],
            fold={'action':'skip'},journal_error=OSError('full disk'))
        self.assertEqual(len(calls),1)
        self.assertIn('100',seen)
        self.assertEqual(retry,set())

    def test_active_thread_defers_every_member_without_seen(self):
        calls,seen,retry,records=self.process(self.pair(seconds=10))
        self.assertEqual(calls,[])
        self.assertFalse({'100','101'} & seen)
        self.assertEqual(retry,{'100','101'})
        self.assertEqual(records['101']['thread_tweet']['id'],'101')

    def test_idle_thread_recovered_from_retry_even_when_not_in_timeline(self):
        tweets=self.pair()
        records={t['id']:{'thread_tweet':t} for t in tweets}
        calls,seen,retry,_=self.process([],retry_records=records)
        self.assertEqual(len(calls),1)
        self.assertTrue({'100','101'}<=seen)
        self.assertEqual(retry,set())

    def test_quota_event_is_immediate_even_inside_idle_window(self):
        tweets=self.pair(seconds=10)
        for tweet in tweets:
            tweet['_push_event_type'] = 'quota_reset'
        calls,seen,retry,_=self.process(tweets)
        self.assertEqual(len(calls),2)
        self.assertTrue({'100','101'} <= seen)
        self.assertEqual(retry,set())


class DryRunFoldTest(unittest.TestCase):
    def test_preview_uses_same_fold_plan_without_writing_delivery_state(self):
        import argparse
        import io
        from contextlib import ExitStack, redirect_stdout
        from unittest.mock import patch
        import twitter_monitor as tm
        from test_twitter_monitor import FakeAI
        tweet = {'id': '200', 'text': 'Opus 5.5 launch and API pricing details',
                 'created_at': (datetime.now(timezone.utc)-timedelta(minutes=3)).isoformat(),
                 'quoted_status': {'id': '100', 'screen_name': 'claudeai'}}
        with tempfile.TemporaryDirectory() as directory, ExitStack() as stack:
            for name, value in {
                'SEEN_DIR': directory, 'EVENT_LEDGER_PATH': str(Path(directory)/'ledger.db'),
                '_ACCOUNT_CONFIG_BY_USERNAME': {'claudedevs': {'push_policy': 'test'}},
                '_OFFICIAL_THREAD_MERGE_ENABLED': True, '_TRANSLATION_REPLY_ENABLED': False,
                '_OFFICIAL_QUOTE_GROUPS': quote_fold.normalize_groups(
                    {'a': ['claudeai', 'ClaudeDevs']}),
                '_SEMANTIC_BUNDLE_ENABLED': False, '_SEMANTIC_BUNDLE_SHADOW': False,
                '_EVENT_DEDUP_EFFECTIVE_MODE': 'off', '_CROSS_DEDUP_ENABLED': False,
            }.items():
                stack.enter_context(patch.object(tm, name, value))
            stack.enter_context(patch.object(tm, 'fetch_tweets', return_value=[tweet]))
            stack.enter_context(patch.object(tm, 'load_seen', return_value=({'old'}, None)))
            stack.enter_context(patch.object(tm, 'load_push_retry', return_value=set()))
            stack.enter_context(patch.object(tm, '_verify_configured_account_identity'))
            stack.enter_context(patch.object(tm, 'classify_official_push',
                                           return_value=('pass', 'launch', 'model_launch')))
            stack.enter_context(patch.object(tm, 'format_message',
                                           return_value=('text', 'rich', 'https://x.com/x/status/200')))
            planned = stack.enter_context(patch.object(tm.quote_fold, 'plan',
                              return_value={'action': 'skip', 'reason': 'official_quote_no_new_facts',
                                            'source_id': '100', 'message_id': 50}))
            writes = [stack.enter_context(patch.object(tm, name)) for name in
                      ('send_tweet', 'save_seen', 'save_push_retry', '_journal_quote_fold',
                       'claim_event_delivery')]
            output = io.StringIO()
            with redirect_stdout(output):
                tm.process_user(None, FakeAI(False), 'ClaudeDevs', 'test', 'dm',
                    argparse.Namespace(test=False, seed=False, dry_run=True, limit=20,
                                       max_push_age_minutes=0),
                    content_chat_id='group', content_thread_id=19)
            self.assertIn('quote-fold dry-run: tweet=200 action=skip', output.getvalue())
            self.assertEqual(planned.call_args.kwargs['chat_id'], 'group')
            self.assertEqual(planned.call_args.kwargs['thread_id'], 19)
            for write in writes:
                write.assert_not_called()


class DryRunStartupTest(unittest.TestCase):
    def test_main_preview_does_not_recover_pending_or_create_missing_ledger(self):
        import io
        import sys
        from contextlib import ExitStack, redirect_stdout
        from unittest.mock import patch
        import twitter_monitor as tm
        from test_twitter_monitor import FakeAI
        for existing in (False, True):
            with self.subTest(existing=existing), tempfile.TemporaryDirectory() as directory, ExitStack() as stack:
                directory = Path(directory)
                config = directory/'config.json'
                config.write_text(json.dumps({'telegram_bot_token': 'bot', 'telegram_chat_id': 'chat',
                    'event_dedup_mode': 'observe', 'translation_reply_enabled': True,
                    'official_quote_groups': {}, 'official_thread_merge_enabled': False}))
                ledger = directory/'event.sqlite3'
                if existing:
                    db = sqlite3.connect(ledger)
                    db.executescript("CREATE TABLE deliveries (state TEXT, updated_at TEXT);"
                        "INSERT INTO deliveries VALUES ('pending','2026-01-01T00:00:00+00:00');"
                        "CREATE TABLE event_observations (decision TEXT,reviewed INTEGER,false_positive INTEGER);")
                    db.commit(); db.close()
                    before = ledger.read_bytes()
                for name, value in {'CONFIG_PATH': str(config), 'SCRIPT_DIR': str(directory),
                                    'EVENT_LEDGER_PATH': str(ledger), 'HAS_GRAPHQL': False}.items():
                    stack.enter_context(patch.object(tm, name, value))
                stack.enter_context(patch.object(tm, 'apply_route_overlay'))
                stack.enter_context(patch.object(tm.TokenPool, 'load', return_value=None))
                stack.enter_context(patch.object(tm.AIClassifier, 'load', return_value=FakeAI(False)))
                stack.enter_context(patch.object(tm, 'load_accounts', return_value=[{'username': 'u'}]))
                stack.enter_context(patch.object(tm, 'load_account_failures', return_value={}))
                stack.enter_context(patch.object(tm, 'process_user', return_value=(0,0,0,0)))
                stack.enter_context(patch.object(tm, 'process_article_queue', return_value=0))
                stack.enter_context(patch.object(tm, 'note_account_success'))
                recover = stack.enter_context(patch.object(tm, 'recover_stale_event_claims'))
                stack.enter_context(patch.object(sys, 'argv', ['twitter_monitor.py', '--dry-run']))
                output = io.StringIO()
                with redirect_stdout(output):
                    self.assertEqual(tm.main(), 0)
                recover.assert_not_called()
                self.assertIn('translation_reply_enabled=True official_quote_groups=0 official_thread_merge_enabled=False', output.getvalue())
                if existing:
                    self.assertEqual(ledger.read_bytes(), before)
                    db=sqlite3.connect(ledger)
                    self.assertEqual(db.execute('SELECT state FROM deliveries').fetchone()[0], 'pending')
                    db.close()
                else:
                    self.assertFalse(ledger.exists())

if __name__ == '__main__':
    unittest.main()
