import argparse
import copy
import json
import tempfile
import unittest
from unittest.mock import Mock, patch
from datetime import datetime, timezone
import twitter_monitor as tm
import twitter_graphql as tg
from test_semantic_bundle import raw

class InformationQualityTest(unittest.TestCase):
    def tweet(self, text='I love working here so much', quote=None, media=None):
        b=tg.build_semantic_bundle(raw('2095646716527579422','curator',text,quote=quote,media=media),'curator')
        return {'id':'2095646716527579422','text':text,'semantic_bundle':b,'created_at':datetime.now(timezone.utc).strftime('%a %b %d %H:%M:%S +0000 %Y')}
    def ai(self, **values):
        ai=Mock();r={'low_information':True,'evidence_present':False,'confidence':.99,'reason':'纯感叹，无具体信息'};r.update(values)
        ai.complete.return_value=(json.dumps(r),'test');ai.complete_with_images.return_value=(json.dumps(r),'test');ai.is_available.return_value=True
        return ai
    def test_filter_and_cache_preserve_source(self):
        t=self.tweet();source=copy.deepcopy(t['semantic_bundle']);ai=self.ai()
        self.assertTrue(tm._review_information_quality(t,ai)[0]);tm._review_information_quality(t,ai)
        self.assertEqual(ai.complete.call_count,1);self.assertEqual(source,t['semantic_bundle'])
    def test_full_quote_has_value(self):
        t=self.tweet('太棒了',quote=raw('2095651088502591861','original','All paid users receive one daily quota reset.'))
        ai=self.ai(low_information=False);self.assertFalse(tm._review_information_quality(t,ai)[0])
        self.assertIn('daily quota reset',ai.complete.call_args.args[0])
    def test_full_note_used(self):
        t=self.tweet();t['semantic_bundle']['anchor']['note']={'text':'教程详细内容'*200+'具体结论最后一句'}
        ai=self.ai(low_information=False);tm._review_information_quality(t,ai)
        self.assertIn('具体结论最后一句',ai.complete.call_args.args[0])
    def test_invalid_results_keep(self):
        for value in [dict(low_information='true'),dict(confidence=True),dict(confidence=.94),dict(confidence=2)]:
            self.assertFalse(tm._review_information_quality(self.tweet(),self.ai(**value))[0])
        for text in ['bad','null','[]']:
            ai=self.ai();ai.complete.return_value=(text,'test');self.assertFalse(tm._review_information_quality(self.tweet(),ai)[0])
    def test_missing_ai_context_and_external_resource_keep(self):
        self.assertFalse(tm._review_information_quality(self.tweet(),None)[0])
        t=self.tweet();t['semantic_bundle']['resolution']['status']='degraded_optional';ai=self.ai();self.assertFalse(tm._review_information_quality(t,ai)[0]);ai.complete.assert_not_called()
        self.assertFalse(tm._review_information_quality(self.tweet('完整教程 https://example.org/tutorial'),self.ai())[0])
    def test_visual_evidence_can_veto_filter(self):
        photo={'type':'photo','media_url_https':'https://pbs.twimg.com/media/chart.jpg'}
        for is_low in [False,True]:
            t=self.tweet('Look at this:',media=[photo]);ai=self.ai();ai.complete_with_images.return_value=(json.dumps({'low_information':is_low,'evidence_present':not is_low,'confidence':.99,'reason':'visual review'}),'test')
            with patch.object(tm,'fetch_article_images',return_value=[{}]):self.assertEqual(tm._review_information_quality(t,ai)[0],is_low)
        t=self.tweet(media=[photo])
        with patch.object(tm,'fetch_article_images',return_value=[]):self.assertFalse(tm._review_information_quality(t,self.ai())[0])
    def test_benchmark_chart_never_filtered_by_model_familiarity(self):
        photo={'type':'photo','media_url_https':'https://pbs.twimg.com/media/chart.jpg'}
        ai=self.ai();t=self.tweet('GPT-6 benchmarks:',media=[photo])
        self.assertFalse(tm._review_information_quality(t,ai)[0]);ai.complete.assert_not_called()

    def test_substantive_evidence_vetoes_low_label(self):
        self.assertFalse(tm._review_information_quality(self.tweet(),self.ai(evidence_present=True))[0])
        self.assertFalse(tm._review_information_quality(self.tweet(),self.ai(evidence_present='false'))[0])

    def test_exception_keeps(self):
        ai=self.ai();ai.complete.side_effect=TimeoutError();self.assertFalse(tm._review_information_quality(self.tweet(),ai)[0])
    def test_filter_never_sends_and_journal_precedes_seen(self):
        self.run_process(False)
    def test_journal_failure_stays_unseen(self):
        self.run_process(True)
    def run_process(self, fail_journal):
        t=self.tweet();events=[];seen=[]
        def journal(*a,**kw):
            events.append('journal')
            if fail_journal:raise OSError('disk full')
        def save(u,ids,*a):events.append('seen');seen.append(set(ids))
        args=argparse.Namespace(test=False,seed=False,dry_run=False,limit=20,max_push_age_minutes=45,test_count=1)
        with tempfile.TemporaryDirectory() as directory, patch.object(tm,'SEEN_DIR',directory), patch.object(tm,'_SEMANTIC_BUNDLE_ENABLED',True), patch.object(tm,'_SEMANTIC_CURATOR_ALLOWLIST',set()), patch.object(tm,'_CROSS_DEDUP_ENABLED',False), patch.object(tm,'_ACCOUNT_CONFIG_BY_USERNAME',{}), patch.object(tm,'fetch_tweets',return_value=[t]), patch.object(tm,'load_seen',return_value=({'warm'},None)), patch.object(tm,'load_push_retry',return_value=set()), patch.object(tm,'save_seen',side_effect=save), patch.object(tm,'save_push_retry'), patch.object(tm,'_journal_semantic_decision',side_effect=journal), patch.object(tm,'send_tweet') as send:
            tm.process_user(None,self.ai(),'curator','MOCK','MOCK',args)
        send.assert_not_called();self.assertLess(events.index('journal'),events.index('seen'));self.assertEqual(t['id'] in seen[-1],not fail_journal)

if __name__=='__main__':unittest.main()
