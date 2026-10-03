import copy
import json
import unittest
from unittest.mock import Mock, patch
import twitter_monitor as tm
from test_semantic_bundle import raw
import twitter_graphql as tg


class QuoteTranslationTest(unittest.TestCase):
    def old_tweets_fixture(self, *, retweet=True, quote=True):
        source = raw('1847662906961514935', 'dontbesilent', 'sell me this pen',
                     media=[self.photo('original')])
        archive = raw('2095921106536317397', 'dbs_old_tweets', 'sell me this pen',
                      quote=source if quote else None, media=[self.photo('duplicate')])
        outer = raw('2095921106536317398', 'dontbesilent', 'RT @dbs_old_tweets',
                    retweet=archive) if retweet else archive
        return {'id': outer['legacy']['id_str'],
                'semantic_bundle': tg.build_semantic_bundle(outer, 'dontbesilent')}

    def test_old_tweets_quote_uses_source_without_ai_and_keeps_identity(self):
        for retweet in (True, False):
            with self.subTest(retweet=retweet):
                t = self.old_tweets_fixture(retweet=retweet)
                before = copy.deepcopy(t)
                ai = self.ai()
                tm._prepare_quote_translation(t, ai)
                ai.complete.assert_not_called()
                self.assertEqual(t['id'], before['id'])
                self.assertEqual(t['semantic_bundle'], before['semantic_bundle'])
                self.assertEqual(t['_quote_translation']['action'], 'source_only')
                plain, rich, link = tm.format_semantic_message('dontbesilent', t)
                self.assertEqual(link, 'https://x.com/dontbesilent/status/1847662906961514935')
                self.assertEqual(rich.count('sell me this pen'), 1)
                self.assertNotIn('↳ 引用', rich)
                self.assertNotIn('/duplicate.jpg', rich)
                self.assertIn('/original.jpg', rich)
                view = tm._semantic_media_view(tm._quote_presentation_tweet(t))
                self.assertEqual([m['url'] for m in view['media']],
                                 ['https://pbs.twimg.com/media/original.jpg'])

    def test_old_tweets_missing_or_incomplete_quote_preserves_content(self):
        t = self.old_tweets_fixture(quote=False)
        tm._prepare_quote_translation(t, None)
        self.assertNotIn('_quote_presentation_bundle', t)
        for status in ('degraded_optional', 'auth_degraded', 'truncated_budget'):
            t = self.old_tweets_fixture()
            t['semantic_bundle']['resolution']['status'] = status
            tm._prepare_quote_translation(t, None)
            self.assertNotIn('_quote_presentation_bundle', t)

    def test_old_tweets_send_fallback_uses_only_original_photo_and_text(self):
        t = self.old_tweets_fixture()
        with patch.object(tm, '_SEMANTIC_BUNDLE_ENABLED', True), \
                patch.object(tm, '_SEMANTIC_CURATOR_ALLOWLIST', set()), \
                patch.object(tm, 'send_telegram_rich', return_value={
                    'ok': False, 'rich_fallback': True}) as rich, \
                patch.object(tm, 'send_telegram_photo', return_value={
                    'ok': False, 'photo_fallback': True}) as photo, \
                patch.object(tm, 'send_telegram', return_value={'ok': True}) as plain:
            tm.send_tweet('fake', 'fake', 'dontbesilent', t, None, thread_id=19)
        self.assertNotIn('/duplicate.jpg', rich.call_args.kwargs['html'])
        self.assertEqual(photo.call_args.args[2], 'https://pbs.twimg.com/media/original.jpg')
        self.assertEqual(plain.call_args.args[2].count('sell me this pen'), 1)
        self.assertNotIn('↳ 引用', plain.call_args.args[2])
        self.assertIn('/1847662906961514935', plain.call_args.args[3])
        self.assertEqual(plain.call_args.kwargs['thread_id'], 19)

    def test_other_chinese_republication_is_not_folded(self):
        t = self.old_tweets_fixture()
        t['semantic_bundle']['anchor']['author'] = 'another_account'
        t['semantic_bundle']['context_nodes'][0]['text'] = '中文原文'
        tm._prepare_quote_translation(t, None)
        self.assertNotIn('_quote_presentation_bundle', t)

    def fixture(self, text='普通开发者 vs Vibe 开发者', own_media=None, source_media=None):
        source = raw('2095813557036458451', 'DataChaz', 'normal coder vs vibe-coder 😭', media=source_media)
        anchor = raw('2095921106536317397', 'dotey', text, quote=source, media=own_media)
        return {'id': '2095921106536317397', 'semantic_bundle': tg.build_semantic_bundle(anchor, 'dotey')}

    def ai(self, **values):
        result = dict(translation_only=True, source_id='2095813557036458451', confidence=1, reason='仅翻译')
        result.update(values)
        ai = Mock()
        ai.is_available.return_value = True
        ai.complete.return_value = json.dumps(result), 'test'
        ai.complete_with_images.return_value = '{"redundant":true}', 'test'
        return ai

    def test_source_only_keeps_identity_and_original_evidence(self):
        t = self.fixture(); before = copy.deepcopy(t); ai = self.ai()
        with patch.object(tm, '_SEMANTIC_BUNDLE_ENABLED', True), patch.object(tm, '_SEMANTIC_CURATOR_ALLOWLIST', set()):
            plain, rich, link = tm.format_message('dotey', t, ai)
            tm.format_message('dotey', t, ai, embed_video=False)
        self.assertEqual(t['semantic_bundle'], before['semantic_bundle'])
        self.assertEqual(ai.complete.call_count, 1)
        self.assertNotIn('普通开发者', plain + rich)
        self.assertIn('normal coder', plain)
        self.assertIn('📢 @DataChaz', plain)
        self.assertIn('经 @dotey 转发发现', plain)
        self.assertEqual(link, 'https://x.com/DataChaz/status/2095813557036458451')

    def test_uncertain_invalid_and_failure_keep_original(self):
        for values in [dict(translation_only=False), dict(translation_only='true'), dict(confidence=.94),
                       dict(confidence=True), dict(source_id='wrong'), dict(confidence=float('nan'))]:
            with self.subTest(values=values):
                t=self.fixture();tm._prepare_quote_translation(t,self.ai(**values))
                self.assertNotIn('_quote_presentation_bundle', t)
        for answer in ['bad', '[]', 'null']:
            t=self.fixture();ai=self.ai();ai.complete.return_value=(answer,'test')
            tm._prepare_quote_translation(t,ai)
            self.assertNotIn('_quote_presentation_bundle',t)
        t=self.fixture();ai=self.ai();ai.complete.side_effect=TimeoutError()
        tm._prepare_quote_translation(t,ai)
        self.assertEqual(t['_quote_translation']['action'],'keep')

    def test_fenced_json_supported_but_trailing_prose_rejected(self):
        self.assertEqual(tm._parse_quote_json('```json\n{"translation_only": true}\n```'), {'translation_only': True})
        with self.assertRaises(ValueError): tm._parse_quote_json('{"translation_only": true} extra')

    def test_no_ai_or_incomplete_context_never_removes(self):
        for status in ['degraded_optional','auth_degraded','truncated_budget']:
            t=self.fixture();t['semantic_bundle']['resolution']['status']=status;ai=self.ai()
            tm._prepare_quote_translation(t,ai);ai.complete.assert_not_called()
        t=self.fixture();tm._prepare_quote_translation(t,None)
        self.assertEqual(t['_quote_translation']['action'],'keep')

    def test_full_note_and_added_opinion_are_supplied(self):
        t=self.fixture();t['semantic_bundle']['anchor']['note']={'text':'普通开发者 vs Vibe 开发者。'+'详细内容'*200+'但我认为这个比较不合理'}
        ai=self.ai(translation_only=False);tm._prepare_quote_translation(t,ai)
        self.assertIn('但我认为这个比较不合理',ai.complete.call_args.args[0])
        self.assertNotIn('_quote_presentation_bundle',t)

    @staticmethod
    def photo(name):
        return {'type':'photo','media_url_https':'https://pbs.twimg.com/media/'+name+'.jpg'}

    def test_distinct_photos_need_visual_confirmation(self):
        for answer,expected in [('{"redundant":false}','keep'),('{"redundant":true}','source_only'),('null','keep')]:
            t=self.fixture(own_media=[self.photo('own')],source_media=[self.photo('source')]);ai=self.ai()
            ai.complete_with_images.return_value=(answer,'test')
            with patch.object(tm,'fetch_article_images',return_value=[{},{}]):
                tm._prepare_quote_translation(t,ai)
            self.assertEqual(t['_quote_translation']['action'],expected)
        t=self.fixture(own_media=[self.photo('own')],source_media=[self.photo('source')])
        with patch.object(tm,'fetch_article_images',return_value=[]):tm._prepare_quote_translation(t,self.ai())
        self.assertEqual(t['_quote_translation']['action'],'keep')

    def test_native_photo_fallback_uses_original_media_and_link(self):
        t=self.fixture(own_media=[self.photo('own')],source_media=[self.photo('source')]);ai=self.ai()
        with patch.object(tm,'fetch_article_images',return_value=[{},{}]):tm._prepare_quote_translation(t,ai)
        view=tm._semantic_media_view(tm._quote_presentation_tweet(t))
        self.assertEqual([m['url'] for m in view['media']],['https://pbs.twimg.com/media/source.jpg'])
        with patch.object(tm,'_SEMANTIC_BUNDLE_ENABLED',True), patch.object(tm,'_SEMANTIC_CURATOR_ALLOWLIST',set()), patch.object(tm,'send_telegram_rich',return_value={'ok':True}) as send:
            tm.send_tweet('fake','fake','dotey',t,ai)
        self.assertNotIn('/own.jpg',send.call_args.kwargs['html'])
        self.assertIn('/source.jpg',send.call_args.kwargs['html'])
        self.assertIn('DataChaz/status/',send.call_args.kwargs['link'])

    def test_photo_then_html_fallback_remain_source_only(self):
        t=self.fixture(own_media=[self.photo('own')],source_media=[self.photo('source')]);ai=self.ai()
        with patch.object(tm,'fetch_article_images',return_value=[{},{}]):tm._prepare_quote_translation(t,ai)
        with patch.object(tm,'_SEMANTIC_BUNDLE_ENABLED',True), patch.object(tm,'_SEMANTIC_CURATOR_ALLOWLIST',set()), patch.object(tm,'send_telegram_rich',return_value={'ok':False,'rich_fallback':True}), patch.object(tm,'send_telegram_photo',return_value={'ok':False,'photo_fallback':True}) as photo, patch.object(tm,'send_telegram',return_value={'ok':True}) as plain:
            tm.send_tweet('fake','fake','dotey',t,ai,thread_id=19)
        self.assertEqual(photo.call_args.args[2],'https://pbs.twimg.com/media/source.jpg')
        self.assertNotIn('普通开发者',photo.call_args.args[3])
        self.assertIn('DataChaz/status/',photo.call_args.args[4])
        self.assertNotIn('普通开发者',plain.call_args.args[2])
        self.assertIn('DataChaz/status/',plain.call_args.args[3])
        self.assertEqual(plain.call_args.kwargs['thread_id'],19)

    def test_nested_source_context_survives(self):
        t=self.fixture();third=dict(t['semantic_bundle']['context_nodes'][0],tweet_id='1234567890123456789',text='Earlier original context')
        t['semantic_bundle']['context_nodes'].append(third)
        tm._prepare_quote_translation(t,self.ai())
        self.assertIn('Earlier original context',tm.format_semantic_message('dotey',t)[0])


if __name__ == '__main__':
    unittest.main()
