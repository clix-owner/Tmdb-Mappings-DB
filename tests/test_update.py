import datetime as dt
import gzip
import importlib.util
import json
import os
from pathlib import Path
import unittest
import urllib.error
import uuid
from unittest.mock import patch

os.environ.setdefault('TMDB_TOKEN', 'test-token')
spec = importlib.util.spec_from_file_location('updater', Path(__file__).resolve().parents[1] / 'scripts/update.py')
u = importlib.util.module_from_spec(spec)
spec.loader.exec_module(u)


class UpdateTests(unittest.TestCase):
    def setUp(self):
        self.root = Path.cwd() / ('test-cache-' + uuid.uuid4().hex)
        self.root.mkdir()
        shards = self.root / 'shards'
        shards.mkdir()
        self.patches = [patch.object(u, 'CACHE', self.root), patch.object(u, 'SHARDS', shards),
                        patch.object(u, 'STATE', self.root / 'checkpoint.json'),
                        patch.object(u, 'FINAL', self.root / 'final.gz'), patch.object(u, 'SHARD_SIZE', 3)]
        for p in self.patches:
            p.start()
        self.state = {'export_date': '2026-10-03', 'positions': {'movie': 0, 'tv': 0}}

    def tearDown(self):
        for p in reversed(self.patches):
            p.stop()
        for f in self.root.rglob('*'):
            if f.is_file():
                f.unlink()
        (self.root / 'shards').rmdir()
        self.root.rmdir()

    def test_404_is_allowed_only_when_explicit(self):
        error = urllib.error.HTTPError('https://example.org', 404, 'Not Found', {}, None)
        with patch.object(u.urllib.request, 'urlopen', side_effect=error):
            self.assertIsNone(u.request_json('https://example.org', allow_not_found=True))
            with self.assertRaises(urllib.error.HTTPError):
                u.request_json('https://example.org')

    def test_authentication_errors_still_fail(self):
        error = urllib.error.HTTPError('https://example.org', 401, 'Unauthorized', {}, None)
        with patch.object(u.urllib.request, 'urlopen', side_effect=error):
            with self.assertRaises(urllib.error.HTTPError):
                u.request_json('https://example.org', allow_not_found=True)

    def test_404_advances_checkpoint_and_resume_skips_it(self):
        fetch = lambda media, tid: (str(tid), None if tid == 2 else {'tmdb': tid})
        with patch.object(u, 'fetch_external', side_effect=fetch) as mock:
            self.assertTrue(u.process_media('movie', [1, 2, 3], self.state, u.time.monotonic()))
            self.assertEqual(self.state['positions']['movie'], 3)
            self.state['positions']['movie'] = 0
            u.process_media('movie', [1, 2, 3], self.state, u.time.monotonic())
            self.assertEqual(mock.call_count, 3)
        self.assertTrue(u.load_shard('movie', 0)['2']['_not_found'])

    def test_new_export_retries_previous_404(self):
        u.save_shard('movie', 0, {'2': {'tmdb': 2, '_not_found': True, '_export_date': '2026-10-02'}})
        with patch.object(u, 'fetch_external', return_value=('2', {'tmdb': 2, 'imdb': 'tt2'})) as mock:
            u.process_media('movie', [2], self.state, u.time.monotonic())
            mock.assert_called_once()
        self.assertNotIn('_not_found', u.load_shard('movie', 0)['2'])

    def test_failure_keeps_all_successes_without_advancing(self):
        def fetch(media, tid):
            if tid == 2:
                raise RuntimeError('server outage')
            return str(tid), {'tmdb': tid}
        with patch.object(u, 'fetch_external', side_effect=fetch):
            with self.assertRaises(RuntimeError):
                u.process_media('movie', [1, 2, 3], self.state, u.time.monotonic())
        self.assertEqual(self.state['positions']['movie'], 0)
        self.assertEqual(set(u.load_shard('movie', 0)), {'1', '3'})

    def test_final_excludes_404_and_old_export_ids(self):
        u.save_shard('movie', 0, {'1': {'tmdb': 1, 'imdb': 'tt1'},
                                 '2': {'tmdb': 2, '_not_found': True}, '99': {'tmdb': 99}})
        u.save_shard('tv', 0, {'3': {'tmdb': 3, 'tvdb': 30}})
        u.merge_final(dt.date(2026, 10, 3), 2, 1, [1, 2], [3])
        with gzip.open(u.FINAL, 'rt') as f:
            data = json.load(f)
        self.assertEqual(set(data['movies']), {'1'})
        self.assertEqual(data['meta']['not_found_count'], 1)
        self.assertEqual(data['meta']['total'], 2)
        self.assertEqual(data['indexes']['tvdb']['30']['tmdb'], 3)


if __name__ == '__main__':
    unittest.main()
