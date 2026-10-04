# TMDB 404 recovery

The supplied run failed at movie 464459 because its external_ids endpoint returned HTTP 404. One unavailable export ID previously aborted the entire build.

The updater now records a 404 marker internally, advances past it, and continues Movie/TV processing. It does not invent IMDb/TVDB mappings. Markers are excluded from the final database and its indexes. On a new export date, unavailable IDs are retried. Successful requests in a failing concurrent batch are saved before the checkpoint is retained at the start of that batch. Authentication, permission and exhausted transient failures still fail visibly.

The final merge also restricts cached rows to the current export's IDs, so old shard entries do not leak into the public result. At completion, `.cache/not-found.json` lists skipped IDs; final metadata includes export counts and not_found_count. Existing consumer fields are preserved.

Replace the repository files with this project and run Actions → Build Mapping DB → Run workflow. Keep the existing `.cache` checkpoint and shards: they do not need to be deleted. A new export date starts a fresh scan while reusing available cached rows, as before.

The archive retains the supplied checkpoint/shards unchanged. Tests use mocked TMDB responses; a complete live API crawl was not performed and no final database is fabricated. Run the workflow until `.cache/checkpoint.json` reports complete:true.

Regression tests:

```
python -m unittest discover -s tests -v
```
