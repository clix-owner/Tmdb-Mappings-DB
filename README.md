# Resumable TMDb ↔ IMDb ↔ TVDb Mapping DB

## Output

The final public database is one file:

`Mappings-DB.json.gz`

It contains:

- Movies indexed by TMDb ID
- TV shows indexed by TMDb ID
- IMDb reverse index
- TVDb reverse index

## Resumable build

Work is stored internally in:

`.cache/shards/`

Progress is stored in:

`.cache/checkpoint.json`

Each completed chunk is written before the checkpoint advances. If a workflow ends before the full build finishes, its progress is committed. The next run resumes from that checkpoint.

A new `Mappings-DB.json.gz` is created only after the current Movie + TV traversal completes. The previous final gzip remains untouched during an incomplete run.

## GitHub setup

Create this Actions secret:

`TMDB_TOKEN`

Value: your TMDb API Read Access Token.

Then run:

Actions → Build Mapping DB → Run workflow

The workflow also starts every Monday.

For a very large first build, manually run the workflow again after each partial run until `.cache/checkpoint.json` shows `"complete":true`.
