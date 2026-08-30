import concurrent.futures as cf
import datetime as dt
import gzip
import json
import os
import time
import urllib.error
import urllib.request
from pathlib import Path

TOKEN = os.environ["TMDB_TOKEN"]
API = "https://api.themoviedb.org/3"
EXPORT_BASE = "https://files.tmdb.org/p/exports"

CACHE = Path(".cache")
SHARDS = CACHE / "shards"
STATE = CACHE / "checkpoint.json"
FINAL = Path("Mappings-DB.json.gz")

SHARD_SIZE = 5000
WORKERS = 6
# Leave enough time for the workflow to commit progress before GitHub kills it.
MAX_RUN_SECONDS = int(os.getenv("MAX_RUN_SECONDS", "15000"))

CACHE.mkdir(exist_ok=True)
SHARDS.mkdir(exist_ok=True)

HEADERS = {
    "Authorization": f"Bearer {TOKEN}",
    "Accept": "application/json",
    "User-Agent": "tmdb-imdb-tvdb-resumable-db/2.0",
}

def request_bytes(url, retries=6):
    last = None
    for n in range(retries):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": HEADERS["User-Agent"]})
            with urllib.request.urlopen(req, timeout=120) as r:
                return r.read()
        except Exception as e:
            last = e
            time.sleep(min(2 ** n, 30))
    raise last

def request_json(url, retries=7):
    last = None
    for n in range(retries):
        try:
            req = urllib.request.Request(url, headers=HEADERS)
            with urllib.request.urlopen(req, timeout=60) as r:
                return json.load(r)
        except urllib.error.HTTPError as e:
            last = e
            if e.code == 429:
                time.sleep(max(int(e.headers.get("Retry-After", "2")), 1))
            elif 500 <= e.code < 600:
                time.sleep(min(2 ** n, 30))
            else:
                raise
        except Exception as e:
            last = e
            time.sleep(min(2 ** n, 30))
    raise last

def load_json(path, default):
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return default

def atomic_json(path, obj):
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(obj, ensure_ascii=False, separators=(",", ":")), encoding="utf-8")
    tmp.replace(path)

def export_day():
    return dt.datetime.now(dt.timezone.utc).date() - dt.timedelta(days=1)

def export_ids(kind, day):
    stamp = day.strftime("%m_%d_%Y")
    url = f"{EXPORT_BASE}/{kind}_ids_{stamp}.json.gz"
    print("Downloading export:", url, flush=True)
    raw = gzip.decompress(request_bytes(url)).decode("utf-8")
    out = []
    for line in raw.splitlines():
        if not line:
            continue
        obj = json.loads(line)
        if isinstance(obj.get("id"), int):
            out.append(obj["id"])
    out.sort()
    return out

def shard_path(media, shard_no):
    return SHARDS / f"{media}-{shard_no:06d}.json"

def load_shard(media, shard_no):
    return load_json(shard_path(media, shard_no), {})

def save_shard(media, shard_no, data):
    atomic_json(shard_path(media, shard_no), data)

def fetch_external(media, tmdb_id):
    endpoint = "movie" if media == "movie" else "tv"
    data = request_json(f"{API}/{endpoint}/{tmdb_id}/external_ids")
    row = {"tmdb": tmdb_id}
    if data.get("imdb_id"):
        row["imdb"] = data["imdb_id"]
    if media == "tv" and data.get("tvdb_id") is not None:
        row["tvdb"] = data["tvdb_id"]
    return str(tmdb_id), row

def process_media(media, ids, state, started):
    position = int(state["positions"].get(media, 0))
    total = len(ids)
    print(f"{media}: resume {position}/{total}", flush=True)

    while position < total:
        if time.monotonic() - started > MAX_RUN_SECONDS:
            print("Time budget reached; saving checkpoint.", flush=True)
            return False

        end = min(position + SHARD_SIZE, total)
        chunk = ids[position:end]
        shard_no = position // SHARD_SIZE
        data = load_shard(media, shard_no)

        missing = [x for x in chunk if str(x) not in data]
        print(f"{media} shard {shard_no}: {position}-{end}, missing {len(missing)}", flush=True)

        if missing:
            with cf.ThreadPoolExecutor(max_workers=WORKERS) as ex:
                futures = {ex.submit(fetch_external, media, tid): tid for tid in missing}
                for fut in cf.as_completed(futures):
                    tid = futures[fut]
                    try:
                        key, row = fut.result()
                        # Keep all TMDb IDs, even if external IDs are absent.
                        data[key] = row
                    except Exception as e:
                        print(f"Failed {media}/{tid}: {e}", flush=True)
                        # Do not advance checkpoint past failures.
                        save_shard(media, shard_no, data)
                        state["positions"][media] = position
                        atomic_json(STATE, state)
                        raise

        data = dict(sorted(data.items(), key=lambda kv: int(kv[0])))
        save_shard(media, shard_no, data)

        position = end
        state["positions"][media] = position
        state["updated_at"] = dt.datetime.now(dt.timezone.utc).isoformat()
        atomic_json(STATE, state)
        print(f"{media}: checkpoint {position}/{total}", flush=True)

    return True

def merge_final(day, movie_total, tv_total):
    movies = {}
    tv = {}

    for p in sorted(SHARDS.glob("movie-*.json")):
        movies.update(load_json(p, {}))
    for p in sorted(SHARDS.glob("tv-*.json")):
        tv.update(load_json(p, {}))

    movies = dict(sorted(movies.items(), key=lambda kv: int(kv[0])))
    tv = dict(sorted(tv.items(), key=lambda kv: int(kv[0])))

    imdb_index = {}
    tvdb_index = {}

    for row in movies.values():
        if row.get("imdb"):
            imdb_index[row["imdb"]] = {"type": "movie", "tmdb": row["tmdb"]}

    for row in tv.values():
        if row.get("imdb"):
            x = {"type": "tv", "tmdb": row["tmdb"]}
            if row.get("tvdb") is not None:
                x["tvdb"] = row["tvdb"]
            imdb_index[row["imdb"]] = x
        if row.get("tvdb") is not None:
            x = {"tmdb": row["tmdb"]}
            if row.get("imdb"):
                x["imdb"] = row["imdb"]
            tvdb_index[str(row["tvdb"])] = x

    payload = {
        "meta": {
            "updated_at": dt.datetime.now(dt.timezone.utc).isoformat().replace("+00:00", "Z"),
            "tmdb_export_date": day.isoformat(),
            "movie_count": len(movies),
            "tv_count": len(tv),
            "total": len(movies) + len(tv),
        },
        "movies": movies,
        "tv": tv,
        "indexes": {
            "imdb": dict(sorted(imdb_index.items())),
            "tvdb": dict(sorted(tvdb_index.items(), key=lambda kv: int(kv[0]))),
        },
    }

    tmp = FINAL.with_suffix(".json.gz.tmp")
    raw = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    with gzip.open(tmp, "wb", compresslevel=9) as f:
        f.write(raw)
    tmp.replace(FINAL)
    print(f"FINAL created: {FINAL} ({FINAL.stat().st_size:,} bytes)", flush=True)

def main():
    started = time.monotonic()
    day = export_day()
    movie_ids = export_ids("movie", day)
    tv_ids = export_ids("tv_series", day)

    state = load_json(STATE, {})
    # A new TMDb export starts a fresh traversal, while existing shard records
    # remain reusable and are skipped when already present.
    if state.get("export_date") != day.isoformat():
        state = {
            "export_date": day.isoformat(),
            "positions": {"movie": 0, "tv": 0},
            "updated_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        }
        atomic_json(STATE, state)

    movie_done = process_media("movie", movie_ids, state, started)
    if not movie_done:
        return

    tv_done = process_media("tv", tv_ids, state, started)
    if not tv_done:
        return

    # Only publish a new final gzip after both traversals finish.
    merge_final(day, len(movie_ids), len(tv_ids))
    state["complete"] = True
    state["completed_at"] = dt.datetime.now(dt.timezone.utc).isoformat()
    atomic_json(STATE, state)

if __name__ == "__main__":
    main()
