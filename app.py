"""YouTube downloader backend: Flask + yt-dlp.

Run locally:      python app.py
Run in production: gunicorn app:app --workers 1 --threads 8 --timeout 120
(Keep ONE worker: job state lives in memory. Use Redis if you ever need more.)
"""
import copy
import json
import logging
import os
import re
import shutil
import tempfile
import threading
import time
import urllib.parse
import urllib.request
import uuid
import zipfile
from collections import defaultdict, deque
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import TimeoutError as FuturesTimeout

import yt_dlp
from flask import Flask, jsonify, request, send_file, send_from_directory

# --------------------------------------------------------------------------- config
def env_int(name, default):
    try:
        return int(os.environ.get(name, default))
    except ValueError:
        return default


MAX_ACTIVE = env_int("MAX_ACTIVE", 1)              # downloads running at once
MAX_QUEUED = env_int("MAX_QUEUED", 2)              # extra jobs allowed to wait
MAX_PER_IP = env_int("MAX_PER_IP", 2)              # unfinished jobs per visitor
MAX_DURATION = env_int("MAX_DURATION_SECONDS", 3 * 3600)
MAX_FILESIZE = env_int("MAX_FILESIZE_MB", 300) * 1024 * 1024
MIN_FREE_BYTES = env_int("MIN_FREE_MB", 1500) * 1024 * 1024
CACHE_TTL = 600                                    # remember a looked-up video for 10 min
READY_TTL = 600                                    # keep a finished file for 10 min
FAILED_TTL = 120

DOWNLOAD_DIR = os.path.join(tempfile.gettempdir(), "ytdl_downloads")
os.makedirs(DOWNLOAD_DIR, exist_ok=True)

URL_RE = re.compile(r"^https?://(www\.|m\.|music\.)?(youtube\.com|youtu\.be)/", re.I)
AUDIO_RATES = {"128", "192", "256", "320"}
ACTIVE_STATES = ("queued", "running")

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("ytdl")

app = Flask(__name__, static_folder="static")
app.config["MAX_CONTENT_LENGTH"] = 16 * 1024  # API bodies are tiny
if os.environ.get("TRUST_PROXY") == "1":      # set when behind nginx / a platform proxy
    from werkzeug.middleware.proxy_fix import ProxyFix
    app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1, x_proto=1)

JOBS = {}
JOBS_LOCK = threading.Lock()
POOL = ThreadPoolExecutor(max_workers=MAX_ACTIVE, thread_name_prefix="dl")
INFO_POOL = ThreadPoolExecutor(max_workers=4, thread_name_prefix="info")
INFO_TIMEOUT = env_int("INFO_TIMEOUT", 45)

# --------------------------------------------------------------------------- proxy / cookies
PROXY = os.environ.get("YTDLP_PROXY")          # e.g. http://user:pass@host:port
COOKIE_SRC = os.environ.get("COOKIE_FILE")     # e.g. /etc/secrets/cookies.txt
COOKIE_PATH = None

if COOKIE_SRC and os.path.exists(COOKIE_SRC):
    # yt-dlp writes back to its cookie file, and Render secret files are read-only,
    # so work on a copy made once at startup.
    try:
        COOKIE_PATH = os.path.join(tempfile.gettempdir(), "cookies.txt")
        shutil.copy(COOKIE_SRC, COOKIE_PATH)
    except OSError:
        COOKIE_PATH = None


def common_opts():
    o = {"quiet": True, "no_warnings": True, "noplaylist": True,
         "socket_timeout": 15, "extractor_retries": 2}
    if PROXY:
        o["proxy"] = PROXY
    if COOKIE_PATH:
        o["cookiefile"] = COOKIE_PATH
    return o


class Cancelled(Exception):
    pass


# --------------------------------------------------------------------------- helpers
class RateLimiter:
    def __init__(self):
        self.hits = defaultdict(deque)
        self.lock = threading.Lock()

    def hit(self, key, limit, window=60):
        now = time.time()
        with self.lock:
            q = self.hits[key]
            while q and now - q[0] > window:
                q.popleft()
            if len(q) >= limit:
                return True
            q.append(now)
            return False

    def sweep(self, window=60):
        now = time.time()
        with self.lock:
            for k in [k for k, q in self.hits.items() if not q or now - q[-1] > window]:
                del self.hits[k]


LIMITER = RateLimiter()
CACHE, CACHE_LOCK = {}, threading.Lock()


def cache_get(url):
    with CACHE_LOCK:
        item = CACHE.get(url)
        if item and time.time() - item[0] < CACHE_TTL:
            return item[1]
    return None


def cache_put(url, meta):
    with CACHE_LOCK:
        if len(CACHE) >= 40:
            CACHE.pop(min(CACHE, key=lambda k: CACHE[k][0]), None)
        CACHE[url] = (time.time(), meta)


def fresh(meta):
    m = copy.deepcopy(meta)
    m.pop("requested_formats", None)
    m.pop("requested_downloads", None)
    return m


def body():
    data = request.get_json(silent=True)
    return data if isinstance(data, dict) else {}


ID_RE = re.compile(r"(?:[?&]v=|youtu\.be/|/shorts/|/embed/|/live/)([A-Za-z0-9_-]{11})")


def clean_url(raw):
    """Accept any YouTube video link; return the canonical single-video URL (drops playlist/mix params)."""
    url = str(raw or "").strip()
    if len(url) > 500 or not URL_RE.match(url):
        return None
    m = ID_RE.search(url)
    return "https://www.youtube.com/watch?v=" + m.group(1) if m else None


def client_ip():
    return request.remote_addr or "?"


def friendly_error(e):
    t = str(e).lower()
    rules = [
        (("private video",), "This video is private."),
        (("confirm your age", "age-restricted", "age restricted"), "Age-restricted videos can't be downloaded."),
        (("not a bot", "sign in to confirm"), "YouTube is asking this network to verify. Try again later."),
        (("not available in your country", "geo-restrict", "geo restrict", "blocked it in your country"),
         "This video isn't available in this region."),
        (("is live", "live event", "premiere"), "Live streams and premieres can't be downloaded until they end."),
        (("video unavailable", "has been removed", "no longer available", "does not exist"), "This video is unavailable."),
        (("ffmpeg", "ffprobe"), "ffmpeg is missing on the server, so this format can't be created."),
        (("file is larger than", "max-filesize"), "That file is too large for this server."),
        (("no space left",), "The server is out of storage. Try again later."),
    ]
    for keys, msg in rules:
        if any(k in t for k in keys):
            return msg
    return None


def finish_job(job, status, error=None, ttl=FAILED_TTL):
    job["status"], job["error"], job["expires"] = status, error, time.time() + ttl
    if status != "ready":
        shutil.rmtree(job["dir"], ignore_errors=True)


def oembed(url):
    q = urllib.parse.urlencode({"url": url, "format": "json"})
    req = urllib.request.Request("https://www.youtube.com/oembed?" + q, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req, timeout=5) as r:
        return json.load(r)


def build_opts(kind, height, abr, subtitles, template):
    base = {
        **common_opts(),
        "outtmpl": template,
        "windowsfilenames": True,
        "max_filesize": MAX_FILESIZE,
        "concurrent_fragment_downloads": 4,
        "retries": 3,
        "fragment_retries": 5,
        "extractor_retries": 3,   # overrides 2 from common_opts
        "socket_timeout": 40,     # overrides 15 from common_opts
    }

    if kind == "audio":
        base.update(
            format="bestaudio[ext=m4a]/bestaudio/best",
            postprocessors=[{"key": "FFmpegExtractAudio", "preferredcodec": "mp3", "preferredquality": abr}],
        )
        return base
    # H.264 + AAC in MP4 merges by plain copy (no re-encode) and plays everywhere.
    cap = f"[height<={height}]" if height else "[height<=1080]"
    base.update(
        format=f"bestvideo{cap}[vcodec^=avc1]+bestaudio[ext=m4a]/bestvideo{cap}+bestaudio/best{cap}",
        merge_output_format="mp4",
    )
    if subtitles:
        base.update(
            writesubtitles=True, writeautomaticsub=True, subtitleslangs=["en"], subtitlesformat="srt/best",
            postprocessors=[{"key": "FFmpegSubtitlesConvertor", "format": "srt"}],
        )
    return base


# --------------------------------------------------------------------------- worker
def run_job(jid):
    job = JOBS.get(jid)
    if not job:
        return
    if job["cancel"]:
        return finish_job(job, "cancelled")
    p, started, seen = job["params"], time.time(), {}
    job["status"] = "running"

    def check():
        if job["cancel"]:
            raise Cancelled()

    def hook(d):
        check()
        key = d.get("filename") or "?"
        if d["status"] == "downloading":
            got = d.get("downloaded_bytes") or 0
            tot = d.get("total_bytes") or d.get("total_bytes_estimate") or 0
            seen[key] = (got, tot)
            total = job["expected"] or sum(t for _, t in seen.values())
            done = sum(g for g, _ in seen.values())
            if total:
                job["pct"] = max(job["pct"], min(99.0, done / total * 100))  # never goes backwards
            job.update(stage="downloading", speed=d.get("speed"), eta=d.get("eta"))
        elif d["status"] == "finished":
            size = d.get("total_bytes") or d.get("downloaded_bytes") or seen.get(key, (0, 0))[0]
            seen[key] = (size, size)

    def pp_hook(d):
        check()
        if d["status"] == "started":
            job.update(stage="processing", speed=None, eta=None)

    try:
        template = os.path.join(job["dir"], "%(title).150B.%(ext)s")
        opts = build_opts(p["kind"], p["height"], p["abr"], p["subtitles"], template)
        opts["progress_hooks"], opts["postprocessor_hooks"] = [hook], [pp_hook]

        with yt_dlp.YoutubeDL(opts) as ydl:
            meta = cache_get(p["url"])
            if meta is None:
                meta = ydl.extract_info(p["url"], download=False)
                cache_put(p["url"], meta)
            check()
            if (meta.get("duration") or 0) > MAX_DURATION:
                raise ValueError("That video is too long (limit is %d hours)." % (MAX_DURATION // 3600))

            try:  # expected size -> accurate percentage
                sel = ydl.process_ie_result(fresh(meta), download=False)
                sizes = [f.get("filesize") or f.get("filesize_approx") or 0 for f in (sel.get("requested_formats") or [sel])]
                job["expected"] = sum(sizes) if all(sizes) else 0
            except Exception:
                job["expected"] = 0

            # yt-dlp silently skips files over max_filesize, so catch it here with a clear message.
            if job["expected"] and job["expected"] > MAX_FILESIZE:
                raise ValueError("That file is too large for this server. Pick a lower quality.")

            job["stage"] = "downloading"
            try:  # fast path: reuse info we already fetched
                ydl.process_ie_result(fresh(meta), download=True)
            except Cancelled:
                raise
            except Exception:
                if job["cancel"]:
                    raise Cancelled()
                log.warning("fast path failed for %s, retrying normally", jid, exc_info=True)
                for f in os.listdir(job["dir"]):
                    try:
                        os.remove(os.path.join(job["dir"], f))
                    except OSError:
                        pass
                job["pct"] = 0.0
                ydl.extract_info(p["url"], download=True)

        files = sorted(os.listdir(job["dir"]))
        if not files:
            raise ValueError("No file was produced. It may be too large for this server.")
        main = next((f for f in files if f.lower().endswith((".mp4", ".mp3"))), files[0])
        if len(files) > 1:  # video + subtitles
            zpath = os.path.join(job["dir"], os.path.splitext(main)[0] + ".zip")
            with zipfile.ZipFile(zpath, "w", zipfile.ZIP_STORED) as z:
                for f in files:
                    z.write(os.path.join(job["dir"], f), f)
            path = zpath
        else:
            path = os.path.join(job["dir"], main)
        job.update(path=path, name=os.path.basename(path), pct=100.0, stage="ready")
        finish_job(job, "ready", ttl=READY_TTL)
        log.info("job %s ready in %.1fs (%s)", jid, time.time() - started, job["name"])

    except Cancelled:
        finish_job(job, "cancelled")
    except Exception as e:
        if job["cancel"]:
            return finish_job(job, "cancelled")
        log.exception("job %s failed", jid)
        msg = str(e) if isinstance(e, ValueError) else (friendly_error(e) or "Download failed. Try again or pick a different quality.")
        finish_job(job, "error", msg)


# --------------------------------------------------------------------------- cleanup
def sweep():
    now = time.time()
    with JOBS_LOCK:
        for jid, j in list(JOBS.items()):
            if j["status"] not in ACTIVE_STATES and now > j.get("expires", 0):
                shutil.rmtree(j["dir"], ignore_errors=True)
                JOBS.pop(jid, None)
        known = set(JOBS)
    for name in os.listdir(DOWNLOAD_DIR):  # orphaned folders from crashes/restarts
        path = os.path.join(DOWNLOAD_DIR, name)
        if name not in known and now - os.path.getmtime(path) > 3600:
            shutil.rmtree(path, ignore_errors=True)
    with CACHE_LOCK:
        for k in [k for k, (t, _) in CACHE.items() if now - t > CACHE_TTL]:
            del CACHE[k]
    LIMITER.sweep()


def janitor():
    while True:
        time.sleep(60)
        try:
            sweep()
        except Exception:
            log.exception("sweep failed")


threading.Thread(target=janitor, daemon=True, name="janitor").start()
if not shutil.which("ffmpeg"):
    log.warning("ffmpeg not found: MP3 and video merging will fail. Install it and add it to PATH.")
if not shutil.which("deno"):
    log.warning("Deno not found. yt-dlp needs it to solve YouTube's JavaScript challenges.")
if PROXY:
    log.info("Using proxy for yt-dlp requests.")
if COOKIE_SRC and not COOKIE_PATH:
    log.warning("COOKIE_FILE is set but the file could not be read: %s", COOKIE_SRC)


# --------------------------------------------------------------------------- http
@app.after_request
def add_headers(resp):
    resp.headers["X-Content-Type-Options"] = "nosniff"
    resp.headers["X-Frame-Options"] = "DENY"
    resp.headers["Referrer-Policy"] = "same-origin"
    if request.path.startswith("/api/") and not request.path.startswith("/api/file/"):
        resp.headers["Cache-Control"] = "no-store"
    if request.path == "/":
        resp.headers["Content-Security-Policy"] = (
            "default-src 'self'; script-src 'self' 'unsafe-inline'; "
            "style-src 'self' 'unsafe-inline' https://fonts.googleapis.com; font-src https://fonts.gstatic.com; "
            "img-src 'self' data: https:; connect-src 'self'; base-uri 'none'; frame-ancestors 'none'"
        )
    return resp


@app.errorhandler(404)
def not_found(_):
    return (jsonify(error="Not found."), 404) if request.path.startswith("/api/") else ("Not found", 404)


@app.errorhandler(413)
def too_big(_):
    return jsonify(error="Request too large."), 413


@app.errorhandler(500)
def server_error(_):
    return jsonify(error="Something went wrong on the server."), 500


@app.route("/")
def index():
    return send_from_directory("static", "index.html")


@app.route("/health")
def health():
    with JOBS_LOCK:
        active = sum(1 for j in JOBS.values() if j["status"] in ACTIVE_STATES)
    return jsonify(ok=True,
                   yt_dlp_version=yt_dlp.version.__version__,
                   ffmpeg=bool(shutil.which("ffmpeg")),
                   js_runtime=bool(shutil.which("deno")),
                   proxy=bool(PROXY),
                   cookies=bool(COOKIE_PATH),
                   active=active)


@app.route("/api/preview", methods=["POST"])
def preview():
    """Instant title/thumbnail so the page never feels slow."""
    if LIMITER.hit(("look", client_ip()), 40):
        return jsonify(error="Too many requests. Slow down a little."), 429
    url = clean_url(body().get("url"))
    if not url:
        return jsonify(error="Enter a valid YouTube link."), 400
    meta = cache_get(url)
    if meta:
        return jsonify(title=meta.get("title"), thumbnail=meta.get("thumbnail"), channel=meta.get("uploader"))
    try:
        d = oembed(url)
        return jsonify(title=d.get("title"), thumbnail=d.get("thumbnail_url"), channel=d.get("author_name"))
    except Exception:
        return jsonify(error="No preview."), 404


def fetch_info(url):
    opts = {**common_opts(), "skip_download": True}
    with yt_dlp.YoutubeDL(opts) as ydl:
        return ydl.extract_info(url, download=False)


@app.route("/api/info", methods=["POST"])
def info():
    if LIMITER.hit(("look", client_ip()), 40):
        return jsonify(error="Too many requests. Slow down a little."), 429
    url = clean_url(body().get("url"))
    if not url:
        return jsonify(error="Enter a valid YouTube link."), 400
    data = cache_get(url)
    if data is None:
        fut = INFO_POOL.submit(fetch_info, url)
        try:
            data = fut.result(timeout=INFO_TIMEOUT)
            cache_put(url, data)
        except FuturesTimeout:
            log.warning("info timed out after %ss for %s", INFO_TIMEOUT, url)
            return jsonify(error="YouTube is responding too slowly. Please try again in a moment."), 504
        except Exception as e:
            log.warning("info failed for %s: %s", url, e)
            detail = (str(e).replace("ERROR: ", "").splitlines() or [""])[0][:140]
            return jsonify(error=friendly_error(e) or "Could not read that video (%s)." % detail), 400
    heights = sorted({f["height"] for f in data.get("formats", []) if f.get("height") and f.get("vcodec") != "none"}, reverse=True)
    return jsonify(title=data.get("title"), thumbnail=data.get("thumbnail"), channel=data.get("uploader"),
                   duration=data.get("duration"), views=data.get("view_count"), heights=heights)


@app.route("/api/start", methods=["POST"])
def start():
    ip, b = client_ip(), body()
    if LIMITER.hit(("start", ip), 10):
        return jsonify(error="Too many downloads. Wait a minute and try again."), 429
    url = clean_url(b.get("url"))
    kind, abr = b.get("kind", "video"), str(b.get("abr") or "192")
    if not url:
        return jsonify(error="Enter a valid YouTube link."), 400
    if kind not in ("video", "audio") or abr not in AUDIO_RATES:
        return jsonify(error="Invalid options."), 400
    try:
        height = int(b["height"]) if b.get("height") else None
        if height is not None and not 144 <= height <= 4320:
            raise ValueError
    except (TypeError, ValueError):
        return jsonify(error="Invalid quality."), 400
    if shutil.disk_usage(DOWNLOAD_DIR).free < MIN_FREE_BYTES:
        return jsonify(error="The server is low on storage. Try again later."), 503

    jid = uuid.uuid4().hex
    jdir = os.path.join(DOWNLOAD_DIR, jid)
    with JOBS_LOCK:
        active = [j for j in JOBS.values() if j["status"] in ACTIVE_STATES]
        if sum(1 for j in active if j["ip"] == ip) >= MAX_PER_IP:
            return jsonify(error="Finish your current downloads first."), 429
        if len(active) >= MAX_ACTIVE + MAX_QUEUED:
            return jsonify(error="The server is busy. Try again in a minute."), 503
        os.makedirs(jdir)
        JOBS[jid] = {
            "status": "queued", "stage": "starting", "pct": 0.0, "speed": None, "eta": None,
            "expected": 0, "cancel": False, "created": time.time(), "expires": 0, "dir": jdir, "ip": ip,
            "path": None, "name": None, "error": None,
            "params": {"url": url, "kind": kind, "height": height, "abr": abr,
                       "subtitles": bool(b.get("subtitles")) and kind == "video"},
        }
    POOL.submit(run_job, jid)
    return jsonify(id=jid)


@app.route("/api/status/<jid>")
def status(jid):
    j = JOBS.get(jid)
    if not j:
        return jsonify(status="error", error="This download expired. Start it again."), 404
    return jsonify({k: j[k] for k in ("status", "stage", "pct", "speed", "eta", "name", "error")})


@app.route("/api/cancel/<jid>", methods=["POST"])
def cancel(jid):
    j = JOBS.get(jid)
    if j and j["status"] in ACTIVE_STATES:
        j["cancel"] = True
    return jsonify(ok=True)


@app.route("/api/file/<jid>")
def get_file(jid):
    j = JOBS.get(jid)
    if not j or j["status"] != "ready" or not os.path.exists(j["path"] or ""):
        return jsonify(error="File not ready or expired."), 404
    j["expires"] = time.time() + 120  # keep briefly so an interrupted save can retry
    return send_file(j["path"], as_attachment=True, download_name=j["name"], max_age=0)


if __name__ == "__main__":
    app.run(host=os.environ.get("HOST", "127.0.0.1"), port=env_int("PORT", 5000),
            debug=os.environ.get("FLASK_DEBUG") == "1", threaded=True)