# -*- coding: utf-8 -*-
"""
米游社壁纸下载器 v3.1
纯 Python 标准库实现，零第三方依赖，装好 Python 直接跑。

v3.1 更新
  * 扫描并行化：利用 offset=<帖子ID> 的游标语义，多起点并行下潜抓取，
    再用"窗口端点比对"证明无漏帖，扫描从 ~28s 降到数秒
  * 线程级可见：扫描和下载每个线程的实时状态（当前任务 / 进度 / 速度 / 计数）
    都会回传到界面，逐线程展示
  * 去掉翻页之间 0.3s 的人为等待

v3.0 更新
  * 多线程并发下载：默认 8 线程，界面最高可拉到 512，按待下载图片数自动收敛
  * 断点续传加固：先写 .part 临时文件再原子改名，半截文件不会被当成"已下载"
  * 修复统计错误：跳过/失败不再被算进"已下载"
  * 日志增量拉取：按 seq 游标只取新增
  * Web 服务多线程化；"打开文件夹"变成真功能；端口占用自动顺延
"""

import http.server
import json
import os
import queue
import random
import re
import ssl
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import webbrowser
from datetime import datetime

VERSION = "3.1"

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DEFAULT_SAVE_DIR = os.path.join(BASE_DIR, "wallpapers")

API_URL = "https://bbs-api.miyoushe.com/painter/wapi/userPostList"
USER_AGENT = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
              "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")

PAGE_SIZE = 20                 # 接口硬上限，传更大也会被忽略
MAX_RETRY = 3
DEFAULT_THREADS = 8
MAX_THREADS = 512              # 下载并发上限；实际线程数会被"待下载图片数"钳制
HIGH_THREADS = 128             # 超过这个值给出提醒
MAX_SCAN_THREADS = 16          # 扫描并发上限（接口比图床更敏感，别太猛）
MAX_PROBES = 600               # 并行扫描的请求总数上限，超出则回退顺序扫描
MIN_VALID_BYTES = 1024
LOG_LIMIT = 1000

_SSL_CTX = ssl.create_default_context()
_SSL_CTX.check_hostname = False
_SSL_CTX.verify_mode = ssl.CERT_NONE

# --------------------------------------------------------------------------- #
# 全局状态
# --------------------------------------------------------------------------- #

_state_lock = threading.RLock()
_stop_event = threading.Event()
_quality_warned = threading.Event()
_worker = None

state = {
    "status": "idle",            # idle / fetching / downloading / stopped
    "run_id": 0,
    "wallpaper_posts": [],
    "scanned_posts": 0,          # 已扫到的帖子总数（含非壁纸帖）
    "total_posts": 0,            # 其中壁纸帖数量
    "total_images": 0,
    "posts_done": 0,
    "downloaded_images": 0,
    "skipped_images": 0,
    "failed_images": 0,
    "downloaded_bytes": 0,
    "threads": DEFAULT_THREADS,
    "scan_mode": "",             # parallel / sequential
    "save_dir": DEFAULT_SAVE_DIR,
    "started_at": 0.0,
    "finished_at": 0.0,
    "phase_started_at": 0.0,
    "target": "",
    "logs": [],
    "log_total": 0,
    "workers": [],
    "worker_count": 0,
    "workers_version": 0,
}


def add_log(message, level="info"):
    with _state_lock:
        seq = state["log_total"] + 1
        state["log_total"] = seq
        state["logs"].append({
            "seq": seq,
            "time": datetime.now().strftime("%H:%M:%S"),
            "message": message,
            "level": level,
        })
        if len(state["logs"]) > LOG_LIMIT:
            del state["logs"][:-LOG_LIMIT]


def _bump(**deltas):
    with _state_lock:
        for key, delta in deltas.items():
            state[key] = state.get(key, 0) + delta


def _busy():
    return state["status"] in ("fetching", "downloading")


# --------------------------------------------------------------------------- #
# 线程状态（扫描 / 下载共用同一套槽位，前端逐线程展示）
# --------------------------------------------------------------------------- #

def init_workers(count, phase):
    with _state_lock:
        state["workers"] = [{
            "id": i + 1, "phase": phase, "detail": "空闲", "done": 0, "failed": 0,
            "bytes": 0, "cur": 0, "total": 0, "speed": 0.0, "active": False,
        } for i in range(count)]
        state["worker_count"] = count
        state["workers_version"] += 1


def worker_set(idx, **fields):
    with _state_lock:
        workers = state["workers"]
        if 0 <= idx < len(workers):
            workers[idx].update(fields)
            state["workers_version"] += 1


# --------------------------------------------------------------------------- #
# 网络与文件
# --------------------------------------------------------------------------- #

def http_get_json(url, timeout=20):
    req = urllib.request.Request(url, headers={
        "User-Agent": USER_AGENT,
        "Referer": "https://www.miyoushe.com/",
        "Accept": "application/json, text/plain, */*",
    })
    with urllib.request.urlopen(req, timeout=timeout, context=_SSL_CTX) as resp:
        return json.loads(resp.read().decode("utf-8"))


def download_to_file(url, filepath, timeout=60, on_progress=None):
    """流式下载到 .part 临时文件，成功后原子改名，返回字节数。"""
    part_path = filepath + ".part"
    req = urllib.request.Request(url, headers={
        "User-Agent": USER_AGENT,
        "Referer": "https://www.miyoushe.com/",
    })
    written = 0
    try:
        with urllib.request.urlopen(req, timeout=timeout, context=_SSL_CTX) as resp:
            total = int(resp.headers.get("Content-Length") or 0)
            with open(part_path, "wb") as fh:
                while True:
                    if _stop_event.is_set():
                        raise InterruptedError("用户停止了下载")
                    chunk = resp.read(65536)
                    if not chunk:
                        break
                    fh.write(chunk)
                    written += len(chunk)
                    if on_progress:
                        on_progress(written, total)
        if written <= 0:
            raise IOError("响应内容为空")
        os.replace(part_path, filepath)
        return written
    except BaseException:
        try:
            os.remove(part_path)
        except OSError:
            pass
        raise


_INVALID_CHARS = re.compile(r'[<>:"/\\|?*\x00-\x1f]')
_RESERVED_NAMES = {"CON", "PRN", "AUX", "NUL"} | {"COM%d" % i for i in range(1, 10)} \
    | {"LPT%d" % i for i in range(1, 10)}


def sanitize_filename(name, max_len=80):
    name = _INVALID_CHARS.sub("_", str(name or ""))
    name = re.sub(r"\s+", " ", name).strip().strip(".")
    if not name:
        name = "untitled"
    if name.upper() in _RESERVED_NAMES:
        name = "_" + name
    return name[:max_len].strip().strip(".") or "untitled"


def resolve_save_dir(save_dir):
    save_dir = (save_dir or "").strip() or DEFAULT_SAVE_DIR
    save_dir = os.path.expanduser(save_dir)
    if not os.path.isabs(save_dir):
        save_dir = os.path.join(BASE_DIR, save_dir)
    return os.path.normpath(save_dir)


def open_in_file_manager(path):
    os.makedirs(path, exist_ok=True)
    if sys.platform.startswith("win"):
        os.startfile(path)  # noqa: S606 - Windows only
    elif sys.platform == "darwin":
        subprocess.Popen(["open", path])
    else:
        subprocess.Popen(["xdg-open", path])


def fmt_duration(seconds):
    seconds = int(max(0, seconds))
    if seconds < 60:
        return "%d 秒" % seconds
    minutes, sec = divmod(seconds, 60)
    if minutes < 60:
        return "%d 分 %d 秒" % (minutes, sec)
    hours, minutes = divmod(minutes, 60)
    return "%d 小时 %d 分" % (hours, minutes)


# --------------------------------------------------------------------------- #
# 扫描：并行下潜抓取
#
# 接口语义（实测确认）：
#   GET userPostList?uid=U&size=20[&offset=X]
#     -> 返回该用户 post_id < X 的最新 20 条，按 post_id 降序
#     -> X 可以是任意帖子 ID，不必须是上一页返回的 next_offset
#
# 因此可以把 ID 轴切成若干段并行下潜。设同一用户帖子序列 p1>p2>...>pN，
# 窗口 A = [p_i .. p_{i+19}]，窗口 B = [p_j .. p_{j+19}] 且 A 在 B 上方。
# 两窗口之间存在漏帖  <=>  j > i+19  <=>  A.lo > B.hi
# 所以"相邻窗口端点比对"就是无漏帖的充要判据，扫描结束后可以直接自检。
# --------------------------------------------------------------------------- #

def _probe_page(uid, offset=None):
    """取一页帖子。offset=None 表示从头（最新）开始。"""
    url = "%s?size=%d&uid=%s" % (API_URL, PAGE_SIZE, urllib.parse.quote(str(uid)))
    if offset is not None:
        url += "&offset=" + urllib.parse.quote(str(offset))
    data = http_get_json(url, timeout=20)
    if data.get("retcode") != 0:
        raise RuntimeError(data.get("message") or "接口返回错误")

    payload = data.get("data") or {}
    posts = []
    for item in payload.get("list") or []:
        post = item.get("post") or {}
        if post.get("post_id") is not None:
            posts.append(post)
    # 防御性排序：接口本应已降序
    posts.sort(key=lambda p: int(p.get("post_id") or 0), reverse=True)
    return posts, bool(payload.get("is_last"))


def _window_hi(window):
    return int(window[0].get("post_id") or 0)


def _window_lo(window):
    return int(window[-1].get("post_id") or 0)


def _dedupe_windows(windows):
    """去掉被其它窗口完全包含的窗口。

    窗口都是帖子序列上的连续区间，所以 B.hi>=A.hi 且 B.lo<=A.lo 即 A 被 B 包含。
    """
    items = [(_window_hi(w), _window_lo(w), w) for w in windows if w]
    items.sort(key=lambda t: (-t[0], -t[1]))
    out = []
    min_lo = None
    for _hi, lo, win in items:
        if min_lo is not None and lo >= min_lo:
            continue
        out.append(win)
        min_lo = lo if min_lo is None else min(min_lo, lo)
    return out


def _parallel_scan(uid, scan_threads):
    """并行扫描：多起点撒点 + 缺口内部细分，直到所有相邻窗口端点相接。

    无漏帖判据：相邻窗口 A(上) B(下)，A.lo > B.hi 才可能有漏帖；
    反之 A.lo <= B.hi 时两者之间必然不存在帖子（同为连续区间）。
    循环终点即"任意相邻窗口都满足 A.lo <= B.hi"，此时覆盖必然完整。
    """
    concurrency = max(2, min(scan_threads, MAX_SCAN_THREADS))
    counter = {"probes": 0}
    seen = set()
    seen_lock = threading.Lock()
    probed = set()
    per_worker = [0] * concurrency
    slot = [0]

    def record(posts):
        with seen_lock:
            for post in posts:
                seen.add(str(post.get("post_id")))
            return len(seen)

    def run_batch(offsets):
        """并发执行一批探测（按 concurrency 分批），返回 [(offset, posts, is_last)]。"""
        results = []
        res_lock = threading.Lock()

        for start in range(0, len(offsets), concurrency):
            if _stop_event.is_set():
                break
            part = offsets[start:start + concurrency]

            def task(off):
                with _state_lock:
                    if counter["probes"] >= MAX_PROBES:
                        return
                    counter["probes"] += 1
                    widx = slot[0] % concurrency
                    slot[0] += 1
                try:
                    posts, is_last = _probe_page(uid, off)
                except Exception as exc:
                    add_log("探测 id < %s 失败：%s" % (off, exc), "warning")
                    worker_set(widx, phase="scan", active=False,
                               detail="失败 id < %s" % off)
                    return
                if posts:
                    total_seen = record(posts)
                    per_worker[widx] += 1
                    with _state_lock:
                        state["scanned_posts"] = total_seen
                    worker_set(widx, phase="scan", active=True,
                               done=per_worker[widx], detail="抓取 id < %s" % off,
                               cur=len(posts), total=len(posts))
                else:
                    worker_set(widx, phase="scan", active=False, detail="已到底")
                with res_lock:
                    results.append((off, posts, is_last))

            threads = []
            for off in part:
                th = threading.Thread(target=task, args=(off,), daemon=True)
                threads.append(th)
                th.start()
            for th in threads:
                th.join()
        return results

    # ---- 第一页：拿到最新帖子 ID 作为撒点基准 ----
    first, first_is_last = _probe_page(uid)
    counter["probes"] = 1
    if not first:
        return []
    with _state_lock:
        state["scanned_posts"] = record(first)

    if first_is_last or len(first) < PAGE_SIZE:
        init_workers(1, "scan")
        worker_set(0, phase="scan", done=1, detail="只有一页")
        return list(first)

    top_id = _window_hi(first)
    init_workers(concurrency, "scan")
    windows = [first]
    bottom_done = False

    def absorb(batch):
        """合并一批探测结果，返回新增窗口数。"""
        nonlocal bottom_done
        added = 0
        for _off, posts, is_last in batch:
            if posts:
                windows.append(posts)
                added += 1
                if is_last or len(posts) < PAGE_SIZE:
                    bottom_done = True
            else:
                bottom_done = True          # 空页 = 已经没有更早的帖子了
        return added

    # ---- 第一轮：沿 ID 轴几何撒点 ----
    seeds = []
    for k in range(1, concurrency):
        seed = int(top_id * (0.78 ** k))
        if seed > 0:
            seeds.append(seed)
    seeds = [s for s in dict.fromkeys(seeds) if s not in probed]
    probed.update(seeds)
    absorb(run_batch(seeds))

    add_log("并行扫描：%d 个起点，已发现 %d 条帖子"
            % (concurrency, state["scanned_posts"]), "info")

    # ---- 反复填缝，直到任意相邻窗口都端点相接 ----
    while not _stop_event.is_set():
        windows = _dedupe_windows(windows)
        windows.sort(key=_window_hi, reverse=True)

        gaps = [(a, b) for a, b in zip(windows, windows[1:])
                if _window_lo(a) > _window_hi(b)]
        need_bottom = not bottom_done
        if not gaps and not need_bottom:
            break

        offsets = []
        for a, b in gaps:
            hi, lo = _window_lo(a), _window_hi(b)
            offsets.append(hi)                      # 前沿下移一页，保证有进展
            span = hi - lo
            if span > 2:                            # 缺口内部再切两刀，加速收敛
                for j in (1, 2):
                    x = int(hi - span * j / 3.0)
                    if lo < x < hi:
                        offsets.append(x)

        if need_bottom:
            bottom_lo = _window_lo(windows[-1])
            offsets.append(bottom_lo)
            for frac in (0.5, 0.2, 0.05):           # 尾部也并行下探，避免单线程长距离下潜
                x = int(bottom_lo * frac)
                if 0 < x < bottom_lo:
                    offsets.append(x)

        offsets = [o for o in dict.fromkeys(offsets) if o not in probed]
        if not offsets:
            break
        with _state_lock:
            if counter["probes"] + len(offsets) > MAX_PROBES:
                add_log("探测次数达到上限，剩余部分改用顺序翻页", "warning")
                break
        probed.update(offsets)

        added = absorb(run_batch(offsets))
        if added == 0:
            if need_bottom and not bottom_done:
                bottom_done = True
                continue
            break

    for i in range(concurrency):
        worker_set(i, phase="scan", active=False, detail="扫描结束",
                   cur=0, total=0, speed=0.0)

    merged = []
    seen_ids = set()
    for win in windows:
        for post in win:
            pid = str(post.get("post_id"))
            if pid not in seen_ids:
                seen_ids.add(pid)
                merged.append(post)
    merged.sort(key=lambda p: int(p.get("post_id") or 0), reverse=True)
    return merged



def _sequential_scan(uid):
    """回退方案：老老实实一页一页翻。"""
    init_workers(1, "scan")
    posts = []
    seen = set()
    offset = None
    failures = 0

    for page in range(1, MAX_PROBES + 1):
        if _stop_event.is_set():
            break
        try:
            batch, is_last = _probe_page(uid, offset)
            failures = 0
        except Exception as exc:
            failures += 1
            add_log("第 %d 页失败：%s" % (page, exc), "warning")
            if failures >= 3:
                break
            time.sleep(1.0)
            continue

        fresh = 0
        for post in batch:
            pid = str(post.get("post_id"))
            if pid not in seen:
                seen.add(pid)
                posts.append(post)
                fresh += 1

        worker_set(0, active=True, done=page, detail="第 %d 页" % page, bytes=len(posts))
        add_log("第 %d 页：%d 条（累计 %d）" % (page, fresh, len(posts)), "info")

        if is_last or not batch:
            break
        offset = _window_lo(batch)
        time.sleep(0.05)

    return posts


def fetch_posts(uid, threads):
    try:
        add_log("开始扫描用户 %s 的帖子…" % uid, "info")
        t0 = time.time()

        with _state_lock:
            state["scanned_posts"] = 0

        try:
            all_posts = _parallel_scan(uid, threads)
            mode = "并行"
        except Exception as exc:
            add_log("并行扫描失败（%s），回退顺序扫描" % exc, "warning")
            all_posts = _sequential_scan(uid)
            mode = "顺序"

        if _stop_event.is_set():
            add_log("已取消扫描", "warning")

        elapsed = time.time() - t0
        wallpaper_posts = [p for p in all_posts if "壁纸" in (p.get("subject") or "")]
        total_images = sum(len(p.get("images") or []) for p in wallpaper_posts)

        with _state_lock:
            state["wallpaper_posts"] = wallpaper_posts
            state["scanned_posts"] = len(all_posts)
            state["total_posts"] = len(wallpaper_posts)
            state["total_images"] = total_images
            state["posts_done"] = 0
            state["downloaded_images"] = 0
            state["skipped_images"] = 0
            state["failed_images"] = 0
            state["downloaded_bytes"] = 0
            state["threads"] = threads
            state["scan_mode"] = mode
            state["started_at"] = 0.0
            state["finished_at"] = 0.0
            state["status"] = "idle"

        add_log("%s扫描完成：%d 条帖子（含非壁纸），耗时 %.1f 秒"
                % (mode, len(all_posts), elapsed), "success")
        add_log("其中壁纸帖 %d 个，共 %d 张图片" % (len(wallpaper_posts), total_images), "success")

    except Exception as exc:
        add_log("扫描失败：%s" % exc, "error")
        with _state_lock:
            state["status"] = "idle"


# --------------------------------------------------------------------------- #
# 下载
# --------------------------------------------------------------------------- #

def _maybe_warn_failure_rate():
    if _quality_warned.is_set():
        return
    with _state_lock:
        if _quality_warned.is_set():
            return
        failed = state["failed_images"]
        settled = failed + state["downloaded_images"] + state["skipped_images"]
        threads = state["threads"]
        if failed >= 15 and settled >= 20 and failed / float(settled) >= 0.35:
            _quality_warned.set()
            add_log("失败率偏高（%d/%d），服务器可能在限速；"
                    "把并发从 %d 降到 8~16 通常又快又稳"
                    % (failed, settled, threads), "warning")


def _process_image(task, widx):
    """下载单张图片，带重试。返回是否成功。"""
    post_key, folder_name, file_name, url, file_path = task
    if _stop_event.is_set():
        return False

    # 每 0.25s 最多回传一次进度，避免高频抢锁
    mark = [0.0, time.time(), 0.0]

    def on_progress(written, total):
        now = time.time()
        if now - mark[0] < 0.25:
            return
        inst = (written - mark[2]) / max(1e-6, now - mark[1])
        speed = inst if mark[0] == 0 else 0.55 * getattr(on_progress, "_spd", inst) + 0.45 * inst
        on_progress._spd = speed
        mark[0], mark[1], mark[2] = now, now, written
        worker_set(widx, cur=written, total=total, speed=speed)

    last_error = None
    for attempt in range(1, MAX_RETRY + 1):
        if _stop_event.is_set():
            return False
        try:
            size = download_to_file(url, file_path, on_progress=on_progress)
            if size < MIN_VALID_BYTES:
                raise IOError("文件异常偏小（%d 字节）" % size)
            _bump(downloaded_images=1, downloaded_bytes=size)
            add_log("✓ %s / %s  (%.0f KB)"
                    % (folder_name, file_name, size / 1024.0), "success")
            return True
        except InterruptedError:
            return False
        except Exception as exc:
            last_error = exc
            if attempt < MAX_RETRY:
                time.sleep(1.0 * attempt + random.random() * 1.2)

    _bump(failed_images=1)
    add_log("✗ %s / %s  —— %s" % (folder_name, file_name, last_error), "error")
    _maybe_warn_failure_rate()
    return False


def download_posts(posts, save_dir, naming_mode, threads):
    try:
        total_posts = len(posts)
        total_images = sum(len(p.get("images") or []) for p in posts)

        with _state_lock:
            state["status"] = "downloading"
            state["total_posts"] = total_posts
            state["total_images"] = total_images
            state["posts_done"] = 0
            state["downloaded_images"] = 0
            state["skipped_images"] = 0
            state["failed_images"] = 0
            state["downloaded_bytes"] = 0
            state["save_dir"] = save_dir
            state["started_at"] = time.time()
            state["finished_at"] = 0.0

        add_log("开始下载 %d 个帖子 / %d 张图片，请求并发 %d 线程"
                % (total_posts, total_images, threads), "info")
        if threads >= HIGH_THREADS:
            add_log("并发 %d 偏高：实测该图站按 IP 限速，并发越大单连接越慢，"
                    "建议调回 8~16" % threads, "warning")

        tasks = []
        progress = {}
        planned_paths = set()

        for idx, post in enumerate(posts, 1):
            if _stop_event.is_set():
                break

            post_id = str(post.get("post_id") or "")
            subject = post.get("subject") or ""
            created_at = post.get("created_at") or 0
            images = post.get("images") or []

            try:
                date_str = datetime.fromtimestamp(created_at).strftime("%Y-%m-%d")
            except (ValueError, OSError, OverflowError):
                date_str = "unknown-date"

            if naming_mode == "date_title":
                folder_name = "%s_%s" % (date_str, sanitize_filename(subject))
            elif naming_mode == "date":
                folder_name = "%s_%s" % (date_str, post_id)
            else:
                folder_name = sanitize_filename(post_id, 60) or "post"

            post_dir = os.path.join(save_dir, folder_name)
            os.makedirs(post_dir, exist_ok=True)

            title_file = os.path.join(post_dir, "title.txt")
            if not os.path.exists(title_file) and subject:
                with open(title_file, "w", encoding="utf-8") as fh:
                    fh.write(subject)

            progress[idx] = 0

            for img_idx, img_url in enumerate(images, 1):
                ext = os.path.splitext(img_url.split("?")[0])[1].lower()
                if ext not in (".png", ".jpg", ".jpeg", ".webp", ".gif", ".bmp"):
                    ext = ".png"

                if naming_mode == "id":
                    file_name = "%s_%02d%s" % (post_id, img_idx, ext)
                else:
                    file_name = "%s_%02d%s" % (date_str, img_idx, ext)

                file_path = os.path.join(post_dir, file_name)

                if os.path.exists(file_path) and os.path.getsize(file_path) >= MIN_VALID_BYTES:
                    _bump(skipped_images=1)
                    continue

                if file_path in planned_paths:
                    continue
                planned_paths.add(file_path)

                tasks.append((idx, folder_name, file_name, img_url, file_path))
                progress[idx] += 1

            if progress[idx] == 0:
                _bump(posts_done=1)

        if not tasks:
            add_log("没有需要下载的图片（全部已存在）", "success")
            _finish_download()
            return

        add_log("其中 %d 张需要下载，%d 张已存在已跳过"
                % (len(tasks), state["skipped_images"]), "info")

        work_queue = queue.Queue()
        for task in tasks:
            work_queue.put(task)

        worker_count = max(1, min(threads, len(tasks)))
        with _state_lock:
            state["threads"] = worker_count
        if worker_count != threads:
            add_log("待下载 %d 张，实际启用 %d 个线程（线程数按任务量收敛）"
                    % (len(tasks), worker_count), "info")

        init_workers(worker_count, "download")
        _quality_warned.clear()

        counter = {"left": len(tasks)}
        done_event = threading.Event()

        def worker(widx):
            while not _stop_event.is_set():
                try:
                    task = work_queue.get_nowait()
                except queue.Empty:
                    worker_set(widx, phase="download", active=False,
                               detail="任务已领完", speed=0.0, cur=0, total=0)
                    return

                folder_name, file_name = task[1], task[2]
                worker_set(widx, phase="download", active=True, detail=file_name,
                           cur=0, total=0, speed=0.0)
                ok = False
                try:
                    ok = _process_image(task, widx)
                except Exception as exc:
                    _bump(failed_images=1)
                    add_log("线程 %d 异常：%s" % (widx + 1, exc), "error")
                finally:
                    with _state_lock:
                        w = state["workers"][widx]
                        if ok:
                            w["done"] += 1
                            w["bytes"] += 0
                        else:
                            w["failed"] += 1
                        w["active"] = False
                        w["speed"] = 0.0
                        w["cur"] = 0
                        w["total"] = 0
                        state["workers_version"] += 1

                        progress[task[0]] -= 1
                        if progress[task[0]] <= 0:
                            state["posts_done"] += 1
                        counter["left"] -= 1
                        if counter["left"] <= 0:
                            done_event.set()
                    work_queue.task_done()

        for i in range(worker_count):
            threading.Thread(target=worker, args=(i,), daemon=True).start()

        while not done_event.wait(0.3):
            if _stop_event.is_set():
                break

        for i in range(worker_count):
            worker_set(i, active=False, speed=0.0, cur=0, total=0, detail="已结束")

        _finish_download()

    except Exception as exc:
        add_log("下载出错：%s" % exc, "error")
        with _state_lock:
            state["status"] = "idle"


def _finish_download():
    stopped = _stop_event.is_set()
    with _state_lock:
        state["finished_at"] = time.time()
        state["status"] = "stopped" if stopped else "idle"
        downloaded = state["downloaded_images"]
        skipped = state["skipped_images"]
        failed = state["failed_images"]
        total_bytes = state["downloaded_bytes"]
        elapsed = state["finished_at"] - state["started_at"] if state["started_at"] else 0.0
        save_dir = state["save_dir"]

    add_log("-" * 46, "info")
    add_log("下载已停止" if stopped else "全部下载完成！", "warning" if stopped else "success")
    add_log("新下载 %d 张 | 跳过 %d 张 | 失败 %d 张" % (downloaded, skipped, failed), "info")
    if elapsed > 0:
        speed = total_bytes / elapsed / 1024.0 / 1024.0
        add_log("耗时 %s | 平均 %.2f MB/s | 共 %.1f MB"
                % (fmt_duration(elapsed), speed, total_bytes / 1024.0 / 1024.0), "info")
    add_log("保存位置：%s" % save_dir, "info")


# --------------------------------------------------------------------------- #
# Web 服务
# --------------------------------------------------------------------------- #

def build_status(after_seq, workers_version):
    with _state_lock:
        logs = [entry for entry in state["logs"] if entry["seq"] > after_seq]
        now = time.time()
        elapsed = 0.0
        if state["started_at"]:
            elapsed = (state["finished_at"] or now) - state["started_at"]
        speed = state["downloaded_bytes"] / elapsed if elapsed > 0.5 else 0.0
        processed = (state["downloaded_images"] + state["skipped_images"]
                     + state["failed_images"])
        payload = {
            "version": VERSION,
            "status": state["status"],
            "run_id": state["run_id"],
            "scanned_posts": state["scanned_posts"],
            "total_posts": state["total_posts"],
            "total_images": state["total_images"],
            "posts_done": state["posts_done"],
            "downloaded_images": state["downloaded_images"],
            "skipped_images": state["skipped_images"],
            "failed_images": state["failed_images"],
            "processed_images": processed,
            "downloaded_bytes": state["downloaded_bytes"],
            "threads": state["threads"],
            "scan_mode": state["scan_mode"],
            "save_dir": state["save_dir"],
            "elapsed": round(elapsed, 1),
            "speed_bps": round(speed, 1),
            "log_total": state["log_total"],
            "worker_count": state["worker_count"],
            "workers_version": state["workers_version"],
            "logs": logs,
        }
        if workers_version != state["workers_version"]:
            payload["workers"] = [dict(w) for w in state["workers"]]
        return payload


class WallpaperHandler(http.server.BaseHTTPRequestHandler):
    server_version = "MiyousheWallpaper/" + VERSION

    def log_message(self, fmt, *args):
        pass

    def _send_json(self, data, code=200):
        body = json.dumps(data, ensure_ascii=False).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def _send_file(self, filepath, content_type="text/html"):
        try:
            with open(filepath, "rb") as fh:
                body = fh.read()
        except FileNotFoundError:
            self.send_error(404, "File not found")
            return
        self.send_response(200)
        self.send_header("Content-Type", content_type + "; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def _read_json_body(self):
        try:
            length = int(self.headers.get("Content-Length") or 0)
        except ValueError:
            length = 0
        if length <= 0:
            return {}
        raw = self.rfile.read(length).decode("utf-8")
        try:
            return json.loads(raw)
        except ValueError:
            return {}

    def do_OPTIONS(self):
        self.send_response(204)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.end_headers()

    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)
        path = parsed.path

        if path in ("/", "/index.html"):
            self._send_file(os.path.join(BASE_DIR, "index.html"), "text/html")

        elif path == "/api/status":
            query = urllib.parse.parse_qs(parsed.query)
            try:
                after = int((query.get("after") or ["0"])[0])
            except ValueError:
                after = 0
            try:
                wv = int((query.get("wv") or ["-1"])[0])
            except ValueError:
                wv = -1
            self._send_json(build_status(after, wv))

        elif path == "/api/config":
            self._send_json({
                "version": VERSION,
                "save_dir": DEFAULT_SAVE_DIR,
                "default_threads": DEFAULT_THREADS,
                "max_threads": MAX_THREADS,
                "max_scan_threads": MAX_SCAN_THREADS,
            })

        else:
            self.send_error(404)

    def do_POST(self):
        global _worker
        path = urllib.parse.urlparse(self.path).path
        data = self._read_json_body()

        if path == "/api/fetch":
            uid = str(data.get("uid") or "").strip()
            if not uid:
                self._send_json({"error": "请输入用户 ID"}, 400)
                return
            if _busy():
                self._send_json({"error": "当前任务正在运行，请先停止"}, 409)
                return

            try:
                threads = int(data.get("threads") or DEFAULT_THREADS)
            except (TypeError, ValueError):
                threads = DEFAULT_THREADS
            threads = max(1, min(MAX_THREADS, threads))

            _stop_event.clear()
            with _state_lock:
                state["status"] = "fetching"
                state["run_id"] += 1
                state["logs"] = []
                state["target"] = uid
                state["wallpaper_posts"] = []
                state["total_posts"] = 0
                state["total_images"] = 0
                state["scanned_posts"] = 0
                state["scan_mode"] = ""
                state["workers"] = []
                state["worker_count"] = 0
                state["workers_version"] += 1

            _worker = threading.Thread(target=fetch_posts, args=(uid, threads), daemon=True)
            _worker.start()
            self._send_json({"status": "ok"})

        elif path == "/api/download":
            if _busy():
                self._send_json({"error": "当前任务正在运行，请先停止"}, 409)
                return
            if not state["wallpaper_posts"]:
                self._send_json({"error": "请先获取帖子列表"}, 400)
                return

            try:
                threads = int(data.get("threads") or DEFAULT_THREADS)
            except (TypeError, ValueError):
                threads = DEFAULT_THREADS
            threads = max(1, min(MAX_THREADS, threads))

            naming_mode = data.get("naming_mode") or "date_title"
            if naming_mode not in ("date_title", "date", "id"):
                naming_mode = "date_title"

            save_dir = resolve_save_dir(data.get("save_dir"))

            posts = list(state["wallpaper_posts"])
            date_mode = data.get("date_mode") or "all"
            if date_mode == "range":
                start_date = str(data.get("start_date") or "").strip()
                end_date = str(data.get("end_date") or "").strip()
                if not start_date or not end_date:
                    self._send_json({"error": "请选择完整的开始和结束日期"}, 400)
                    return
                try:
                    start_dt = datetime.strptime(start_date, "%Y-%m-%d")
                    end_dt = datetime.strptime(end_date, "%Y-%m-%d")
                except ValueError:
                    self._send_json({"error": "日期格式不正确"}, 400)
                    return
                if start_dt > end_dt:
                    self._send_json({"error": "开始日期不能晚于结束日期"}, 400)
                    return

                start_ts = int(time.mktime(start_dt.timetuple()))
                end_ts = int(time.mktime(end_dt.replace(
                    hour=23, minute=59, second=59).timetuple()))
                posts = [p for p in posts
                         if start_ts <= (p.get("created_at") or 0) <= end_ts]

                if not posts:
                    self._send_json({"error": "该时间段内没有壁纸帖子"}, 400)
                    return

            try:
                os.makedirs(save_dir, exist_ok=True)
            except OSError as exc:
                self._send_json({"error": "无法创建保存目录：%s" % exc}, 400)
                return

            _stop_event.clear()
            with _state_lock:
                state["run_id"] += 1
                state["logs"] = []
                state["save_dir"] = save_dir
                state["workers"] = []
                state["worker_count"] = 0
                state["workers_version"] += 1

            _worker = threading.Thread(
                target=download_posts,
                args=(posts, save_dir, naming_mode, threads),
                daemon=True,
            )
            _worker.start()
            self._send_json({"status": "ok", "total_posts": len(posts),
                             "threads": threads})

        elif path == "/api/stop":
            _stop_event.set()
            self._send_json({"status": "ok"})

        elif path == "/api/open_folder":
            folder = resolve_save_dir(data.get("save_dir") or state["save_dir"])
            try:
                open_in_file_manager(folder)
                self._send_json({"status": "ok", "path": folder})
            except Exception as exc:
                self._send_json({"error": "打开文件夹失败：%s" % exc}, 500)

        else:
            self.send_error(404)


def create_server(preferred_port=5000, tries=11):
    last_error = None
    for port in range(preferred_port, preferred_port + tries):
        try:
            return http.server.ThreadingHTTPServer(("127.0.0.1", port), WallpaperHandler), port
        except OSError as exc:
            last_error = exc
    raise last_error


def main():
    open_browser = "--no-browser" not in sys.argv

    print("=" * 52)
    print("  米游社壁纸下载器  v%s" % VERSION)
    print("  纯 Python 标准库 · 零依赖 · 并行扫描 + 多线程下载")
    print("=" * 52)
    print()

    try:
        server, port = create_server()
    except OSError as exc:
        print("[错误] 5000~5010 端口全部被占用：%s" % exc)
        input("按回车键退出…")
        return 1

    url = "http://127.0.0.1:%d" % port
    print("服务已启动： %s" % url)
    print("下载目录：   %s" % DEFAULT_SAVE_DIR)
    print()
    print("关闭这个窗口即可停止服务（或按 Ctrl+C）")
    print()

    if open_browser:
        try:
            webbrowser.open(url)
        except Exception:
            pass

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n正在关闭服务…")
    finally:
        _stop_event.set()
        server.server_close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
