#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
IPTV 直播源自动聚合脚本 (纯标准库，无需 pip install)
==================================================
功能：
  1. 从多个公开权威 M3U 源抓取直播源
  2. 合并 + 去重 + 按频道名分组
  3. 并发做存活探测（TCP + HTTP），剔除失效线路
  4. 输出标准 M3U 播放列表 + TVBox 可用的 JSON 配置
  5. 全程零第三方依赖，GitHub Actions 上开箱即用

用法:
  python scripts/aggregate.py
"""

import json
import os
import re
import socket
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone, timedelta
from urllib.parse import urlparse

import urllib.request
import urllib.error

# ============================================================
# 配置区：想增删源，只改这里就行
# ============================================================

# 公开权威 IPTV 源清单（全部来自开源社区长期维护项目）
# 每个源可带代理前缀（GitHub 直连在国内常被墙，用镜像加速）
GH_PROXY = "https://gh-proxy.com/"

SOURCES = [
    # iptv-org 中国区（权威、精简）
    "https://iptv-org.github.io/iptv/countries/cn.m3u",
    # epg.pw：带节目单的中国大陆频道
    "https://epg.pw/test_channels.m3u",
    # YueChan/Live：中文频道优质源
    GH_PROXY + "https://raw.githubusercontent.com/YueChan/Live/main/IPTV.m3u",
    # Gitee 源（国内直连快，不依赖代理）
    "https://gitee.com/xxy002/zhiboyuan/raw/master/dsy",
    # Free-TV/IPTV 中国区
    GH_PROXY + "https://raw.githubusercontent.com/Free-TV/IPTV/master/playlists/playlist_china.m3u8",
    # Kimentanm 源（含部分 IPv6）
    GH_PROXY + "https://raw.githubusercontent.com/Kimentanm/aptv/master/m3u/iptv.m3u",
    # joevess/IPTV
    GH_PROXY + "https://raw.githubusercontent.com/joevess/IPTV/main/sources/iptv_sources.m3u8",
]

# 单个源最大下载体积（字节），超过就截断，防止巨型索引卡死
MAX_SOURCE_BYTES = 3 * 1024 * 1024   # 3 MB

# 只保留这些关键词命中的频道（留空列表 = 全部保留）
# 默认聚焦中文主流频道，避免列表过大
KEEP_KEYWORDS = [
    # 央视
    "CCTV", "中央", "央视",
    # 卫视
    "卫视", "湖南", "浙江", "江苏", "东方", "北京", "广东", "深圳", "安徽",
    "山东", "天津", "四川", "湖北", "河南", "江西", "辽宁", "黑龙江", "吉林",
    "河北", "山西", "陕西", "福建", "云南", "贵州", "广西", "甘肃", "青海",
    "宁夏", "新疆", "内蒙古", "西藏", "海南", "重庆", "厦门", "苏州",
]

# 存活探测配置
TIMEOUT = 6          # 单条超时（秒）
MAX_WORKERS = 32     # 并发线程数
PROBE_LIMIT = 800    # 最多探测多少条（防止 Actions 超时）
MAX_LINES_PER_CHANNEL = 5   # 同一频道最多探测几条线路（够用就行，省时间）
MIN_OK = 20          # 至少保留多少条可用，低于此值认为本次抓取失败

# 输出路径
OUT_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "output")
PLAYLIST_OUT = os.path.join(OUT_DIR, "playlist.m3u")
TVBOX_OUT = os.path.join(OUT_DIR, "tvbox.json")
STATUS_OUT = os.path.join(OUT_DIR, "status.json")


# ============================================================
# 抓取
# ============================================================

def fetch(url, timeout=20, max_bytes=MAX_SOURCE_BYTES):
    """下载文本（带体积上限），失败返回空串。"""
    req = urllib.request.Request(url, headers={
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                      "AppleWebKit/537.36 (KHTML, like Gecko) "
                      "Chrome/120.0 Safari/537.36",
        "Accept": "*/*",
    })
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            # 分块读取，超过上限即停止，避免巨型文件拖死
            chunks = []
            got = 0
            while got < max_bytes:
                chunk = resp.read(65536)
                if not chunk:
                    break
                chunks.append(chunk)
                got += len(chunk)
            raw = b"".join(chunks)
        # 尝试多种编码
        for enc in ("utf-8", "gbk", "gb18030", "latin-1"):
            try:
                return raw.decode(enc)
            except (UnicodeDecodeError, LookupError):
                continue
        return raw.decode("utf-8", errors="ignore")
    except Exception as exc:  # noqa: BLE001
        print(f"  [!] 抓取失败 {url[:70]}... -> {exc}")
        return ""


# ============================================================
# 解析
# ============================================================

ATTR_RE = re.compile(r'([\w-]+)="([^"]*)"')


def parse_m3u(text):
    """解析 M3U/M3U8，返回 [{'name','url','logo','group'}, ...]"""
    items = []
    lines = [ln.strip() for ln in text.splitlines()]
    pending = None

    for i, line in enumerate(lines):
        if not line:
            continue
        if line.startswith("#EXTINF"):
            attrs = dict(ATTR_RE.findall(line))
            # 频道名：取逗号后部分，没有则用 tvg-name
            name = line.split(",", 1)[1].strip() if "," in line else ""
            name = name or attrs.get("tvg-name", "") or attrs.get("tvg-id", "")
            name = clean_name(name)
            pending = {
                "name": name,
                "logo": attrs.get("tvg-logo", ""),
                "group": attrs.get("group-title", "") or "未分组",
            }
        elif line.startswith("#"):
            continue
        elif pending is not None:
            pending["url"] = line
            if pending.get("name") and pending.get("url"):
                items.append(pending)
            pending = None

    # 处理纯 txt 格式（每行 name,url）
    if not items:
        for line in lines:
            if not line or line.startswith("#"):
                continue
            if "," in line:
                parts = line.rsplit(",", 1)
                if len(parts) == 2 and parts[1].startswith("http"):
                    items.append({
                        "name": clean_name(parts[0]),
                        "url": parts[1].strip(),
                        "logo": "",
                        "group": "未分组",
                    })
    return items


def clean_name(name):
    """清掉频道名里的杂质，并做归一化，方便同一频道的多线路合并。"""
    name = name.strip()
    # 去掉括号备注
    name = re.sub(r"\[.*?\]|\(.*?\)|（.*?）|【.*?】", "", name)
    # 全角转半角
    name = normalize_width(name)
    name = re.sub(r"\s+", " ", name).strip()

    # 归一化 CCTV：CCTV1 / CCTV 1 / CCTV-1 → CCTV-1
    m = re.match(r"^(CCTV)\s*-?\s*(\d{1,2})\s*\+?\s*(HD|SD|FHD|4K|8K)?$", name, re.I)
    if m:
        num = m.group(2)
        tag = (m.group(3) or "").upper()
        return f"CCTV-{num}" + (f" {tag}" if tag else "")

    # 归一化 CGTN 等
    name = re.sub(r"^(CGTN)\s*-?\s*", r"\1 ", name, flags=re.I)

    return name or "未命名"


def normalize_width(s):
    """全角字符转半角。"""
    out = []
    for ch in s:
        code = ord(ch)
        if code == 0x3000:
            out.append(" ")
        elif 0xFF01 <= code <= 0xFF5E:
            out.append(chr(code - 0xFEE0))
        else:
            out.append(ch)
    return "".join(out)


# ============================================================
# 过滤 / 去重
# ============================================================

def keep(name, url):
    if not url.startswith("http"):
        return False
    if not KEEP_KEYWORDS:
        return True
    return any(kw.lower() in name.lower() for kw in KEEP_KEYWORDS)


def dedupe(items):
    """按频道名分组（归一化+大小写不敏感），同一频道保留多条线路，URL 全局去重。
    返回 {分组键: [线路, ...]}，每条线路自带 "name" 作为展示名。"""
    seen_urls = set()
    groups = {}
    for it in items:
        url = it["url"]
        key = url.split("|")[0]  # 去掉 url 后挂的 #key 参数再比
        if key in seen_urls:
            continue
        seen_urls.add(key)
        gkey = it["name"].lower()
        groups.setdefault(gkey, []).append(it)
    return groups


# ============================================================
# 存活探测
# ============================================================

def probe(item):
    """真实探测：先建 TCP 连接，若是 HTTP 再发一次 HEAD/GET 请求。
    返回 (item, ok, latency_ms)"""
    url = item["url"]
    t0 = time.time()
    try:
        p = urlparse(url)
        host = p.hostname
        port = p.port or (443 if p.scheme == "https" else 80)
        if not host:
            return item, False, 0

        # 1) TCP 连通性
        with socket.create_connection((host, port), timeout=TIMEOUT):
            pass

        # 2) HTTP 层确认（对 m3u8 更有意义）
        if p.scheme in ("http", "https"):
            req = urllib.request.Request(url, method="GET", headers={
                "User-Agent": "Mozilla/5.0 (VLC/3.0.20 LibVLC/3.0.20)",
                "Range": "bytes=0-256",
            })
            try:
                with urllib.request.urlopen(req, timeout=TIMEOUT) as resp:
                    resp.read(256)
            except urllib.error.HTTPError as he:
                # 403/404 等也算有一定存活意义，401/403 常因防盗链
                if he.code in (401, 403):
                    pass
                else:
                    return item, False, 0

        return item, True, int((time.time() - t0) * 1000)
    except Exception:  # noqa: BLE001
        return item, False, 0


def probe_all(groups):
    """对每条线路并发探测，返回 {分组键: [可用线路(按延迟排序)]}"""
    flat = []
    for gkey, lines in groups.items():
        # 同一频道最多探测 N 条，避免某个频道拖垮整体耗时
        for it in lines[:MAX_LINES_PER_CHANNEL]:
            it["_gkey"] = gkey
            flat.append(it)

    if len(flat) > PROBE_LIMIT:
        print(f"  [i] 线路数 {len(flat)} 超过上限 {PROBE_LIMIT}，截取前 {PROBE_LIMIT} 条探测")
        flat = flat[:PROBE_LIMIT]

    print(f"  [i] 开始并发探测 {len(flat)} 条线路（{MAX_WORKERS} 线程，单条 {TIMEOUT}s 超时）...")
    result = {}
    done = 0
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = {pool.submit(probe, it): it for it in flat}
        for fut in as_completed(futures):
            done += 1
            if done % 100 == 0:
                print(f"      进度 {done}/{len(flat)}")
            try:
                item, ok, latency = fut.result()
            except Exception:  # noqa: BLE001
                continue
            if ok:
                gkey = item.pop("_gkey")
                item["latency"] = latency
                result.setdefault(gkey, []).append(item)

    for gkey in result:
        result[gkey].sort(key=lambda x: x["latency"])
    return result


# ============================================================
# 输出
# ============================================================

def write_playlist(groups, path):
    now = datetime.now(timezone(timedelta(hours=8))).strftime("%Y-%m-%d %H:%M:%S")
    lines = ['#EXTM3U x-tvg-url="https://epg.pw/xmltv/epg_CN.xml"']
    lines.append(f"# 由 iptv-auto 自动生成 | 更新时间: {now} (UTC+8)")
    total = 0
    for gkey in sorted(groups):
        for it in groups[gkey]:
            name = it["name"]
            lines.append(
                f'#EXTINF:-1 tvg-name="{name}" tvg-logo="{it.get("logo", "")}" '
                f'group-title="{it.get("group", "未分组")}",{name}'
            )
            lines.append(it["url"])
            total += 1
    with open(path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")
    return total


def write_tvbox(groups, path):
    """生成 TVBox 可直接订阅的 JSON 配置（live 部分）。"""
    now = datetime.now(timezone(timedelta(hours=8))).strftime("%Y-%m-%d %H:%M:%S")
    channels = []
    for gkey in sorted(groups):
        lines = groups[gkey]
        if lines:
            channels.append({
                "name": lines[0]["name"],
                "urls": [it["url"] for it in lines],
            })

    cfg = {
        "name": "我的自动直播源",
        "version": 1,
        "updateTime": now,
        "lives": [
            {
                "name": "自动聚合直播",
                "type": 0,
                # 订阅地址：优先取环境变量 SELF_URL，其次用相对文件名
                "url": os.environ.get("SELF_URL", "playlist.m3u"),
                "playerType": 1,
                "epg": "https://epg.pw/xmltv/epg_CN.xml",
                "channels": channels,
            }
        ],
    }
    with open(path, "w", encoding="utf-8") as f:
        json.dump(cfg, f, ensure_ascii=False, indent=2)
    return len(channels)


def write_status(groups, total_lines, path):
    now = datetime.now(timezone(timedelta(hours=8))).strftime("%Y-%m-%d %H:%M:%S")
    status = {
        "updatedAt": now,
        "channelCount": len(groups),
        "lineCount": total_lines,
        "channels": {lines[0]["name"]: len(lines) for _, lines in sorted(groups.items())},
    }
    with open(path, "w", encoding="utf-8") as f:
        json.dump(status, f, ensure_ascii=False, indent=2)
    return status


# ============================================================
# 主流程
# ============================================================

def main():
    print("=" * 60)
    print("  IPTV 自动聚合开始")
    print("=" * 60)

    # 1. 抓取
    print("\n[1/5] 抓取公开源...")
    all_items = []
    for src in SOURCES:
        print(f"  -> {src[:80]}")
        text = fetch(src)
        if text:
            parsed = parse_m3u(text)
            print(f"     解析到 {len(parsed)} 条")
            all_items.extend(parsed)

    print(f"\n  合并后总计: {len(all_items)} 条")
    if not all_items:
        print("[x] 所有源都抓取失败，终止。")
        sys.exit(1)

    # 2. 过滤
    print("\n[2/5] 关键词过滤...")
    filtered = [it for it in all_items if keep(it["name"], it["url"])]
    print(f"  过滤后: {len(filtered)} 条")

    # 3. 去重
    print("\n[3/5] 去重分组...")
    groups = dedupe(filtered)
    print(f"  共 {len(groups)} 个频道")

    # 4. 探测
    print("\n[4/5] 存活探测...")
    alive = probe_all(groups)
    alive_lines = sum(len(v) for v in alive.values())
    print(f"  可用: {len(alive)} 个频道 / {alive_lines} 条线路")

    if len(alive) < 5 or alive_lines < MIN_OK:
        print(f"[x] 可用线路仅有 {alive_lines} 条，低于阈值 {MIN_OK}，判定为异常，不覆盖旧文件。")
        sys.exit(2)

    # 5. 输出
    print("\n[5/5] 生成输出文件...")
    os.makedirs(OUT_DIR, exist_ok=True)
    total = write_playlist(alive, PLAYLIST_OUT)
    chans = write_tvbox(alive, TVBOX_OUT)
    status = write_status(alive, total, STATUS_OUT)

    print(f"  [v] {PLAYLIST_OUT}  ({total} 条线路)")
    print(f"  [v] {TVBOX_OUT}  ({chans} 个频道)")
    print(f"  [v] {STATUS_OUT}")
    print("\n" + "=" * 60)
    print(f"  完成！更新时间 {status['updatedAt']}")
    print("=" * 60)


if __name__ == "__main__":
    main()
