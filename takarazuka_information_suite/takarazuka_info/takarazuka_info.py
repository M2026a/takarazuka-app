from __future__ import annotations

import hashlib
import html
import json
import re
import sqlite3
from collections import Counter, defaultdict
from datetime import datetime, timedelta
from pathlib import Path
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

ROOT = Path(__file__).resolve().parent.parent
SHARED = ROOT / 'shared'
OUTPUT = Path(__file__).resolve().parent / 'output'
OUTPUT.mkdir(parents=True, exist_ok=True)

CONFIG = json.loads((SHARED / 'config.json').read_text(encoding='utf-8'))
THEMES = json.loads((SHARED / 'themes.json').read_text(encoding='utf-8'))
SOURCES = json.loads((SHARED / 'sources.json').read_text(encoding='utf-8'))

APP_NAME = CONFIG['app_name']
APP_VERSION = CONFIG['version']
HISTORY_DAYS = int(CONFIG.get('history_days', 45))
NEW_BADGE_HOURS = int(CONFIG.get('new_badge_hours', 24))
MAX_NEWS_ITEMS = int(CONFIG.get('max_news_items', 150))
DETAIL_FETCH_LIMIT = int(CONFIG.get('detail_fetch_limit', 35))
DB_FILE = OUTPUT / 'takarazuka_info.db'
TIMEOUT = 20
USER_AGENT = f'Mozilla/5.0 TakarazukaInformationSuite/{APP_VERSION}'

CATEGORY_ORDER = ['公演', 'スター', '配信・放送', '商品', '会員・チケット', '劇場・施設', '学校・イベント', '公演スケジュール', 'その他']
TROUPES = THEMES['troupes']


def log(msg: str) -> None:
    print(msg, flush=True)


def load_text(path: Path, default: str = '') -> str:
    return path.read_text(encoding='utf-8') if path.exists() else default


def clean_text(text: str) -> str:
    text = html.unescape(text or '')
    text = re.sub(r'<[^>]+>', ' ', text)
    text = re.sub(r'\s+', ' ', text)
    return text.strip()


def normalize_text(text: str) -> str:
    return re.sub(r'\s+', ' ', text or '').strip()


def safe_filename(name: str) -> str:
    return re.sub(r'[^a-zA-Z0-9_-]+', '_', name)


def is_allowed_domain(url: str, allowed_domains: list[str]) -> bool:
    if not url:
        return False
    host = (urlparse(url).hostname or '').lower()
    if host.startswith('www.'):
        host = host[4:]
    for domain in allowed_domains:
        d = domain.lower()
        if d.startswith('www.'):
            d = d[4:]
        if host == d or host.endswith('.' + d):
            return True
    return False


def parse_news_date(s: str) -> datetime | None:
    m = re.search(r'(\d{4})[./](\d{2})[./](\d{2})', s)
    if not m:
        return None
    try:
        return datetime(int(m.group(1)), int(m.group(2)), int(m.group(3)))
    except ValueError:
        return None


def format_date(dt: datetime | None) -> str:
    return dt.strftime('%Y-%m-%d') if dt else ''


def is_new(dt: datetime | None) -> bool:
    return bool(dt and (datetime.now() - dt) <= timedelta(hours=NEW_BADGE_HOURS))


def get_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({'User-Agent': USER_AGENT})
    return s


def extract_troupes(text: str) -> list[str]:
    found = [t for t in TROUPES if t in text]
    return found or ['全組共通']


def classify_theme(text: str) -> str:
    if '公演スケジュール' in text:
        return '公演スケジュール'
    if '配信・放送' in text or 'タカスク' in text or 'オン・デマンド' in text or 'テレビ' in text or 'ライブ配信' in text:
        return '配信・放送'
    if '会員サービス' in text or '友の会' in text or 'チケット' in text or '先行販売' in text or '一般前売' in text:
        return '会員・チケット'
    if '劇場・店舗' in text or 'レビューショップ' in text or 'レストラン' in text or '駐車場' in text:
        return '劇場・施設'
    if '商品' in text or 'ブルーレイ' in text or 'DVD' in text or 'CD' in text or '発売' in text or 'GRAPH' in text:
        return '商品'
    if 'スター' in text or 'ディナーショー' in text or '組配属' in text or '副組長' in text:
        return 'スター'
    if '音楽学校' in text or '卒業式' in text or '研究科一年' in text:
        return '学校・イベント'
    if '公演' in text or '人物相関図' in text or '休演者' in text or 'PR映像' in text or '公演時間' in text:
        return '公演'
    return 'その他'


def score_item(item: dict) -> int:
    score = 1
    if item.get('official'):
        score += 2
    if item.get('theme') in {'公演', '公演スケジュール', '会員・チケット'}:
        score += 1
    if item.get('is_new'):
        score += 1
    title = item.get('title', '')
    hot_words = ['出演者決定', '先行販売', '一般前売', '休演者', '公演時間', 'ライブ配信', 'PR映像', '初日舞台映像']
    if any(w in title for w in hot_words):
        score += 1
    return min(score, 5)


def fetch_page(session: requests.Session, url: str) -> str:
    r = session.get(url, timeout=TIMEOUT)
    r.raise_for_status()
    r.encoding = r.apparent_encoding or r.encoding
    return r.text


def parse_news_anchor_text(text: str) -> tuple[datetime | None, str, list[str], str]:
    text = normalize_text(text)
    dt = parse_news_date(text)
    if dt:
        text = re.sub(r'^\d{4}[./]\d{2}[./]\d{2}\s*', '', text).strip()
    theme = classify_theme(text)
    troupes = extract_troupes(text)

    removable = ['公演', 'スター', '配信・放送', '商品', '会員サービス', '劇場・店舗', 'その他'] + TROUPES
    title = text
    for word in removable:
        title = re.sub(rf'\b{re.escape(word)}\b', ' ', title)
        title = title.replace(word, ' ')
    title = normalize_text(title)
    if not title:
        title = text
    if theme == '会員・チケット' and '会員サービス' in text and '会員サービス' not in title:
        title = f'会員サービス {title}'.strip()
    return dt, theme, troupes, title


def fetch_detail_summary(session: requests.Session, url: str) -> str:
    try:
        html_text = fetch_page(session, url)
    except Exception:
        return ''
    soup = BeautifulSoup(html_text, 'html.parser')
    paragraphs: list[str] = []
    for tag in soup.find_all(['p', 'li']):
        text = clean_text(tag.get_text(' ', strip=True))
        if not text:
            continue
        if len(text) < 8:
            continue
        if text in {'Tweet', 'ページトップへ'}:
            continue
        paragraphs.append(text)
        if len(' '.join(paragraphs)) > 220:
            break
    return normalize_text(' '.join(paragraphs))[:260]


def collect_news_html(session: requests.Session, source: dict) -> list[dict]:
    log(f"  取得中: {source['name']}")
    html_text = fetch_page(session, source['url'])
    soup = BeautifulSoup(html_text, 'html.parser')
    allowed_domains = source.get('allowed_domains', [])

    seen: set[str] = set()
    items: list[dict] = []
    detail_targets: list[dict] = []

    for a in soup.find_all('a', href=True):
        href = urljoin(source['url'], a['href'])
        text = normalize_text(a.get_text(' ', strip=True))
        if not text or len(text) < 8:
            continue
        if not parse_news_date(text):
            continue
        if not is_allowed_domain(href, allowed_domains):
            continue
        if href in seen:
            continue
        seen.add(href)

        pub_dt, theme, troupes, title = parse_news_anchor_text(text)
        if pub_dt and (datetime.now() - pub_dt).days > HISTORY_DAYS:
            continue

        item = {
            'id': 'item-' + hashlib.md5(href.encode()).hexdigest()[:12],
            'source': source['name'],
            'kind': 'news',
            'official': bool(source.get('official', False)),
            'title': title,
            'raw_title': text,
            'summary': text,
            'link': href,
            'published': format_date(pub_dt),
            'pub_dt': pub_dt.isoformat() if pub_dt else '',
            'theme': theme,
            'troupes': troupes,
            'is_new': is_new(pub_dt),
        }
        item['score'] = score_item(item)
        items.append(item)
        if 'kageki.hankyu.co.jp/news/' in href:
            detail_targets.append(item)
        if len(items) >= MAX_NEWS_ITEMS:
            break

    for item in detail_targets[:DETAIL_FETCH_LIMIT]:
        summary = fetch_detail_summary(session, item['link'])
        if summary:
            item['summary'] = summary

    log(f"  ✓ {source['name']}: {len(items)}件")
    return items


def lines_from_html(html_text: str) -> list[str]:
    soup = BeautifulSoup(html_text, 'html.parser')
    text = soup.get_text('\n')
    lines = [normalize_text(x) for x in text.splitlines()]
    return [x for x in lines if x]


def parse_performance_blocks(lines: list[str]) -> list[list[str]]:
    blocks: list[list[str]] = []
    current: list[str] = []
    started = False
    stop_words = {'サイトマップ', 'ページトップへ', '観劇マナーについて'}

    for line in lines:
        if line == '公演詳細を見る':
            if current:
                blocks.append(current)
                current = []
            started = True
            continue
        if not started:
            continue
        if line in stop_words:
            break
        current.append(line)
    if current:
        blocks.append(current)
    return blocks


def build_performance_summary(block: list[str]) -> tuple[str, str, list[str], datetime | None]:
    title_lines = [x for x in block if x.startswith('『') or '『' in x]
    title = ' / '.join(dict.fromkeys(title_lines[:2])) if title_lines else (block[0] if block else '公演情報')
    troupe_candidates = [t for t in TROUPES if any(t in line for line in block)]
    if not troupe_candidates:
        joined = ' '.join(block)
        mapping = {
            '永久輝 せあ': '花組', '星空 美咲': '花組', '鳳月 杏': '月組', '天紫 珠李': '月組',
            '朝美 絢': '雪組', '音彩 唯': '雪組', '暁 千星': '星組', '詩 ちづる': '星組',
            '桜木 みなと': '宙組', '春乃 さくら': '宙組'
        }
        for name, troupe in mapping.items():
            if name in joined:
                troupe_candidates = [troupe]
                break
    troupes = troupe_candidates or ['全組共通']

    dt = None
    for line in block:
        m = re.search(r'(\d{4})年(\d{1,2})月(\d{1,2})日', line)
        if m:
            try:
                dt = datetime(int(m.group(1)), int(m.group(2)), int(m.group(3)))
                break
            except ValueError:
                pass

    useful = []
    for line in block:
        if line == '公演詳細を見る':
            continue
        if len(useful) >= 8:
            break
        useful.append(line)
    summary = ' / '.join(useful[:8])[:320]
    return title, summary, troupes, dt


def collect_performance_html(session: requests.Session, source: dict) -> list[dict]:
    log(f"  取得中: {source['name']}")
    html_text = fetch_page(session, source['url'])
    soup = BeautifulSoup(html_text, 'html.parser')
    links = [urljoin(source['url'], a['href']) for a in soup.find_all('a', href=True) if '公演詳細を見る' in a.get_text(' ', strip=True)]
    blocks = parse_performance_blocks(lines_from_html(html_text))
    items: list[dict] = []

    for idx, block in enumerate(blocks):
        title, summary, troupes, pub_dt = build_performance_summary(block)
        href = links[idx] if idx < len(links) else source['url']
        item = {
            'id': 'item-' + hashlib.md5((href + title).encode()).hexdigest()[:12],
            'source': source['name'],
            'kind': 'schedule',
            'official': bool(source.get('official', False)),
            'title': title,
            'raw_title': title,
            'summary': summary,
            'link': href,
            'published': format_date(pub_dt),
            'pub_dt': pub_dt.isoformat() if pub_dt else '',
            'theme': '公演スケジュール',
            'troupes': troupes,
            'is_new': False,
        }
        item['score'] = score_item(item)
        items.append(item)

    log(f"  ✓ {source['name']}: {len(items)}件")
    return items


def collect_all() -> list[dict]:
    session = get_session()
    all_items: list[dict] = []
    for source in SOURCES:
        try:
            if source['kind'] == 'news_html':
                items = collect_news_html(session, source)
            elif source['kind'] == 'performance_html':
                items = collect_performance_html(session, source)
            else:
                items = []
            all_items.extend(items)
        except Exception as e:
            log(f"  ⚠ {source['name']}: {e}")

    dedup: dict[str, dict] = {}
    for item in all_items:
        key = item['link'] if item['kind'] == 'news' else f"{item['link']}::{item['title']}"
        if key not in dedup or item['score'] > dedup[key]['score']:
            dedup[key] = item

    items = list(dedup.values())
    items.sort(key=lambda x: (x.get('pub_dt', ''), x.get('score', 0)), reverse=True)
    log(f'\n✅ 合計 {len(items)} 件収集')
    return items


def db_init() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_FILE)
    conn.execute('''
        CREATE TABLE IF NOT EXISTS items (
            id TEXT PRIMARY KEY,
            source TEXT,
            kind TEXT,
            official INTEGER,
            title TEXT,
            raw_title TEXT,
            summary TEXT,
            link TEXT,
            published TEXT,
            pub_dt TEXT,
            theme TEXT,
            troupes TEXT,
            is_new INTEGER,
            score INTEGER
        )
    ''')
    conn.execute('DELETE FROM items')
    return conn


def db_save(items: list[dict]) -> None:
    conn = db_init()
    rows = [(
        x['id'], x['source'], x['kind'], int(x['official']), x['title'], x['raw_title'], x['summary'], x['link'],
        x['published'], x['pub_dt'], x['theme'], json.dumps(x['troupes'], ensure_ascii=False), int(x['is_new']), x['score']
    ) for x in items]
    conn.executemany('INSERT INTO items VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)', rows)
    conn.commit()
    conn.close()


def esc(s: str) -> str:
    return html.escape(s or '')


def theme_badge(theme: str) -> str:
    cfg = THEMES['themes'].get(theme, THEMES['themes']['その他'])
    return f"<span class='tag tag-theme' style='background:{cfg['color']}22;border-color:{cfg['color']};color:{cfg['color']}'>{cfg['icon']} {esc(theme)}</span>"


def troupe_badges(troupes: list[str]) -> str:
    return ''.join(f"<span class='tag troupe-tag'>{esc(t)}</span>" for t in troupes)


def card_html(item: dict) -> str:
    meta = [esc(item['source'])]
    if item.get('published'):
        meta.append(esc(item['published']))
    meta_html = ' / '.join(meta)
    tags = theme_badge(item['theme']) + troupe_badges(item['troupes'])
    if item.get('official'):
        tags += "<span class='tag tag-official'>公式</span>"
    if item.get('is_new'):
        tags += "<span class='tag tag-new'>NEW</span>"
    tags += f"<span class='tag tag-score'>重要度 {item['score']}</span>"
    return f"""
    <article class='card' data-theme='{esc(item['theme'])}' data-troupe='{esc('|'.join(item['troupes']))}' data-kind='{esc(item['kind'])}'>
      <div class='card-top'>
        <div class='meta'>{meta_html}</div>
      </div>
      <h3><a href='{esc(item['link'])}' target='_blank' rel='noopener noreferrer'>{esc(item['title'])}</a></h3>
      <p class='summary'>{esc(item['summary'])}</p>
      <div class='tags'>{tags}</div>
    </article>
    """


def base_css() -> str:
    return """
:root{--bg:#0d1016;--panel:#151a23;--panel2:#1b2230;--line:#2a3446;--text:#ebf0f8;--muted:#9ba9bc;--accent:#4ea1ff;--accent2:#1e2f49}
*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--text);font-family:'Segoe UI',Meiryo,sans-serif}
a{color:inherit;text-decoration:none}header{position:sticky;top:0;z-index:30;background:rgba(13,16,22,.96);backdrop-filter:blur(10px);border-bottom:1px solid var(--line)}
.wrap{max-width:1480px;margin:0 auto;padding:14px 20px}.top{display:flex;justify-content:space-between;gap:16px;align-items:flex-start;flex-wrap:wrap}.title h1{margin:0;font-size:24px}.title .sub{font-size:12px;color:var(--muted);margin-top:4px}.nav{display:flex;gap:8px;flex-wrap:wrap;margin-top:12px}.nav a{padding:10px 16px;border-radius:999px;background:rgba(255,255,255,.04);border:1px solid rgba(255,255,255,.12);font-size:13px;font-weight:700}.nav a.active{background:linear-gradient(135deg,#4ea1ff,#5a86ff);border-color:transparent}
.main{max-width:1480px;margin:20px auto;padding:0 20px 40px}.panel{background:var(--panel);border:1px solid var(--line);border-radius:16px;padding:16px;margin-bottom:14px}.stat-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(180px,1fr));gap:12px}.stat{background:var(--panel);border:1px solid var(--line);border-radius:14px;padding:14px}.num{font-size:28px;font-weight:800}.label{font-size:12px;color:var(--muted);margin-top:4px}
.filters{display:flex;gap:8px;flex-wrap:wrap;align-items:center}.search{background:var(--panel2);border:1px solid var(--line);color:var(--text);border-radius:10px;padding:9px 12px;min-width:260px}.flt-btn{background:var(--panel2);border:1px solid var(--line);color:var(--text);border-radius:10px;padding:8px 12px;font-size:13px;cursor:pointer}.flt-btn.active{background:var(--accent2);border-color:var(--accent)}
.grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(340px,1fr));gap:14px}.card{background:var(--panel);border:1px solid var(--line);border-radius:14px;padding:14px;display:flex;flex-direction:column;gap:10px}.card:hover{border-color:var(--accent)}.meta{font-size:12px;color:var(--muted)}.card h3{margin:0;font-size:15px;line-height:1.55}.summary{margin:0;color:#ced7e6;font-size:13px;line-height:1.65;white-space:pre-wrap}.tags{display:flex;gap:6px;flex-wrap:wrap;margin-top:auto}.tag{padding:3px 8px;border-radius:999px;border:1px solid transparent;font-size:11px;font-weight:700}.tag-official{background:#173126;border-color:#2f7254;color:#c6f1dc}.tag-new{background:#1a2e1a;border-color:#2ecc71;color:#2ecc71}.tag-score{background:#1e2f49;border-color:#4ea1ff;color:#cae2ff}.troupe-tag{background:#2b2237;border-color:#7f5af0;color:#e6ddff}
.section-title{margin:0 0 12px;font-size:18px}.group{margin-bottom:20px}.small{font-size:12px;color:var(--muted)}table{width:100%;border-collapse:collapse}th,td{border-bottom:1px solid var(--line);padding:10px 8px;font-size:13px;text-align:left}th{color:var(--muted)}
@media (max-width:800px){.grid{grid-template-columns:1fr}.search{min-width:100%}}
"""


def base_js() -> str:
    return """
function applyFilters(){
  const q=(document.getElementById('searchBox')?.value||'').toLowerCase();
  const theme=document.querySelector('.flt-btn.theme.active')?.dataset.value||'ALL';
  const troupe=document.querySelector('.flt-btn.troupe.active')?.dataset.value||'ALL';
  document.querySelectorAll('.card').forEach(card=>{
    const text=card.innerText.toLowerCase();
    const okQ=!q || text.includes(q);
    const okTheme=theme==='ALL' || card.dataset.theme===theme;
    const okTroupe=troupe==='ALL' || (card.dataset.troupe||'').includes(troupe);
    card.style.display=(okQ && okTheme && okTroupe)?'flex':'none';
  });
}
function activateFilter(btn, cls){document.querySelectorAll('.flt-btn.'+cls).forEach(x=>x.classList.remove('active'));btn.classList.add('active');applyFilters();}
document.addEventListener('DOMContentLoaded', ()=>{document.querySelectorAll('.flt-btn.theme').forEach(btn=>btn.onclick=()=>activateFilter(btn,'theme'));document.querySelectorAll('.flt-btn.troupe').forEach(btn=>btn.onclick=()=>activateFilter(btn,'troupe'));document.getElementById('searchBox')?.addEventListener('input',applyFilters);applyFilters();});
"""


def page_shell(title: str, active: str, body: str, items: list[dict]) -> str:
    total = len(items)
    news_count = sum(1 for x in items if x['kind'] == 'news')
    schedule_count = sum(1 for x in items if x['kind'] == 'schedule')
    new_count = sum(1 for x in items if x.get('is_new'))
    updated = datetime.now().strftime('%Y-%m-%d %H:%M')
    navs = [
        ('index.html', 'メイン'), ('pickup.html', '注目情報'), ('themes.html', 'テーマ別'),
        ('troupe.html', '組別'), ('schedule.html', '公演スケジュール'), ('analysis.html', '分析')
    ]
    nav_html = ''.join(f"<a href='{href}' class='{'active' if href==active else ''}'>{label}</a>" for href, label in navs)
    return f"""<!doctype html><html lang='ja'><head><meta charset='utf-8'><meta name='viewport' content='width=device-width,initial-scale=1'><title>{esc(title)} | {esc(APP_NAME)}</title><style>{base_css()}</style></head><body>
<header><div class='wrap'><div class='top'><div class='title'><h1>{esc(APP_NAME)}</h1><div class='sub'>宝塚情報取得アプリ / v{esc(APP_VERSION)} / 更新 {updated}</div></div></div><div class='nav'>{nav_html}</div></div></header>
<main class='main'>
<div class='stat-grid'>
<div class='stat'><div class='num'>{total}</div><div class='label'>総件数</div></div>
<div class='stat'><div class='num'>{news_count}</div><div class='label'>ニュース</div></div>
<div class='stat'><div class='num'>{schedule_count}</div><div class='label'>公演スケジュール</div></div>
<div class='stat'><div class='num'>{new_count}</div><div class='label'>NEW件数</div></div>
</div>
{body}
</main><script>{base_js()}</script></body></html>"""


def render_index(items: list[dict]) -> str:
    theme_buttons = "".join([f"<button class='flt-btn theme {'active' if i==0 else ''}' data-value='{esc(v)}'>{esc(v)}</button>" for i, v in enumerate(['ALL'] + CATEGORY_ORDER)])
    troupe_buttons = "".join([f"<button class='flt-btn troupe {'active' if i==0 else ''}' data-value='{esc(v)}'>{esc(v)}</button>" for i, v in enumerate(['ALL'] + TROUPES)])
    cards = ''.join(card_html(x) for x in items)
    body = f"""
    <section class='panel'><div class='filters'><input id='searchBox' class='search' placeholder='タイトル・本文で検索'><span class='small'>テーマ</span>{theme_buttons}</div><div class='filters' style='margin-top:8px'><span class='small'>組</span>{troupe_buttons}</div></section>
    <section class='grid'>{cards}</section>
    """
    return page_shell('メイン', 'index.html', body, items)


def render_pickup(items: list[dict]) -> str:
    sorted_items = sorted(items, key=lambda x: (x['score'], x['pub_dt']), reverse=True)
    top = sorted_items[:12]
    body = "<section class='panel'><h2 class='section-title'>注目情報 TOP12</h2><div class='grid'>" + ''.join(card_html(x) for x in top) + '</div></section>'
    return page_shell('注目情報', 'pickup.html', body, items)


def render_themes(items: list[dict]) -> str:
    grouped: dict[str, list[dict]] = defaultdict(list)
    for x in items:
        grouped[x['theme']].append(x)
    sections = []
    for theme in CATEGORY_ORDER:
        if not grouped.get(theme):
            continue
        cards = ''.join(card_html(x) for x in grouped[theme])
        sections.append(f"<section class='group'><h2 class='section-title'>{theme}</h2><div class='grid'>{cards}</div></section>")
    return page_shell('テーマ別', 'themes.html', ''.join(sections), items)


def render_troupe(items: list[dict]) -> str:
    grouped: dict[str, list[dict]] = defaultdict(list)
    for x in items:
        for troupe in x['troupes']:
            grouped[troupe].append(x)
    sections = []
    for troupe in TROUPES + ['全組共通']:
        if not grouped.get(troupe):
            continue
        cards = ''.join(card_html(x) for x in grouped[troupe])
        sections.append(f"<section class='group'><h2 class='section-title'>{troupe}</h2><div class='grid'>{cards}</div></section>")
    return page_shell('組別', 'troupe.html', ''.join(sections), items)


def render_schedule(items: list[dict]) -> str:
    schedules = [x for x in items if x['kind'] == 'schedule']
    body = "<section class='panel'><h2 class='section-title'>公演スケジュール</h2><div class='grid'>" + ''.join(card_html(x) for x in schedules) + '</div></section>'
    return page_shell('公演スケジュール', 'schedule.html', body, items)


def render_analysis(items: list[dict]) -> str:
    theme_counter = Counter(x['theme'] for x in items)
    troupe_counter = Counter(t for x in items for t in x['troupes'])
    source_counter = Counter(x['source'] for x in items)

    def rows(counter: Counter) -> str:
        return ''.join(f"<tr><td>{esc(k)}</td><td>{v}</td></tr>" for k, v in counter.most_common())

    body = f"""
    <section class='panel'><h2 class='section-title'>テーマ別件数</h2><table><thead><tr><th>テーマ</th><th>件数</th></tr></thead><tbody>{rows(theme_counter)}</tbody></table></section>
    <section class='panel'><h2 class='section-title'>組別件数</h2><table><thead><tr><th>組</th><th>件数</th></tr></thead><tbody>{rows(troupe_counter)}</tbody></table></section>
    <section class='panel'><h2 class='section-title'>取得元別件数</h2><table><thead><tr><th>取得元</th><th>件数</th></tr></thead><tbody>{rows(source_counter)}</tbody></table></section>
    """
    return page_shell('分析', 'analysis.html', body, items)


def write_pages(items: list[dict]) -> None:
    pages = {
        'index.html': render_index(items),
        'pickup.html': render_pickup(items),
        'themes.html': render_themes(items),
        'troupe.html': render_troupe(items),
        'schedule.html': render_schedule(items),
        'analysis.html': render_analysis(items),
    }
    for name, content in pages.items():
        (OUTPUT / name).write_text(content, encoding='utf-8')


def main() -> None:
    print('=' * 46)
    print(f'  {APP_NAME} v{APP_VERSION}')
    print('=' * 46)
    print()
    items = collect_all()
    db_save(items)
    write_pages(items)
    print()
    print('完了: HTML を生成しました')
    print(OUTPUT / 'index.html')


if __name__ == '__main__':
    main()
