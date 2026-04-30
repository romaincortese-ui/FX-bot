from __future__ import annotations

import hashlib
import html
import json
import math
import re
import time
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from typing import Any, Iterable, Mapping, Sequence


DEFAULT_RSS_FEEDS: tuple[dict[str, str], ...] = (
    {"name": "Federal Reserve", "url": "https://www.federalreserve.gov/feeds/press_all.xml", "tier": "official"},
    {"name": "Bank of Japan", "url": "https://www.boj.or.jp/rss/whatsnew.xml", "tier": "official"},
    {"name": "ECB", "url": "https://www.ecb.europa.eu/rss/press.html", "tier": "official"},
    {"name": "Bank of England", "url": "https://www.bankofengland.co.uk/rss/news", "tier": "official"},
    {"name": "FXStreet", "url": "https://www.fxstreet.com/rss/news", "tier": "market"},
    {"name": "ForexLive", "url": "https://www.forexlive.com/feed/", "tier": "market"},
    {"name": "DailyFX", "url": "https://www.dailyfx.com/feeds/all", "tier": "market"},
)

SOURCE_QUALITY = {
    "official": 1.00,
    "wire": 0.90,
    "market": 0.72,
    "rss": 0.55,
}

CURRENCY_KEYWORDS: dict[str, tuple[str, ...]] = {
    "USD": ("usd", "dollar", "greenback", "fed", "fomc", "powell", "federal reserve", "treasury", "us yields"),
    "JPY": ("jpy", "yen", "boj", "bank of japan", "ueda", "japan mof", "ministry of finance", "intervention", "tokyo cpi"),
    "EUR": ("eur", "euro", "ecb", "lagarde", "eurozone"),
    "GBP": ("gbp", "sterling", "pound", "boe", "bank of england", "bailey"),
    "CHF": ("chf", "franc", "snb", "swiss national bank"),
    "CAD": ("cad", "canadian dollar", "boc", "bank of canada", "oil", "crude"),
    "AUD": ("aud", "aussie", "rba", "reserve bank of australia", "iron ore", "china"),
    "NZD": ("nzd", "kiwi", "rbnz", "reserve bank of new zealand", "dairy"),
}

SEVERITY_KEYWORDS: tuple[tuple[str, float], ...] = (
    ("intervention", 1.00),
    ("emergency", 1.00),
    ("unscheduled", 0.95),
    ("rate decision", 0.92),
    ("policy decision", 0.90),
    ("monetary policy", 0.88),
    ("press conference", 0.85),
    ("statement", 0.75),
    ("inflation", 0.72),
    ("cpi", 0.72),
    ("pce", 0.72),
    ("gdp", 0.70),
    ("employment", 0.68),
    ("payroll", 0.68),
    ("yields", 0.62),
    ("tariff", 0.60),
    ("geopolitical", 0.60),
)

STRENGTH_TERMS = (
    "hawkish", "hike", "hikes", "tightening", "higher rates", "inflation hot",
    "stronger", "strengthens", "rallies", "surges", "intervention", "safe haven",
)
WEAKNESS_TERMS = (
    "dovish", "cut", "cuts", "easing", "lower rates", "weakens", "falls",
    "slides", "sells off", "stimulus", "qe", "intervene to weaken",
)


@dataclass(frozen=True, slots=True)
class FeedItem:
    title: str
    summary: str
    url: str
    published_at: datetime
    source: str
    source_tier: str = "rss"

    @property
    def text(self) -> str:
        return f"{self.title} {self.summary}".strip()

    @property
    def title_hash(self) -> str:
        return hashlib.sha1(normalize_text(self.title).encode("utf-8")).hexdigest()[:16]


def normalize_text(value: str | None) -> str:
    value = html.unescape(value or "")
    value = re.sub(r"<[^>]+>", " ", value)
    value = re.sub(r"\s+", " ", value)
    return value.strip().lower()


def _parse_datetime(value: str | None, now: datetime | None = None) -> datetime:
    now = now or datetime.now(timezone.utc)
    if not value:
        return now
    text = value.strip().replace("Z", "+00:00")
    try:
        parsed = parsedate_to_datetime(text)
    except Exception:
        try:
            parsed = datetime.fromisoformat(text)
        except Exception:
            return now
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _child_text(node: ET.Element, names: Sequence[str]) -> str:
    for name in names:
        found = node.find(name)
        if found is not None and found.text:
            return found.text.strip()
        for child in node:
            if child.tag.lower().endswith(name.lower()) and child.text:
                return child.text.strip()
    return ""


def parse_feed_items(
    content: bytes | str,
    *,
    source_url: str,
    source_name: str | None = None,
    source_tier: str = "rss",
    now: datetime | None = None,
) -> list[FeedItem]:
    now = now or datetime.now(timezone.utc)
    raw = content.encode("utf-8") if isinstance(content, str) else content
    root = ET.fromstring(raw)
    nodes = root.findall(".//item") or root.findall(".//{http://www.w3.org/2005/Atom}entry") or root.findall(".//entry")
    items: list[FeedItem] = []
    for node in nodes:
        title = _child_text(node, ("title", "{http://www.w3.org/2005/Atom}title"))
        if not title:
            continue
        summary = _child_text(node, ("description", "summary", "content"))
        url = _child_text(node, ("link", "guid")) or source_url
        published = _child_text(node, ("pubDate", "published", "updated", "date"))
        if not url:
            link = node.find("{http://www.w3.org/2005/Atom}link") or node.find("link")
            if link is not None:
                url = link.attrib.get("href", "")
        items.append(
            FeedItem(
                title=html.unescape(title).strip(),
                summary=html.unescape(summary).strip(),
                url=url or source_url,
                published_at=_parse_datetime(published, now),
                source=source_name or source_url,
                source_tier=source_tier or "rss",
            )
        )
    return items


def parse_feed_config(raw_value: str | None) -> list[dict[str, str]]:
    if not raw_value or not raw_value.strip():
        return [dict(item) for item in DEFAULT_RSS_FEEDS]
    text = raw_value.strip()
    try:
        parsed = json.loads(text)
        if isinstance(parsed, list):
            feeds = []
            for item in parsed:
                if isinstance(item, str):
                    feeds.append({"name": item, "url": item, "tier": "rss"})
                elif isinstance(item, Mapping) and item.get("url"):
                    feeds.append({
                        "name": str(item.get("name") or item["url"]),
                        "url": str(item["url"]),
                        "tier": str(item.get("tier") or "rss"),
                    })
            if feeds:
                return feeds
    except Exception:
        pass
    feeds = []
    for part in re.split(r"[|,]\s*", text):
        url = part.strip()
        if url:
            feeds.append({"name": url, "url": url, "tier": "rss"})
    return feeds or [dict(item) for item in DEFAULT_RSS_FEEDS]


def classify_item(item: FeedItem) -> dict[str, Any]:
    text = normalize_text(item.text)
    currencies = [ccy for ccy, keywords in CURRENCY_KEYWORDS.items() if any(keyword in text for keyword in keywords)]
    severity = 0.0
    matched_terms = []
    for term, weight in SEVERITY_KEYWORDS:
        if term in text:
            severity = max(severity, weight)
            matched_terms.append(term)
    if any(keyword in text for keyword in ("boj", "fomc", "ecb", "boe", "snb", "boc", "rba", "rbnz")):
        severity = max(severity, 0.82)

    strength_hits = sum(1 for term in STRENGTH_TERMS if term in text)
    weakness_hits = sum(1 for term in WEAKNESS_TERMS if term in text)
    direction = "UNKNOWN"
    if strength_hits > weakness_hits:
        direction = "STRENGTH"
    elif weakness_hits > strength_hits:
        direction = "WEAKNESS"

    return {
        "currencies": currencies,
        "keyword_severity": round(severity, 3),
        "matched_terms": matched_terms[:8],
        "currency_direction": direction,
        "source_quality": SOURCE_QUALITY.get(item.source_tier, SOURCE_QUALITY["rss"]),
    }


def _baseline_for(previous_state: Mapping[str, Any] | None, currency: str) -> float:
    if not isinstance(previous_state, Mapping):
        return 0.0
    baselines = previous_state.get("baselines")
    if not isinstance(baselines, Mapping):
        return 0.0
    try:
        return max(0.0, float(baselines.get(currency, 0.0) or 0.0))
    except (TypeError, ValueError):
        return 0.0


def _previous_hashes(previous_state: Mapping[str, Any] | None) -> set[str]:
    if not isinstance(previous_state, Mapping):
        return set()
    hashes = previous_state.get("recent_title_hashes")
    if not isinstance(hashes, Sequence) or isinstance(hashes, (str, bytes)):
        return set()
    return {str(value) for value in hashes}


def _score_direction(classified_items: Sequence[tuple[FeedItem, dict[str, Any]]]) -> tuple[str, float]:
    strength = 0.0
    weakness = 0.0
    for _, data in classified_items:
        weight = float(data.get("keyword_severity", 0.0) or 0.0) + float(data.get("source_quality", 0.0) or 0.0) * 0.5
        if data.get("currency_direction") == "STRENGTH":
            strength += weight
        elif data.get("currency_direction") == "WEAKNESS":
            weakness += weight
    if strength == 0 and weakness == 0:
        return "UNKNOWN", 0.0
    total = strength + weakness
    confidence = abs(strength - weakness) / total if total > 0 else 0.0
    return ("STRENGTH" if strength > weakness else "WEAKNESS"), round(confidence, 3)


def build_event_intelligence_state(
    items: Sequence[FeedItem],
    *,
    previous_state: Mapping[str, Any] | None = None,
    now: datetime | None = None,
    lookback_minutes: int = 90,
    ttl_minutes: int = 120,
    min_score: float = 0.35,
) -> dict[str, Any]:
    now = now or datetime.now(timezone.utc)
    cutoff = now - timedelta(minutes=max(5, lookback_minutes))
    previous_hashes = _previous_hashes(previous_state)
    grouped: dict[str, list[tuple[FeedItem, dict[str, Any]]]] = {currency: [] for currency in CURRENCY_KEYWORDS}
    recent_hashes: list[str] = []

    for item in items:
        if item.published_at < cutoff or item.published_at > now + timedelta(minutes=10):
            continue
        data = classify_item(item)
        if not data["currencies"]:
            continue
        recent_hashes.append(item.title_hash)
        for currency in data["currencies"]:
            grouped.setdefault(currency, []).append((item, data))

    currencies: dict[str, dict[str, Any]] = {}
    baselines: dict[str, float] = {}
    for currency, classified in grouped.items():
        count = len(classified)
        old_baseline = _baseline_for(previous_state, currency)
        baseline = (0.85 * old_baseline + 0.15 * count) if old_baseline > 0 else float(count)
        baselines[currency] = round(baseline, 3)
        if count == 0:
            continue

        source_quality = max(float(data.get("source_quality", 0.0) or 0.0) for _, data in classified)
        keyword_severity = max(float(data.get("keyword_severity", 0.0) or 0.0) for _, data in classified)
        new_count = sum(1 for item, _ in classified if item.title_hash not in previous_hashes)
        novelty = new_count / max(1, count)
        zscore = (count - old_baseline) / math.sqrt(max(1.0, old_baseline)) if old_baseline > 0 else min(3.0, count / 2.0)
        volume_component = max(0.0, min(1.0, zscore / 4.0 if zscore > 0 else count / 8.0))
        event_score = (
            0.30 * volume_component
            + 0.25 * source_quality
            + 0.25 * keyword_severity
            + 0.20 * novelty
        )
        direction, direction_confidence = _score_direction(classified)
        confidence = round(min(1.0, 0.45 * source_quality + 0.35 * keyword_severity + 0.20 * direction_confidence), 3)

        if event_score < min_score and keyword_severity < 0.85:
            continue

        top_items = sorted(
            classified,
            key=lambda pair: (pair[1].get("keyword_severity", 0.0), pair[1].get("source_quality", 0.0), pair[0].published_at),
            reverse=True,
        )[:5]
        summary_titles = "; ".join(item.title[:120] for item, _ in top_items[:3])
        currencies[currency] = {
            "event_risk_score": round(min(1.0, event_score), 3),
            "headline_count": count,
            "headline_volume_zscore": round(zscore, 3),
            "source_quality_score": round(source_quality, 3),
            "keyword_severity_score": round(keyword_severity, 3),
            "novelty_score": round(novelty, 3),
            "direction_hint": direction,
            "direction_confidence": direction_confidence,
            "confidence": confidence,
            "source_summary": summary_titles,
            "expires_at": (now + timedelta(minutes=ttl_minutes)).isoformat(),
            "events": [
                {
                    "title": item.title,
                    "source": item.source,
                    "url": item.url,
                    "published_at": item.published_at.isoformat(),
                    "matched_terms": data.get("matched_terms", []),
                }
                for item, data in top_items
            ],
        }

    return {
        "generated_at": now.isoformat(),
        "expires_at": (now + timedelta(minutes=ttl_minutes)).isoformat(),
        "lookback_minutes": lookback_minutes,
        "ttl_minutes": ttl_minutes,
        "currencies": currencies,
        "baselines": baselines,
        "recent_title_hashes": sorted(set(recent_hashes))[-500:],
    }


def parse_timestamp(value: Any) -> datetime | None:
    if not isinstance(value, str) or not value.strip():
        return None
    return _parse_datetime(value)


def is_state_fresh(state: Mapping[str, Any] | None, *, now: datetime | None = None, grace_seconds: int = 0) -> bool:
    if not isinstance(state, Mapping):
        return False
    now = now or datetime.now(timezone.utc)
    expires_at = parse_timestamp(state.get("expires_at"))
    if expires_at is None:
        return False
    return now <= expires_at + timedelta(seconds=max(0, grace_seconds))


def _currency_signal(state: Mapping[str, Any], currency: str) -> dict[str, Any] | None:
    currencies = state.get("currencies") if isinstance(state, Mapping) else None
    if not isinstance(currencies, Mapping):
        return None
    raw = currencies.get(currency.upper())
    return raw if isinstance(raw, dict) else None


def event_signal_for_instrument(
    state: Mapping[str, Any] | None,
    instrument: str,
    *,
    now: datetime | None = None,
    min_score: float = 0.65,
    grace_seconds: int = 0,
) -> dict[str, Any] | None:
    if not is_state_fresh(state, now=now, grace_seconds=grace_seconds):
        return None
    if not instrument or "_" not in instrument:
        return None
    base, quote = instrument.upper().split("_", 1)
    candidates: list[tuple[str, dict[str, Any], str | None]] = []
    for currency, pair_side in ((base, "base"), (quote, "quote")):
        signal = _currency_signal(state or {}, currency)
        if not signal:
            continue
        try:
            score = float(signal.get("event_risk_score", 0.0) or 0.0)
        except (TypeError, ValueError):
            continue
        if score < min_score:
            continue
        ccy_direction = str(signal.get("direction_hint") or "UNKNOWN").upper()
        pair_direction: str | None = None
        if ccy_direction == "STRENGTH":
            pair_direction = "LONG" if pair_side == "base" else "SHORT"
        elif ccy_direction == "WEAKNESS":
            pair_direction = "SHORT" if pair_side == "base" else "LONG"
        candidates.append((currency, signal, pair_direction))

    if not candidates:
        return None

    best_currency, best_signal, best_pair_direction = max(
        candidates,
        key=lambda item: float(item[1].get("event_risk_score", 0.0) or 0.0),
    )
    return {
        "instrument": instrument.upper(),
        "event_currency": best_currency,
        "event_risk_score": float(best_signal.get("event_risk_score", 0.0) or 0.0),
        "confidence": float(best_signal.get("confidence", 0.0) or 0.0),
        "direction_hint": best_pair_direction or "UNKNOWN",
        "currency_direction_hint": best_signal.get("direction_hint", "UNKNOWN"),
        "source_summary": best_signal.get("source_summary", ""),
        "expires_at": best_signal.get("expires_at"),
        "events": best_signal.get("events", []),
    }