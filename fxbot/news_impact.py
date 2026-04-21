"""Per-pair news-impact classifier (FX-bot Sprint 1 §2.5).

The current ``news.py`` applies a global time-based blackout to *every*
pair around any high-impact event. That is overly conservative — EUR/USD
during US NFP should halt, but AUD/NZD is largely unaffected by the same
release.

This module classifies an event against an instrument and returns one of
three impacts:

* ``BLOCK``  — hard-block trading (skip the bar).
* ``REDUCE`` — allow the trade but apply a 0.5× (configurable) risk
  multiplier.
* ``PASS``   — fully unaffected.

The classification is done from the event *title* (Forex Factory
convention) and the event *currency* code (``USD``, ``EUR``, etc.) using
a compact keyword map and a fallback rule by currency. The map is
deliberately conservative: anything not recognised defaults to ``BLOCK``
for pairs that touch the event currency and ``PASS`` otherwise — i.e.
the same behaviour as the legacy blackout on ambiguous events, never
more risky.
"""
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


class NewsImpact(str, Enum):
    BLOCK = "BLOCK"
    REDUCE = "REDUCE"
    PASS = "PASS"


@dataclass(frozen=True, slots=True)
class ImpactDecision:
    impact: NewsImpact
    reason: str


# Tier-1 events that drag *all* USD pairs and reduce commodity crosses.
_US_TIER1_KEYWORDS = (
    "non-farm",
    "non farm",
    "nfp",
    "fomc",
    "fed funds",
    "federal funds",
    "cpi",
    "core cpi",
    "ppi",
    "retail sales",
    "employment",
    "unemployment",
    "ism",
)

# Eurozone tier-1.
_EU_TIER1_KEYWORDS = (
    "ecb",
    "main refinanc",
    "deposit facility",
    "cpi",
    "hicp",
    "lagarde",
    "euro-zone",
    "eurozone",
)

# UK tier-1.
_UK_TIER1_KEYWORDS = (
    "boe",
    "bank of england",
    "mpc",
    "bank rate",
    "cpi",
    "gdp",
    "gilts",
    "bailey",
)

# Japan tier-1.
_JP_TIER1_KEYWORDS = (
    "boj",
    "bank of japan",
    "core cpi",
    "tankan",
    "ueda",
)

# Commodity-currency tier-1 (AUD/NZD/CAD).
_AU_TIER1_KEYWORDS = ("rba", "cash rate", "cpi", "employment")
_NZ_TIER1_KEYWORDS = ("rbnz", "ocr", "cpi")
_CA_TIER1_KEYWORDS = ("boc", "overnight rate", "cpi", "employment change")


def _matches(title: str, keywords: tuple[str, ...]) -> bool:
    lower = title.lower()
    return any(kw in lower for kw in keywords)


def _classify_event_family(title: str, currency: str) -> str | None:
    """Return the event *family* code for a known tier-1 event, else None.

    Family codes: ``"US_T1"``, ``"EU_T1"``, ``"UK_T1"``, ``"JP_T1"``,
    ``"AU_T1"``, ``"NZ_T1"``, ``"CA_T1"``.
    """
    currency = (currency or "").upper()
    lowered = title or ""
    if currency == "USD" and _matches(lowered, _US_TIER1_KEYWORDS):
        return "US_T1"
    if currency == "EUR" and _matches(lowered, _EU_TIER1_KEYWORDS):
        return "EU_T1"
    if currency == "GBP" and _matches(lowered, _UK_TIER1_KEYWORDS):
        return "UK_T1"
    if currency == "JPY" and _matches(lowered, _JP_TIER1_KEYWORDS):
        return "JP_T1"
    if currency == "AUD" and _matches(lowered, _AU_TIER1_KEYWORDS):
        return "AU_T1"
    if currency == "NZD" and _matches(lowered, _NZ_TIER1_KEYWORDS):
        return "NZ_T1"
    if currency == "CAD" and _matches(lowered, _CA_TIER1_KEYWORDS):
        return "CA_T1"
    return None


def _instrument_currencies(instrument: str) -> tuple[str, str] | None:
    if not instrument or "_" not in instrument:
        return None
    parts = instrument.upper().split("_")
    if len(parts) != 2:
        return None
    return parts[0], parts[1]


def classify_news_impact(
    *,
    event_title: str,
    event_currency: str,
    instrument: str,
) -> ImpactDecision:
    """Classify the effect of ``event`` on ``instrument``.

    Rules (from review memo §2.5):

    * US T1 → block all USD pairs; reduce AUD/NZD crosses; pass EUR/GBP
      and CHF crosses.
    * EU T1 → block all EUR pairs; reduce GBP crosses; pass USD/JPY and
      AUD/NZD.
    * UK T1 → block all GBP pairs; reduce EUR/GBP; pass USD/JPY and
      AUD/NZD.
    * JP T1 → block all JPY pairs; pass USD-majors ex-JPY.
    * AU T1 → block AUD pairs; reduce NZD pairs and commodity crosses;
      pass EUR majors.
    * NZ T1 → block NZD pairs; reduce AUD pairs.
    * CA T1 → block CAD pairs; reduce commodity crosses; pass others.
    * Unknown event → if the event currency matches either leg, BLOCK
      (legacy conservative default). Otherwise PASS.
    """
    pair = _instrument_currencies(instrument)
    if pair is None:
        return ImpactDecision(NewsImpact.PASS, "unknown_instrument_format")
    base, quote = pair
    event_ccy = (event_currency or "").upper()
    family = _classify_event_family(event_title or "", event_ccy)

    def _pair_contains(*codes: str) -> bool:
        return any(code in (base, quote) for code in codes)

    if family == "US_T1":
        if _pair_contains("USD"):
            return ImpactDecision(NewsImpact.BLOCK, "us_tier1_usd_leg")
        if _pair_contains("AUD", "NZD"):
            return ImpactDecision(NewsImpact.REDUCE, "us_tier1_commodity_cross")
        return ImpactDecision(NewsImpact.PASS, "us_tier1_unrelated")

    if family == "EU_T1":
        if _pair_contains("EUR"):
            return ImpactDecision(NewsImpact.BLOCK, "eu_tier1_eur_leg")
        if _pair_contains("GBP"):
            return ImpactDecision(NewsImpact.REDUCE, "eu_tier1_gbp_cross")
        return ImpactDecision(NewsImpact.PASS, "eu_tier1_unrelated")

    if family == "UK_T1":
        if _pair_contains("GBP"):
            return ImpactDecision(NewsImpact.BLOCK, "uk_tier1_gbp_leg")
        if _pair_contains("EUR") and ("EUR" in (base, quote) and "GBP" in (base, quote)):
            # EUR/GBP specifically — already handled by GBP block above.
            return ImpactDecision(NewsImpact.REDUCE, "uk_tier1_eur_gbp")
        return ImpactDecision(NewsImpact.PASS, "uk_tier1_unrelated")

    if family == "JP_T1":
        if _pair_contains("JPY"):
            return ImpactDecision(NewsImpact.BLOCK, "jp_tier1_jpy_leg")
        return ImpactDecision(NewsImpact.PASS, "jp_tier1_unrelated")

    if family == "AU_T1":
        if _pair_contains("AUD"):
            return ImpactDecision(NewsImpact.BLOCK, "au_tier1_aud_leg")
        if _pair_contains("NZD", "CAD"):
            return ImpactDecision(NewsImpact.REDUCE, "au_tier1_commodity_cross")
        return ImpactDecision(NewsImpact.PASS, "au_tier1_unrelated")

    if family == "NZ_T1":
        if _pair_contains("NZD"):
            return ImpactDecision(NewsImpact.BLOCK, "nz_tier1_nzd_leg")
        if _pair_contains("AUD"):
            return ImpactDecision(NewsImpact.REDUCE, "nz_tier1_aud_cross")
        return ImpactDecision(NewsImpact.PASS, "nz_tier1_unrelated")

    if family == "CA_T1":
        if _pair_contains("CAD"):
            return ImpactDecision(NewsImpact.BLOCK, "ca_tier1_cad_leg")
        if _pair_contains("AUD", "NZD"):
            return ImpactDecision(NewsImpact.REDUCE, "ca_tier1_commodity_cross")
        return ImpactDecision(NewsImpact.PASS, "ca_tier1_unrelated")

    # Unknown / lower-tier: conservative default — block if currency
    # touches either leg, pass otherwise.
    if event_ccy and event_ccy in (base, quote):
        return ImpactDecision(NewsImpact.BLOCK, "unclassified_event_currency_leg")
    return ImpactDecision(NewsImpact.PASS, "unclassified_event_unrelated")
