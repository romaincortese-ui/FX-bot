import os
from typing import Any, Mapping

try:
    from dotenv import find_dotenv, load_dotenv
except ImportError:
    find_dotenv = None  # type: ignore[assignment]
    load_dotenv = None  # type: ignore[assignment]


if load_dotenv is not None and find_dotenv is not None:
    dotenv_path = find_dotenv(usecwd=True)
    if dotenv_path:
        load_dotenv(dotenv_path, override=False)

from pydantic import BaseModel, ConfigDict, Field, ValidationError, model_validator


def env_str(name: str, default: str = "") -> str:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip()


def env_bool(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    text = value.strip().lower()
    if text in {"1", "true", "yes", "y", "on"}:
        return True
    if text in {"0", "false", "no", "n", "off"}:
        return False
    raise ValueError(f"Invalid boolean environment value for {name}: {value}")


def env_int(name: str, default: int) -> int:
    value = os.getenv(name)
    if value is None or value.strip() == "":
        return default
    try:
        return int(value)
    except ValueError as exc:
        raise ValueError(f"Invalid integer environment value for {name}: {value}") from exc


def env_float(name: str, default: float) -> float:
    value = os.getenv(name)
    if value is None or value.strip() == "":
        return default
    try:
        return float(value)
    except ValueError as exc:
        raise ValueError(f"Invalid float environment value for {name}: {value}") from exc


def env_csv(name: str, default: str) -> list[str]:
    value = os.getenv(name, default)
    return [item.strip() for item in value.split(",") if item.strip()]


class MainRuntimeConfig(BaseModel):
    model_config = ConfigDict(extra="ignore")

    fx_budget_allocation: float = Field(alias="FX_BUDGET_ALLOCATION", ge=0.0, le=1.0)
    gold_budget_allocation: float = Field(alias="GOLD_BUDGET_ALLOCATION", ge=0.0, le=1.0)
    scalper_allocation_pct: float = Field(alias="SCALPER_ALLOCATION_PCT", ge=0.0, le=1.0)
    trend_allocation_pct: float = Field(alias="TREND_ALLOCATION_PCT", ge=0.0, le=1.0)
    reversal_allocation_pct: float = Field(alias="REVERSAL_ALLOCATION_PCT", ge=0.0, le=1.0)
    breakout_allocation_pct: float = Field(alias="BREAKOUT_ALLOCATION_PCT", ge=0.0, le=1.0)
    max_risk_per_trade: float = Field(alias="MAX_RISK_PER_TRADE", ge=0.0, le=1.0)
    max_risk_per_pair: float = Field(alias="MAX_RISK_PER_PAIR", ge=0.0, le=1.0)
    max_total_exposure: float = Field(alias="MAX_TOTAL_EXPOSURE", ge=0.0, le=1.0)
    max_correlated_trades: int = Field(alias="MAX_CORRELATED_TRADES", ge=1)
    max_open_trades: int = Field(alias="MAX_OPEN_TRADES", ge=1)
    leverage: float = Field(alias="LEVERAGE", gt=0.0)
    daily_loss_limit_pct: float = Field(alias="DAILY_LOSS_LIMIT_PCT", ge=0.0, le=1.0)
    streak_loss_max: int = Field(alias="STREAK_LOSS_MAX", ge=1)
    session_loss_pause_pct: float = Field(alias="SESSION_LOSS_PAUSE_PCT", ge=0.0, le=1.0)
    session_loss_pause_mins: int = Field(alias="SESSION_LOSS_PAUSE_MINS", ge=1)
    pair_health_block_base_secs: int = Field(alias="PAIR_HEALTH_BLOCK_BASE_SECS", ge=1)
    pair_health_block_max_secs: int = Field(alias="PAIR_HEALTH_BLOCK_MAX_SECS", ge=1)
    pair_health_probe_interval_secs: int = Field(alias="PAIR_HEALTH_PROBE_INTERVAL_SECS", ge=1)
    scan_interval_base: int = Field(alias="SCAN_INTERVAL_BASE", ge=1)
    scan_interval_active: int = Field(alias="SCAN_INTERVAL_ACTIVE", ge=1)

    @model_validator(mode="after")
    def validate_allocations(self) -> "MainRuntimeConfig":
        sleeve_total = self.fx_budget_allocation + self.gold_budget_allocation
        if abs(sleeve_total - 1.0) > 0.01:
            raise ValueError(f"FX_BUDGET_ALLOCATION and GOLD_BUDGET_ALLOCATION must sum to 1.0, got {sleeve_total:.4f}")
        total = (
            self.scalper_allocation_pct
            + self.trend_allocation_pct
            + self.reversal_allocation_pct
            + self.breakout_allocation_pct
        )
        if abs(total - 1.0) > 0.01:
            raise ValueError(f"Core strategy allocations must sum to 1.0, got {total:.4f}")
        if self.max_risk_per_pair < self.max_risk_per_trade:
            raise ValueError("MAX_RISK_PER_PAIR must be >= MAX_RISK_PER_TRADE")
        if self.max_total_exposure < self.max_risk_per_pair:
            raise ValueError("MAX_TOTAL_EXPOSURE must be >= MAX_RISK_PER_PAIR")
        if self.pair_health_block_max_secs < self.pair_health_block_base_secs:
            raise ValueError("PAIR_HEALTH_BLOCK_MAX_SECS must be >= PAIR_HEALTH_BLOCK_BASE_SECS")
        if self.scan_interval_active > self.scan_interval_base:
            raise ValueError("SCAN_INTERVAL_ACTIVE should be <= SCAN_INTERVAL_BASE")
        return self


class MacroRuntimeConfig(BaseModel):
    model_config = ConfigDict(extra="ignore")

    rate_spread_threshold: float = Field(alias="RATE_SPREAD_THRESHOLD", ge=0.0)
    commodity_momentum_threshold: float = Field(alias="COMMODITY_MOMENTUM_THRESHOLD", ge=0.0)
    esi_threshold: float = Field(alias="ESI_THRESHOLD", ge=0.0)
    liquidity_risk_threshold: float = Field(alias="LIQUIDITY_RISK_THRESHOLD", ge=0.0)
    fx_index_momentum_threshold: float = Field(alias="FX_INDEX_MOMENTUM_THRESHOLD", ge=0.0)
    news_pause_before_minutes: int = Field(alias="NEWS_PAUSE_BEFORE_MINUTES", ge=0)
    news_cache_max_hours: int = Field(alias="NEWS_CACHE_MAX_HOURS", ge=1)
    default_economic_calendar_urls: list[str] = Field(alias="DEFAULT_ECONOMIC_CALENDAR_URLS")
    economic_calendar_url: str = Field(alias="ECONOMIC_CALENDAR_URL", min_length=1)

    @model_validator(mode="after")
    def validate_urls(self) -> "MacroRuntimeConfig":
        urls = [self.economic_calendar_url, *self.default_economic_calendar_urls]
        if not all(url.startswith("http") for url in urls):
            raise ValueError("Economic calendar URLs must be HTTP(S) URLs")
        return self


def validate_main_config(raw: Mapping[str, Any]) -> None:
    try:
        MainRuntimeConfig.model_validate(raw)
    except ValidationError as exc:
        raise ValueError(f"Main bot configuration invalid:\n{exc}") from exc


def validate_macro_config(raw: Mapping[str, Any]) -> None:
    try:
        MacroRuntimeConfig.model_validate(raw)
    except ValidationError as exc:
        raise ValueError(f"Macro engine configuration invalid:\n{exc}") from exc
