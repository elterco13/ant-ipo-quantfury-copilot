"""Real-time market telemetry engine, auto-VWAP, news/SEC scanner, and direct Buy/Sell signal engine."""

import asyncio
from datetime import datetime, timezone
import json
import math
import re
import time
from typing import Any
from zoneinfo import ZoneInfo

import feedparser
import httpx

from alerts import AlertManager
from config import (
    ALPHABET_MTM_2026_USD,
    AMAZON_MTM_2026_USD,
    BINANCE_SYMBOL_MAP,
    ESTIMATED_NET_PROCEEDS_USD,
    IPO_DATE_ISO,
    PRICING_NIGHT_ISO,
    SECTORS,
    YAHOO_EQUITY_TICKERS,
)
from quantfury_risk import calculate_quantfury_plan
from sec_edgar import _extract_price_from_text, check_anthropic_filings, parse_424b4_text

MADRID_TZ = ZoneInfo("Europe/Madrid")
NY_TZ = ZoneInfo("America/New_York")

BINANCE_24HR_URL = "https://api.binance.com/api/v3/ticker/24hr"
YAHOO_CHART_URL = "https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
GOOGLE_NEWS_RSS_URL = (
    "https://news.google.com/rss/search?q=Anthropic+IPO+price+OR+Nasdaq&hl=en-US&gl=US&ceid=US:en"
)

FALLBACK_QUOTES: dict[str, dict[str, Any]] = {
    "QQQ": {"price": 512.40, "change_pct": 0.0, "source": "FALLBACK", "intraday_series": []},
    "GOOGL": {"price": 182.50, "change_pct": 0.0, "source": "FALLBACK", "intraday_series": []},
    "AMZN": {"price": 210.80, "change_pct": 0.0, "source": "FALLBACK", "intraday_series": []},
    "MSFT": {"price": 425.10, "change_pct": 0.0, "source": "FALLBACK", "intraday_series": []},
    "NVDA": {"price": 146.20, "change_pct": 0.0, "source": "FALLBACK", "intraday_series": []},
    "AVGO": {"price": 176.50, "change_pct": 0.0, "source": "FALLBACK", "intraday_series": []},
    "MU": {"price": 104.30, "change_pct": 0.0, "source": "FALLBACK", "intraday_series": []},
    "TSM": {"price": 192.40, "change_pct": 0.0, "source": "FALLBACK", "intraday_series": []},
    "AMD": {"price": 141.60, "change_pct": 0.0, "source": "FALLBACK", "intraday_series": []},
    "CRWV": {"price": 62.10, "change_pct": 0.0, "source": "FALLBACK", "intraday_series": []},
    "CRM": {"price": 328.90, "change_pct": 0.0, "source": "FALLBACK", "intraday_series": []},
    "NOW": {"price": 1030.00, "change_pct": 0.0, "source": "FALLBACK", "intraday_series": []},
    "ADBE": {"price": 510.20, "change_pct": 0.0, "source": "FALLBACK", "intraday_series": []},
    "WDAY": {"price": 265.40, "change_pct": 0.0, "source": "FALLBACK", "intraday_series": []},
    "TEAM": {"price": 248.00, "change_pct": 0.0, "source": "FALLBACK", "intraday_series": []},
    "CEG": {"price": 245.30, "change_pct": 0.0, "source": "FALLBACK", "intraday_series": []},
    "VST": {"price": 148.90, "change_pct": 0.0, "source": "FALLBACK", "intraday_series": []},
    "OKLO": {"price": 22.80, "change_pct": 0.0, "source": "FALLBACK", "intraday_series": []},
    "CCJ": {"price": 56.40, "change_pct": 0.0, "source": "FALLBACK", "intraday_series": []},
    "URA": {"price": 31.20, "change_pct": 0.0, "source": "FALLBACK", "intraday_series": []},
    "IREN": {"price": 12.10, "change_pct": 0.0, "source": "FALLBACK", "intraday_series": []},
    "CLSK": {"price": 14.50, "change_pct": 0.0, "source": "FALLBACK", "intraday_series": []},
    "BTC-USD": {"price": 68000.00, "change_pct": 0.0, "source": "FALLBACK", "intraday_series": []},
    "ETH-USD": {"price": 2650.00, "change_pct": 0.0, "source": "FALLBACK", "intraday_series": []},
    "SOL-USD": {"price": 175.00, "change_pct": 0.0, "source": "FALLBACK", "intraday_series": []},
    "TAO-USD": {"price": 460.00, "change_pct": 0.0, "source": "FALLBACK", "intraday_series": []},
    "RENDER-USD": {"price": 7.40, "change_pct": 0.0, "source": "FALLBACK", "intraday_series": []},
}

ASSET_IMPACT_PROFILES: dict[str, dict[str, str]] = {
    "ANT": {
        "name": "Anthropic",
        "sector": "1. Subyacente OPI",
        "bias": "ANT_CORE",
        "impact": "OPI principal ($80 ref). Choque entre FOMO y 316M acciones desbloqueadas el Día 1.",
    },
    "QQQ": {
        "name": "Nasdaq 100 ETF",
        "sector": "Benchmark Tech",
        "bias": "BULLISH_AI_CAPEX",
        "impact": "Termómetro general de apetito tecnológico y liquidez para absorber la OPI.",
    },
    "GOOGL": {
        "name": "Alphabet",
        "sector": "2. Hyperscalers",
        "bias": "BULLISH_HYPERSCALER_MTM",
        "impact": "Ganancia contable Mark-to-Market de $127.3B + retorno de parte de los $84.6B a Google Cloud.",
    },
    "AMZN": {
        "name": "Amazon",
        "sector": "2. Hyperscalers",
        "bias": "BULLISH_HYPERSCALER_MTM",
        "impact": "Ganancia contable Mark-to-Market de $62.8B + gasto masivo de Anthropic en AWS.",
    },
    "MSFT": {
        "name": "Microsoft",
        "sector": "2. Hyperscalers",
        "bias": "BULLISH_HYPERSCALER_MTM",
        "impact": "Socio en colocación privada ($26B) e infraestructura Azure para cómputo IA.",
    },
    "NVDA": {
        "name": "Nvidia",
        "sector": "3. Semiconductores IA",
        "bias": "BULLISH_AI_CAPEX",
        "impact": "Aportó $10B en colocación privada; debut fuerte valida su ciclo de capex IA.",
    },
    "AVGO": {
        "name": "Broadcom",
        "sector": "3. Semiconductores IA",
        "bias": "BULLISH_AI_CAPEX",
        "impact": "Aceleradores IA y networking; socio estratégico en colocación privada Clase N.",
    },
    "MU": {
        "name": "Micron",
        "sector": "3. Semiconductores IA",
        "bias": "BULLISH_AI_CAPEX",
        "impact": "Memoria HBM crítica para clústeres de entrenamiento e inferencia de Anthropic.",
    },
    "TSM": {
        "name": "TSMC",
        "sector": "3. Semiconductores IA",
        "bias": "BULLISH_AI_CAPEX",
        "impact": "Fundición clave de chips IA para Nvidia, AMD y Broadcom; socio colocación privada.",
    },
    "AMD": {
        "name": "AMD",
        "sector": "3. Semiconductores IA",
        "bias": "BULLISH_AI_CAPEX",
        "impact": "Proveedor alternativo de GPUs IA y socio directo en colocación privada.",
    },
    "CRWV": {
        "name": "CoreWeave",
        "sector": "3. Semiconductores IA",
        "bias": "BULLISH_AI_CAPEX",
        "impact": "Infraestructura cloud GPU de alta beta; muy sensible al éxito o fallo de ANT.",
    },
    "CRM": {
        "name": "Salesforce",
        "sector": "4. Software SaaS",
        "bias": "BEARISH_SEAT_COMPRESSION",
        "impact": "Amenaza directa de Claude Cowork (compresión de licencias por asiento).",
    },
    "NOW": {
        "name": "ServiceNow",
        "sector": "4. Software SaaS",
        "bias": "BEARISH_SEAT_COMPRESSION",
        "impact": "Riesgo de automatización de flujos empresariales por agentes autónomos.",
    },
    "ADBE": {
        "name": "Adobe",
        "sector": "4. Software SaaS",
        "bias": "BEARISH_SEAT_COMPRESSION",
        "impact": "Presión competitiva en flujos creativos y de conocimiento por IA agéntica.",
    },
    "WDAY": {
        "name": "Workday",
        "sector": "4. Software SaaS",
        "bias": "BEARISH_SEAT_COMPRESSION",
        "impact": "Software empresarial por usuario vulnerable a Seat Compression.",
    },
    "TEAM": {
        "name": "Atlassian",
        "sector": "4. Software SaaS",
        "bias": "BEARISH_SEAT_COMPRESSION",
        "impact": "Herramientas de equipo amenazadas por agentes de código y flujos Claude.",
    },
    "CEG": {
        "name": "Constellation Energy",
        "sector": "5. Energía y Nuclear",
        "bias": "BULLISH_NUCLEAR_PPA",
        "impact": "Energía nuclear firme para los $70.7B de compromisos de cómputo en 12M.",
    },
    "VST": {
        "name": "Vistra",
        "sector": "5. Energía y Nuclear",
        "bias": "BULLISH_NUCLEAR_PPA",
        "impact": "Generación eléctrica base para nuevos contratos PPA de centros de datos.",
    },
    "OKLO": {
        "name": "Oklo",
        "sector": "5. Energía y Nuclear",
        "bias": "BULLISH_NUCLEAR_PPA",
        "impact": "Reactores nucleares avanzados; alta sensibilidad al cuello de botella eléctrico.",
    },
    "CCJ": {
        "name": "Cameco",
        "sector": "5. Energía y Nuclear",
        "bias": "BULLISH_NUCLEAR_PPA",
        "impact": "Suministro de uranio impulsado por demanda nuclear de centros IA.",
    },
    "URA": {
        "name": "Uranium ETF",
        "sector": "5. Energía y Nuclear",
        "bias": "BULLISH_NUCLEAR_PPA",
        "impact": "Cesta global de uranio ligada al despliegue eléctrico de centros de datos IA.",
    },
    "BTC-USD": {
        "name": "Bitcoin",
        "sector": "6. Cripto y Mineros",
        "bias": "CRYPTO_LIQUIDITY_DRAIN",
        "impact": "Drenaje de liquidez minorista previo a OPI (precedente SpaceX: -19% antes de rebotar).",
    },
    "ETH-USD": {
        "name": "Ethereum",
        "sector": "6. Cripto y Mineros",
        "bias": "CRYPTO_LIQUIDITY_DRAIN",
        "impact": "Arrastre de flujo minorista hacia renta variable IA durante la semana OPI.",
    },
    "SOL-USD": {
        "name": "Solana",
        "sector": "6. Cripto y Mineros",
        "bias": "CRYPTO_LIQUIDITY_DRAIN",
        "impact": "Alta sensibilidad a salidas de capital especulativo minorista hacia ANT.",
    },
    "TAO-USD": {
        "name": "Bittensor",
        "sector": "6. Cripto y Mineros",
        "bias": "CRYPTO_LIQUIDITY_DRAIN",
        "impact": "Token cripto-IA expuesto a fuga temporal de capital especulativo hacia Anthropic.",
    },
    "RENDER-USD": {
        "name": "Render",
        "sector": "6. Cripto y Mineros",
        "bias": "CRYPTO_LIQUIDITY_DRAIN",
        "impact": "Token cripto-IA con rotación de corto plazo hacia acciones IA tradicionales.",
    },
    "IREN": {
        "name": "IREN",
        "sector": "6. Cripto y Mineros",
        "bias": "BULLISH_HPC_MINER",
        "impact": "Minero reconvertido a centros de datos HPC/IA; suele subir si ANT debuta fuerte.",
    },
    "CLSK": {
        "name": "CleanSpark",
        "sector": "6. Cripto y Mineros",
        "bias": "BULLISH_HPC_MINER",
        "impact": "Minero con exposición energética/HPC correlacionado positivamente con ciclo IA.",
    },
}


def _compute_intraday_metrics(result_obj: dict[str, Any]) -> dict[str, Any]:
    """Compute real intraday VWAP, opening print, HOD/LOD, vol ratio, and chart series from Yahoo bars."""
    meta = result_obj.get("meta", {})
    timestamps = result_obj.get("timestamp") or []
    quote = (result_obj.get("indicators", {}).get("quote") or [{}])[0]

    opens = quote.get("open") or []
    highs = quote.get("high") or []
    lows = quote.get("low") or []
    closes = quote.get("close") or []
    volumes = quote.get("volume") or []

    regular_start = (
        meta.get("currentTradingPeriod", {}).get("regular", {}).get("start")
    )

    all_bars: list[dict[str, Any]] = []
    for i, ts in enumerate(timestamps):
        c = closes[i] if i < len(closes) else None
        if c is None or c <= 0:
            continue
        o = opens[i] if i < len(opens) and opens[i] else c
        h = highs[i] if i < len(highs) and highs[i] else c
        l_val = lows[i] if i < len(lows) and lows[i] else c
        vol = float(volumes[i]) if i < len(volumes) and volumes[i] else 0.0
        typical = (float(h) + float(l_val) + float(c)) / 3.0
        all_bars.append(
            {
                "ts": float(ts),
                "open": float(o),
                "high": float(h),
                "low": float(l_val),
                "close": float(c),
                "typical": typical,
                "volume": vol,
            }
        )

    reg_bars = [b for b in all_bars if regular_start and b["ts"] >= float(regular_start)]
    active_bars = reg_bars if reg_bars else all_bars

    cum_pv = 0.0
    cum_vol = 0.0
    cum_p2v = 0.0
    pos_vols: list[float] = []
    series: list[dict[str, Any]] = []
    last_vwap: float | None = None
    last_std: float = 0.0

    for b in active_bars:
        w = b["volume"] if b["volume"] > 0 else 1.0
        if b["volume"] > 0:
            pos_vols.append(b["volume"])
        px = b["close"]
        typ = b["typical"]
        if any(x["volume"] > 0 for x in active_bars):
            if b["volume"] > 0:
                cum_pv += typ * b["volume"]
                cum_vol += b["volume"]
                cum_p2v += (typ * typ) * b["volume"]
            vwap_val = (cum_pv / cum_vol) if cum_vol > 0 else px
            var_val = max(0.0, (cum_p2v / cum_vol) - (vwap_val * vwap_val)) if cum_vol > 0 else 0.0
        else:
            cum_pv += px * w
            cum_vol += w
            cum_p2v += (px * px) * w
            vwap_val = cum_pv / cum_vol
            var_val = max(0.0, (cum_p2v / cum_vol) - (vwap_val * vwap_val))

        last_vwap = vwap_val
        last_std = math.sqrt(var_val)
        time_cet = datetime.fromtimestamp(b["ts"], tz=MADRID_TZ).strftime("%H:%M")
        series.append(
            {
                "t": time_cet,
                "p": round(px, 2),
                "v": round(vwap_val, 2),
            }
        )

    reg_price = meta.get("regularMarketPrice")
    last_px = float(active_bars[-1]["close"]) if active_bars else (float(reg_price) if reg_price else 0.0)
    prev_close = meta.get("chartPreviousClose") or meta.get("previousClose")
    opening_print = (
        float(meta["regularMarketOpen"])
        if meta.get("regularMarketOpen")
        else (active_bars[0]["open"] if active_bars else None)
    )
    open_ts = active_bars[0]["ts"] if active_bars else None

    vwap_final = round(last_vwap, 2) if last_vwap is not None else (last_px if last_px > 0 else None)
    vwap_std = round(last_std, 2)
    hod = max((b["high"] for b in active_bars), default=meta.get("regularMarketDayHigh") or last_px)
    lod = min((b["low"] for b in active_bars), default=meta.get("regularMarketDayLow") or last_px)

    vol_ratio = 1.0
    if len(pos_vols) >= 2:
        avg_v = sum(pos_vols[:-1]) / len(pos_vols[:-1])
        if avg_v > 0:
            vol_ratio = pos_vols[-1] / avg_v

    vs_vwap_pct = (
        round(((last_px - vwap_final) / vwap_final) * 100.0, 2)
        if (last_px > 0 and vwap_final and vwap_final > 0)
        else 0.0
    )
    verdict = (
        "ALCISTA (Sobre VWAP)"
        if vs_vwap_pct > 0.15
        else ("BAJISTA (Bajo VWAP)" if vs_vwap_pct < -0.15 else "NEUTRAL (En VWAP)")
    )

    pct = (
        ((last_px - float(prev_close)) / float(prev_close)) * 100.0
        if (prev_close and float(prev_close) > 0 and last_px > 0)
        else 0.0
    )

    return {
        "price": round(last_px, 2),
        "prev_close": round(float(prev_close), 2) if prev_close else None,
        "regular_price": round(float(reg_price), 2) if reg_price else round(last_px, 2),
        "change_pct": round(pct, 2),
        "opening_print": round(float(opening_print), 2) if opening_print else None,
        "opening_print_ts": open_ts,
        "vwap": round(float(vwap_final), 2) if vwap_final else None,
        "vwap_std": vwap_std,
        "hod": round(float(hod), 2) if hod else None,
        "lod": round(float(lod), 2) if lod else None,
        "vol_ratio": round(vol_ratio, 2),
        "last_volume": pos_vols[-1] if pos_vols else 0.0,
        "avg_1m_volume": round(sum(pos_vols) / len(pos_vols), 0) if pos_vols else 0.0,
        "vs_vwap_pct": vs_vwap_pct,
        "verdict": verdict,
        "intraday_series": series[-30:],
        "market_state": meta.get("marketState", "UNKNOWN"),
    }


class TelemetryEngine:
    """Orchestrates automatic live market data, SEC/news checks, VWAP analysis, and Buy/Sell signals."""

    def __init__(self, alert_manager: AlertManager) -> None:
        self.alerts = alert_manager
        self.quotes: dict[str, dict[str, Any]] = {
            sym: dict(val, symbol=sym, updated_at=None) for sym, val in FALLBACK_QUOTES.items()
        }
        self.sim_overrides: dict[str, float] = {}

        # Prospectus / Structural state (auto-updated when 424B4 or pricing headline publishes)
        self.prospectus: dict[str, Any] = parse_424b4_text(manual_price=80.0, greenshoe_exercised=True)
        self.sec_status: dict[str, Any] = {
            "checked_at": None,
            "source_status": "PENDING",
            "found_424b4": False,
            "found_s1": False,
            "filings": [],
        }
        self.news_headlines: list[dict[str, str]] = []
        self._last_slow_scan_ts: float = 0.0

        # ANT Auction & Trading Tape state (auto-fed from Yahoo ANT once IPO crosses)
        self.ant_state: dict[str, Any] = {
            "ant_live_trading": False,
            "ant_ticker_detected": False,
            "ant_short_name": "Anthropic, PBC",
            "phase_override": None,
            "quantfury_status": "NOT_LISTED_YET",
            "indication_low": None,
            "indication_high": None,
            "indication_history": [],
            "auction_signal": "ESPERANDO_SUBASTA",
            "opening_print": None,
            "opening_print_ts": None,
            "opening_print_cet": None,
            "current_price": None,
            "vwap": None,
            "vwap_std": 0.0,
            "hod": None,
            "lod": None,
            "last_volume": 0.0,
            "avg_1m_volume": 0.0,
            "vol_ratio": 0.0,
            "cum_pv": 0.0,
            "cum_vol": 0.0,
            "cum_p2v": 0.0,
            "ticks": [],
            "luld_halt_active": False,
            "luld_reason": None,
            "simulation_name": None,
        }

        self.quantfury_params: dict[str, Any] = {
            "account_equity_usd": 10000.0,
            "requested_leverage": 2.0,
            "expected_slippage_pct": 4.0,
            "luld_halt_gap_pct": 7.5,
        }

        self._running = False
        self._task: asyncio.Task[Any] | None = None
        self._last_fetch_summary: dict[str, Any] = {
            "binance": "PENDING",
            "yahoo": "PENDING",
            "ant_feed": "PENDING",
            "news": "PENDING",
        }

    async def start(self) -> None:
        """Start background 5-second telemetry loop + 60-second SEC/news scanner."""
        if self._running:
            return
        self._running = True
        await self.refresh_market_data()
        await self._refresh_sec_and_news()
        self._task = asyncio.create_task(self._loop())

    async def stop(self) -> None:
        """Stop background telemetry loop cleanly."""
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

    async def _loop(self) -> None:
        while self._running:
            await asyncio.sleep(5.0)
            try:
                await self.refresh_market_data()
                if (time.monotonic() - self._last_slow_scan_ts) >= 60.0:
                    await self._refresh_sec_and_news()
                await self.evaluate_alerts()
            except Exception:
                pass

    async def refresh_market_data(self) -> None:
        """Fetch live Binance crypto tickers, Yahoo Finance US equities + intraday VWAP, and ANT live."""
        async with httpx.AsyncClient(
            timeout=7.0,
            headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"},
            follow_redirects=True,
        ) as client:
            await asyncio.gather(
                self._fetch_binance(client),
                self._fetch_yahoo_all(client),
                self._fetch_ant_live(client),
            )

    async def _refresh_sec_and_news(self) -> None:
        """Auto-scan SEC EDGAR and Google News RSS for Anthropic IPO pricing updates."""
        self._last_slow_scan_ts = time.monotonic()
        try:
            sec_res = await check_anthropic_filings()
            self.sec_status = sec_res
            if sec_res.get("found_424b4"):
                await self.alerts.emit(
                    severity="SEC_424B4",
                    category="SEC_EDGAR",
                    title="Prospecto 424B4 de Anthropic Publicado",
                    message="SEC EDGAR detectó el formulario definitivo 424B4 de Anthropic.",
                    dedup_key="auto_sec_424b4",
                    cooldown_seconds=1800.0,
                )
        except Exception:
            pass

        try:
            async with httpx.AsyncClient(
                timeout=6.0,
                headers={"User-Agent": "Mozilla/5.0"},
                follow_redirects=True,
            ) as client:
                resp = await client.get(GOOGLE_NEWS_RSS_URL)
                if resp.status_code == 200:
                    feed = feedparser.parse(resp.text)
                    headlines: list[dict[str, str]] = []
                    for entry in feed.entries[:6]:
                        title = entry.get("title", "")
                        link = entry.get("link", "")
                        pub = entry.get("published", "")
                        headlines.append({"title": title, "link": link, "published": pub})

                        # Auto-extract IPO pricing ONLY on authoritative pricing verbs
                        if re.search(r"\b(prices|priced|sets final|fixes ipo)\b", title, re.IGNORECASE):
                            extracted = _extract_price_from_text(title)
                            if extracted and 60.0 <= extracted <= 120.0:
                                if abs(extracted - self.prospectus["final_ipo_price"]) >= 0.25:
                                    self.update_prospectus(raw_text=title, manual_price=extracted)
                    self.news_headlines = headlines
                    self._last_fetch_summary["news"] = f"OK ({len(headlines)} noticias)"
        except Exception as exc:
            self._last_fetch_summary["news"] = f"ERR: {exc}"

    async def _fetch_binance(self, client: httpx.AsyncClient) -> None:
        symbols = list(BINANCE_SYMBOL_MAP.values())
        reverse_map = {v: k for k, v in BINANCE_SYMBOL_MAP.items()}
        try:
            resp = await client.get(
                BINANCE_24HR_URL,
                params={"symbols": json.dumps(symbols, separators=(",", ":"))},
            )
            if resp.status_code == 200:
                items = resp.json()
                now_iso = datetime.now(timezone.utc).isoformat()
                for item in items:
                    b_sym = item.get("symbol")
                    key = reverse_map.get(b_sym)
                    if not key:
                        continue
                    price = float(item.get("lastPrice", 0.0))
                    pct = float(item.get("priceChangePercent", 0.0))
                    vol_usd = float(item.get("quoteVolume", 0.0))
                    verdict = (
                        "ALCISTA" if pct >= 0.5 else ("BAJISTA" if pct <= -0.5 else "NEUTRAL")
                    )
                    self.quotes[key] = {
                        "symbol": key,
                        "price": price,
                        "change_pct": round(pct, 2),
                        "volume_usd": round(vol_usd, 0),
                        "vwap": None,
                        "vs_vwap_pct": None,
                        "verdict": verdict,
                        "intraday_series": [],
                        "source": "LIVE_BINANCE",
                        "updated_at": now_iso,
                    }
                self._last_fetch_summary["binance"] = f"OK ({len(items)} pares)"
            else:
                self._last_fetch_summary["binance"] = f"HTTP_{resp.status_code}"
        except Exception as exc:
            self._last_fetch_summary["binance"] = f"ERR: {exc}"

    async def _fetch_yahoo_one(
        self, client: httpx.AsyncClient, sem: asyncio.Semaphore, symbol: str
    ) -> bool:
        async with sem:
            try:
                resp = await client.get(
                    YAHOO_CHART_URL.format(symbol=symbol),
                    params={"interval": "5m", "range": "1d", "includePrePost": "true"},
                )
                if resp.status_code != 200:
                    return False
                data = resp.json()
                result = data.get("chart", {}).get("result")
                if not result:
                    return False

                metrics = _compute_intraday_metrics(result[0])
                if metrics["price"] <= 0:
                    return False

                self.quotes[symbol] = {
                    "symbol": symbol,
                    **metrics,
                    "source": "LIVE_YAHOO",
                    "updated_at": datetime.now(timezone.utc).isoformat(),
                }
                return True
            except Exception:
                return False

    async def _fetch_yahoo_all(self, client: httpx.AsyncClient) -> None:
        sem = asyncio.Semaphore(8)
        results = await asyncio.gather(
            *(self._fetch_yahoo_one(client, sem, sym) for sym in YAHOO_EQUITY_TICKERS)
        )
        ok_count = sum(1 for r in results if r)
        self._last_fetch_summary["yahoo"] = f"OK ({ok_count}/{len(YAHOO_EQUITY_TICKERS)})"

    async def _fetch_ant_live(self, client: httpx.AsyncClient) -> None:
        """Auto-detect Anthropic (ANT) live debut on Yahoo Finance and populate VWAP/ticks automatically."""
        if self.ant_state.get("simulation_name"):
            self._last_fetch_summary["ant_feed"] = f"SIM ({self.ant_state['simulation_name']})"
            return

        try:
            resp = await client.get(
                YAHOO_CHART_URL.format(symbol="ANT"),
                params={"interval": "5m", "range": "1d", "includePrePost": "true"},
            )
            if resp.status_code != 200:
                self._last_fetch_summary["ant_feed"] = f"HTTP_{resp.status_code}"
                return

            result = resp.json().get("chart", {}).get("result")
            if not result:
                self._last_fetch_summary["ant_feed"] = "PRE-OPI (Sin datos)"
                return

            meta = result[0].get("meta", {})
            short_name = str(meta.get("shortName") or "")
            long_name = str(meta.get("longName") or "")
            is_anthropic = "anthropic" in short_name.lower() or "anthropic" in long_name.lower()
            self.ant_state["ant_ticker_detected"] = is_anthropic
            if short_name:
                self.ant_state["ant_short_name"] = short_name

            if not is_anthropic:
                self._last_fetch_summary["ant_feed"] = "Esperando ticker Anthropic"
                return

            metrics = _compute_intraday_metrics(result[0])
            series = metrics["intraday_series"]
            has_live_price = metrics["price"] > 0 and (len(series) > 0 or meta.get("regularMarketPrice"))

            if not has_live_price:
                self.ant_state["ant_live_trading"] = False
                self._last_fetch_summary["ant_feed"] = "ANT Registrado (Pre-Apertura)"
                return

            # Live ANT trades detected!
            prev_px = self.ant_state.get("current_price")
            new_px = metrics["price"]
            step_move_pct = abs((new_px - prev_px) / prev_px) * 100.0 if (prev_px and prev_px > 0) else 0.0

            self.ant_state["ant_live_trading"] = True
            if self.ant_state["quantfury_status"] == "NOT_LISTED_YET":
                self.ant_state["quantfury_status"] = "LONG_ONLY"

            op = metrics["opening_print"] or (series[0]["p"] if series else new_px)
            op_ts = metrics["opening_print_ts"] or time.time()
            op_cet = datetime.fromtimestamp(op_ts, tz=MADRID_TZ).strftime("%H:%M:%S")

            self.ant_state.update(
                {
                    "opening_print": op,
                    "opening_print_ts": op_ts,
                    "opening_print_cet": op_cet,
                    "current_price": new_px,
                    "vwap": metrics["vwap"],
                    "vwap_std": metrics["vwap_std"],
                    "hod": metrics["hod"],
                    "lod": metrics["lod"],
                    "last_volume": metrics["last_volume"],
                    "avg_1m_volume": metrics["avg_1m_volume"],
                    "vol_ratio": metrics["vol_ratio"],
                    "ticks": series,
                    "luld_halt_active": step_move_pct >= 5.0,
                    "luld_reason": (
                        f"Salto rápido de {step_move_pct:.1f}% en ANT: posible LULD Trading Halt."
                        if step_move_pct >= 5.0
                        else None
                    ),
                }
            )
            self._last_fetch_summary["ant_feed"] = f"EN VIVO (${new_px:.2f})"
        except Exception as exc:
            self._last_fetch_summary["ant_feed"] = f"ERR: {exc}"

    def _get_effective_quote(self, symbol: str) -> dict[str, Any]:
        base = dict(
            self.quotes.get(
                symbol,
                {"symbol": symbol, "price": 0.0, "change_pct": 0.0, "source": "N/A", "intraday_series": []},
            )
        )
        if symbol in self.sim_overrides:
            delta_pct = self.sim_overrides[symbol]
            orig_price = float(base.get("price") or 100.0)
            base["price"] = round(orig_price * (1.0 + delta_pct / 100.0), 2)
            base["change_pct"] = round(delta_pct, 2)
            base["source"] = "SIMULATED"
        return base

    def update_prospectus(
        self, raw_text: str = "", manual_price: float | None = None, greenshoe_exercised: bool = True
    ) -> dict[str, Any]:
        self.prospectus = parse_424b4_text(
            raw_text=raw_text,
            manual_price=manual_price,
            greenshoe_exercised=greenshoe_exercised,
        )
        return self.prospectus

    def update_auction_indication(self, low: float, high: float) -> dict[str, Any]:
        low_v = round(min(low, high), 2)
        high_v = round(max(low, high), 2)
        mid = round((low_v + high_v) / 2.0, 2)
        ipo_price = self.prospectus["final_ipo_price"]
        gap_pct = ((mid - ipo_price) / ipo_price) * 100.0
        now_cet = datetime.now(MADRID_TZ).strftime("%H:%M:%S")
        history = self.ant_state["indication_history"]
        prev_mid = history[-1]["mid"] if history else None
        history.append({"time_cet": now_cet, "low": low_v, "high": high_v, "mid": mid, "gap_pct": round(gap_pct, 2)})
        if len(history) > 30:
            history.pop(0)

        self.ant_state["indication_low"] = low_v
        self.ant_state["indication_high"] = high_v
        if gap_pct > 38.0 and prev_mid is not None and mid < prev_mid:
            signal = "BAJISTA_FOMO_RECORTE"
        elif gap_pct > 38.0:
            signal = "ALERTA_FOMO_VERTICAL"
        elif prev_mid is not None and mid >= prev_mid and 10.0 <= gap_pct <= 32.0:
            signal = "ALCISTA_ORDENADA"
        else:
            signal = "DESCUBRIMIENTO_SUBASTA"
        self.ant_state["auction_signal"] = signal
        return {
            "indication_low": low_v,
            "indication_high": high_v,
            "mid": mid,
            "gap_pct": round(gap_pct, 2),
            "auction_signal": signal,
        }

    def record_opening_print(self, price: float, volume: float = 2_500_000.0) -> dict[str, Any]:
        p = round(float(price), 2)
        v = max(float(volume), 1.0)
        now_ts = time.time()
        now_cet = datetime.now(MADRID_TZ).strftime("%H:%M:%S")
        self.ant_state.update(
            {
                "ant_live_trading": True,
                "opening_print": p,
                "opening_print_ts": now_ts,
                "opening_print_cet": now_cet,
                "current_price": p,
                "vwap": p,
                "vwap_std": 0.0,
                "hod": p,
                "lod": p,
                "last_volume": v,
                "avg_1m_volume": v,
                "vol_ratio": 1.0,
                "cum_pv": p * v,
                "cum_vol": v,
                "cum_p2v": (p * p) * v,
                "luld_halt_active": False,
                "luld_reason": None,
                "ticks": [
                    {
                        "ts": now_ts,
                        "time_cet": now_cet,
                        "price": p,
                        "volume": v,
                        "vwap": p,
                        "upper_1s": p,
                        "lower_1s": p,
                    }
                ],
            }
        )
        return self.ant_state

    def add_ant_tick(self, price: float, volume: float = 450_000.0, timestamp_override: float | None = None) -> dict[str, Any]:
        if self.ant_state["opening_print"] is None:
            return self.record_opening_print(price, volume)
        p = round(float(price), 2)
        v = max(float(volume), 1.0)
        prev_price = self.ant_state["current_price"] or p
        now_ts = timestamp_override or time.time()
        now_cet = datetime.fromtimestamp(now_ts, tz=MADRID_TZ).strftime("%H:%M:%S")

        cum_pv = self.ant_state["cum_pv"] + (p * v)
        cum_vol = self.ant_state["cum_vol"] + v
        cum_p2v = self.ant_state["cum_p2v"] + (p * p * v)

        vwap = cum_pv / cum_vol
        variance = max(0.0, (cum_p2v / cum_vol) - (vwap * vwap))
        std = math.sqrt(variance)

        ticks = self.ant_state["ticks"]
        prev_vols = [t["volume"] for t in ticks[-10:]]
        avg_vol = sum(prev_vols) / len(prev_vols) if prev_vols else v
        vol_ratio = v / avg_vol if avg_vol > 0 else 1.0

        step_move_pct = abs((p - prev_price) / prev_price) * 100.0 if prev_price > 0 else 0.0
        recent_prices = [t["price"] for t in ticks[-5:]] + [p]
        window_swing_pct = (
            ((max(recent_prices) - min(recent_prices)) / min(recent_prices)) * 100.0
            if min(recent_prices) > 0
            else 0.0
        )
        luld_active = step_move_pct >= 5.0 or window_swing_pct >= 8.0
        luld_reason = (
            f"Movimiento rápido ({max(step_move_pct, window_swing_pct):.1f}%): riesgo LULD Trading Halt (5 min)."
            if luld_active
            else None
        )

        self.ant_state["cum_pv"] = cum_pv
        self.ant_state["cum_vol"] = cum_vol
        self.ant_state["cum_p2v"] = cum_p2v
        self.ant_state["current_price"] = p
        self.ant_state["vwap"] = round(vwap, 2)
        self.ant_state["vwap_std"] = round(std, 2)
        self.ant_state["hod"] = round(max(self.ant_state["hod"] or p, p), 2)
        self.ant_state["lod"] = round(min(self.ant_state["lod"] or p, p), 2)
        self.ant_state["last_volume"] = v
        self.ant_state["avg_1m_volume"] = round(avg_vol, 0)
        self.ant_state["vol_ratio"] = round(vol_ratio, 2)
        self.ant_state["luld_halt_active"] = luld_active
        self.ant_state["luld_reason"] = luld_reason
        ticks.append(
            {
                "ts": now_ts,
                "time_cet": now_cet,
                "price": p,
                "volume": v,
                "vwap": round(vwap, 2),
                "upper_1s": round(vwap + std, 2),
                "lower_1s": round(vwap - std, 2),
            }
        )
        if len(ticks) > 120:
            ticks.pop(0)
        return self.ant_state

    async def run_scenario_simulation(self, scenario_name: str) -> dict[str, Any]:
        """Keep simulation endpoint functional for testing / MCP replay."""
        self.sim_overrides.clear()
        self.ant_state["simulation_name"] = scenario_name if scenario_name != "RESET" else None
        ipo_price = self.prospectus["final_ipo_price"]

        if scenario_name == "RESET":
            self.ant_state.update(
                {
                    "ant_live_trading": False,
                    "phase_override": None,
                    "quantfury_status": "NOT_LISTED_YET",
                    "indication_low": None,
                    "indication_high": None,
                    "indication_history": [],
                    "auction_signal": "ESPERANDO_SUBASTA",
                    "opening_print": None,
                    "opening_print_ts": None,
                    "opening_print_cet": None,
                    "current_price": None,
                    "vwap": None,
                    "vwap_std": 0.0,
                    "hod": None,
                    "lod": None,
                    "last_volume": 0.0,
                    "avg_1m_volume": 0.0,
                    "vol_ratio": 0.0,
                    "cum_pv": 0.0,
                    "cum_vol": 0.0,
                    "cum_p2v": 0.0,
                    "ticks": [],
                    "luld_halt_active": False,
                    "luld_reason": None,
                }
            )
            await self.refresh_market_data()
            return self.get_full_state()

        if scenario_name == "SCENARIO_A":
            self.ant_state["phase_override"] = "PHASE_3_LIVE"
            self.ant_state["quantfury_status"] = "LONG_ONLY"
            self.sim_overrides.update({"BTC-USD": -2.6, "NVDA": 2.4, "GOOGL": 2.8, "CRM": -2.1})
            base_ts = time.time() - 420
            open_p = round(ipo_price * 1.15, 2)
            self.record_opening_print(open_p, 2_800_000)
            self.ant_state["opening_print_ts"] = base_ts
            self.ant_state["ticks"][0]["ts"] = base_ts
            for idx, (px, vol) in enumerate(
                [(open_p * 0.992, 600_000), (open_p * 1.004, 710_000), (open_p * 1.045, 1_650_000), (open_p * 1.07, 2_100_000)],
                start=1,
            ):
                self.add_ant_tick(px, vol, timestamp_override=base_ts + idx * 60)

        elif scenario_name == "SCENARIO_B":
            self.ant_state["phase_override"] = "PHASE_3_LIVE"
            self.ant_state["quantfury_status"] = "FULL_LONG_SHORT"
            self.sim_overrides.update({"BTC-USD": -1.9, "NVDA": -0.7, "CRM": -2.8, "ADBE": -2.4})
            base_ts = time.time() - 660
            open_p = round(ipo_price * 1.41, 2)
            self.record_opening_print(open_p, 4_200_000)
            self.ant_state["opening_print_ts"] = base_ts
            self.ant_state["ticks"][0]["ts"] = base_ts
            for idx, (px, vol) in enumerate(
                [(open_p * 1.03, 1_200_000), (open_p * 0.97, 1_500_000), (open_p * 0.91, 2_400_000)],
                start=1,
            ):
                self.add_ant_tick(px, vol, timestamp_override=base_ts + idx * 80)

        elif scenario_name == "SCENARIO_C":
            self.ant_state["phase_override"] = "PHASE_3_LIVE"
            self.ant_state["quantfury_status"] = "FULL_LONG_SHORT"
            self.sim_overrides.update({"QQQ": -1.85, "NVDA": -3.4, "CRWV": -6.2, "BTC-USD": -4.2})
            base_ts = time.time() - 600
            open_p = round(ipo_price * 1.03, 2)
            self.record_opening_print(open_p, 2_100_000)
            self.ant_state["opening_print_ts"] = base_ts
            self.ant_state["ticks"][0]["ts"] = base_ts
            for idx, (px, vol) in enumerate([(ipo_price * 0.99, 1_200_000), (ipo_price * 0.95, 2_000_000)], start=1):
                self.add_ant_tick(px, vol, timestamp_override=base_ts + idx * 90)

        await self.evaluate_alerts()
        return self.get_full_state()

    def _detect_current_phase(self) -> dict[str, Any]:
        now_cet = datetime.now(MADRID_TZ)
        now_et = datetime.now(NY_TZ)
        date_str = now_cet.strftime("%Y-%m-%d")
        hm = now_cet.hour * 60 + now_cet.minute

        override = self.ant_state.get("phase_override")
        if override:
            code = override
        elif self.ant_state.get("ant_live_trading") and self.ant_state.get("opening_print") is not None:
            code = "PHASE_4_CLOSE" if hm >= 21 * 60 else "PHASE_3_LIVE"
        elif date_str <= PRICING_NIGHT_ISO:
            code = "PHASE_1_PRICING"
        elif date_str == IPO_DATE_ISO:
            if hm < 15 * 60 + 30:
                code = "PHASE_2_PREMARKET"
            elif hm < 17 * 60 + 30:
                code = "PHASE_2_AUCTION"
            elif hm < 21 * 60:
                code = "PHASE_3_LIVE"
            else:
                code = "PHASE_4_CLOSE"
        else:
            code = "PHASE_3_LIVE"

        labels = {
            "PHASE_1_PRICING": "Fase 1: Esta Noche · Fijación de Precio (22:00–01:00 CET)",
            "PHASE_2_PREMARKET": "Fase 2A: Pre-Mercado EE.UU. (14:00–15:30 CET)",
            "PHASE_2_AUCTION": "Fase 2B: Subasta IPO Cross en Nasdaq (15:30–17:15 CET)",
            "PHASE_3_LIVE": "Fase 3: Mercado Abierto · Señales en Vivo",
            "PHASE_4_CLOSE": "Fase 4: Última Hora Wall Street (21:00–22:00 CET)",
        }
        return {
            "code": code,
            "label": labels.get(code, code),
            "cet_time": now_cet.strftime("%Y-%m-%d %H:%M:%S CET"),
            "et_time": now_et.strftime("%Y-%m-%d %H:%M:%S ET"),
            "is_override": bool(override),
        }

    @staticmethod
    def _calc_levels(
        entry_price: float,
        vwap_ref: float,
        hod_ref: float,
        lod_ref: float,
        is_short: bool,
        slippage_frac: float,
        is_ant: bool = False,
        ipo_price: float = 80.0,
    ) -> tuple[float, float, float]:
        """Compute stop-loss, take-profit 1, and take-profit 2 uniformly for any asset."""
        if entry_price <= 0:
            return 0.0, 0.0, 0.0
        dec = 4 if entry_price < 2 else 2
        if is_short:
            raw_stop = max(hod_ref, vwap_ref, entry_price * 1.018) * 1.005
            stop_loss = round(raw_stop * (1.0 + slippage_frac), dec)
            risk_dist = max(entry_price * 0.012, stop_loss - entry_price)
            tp1 = round(max(0.01, entry_price - risk_dist * 1.5), dec)
            tp2_raw = ipo_price if is_ant and entry_price > ipo_price else (entry_price - risk_dist * 2.5)
            tp2 = round(max(0.01, tp2_raw), dec)
        else:
            raw_stop = min(lod_ref, vwap_ref, entry_price * 0.982) * 0.995
            stop_loss = round(max(0.01, raw_stop * (1.0 - slippage_frac)), dec)
            risk_dist = max(entry_price * 0.012, entry_price - stop_loss)
            if is_ant:
                tp1 = round(max(100.0, entry_price + risk_dist * 1.6), dec)
                tp2 = round(max(110.0, entry_price + risk_dist * 2.8), dec)
            else:
                tp1 = round(entry_price + risk_dist * 1.5, dec)
                tp2 = round(entry_price + risk_dist * 2.5, dec)
        return stop_loss, tp1, tp2

    def _evaluate_single_asset_signal(
        self,
        symbol: str,
        quote: dict[str, Any],
        matrix_signals: dict[str, Any],
        equity: float,
        safe_lev: float,
        slip_pct: float,
    ) -> dict[str, Any]:
        """Evaluate Buy/Sell/Wait signal, stop-loss, take-profit, and ANT impact thesis for any asset."""
        profile = ASSET_IMPACT_PROFILES.get(
            symbol,
            {"name": symbol, "sector": "Otros", "bias": "BULLISH_AI_CAPEX", "impact": "Correlación con flujo IA."},
        )
        bias = profile["bias"]
        price = float(quote.get("price") or 0.0)
        chg = float(quote.get("change_pct") or 0.0)
        vwap = quote.get("vwap")
        vs_vwap = float(quote.get("vs_vwap_pct") or 0.0)
        hod = float(quote.get("hod") or (price * 1.025))
        lod = float(quote.get("lod") or (price * 0.975))
        ipo_price = self.prospectus["final_ipo_price"]

        action = "ESPERAR"
        action_color = "AMBER"
        score = 35
        reason = profile["impact"]
        is_ant = symbol == "ANT"
        slip_frac = (slip_pct / 100.0) if is_ant else 0.008

        if is_ant:
            ant_live = bool(self.ant_state.get("ant_live_trading"))
            if not ant_live:
                action = "ESPERAR"
                action_color = "AMBER"
                score = 35
                reason = "En Pre-Estreno. Esperar el primer cruce oficial en Nasdaq + regla de 3–5 min."
                return {
                    **quote,
                    "name": profile["name"],
                    "sector_name": profile["sector"],
                    "impact_thesis": profile["impact"],
                    "action": action,
                    "action_color": action_color,
                    "confidence_score": score,
                    "entry_price": ipo_price,
                    "stop_loss": round(ipo_price * 0.92, 2),
                    "take_profit_1": 100.0,
                    "take_profit_2": 110.0,
                    "position_size_usd": round(equity * safe_lev, 2),
                    "position_shares": round((equity * safe_lev) / ipo_price, 2) if ipo_price > 0 else 0.0,
                    "reason": reason,
                }
            # Live ANT evaluation
            if vs_vwap > 0.1 and 5.0 <= chg <= 28.0:
                action, action_color = "COMPRAR (LONG)", "GREEN"
                score = min(98, int(76 + vs_vwap * 5))
                reason = f"Sobre VWAP (${vwap}) con gap moderado ({chg:+.1f}%): compradores institucionales dominan."
            elif vs_vwap < -0.1 and (chg >= 25.0 or price < ipo_price):
                action, action_color = "VENDER EN CORTO (SHORT)", "RED"
                score = min(98, int(78 + abs(vs_vwap) * 5))
                reason = f"Debajo de VWAP (${vwap}): presión vendedora de 316M desbloqueadas / rechazo de precio."
            else:
                reason = f"Consolidando cerca de VWAP (${vwap}). Esperar ruptura clara."

        elif bias in {"BULLISH_HYPERSCALER_MTM", "BULLISH_AI_CAPEX"}:
            min_chg = 0.15 if bias == "BULLISH_HYPERSCALER_MTM" else 0.25
            if vwap is not None and vs_vwap > 0 and chg >= min_chg:
                action, action_color = "COMPRAR (LONG)", "GREEN"
                score = min(96, int(70 + chg * 6 + vs_vwap * 8))
                reason = f"Sobre VWAP (${vwap}, {vs_vwap:+.2f}%) y en verde ({chg:+.2f}%). {profile['impact']}"
            elif vwap is not None and vs_vwap < 0 and chg <= -1.0:
                action, action_color = "VENDER EN CORTO (SHORT)", "RED"
                score = min(94, int(66 + abs(chg) * 6 + abs(vs_vwap) * 6))
                reason = f"Perdió su VWAP (${vwap}, {vs_vwap:+.2f}%) cayendo {chg:+.2f}%: rechazo temporal de capex/riesgo."
            else:
                reason = f"Lateral respecto a VWAP (${vwap}). {profile['impact']}"

        elif bias == "BEARISH_SEAT_COMPRESSION":
            if (vwap is not None and vs_vwap < -0.1) or chg <= -0.3:
                action, action_color = "VENDER EN CORTO (SHORT)", "RED"
                score = min(95, int(70 + abs(min(0.0, chg)) * 7 + abs(min(0.0, vs_vwap)) * 8))
                reason = f"Presión bajista bajo VWAP (${vwap}, {chg:+.2f}%). {profile['impact']}"
            elif vwap is not None and vs_vwap > 0.2 and chg >= 1.2:
                action, action_color = "COMPRAR (LONG)", "GREEN"
                score = min(85, int(62 + chg * 5))
                reason = f"Rebote fuerte (+{chg:.2f}%) recuperando VWAP (${vwap}) pese a presión SaaS."
            else:
                reason = f"En rango neutro ({chg:+.2f}%). Vigilar quiebre bajo VWAP (${vwap}) para corto Seat Compression."

        elif bias == "BULLISH_NUCLEAR_PPA":
            if vwap is not None and vs_vwap > 0 and chg > 0.0:
                action, action_color = "COMPRAR (LONG)", "GREEN"
                score = min(95, int(68 + chg * 5 + vs_vwap * 6))
                reason = f"Fuerza sobre VWAP (${vwap}, {chg:+.2f}%). {profile['impact']}"
            elif vwap is not None and vs_vwap < 0 and chg <= -1.2:
                action, action_color = "VENDER EN CORTO (SHORT)", "RED"
                score = min(88, int(64 + abs(chg) * 5))
                reason = f"Debilidad bajo VWAP (${vwap}, {chg:+.2f}%): toma de beneficios en utilities/uranio."
            else:
                reason = f"Sin ruptura sobre VWAP (${vwap}). {profile['impact']}"

        elif bias == "BULLISH_HPC_MINER":
            if vwap is not None and vs_vwap > 0 and chg > 0.3:
                action, action_color = "COMPRAR (LONG)", "GREEN"
                score = min(92, int(66 + chg * 4 + vs_vwap * 5))
                reason = f"Minero HPC sobre VWAP (${vwap}, {chg:+.2f}%). {profile['impact']}"
            elif vwap is not None and vs_vwap < 0 and chg <= -1.2:
                action, action_color = "VENDER EN CORTO (SHORT)", "RED"
                score = min(88, int(64 + abs(chg) * 4))
                reason = f"Debilidad bajo VWAP (${vwap}, {chg:+.2f}%) arrastrado por caída cripto."
            else:
                reason = f"Rango neutro ({chg:+.2f}%). {profile['impact']}"

        elif bias == "CRYPTO_LIQUIDITY_DRAIN":
            if chg <= -1.5:
                action, action_color = "VENDER EN CORTO (SHORT)", "RED"
                score = min(94, int(68 + abs(chg) * 4))
                reason = f"Fuga de liquidez activa ({chg:+.2f}%). {profile['impact']}"
            elif chg >= 1.5:
                action, action_color = "COMPRAR (LONG)", "GREEN"
                score = min(90, int(66 + chg * 4))
                reason = f"Rebote fuerte (+{chg:.2f}%) absorbiendo liquidez. {profile['impact']}"
            else:
                reason = f"Rango ({chg:+.2f}%). {profile['impact']}"

        is_short = "SHORT" in action
        vwap_ref = float(vwap) if vwap is not None else price
        stop_loss, tp1, tp2 = self._calc_levels(
            price, vwap_ref, hod, lod, is_short, slip_frac, is_ant=is_ant, ipo_price=ipo_price
        )
        notional = round(equity * safe_lev, 2)
        shares = round(notional / price, 4 if price < 20 else 2) if price > 0 else 0.0

        return {
            **quote,
            "name": profile["name"],
            "sector_name": profile["sector"],
            "impact_thesis": profile["impact"],
            "action": action,
            "action_color": action_color,
            "confidence_score": score,
            "entry_price": round(price, 4 if price < 2 else 2),
            "stop_loss": stop_loss,
            "take_profit_1": tp1,
            "take_profit_2": tp2,
            "position_size_usd": notional,
            "position_shares": shares,
            "reason": reason,
        }

    def _compute_intermarket_matrix(self) -> dict[str, Any]:
        sectors_out: dict[str, Any] = {}
        sector_avgs: dict[str, float] = {}
        all_evaluated: list[dict[str, Any]] = []

        equity = max(float(self.quantfury_params.get("account_equity_usd") or 10000.0), 100.0)
        safe_lev = min(float(self.quantfury_params.get("requested_leverage") or 2.0), 3.0)
        slip_pct = float(self.quantfury_params.get("expected_slippage_pct") or 4.0)

        ipo_price = self.prospectus["final_ipo_price"]
        ant_px = self.ant_state.get("current_price")
        ant_gap_pct = round(((ant_px - ipo_price) / ipo_price) * 100.0, 2) if ant_px else None

        qqq_raw = self._get_effective_quote("QQQ")
        btc_raw = self._get_effective_quote("BTC-USD")
        nvda_raw = self._get_effective_quote("NVDA")
        googl_raw = self._get_effective_quote("GOOGL")

        btc_pct = float(btc_raw.get("change_pct") or 0.0)
        nvda_pct = float(nvda_raw.get("change_pct") or 0.0)
        googl_pct = float(googl_raw.get("change_pct") or 0.0)
        qqq_pct = float(qqq_raw.get("change_pct") or 0.0)

        prelim_signals = {
            "btc_pct": btc_pct,
            "nvda_pct": nvda_pct,
            "googl_pct": googl_pct,
            "qqq_pct": qqq_pct,
        }

        for key, info in SECTORS.items():
            rows: list[dict[str, Any]] = []
            changes: list[float] = []
            for sym in info["tickers"]:
                if sym == "ANT":
                    ant_vwap = self.ant_state.get("vwap")
                    vs_v = round(((ant_px - ant_vwap) / ant_vwap) * 100.0, 2) if (ant_px and ant_vwap) else None
                    verdict = (
                        "ALCISTA (Sobre VWAP)" if (vs_v is not None and vs_v > 0.15)
                        else ("BAJISTA (Bajo VWAP)" if (vs_v is not None and vs_v < -0.15) else "PRE-ESTRENO")
                    )
                    ant_quote = {
                        "symbol": "ANT",
                        "price": ant_px if ant_px is not None else ipo_price,
                        "change_pct": ant_gap_pct if ant_gap_pct is not None else 0.0,
                        "vwap": ant_vwap,
                        "hod": self.ant_state.get("hod"),
                        "lod": self.ant_state.get("lod"),
                        "vs_vwap_pct": vs_v,
                        "verdict": verdict,
                        "intraday_series": [
                            {
                                "t": t.get("t") or t.get("time_cet", ""),
                                "p": t.get("p") if t.get("p") is not None else t.get("price"),
                                "v": t.get("v") if t.get("v") is not None else t.get("vwap"),
                            }
                            for t in (self.ant_state.get("ticks") or [])[-30:]
                        ],
                        "source": "LIVE_YAHOO" if self.ant_state.get("ant_live_trading") else "REF_OPI",
                    }
                    ev = self._evaluate_single_asset_signal("ANT", ant_quote, prelim_signals, equity, safe_lev, slip_pct)
                    rows.append(ev)
                    all_evaluated.append(ev)
                    if ant_gap_pct is not None:
                        changes.append(ant_gap_pct)
                elif sym in {"ANUU", "ANDD", "ANSS"}:
                    rows.append(
                        {
                            "symbol": sym,
                            "name": sym,
                            "price": None,
                            "change_pct": None,
                            "vwap": None,
                            "vs_vwap_pct": None,
                            "verdict": "NO LISTADO DÍA 1",
                            "action": "NO DISPONIBLE",
                            "action_color": "AMBER",
                            "confidence_score": 0,
                            "stop_loss": None,
                            "take_profit_1": None,
                            "reason": "ETF apalancado no disponible el Día 1 en Quantfury.",
                            "intraday_series": [],
                            "source": "ETF_FUTURO",
                        }
                    )
                else:
                    q = self._get_effective_quote(sym)
                    ev = self._evaluate_single_asset_signal(sym, q, prelim_signals, equity, safe_lev, slip_pct)
                    rows.append(ev)
                    all_evaluated.append(ev)
                    if isinstance(q.get("change_pct"), (int, float)):
                        changes.append(float(q["change_pct"]))

            avg_chg = round(sum(changes) / len(changes), 2) if changes else 0.0
            sector_avgs[key] = avg_chg
            sectors_out[key] = {
                "label": info["label"],
                "transmission": info["transmission"],
                "avg_change_pct": avg_chg,
                "tickers": rows,
            }

        qqq_eval = self._evaluate_single_asset_signal("QQQ", qqq_raw, prelim_signals, equity, safe_lev, slip_pct)
        all_evaluated.append(qqq_eval)

        crypto_to_ai_rotation = btc_pct <= -1.5 and (nvda_pct > 0 or googl_pct > 0)
        op = self.ant_state.get("opening_print")
        ant_intraday_pct = (
            ((ant_px - op) / op) * 100.0 if (ant_px is not None and op is not None and op > 0) else 0.0
        )
        pure_rotation_to_ant = ant_intraday_pct > 2.0 and btc_pct < 0 and nvda_pct < 0

        ai_proxy_avg = sector_avgs.get("SEMICONDUCTORES_IA", 0.0)
        saas_avg = sector_avgs.get("SAAS_TRADICIONAL", 0.0)
        seat_compression_spread = round((ant_gap_pct if ant_gap_pct is not None else ai_proxy_avg) - saas_avg, 2)

        # Rank active opportunities across all 28 assets
        active_ops = [a for a in all_evaluated if a["action"] in {"COMPRAR (LONG)", "VENDER EN CORTO (SHORT)"}]
        active_ops.sort(key=lambda x: x["confidence_score"], reverse=True)

        long_count = sum(1 for a in all_evaluated if a["action"] == "COMPRAR (LONG)")
        short_count = sum(1 for a in all_evaluated if a["action"] == "VENDER EN CORTO (SHORT)")
        wait_count = sum(1 for a in all_evaluated if a["action"] == "ESPERAR")

        # Pair trade: Best AI/Hyperscaler/Nuclear Long vs Weakest SaaS Short
        long_pool = (
            sectors_out["HYPERSCALERS"]["tickers"]
            + sectors_out["SEMICONDUCTORES_IA"]["tickers"]
            + sectors_out["ENERGIA_NUCLEAR"]["tickers"]
        )
        if self.ant_state.get("ant_live_trading"):
            long_pool = [sectors_out["ANT_SUBYACENTE"]["tickers"][0]] + long_pool
        short_pool = sectors_out["SAAS_TRADICIONAL"]["tickers"]

        best_long = max(
            long_pool,
            key=lambda x: (100 if x["action"] == "COMPRAR (LONG)" else 0) + float(x.get("change_pct") or 0) + float(x.get("vs_vwap_pct") or 0),
        )
        best_short = min(
            short_pool,
            key=lambda x: (-100 if x["action"] == "VENDER EN CORTO (SHORT)" else 0) + float(x.get("change_pct") or 0) + float(x.get("vs_vwap_pct") or 0),
        )
        pair_spread = round(float(best_long.get("change_pct") or 0) - float(best_short.get("change_pct") or 0), 2)
        pair_active = (best_long["action"] == "COMPRAR (LONG)" or float(best_long.get("change_pct") or 0) > 0) and (
            best_short["action"] == "VENDER EN CORTO (SHORT)" or float(best_short.get("change_pct") or 0) < 0
        )

        pair_trade = {
            "active": pair_active,
            "long_ticker": best_long["symbol"],
            "long_name": best_long["name"],
            "long_price": best_long["entry_price"],
            "long_chg": best_long["change_pct"],
            "long_stop": best_long["stop_loss"],
            "short_ticker": best_short["symbol"],
            "short_name": best_short["name"],
            "short_price": best_short["entry_price"],
            "short_chg": best_short["change_pct"],
            "short_stop": best_short["stop_loss"],
            "spread_pct": pair_spread,
            "summary": (
                f"Par Activo: LONG {best_long['symbol']} ({best_long['change_pct']:+.2f}%) + SHORT {best_short['symbol']} ({best_short['change_pct']:+.2f}%)"
                if pair_active
                else f"Par en vigilancia: mejor candidato IA ({best_long['symbol']} {best_long['change_pct']:+.2f}%) vs SaaS más débil ({best_short['symbol']} {best_short['change_pct']:+.2f}%)."
            ),
        }

        mtm_multiplier = (ant_px / ipo_price) if (ant_px and ipo_price > 0) else 1.0
        return {
            "benchmark_qqq": qqq_eval,
            "sectors": sectors_out,
            "sector_avgs": sector_avgs,
            "top_opportunities": active_ops[:10],
            "opportunity_counts": {
                "longs": long_count,
                "shorts": short_count,
                "waits": wait_count,
                "total_assets": len(all_evaluated),
            },
            "pair_trade": pair_trade,
            "signals": {
                "crypto_to_ai_rotation": crypto_to_ai_rotation,
                "pure_rotation_to_ant": pure_rotation_to_ant,
                "seat_compression_spread_pct": seat_compression_spread,
                "ant_intraday_vs_open_pct": round(ant_intraday_pct, 2),
                "ant_gap_vs_ipo_pct": ant_gap_pct,
                "btc_price": btc_raw.get("price"),
                "btc_pct": btc_pct,
                "nvda_pct": nvda_pct,
                "googl_pct": googl_pct,
                "qqq_pct": qqq_pct,
            },
            "hyperscaler_mtm": {
                "alphabet_2026_baseline_usd": ALPHABET_MTM_2026_USD,
                "alphabet_live_implied_usd": round(ALPHABET_MTM_2026_USD * mtm_multiplier, 0),
                "amazon_2026_baseline_usd": AMAZON_MTM_2026_USD,
                "amazon_live_implied_usd": round(AMAZON_MTM_2026_USD * mtm_multiplier, 0),
                "estimated_cloud_return_usd": ESTIMATED_NET_PROCEEDS_USD,
            },
        }

    def _classify_scenario(self, matrix: dict[str, Any]) -> dict[str, Any]:
        ipo_price = self.prospectus["final_ipo_price"]
        op = self.ant_state.get("opening_print")
        px = self.ant_state.get("current_price")
        vwap = self.ant_state.get("vwap")
        hod = self.ant_state.get("hod")
        vol_ratio = float(self.ant_state.get("vol_ratio") or 0.0)

        sig = matrix["signals"]
        nvda_pct = sig["nvda_pct"]
        qqq_pct = sig["qqq_pct"]
        googl_pct = sig["googl_pct"]

        if op is None or px is None or vwap is None:
            return {
                "code": "WAIT",
                "title": "Esperando Apertura Oficial de ANT en Nasdaq",
                "bias": "NEUTRAL",
                "confidence": "PRE-APERTURA",
                "checklist": [
                    f"Precio OPI de referencia: ${ipo_price:.2f}",
                    f"Bitcoin: ${sig['btc_price']:,.0f} ({sig['btc_pct']:+.2f}%) | NVDA: {nvda_pct:+.2f}%",
                    f"Spread IA vs SaaS tradicional: {sig['seat_compression_spread_pct']:+.2f}%",
                ],
                "action": "La app monitoriza en vivo SEC EDGAR, Binance y Wall Street. Actúa solo si salta una señal verde o roja arriba.",
            }

        open_gap_pct = ((op - ipo_price) / ipo_price) * 100.0
        hod_gap_pct = ((hod - ipo_price) / ipo_price) * 100.0 if hod else open_gap_pct

        if px < ipo_price or (qqq_pct <= -1.2 and nvda_pct <= -1.5):
            return {
                "code": "C",
                "title": "Escenario C: Contagio Macro / Busted IPO (SHORT)",
                "bias": "SHORT",
                "confidence": "ALTA" if px < ipo_price else "MEDIA",
                "checklist": [
                    f"ANT {'cayó debajo del precio de OPI ($' + str(ipo_price) + ')' if px < ipo_price else 'arrastrado por Nasdaq'}",
                    f"QQQ: {qqq_pct:+.2f}% | NVDA: {nvda_pct:+.2f}%",
                ],
                "action": "VENDER EN CORTO QQQ o NVDA en Quantfury para evitar parones (halts) individuales.",
            }

        if (open_gap_pct >= 30.0 and px < vwap) or (hod_gap_pct >= 25.0 and px < op and px < vwap):
            return {
                "code": "B",
                "title": "Escenario B: Agotamiento y Flipping (SHORT)",
                "bias": "SHORT",
                "confidence": "ALTA" if open_gap_pct >= 35.0 else "MEDIA",
                "checklist": [
                    f"Gap alto ({open_gap_pct:+.1f}%): empleados (316M desbloqueadas) y retail (165M) tomando beneficios.",
                    f"Precio (${px:.2f}) perdió la línea VWAP (${vwap:.2f}) y la apertura (${op:.2f}).",
                ],
                "action": "VENDER EN CORTO ANT (o CRM si Quantfury no permite corto en ANT).",
            }

        if 5.0 <= open_gap_pct <= 28.0 and px >= op * 0.995 and px >= vwap:
            confirm_macro = nvda_pct >= 0 or googl_pct >= 0
            return {
                "code": "A",
                "title": "Escenario A: Ruptura Institucional (LONG)",
                "bias": "LONG",
                "confidence": "ALTA" if (confirm_macro and vol_ratio >= 1.2) else "MEDIA",
                "checklist": [
                    f"Gap moderado ({open_gap_pct:+.1f}%): sin euforia extrema.",
                    f"Precio (${px:.2f}) sostenido encima de apertura (${op:.2f}) y VWAP (${vwap:.2f}).",
                ],
                "action": "COMPRAR ANT (LONG) en Quantfury con objetivo $100 / $110.",
            }

        return {
            "code": "WAIT",
            "title": "Consolidación — Esperar Confirmación Clara",
            "bias": "NEUTRAL",
            "confidence": "BAJA",
            "checklist": [
                f"Precio: ${px:.2f} vs VWAP: ${vwap:.2f} (Gap: {open_gap_pct:+.1f}%).",
            ],
            "action": "No entrar todavía. Esperar ruptura clara sobre VWAP o pérdida confirmada.",
        }

    def _compute_action_signal(
        self,
        phase: dict[str, Any],
        matrix: dict[str, Any],
        scenario: dict[str, Any],
    ) -> dict[str, Any]:
        """Translate all live market telemetry and S-1/A rules into one clear Buy/Sell/Wait order."""
        equity = max(float(self.quantfury_params.get("account_equity_usd") or 10000.0), 100.0)
        safe_lev = min(float(self.quantfury_params.get("requested_leverage") or 2.0), 3.0)
        slip_pct = float(self.quantfury_params.get("expected_slippage_pct") or 4.0)

        sig = matrix["signals"]
        sector_avgs = matrix["sector_avgs"]
        ipo_price = self.prospectus["final_ipo_price"]
        regime = self.prospectus["pricing_regime"]

        nvda = self._get_effective_quote("NVDA")
        googl = self._get_effective_quote("GOOGL")
        crm = self._get_effective_quote("CRM")
        qqq = self._get_effective_quote("QQQ")
        btc = self._get_effective_quote("BTC-USD")

        ant_live = bool(self.ant_state.get("ant_live_trading"))
        op_ts = self.ant_state.get("opening_print_ts")
        sec_since_open = round(time.time() - op_ts, 1) if (ant_live and op_ts) else None

        # Default decision values
        signal = "ESPERAR (NO ENTRAR AÚN)"
        signal_color = "AMBER"
        target_ticker = "ANT" if ant_live else "NVDA"
        is_proxy = not ant_live
        plain_headline = ""
        plain_reasons: list[str] = []
        when_to_close = "Esperar a que se active una señal confirmada antes de abrir operación."

        if ant_live:
            px = float(self.ant_state["current_price"])
            vwap = float(self.ant_state.get("vwap") or px)
            op = float(self.ant_state.get("opening_print") or px)
            hod = float(self.ant_state.get("hod") or px)
            lod = float(self.ant_state.get("lod") or px)

            if sec_since_open is not None and sec_since_open < 180:
                rem = int(180 - sec_since_open)
                signal = "ESPERAR (NO ENTRAR AÚN)"
                signal_color = "AMBER"
                target_ticker = "ANT"
                is_proxy = False
                plain_headline = f"ANT acaba de abrir a ${op:.2f}. Espera {rem}s (regla de 3–5 min) para no quedar atrapado en órdenes iniciales."
                plain_reasons = [
                    f"Primer cruce oficial en ${op:.2f} ({((op - ipo_price)/ipo_price)*100:+.1f}% frente a OPI de ${ipo_price:.2f}).",
                    "En los primeros 3–5 minutos se limpian las órdenes automáticas y se forma el nivel institucional VWAP.",
                    "Apenas termine la cuenta atrás, el sistema te dirá si COMPRAR o VENDER EN CORTO.",
                ]
                when_to_close = "Aún no debes abrir operación."
            elif scenario["code"] == "C":
                signal = "VENDER EN CORTO (SHORT)"
                signal_color = "RED"
                target_ticker = "QQQ"
                is_proxy = True
                plain_headline = "ALERTA BAJISTA: ANT cayó bajo precio de OPI o el Nasdaq arrastra a la IA. Vender en corto QQQ (más seguro)."
                plain_reasons = [
                    f"ANT está en ${px:.2f} (OPI ${ipo_price:.2f}) mientras QQQ cae {sig['qqq_pct']:+.2f}% y NVDA {sig['nvda_pct']:+.2f}%.",
                    "Operar QQQ en Quantfury evita los parones bruscos (halts) de ANT.",
                ]
                when_to_close = f"Cerrar el corto si QQQ recupera su VWAP (${qqq.get('vwap') or qqq['price']:.2f}) o al tocar Objetivo 1."
            elif scenario["code"] == "B":
                use_crm = self.ant_state["quantfury_status"] != "FULL_LONG_SHORT"
                target_ticker = "CRM" if use_crm else "ANT"
                is_proxy = use_crm
                signal = "VENDER EN CORTO (SHORT)"
                signal_color = "RED"
                plain_headline = (
                    f"AGOTAMIENTO Y VENTAS DE EMPLEADOS: Gap inicial muy alto y pérdida de VWAP. Abrir SHORT en {target_ticker}."
                )
                plain_reasons = [
                    f"ANT abrió muy arriba (${op:.2f}) activando ventas de las 316M acciones desbloqueadas y minoristas.",
                    f"El precio (${px:.2f}) cayó por debajo de la línea institucional VWAP (${vwap:.2f}).",
                    "Si Quantfury bloquea cortos en ANT, CRM (Salesforce) captura la rotación bajista contra el software clásico." if use_crm else "Short directo en ANT siguiendo la pérdida del VWAP.",
                ]
                when_to_close = f"Cerrar en pérdidas si ANT recupera con fuerza el máximo del día (${hod:.2f}); tomar ganancias cerca de ${ipo_price:.2f}."
            elif scenario["code"] == "A":
                signal = "COMPRAR (LONG)"
                signal_color = "GREEN"
                target_ticker = "ANT"
                is_proxy = False
                plain_headline = f"FUERZA INSTITUCIONAL: ANT defiende la línea azul VWAP (${vwap:.2f}). Señal de COMPRA (LONG) en ANT."
                plain_reasons = [
                    f"Apertura sana (${op:.2f}, {((op - ipo_price)/ipo_price)*100:+.1f}% sobre OPI) sin exceso de burbuja.",
                    f"El precio actual (${px:.2f}) está encima del VWAP (${vwap:.2f}): los grandes fondos están comprando.",
                    f"Semiconductores e Hyperscalers acompañan (NVDA {sig['nvda_pct']:+.2f}%, GOOGL {sig['googl_pct']:+.2f}%).",
                ]
                when_to_close = f"Vender / cerrar si el precio cae claro por debajo de ${min(lod, vwap):.2f}, o ir tomando ganancias en $100 y $110."
            elif phase["code"] == "PHASE_4_CLOSE" and px < vwap:
                signal = "CERRAR POSICIÓN / TOMAR GANANCIAS"
                signal_color = "BLUE"
                target_ticker = "ANT"
                is_proxy = False
                plain_headline = "ÚLTIMA HORA (21:00–22:00 CET) BAJO VWAP: Cierra posiciones largas antes del cierre de Wall Street."
                plain_reasons = [
                    f"ANT llega a la hora final por debajo del VWAP (${px:.2f} < ${vwap:.2f}).",
                    "Los traders intradía suelen cerrar antes de la campana acelerando ventas.",
                ]
                when_to_close = "Cerrar largos inmediatamente; no mantener riesgo nocturno bajo VWAP."
            else:
                signal = "ESPERAR (NO ENTRAR AÚN)"
                signal_color = "AMBER"
                target_ticker = "ANT"
                is_proxy = False
                plain_headline = f"ANT cotizando en ${px:.2f} sin ventaja clara respecto al VWAP (${vwap:.2f}). Esperar ruptura."
                plain_reasons = [
                    f"Precio actual: ${px:.2f} | Nivel institucional VWAP: ${vwap:.2f}.",
                    "Entraremos en COMPRA si rompe y se sostiene sobre VWAP con volumen, o en CORTO si pierde VWAP tras subida fuerte.",
                ]
                when_to_close = "Esperar confirmación antes de operar."
        else:
            # PRE-IPO / SUBASTA: evaluate live proxy opportunities automatically
            nvda_vs_vwap = float(nvda.get("vs_vwap_pct") or 0.0)
            crm_vs_vwap = float(crm.get("vs_vwap_pct") or 0.0)
            qqq_vs_vwap = float(qqq.get("vs_vwap_pct") or 0.0)
            ai_avg = sector_avgs.get("SEMICONDUCTORES_IA", 0.0)
            saas_avg = sector_avgs.get("SAAS_TRADICIONAL", 0.0)

            if sig["qqq_pct"] <= -1.2 and sig["nvda_pct"] <= -1.5 and nvda_vs_vwap < 0:
                signal = "VENDER EN CORTO (SHORT)"
                signal_color = "RED"
                target_ticker = "QQQ"
                is_proxy = True
                plain_headline = "DEBILIDAD PREVIA EN NASDAQ E IA: Mientras ANT no abre, la señal en vivo es CORTO en QQQ."
                plain_reasons = [
                    f"El Nasdaq (QQQ {sig['qqq_pct']:+.2f}%) y Nvidia (NVDA {sig['nvda_pct']:+.2f}%) están cayendo bajo su nivel institucional VWAP.",
                    "Según las reglas de contagio macro, evitar riesgo alcista hasta ver fortaleza.",
                ]
                when_to_close = f"Cerrar el corto en QQQ si supera su VWAP (${qqq.get('vwap') or qqq['price']:.2f}) o antes de la subasta de ANT."
            elif sig["crypto_to_ai_rotation"] and nvda_vs_vwap > 0:
                signal = "COMPRAR (LONG)"
                signal_color = "GREEN"
                target_ticker = "NVDA"
                is_proxy = True
                plain_headline = "ROTACIÓN BITCOIN → IA EN VIVO: Bitcoin cae y el dinero entra en Nvidia. COMPRAR NVDA en Quantfury."
                plain_reasons = [
                    f"Bitcoin baja ({sig['btc_pct']:+.2f}% a ${sig['btc_price']:,.0f}) mientras Nvidia sube ({sig['nvda_pct']:+.2f}%) sobre su VWAP (${nvda.get('vwap'):.2f}).",
                    "Como ANT aún no cotiza en Quantfury, NVDA es el proxy líquido inmediato.",
                ]
                when_to_close = f"Cerrar en pérdidas si NVDA cae bajo su VWAP (${nvda.get('vwap'):.2f}); tomar ganancias parciales al abrir ANT."
            elif ai_avg >= 0.5 and saas_avg <= -0.8 and crm_vs_vwap < 0:
                signal = "VENDER EN CORTO (SHORT)"
                signal_color = "RED"
                target_ticker = "CRM"
                is_proxy = True
                plain_headline = "COMPRESIÓN SAAS ACTIVA: El dinero huye del software tradicional hacia IA. CORTO en CRM (Salesforce)."
                plain_reasons = [
                    f"Los chips/IA suben de media {ai_avg:+.2f}% mientras el software clásico cae {saas_avg:+.2f}%.",
                    f"CRM cotiza debajo de su VWAP (${crm.get('vwap'):.2f}), confirmando presión vendedora.",
                ]
                when_to_close = f"Cerrar el corto si CRM recupera con fuerza su VWAP (${crm.get('vwap'):.2f})."
            else:
                signal = "ESPERAR (NO ENTRAR AÚN)"
                signal_color = "AMBER"
                target_ticker = "NVDA" if sig["nvda_pct"] >= sig["qqq_pct"] else "QQQ"
                is_proxy = True
                plain_headline = "ANT aún no ha debutado y los mercados no muestran un giro extremo todavía. Mantener liquidez lista."
                plain_reasons = [
                    f"Anthropic (ANT) sigue en fase previa a su estreno (precio OPI estimado ${ipo_price:.2f}).",
                    f"Bitcoin está en ${sig['btc_price']:,.0f} ({sig['btc_pct']:+.2f}%), QQQ en ${qqq['price']} ({sig['qqq_pct']:+.2f}%) y NVDA en ${nvda['price']} ({sig['nvda_pct']:+.2f}%).",
                    "La app cambiará automáticamente a COMPRAR o VENDER en cuanto salga el 1er cruce de ANT o se dispare la rotación Bitcoin → IA.",
                ]
                when_to_close = "Sin operación abierta. Espera el aviso sonoro/visual en pantalla."

        # Calculate live entry, stop-loss, and take-profit for the target_ticker
        if target_ticker == "ANT" and ant_live:
            entry_price = float(self.ant_state["current_price"])
            vwap_ref = float(self.ant_state.get("vwap") or entry_price)
            hod_ref = float(self.ant_state.get("hod") or entry_price)
            lod_ref = float(self.ant_state.get("lod") or entry_price)
            eff_slip = slip_pct / 100.0
        else:
            q_target = self._get_effective_quote(target_ticker)
            entry_price = float(q_target.get("price") or ipo_price)
            vwap_ref = float(q_target.get("vwap") or entry_price)
            hod_ref = float(q_target.get("hod") or entry_price * 1.02)
            lod_ref = float(q_target.get("lod") or entry_price * 0.98)
            eff_slip = 0.008  # Liquid US mega-cap / ETF proxy has minimal slippage

        stop_loss_price, take_profit_1, take_profit_2 = self._calc_levels(
            entry_price,
            vwap_ref,
            hod_ref,
            lod_ref,
            is_short=("VENDER EN CORTO" in signal),
            slippage_frac=eff_slip,
            is_ant=(target_ticker == "ANT"),
            ipo_price=ipo_price,
        )

        notional_usd = round(equity * safe_lev, 2)
        shares = round(notional_usd / entry_price, 2) if entry_price > 0 else 0.0

        # Choose live chart source: ANT ticks if ANT is live, otherwise live intraday candles of target_ticker
        if ant_live and self.ant_state["ticks"]:
            chart_symbol = "ANT"
            chart_label = "Anthropic (ANT) en Vivo vs VWAP Institucional"
            chart_series = [
                {
                    "t": t.get("t") or t.get("time_cet", ""),
                    "p": t.get("p") if t.get("p") is not None else t.get("price"),
                    "v": t.get("v") if t.get("v") is not None else t.get("vwap"),
                }
                for t in self.ant_state["ticks"][-30:]
            ]
            chart_vwap = self.ant_state.get("vwap")
            chart_open = self.ant_state.get("opening_print")
            chart_price = self.ant_state.get("current_price")
        else:
            q_chart = self._get_effective_quote(target_ticker)
            if not q_chart.get("intraday_series"):
                q_chart = self._get_effective_quote("NVDA")
                chart_symbol = "NVDA"
            else:
                chart_symbol = target_ticker
            chart_label = f"{chart_symbol} (Proxy en Vivo mientras debuta ANT) vs VWAP de Hoy"
            chart_series = q_chart.get("intraday_series") or []
            chart_vwap = q_chart.get("vwap")
            chart_open = q_chart.get("opening_print")
            chart_price = q_chart.get("price")

        # 6 Automatic Strategy Traffic Lights
        t_open = self.ant_state.get("opening_print")
        t_px = self.ant_state.get("current_price")
        t_vwap = self.ant_state.get("vwap")
        rules_status = [
            {
                "id": 1,
                "name": "Precio OPI & Trampa 316M",
                "light": "GREEN" if regime["code"] in {"OPTIMAL", "CONSERVATIVE"} else ("RED" if regime["code"] == "EXHAUSTION_DANGER" else "AMBER"),
                "status": f"${ipo_price:.2f} ({regime['code']})",
                "detail": f"481M acciones (316M empleados + 165M retail) listas para vender el Día 1. {'424B4 detectado en SEC' if self.sec_status.get('found_424b4') else 'Esperando 424B4 definitivo en SEC'}.",
            },
            {
                "id": 2,
                "name": "Termómetro Bitcoin → IA",
                "light": "GREEN" if sig["crypto_to_ai_rotation"] else ("RED" if sig["btc_pct"] < -2 and sig["nvda_pct"] < -1 else "AMBER"),
                "status": "ROTACIÓN ACTIVA" if sig["crypto_to_ai_rotation"] else "SIN ROTACIÓN CLARA",
                "detail": f"Bitcoin ${sig['btc_price']:,.0f} ({sig['btc_pct']:+.2f}%) vs Nvidia ({sig['nvda_pct']:+.2f}%) y Google ({sig['googl_pct']:+.2f}%).",
            },
            {
                "id": 3,
                "name": "IA vs Software Clásico (SaaS)",
                "light": "GREEN" if sig["seat_compression_spread_pct"] > 0.8 else ("RED" if sig["seat_compression_spread_pct"] < -0.8 else "AMBER"),
                "status": f"Diferencial {sig['seat_compression_spread_pct']:+.2f}%",
                "detail": f"Chips IA ({sector_avgs.get('SEMICONDUCTORES_IA', 0):+.2f}%) frente a CRM/ADBE/NOW ({sector_avgs.get('SAAS_TRADICIONAL', 0):+.2f}%).",
            },
            {
                "id": 4,
                "name": "Estreno ANT & Regla 3–5 Min",
                "light": "GREEN" if (ant_live and (sec_since_open or 999) >= 180) else ("RED" if (ant_live and (sec_since_open or 0) < 180) else "AMBER"),
                "status": "COTIZANDO LIBRE" if (ant_live and (sec_since_open or 999) >= 180) else ("ESPERANDO 3–5 MIN" if ant_live else "PRE-ESTRENO"),
                "detail": f"Primer cruce en ${t_open:.2f}" if t_open else "Nasdaq aún no ha realizado el primer cruce oficial de ANT.",
            },
            {
                "id": 5,
                "name": "Nivel Institucional (VWAP)",
                "light": "GREEN" if (chart_price and chart_vwap and chart_price >= chart_vwap) else ("RED" if (chart_price and chart_vwap and chart_price < chart_vwap) else "AMBER"),
                "status": f"{chart_symbol}: ${chart_price} vs VWAP ${chart_vwap}" if (chart_price and chart_vwap) else "Calculando VWAP",
                "detail": "Por encima de VWAP mandan los compradores; por debajo mandan los vendedores.",
            },
            {
                "id": 6,
                "name": "Seguridad & Halts (2x Seguro)",
                "light": "RED" if self.ant_state.get("luld_halt_active") else "GREEN",
                "status": "RIESGO HALT >=5%" if self.ant_state.get("luld_halt_active") else f"NORMAL (Uso {safe_lev:.1f}x)",
                "detail": self.ant_state.get("luld_reason") or "Volatilidad controlada. Apalancamiento limitado a max 2x–3x para absorber gaps.",
            },
        ]

        return {
            "signal": signal,
            "signal_color": signal_color,
            "target_ticker": target_ticker,
            "is_proxy": is_proxy,
            "entry_price": round(entry_price, 2),
            "stop_loss_price": stop_loss_price,
            "take_profit_1": take_profit_1,
            "take_profit_2": take_profit_2,
            "position_size_usd": notional_usd,
            "position_shares": shares,
            "effective_leverage": safe_lev,
            "plain_headline": plain_headline,
            "plain_reasons": plain_reasons,
            "when_to_close": when_to_close,
            "rules_status": rules_status,
            "live_chart": {
                "symbol": chart_symbol,
                "label": chart_label,
                "series": chart_series,
                "vwap": chart_vwap,
                "opening_print": chart_open,
                "current_price": chart_price,
                "ipo_price": ipo_price if chart_symbol == "ANT" else None,
            },
        }

    async def evaluate_alerts(self) -> None:
        matrix = self._compute_intermarket_matrix()
        sig = matrix["signals"]
        scenario = self._classify_scenario(matrix)
        phase = self._detect_current_phase()
        decision = self._compute_action_signal(phase, matrix, scenario)

        if sig["crypto_to_ai_rotation"]:
            await self.alerts.emit(
                severity="ROTATION",
                category="FLUJO_EN_VIVO",
                title="Rotación Bitcoin → Inteligencia Artificial",
                message=f"BTC cae {sig['btc_pct']:+.2f}% mientras NVDA ({sig['nvda_pct']:+.2f}%) sube.",
                dedup_key="crypto_to_ai",
                cooldown_seconds=300.0,
            )

        if self.ant_state.get("luld_halt_active"):
            await self.alerts.emit(
                severity="LULD_HALT",
                category="VOLATILIDAD",
                title="Alerta de Salto Brusco / Posible Halt en ANT",
                message=self.ant_state.get("luld_reason") or "Movimiento extremo >= 5% en ANT.",
                dedup_key="luld_halt",
                cooldown_seconds=60.0,
            )

        if decision["signal"] in {"COMPRAR (LONG)", "VENDER EN CORTO (SHORT)", "CERRAR POSICIÓN / TOMAR GANANCIAS"}:
            await self.alerts.emit(
                severity="SCENARIO_TRIGGER",
                category=decision["target_ticker"],
                title=f"{decision['signal']} en {decision['target_ticker']} @ ${decision['entry_price']}",
                message=f"{decision['plain_headline']} | Stop: ${decision['stop_loss_price']} | Obj 1: ${decision['take_profit_1']}",
                dedup_key=f"decision_{decision['signal']}_{decision['target_ticker']}",
                cooldown_seconds=180.0,
            )

    def get_full_state(self) -> dict[str, Any]:
        phase = self._detect_current_phase()
        matrix = self._compute_intermarket_matrix()
        scenario = self._classify_scenario(matrix)
        decision = self._compute_action_signal(phase, matrix, scenario)

        op_ts = self.ant_state.get("opening_print_ts")
        sec_since_open = round(time.time() - op_ts, 1) if (self.ant_state.get("ant_live_trading") and op_ts) else None

        proxy_prices = {
            sym: float(self._get_effective_quote(sym).get("price") or 0.0)
            for sym in ["NVDA", "GOOGL", "AMZN", "CRM", "ADBE", "QQQ"]
        }
        quantfury_plan = calculate_quantfury_plan(
            account_equity_usd=self.quantfury_params["account_equity_usd"],
            requested_leverage=self.quantfury_params["requested_leverage"],
            expected_slippage_pct=self.quantfury_params["expected_slippage_pct"],
            luld_halt_gap_pct=self.quantfury_params["luld_halt_gap_pct"],
            ant_status_in_quantfury=self.ant_state["quantfury_status"],
            active_scenario=scenario["code"],
            ipo_price=self.prospectus["final_ipo_price"],
            opening_print=self.ant_state.get("opening_print"),
            current_price=self.ant_state.get("current_price"),
            vwap=self.ant_state.get("vwap"),
            hod=self.ant_state.get("hod"),
            lod=self.ant_state.get("lod"),
            seconds_since_opening_print=sec_since_open,
            proxy_prices=proxy_prices,
        )

        return {
            "timestamp_utc": datetime.now(timezone.utc).isoformat(),
            "phase": phase,
            "fetch_status": self._last_fetch_summary,
            "prospectus": self.prospectus,
            "sec_status": self.sec_status,
            "news_headlines": self.news_headlines,
            "ant_tape": {
                **self.ant_state,
                "seconds_since_opening_print": sec_since_open,
            },
            "intermarket": matrix,
            "scenario": scenario,
            "decision": decision,
            "quantfury_plan": quantfury_plan,
            "alerts_config": self.alerts.get_public_config(),
            "recent_alerts": self.alerts.list_alerts(limit=25),
        }
