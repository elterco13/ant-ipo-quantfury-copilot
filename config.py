"""Central configuration and structural constants for the Anthropic (ANT) IPO Copilot."""

from pathlib import Path
from typing import Any

BASE_DIR = Path(__file__).resolve().parent
STATIC_DIR = BASE_DIR / "static"
DB_PATH = BASE_DIR / "copilot_state.db"

# SEC EDGAR settings
SEC_USER_AGENT = "AntigravityQuantCopilot/1.0 (ops@ant-copilot.local)"

# Structural IPO Share Counts & Economics (S-1/A)
BASE_SHARES = 825_000_000
NEW_SHARES = 801_350_000
COFOUNDER_SHARES = 23_650_000
GREENSHOE_SHARES = 123_750_000
UNLOCKED_INSIDER_SHARES = 316_000_000  # ~38.3% of base offer unlocked on Day 1
RETAIL_FLIPPING_SHARES = 165_000_000   # ~20% retail allocation (Fidelity, Robinhood, Schwab)

PRIVATE_PLACEMENT_USD = 26_000_000_000
PRIVATE_PLACEMENT_INVESTORS = {
    "Google": 10_000_000_000,
    "Nvidia": 10_000_000_000,
    "Resto (MSFT, AMD, AVGO, MU, TSM - Clase N)": 6_000_000_000,
}

S1_PRICE_RANGE = (70.0, 78.0)
WSJ_TARGET_PRICE = 80.0
FULLY_DILUTED_SHARES_APPROX = 18_500_000_000

# Fundamentals & Commitments
RUN_RATE_END_2025 = 9_000_000_000
RUN_RATE_OCT_2026 = 81_000_000_000
Q3_REV = 17_000_000_000
Q3_QOQ_GROWTH = 0.47
ENTERPRISE_CUSTOMERS_100K = 11_900
GROSS_MARGIN = 0.44
TRADITIONAL_SAAS_GROSS_MARGIN = 0.70
COMPUTE_COMMITMENTS_TOTAL = 900_900_000_000
COMPUTE_COMMITMENTS_12M = 70_700_000_000

# Hyperscaler Mark-to-Market 2026 & Net Proceeds
ALPHABET_MTM_2026_USD = 127_300_000_000
AMAZON_MTM_2026_USD = 62_800_000_000
ESTIMATED_NET_PROCEEDS_USD = 84_600_000_000

# SpaceX June 2026 Precedent Reference
SPACEX_PRECEDENT = {
    "event": "OPI SpaceX (Junio 2026)",
    "btc_pre_ipo_drawdown_pct": -19.0,
    "btc_bottom_days_after": 18,
    "btc_rebound_100d_pct": 28.0,
}

# Leveraged ETFs on ANT (operational context in Quantfury)
LEVERAGED_ETFS: list[dict[str, str]] = [
    {"ticker": "ANUU", "name": "Leverage Shares 2X Long ANT", "status": "Registrado - No día 1 en Quantfury"},
    {"ticker": "ANDD", "name": "Leverage Shares 2X Short ANT", "status": "Registrado - No día 1 en Quantfury"},
    {"ticker": "ANSS", "name": "Leverage Shares 1X Short ANT", "status": "Registrado - No día 1 en Quantfury"},
    {"ticker": "TRADR-2X", "name": "Tradr 2X Long/Short ANT (Cboe)", "status": "Lanzamiento 23 Nov 2026"},
]

# Intermarket Matrix Sectors & Tickers
SECTORS: dict[str, dict[str, Any]] = {
    "ANT_SUBYACENTE": {
        "label": "1. Anthropic (Subyacente & ETFs)",
        "tickers": ["ANT", "ANUU", "ANDD", "ANSS"],
        "transmission": "Choque FOMO vs 316M desbloqueadas. Gap > +30% eleva probabilidad de fade.",
    },
    "HYPERSCALERS": {
        "label": "2. Hyperscalers e Inversores Directos",
        "tickers": ["GOOGL", "AMZN", "MSFT"],
        "transmission": "Mark-to-Market trimestral (GOOGL $127.3B, AMZN $62.8B) + retorno de ~$84.6B a cloud.",
    },
    "SEMICONDUCTORES_IA": {
        "label": "3. Semiconductores e Infraestructura IA",
        "tickers": ["NVDA", "AVGO", "MU", "TSM", "AMD", "CRWV"],
        "transmission": "Validación post-resultados NVDA. Debut fuerte valida capex; fallo arrastra hardware.",
    },
    "SAAS_TRADICIONAL": {
        "label": "4. Software Empresarial (SaaS Tradicional)",
        "tickers": ["CRM", "NOW", "ADBE", "WDAY", "TEAM"],
        "transmission": "Amenaza Seat Compression por Claude Cowork. Par: Long ANT / Short cesta SaaS.",
    },
    "ENERGIA_NUCLEAR": {
        "label": "5. Energía, Utilities Nucleares y Uranio",
        "tickers": ["CEG", "VST", "OKLO", "CCJ", "URA"],
        "transmission": "Cuello de botella físico (GW firmes para $70.7B en 12M). Liquidez OPI impulsa PPAs.",
    },
    "CRIPTO_MINEROS": {
        "label": "6. Criptoactivos y Mineros BTC/HPC",
        "tickers": ["BTC-USD", "ETH-USD", "SOL-USD", "TAO-USD", "RENDER-USD", "IREN", "CLSK"],
        "transmission": "Drenaje minorista (~$68k BTC, precedente SpaceX). Tokens IA sufren fuga; mineros HPC siguen a ANT.",
    },
}

BENCHMARK_TICKERS = ["QQQ"]

BINANCE_SYMBOL_MAP: dict[str, str] = {
    "BTC-USD": "BTCUSDT",
    "ETH-USD": "ETHUSDT",
    "SOL-USD": "SOLUSDT",
    "TAO-USD": "TAOUSDT",
    "RENDER-USD": "RENDERUSDT",
}

YAHOO_EQUITY_TICKERS: list[str] = [
    "QQQ",
    "GOOGL", "AMZN", "MSFT",
    "NVDA", "AVGO", "MU", "TSM", "AMD", "CRWV",
    "CRM", "NOW", "ADBE", "WDAY", "TEAM",
    "CEG", "VST", "OKLO", "CCJ", "URA",
    "IREN", "CLSK",
]

# Quantfury operational constraints & proxies
QUANTFURY_DEFAULTS = {
    "max_recommended_leverage": 3.0,
    "default_leverage": 2.5,
    "min_slippage_pct": 3.0,
    "max_slippage_pct": 5.0,
    "luld_halt_threshold_pct": 5.0,
    "opening_wait_minutes": 4,  # 3-5 minutes rule after Opening Print
    "proxies": {
        "LONG_IA": ["NVDA", "GOOGL", "AMZN"],
        "SHORT_SAAS": ["CRM", "ADBE"],
        "SHORT_MACRO": ["QQQ", "NVDA"],
    },
}

# Chronological Phases (Madrid / CET)
IPO_DATE_ISO = "2026-11-19"
PRICING_NIGHT_ISO = "2026-11-18"

