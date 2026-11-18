"""SEC EDGAR async ingestion and Form 424B4 / S-1/A prospectus analyzer."""

from datetime import datetime, timezone
import re
from typing import Any

import httpx

from config import (
    BASE_SHARES,
    COFOUNDER_SHARES,
    FULLY_DILUTED_SHARES_APPROX,
    GREENSHOE_SHARES,
    NEW_SHARES,
    RETAIL_FLIPPING_SHARES,
    S1_PRICE_RANGE,
    SEC_USER_AGENT,
    UNLOCKED_INSIDER_SHARES,
    WSJ_TARGET_PRICE,
)

EDGAR_SEARCH_URL = "https://efts.sec.gov/LATEST/search-index"
EDGAR_SUBMISSIONS_TEMPLATE = "https://data.sec.gov/submissions/CIK{cik}.json"


def classify_ipo_pricing_regime(price: float) -> dict[str, str]:
    """Classify the final IPO pricing into operational risk/upside regimes."""
    if price < S1_PRICE_RANGE[1]:
        return {
            "code": "CONSERVATIVE",
            "label": "Conservador / Descuento Institucional",
            "severity": "INFO",
            "interpretation": (
                f"Precio (${price:.2f}) dentro o debajo del rango S-1/A ($70–$78). "
                "Demanda más contenida o colocación disciplinada; margen amplio de recorrido si entra flujo."
            ),
        }
    if price <= 80.0:
        return {
            "code": "OPTIMAL",
            "label": "Zona Óptima ($78–$80)",
            "severity": "BULLISH",
            "interpretation": (
                f"Precio (${price:.2f}) entre $78 y $80: deja margen psicológico para que el mercado "
                "secundario busque los $90–$100 en la apertura."
            ),
        }
    if price < 84.0:
        return {
            "code": "DEMANDING",
            "label": "Zona Exigente ($80.01–$83.99)",
            "severity": "WARNING",
            "interpretation": (
                f"Precio (${price:.2f}) por encima de $80: refleja demanda >6x, pero reduce el colchón "
                "de apertura. Vigilar si el gap supera +30%."
            ),
        }
    return {
        "code": "EXHAUSTION_DANGER",
        "label": "Zona de Peligro / Exprimido ($84–$88+)",
        "severity": "CRITICAL",
        "interpretation": (
            f"Precio (${price:.2f}) en $84+: los bancos han exprimido casi todo el apetito previo; "
            "aumenta drásticamente el riesgo de techo rápido en el Día 1 y descarga de 316M desbloqueadas."
        ),
    }


def _extract_price_from_text(raw_text: str) -> float | None:
    """Extract initial public offering price per share from raw 424B4 text or news headlines."""
    patterns = [
        r"(?:initial public offering price|offering price per share|price to public)\s*(?:of|is|at|:)?\s*\$\s*(\d+(?:\.\d{1,2})?)",
        r"(?:prices?|priced|sets?|fixing)\s+(?:anthropic\s+)?(?:ipo\s+)?(?:at|above|near)\s*\$\s*(\d+(?:\.\d{1,2})?)",
        r"\$\s*(\d+(?:\.\d{1,2})?)\s*(?:per\s*(?:Class\s*A\s*)?(?:common\s*)?share|a\s*share|ipo\s*price)",
        r"final\s*price\s*(?:of|=|:|at)?\s*\$\s*(\d+(?:\.\d{1,2})?)",
    ]
    for pat in patterns:
        match = re.search(pat, raw_text, flags=re.IGNORECASE)
        if match:
            val = float(match.group(1))
            if 20.0 <= val <= 300.0:
                return val
    return None


def _extract_greenshoe_from_text(raw_text: str, default: bool) -> bool:
    """Detect whether the 123.75M greenshoe option was exercised from filing text."""
    lower = raw_text.lower()
    if re.search(r"greenshoe\s*(?:not|no)\s*exercised|over-allotment\s*(?:not|un)exercised", lower):
        return False
    if re.search(r"greenshoe\s*exercised|over-allotment\s*(?:option\s*)?(?:has\s*been\s*)?exercised", lower):
        return True
    return default


def parse_424b4_text(
    raw_text: str = "",
    manual_price: float | None = None,
    greenshoe_exercised: bool = True,
) -> dict[str, Any]:
    """Parse 424B4 text (or manual override) and compute exact Day 1 float & valuation metrics."""
    extracted_price = _extract_price_from_text(raw_text) if raw_text.strip() else None
    final_price = (
        round(float(manual_price), 2)
        if manual_price is not None
        else (round(extracted_price, 2) if extracted_price is not None else WSJ_TARGET_PRICE)
    )
    greenshoe_flag = _extract_greenshoe_from_text(raw_text, greenshoe_exercised) if raw_text.strip() else greenshoe_exercised

    gs_shares = GREENSHOE_SHARES if greenshoe_flag else 0
    total_ipo_offer_shares = BASE_SHARES + gs_shares
    day1_total_free_float = total_ipo_offer_shares + UNLOCKED_INSIDER_SHARES
    day1_immediate_sell_pool = UNLOCKED_INSIDER_SHARES + RETAIL_FLIPPING_SHARES

    new_capital_shares = NEW_SHARES + gs_shares
    gross_proceeds_usd = new_capital_shares * final_price
    implied_fdv_usd = FULLY_DILUTED_SHARES_APPROX * final_price
    regime = classify_ipo_pricing_regime(final_price)

    return {
        "final_ipo_price": final_price,
        "price_source": "424B4_TEXT" if extracted_price is not None and manual_price is None else "MANUAL_OR_WSJ",
        "greenshoe_exercised": greenshoe_flag,
        "base_shares": BASE_SHARES,
        "new_shares": NEW_SHARES,
        "cofounder_shares": COFOUNDER_SHARES,
        "greenshoe_shares": gs_shares,
        "total_ipo_offer_shares": total_ipo_offer_shares,
        "unlocked_insider_shares": UNLOCKED_INSIDER_SHARES,
        "retail_flipping_shares": RETAIL_FLIPPING_SHARES,
        "day1_total_free_float": day1_total_free_float,
        "day1_immediate_sell_pool": day1_immediate_sell_pool,
        "unlocked_vs_base_pct": round((UNLOCKED_INSIDER_SHARES / BASE_SHARES) * 100, 1),
        "retail_vs_base_pct": round((RETAIL_FLIPPING_SHARES / BASE_SHARES) * 100, 1),
        "immediate_sell_vs_float_pct": round((day1_immediate_sell_pool / day1_total_free_float) * 100, 1),
        "gross_proceeds_usd": round(gross_proceeds_usd, 2),
        "implied_fdv_usd": round(implied_fdv_usd, 2),
        "pricing_regime": regime,
        "parsed_at": datetime.now(timezone.utc).isoformat(),
    }


async def check_anthropic_filings(cik: str | None = None, timeout: float = 8.0) -> dict[str, Any]:
    """Query SEC EDGAR public APIs for Anthropic S-1, S-1/A, 424B4, and EFFECT filings."""
    headers = {
        "User-Agent": SEC_USER_AGENT,
        "Accept": "application/json",
    }
    filings: list[dict[str, str]] = []
    found_424b4 = False
    found_s1 = False
    source_status = "OK"
    error_detail: str | None = None

    try:
        async with httpx.AsyncClient(timeout=timeout, headers=headers, follow_redirects=True) as client:
            if cik:
                clean_cik = cik.strip().zfill(10)
                resp = await client.get(EDGAR_SUBMISSIONS_TEMPLATE.format(cik=clean_cik))
                if resp.status_code == 200:
                    data = resp.json()
                    recent = data.get("filings", {}).get("recent", {})
                    forms = recent.get("form", [])
                    dates = recent.get("filingDate", [])
                    acc_nums = recent.get("accessionNumber", [])
                    docs = recent.get("primaryDocument", [])
                    entity_name = data.get("name", f"CIK {clean_cik}")
                    for form, fdate, acc, doc in zip(forms[:25], dates[:25], acc_nums[:25], docs[:25]):
                        if form in {"424B4", "S-1/A", "S-1", "EFFECT", "8-A12B"}:
                            acc_no_dash = acc.replace("-", "")
                            doc_url = f"https://www.sec.gov/Archives/edgar/data/{int(clean_cik)}/{acc_no_dash}/{doc}"
                            filings.append(
                                {
                                    "entity": entity_name,
                                    "form": form,
                                    "filing_date": fdate,
                                    "accession": acc,
                                    "url": doc_url,
                                }
                            )
                            if form == "424B4":
                                found_424b4 = True
                            if form.startswith("S-1"):
                                found_s1 = True
                else:
                    source_status = f"HTTP_{resp.status_code}"
                    error_detail = f"SEC submissions returned status {resp.status_code}"
            else:
                params = {
                    "q": '"Anthropic"',
                    "forms": "424B4,S-1/A,S-1,EFFECT",
                }
                resp = await client.get(EDGAR_SEARCH_URL, params=params)
                if resp.status_code == 200:
                    data = resp.json()
                    hits = data.get("hits", {}).get("hits", [])
                    for hit in hits[:15]:
                        src = hit.get("_source", {})
                        form = src.get("form", "")
                        display_names = src.get("display_names", ["Anthropic"])
                        entity = display_names[0] if display_names else "Anthropic"
                        fdate = src.get("file_date", "")
                        ciks = src.get("ciks", [])
                        adsh = src.get("adsh", "")
                        doc_id = hit.get("_id", "")
                        file_part = doc_id.split(":")[-1] if ":" in doc_id else ""
                        cik_num = str(int(ciks[0])) if ciks else ""
                        adsh_clean = adsh.replace("-", "")
                        url = (
                            f"https://www.sec.gov/Archives/edgar/data/{cik_num}/{adsh_clean}/{file_part}"
                            if cik_num and adsh_clean and file_part
                            else f"https://www.sec.gov/edgar/search/#/q=Anthropic&forms={form}"
                        )
                        filings.append(
                            {
                                "entity": entity,
                                "form": form,
                                "filing_date": fdate,
                                "accession": adsh,
                                "url": url,
                            }
                        )
                        if form == "424B4" and "anthropic" in entity.lower():
                            found_424b4 = True
                        if form.startswith("S-1") and "anthropic" in entity.lower():
                            found_s1 = True
                else:
                    source_status = f"HTTP_{resp.status_code}"
                    error_detail = f"SEC EFTS returned status {resp.status_code}"
    except Exception as exc:
        source_status = "ERROR"
        error_detail = str(exc)

    return {
        "checked_at": datetime.now(timezone.utc).isoformat(),
        "source_status": source_status,
        "error": error_detail,
        "found_424b4": found_424b4,
        "found_s1": found_s1,
        "filings_count": len(filings),
        "filings": filings,
    }

