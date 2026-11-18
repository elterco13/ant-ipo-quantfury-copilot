"""Quantfury operational execution planner, proxy selector, and LULD/slippage risk calculator."""

from typing import Any

from config import LEVERAGED_ETFS, QUANTFURY_DEFAULTS


def _determine_instrument(
    scenario: str,
    ant_status: str,
    proxy_prices: dict[str, float],
) -> dict[str, Any]:
    """Select direct ANT or immediate liquid Quantfury proxy based on availability and scenario."""
    if scenario == "A":
        if ant_status in {"LONG_ONLY", "FULL_LONG_SHORT"}:
            return {
                "direction": "LONG",
                "primary_ticker": "ANT",
                "is_proxy": False,
                "backup_proxies": QUANTFURY_DEFAULTS["proxies"]["LONG_IA"],
                "reason": "ANT habilitado para compras en Quantfury tras primer cruce oficial.",
            }
        return {
            "direction": "LONG",
            "primary_ticker": "NVDA",
            "is_proxy": True,
            "backup_proxies": QUANTFURY_DEFAULTS["proxies"]["LONG_IA"],
            "reason": (
                "ANT aún en subasta IPO Cross o sin habilitar en Quantfury: ejecutar tesis alcista "
                "vía proxies líquidos inmediatos (NVDA / GOOGL / AMZN) operativos desde las 15:30 CET."
            ),
            "reference_price": proxy_prices.get("NVDA"),
        }

    if scenario == "B":
        if ant_status == "FULL_LONG_SHORT":
            return {
                "direction": "SHORT",
                "primary_ticker": "ANT",
                "is_proxy": False,
                "backup_proxies": QUANTFURY_DEFAULTS["proxies"]["SHORT_SAAS"],
                "reason": "ANT permite cortos en Quantfury: ejecutar fade de gap/VWAP por descarga de 316M desbloqueadas.",
            }
        return {
            "direction": "SHORT",
            "primary_ticker": "CRM",
            "is_proxy": True,
            "backup_proxies": QUANTFURY_DEFAULTS["proxies"]["SHORT_SAAS"],
            "reason": (
                "Botón Short en ANT desactivado o ticker no habilitado en Quantfury: "
                "ejecutar SHORT de rotación sectorial en CRM / ADBE (amenaza Seat Compression)."
            ),
            "reference_price": proxy_prices.get("CRM"),
        }

    if scenario == "C":
        return {
            "direction": "SHORT",
            "primary_ticker": "QQQ",
            "is_proxy": True,
            "backup_proxies": QUANTFURY_DEFAULTS["proxies"]["SHORT_MACRO"],
            "reason": (
                "Escenario C (Busted IPO / Contagio Macro): ejecutar SHORT en QQQ / NVDA para máxima "
                "liquidez instantánea sin riesgo de LULD halts individuales."
            ),
            "reference_price": proxy_prices.get("QQQ"),
        }

    return {
        "direction": "FLAT",
        "primary_ticker": "ESPERAR",
        "is_proxy": False,
        "backup_proxies": ["NVDA", "GOOGL", "QQQ"],
        "reason": "Sin gatillo confirmado todavía. Mantener barra de búsqueda abierta en Quantfury.",
    }


def calculate_quantfury_plan(
    account_equity_usd: float,
    requested_leverage: float = 2.5,
    expected_slippage_pct: float = 4.0,
    luld_halt_gap_pct: float = 7.5,
    ant_status_in_quantfury: str = "NOT_LISTED_YET",
    active_scenario: str = "WAIT",
    ipo_price: float = 80.0,
    opening_print: float | None = None,
    current_price: float | None = None,
    vwap: float | None = None,
    hod: float | None = None,
    lod: float | None = None,
    seconds_since_opening_print: float | None = None,
    proxy_prices: dict[str, float] | None = None,
) -> dict[str, Any]:
    """Compute position sizing, effective stop with slippage/LULD, and Quantfury ticker routing."""
    equity = max(float(account_equity_usd), 100.0)
    req_lev = max(float(requested_leverage), 0.5)
    max_rec_lev = QUANTFURY_DEFAULTS["max_recommended_leverage"]
    effective_lev = min(req_lev, max_rec_lev)
    leverage_warning = (
        f"Apalancamiento solicitado ({req_lev:.1f}x) excede el máximo recomendado (2x–3x) para apertura OPI. "
        f"Plan ajustado a {effective_lev:.1f}x por riesgo de gap LULD."
        if req_lev > max_rec_lev
        else None
    )

    slippage_pct = max(0.5, min(float(expected_slippage_pct), 15.0))
    luld_pct = max(2.0, min(float(luld_halt_gap_pct), 25.0))
    proxies = proxy_prices or {}

    selection = _determine_instrument(active_scenario, ant_status_in_quantfury, proxies)
    direction = selection["direction"]
    ticker = selection["primary_ticker"]

    # Determine reference price for sizing and stop targets
    if not selection["is_proxy"] and current_price:
        ref_price = float(current_price)
    elif selection.get("reference_price"):
        ref_price = float(selection["reference_price"])
    elif current_price:
        ref_price = float(current_price)
    else:
        ref_price = float(ipo_price)

    notional_usd = round(equity * effective_lev, 2)
    position_units = round(notional_usd / ref_price, 2) if ref_price > 0 else 0.0

    # Check 3-5 minute rule after Opening Print
    wait_seconds_required = QUANTFURY_DEFAULTS["opening_wait_minutes"] * 60
    opening_cooldown_active = (
        seconds_since_opening_print is not None
        and 0 <= seconds_since_opening_print < 180
        and not selection["is_proxy"]
    )
    remaining_wait_sec = (
        max(0, int(180 - seconds_since_opening_print))
        if opening_cooldown_active and seconds_since_opening_print is not None
        else 0
    )

    # Calculate technical stops and targets
    tech_stop: float | None = None
    effective_stop: float | None = None
    target_1: float | None = None
    target_2: float | None = None
    risk_reward: float | None = None

    if direction == "LONG":
        if not selection["is_proxy"]:
            base_support = min(x for x in [lod, vwap, opening_print, ref_price * 0.96] if x is not None)
            tech_stop = round(base_support * 0.992, 2)
            effective_stop = round(tech_stop * (1 - slippage_pct / 100.0), 2)
            target_1 = round(max(100.0, ref_price * 1.12), 2)
            target_2 = round(max(110.0, ref_price * 1.25), 2)
        else:
            tech_stop = round(ref_price * 0.975, 2)
            effective_stop = round(tech_stop * (1 - 0.005), 2)
            target_1 = round(ref_price * 1.04, 2)
            target_2 = round(ref_price * 1.075, 2)

        risk_per_share = max(0.01, ref_price - effective_stop)
        reward_per_share = max(0.0, target_1 - ref_price)
        risk_reward = round(reward_per_share / risk_per_share, 2)

    elif direction == "SHORT":
        if not selection["is_proxy"]:
            base_res = max(x for x in [hod, vwap, opening_print, ref_price * 1.04] if x is not None)
            tech_stop = round(base_res * 1.008, 2)
            effective_stop = round(tech_stop * (1 + slippage_pct / 100.0), 2)
            target_1 = round(max(ipo_price * 1.05, (ref_price + ipo_price) / 2.0), 2)
            target_2 = round(ipo_price, 2)
        else:
            tech_stop = round(ref_price * 1.025, 2)
            effective_stop = round(tech_stop * (1 + 0.005), 2)
            target_1 = round(ref_price * 0.965, 2)
            target_2 = round(ref_price * 0.935, 2)

        risk_per_share = max(0.01, effective_stop - ref_price)
        reward_per_share = max(0.0, ref_price - target_1)
        risk_reward = round(reward_per_share / risk_per_share, 2)

    slippage_risk_usd = round(notional_usd * (slippage_pct / 100.0), 2)
    luld_gap_risk_usd = round(notional_usd * (luld_pct / 100.0), 2)
    worst_case_equity_drawdown_pct = round((luld_gap_risk_usd / equity) * 100.0, 2)

    return {
        "account_equity_usd": equity,
        "requested_leverage": req_lev,
        "effective_leverage": effective_lev,
        "leverage_warning": leverage_warning,
        "notional_usd": notional_usd,
        "instrument": ticker,
        "is_proxy": selection["is_proxy"],
        "direction": direction,
        "backup_proxies": selection["backup_proxies"],
        "routing_reason": selection["reason"],
        "reference_price": round(ref_price, 2),
        "estimated_units": position_units,
        "tech_stop_loss": tech_stop,
        "effective_stop_with_slippage": effective_stop,
        "target_1": target_1,
        "target_2": target_2,
        "risk_reward_ratio": risk_reward,
        "expected_slippage_pct": slippage_pct,
        "slippage_risk_usd": slippage_risk_usd,
        "luld_halt_gap_pct": luld_pct,
        "luld_gap_risk_usd": luld_gap_risk_usd,
        "worst_case_equity_drawdown_pct": worst_case_equity_drawdown_pct,
        "opening_cooldown_active": opening_cooldown_active,
        "opening_cooldown_remaining_sec": remaining_wait_sec,
        "opening_wait_rule_note": (
            f"ESPERAR {remaining_wait_sec}s: Regla de los primeros 3–5 minutos tras Opening Print "
            "para limpiar órdenes a mercado acumuladas."
            if opening_cooldown_active
            else f"Ventana de {wait_seconds_required // 60} min superada o operando proxy líquido."
        ),
        "etfs_warning": (
            "Los ETFs apalancados 2X (ANUU, ANDD, ANSS, Tradr Cboe 23-Nov) NO están disponibles "
            "el Día 1 en Quantfury y sufren decaimiento diario. Opera spot directo con Trading Power."
        ),
        "leveraged_etfs": LEVERAGED_ETFS,
    }

