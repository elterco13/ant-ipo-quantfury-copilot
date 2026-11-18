"""Model Context Protocol (MCP) JSON-RPC 2.0 stdio server & shared tool registry for ANT IPO."""

import asyncio
import json
import sys
from typing import Any

import httpx

from alerts import AlertManager
from config import (
    COMPUTE_COMMITMENTS_12M,
    COMPUTE_COMMITMENTS_TOTAL,
    ENTERPRISE_CUSTOMERS_100K,
    GROSS_MARGIN,
    PRIVATE_PLACEMENT_INVESTORS,
    PRIVATE_PLACEMENT_USD,
    Q3_QOQ_GROWTH,
    Q3_REV,
    RUN_RATE_END_2025,
    RUN_RATE_OCT_2026,
    SPACEX_PRECEDENT,
    TRADITIONAL_SAAS_GROSS_MARGIN,
)
from quantfury_risk import calculate_quantfury_plan
from sec_edgar import check_anthropic_filings
from telemetry_engine import TelemetryEngine

LOCAL_BACKEND_URL = "http://127.0.0.1:8000"

MCP_TOOLS_SCHEMA: list[dict[str, Any]] = [
    {
        "name": "get_ipo_structural_radiography",
        "description": (
            "Devuelve la radiografía estructural S-1/A y 424B4 de la OPI de Anthropic (ANT): "
            "precio fijado, greenshoe (123.75M), trampa de liquidez Día 1 (316M desbloqueadas + 165M retail), "
            "colocación privada ($26B) y fundamentales."
        ),
        "inputSchema": {"type": "object", "properties": {}},
    },
    {
        "name": "get_live_intermarket_matrix",
        "description": (
            "Obtiene la matriz intermercado en tiempo real de los 6 sectores (Hyperscalers, Semis IA, "
            "SaaS Tradicional, Energía Nuclear/Uranio, Cripto/Mineros y ANT) con divergencias y spread Seat Compression."
        ),
        "inputSchema": {"type": "object", "properties": {}},
    },
    {
        "name": "get_ant_vwap_and_scenario",
        "description": (
            "Consulta la fase cronológica CET/ET, bandas indicativas de subasta IPO Cross, Opening Print, "
            "VWAP anclado, detector LULD Halt y dictamen de Escenario (A Long / B Short / C Macro)."
        ),
        "inputSchema": {"type": "object", "properties": {}},
    },
    {
        "name": "get_quantfury_execution_plan",
        "description": (
            "Calcula el plan de ejecución en Quantfury (ticker directo o proxy líquido a las 15:30 CET), "
            "tamaño de posición con apalancamiento máx 2x-3x y stop efectivo incluyendo slippage (3-5%) y LULD gap."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "account_equity_usd": {"type": "number", "description": "Capital / Trading Power base en USD"},
                "requested_leverage": {"type": "number", "description": "Apalancamiento solicitado (máx rec 3.0x)"},
                "ant_status_in_quantfury": {
                    "type": "string",
                    "enum": ["NOT_LISTED_YET", "LONG_ONLY", "FULL_LONG_SHORT"],
                    "description": "Disponibilidad actual de ANT en la app Quantfury",
                },
                "expected_slippage_pct": {"type": "number", "description": "Deslizamiento esperado en apertura (%)"},
            },
        },
    },
    {
        "name": "check_sec_edgar_424b4",
        "description": (
            "Consulta la API pública de SEC EDGAR para detectar la publicación del prospecto definitivo "
            "424B4, S-1/A o EFFECT de Anthropic."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "cik": {"type": "string", "description": "CIK opcional de 10 dígitos si ya se conoce"},
            },
        },
    },
]


async def _fetch_state_from_running_app() -> dict[str, Any] | None:
    try:
        async with httpx.AsyncClient(timeout=2.0) as client:
            resp = await client.get(f"{LOCAL_BACKEND_URL}/api/state")
            if resp.status_code == 200:
                return resp.json()
    except Exception:
        return None
    return None


async def execute_mcp_tool(
    name: str,
    arguments: dict[str, Any] | None = None,
    engine: TelemetryEngine | None = None,
) -> dict[str, Any]:
    """Execute one of the ANT IPO MCP tools against live engine or running backend."""
    args = arguments or {}

    if name == "check_sec_edgar_424b4":
        return await check_anthropic_filings(cik=args.get("cik"))

    state: dict[str, Any] | None = None
    if engine is not None:
        state = engine.get_full_state()
    else:
        state = await _fetch_state_from_running_app()
        if state is None:
            standalone = TelemetryEngine(AlertManager())
            await standalone.refresh_market_data()
            state = standalone.get_full_state()

    if name == "get_ipo_structural_radiography":
        return {
            "prospectus": state["prospectus"],
            "private_placement": {
                "total_usd": PRIVATE_PLACEMENT_USD,
                "investors": PRIVATE_PLACEMENT_INVESTORS,
            },
            "fundamentals": {
                "run_rate_end_2025_usd": RUN_RATE_END_2025,
                "run_rate_oct_2026_usd": RUN_RATE_OCT_2026,
                "q3_rev_usd": Q3_REV,
                "q3_qoq_growth": Q3_QOQ_GROWTH,
                "enterprise_customers_over_100k": ENTERPRISE_CUSTOMERS_100K,
                "gross_margin": GROSS_MARGIN,
                "traditional_saas_gross_margin": TRADITIONAL_SAAS_GROSS_MARGIN,
                "compute_commitments_total_usd": COMPUTE_COMMITMENTS_TOTAL,
                "compute_commitments_12m_usd": COMPUTE_COMMITMENTS_12M,
            },
            "spacex_precedent": SPACEX_PRECEDENT,
        }

    if name == "get_live_intermarket_matrix":
        return state["intermarket"]

    if name == "get_ant_vwap_and_scenario":
        return {
            "phase": state["phase"],
            "ant_tape": state["ant_tape"],
            "scenario": state["scenario"],
        }

    if name == "get_quantfury_execution_plan":
        if not args:
            return state["quantfury_plan"]
        tape = state["ant_tape"]
        matrix = state["intermarket"]
        proxies = {}
        for sec in matrix["sectors"].values():
            for row in sec["tickers"]:
                if row.get("price"):
                    proxies[row["symbol"]] = float(row["price"])
        proxies["QQQ"] = float(matrix["benchmark_qqq"].get("price") or 0.0)

        return calculate_quantfury_plan(
            account_equity_usd=float(args.get("account_equity_usd", state["quantfury_plan"]["account_equity_usd"])),
            requested_leverage=float(args.get("requested_leverage", state["quantfury_plan"]["requested_leverage"])),
            expected_slippage_pct=float(
                args.get("expected_slippage_pct", state["quantfury_plan"]["expected_slippage_pct"])
            ),
            luld_halt_gap_pct=float(state["quantfury_plan"]["luld_halt_gap_pct"]),
            ant_status_in_quantfury=str(args.get("ant_status_in_quantfury", tape["quantfury_status"])),
            active_scenario=state["scenario"]["code"],
            ipo_price=state["prospectus"]["final_ipo_price"],
            opening_print=tape.get("opening_print"),
            current_price=tape.get("current_price"),
            vwap=tape.get("vwap"),
            hod=tape.get("hod"),
            lod=tape.get("lod"),
            seconds_since_opening_print=tape.get("seconds_since_opening_print"),
            proxy_prices=proxies,
        )

    raise ValueError(f"Unknown MCP tool: {name}")


async def _handle_jsonrpc_message(msg: dict[str, Any]) -> dict[str, Any] | None:
    msg_id = msg.get("id")
    method = msg.get("method", "")
    params = msg.get("params") or {}

    if method == "initialize":
        return {
            "jsonrpc": "2.0",
            "id": msg_id,
            "result": {
                "protocolVersion": "2024-11-05",
                "capabilities": {"tools": {}},
                "serverInfo": {"name": "ant-ipo-quantfury-copilot", "version": "1.0.0"},
            },
        }
    if method == "notifications/initialized":
        return None
    if method == "tools/list":
        return {
            "jsonrpc": "2.0",
            "id": msg_id,
            "result": {"tools": MCP_TOOLS_SCHEMA},
        }
    if method == "tools/call":
        tool_name = params.get("name", "")
        arguments = params.get("arguments") or {}
        try:
            res = await execute_mcp_tool(tool_name, arguments)
            return {
                "jsonrpc": "2.0",
                "id": msg_id,
                "result": {
                    "content": [{"type": "text", "text": json.dumps(res, ensure_ascii=False, indent=2)}],
                    "isError": False,
                },
            }
        except Exception as exc:
            return {
                "jsonrpc": "2.0",
                "id": msg_id,
                "result": {
                    "content": [{"type": "text", "text": f"Error: {exc}"}],
                    "isError": True,
                },
            }

    if msg_id is not None:
        return {
            "jsonrpc": "2.0",
            "id": msg_id,
            "error": {"code": -32601, "message": f"Method not found: {method}"},
        }
    return None


async def _run_stdio_server() -> None:
    loop = asyncio.get_running_loop()
    reader = asyncio.StreamReader()
    protocol = asyncio.StreamReaderProtocol(reader)
    await loop.connect_read_pipe(lambda: protocol, sys.stdin)

    while True:
        line_bytes = await reader.readline()
        if not line_bytes:
            break
        line = line_bytes.decode("utf-8").strip()
        if not line:
            continue
        try:
            msg = json.loads(line)
            resp = await _handle_jsonrpc_message(msg)
            if resp is not None:
                sys.stdout.write(json.dumps(resp, ensure_ascii=False) + "\n")
                sys.stdout.flush()
        except json.JSONDecodeError:
            continue


if __name__ == "__main__":
    asyncio.run(_run_stdio_server())

