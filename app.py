"""Starlette + WebSocket + Pydantic backend for the Anthropic (ANT) IPO & Quantfury Copilot."""

import asyncio
from contextlib import asynccontextmanager
from typing import Any, TypeVar

from pydantic import BaseModel, ValidationError
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import FileResponse, JSONResponse
from starlette.routing import Mount, Route, WebSocketRoute
from starlette.staticfiles import StaticFiles
from starlette.websockets import WebSocket, WebSocketDisconnect

from alerts import AlertManager
from config import STATIC_DIR
from mcp_server import MCP_TOOLS_SCHEMA, execute_mcp_tool
from sec_edgar import check_anthropic_filings
from telemetry_engine import TelemetryEngine

TModel = TypeVar("TModel", bound=BaseModel)

alert_manager = AlertManager()
engine = TelemetryEngine(alert_manager)
connected_clients: set[WebSocket] = set()


async def broadcast_state() -> None:
    """Push current telemetry state to all connected WebSocket clients."""
    if not connected_clients:
        return
    payload = engine.get_full_state()
    dead: list[WebSocket] = []
    for ws in list(connected_clients):
        try:
            await ws.send_json(payload)
        except Exception:
            dead.append(ws)
    for ws in dead:
        connected_clients.discard(ws)


async def _ws_broadcaster_loop() -> None:
    while True:
        await asyncio.sleep(2.5)
        await broadcast_state()


@asynccontextmanager
async def lifespan(_app: Starlette):
    await engine.start()
    ws_task = asyncio.create_task(_ws_broadcaster_loop())
    try:
        yield
    finally:
        ws_task.cancel()
        try:
            await ws_task
        except asyncio.CancelledError:
            pass
        await engine.stop()


class SecScanReq(BaseModel):
    cik: str | None = None


class Update424b4Req(BaseModel):
    raw_text: str = ""
    manual_price: float | None = None
    greenshoe_exercised: bool = True


class IndicationReq(BaseModel):
    low: float
    high: float


class OpeningPrintReq(BaseModel):
    price: float
    volume: float = 2_500_000.0


class TickReq(BaseModel):
    price: float
    volume: float = 450_000.0


class AntSettingsReq(BaseModel):
    phase_override: str | None = None
    quantfury_status: str | None = None


class ScenarioSimReq(BaseModel):
    scenario_name: str


class QuantfuryCalcReq(BaseModel):
    account_equity_usd: float = 10000.0
    requested_leverage: float = 2.5
    expected_slippage_pct: float = 4.0
    luld_halt_gap_pct: float = 7.5
    ant_status_in_quantfury: str | None = None


class AlertConfigReq(BaseModel):
    telegram_enabled: bool | None = None
    telegram_bot_token: str | None = None
    telegram_chat_id: str | None = None
    webhook_enabled: bool | None = None
    webhook_url: str | None = None
    audio_enabled: bool | None = None


class McpCallReq(BaseModel):
    name: str
    arguments: dict[str, Any] = {}


async def _parse_body(request: Request, model_cls: type[TModel]) -> TModel:
    raw = await request.body()
    data = await request.json() if raw.strip() else {}
    return model_cls.model_validate(data)


async def index(_request: Request) -> FileResponse:
    return FileResponse(str(STATIC_DIR / "index.html"))


async def get_state(_request: Request) -> JSONResponse:
    return JSONResponse(engine.get_full_state())


async def refresh_market(_request: Request) -> JSONResponse:
    await engine.refresh_market_data()
    await engine.evaluate_alerts()
    await broadcast_state()
    return JSONResponse(engine.get_full_state())


async def scan_sec(request: Request) -> JSONResponse:
    req = await _parse_body(request, SecScanReq)
    res = await check_anthropic_filings(cik=req.cik)
    engine.sec_status = res
    if res.get("found_424b4"):
        await alert_manager.emit(
            severity="SEC_424B4",
            category="SEC_EDGAR",
            title="Formulario 424B4 de Anthropic Detectado",
            message=f"Detectados {res['filings_count']} documentos en SEC EDGAR. Revisar precio final y greenshoe.",
            dedup_key="sec_424b4_detected",
            cooldown_seconds=600.0,
        )
    await broadcast_state()
    return JSONResponse({"sec_status": res, "state": engine.get_full_state()})


async def update_424b4(request: Request) -> JSONResponse:
    req = await _parse_body(request, Update424b4Req)
    parsed = engine.update_prospectus(
        raw_text=req.raw_text,
        manual_price=req.manual_price,
        greenshoe_exercised=req.greenshoe_exercised,
    )
    regime = parsed["pricing_regime"]
    await alert_manager.emit(
        severity="SEC_424B4",
        category="IPO_PRICING",
        title=f"Precio OPI Actualizado: ${parsed['final_ipo_price']:.2f} ({regime['code']})",
        message=regime["interpretation"],
        dedup_key=f"ipo_price_{parsed['final_ipo_price']}",
        cooldown_seconds=15.0,
    )
    await broadcast_state()
    return JSONResponse(engine.get_full_state())


async def post_indication(request: Request) -> JSONResponse:
    req = await _parse_body(request, IndicationReq)
    info = engine.update_auction_indication(req.low, req.high)
    if info["auction_signal"] == "BAJISTA_FOMO_RECORTE":
        await alert_manager.emit(
            severity="WARNING",
            category="IPO_CROSS",
            title="Recorte en Subasta IPO Cross tras FOMO",
            message=f"Banda bajó a ${info['indication_low']}–${info['indication_high']} ({info['gap_pct']:+.1f}%). Riesgo de techo en apertura.",
            dedup_key="auction_fomo_cut",
            cooldown_seconds=60.0,
        )
    await broadcast_state()
    return JSONResponse(engine.get_full_state())


async def post_opening_print(request: Request) -> JSONResponse:
    req = await _parse_body(request, OpeningPrintReq)
    engine.record_opening_print(req.price, req.volume)
    ipo_p = engine.prospectus["final_ipo_price"]
    gap = ((req.price - ipo_p) / ipo_p) * 100.0
    await alert_manager.emit(
        severity="OPENING_PRINT",
        category="ANT_TAPE",
        title=f"Primer Cruce Oficial ANT: ${req.price:.2f} ({gap:+.1f}% vs OPI)",
        message=(
            f"Opening Print fijado en ${req.price:.2f} (OPI ${ipo_p:.2f}). "
            "Regla operativa: esperar 3–5 minutos para limpieza de órdenes a mercado."
        ),
        dedup_key=f"opening_print_{req.price}",
        cooldown_seconds=10.0,
    )
    await engine.evaluate_alerts()
    await broadcast_state()
    return JSONResponse(engine.get_full_state())


async def post_ant_tick(request: Request) -> JSONResponse:
    req = await _parse_body(request, TickReq)
    engine.add_ant_tick(req.price, req.volume)
    await engine.evaluate_alerts()
    await broadcast_state()
    return JSONResponse(engine.get_full_state())


async def post_ant_settings(request: Request) -> JSONResponse:
    req = await _parse_body(request, AntSettingsReq)
    if req.phase_override == "AUTO":
        engine.ant_state["phase_override"] = None
    elif req.phase_override:
        engine.ant_state["phase_override"] = req.phase_override

    if req.quantfury_status:
        engine.ant_state["quantfury_status"] = req.quantfury_status

    await broadcast_state()
    return JSONResponse(engine.get_full_state())


async def run_sim(request: Request) -> JSONResponse:
    req = await _parse_body(request, ScenarioSimReq)
    state = await engine.run_scenario_simulation(req.scenario_name)
    await broadcast_state()
    return JSONResponse(state)


async def calc_quantfury(request: Request) -> JSONResponse:
    req = await _parse_body(request, QuantfuryCalcReq)
    engine.quantfury_params.update(
        {
            "account_equity_usd": req.account_equity_usd,
            "requested_leverage": req.requested_leverage,
            "expected_slippage_pct": req.expected_slippage_pct,
            "luld_halt_gap_pct": req.luld_halt_gap_pct,
        }
    )
    if req.ant_status_in_quantfury:
        engine.ant_state["quantfury_status"] = req.ant_status_in_quantfury
    await broadcast_state()
    return JSONResponse(engine.get_full_state())


async def update_alerts_cfg(request: Request) -> JSONResponse:
    req = await _parse_body(request, AlertConfigReq)
    updates = req.model_dump()
    if updates.get("telegram_bot_token") == "":
        updates.pop("telegram_bot_token", None)
    cfg = alert_manager.update_config(updates)
    await broadcast_state()
    return JSONResponse({"alerts_config": cfg})


async def test_alert(_request: Request) -> JSONResponse:
    rec = await alert_manager.emit(
        severity="INFO",
        category="SYSTEM_TEST",
        title="Prueba de Alerta Copiloto ANT IPO",
        message="Canal de alertas conectado y operativo para la salida a bolsa de Anthropic.",
        dedup_key=None,
    )
    await broadcast_state()
    return JSONResponse({"alert": rec, "recent_alerts": alert_manager.list_alerts(limit=15)})


async def list_mcp_tools(_request: Request) -> JSONResponse:
    return JSONResponse({"tools": MCP_TOOLS_SCHEMA})


async def call_mcp_tool(request: Request) -> JSONResponse:
    req = await _parse_body(request, McpCallReq)
    try:
        res = await execute_mcp_tool(req.name, req.arguments, engine=engine)
        return JSONResponse({"tool": req.name, "result": res})
    except ValueError as exc:
        return JSONResponse({"detail": str(exc)}, status_code=404)


async def ws_telemetry(ws: WebSocket) -> None:
    await ws.accept()
    connected_clients.add(ws)
    try:
        await ws.send_json(engine.get_full_state())
        while True:
            await ws.receive_text()
    except WebSocketDisconnect:
        connected_clients.discard(ws)
    except Exception:
        connected_clients.discard(ws)


async def _validation_exception_handler(_request: Request, exc: ValidationError) -> JSONResponse:
    return JSONResponse({"detail": exc.errors()}, status_code=422)


STATIC_DIR.mkdir(parents=True, exist_ok=True)

app = Starlette(
    routes=[
        Route("/", index, methods=["GET"]),
        Route("/api/state", get_state, methods=["GET"]),
        Route("/api/refresh", refresh_market, methods=["POST"]),
        Route("/api/sec/scan", scan_sec, methods=["POST"]),
        Route("/api/sec/update-424b4", update_424b4, methods=["POST"]),
        Route("/api/ant/indication", post_indication, methods=["POST"]),
        Route("/api/ant/record-opening-print", post_opening_print, methods=["POST"]),
        Route("/api/ant/update-tick", post_ant_tick, methods=["POST"]),
        Route("/api/ant/settings", post_ant_settings, methods=["POST"]),
        Route("/api/simulator/run-scenario", run_sim, methods=["POST"]),
        Route("/api/quantfury/calculate", calc_quantfury, methods=["POST"]),
        Route("/api/alerts/config", update_alerts_cfg, methods=["POST"]),
        Route("/api/alerts/test", test_alert, methods=["POST"]),
        Route("/api/mcp/tools", list_mcp_tools, methods=["GET"]),
        Route("/api/mcp/call", call_mcp_tool, methods=["POST"]),
        WebSocketRoute("/ws/telemetry", ws_telemetry),
        Mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static"),
    ],
    exception_handlers={ValidationError: _validation_exception_handler},
    lifespan=lifespan,
)
