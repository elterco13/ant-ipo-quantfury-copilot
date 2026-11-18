"""Streamlit entrypoint for ANT IPO & Big Tech Quantfury Copilot (Local + Community Cloud)."""

import asyncio
import json
from pathlib import Path
import threading
import time
from typing import Any, Coroutine, TypeVar

import streamlit as st
import streamlit.components.v1 as components

from alerts import AlertManager
from telemetry_engine import TelemetryEngine

T = TypeVar("T")

st.set_page_config(
    page_title="ANT IPO · Piloto Automático Quantfury",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# Remove default Streamlit padding so the War Room fills the screen cleanly
st.markdown(
    """
    <style>
      .block-container { padding: 0.4rem 0.6rem 0.4rem 0.6rem !important; max-width: 100% !important; }
      header[data-testid="stHeader"] { height: 0 !important; visibility: hidden !important; }
      footer { visibility: hidden !important; }
    </style>
    """,
    unsafe_allow_html=True,
)


def _run_async(coro: Coroutine[Any, Any, T]) -> T:
    """Run an async coroutine inside an isolated event loop safe for Streamlit threads."""
    loop = asyncio.new_event_loop()
    try:
        asyncio.set_event_loop(loop)
        return loop.run_until_complete(coro)
    finally:
        loop.close()


@st.cache_resource
def get_engine_bundle() -> dict[str, Any]:
    """Create and cache a single shared TelemetryEngine across Streamlit reruns."""
    engine = TelemetryEngine(AlertManager())
    lock = threading.Lock()
    _run_async(engine.refresh_market_data())
    _run_async(engine._refresh_sec_and_news())
    return {
        "engine": engine,
        "lock": lock,
        "last_slow_ts": time.monotonic(),
    }


@st.cache_data
def load_html_template() -> str:
    html_path = Path(__file__).resolve().parent / "static" / "index.html"
    return html_path.read_text(encoding="utf-8")


@st.fragment(run_every="10s")
def render_live_dashboard() -> None:
    bundle = get_engine_bundle()
    engine: TelemetryEngine = bundle["engine"]
    lock: threading.Lock = bundle["lock"]

    with lock:
        _run_async(engine.refresh_market_data())
        if (time.monotonic() - bundle["last_slow_ts"]) >= 60.0:
            _run_async(engine._refresh_sec_and_news())
            bundle["last_slow_ts"] = time.monotonic()
        _run_async(engine.evaluate_alerts())
        state = engine.get_full_state()

    safe_json = json.dumps(state, ensure_ascii=False).replace("</", "<\\/")
    html = load_html_template()
    injection = f"<script>window.__INITIAL_STATE__ = {safe_json};</script>\n</head>"
    rendered_html = html.replace("</head>", injection, 1)

    components.html(rendered_html, height=2350, scrolling=True)


render_live_dashboard()
