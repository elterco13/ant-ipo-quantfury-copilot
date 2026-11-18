# ANT IPO · Piloto Automático Quantfury & Acciones Tech Impactadas

Copiloto en tiempo real para operar la salida a bolsa de **Anthropic (`ANT`)** y los **28 activos de los 6 sectores impactados** (Hyperscalers, Semiconductores IA, Software SaaS, Energía Nuclear/Uranio y Cripto/Mineros HPC) adaptado a las reglas de **Quantfury**.

[![Deploy to Streamlit Community Cloud](https://static.streamlit.io/badges/streamlit_badge_black_white.svg)](https://share.streamlit.io/deploy?repository=elterco13/ant-ipo-quantfury-copilot&branch=main&mainModule=streamlit_app.py)

## Qué incluye

- **Ingesta automática en vivo:**
  - **Binance (`5` pares cripto):** `BTC`, `ETH`, `SOL`, `TAO`, `RENDER`.
  - **Yahoo Finance (`22` tickers EE.UU. + `ANT`):** Cálculo automático de **VWAP intradía real**, `Opening Print`, máximos/mínimos del día y velas en vivo.
  - **SEC EDGAR + Google News RSS:** Escaneo automático cada 60 segundos de formularios `424B4` / `S-1/A` y noticias de fijación de precio de Anthropic.
- **Señales directas por activo (`COMPRAR (LONG)`, `VENDER EN CORTO (SHORT)`, `ESPERAR`):**
  - Orden principal de `ANT` (con regla de espera de **3–5 min**, detección de **LULD Halt**, y ajuste de apalancamiento seguro `2x–3x`).
  - Señales individuales con Precio de Entrada, Stop-Loss y Take Profit para las Big Tech y sectores impactados (`GOOGL`, `AMZN`, `MSFT`, `NVDA`, `AVGO`, `MU`, `TSM`, `AMD`, `CRWV`, `CRM`, `NOW`, `ADBE`, `WDAY`, `TEAM`, `CEG`, `VST`, `OKLO`, `CCJ`, `URA`, `IREN`, `CLSK`, `QQQ`).
  - **Trade de Pares en vivo:** `LONG Ganador IA` vs `SHORT Software SaaS tradicional (Seat Compression)`.
- **Servidor MCP (`mcp_server.py`)** y **Backend WebSocket (`app.py`)** opcionales para ejecución local con baja latencia y alertas por Telegram.

## Ejecución local

### Opción 1: Streamlit (igual que en la nube)
```bash
pip install -r requirements.txt
streamlit run streamlit_app.py
```

### Opción 2: Servidor Starlette + WebSocket (latencia 2.5s)
```bash
pip install -r requirements.txt
python -m uvicorn app:app --host 127.0.0.1 --port 8000
```
