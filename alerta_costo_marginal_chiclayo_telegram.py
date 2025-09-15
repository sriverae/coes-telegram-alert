# -*- coding: utf-8 -*-
"""
Alerta COES → Telegram (CHICLAYO 220)
Flujo 2025-09:
- Acepta aviso
- Exportar Masivo → Fecha desde / Hasta = HOY
- Captura el Excel en memoria (sin descargar a tu laptop)
- Lee Energía / Congestión / Total para CHICLAYO 220 (soporta columnas: NOMBRE BARRA, FECHA HORA, ENERGÍA, CONGESTIÓN, TOTAL)
- Elige la media hora más cercana hacia atrás
"""

import os, time, json, re, io, unicodedata
import pandas as pd
import requests
from datetime import datetime, timedelta, time as dtime
from zoneinfo import ZoneInfo

try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

from playwright.sync_api import sync_playwright

# ==================== CONFIG ====================
BARRA_BUSCADA = os.getenv("BARRA_BUSCADA", "CHICLAYO 220")
UMBRAL_S_POR_MWH = float(os.getenv("UMBRAL_S_POR_MWH", "400"))
INTERVALO_MINUTOS = int(os.getenv("INTERVALO_MINUTOS", "30"))
TZ = ZoneInfo(os.getenv("TZ", "America/Lima"))
SILENCIO_DESDE = os.getenv("SILENCIO_DESDE")
SILENCIO_HASTA = os.getenv("SILENCIO_HASTA")
ONESHOT = os.getenv("ONESHOT", "0") == "1"

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")

STATE_FILE = os.getenv("STATE_FILE", "estado_alerta_chiclayo220.json")

GIST_TOKEN = os.getenv("GIST_TOKEN", "")
GIST_ID = os.getenv("GIST_ID", "")
GIST_FILENAME = os.getenv("GIST_FILENAME", "estado_alerta_chiclayo220.json")

URL_COSTOS_TIEMPO_REAL = os.getenv(
    "URL_COSTOS_TIEMPO_REAL",
    "https://www.coes.org.pe/Portal/mercadomayorista/costosmarginales/index"
)

# ==================== UTILIDADES ====================
def _parse_hhmm(s: str):
    try:
        hh, mm = s.split(":")
        return dtime(int(hh), int(mm))
    except Exception:
        return None

def en_horario_sonido(ahora: datetime) -> bool:
    if not SILENCIO_DESDE or not SILENCIO_HASTA:
        return True
    t_desde = _parse_hhmm(SILENCIO_DESDE)
    t_hasta = _parse_hhmm(SILENCIO_HASTA)
    if not (t_desde and t_hasta):
        return True
    t = ahora.time()
    if t_desde < t_hasta:
        return not (t_desde <= t < t_hasta)
    else:
        return not (t >= t_desde or t < t_hasta)

# --------- Gist (persistencia opcional) ---------
def _gist_headers():
    return {"Authorization": f"Bearer {GIST_TOKEN}", "Accept": "application/vnd.github+json"} if GIST_TOKEN else {}

def _gist_api_url():
    return f"https://api.github.com/gists/{GIST_ID}"

def _gist_read_state():
    if not (GIST_TOKEN and GIST_ID):
        raise RuntimeError("Gist no configurado")
    r = requests.get(_gist_api_url(), headers=_gist_headers(), timeout=30)
    r.raise_for_status()
    j = r.json()
    files = j.get("files", {})
    file = files.get(GIST_FILENAME)
    if not file:
        return {"ultimo_envio_ts": None, "ultimo_registro_hora": None, "ultimo_valor": None}
    if not file.get("truncated") and "content" in file:
        txt = file["content"]
    else:
        raw_url = file.get("raw_url")
        if not raw_url:
            return {"ultimo_envio_ts": None, "ultimo_registro_hora": None, "ultimo_valor": None}
        rr = requests.get(raw_url, headers=_gist_headers(), timeout=30)
        rr.raise_for_status()
        txt = rr.text
    try:
        return json.loads(txt)
    except Exception:
        return {"ultimo_envio_ts": None, "ultimo_registro_hora": None, "ultimo_valor": None}

def _gist_write_state(state: dict):
    if not (GIST_TOKEN and GIST_ID):
        raise RuntimeError("Gist no configurado")
    payload = {"files": {GIST_FILENAME: {"content": json.dumps(state, ensure_ascii=False, indent=2)}}}
    r = requests.patch(_gist_api_url(), headers=_gist_headers(), json=payload, timeout=30)
    r.raise_for_status()
    return r.json()

def cargar_estado():
    if GIST_TOKEN and GIST_ID:
        try:
            return _gist_read_state()
        except Exception:
            pass
    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {"ultimo_envio_ts": None, "ultimo_registro_hora": None, "ultimo_valor": None}

def guardar_estado(state):
    if GIST_TOKEN and GIST_ID:
        try:
            _gist_write_state(state)
            return
        except Exception:
            pass
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)

def enviar_telegram(mensaje: str):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        raise RuntimeError("Faltan TELEGRAM_BOT_TOKEN o TELEGRAM_CHAT_ID.")
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": mensaje, "parse_mode": "HTML", "disable_web_page_preview": True}
    r = requests.post(url, json=payload, timeout=30)
    r.raise_for_status()
    return r.json()

# ==================== AYUDAS DE PARSEO ====================
def _strip_accents(s: str) -> str:
    return "".join(c for c in unicodedata.normalize("NFKD", s) if not unicodedata.combining(c))

def _norm_text(s: str) -> str:
    s = _strip_accents(str(s)).upper()
    s = re.sub(r"\s+", " ", s).strip()
    return s

def _to_float_series(s: pd.Series) -> pd.Series:
    return (
        s.astype(str)
         .str.replace(",", ".", regex=False)
         .str.extract(r"([-]?[0-9]+(?:\.[0-9]+)?)")[0]
         .astype(float)
    )

# ==================== PARSEO EXCEL (ROBUSTO) ====================
def _reshape_from_dataframe(df0: pd.DataFrame) -> pd.DataFrame | None:
    if df0 is None or df0.empty:
        return None

    # mapear columnas por nombres (insensible a acentos/espacios)
    cols_norm = {c: _norm_text(c) for c in df0.columns}

    def _find_col_by_keywords(keywords: list[str]) -> str | None:
        keys = [_norm_text(k) for k in keywords]
        for c, cn in cols_norm.items():
            for k in keys:
                if k in cn:
                    return c
        return None

    col_barra = _find_col_by_keywords(["NOMBRE BARRA", "BARRA", "NODO", "PUNTO"])
    col_fechahora = _find_col_by_keywords(["FECHA HORA", "FECHA-HORA", "FECHAHORA", "FECHA Y HORA"])
    col_fecha = _find_col_by_keywords(["FECHA"])
    col_hora  = _find_col_by_keywords(["HORA"])
    col_cm_energia = _find_col_by_keywords(["CM ENERGIA", "ENERGIA", "ENERGÍA", "COSTO MARGINAL ENERGIA", "COSTO MARGINAL ENERGÍA"])
    col_cm_cong    = _find_col_by_keywords(["CM CONGESTION", "CONGESTION", "CONGESTIÓN"])
    col_cm_total   = _find_col_by_keywords(["CM TOTAL", "TOTAL", "COSTO MARGINAL TOTAL"])

    # necesitamos: Barra y algún CM; y un timestamp (FechaHora o Fecha+Hora)
    if not col_barra:
        return None
    if not (col_cm_total or col_cm_energia or col_cm_cong):
        return None
    if not (col_fechahora or (col_fecha and col_hora)):
        return None

    keep = [c for c in [col_barra, col_fechahora, col_fecha, col_hora, col_cm_energia, col_cm_cong, col_cm_total] if c]
    df = df0[keep].copy()

    rename_map = {col_barra: "Barra"}
    if col_fechahora: rename_map[col_fechahora] = "FechaHora"
    if col_fecha:     rename_map[col_fecha]     = "Fecha"
    if col_hora:      rename_map[col_hora]      = "Hora"
    if col_cm_energia: rename_map[col_cm_energia] = "CM_Energia"
    if col_cm_cong:    rename_map[col_cm_cong]    = "CM_Congestion"
    if col_cm_total:   rename_map[col_cm_total]   = "CM_Total"
    df = df.rename(columns=rename_map)

    # numéricos
    for c in ["CM_Energia", "CM_Congestion", "CM_Total"]:
        if c in df.columns:
            df[c] = _to_float_series(df[c])

    # timestamp
    if "FechaHora" in df.columns:
        df["ts"] = pd.to_datetime(df["FechaHora"], dayfirst=True, errors="coerce")
    else:
        df["ts"] = pd.to_datetime(
            df["Fecha"].astype(str).str.strip() + " " + df["Hora"].astype(str).str.strip(),
            dayfirst=True, errors="coerce"
        )

    return df.dropna(subset=["ts"])

def leer_excel_exportado_en_memoria(binary: bytes) -> pd.DataFrame:
    """
    1) Intentamos directo con header=0.
    2) Si no, detectamos fila de encabezado.
    Soporta columnas: NOMBRE BARRA, FECHA HORA, ENERGÍA, CONGESTIÓN, TOTAL.
    """
    # intento 1: header=0
    try:
        df0 = pd.read_excel(io.BytesIO(binary), header=0, engine="openpyxl")
        df = _reshape_from_dataframe(df0)
        if df is not None and not df.empty:
            return df
    except Exception:
        pass

    # intento 2: localizar encabezado internamente
    raw = pd.read_excel(io.BytesIO(binary), header=None, engine="openpyxl")
    # buscar primera fila que contenga 'hora' y 'barra|nodo|punto'
    sraw = raw.astype(str)
    has_hora  = sraw.apply(lambda r: r.str.contains(r"\bhora\b", case=False, na=False).any(), axis=1)
    has_barra = sraw.apply(lambda r: r.str.contains(r"\b(?:barra|nodo|punto)\b", case=False, na=False).any(), axis=1)
    idxs = sraw.index[(has_hora & has_barra)].tolist()
    if not idxs:
        # también podría venir 'FECHA HORA' en una sola columna
        has_fh = sraw.apply(lambda r: r.str.contains(r"fecha\s*hora", case=False, na=False).any(), axis=1)
        idxs = sraw.index[(has_fh)].tolist()
    if not idxs:
        raise RuntimeError("No se encontró encabezado en el Excel exportado.")

    hdr = idxs[0]
    header_row = raw.loc[hdr, :].tolist()
    first_non_null = next((i for i, v in enumerate(header_row) if pd.notna(v)), 0)
    cols = [str(c).strip() for c in raw.loc[hdr, first_non_null:].tolist()]
    data = raw.loc[hdr+1:, first_non_null:].copy()
    data.columns = cols
    data = data.dropna(how="all")

    df = _reshape_from_dataframe(data)
    if df is None or df.empty:
        raise RuntimeError("Faltan columnas esperadas (NOMBRE BARRA / FECHA HORA / ENERGÍA / CONGESTIÓN / TOTAL).")
    return df

# ==================== NORMALIZACIÓN BARRA ====================
def _norm_barra(s: str) -> str:
    if s is None:
        return ""
    t = _norm_text(str(s))
    t = t.replace(".", "").replace(" ", "")
    return t

def filtrar_barra_robusto(df: pd.DataFrame, barra_objetivo: str) -> pd.DataFrame:
    objetivo = _norm_barra(barra_objetivo)
    df = df.copy()
    df["Barra_norm"] = df["Barra"].map(_norm_barra)

    ciudad = _norm_barra("CHICLAYO")
    candidatos = df[
        df["Barra_norm"].str.contains(_norm_text(barra_objetivo).replace(" ", ""), na=False)
        | df["Barra_norm"].str.contains(ciudad, na=False)
        | df["Barra_norm"].str.contains("CHICLAYO220", na=False)
        | df["Barra_norm"].str.contains("CHICLAYO220K?V?", na=False, regex=True)
    ]

    if candidatos.empty:
        ejemplos = ", ".join(df["Barra"].dropna().astype(str).unique()[:10])
        raise RuntimeError(f"No se halló la barra '{barra_objetivo}'. Ejemplos en archivo: {ejemplos}")

    con220 = candidatos[candidatos["Barra_norm"].str.contains("220", na=False)]
    return con220 if not con220.empty else candidatos

# ==================== FLUJO WEB: EXPORTAR MASIVO ====================
def obtener_ultimo_costo_por_export(timeout_ms=150_000):
    """
    1) Abre la página y cierra el aviso.
    2) Click en 'Exportar Masivo'.
    3) En el modal: poner HOY en 'Fecha desde' y 'Hasta' (dd/mm/yyyy).
    4) Click en 'Aceptar' capturando el Excel en memoria.
    5) Parsear y elegir la media hora más cercana hacia atrás (hoy).
    """
    def _screenshot(page, name):
        try:
            page.screenshot(path=name)
        except Exception:
            pass

    def _click_possibles(page, textos, timeout=7000):
        for t in textos:
            try:
                page.get_by_role("button", name=re.compile(t, re.I)).click(timeout=timeout); return True
            except Exception: pass
            try:
                page.get_by_text(re.compile(t, re.I)).first.click(timeout=timeout); return True
            except Exception: pass
            try:
                page.locator(f"button:has-text('{t}')").first.click(timeout=timeout); return True
            except Exception: pass
            try:
                page.locator(f"a:has-text('{t}')").first.click(timeout=timeout); return True
            except Exception: pass
            try:
                page.locator(f"input[type='button'][value*='{t}']").first.click(timeout=timeout); return True
            except Exception: pass
        return False

    def _cerrar_aviso(page):
        _click_possibles(page, [r"^Aceptar$", "Aceptar", r"×", r"X"])
        page.wait_for_timeout(250)

    def _abrir_exportar_masivo(page):
        return _click_possibles(page, [r"^Exportar Masivo$", "Exportar Masivo"])

    def _find_modal_inputs(page):
        """
        Busca los dos inputs del modal 'Exportar Datos' (Fecha desde / Hasta)
        tolerando variaciones (con/sin dos puntos).
        """
        candidatos = []
        # por label/etiqueta cercana
        try:
            loc = page.locator("xpath=//*[contains(translate(normalize-space(.),'ABCDEFGHIJKLMNOPQRSTUVWXYZÁÉÍÓÚ','abcdefghijklmnopqrstuvwxyzáéíóú'),'fecha desde')]/following::input[1]")
            if loc.count() > 0 and loc.first.is_visible():
                candidatos.append(loc.first)
        except Exception:
            pass
        try:
            loc = page.locator("xpath=//*[contains(translate(normalize-space(.),'ABCDEFGHIJKLMNOPQRSTUVWXYZÁÉÍÓÚ','abcdefghijklmnopqrstuvwxyzáéíóú'),'hasta')]/following::input[1]")
            if loc.count() > 0 and loc.first.is_visible():
                candidatos.append(loc.first)
        except Exception:
            pass

        # si no encontró 2, buscar inputs visibles dentro del contenedor del modal
        if len(candidatos) < 2:
            try:
                modal = page.locator("xpath=//*[contains(.,'Exportar Datos')]/ancestor::div[1]")
                if modal.count() == 0:
                    aceptar = page.get_by_role("button", name=re.compile(r"Aceptar", re.I)).last
                    modal = aceptar.locator("xpath=ancestor::div[1]")
                inputs = modal.locator("input")
                vis = [inputs.nth(i) for i in range(inputs.count()) if inputs.nth(i).is_visible()]
                # priorizar type='date'
                ordered = sorted(
                    vis,
                    key=lambda el: 2 if ((el.get_attribute("type") or "").lower() == "date") else 1,
                    reverse=True
                )
                candidatos = ordered[:2]
            except Exception:
                pass

        if len(candidatos) < 2:
            raise RuntimeError("No se encontraron campos 'Fecha desde/Hasta' en el modal de exportación.")
        return candidatos[0], candidatos[1]

    # --------- navegación ---------
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page(viewport={"width": 1440, "height": 900})

        page.goto(URL_COSTOS_TIEMPO_REAL, wait_until="domcontentloaded", timeout=timeout_ms)
        page.wait_for_load_state("networkidle")
        _screenshot(page, "step1_loaded.png")

        _cerrar_aviso(page)
        _screenshot(page, "step2_modal_closed.png")

        if not _abrir_exportar_masivo(page):
            browser.close()
            raise RuntimeError("No se pudo abrir 'Exportar Masivo'.")

        page.wait_for_selector("text=/Exportar Datos/i", timeout=10_000)
        _screenshot(page, "step3_open_export_modal.png")

        # HOY (dd/mm/yyyy)
        hoy = datetime.now(TZ).strftime("%d/%m/%Y")
        inp_desde, inp_hasta = _find_modal_inputs(page)
        for inp in (inp_desde, inp_hasta):
            try:
                inp.click()
                inp.fill("")
                inp.type(hoy, delay=15)
            except Exception:
                pass
        _screenshot(page, "step4_dates_filled.png")

        # Descargar/capturar
        with page.expect_download(timeout=60_000) as dl_info:
            _click_possibles(page, [r"^Aceptar$", "Aceptar"])
        download = dl_info.value

        export_path = "export_debug.xlsx"
        try:
            download.save_as(export_path)
        except Exception:
            pass
        path = None
        try:
            path = download.path()
        except Exception:
            pass
        if path and not os.path.exists(export_path):
            try:
                with open(path, "rb") as src, open(export_path, "wb") as dst:
                    dst.write(src.read())
            except Exception:
                pass
        _screenshot(page, "step5_after_accept.png")

        binary = None
        for candidate in [export_path, path]:
            if candidate and os.path.exists(candidate):
                with open(candidate, "rb") as f:
                    binary = f.read()
                break

        browser.close()

    if not binary:
        raise RuntimeError("No se pudo capturar el archivo Excel exportado.")

    # ---- Parseo y selección del registro adecuado ----
    df = leer_excel_exportado_en_memoria(binary)
    df = filtrar_barra_robusto(df, BARRA_BUSCADA)

    # objetivo: media hora más cercana hacia atrás (hoy)
    ahora = datetime.now(TZ)
    target_min = (ahora.hour * 60 + ahora.minute) // 30 * 30
    target_ts = ahora.replace(hour=target_min // 60, minute=target_min % 60, second=0, microsecond=0)

    # timezone
    if df["ts"].dt.tz is None:
        df["ts"] = df["ts"].dt.tz_localize(TZ)
    else:
        df["ts"] = df["ts"].dt.tz_convert(TZ)

    # filtrar por hoy
    hoy_ini = ahora.replace(hour=0, minute=0, second=0, microsecond=0)
    hoy_fin = hoy_ini + timedelta(days=1)
    dfd = df[(df["ts"] >= hoy_ini) & (df["ts"] < hoy_fin)].copy()
    if dfd.empty:
        raise RuntimeError("No se hallaron registros para hoy en el Excel exportado.")

    elegido = None
    for k in range(0, 48):
        cand = target_ts - timedelta(minutes=30 * k)
        hit = dfd[dfd["ts"] == cand]
        if not hit.empty:
            elegido = hit.sort_values("ts").iloc[-1]
            break
    if elegido is None:
        menor = dfd[dfd["ts"] <= target_ts]
        if not menor.empty:
            elegido = menor.sort_values("ts").iloc[-1]
    if elegido is None:
        elegido = dfd.sort_values("ts").iloc[-1]

    energia = float(elegido["CM_Energia"]) if "CM_Energia" in dfd.columns else None
    congestion = float(elegido["CM_Congestion"]) if "CM_Congestion" in dfd.columns else None
    total = float(elegido["CM_Total"]) if "CM_Total" in dfd.columns else None
    ts = elegido["ts"]

    return {
        "barra": elegido["Barra"],
        "ts": ts,
        "energia": energia,
        "congestion": congestion,
        "total": total,
    }

# ==================== LOOP / MENSAJE ====================
def ejecutar_iteracion():
    estado = cargar_estado()
    dato = obtener_ultimo_costo_por_export()
    energia = dato.get("energia")
    congestion = dato.get("congestion")
    total = dato.get("total")
    ts_local = dato["ts"].astimezone(TZ)

    ahora = datetime.now(TZ)
    lineas = [
        f"<b>COES</b> • <b>{dato['barra']}</b>",
        f"<b>{ts_local:%Y-%m-%d %H:%M}</b> (America/Lima)",
    ]
    if energia is not None:
        lineas.append(f"CM Energía: <b>S/ {energia:,.2f}</b> / MWh")
    if congestion is not None:
        lineas.append(f"CM Congestión: <b>S/ {congestion:,.2f}</b> / MWh")
    if total is not None:
        lineas.append(f"CM Total: <b>S/ {total:,.2f}</b> / MWh")
    mensaje = "\n".join(lineas)

    es_nuevo = (
        str(estado.get("ultimo_registro_hora")) != ts_local.strftime("%Y-%m-%d %H:%M")
        or estado.get("ultimo_valor") != total
    )

    if (total is not None) and (total > UMBRAL_S_POR_MWH) and es_nuevo and en_horario_sonido(ahora):
        enviar_telegram(mensaje + "\n\n⚠️ Superó el umbral configurado (S/ {:.2f} por MWh).".format(UMBRAL_S_POR_MWH))
        estado["ultimo_envio_ts"] = ahora.isoformat()
        estado["ultimo_registro_hora"] = ts_local.strftime("%Y-%m-%d %H:%M")
        estado["ultimo_valor"] = total
        guardar_estado(estado)
        print(f"[OK] Alerta enviada.")
    else:
        motivo = []
        if total is None or total <= UMBRAL_S_POR_MWH: motivo.append("<= umbral")
        if not es_nuevo: motivo.append("dato repetido")
        if not en_horario_sonido(ahora): motivo.append("horario silencioso")
        print(f"[INFO] {ts_local:%Y-%m-%d %H:%M} | {dato['barra']} = Total {('S/ ' + f'{total:.2f}') if total is not None else 'N/D'} ({', '.join(motivo) or 'sin alerta'}).")

def main():
    if ONESHOT:
        ejecutar_iteracion()
        return
    while True:
        ejecutar_iteracion()
        time.sleep(INTERVALO_MINUTOS * 60)

if __name__ == "__main__":
    main()


