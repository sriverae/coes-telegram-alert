# -*- coding: utf-8 -*-
"""
Alerta COES → Telegram (CHICLAYO 220) con Playwright
Modo: Exportar Masivo → leer Excel en memoria
"""

import os, time, json, re, io
import pandas as pd
import requests
from datetime import datetime, time as dtime
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

# ==================== UTILIDADES HORARIO ====================

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

# ==================== GIST (opcional) ====================

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

# ==================== PARSEO EXCEL EXPORTADO ====================

def _find_header_row_export(raw: pd.DataFrame) -> tuple[int, int]:
    """
    Devuelve (fila_encabezado, primera_columna_util)
    Busca una fila que contenga 'FECHA HORA' y 'NOMBRE BARRA' (o 'BARRA'/'NODO').
    """
    sraw = raw.astype(str).fillna("")
    hdr_idx = None
    for i in range(len(sraw)):
        row = " | ".join(sraw.loc[i].tolist()).upper()
        if ("FECHA HORA" in row or ("FECHA" in row and "HORA" in row)) and \
           (("NOMBRE BARRA" in row) or (" BARRA" in row) or ("NODO" in row)):
            hdr_idx = i
            break
    if hdr_idx is None:
        raise RuntimeError("No se encontró el encabezado en el Excel exportado (busqué 'FECHA HORA' y 'NOMBRE BARRA').")

    header_row = sraw.loc[hdr_idx].tolist()
    first_non_null = next((j for j, v in enumerate(header_row) if str(v).strip() not in ("", "nan", "None")), 0)
    return hdr_idx, first_non_null

def _clean_numeric_series(s: pd.Series) -> pd.Series:
    return (
        s.astype(str)
         .str.replace(",", ".", regex=False)
         .str.extract(r"([-]?\d+(?:\.\d+)?)")[0]
         .astype(float)
    )

def _norm_barra(s: str) -> str:
    if s is None:
        return ""
    t = str(s).upper()
    t = re.sub(r"\s+", "", t)
    t = t.replace(".", "")
    return t

def leer_excel_exportado_en_memoria(binary: bytes) -> pd.DataFrame:
    """
    Lee el Excel de 'Exportar Masivo' y retorna un DF con columnas:
    ['ts','Barra','CM_Energia','CM_Congestion','CM_Total']
    """
    # Intentar hoja específica; si cambia, leo la primera
    try:
        xls = pd.ExcelFile(io.BytesIO(binary))
        sheet = "COSTOMARGINAL" if "COSTOMARGINAL" in xls.sheet_names else xls.sheet_names[0]
        raw = pd.read_excel(io.BytesIO(binary), header=None, sheet_name=sheet, engine="openpyxl")
    except Exception:
        raw = pd.read_excel(io.BytesIO(binary), header=None, engine="openpyxl")

    hdr, c0 = _find_header_row_export(raw)
    data = raw.iloc[hdr+1:, c0:].copy()
    cols = [str(c).strip() for c in raw.iloc[hdr, c0:].tolist()]
    data.columns = cols
    data = data.dropna(how="all").copy()

    # Mapeo tolerante por nombres (con o sin acentos)
    def fcol(patterns):
        for p in patterns:
            for c in data.columns:
                if re.search(p, str(c), flags=re.I):
                    return c
        return None

    col_fh   = fcol([r"^fecha\s*hora$"])
    col_bar  = fcol([r"nombre\s*barra", r"\bbarra\b", r"nodo\s*emd", r"\bnodo\b"])
    col_en   = fcol([r"energ[ií]a"])
    col_con  = fcol([r"congesti[oó]n"])
    col_tot  = fcol([r"^total$"])

    if not (col_fh and col_bar and (col_tot or col_en or col_con)):
        raise RuntimeError("No se hallaron columnas esperadas (FECHA HORA, NOMBRE BARRA, ENERGÍA/CONGESTIÓN/TOTAL).")

    out = pd.DataFrame()
    out["Barra"] = data[col_bar].astype(str)
    # Valores numéricos
    if col_en:  out["CM_Energia"]    = _clean_numeric_series(data[col_en])
    if col_con: out["CM_Congestion"] = _clean_numeric_series(data[col_con])
    if col_tot: out["CM_Total"]      = _clean_numeric_series(data[col_tot])

    # Timestamp
    out["ts"] = pd.to_datetime(data[col_fh].astype(str), dayfirst=True, errors="coerce")
    # Localizar a Lima si viene naïve
    out["ts"] = out["ts"].dt.tz_localize(TZ, nonexistent="shift_forward", ambiguous="NaT", errors="coerce")

    # Filtrado de barra robusto
    objetivo = _norm_barra(BARRA_BUSCADA)
    out["Barra_norm"] = out["Barra"].map(_norm_barra)

    # ejemplos: "CHICLAYO 220", "CHICLAYO220", "CHICLAOE220" (código en NODO EMD)
    mask = (
        out["Barra_norm"].str.contains(objetivo, na=False)
        | out["Barra_norm"].str.contains("CHICLAYO220", na=False)
        | out["Barra_norm"].str.contains(r"CHICLAYO220K?V?", na=False)
    )
    filtered = out[mask].copy()
    if filtered.empty:
        ejemplos = ", ".join(out["Barra"].dropna().astype(str).unique()[:10])
        raise RuntimeError(f"No se halló la barra '{BARRA_BUSCADA}' en el Excel. Ejemplos: {ejemplos}")

    # Elegir la marca de tiempo más cercana hacia atrás a ahora
    now_lima = datetime.now(TZ)
    filtered = filtered.sort_values("ts")
    prev = filtered[filtered["ts"] <= now_lima]
    elegido = prev.iloc[-1] if not prev.empty else filtered.iloc[-1]

    return {
        "barra": str(elegido["Barra"]),
        "ts": elegido["ts"],
        "energia": float(elegido["CM_Energia"]) if "CM_Energia" in filtered.columns else None,
        "congestion": float(elegido["CM_Congestion"]) if "CM_Congestion" in filtered.columns else None,
        "total": float(elegido["CM_Total"]) if "CM_Total" in filtered.columns else None,
    }

# ==================== NAVEGACIÓN (EXPORTAR MASIVO) ====================

def obtener_ultimo_costo_por_export(timeout_ms=120000):
    """
    Flujo:
      - Cerrar modal "Aviso".
      - Click "Exportar Masivo".
      - Completar 'Fecha desde' y 'Hasta' con HOY (dd/mm/yyyy).
      - Click Aceptar y capturar el Excel en memoria.
      - Parsear y devolver dict con {barra, ts, energia, congestion, total}.
    Además, guarda step*.png para debug.
    """

    def _screenshot(page, name):
        try: page.screenshot(path=name)
        except Exception: pass

    today_str = datetime.now(TZ).strftime("%d/%m/%Y")

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page(viewport={"width": 1440, "height": 900})

        page.goto(URL_COSTOS_TIEMPO_REAL, wait_until="domcontentloaded", timeout=timeout_ms)
        page.wait_for_load_state("networkidle")
        _screenshot(page, "step1_loaded.png")

        # Cerrar aviso si aparece
        try:
            page.get_by_role("button", name=re.compile(r"^Aceptar$", re.I)).click(timeout=3000)
        except Exception:
            try:
                page.get_by_text(re.compile(r"Aceptar", re.I)).first.click(timeout=1500)
            except Exception:
                pass
        _screenshot(page, "step2_modal_closed.png")

        # Abrir Exportar Masivo
        try:
            page.get_by_role("button", name=re.compile(r"Exportar\s*Masivo", re.I)).click(timeout=6000)
        except Exception:
            page.get_by_text(re.compile(r"Exportar\s*Masivo", re.I)).first.click(timeout=6000)
        _screenshot(page, "step3_open_export_modal.png")

        # Intentar ubicar el contenedor del modal y los inputs
        modal = None
        for sel in [
            "xpath=//*[contains(.,'Exportar Datos')]/ancestor::div[contains(@class,'modal')][1]",
            "div.modal.show", "div.modal.in", "div[role='dialog']"
        ]:
            try:
                loc = page.locator(sel)
                if loc.count() > 0 and loc.first.is_visible():
                    modal = loc.first
                    break
            except Exception:
                pass
        if not modal:
            # fallback: usar el último modal visible en el DOM
            try:
                all_modals = page.locator("div.modal, div[role='dialog']")
                if all_modals.count() > 0:
                    modal = all_modals.last
            except Exception:
                pass

        if not modal:
            browser.close()
            raise RuntimeError("No se pudo localizar el modal de 'Exportar Datos'.")

        # Buscar inputs dentro del modal
        inputs = []
        try:
            # priorizar inputs cercanos a los textos 'Fecha desde' y 'Hasta'
            for label_txt in ["Fecha desde", "Desde", "Hasta"]:
                try:
                    lbl = modal.get_by_text(re.compile(label_txt, re.I)).first
                    if lbl:
                        candidate = lbl.locator("xpath=following::input[1]")
                        if candidate.count() > 0:
                            inputs.append(candidate.first)
                except Exception:
                    pass
            # completar con todos los inputs si no se encontraron suficientes
            if len(inputs) < 2:
                extra = modal.locator("input")
                for i in range(min(extra.count(), 4)):
                    el = extra.nth(i)
                    if el not in inputs:
                        inputs.append(el)
        except Exception:
            pass

        # Filtrar únicos visibles y tomar los dos primeros
        vis = []
        for el in inputs:
            try:
                if el.is_visible():
                    vis.append(el)
            except Exception:
                continue
        inputs = vis[:2]

        if len(inputs) < 2:
            browser.close()
            raise RuntimeError("No se encontraron campos 'Fecha desde/Hasta' en el modal de exportación.")

        # Completar ambas fechas (dd/mm/yyyy)
        for el in inputs:
            try:
                el.click()
                el.fill("")
                el.type(today_str, delay=15)
            except Exception:
                pass
        _screenshot(page, "step4_modal_filled.png")

        # Capturar descarga
        binary = None
        with page.expect_download(timeout=20000) as dl_info:
            # Botón Aceptar del modal
            try:
                modal.get_by_role("button", name=re.compile(r"Aceptar", re.I)).click(timeout=4000)
            except Exception:
                modal.get_by_text(re.compile(r"Aceptar", re.I)).first.click(timeout=4000)
        download = dl_info.value
        try:
            tmp_path = "export_debug.xlsx"
            download.save_as(tmp_path)
            with open(tmp_path, "rb") as f:
                binary = f.read()
        except Exception:
            # Fallback a usar stream
            pth = download.path()
            with open(pth, "rb") as f:
                binary = f.read()

        _screenshot(page, "step5_downloaded.png")
        browser.close()

    if not binary:
        raise RuntimeError("No se pudo capturar el archivo exportado.")

    # Parsear Excel y devolver valores
    return leer_excel_exportado_en_memoria(binary)

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
        if total is not None and total <= UMBRAL_S_POR_MWH: motivo.append("<= umbral")
        if not es_nuevo: motivo.append("dato repetido")
        if not en_horario_sonido(ahora): motivo.append("horario silencioso")
        print(f"[INFO] {ts_local:%Y-%m-%d %H:%M} | {dato['barra']} = Total S/ {total if total is not None else float('nan'):.2f} ({', '.join(motivo) or 'sin alerta'}).")

def main():
    if ONESHOT:
        ejecutar_iteracion()
        return
    while True:
        ejecutar_iteracion()
        time.sleep(INTERVALO_MINUTOS * 60)

if __name__ == "__main__":
    main()

