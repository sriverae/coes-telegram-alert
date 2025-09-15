# -*- coding: utf-8 -*-
"""
Alerta COES → Telegram (CHICLAYO 220) usando Exportar Masivo
- One-shot amigable para GitHub Actions
- Captura el Excel en memoria (Playwright expect_download)
- Lee columnas Energía, Congestión y Total de forma robusta
- Selecciona la hora más cercana por debajo a la hora actual (pasos de 30 min)
- Guarda step*.png y export_debug.xlsx para debug
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

# =============== Config ===============
BARRA_BUSCADA = os.getenv("BARRA_BUSCADA", "CHICLAYO 220")
UMBRAL_S_POR_MWH = float(os.getenv("UMBRAL_S_POR_MWH", "400"))
INTERVALO_MINUTOS = int(os.getenv("INTERVALO_MINUTOS", "30"))
TZ = ZoneInfo(os.getenv("TZ", "America/Lima"))
SILENCIO_DESDE = os.getenv("SILENCIO_DESDE")  # "22:00"
SILENCIO_HASTA = os.getenv("SILENCIO_HASTA")  # "06:30"
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

# =============== Utiles horario ===============
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

# =============== Gist state (opcional) ===============
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

# =============== Telegram ===============
def enviar_telegram(mensaje: str):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        raise RuntimeError("Faltan TELEGRAM_BOT_TOKEN o TELEGRAM_CHAT_ID.")
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": mensaje, "parse_mode": "HTML", "disable_web_page_preview": True}
    r = requests.post(url, json=payload, timeout=30)
    r.raise_for_status()
    return r.json()

# =============== Normalización barra ===============
def _norm_barra(s: str) -> str:
    if s is None:
        return ""
    t = str(s).upper()
    t = re.sub(r"\s+", "", t)
    t = t.replace(".", "")
    return t

def filtrar_barra_robusto(df: pd.DataFrame, barra_objetivo: str) -> pd.DataFrame:
    objetivo = _norm_barra(barra_objetivo)
    df = df.copy()
    df["Barra_norm"] = df["Barra"].map(_norm_barra)

    ciudad = _norm_barra("CHICLAYO")
    candidatos = df[
        df["Barra_norm"].str.contains(objetivo, na=False)
        | df["Barra_norm"].str.contains(ciudad, na=False)
        | df["Barra_norm"].str.contains("CHICLAYO220", na=False)
        | df["Barra_norm"].str.contains("CHICLAYO220K?V?", na=False, regex=True)
    ]

    if candidatos.empty:
        ejemplos = ", ".join(df["Barra"].dropna().astype(str).unique()[:10])
        raise RuntimeError(f"No se halló la barra '{barra_objetivo}'. Ejemplos en archivo: {ejemplos}")

    con220 = candidatos[candidatos["Barra_norm"].str.contains("220", na=False)]
    return con220 if not con220.empty else candidatos

# =============== Lectura Excel Exportado ===============
def _find_header_row(raw: pd.DataFrame) -> int:
    sraw = raw.astype(str)
    has_hora  = sraw.apply(lambda r: r.str.contains(r"\bhora\b", case=False, na=False).any(), axis=1)
    has_barra = sraw.apply(lambda r: r.str.contains(r"\b(barra|nodo|punto)\b", case=False, na=False).any(), axis=1)
    idxs = sraw.index[(has_hora) & (has_barra)].tolist()
    if not idxs:
        raise RuntimeError("No se encontró encabezado con 'Hora' y 'Barra/Nodo' en el Excel exportado.")
    return idxs[0]

def leer_excel_exportado_en_memoria(binary: bytes) -> pd.DataFrame:
    """Lee el Excel exportado por 'Exportar Masivo' (formato 'CostosMarginalesNodales') de forma robusta."""
    raw = pd.read_excel(io.BytesIO(binary), header=None, engine="openpyxl")
    hdr = _find_header_row(raw)
    header_row = raw.loc[hdr, :].tolist()
    first_non_null = next((i for i, v in enumerate(header_row) if pd.notna(v)), 0)
    cols = [str(c).strip() for c in raw.loc[hdr, first_non_null:].tolist()]
    data = raw.loc[hdr+1:, first_non_null:].copy()
    data.columns = cols
    data = data.dropna(how="all")

    def find_col(patterns):
        if isinstance(patterns, str):
            patterns = [patterns]
        for p in patterns:
            for c in data.columns:
                if re.search(p, str(c), re.I):
                    return c
        return None

    col_fecha = find_col([r"\bfecha\b"])
    col_hora  = find_col([r"\bhora\b"])
    col_barra = find_col([r"\b(barra|nodo|punto)\b"])
    col_cm_energia = find_col([r"\bcm\b.*energ", r"costo.*energ"])
    col_cm_cong    = find_col([r"\bcm\b.*conges", r"costo.*conges"])
    col_cm_total   = find_col([r"\bcm\b.*total", r"costo.*marginal.*total"])

    if not (col_hora and col_barra and (col_cm_total or col_cm_energia or col_cm_cong)):
        raise RuntimeError("Faltan columnas clave (Hora/Barra y alguna de CM Energía/Congestión/Total).")

    keep = [c for c in [col_fecha, col_hora, col_barra, col_cm_energia, col_cm_cong, col_cm_total] if c]
    df = data[keep].copy()

    rename_map = {col_barra: "Barra", col_hora: "Hora"}
    if col_fecha:      rename_map[col_fecha] = "Fecha"
    if col_cm_energia: rename_map[col_cm_energia] = "CM_Energia"
    if col_cm_cong:    rename_map[col_cm_cong]    = "CM_Congestion"
    if col_cm_total:   rename_map[col_cm_total]   = "CM_Total"
    df = df.rename(columns=rename_map)

    # Normaliza valores numéricos
    for c in ["CM_Energia", "CM_Congestion", "CM_Total"]:
        if c in df.columns:
            df[c] = (
                df[c].astype(str)
                    .str.replace(",", ".", regex=False)
                    .str.extract(r"([-]?[0-9]+(?:\.[0-9]+)?)")[0]
                    .astype(float)
            )

    # ts = Fecha + Hora (o solo Hora si ya viene fecha embebida)
    if "Fecha" in df.columns:
        df["ts"] = pd.to_datetime(df["Fecha"].astype(str).str.strip() + " " + df["Hora"].astype(str).str.strip(),
                                  dayfirst=True, errors="coerce")
    else:
        df["ts"] = pd.to_datetime(df["Hora"], dayfirst=True, errors="coerce")

    return df

# =============== Select filtro (por si aplica) ===============
def _select_filter_barras_138(page):
    """Intenta seleccionar el filtro 'Barras mayores a 138' (texto flexible)."""
    selects = page.locator("select")
    try:
        n = selects.count()
    except:
        n = 0
    for i in range(n):
        sel = selects.nth(i)
        try:
            opts = sel.locator("option")
            texts = opts.all_inner_texts()
            for idx, t in enumerate(texts):
                if re.search(r"barras\s+mayores?\s+a\s*138", t or "", re.I):
                    val = opts.nth(idx).get_attribute("value")
                    if val and val.strip():
                        sel.select_option(val)
                        return True
        except Exception:
            pass
    return False

# =============== Flujo principal (Exportar Masivo) ===============
def obtener_ultimo_costo_por_export(timeout_ms=180000):
    """
    Flujo:
      1) Abrir página y cerrar 'Aviso' → step1/step2.
      2) (opcional) Filtro 'Barras mayores a 138 kV'.
      3) Click 'Exportar Masivo' → aparece modal → step3.
      4) Rellenar 'Fecha desde' y 'Hasta' con HOY (dd/mm/yyyy) → step4.
      5) Click 'Aceptar' y capturar el Excel vía expect_download → step5 + export_debug.xlsx.
      6) Parsear Excel en memoria, filtrar 'CHICLAYO 220', elegir hora más cercana por debajo a ahora.
      7) Retornar energía, congestión, total, ts y barra.
    """
    def _screenshot(page, name):
        try: page.screenshot(path=name)
        except Exception: pass

    hoy_str = datetime.now(TZ).strftime("%d/%m/%Y")

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page(viewport={"width": 1440, "height": 900})

        page.goto(URL_COSTOS_TIEMPO_REAL, wait_until="domcontentloaded", timeout=timeout_ms)
        page.wait_for_load_state("networkidle")
        _screenshot(page, "step1_loaded.png")

        # Cerrar modal "Aviso"
        for sel in [r"^Aceptar$", "Aceptar", "×", "X"]:
            try:
                page.get_by_role("button", name=re.compile(sel, re.I)).click(timeout=2000)
                break
            except Exception:
                try:
                    page.get_by_text(re.compile(sel, re.I)).first.click(timeout=2000)
                    break
                except Exception:
                    pass
        page.wait_for_timeout(300)
        _screenshot(page, "step2_modal_closed.png")

        # (Opcional) aplicar filtro
        try:
            _select_filter_barras_138(page)
        except Exception:
            pass

        # Abrir "Exportar Masivo" (el botón de la derecha)
        opened = False
        for try_sel in [r"^Exportar\s*Masivo$", "Exportar Masivo", "Exportar\s*Masivo"]:
            try:
                page.get_by_role("button", name=re.compile(try_sel, re.I)).click(timeout=4000)
                opened = True
                break
            except Exception:
                try:
                    page.locator(f"button:has-text('{try_sel}')").first.click(timeout=4000)
                    opened = True
                    break
                except Exception:
                    pass
        if not opened:
            browser.close()
            raise RuntimeError("No se pudo abrir 'Exportar Masivo'.")

        page.wait_for_timeout(400)
        _screenshot(page, "step3_open_export_modal.png")

        # Localizar inputs dentro del modal
        def _loc_fecha(label_texts):
            for t in label_texts:
                try:
                    loc = page.locator(f"xpath=//div[contains(@class,'modal') or contains(@class,'swal2-container')]//label[contains(.,'{t}')]/following::input[1]")
                    if loc.count() > 0:
                        return loc.first
                except Exception:
                    pass
            # fallback: primeros dos inputs visibles dentro del modal
            try:
                modal_inputs = page.locator("div.modal-dialog input, div.modal-content input").filter(has_text=None)
                if modal_inputs.count() >= 2:
                    return modal_inputs.nth(0)
            except Exception:
                pass
            return None

        fecha_desde = _loc_fecha(["Fecha desde", "Desde"])
        fecha_hasta = None
        # intentar ubicar el segundo campo por label directa
        for t in ["Hasta", "Fecha hasta"]:
            try:
                loc = page.locator(f"xpath=//div[contains(@class,'modal') or contains(@class,'swal2-container')]//label[contains(.,'{t}')]/following::input[1]")
                if loc.count() > 0:
                    fecha_hasta = loc.first
                    break
            except Exception:
                pass
        # fallback: segundo input
        if not fecha_hasta:
            try:
                modal_inputs = page.locator("div.modal-dialog input, div.modal-content input").filter(has_text=None)
                if modal_inputs.count() >= 2:
                    fecha_hasta = modal_inputs.nth(1)
            except Exception:
                pass

        if not (fecha_desde and fecha_hasta):
            browser.close()
            raise RuntimeError("No se encontraron campos 'Fecha desde/Hasta' en el modal de exportación.")

        # Rellenar dd/mm/yyyy y disparar eventos
        for campo in [fecha_desde, fecha_hasta]:
            try:
                campo.click()
                campo.fill("")
                campo.type(hoy_str, delay=15)
                page.evaluate("(el)=>{el.dispatchEvent(new Event('input',{bubbles:true}));el.dispatchEvent(new Event('change',{bubbles:true}));}", campo)
            except Exception:
                pass

        page.wait_for_timeout(200)
        _screenshot(page, "step4_filled_export_dates.png")

        # Click Aceptar y capturar la descarga
        excel_bytes = None
        try:
            with page.expect_download(timeout=30000) as dl_info:
                # botón Aceptar dentro del modal
                clicked = False
                for t in [r"^Aceptar$", "Aceptar", "Exportar"]:
                    try:
                        page.get_by_role("button", name=re.compile(t, re.I)).click(timeout=3000)
                        clicked = True
                        break
                    except Exception:
                        try:
                            page.locator(f"xpath=//div[contains(@class,'modal')]//button[contains(.,'{t}')]").first.click(timeout=3000)
                            clicked = True
                            break
                        except Exception:
                            pass
                if not clicked:
                    raise RuntimeError("No se pudo hacer clic en 'Aceptar' del modal de exportación.")
            download = dl_info.value
            # Guardar a disco para artifact y obtener bytes en memoria
            try:
                download.save_as("export_debug.xlsx")
            except Exception:
                pass
            try:
                excel_bytes = download.content()
            except Exception:
                # fallback a leer del archivo guardado
                try:
                    with open("export_debug.xlsx", "rb") as f:
                        excel_bytes = f.read()
                except Exception:
                    excel_bytes = None
        except Exception as e:
            browser.close()
            raise RuntimeError(f"Fallo al capturar la descarga del Excel: {e}")

        page.wait_for_timeout(400)
        _screenshot(page, "step5_download_done.png")
        browser.close()

    if not excel_bytes:
        raise RuntimeError("No se pudo obtener el contenido del Excel exportado.")

    # ---- Parseo Excel ----
    df = leer_excel_exportado_en_memoria(excel_bytes)

    # Filtrar por barra
    df = filtrar_barra_robusto(df, BARRA_BUSCADA)
    if df.empty:
        raise RuntimeError(f"No se obtuvo ningún registro para '{BARRA_BUSCADA}' en el Excel.")

    # Seleccionar hora más cercana por debajo (saltos de 30 min)
    ahora = datetime.now(TZ)
    df = df.dropna(subset=["ts"]).copy()
    if df.empty:
        raise RuntimeError("El Excel no contiene una columna de tiempo legible ('ts').")

    # solo filas de HOY (por si vienen varias fechas)
    df["ts_local"] = df["ts"].dt.tz_localize(TZ, nonexistent='shift_forward', ambiguous='NaT') if df["ts"].dt.tz is None else df["ts"].dt.tz_convert(TZ)
    df_hoy = df[df["ts_local"].dt.date == ahora.date()]
    if df_hoy.empty:
        df_hoy = df  # si no hay explícito de hoy, usamos todo

    candidatos = df_hoy[df_hoy["ts_local"] <= ahora].sort_values("ts_local")
    row = candidatos.iloc[-1] if not candidatos.empty else df_hoy.sort_values("ts_local").iloc[-1]

    energia    = float(row["CM_Energia"])   if "CM_Energia"   in row.index and pd.notna(row["CM_Energia"])   else None
    congestion = float(row["CM_Congestion"])if "CM_Congestion"in row.index and pd.notna(row["CM_Congestion"])else None
    total      = float(row["CM_Total"])     if "CM_Total"     in row.index and pd.notna(row["CM_Total"])     else None
    if total is None:
        # si no viene total, suma energía + congestión si existen
        if (energia is not None) and (congestion is not None):
            total = energia + congestion
        else:
            raise RuntimeError("No fue posible determinar 'CM_Total'.")

    ts = row["ts_local"]
    barra = row["Barra"]

    return {"barra": barra, "ts": ts, "energia": energia, "congestion": congestion, "total": total}

# =============== LOOP / Mensaje ===============
def ejecutar_iteracion():
    estado = cargar_estado()
    dato = obtener_ultimo_costo_por_export()

    energia    = dato.get("energia")
    congestion = dato.get("congestion")
    total      = dato["total"]
    ts_local   = dato["ts"].astimezone(TZ)

    ahora = datetime.now(TZ)
    lineas = [
        f"<b>COES</b> • <b>{dato['barra']}</b>",
        f"<b>{ts_local:%Y-%m-%d %H:%M}</b> (America/Lima)",
    ]
    if energia is not None:
        lineas.append(f"CM Energía: <b>S/ {energia:,.2f}</b> / MWh")
    if congestion is not None:
        lineas.append(f"CM Congestión: <b>S/ {congestion:,.2f}</b> / MWh")
    lineas.append(f"CM Total: <b>S/ {total:,.2f}</b> / MWh")
    mensaje = "\n".join(lineas)

    es_nuevo = (
        str(estado.get("ultimo_registro_hora")) != ts_local.strftime("%Y-%m-%d %H:%M")
        or estado.get("ultimo_valor") != total
    )

    if total > UMBRAL_S_POR_MWH and es_nuevo and en_horario_sonido(ahora):
        enviar_telegram(mensaje + "\n\n⚠️ Superó el umbral configurado (S/ {:.2f} por MWh).".format(UMBRAL_S_POR_MWH))
        estado["ultimo_envio_ts"] = ahora.isoformat()
        estado["ultimo_registro_hora"] = ts_local.strftime("%Y-%m-%d %H:%M")
        estado["ultimo_valor"] = total
        guardar_estado(estado)
        print(f"[OK] Alerta enviada.")
    else:
        motivo = []
        if total <= UMBRAL_S_POR_MWH: motivo.append("<= umbral")
        if not es_nuevo: motivo.append("dato repetido")
        if not en_horario_sonido(ahora): motivo.append("horario silencioso")
        print(f"[INFO] {ts_local:%Y-%m-%d %H:%M} | {dato['barra']} = Total S/ {total:.2f} ({', '.join(motivo) or 'sin alerta'}).")

def main():
    if ONESHOT:
        ejecutar_iteracion()
        return
    while True:
        ejecutar_iteracion()
        time.sleep(INTERVALO_MINUTOS * 60)

if __name__ == "__main__":
    main()

