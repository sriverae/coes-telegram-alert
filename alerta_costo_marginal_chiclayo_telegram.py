# -*- coding: utf-8 -*-
"""
Alerta COES → Telegram (CHICLAYO 220) con Playwright
Flujo: Buscar → Datos → Exportar Excel → Parsear Excel
"""

import os, time, json, re, io
import pandas as pd
import requests
from datetime import datetime, time as dtime, timedelta
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

# ==================== UTILS ====================

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

# -------------------- Gist helpers --------------------

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

# -------------------- Normalización barra --------------------

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
    if "Barra" not in df.columns:
        raise RuntimeError("El Excel exportado no contiene columna 'Barra/Nodo/Punto' reconocible.")
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

# -------------------- Excel parser (flexible) --------------------

def _col(df_cols, *pats):
    for c in df_cols:
        s = str(c)
        if any(re.search(p, s, re.I) for p in pats):
            return c
    return None

def _excel_hora_to_hhmm(val) -> str | None:
    if pd.isna(val):
        return None
    if isinstance(val, (int, float)):
        total_min = int(round(float(val) * 24 * 60))
        hh, mm = divmod(total_min, 60)
        return f"{hh:02d}:{mm:02d}"
    s = str(val).strip()
    m = re.search(r"(\d{1,2}):(\d{2})", s)
    if m:
        return f"{int(m.group(1)):02d}:{int(m.group(2)):02d}"
    # último recurso: puede venir como datetime
    try:
        dt = pd.to_datetime(val, errors="coerce")
        if pd.notna(dt):
            return f"{dt.hour:02d}:{dt.minute:02d}"
    except Exception:
        pass
    return None

def leer_excel_exportado_en_memoria(binary: bytes) -> pd.DataFrame:
    # lee sin asumir encabezado en primera fila
    raw = pd.read_excel(io.BytesIO(binary), header=None, engine="openpyxl")
    sraw = raw.astype(str)

    # tratar de ubicar fila de encabezados
    fila_hdr = None
    for i in range(len(sraw)):
        fila = " | ".join(list(sraw.iloc[i, :]))
        if re.search(r"\bhora\b", fila, re.I) and (re.search(r"\bbarra\b|\bnodo\b|\bpunto\b", fila, re.I)):
            fila_hdr = i
            break
    if fila_hdr is None:
        # si no encuentra, asumir primera fila como header
        fila_hdr = 0

    header_row = raw.iloc[fila_hdr, :].tolist()
    first_non_null = next((i for i, v in enumerate(header_row) if pd.notna(v)), 0)
    cols = [str(c).strip() for c in raw.iloc[fila_hdr, first_non_null:].tolist()]

    data = raw.iloc[fila_hdr + 1:, first_non_null:].copy()
    data.columns = cols
    data = data.dropna(how="all")

    # mapear nombres
    col_barra = _col(data.columns, r"\bbarra\b", r"\bnodo\b", r"\bpunto\b")
    col_hora  = _col(data.columns, r"\bhora\b")
    col_fecha = _col(data.columns, r"\b(fecha|día)\b")
    col_en    = _col(data.columns, r"cm\s*energ", r"costo\s*marginal\s*energ")
    col_cg    = _col(data.columns, r"cm\s*conges")
    col_to    = _col(data.columns, r"cm\s*total", r"costo\s*marginal\s*total")

    if not col_barra or not col_hora or not col_to:
        raise RuntimeError("Excel exportado sin columnas clave (Barra/Nodo, Hora, CM Total).")

    keep = [c for c in [col_fecha, col_hora, col_barra, col_en, col_cg, col_to] if c is not None]
    df = data[keep].copy()

    rename = {col_barra: "Barra", col_hora: "Hora", col_to: "CM_Total"}
    if col_fecha: rename[col_fecha] = "Fecha"
    if col_en:    rename[col_en]    = "CM_Energia"
    if col_cg:    rename[col_cg]    = "CM_Congestion"
    df = df.rename(columns=rename)

    # limpiar números
    for c in ["CM_Energia", "CM_Congestion", "CM_Total"]:
        if c in df.columns:
            df[c] = (
                df[c]
                .astype(str)
                .str.replace("\u00a0", " ", regex=False)
                .str.replace(",", ".", regex=False)
                .str.extract(r"([-]?\d+(?:\.\d+)?)")[0]
                .astype(float, errors="ignore")
            )

    # normalizar Hora / Fecha → ts
    if "Hora" in df.columns:
        df["Hora_norm"] = df["Hora"].map(_excel_hora_to_hhmm)
    else:
        df["Hora_norm"] = None

    if "Fecha" in df.columns:
        base_fecha = df["Fecha"].astype(str).str.replace("\u00a0", " ", regex=False).str.strip()
    else:
        # si el Excel no trae fecha, usar HOY (la consulta siempre es del día)
        base_fecha = pd.Series([datetime.now(TZ).strftime("%d/%m/%Y")] * len(df))

    df["ts"] = pd.to_datetime(
        (base_fecha + " " + df["Hora_norm"].fillna("").astype(str)).str.strip(),
        dayfirst=True, errors="coerce"
    )
    # fallback si arriba fallara
    if df["ts"].isna().all() and "Hora" in df.columns:
        df["ts"] = pd.to_datetime(df["Hora"].astype(str), dayfirst=True, errors="coerce")

    # proyectar a TZ local
    df["ts"] = df["ts"].dt.tz_localize(TZ, nonexistent="NaT", ambiguous="NaT", errors="coerce")

    # estandarizar nombre de barra
    df["Barra"] = df["Barra"].astype(str)

    return df.dropna(how="all")

# ==================== WEB (Exportar Excel) ====================

def obtener_ultimo_costo_por_export(timeout_ms=120000):
    """
    Flujo robusto (sin pelear con la hora en UI):
      1) Ir a la URL, cerrar 'Aceptar' (si aparece).   → step1/step2
      2) Poner FECHA de HOY (dd/mm/yyyy).              → step2
         (solo para que el sitio cargue el día correcto)
      3) (Auditoría) Abrir desplegable de horas si existe. → step2a
      4) Click 'Buscar' y luego 'Datos'.                → step3/step4
      5) Click 'Exportar Excel' y capturar descarga.    → step5
      6) Leer 'export_debug.xlsx', filtrar barra y elegir la
         hora más cercana <= ahora; si no hay, bajar 30 min sucesivamente.
    """
    def _screenshot(page, name):
        try: page.screenshot(path=name)
        except Exception: pass

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
                page.locator(f"input[type='button'][value*='{t}']").first.click(timeout=timeout); return True
            except Exception: pass
            try:
                page.locator(f"a:has-text('{t}')").first.click(timeout=timeout); return True
            except Exception: pass
        return False

    def _cerrar_aviso(page):
        _click_possibles(page, [r"^Aceptar$", "Aceptar", r"×", r"X"])
        page.wait_for_timeout(200)

    def _fecha_input(page):
        for sel in ["#txtFecha", "#TxtFecha", "#fecha",
                    "input[name='fecha']", "input[placeholder*='Fecha' i]",
                    "xpath=//label[contains(normalize-space(.),'Fecha')]/following::input[1]"]:
            try:
                loc = page.locator(sel)
                if loc.count() > 0:
                    return loc.first
            except Exception:
                pass
        return None

    def _set_fecha_hoy(page):
        hoy = datetime.now(TZ).strftime("%d/%m/%Y")
        inp = _fecha_input(page)
        if not inp: return
        try:
            inp.click(); inp.fill(""); inp.type(hoy, delay=20)
            # disparar eventos para que el sitio calcule
            page.evaluate("(el)=>{el.dispatchEvent(new Event('input',{bubbles:true}));el.dispatchEvent(new Event('change',{bubbles:true}));}", inp)
            inp.press("Enter")
            page.wait_for_timeout(250)
        except Exception:
            pass

    def _abrir_dropdown_horas(page):
        # Solo para step de auditoría (no dependemos de fijar hora)
        for css in ["#cbHoras", "select[name='hora']"]:
            try:
                loc = page.locator(css)
                if loc.count() > 0 and loc.first.is_visible():
                    loc.first.click(); _screenshot(page, "step2a_horas_dropdown.png")
                    break
            except Exception:
                continue

    def _abrir_datos(page):
        if _click_possibles(page, [r"^Datos$", "Datos"], timeout=6000): return True
        try:
            page.locator("[data-fuente='datos'], [aria-controls*=Datos], [data-target*=Datos]").first.click(timeout=6000)
            return True
        except Exception:
            return False

    def _click_export(page):
        # Intentar variantes comunes del botón de exportación
        textos = [r"^Exportar\s*Excel$", r"Exportar Excel", r"^Exportar$", r"Excel"]
        if _click_possibles(page, textos, timeout=8000): return True
        # enlaces / iconos
        for sel in [
            "a[download$='.xlsx']", "a[href*='Export']",
            "button:has(svg)", "a:has(svg)"
        ]:
            try:
                loc = page.locator(sel)
                if loc.count() > 0 and loc.first.is_visible():
                    loc.first.click(timeout=6000); return True
            except Exception:
                pass
        return False

    # --------- Navegación + descarga ---------
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(accept_downloads=True)
        page = context.new_page(viewport={"width": 1366, "height": 900})

        page.goto(URL_COSTOS_TIEMPO_REAL, wait_until="domcontentloaded", timeout=timeout_ms)
        page.wait_for_load_state("networkidle")
        _screenshot(page, "step1_loaded.png")

        _cerrar_aviso(page)
        _screenshot(page, "step2_modal_closed.png")

        _set_fecha_hoy(page)
        _abrir_dropdown_horas(page)  # solo para el step de evidencia

        # Buscar
        _click_possibles(page, [r"^Buscar$", "Buscar"])
        page.wait_for_load_state("networkidle")
        page.wait_for_timeout(600)
        _screenshot(page, "step3_clicked_buscar.png")

        # Datos
        if not _abrir_datos(page):
            _screenshot(page, "step4_no_datos_tab.png")
            context.close(); browser.close()
            raise RuntimeError("No se pudo abrir la pestaña 'Datos'.")
        page.wait_for_timeout(600)
        _screenshot(page, "step4_tab_datos.png")

        # Exportar Excel (capturar download)
        save_to = "export_debug.xlsx"
        try:
            with page.expect_download(timeout=20000) as dl_info:
                clicked = _click_export(page)
                if not clicked:
                    _screenshot(page, "step5_export_not_found.png")
                    context.close(); browser.close()
                    raise RuntimeError("No se encontró el botón de 'Exportar Excel'.")
            download = dl_info.value
            # Guardar a un nombre estable para los artifacts
            download.save_as(save_to)
            _screenshot(page, "step5_export_clicked.png")
        except Exception as e:
            _screenshot(page, "step5_export_failed.png")
            context.close(); browser.close()
            raise RuntimeError(f"Falló la exportación del Excel: {e}")

        # Leer binarios
        try:
            with open(save_to, "rb") as f:
                binary = f.read()
        except Exception as e:
            context.close(); browser.close()
            raise RuntimeError(f"No se pudo abrir el Excel descargado: {e}")

        context.close()
        browser.close()

    # --------- Parseo y elección de hora ---------
    df = leer_excel_exportado_en_memoria(binary)

    # mapear columnas de nombres alternos -> estándar
    def _map_barra(df):
        for c in df.columns:
            if re.search(r"\b(Barra|Nodo|Punto)\b", str(c), re.I):
                if c != "Barra":
                    df = df.rename(columns={c: "Barra"})
                return df
        return df

    df = _map_barra(df)

    # Filtro de barra robusto
    df = filtrar_barra_robusto(df, BARRA_BUSCADA)

    if "ts" not in df.columns or df["ts"].isna().all():
        # construir ts a partir de hoy + Hora_norm si todo falló
        hoy = datetime.now(TZ).strftime("%d/%m/%Y")
        if "Hora_norm" in df.columns:
            df["ts"] = pd.to_datetime(hoy + " " + df["Hora_norm"].fillna("00:00"), dayfirst=True, errors="coerce").dt.tz_localize(TZ)
        else:
            raise RuntimeError("No se pudo construir la columna temporal (ts) desde el Excel.")

    # Elegir la hora mas cercana hacia atrás (<= ahora). Si no hay, descender en bloques de 30 min.
    ahora = datetime.now(TZ)
    df_ok = df[df["ts"] <= ahora].copy()
    if df_ok.empty:
        # todo futuro → elegir el mínimo ts disponible
        ts_pick = df["ts"].min()
    else:
        ts_pick = df_ok["ts"].max()

    # si aun así NaT, bajar manualmente 30m hasta encontrar
    if pd.isna(ts_pick):
        cand = ahora
        found = None
        for _ in range(48):  # hasta 24 horas hacia atrás
            cand -= timedelta(minutes=30)
            tmp = df[df["ts"] == cand]
            if not tmp.empty:
                found = cand
                break
        ts_pick = found if found else df["ts"].dropna().max()

    if pd.isna(ts_pick):
        raise RuntimeError("No se encontró un registro horario válido en el Excel exportado.")

    # quedarnos con esa hora
    elegido = df[df["ts"] == ts_pick].copy()
    if elegido.empty:
        # buscar el más cercano hacia abajo
        elegido = df.sort_values("ts").iloc[[-1]]

    row = elegido.iloc[0]

    energia = float(row["CM_Energia"]) if "CM_Energia" in elegido.columns and pd.notna(row["CM_Energia"]) else None
    congestion = float(row["CM_Congestion"]) if "CM_Congestion" in elegido.columns and pd.notna(row["CM_Congestion"]) else None
    total = float(row["CM_Total"]) if pd.notna(row["CM_Total"]) else None

    ts = row["ts"]
    if ts is not None and ts.tzinfo is None:
        ts = ts.tz_localize(TZ)

    return {"barra": row["Barra"], "ts": ts, "energia": energia, "congestion": congestion, "total": total}

# ==================== LOOP ====================

def ejecutar_iteracion():
    estado = cargar_estado()
    dato = obtener_ultimo_costo_por_export()
    energia = dato.get("energia")
    congestion = dato.get("congestion")
    total = dato["total"]
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

