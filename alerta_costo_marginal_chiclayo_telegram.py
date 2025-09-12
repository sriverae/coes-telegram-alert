# -*- coding: utf-8 -*-
"""
Alerta COES → Telegram (CHICLAYO 220) con Playwright
v3: ONE-SHOT para GitHub Actions + Persistencia opcional en Gist
(12/09/2025) Fix: fecha dd/mm/yyyy + selección manual de hora (bloques de 30min) con backoff.
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
    "https://www.coes.org.pe/Portal/mercadomayorista/costosmarginales/index",
)

# --------------------------------------------------------------------------
# Utilidades varias
# --------------------------------------------------------------------------

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

def _gist_headers():
    return {
        "Authorization": f"Bearer {GIST_TOKEN}",
        "Accept": "application/vnd.github+json",
    } if GIST_TOKEN else {}

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

# --------------------------------------------------------------------------
# Lectura de Excel/HTML (se mantienen)
# --------------------------------------------------------------------------

def _find_header_row(raw: pd.DataFrame) -> int:
    sraw = raw.astype(str)
    has_hora  = sraw.apply(lambda r: r.str.contains(r"\bhora\b", case=False, na=False).any(), axis=1)
    has_barra = sraw.apply(lambda r: r.str.contains(r"\bbarra\b", case=False, na=False).any(), axis=1)
    idxs = sraw.index[(has_hora) & (has_barra)].tolist()
    if not idxs:
        raise RuntimeError("No se encontró encabezado con 'Hora' y 'Barra' en el Excel exportado.")
    return idxs[0]

def leer_excel_exportado_en_memoria(binary: bytes) -> pd.DataFrame:
    raw = pd.read_excel(io.BytesIO(binary), header=None, engine="openpyxl")
    hdr = _find_header_row(raw)
    header_row = raw.loc[hdr, :].tolist()
    first_non_null = next((i for i, v in enumerate(header_row) if pd.notna(v)), 0)
    cols = [str(c).strip() for c in raw.loc[hdr, first_non_null:].tolist()]
    data = raw.loc[hdr+1:, first_non_null:].copy()
    data.columns = cols
    data = data.dropna(how="all")

    def find_col(pattern):
        return next((c for c in data.columns if re.search(pattern, str(c), re.I)), None)

    col_hora = find_col(r"\bhora\b")
    col_barra = find_col(r"\bbarra\b")
    col_cm_energia = find_col(r"cm\s*energ")
    col_cm_cong   = find_col(r"cm\s*conges")
    col_cm_total  = find_col(r"cm\s*total")

    if not (col_hora and col_barra and col_cm_total):
        raise RuntimeError("Faltan columnas clave (Hora, Barra, CM Total).")

    keep = [col_hora, col_barra, col_cm_total]
    if col_cm_energia: keep.insert(2, col_cm_energia)
    if col_cm_cong:    keep.insert(3 if col_cm_energia else 2, col_cm_cong)

    df = data[keep].copy()
    rename_map = {col_hora: "Hora", col_barra: "Barra", col_cm_total: "CM_Total"}
    if col_cm_energia: rename_map[col_cm_energia] = "CM_Energia"
    if col_cm_cong:    rename_map[col_cm_cong]    = "CM_Congestion"
    df = df.rename(columns=rename_map)

    for c in ["CM_Energia", "CM_Congestion", "CM_Total"]:
        if c in df.columns:
            df[c] = (
                df[c].astype(str)
                .str.replace(",", ".", regex=False)
                .str.extract(r"([-]?[0-9]+(?:\.[0-9]+)?)")[0]
                .astype(float)
            )
    df["ts"] = pd.to_datetime(df["Hora"], dayfirst=True, errors="coerce")
    return df

def leer_tabla_html(html: str) -> pd.DataFrame:
    import io as _io
    tablas = pd.read_html(_io.StringIO(html))
    if not tablas:
        raise RuntimeError("No se encontraron tablas en HTML.")

    barra_pats    = [r"\bbarra\b", r"\bnodo\b", r"\bpunto\b"]
    cm_total_pats = [r"cm\s*total", r"costo\s*marginal\s*total"]
    cm_ener_pats  = [r"cm\s*energ", r"costo\s*marginal\s*energ"]
    cm_cong_pats  = [r"cm\s*conges"]

    def _find_col(cols, pats):
        for c in cols:
            s = str(c)
            if any(re.search(p, s, re.I) for p in pats):
                return c
        return None

    for t in tablas:
        data = t.copy()
        cols = list(data.columns)

        col_barra = _find_col(cols, barra_pats)
        col_total = _find_col(cols, cm_total_pats)
        col_ener  = _find_col(cols, cm_ener_pats)
        col_cong  = _find_col(cols, cm_cong_pats)
        col_hora  = _find_col(cols, [r"\bhora\b"])

        if not col_barra or not (col_total or col_ener or col_cong):
            continue

        keep = [c for c in [col_hora, col_barra, col_ener, col_cong, col_total] if c is not None]
        df = data[keep].copy()

        rename_map = {col_barra: "Barra"}
        if col_hora:  rename_map[col_hora]  = "Hora"
        if col_ener:  rename_map[col_ener]  = "CM_Energia"
        if col_cong:  rename_map[col_cong]  = "CM_Congestion"
        if col_total: rename_map[col_total] = "CM_Total"
        df = df.rename(columns=rename_map)

        for c in ["CM_Energia", "CM_Congestion", "CM_Total"]:
            if c in df.columns:
                df[c] = (
                    df[c].astype(str)
                    .str.replace(",", ".", regex=False)
                    .str.extract(r"([-]?[0-9]+(?:\.[0-9]+)?)")[0]
                    .astype(float)
                )

        if "Hora" in df.columns:
            df["ts"] = pd.to_datetime(df["Hora"], dayfirst=True, errors="coerce")
        else:
            df["ts"] = pd.Timestamp.now(tz=TZ)

        return df

    raise RuntimeError("No se encontró una tabla HTML con columnas esperadas (Barra/Nodo y CM).")

# --------------------------------------------------------------------------
# Helpers DOM
# --------------------------------------------------------------------------

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

# --------------------------------------------------------------------------
# Navegación principal
# --------------------------------------------------------------------------

def obtener_ultimo_costo_por_export(timeout_ms=120000):
    """
    Flujo robusto:
      - Cierra modal.
      - FECHA = hoy (dd/mm/yyyy, America/Lima).
      - HORA = bloque de 30min más reciente; si falla, retrocede 30min (backoff).
      - Buscar → Datos → obtiene la tabla (con buscador o paginación) y devuelve el último registro.
    """
    from io import StringIO

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

    # --- Modal ---
    def _cerrar_aviso(page):
        _click_possibles(page, [r"^Aceptar$", "Aceptar", r"×", r"X"])
        page.wait_for_timeout(200)

    # --- Fecha hoy formato dd/mm/yyyy (dispara change para repoblar horas) ---
    def _set_fecha_hoy_ddmmyyyy(page):
        hoy = datetime.now(TZ).strftime("%d/%m/%Y")
        candidatos = ["#txtFecha", "#TxtFecha", "#fecha",
                      "input[name='fecha']", "input[placeholder*='Fecha' i]"]
        fecha = None
        for sel in candidatos:
            try:
                loc = page.locator(sel)
                if loc.count() > 0:
                    fecha = loc.first
                    break
            except Exception:
                pass
        if not fecha:
            try:
                fecha = page.locator("xpath=//label[contains(normalize-space(.),'Fecha')]/following::input[1]")
            except Exception:
                return False

        try:
            fecha.click()
            fecha.fill("")
            fecha.type(hoy, delay=20)
            # change + blur para que el sitio cargue el combo de horas
            page.evaluate("(el)=>el.dispatchEvent(new Event('change',{bubbles:true}))", fecha)
            fecha.blur()
            page.wait_for_timeout(250)
            return True
        except Exception:
            return False

    # --- Localiza el <select> de Hora (o el que contenga opciones HH:MM) ---
    def _locate_hora_select(page):
        # intentos directos comunes
        for css in ["#cbHoras", "select[name='hora']"]:
            try:
                loc = page.locator(css)
                if loc.count() > 0:
                    return loc.first
            except Exception:
                pass
        # fallback: por label
        try:
            loc = page.locator("xpath=//label[contains(normalize-space(.),'Hora')]/following::select[1]")
            if loc.count() > 0:
                return loc.first
        except Exception:
            pass
        # heurística: primer select con opciones HH:MM
        try:
            selects = page.locator("select")
            n = selects.count()
        except Exception:
            n = 0
        for i in range(n):
            s = selects.nth(i)
            try:
                txts = [t.strip() for t in s.locator("option").all_inner_texts()]
                if any(re.match(r"^\d{1,2}:\d{2}$", t) for t in txts):
                    return s
            except Exception:
                continue
        return None

    # --- Selecciona una hora exacta HH:MM en el combo, por label o value ---
    def _select_hora_exacta(page, hhmm: str) -> bool:
        sel = _locate_hora_select(page)
        if not sel:
            return False
        try:
            opts = sel.locator("option")
            cnt = opts.count()
        except Exception:
            cnt = 0
        target_value, target_label = None, None
        for j in range(cnt):
            try:
                txt = (opts.nth(j).inner_text() or "").strip()
                val = (opts.nth(j).get_attribute("value") or "").strip()
                if txt == hhmm or val == hhmm:
                    target_value = val or None
                    target_label = txt or None
                    break
            except Exception:
                continue
        if target_value or target_label:
            try:
                if target_value:
                    sel.select_option(value=target_value)
                else:
                    sel.select_option(label=target_label)
                page.wait_for_timeout(150)
                return True
            except Exception:
                return False
        return False

    # --- Genera candidatos 30min: ahora→atrás (p. ej. 17:55→17:30, 17:00→17:00, 16:30, 16:00 ...) ---
    def _generar_candidatos_hora(now_lima: datetime, max_slots=8):
        base = now_lima.replace(second=0, microsecond=0)
        minute = 30 if base.minute >= 30 else 0
        base = base.replace(minute=minute)
        cands = []
        t = base
        for _ in range(max_slots):
            cands.append(f"{t.hour:02d}:{t.minute:02d}")
            t -= timedelta(minutes=30)
        return cands

    # --- Click Buscar y valida si hubo “error” o si Hora quedó vacía ---
    def _buscar_y_ok(page) -> bool:
        _click_possibles(page, [r"^Buscar$", "Buscar"])
        page.wait_for_load_state("networkidle")
        page.wait_for_timeout(500)
        # error visible?
        try:
            if page.locator("text=/Se ha producido un error/i").first.is_visible():
                return False
        except Exception:
            pass
        # hora vacía?
        try:
            sel = _locate_hora_select(page)
            if sel:
                val = (sel.input_value() or "").strip()
                if not val or re.match(r"^Seleccione", val, re.I):
                    return False
        except Exception:
            pass
        return True

    # --- Abre pestaña Datos ---
    def _abrir_datos(page):
        for t in [r"^Datos$", "Datos"]:
            try:
                page.get_by_role("button", name=re.compile(t, re.I)).click(timeout=5000); return True
            except Exception: pass
            try:
                page.get_by_text(re.compile(t, re.I)).first.click(timeout=5000); return True
            except Exception: pass
        try:
            page.locator("[data-fuente='datos'], [aria-controls*=Datos], [data-target*=Datos]").first.click(timeout=5000)
            return True
        except Exception:
            return False

    # --- Devuelve outerHTML de la tabla (si existe) ---
    def _tabla_html(page):
        try:
            page.wait_for_selector("#resultado table tbody tr, #resultado .dataTables_empty, .dataTables_wrapper table", timeout=8000, state="attached")
        except Exception:
            pass
        try:
            t = page.locator("#resultado table").first
            if t.count() == 0:
                t = page.locator(".dataTables_wrapper table").first
            return t.evaluate("el => el.outerHTML")
        except Exception:
            return None

    # --- Busca input global de DataTables ---
    def _buscar_input_datatables(page):
        for sel in [
            "div.dataTables_wrapper div.dataTables_filter input[type='search']",
            "#resultado .dataTables_filter input[type='search']",
            "div.dataTables_filter input",
            "input[aria-label='Search']",
            "xpath=//*[contains(normalize-space(.),'Mostrar número de filas')]/following::input[1]",
        ]:
            try:
                loc = page.locator(sel)
                if loc.count() > 0 and loc.first.is_visible():
                    return loc.first
            except Exception:
                pass
        return None

    def _tabla_contiene_barra(html, barra):
        if not html: return False
        return (barra or "").upper().replace(" ", "") in re.sub(r"\s+", "", html.upper())

    # -------------------- Navegación --------------------
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page(viewport={"width": 1366, "height": 900})

        page.goto(URL_COSTOS_TIEMPO_REAL, wait_until="domcontentloaded", timeout=timeout_ms)
        page.wait_for_load_state("networkidle")
        _screenshot(page, "step1_loaded.png")

        _cerrar_aviso(page)
        _screenshot(page, "step2_modal_closed.png")

        # Fecha → candidatos de hora → intentar hasta encontrar una búsqueda válida
        _set_fecha_hoy_ddmmyyyy(page)
        candidatos = _generar_candidatos_hora(datetime.now(TZ), max_slots=10)
        exito = False
        elegido = None
        for hhmm in candidatos:
            if _select_hora_exacta(page, hhmm) and _buscar_y_ok(page):
                exito, elegido = True, hhmm
                break
        if not exito:
            _screenshot(page, "step3_buscar_fallo.png")
            browser.close()
            raise RuntimeError("No fue posible fijar una hora válida (combo vacío o errores sucesivos).")

        _screenshot(page, "step3_clicked_buscar.png")

        # Datos
        if not _abrir_datos(page):
            _screenshot(page, "step4_no_datos_tab.png")
            browser.close()
            raise RuntimeError("No se pudo abrir la pestaña 'Datos'.")
        page.wait_for_timeout(600)
        _screenshot(page, "step4_tab_datos.png")

        # Buscador / paginación
        buscador, html_tabla = _buscar_input_datatables(page), None
        if buscador is not None:
            try:
                buscador.fill("")
                buscador.type(BARRA_BUSCADA, delay=18)
                page.wait_for_function(
                    """(texto)=>{const u=(texto||'').toUpperCase();
                       const wrap=document.querySelector('#resultado')||document;
                       const t=wrap.querySelector('table'); if(!t) return false;
                       const rows=t.querySelectorAll('tbody tr');
                       return Array.from(rows).some(r=>r.innerText.toUpperCase().includes(u));}""",
                    BARRA_BUSCADA, timeout=9000
                )
                html_tabla = _tabla_html(page)
            except Exception:
                pass

        if not html_tabla:
            html = _tabla_html(page)
            if _tabla_contiene_barra(html, BARRA_BUSCADA):
                html_tabla = html
            else:
                def _sel_siguiente():
                    for s in [
                        ".dataTables_paginate .paginate_button.next:not(.disabled)",
                        "a.paginate_button.next:not(.disabled)",
                        "a:has-text('Siguiente'):not(.disabled)",
                        "a:has-text('›')", "a:has-text('»')",
                    ]:
                        try:
                            if page.locator(s).count() > 0 and page.locator(s).first.is_visible(): return s
                        except Exception: pass
                    return None

                for _ in range(60):
                    nxt = _sel_siguiente()
                    if not nxt: break
                    try:
                        page.locator(nxt).first.click(); page.wait_for_timeout(450)
                        html = _tabla_html(page)
                        if _tabla_contiene_barra(html, BARRA_BUSCADA):
                            html_tabla = html; break
                    except Exception:
                        break

        if not html_tabla:
            _screenshot(page, "step5_no_table.png")
            browser.close()
            raise RuntimeError("No se encontró ninguna tabla en 'Datos'.")

        try:
            with open("datos_tabla.html", "w", encoding="utf-8") as f:
                f.write(html_tabla)
        except Exception:
            pass

        browser.close()

    # ---- Parseo y selección del último registro ----
    tablas = pd.read_html(StringIO(html_tabla))
    if not tablas:
        raise RuntimeError("No se pudo parsear la tabla de 'Datos' a DataFrame.")
    df = tablas[0].copy()

    def fcol(pat): return next((c for c in df.columns if re.search(pat, str(c), re.I)), None)
    col_hora = fcol(r"\bhora\b")
    col_barra = fcol(r"\b(Barra|Nodo|Punto)\b")
    col_cm_en = fcol(r"cm\s*energ")
    col_cm_co = fcol(r"cm\s*conges")
    col_cm_to = fcol(r"(cm\s*total|costo\s*marginal\s*total)")
    if not (col_hora and col_barra and col_cm_to):
        raise RuntimeError("No se encontró una tabla con columnas esperadas (Hora/Barra/CM Total).")

    df = df.rename(columns={
        col_hora: "Hora", col_barra: "Barra", col_cm_to: "CM_Total",
        **({col_cm_en: "CM_Energia"} if col_cm_en else {}),
        **({col_cm_co: "CM_Congestion"} if col_cm_co else {}),
    })
    for c in ["CM_Energia","CM_Congestion","CM_Total"]:
        if c in df.columns:
            df[c] = (df[c].astype(str)
                         .str.replace(",", ".", regex=False)
                         .str.extract(r"([-]?\d+(?:\.\d+)?)")[0]
                         .astype(float))
    df["ts"] = pd.to_datetime(df["Hora"], dayfirst=True, errors="coerce")

    df = filtrar_barra_robusto(df, BARRA_BUSCADA)
    if df.empty:
        raise RuntimeError(f"No se obtuvo ningún registro para '{BARRA_BUSCADA}' tras filtrar la tabla.")
    df = df.sort_values("ts")
    row = df.iloc[-1]
    energia = float(row["CM_Energia"]) if "CM_Energia" in df.columns else None
    congestion = float(row["CM_Congestion"]) if "CM_Congestion" in df.columns else None
    total = float(row["CM_Total"])
    ts = row["ts"]
    if ts.tzinfo is None: ts = ts.tz_localize(TZ)
    return {"barra": row["Barra"], "ts": ts, "energia": energia, "congestion": congestion, "total": total}

# --------------------------------------------------------------------------
# Loop principal
# --------------------------------------------------------------------------

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

