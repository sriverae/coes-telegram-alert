# -*- coding: utf-8 -*-
"""
Alerta COES → Telegram (CHICLAYO 220) con Playwright
v3: Modo ONE-SHOT para GitHub Actions + Persistencia opcional en Gist
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
    "https://www.coes.org.pe/portal/operacion/costosmarginales"
)

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
        
def _close_aviso(page):
    for _ in range(2):
        try:
            page.get_by_role("button", name=re.compile(r"Aceptar", re.I)).click(timeout=1500)
            page.wait_for_timeout(200)
        except Exception:
            pass
        try:
            page.get_by_text(re.compile(r"Aceptar", re.I)).click(timeout=1500)
            page.wait_for_timeout(200)
        except Exception:
            pass

def _select_filter_barras_138(page):
    try:
        # Si es un <select> estándar
        page.select_option("select", label=re.compile(r"mayores a 138", re.I))
    except Exception:
        # Si es un dropdown custom, intenta abrir y elegir
        try:
            page.get_by_role("combobox").click(timeout=1200)
            page.get_by_role("option", name=re.compile(r"mayores a 138", re.I)).click(timeout=1200)
        except Exception:
            pass

def _select_last_hour(page):
    # Si hay campo hora, intenta seleccionar la última opción
    try:
        # Abre el control de hora (input/select)
        page.get_by_role("combobox", name=re.compile(r"Hora", re.I)).click(timeout=1500)
        opciones = page.locator("role=option")
        n = opciones.count()
        if n > 0:
            opciones.nth(n-1).click(timeout=1500)
    except Exception:
        pass

def _wait_for_data(page):
    """Espera a que la tabla de Datos esté cargada antes de exportar."""
    try:
        # texto típico de columna
        page.wait_for_selector("text=CM Total", timeout=8000)
        return
    except Exception:
        pass
    try:
        # cualquier tabla visible
        page.wait_for_selector("table", timeout=8000)
        page.wait_for_timeout(1000)
    except Exception:
        pass

def _select_last_hour(page):
    """Selecciona la última hora disponible en el <select> que contiene opciones tipo HH:MM."""
    selects = page.locator("select")
    try:
        n = selects.count()
    except:
        n = 0
    for i in range(n):
        sel = selects.nth(i)
        try:
            options = sel.locator("option")
            texts = options.all_inner_texts()
            if any(re.fullmatch(r"\d{1,2}:\d{2}", t.strip()) for t in texts):
                # Elegir la última opción con value no vacío
                cnt = options.count()
                last_val = None
                for j in range(cnt - 1, -1, -1):
                    v = options.nth(j).get_attribute("value")
                    if v and v.strip():
                        last_val = v
                        break
                if last_val:
                    sel.select_option(last_val)
                    return True
        except Exception:
            pass
    return False

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
    """Lee tablas desde HTML renderizado y busca columnas equivalentes (Barra/Nodo/Punto y CM*)."""
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

def leer_tabla_html_desde_frames(page) -> pd.DataFrame:
    """
    Busca tablas reales (<table>) en el documento principal y en todos los iframes.
    Guarda artefactos de debug: html_main.html, frame_*.html y frame*_table*.html
    Devuelve el primer DataFrame que tenga columnas de Barra/Nodo y CM.
    """
    import io

    def _df_es_valido(df: pd.DataFrame) -> bool:
        cols = [str(c).strip().lower() for c in df.columns]
        tiene_barra = any(("barra" in c) or ("nodo" in c) for c in cols)
        tiene_cm = any(("cm" in c) or ("costo" in c) for c in cols)
        return tiene_barra and tiene_cm

    # 0) volcado del HTML principal
    try:
        html_main = page.content()
        with open("html_main.html", "w", encoding="utf-8") as f:
            f.write(html_main)
    except Exception:
        pass

    # 1) helper: intenta leer todas las <table> de un frame dado
    def _probar_frame(fr, prefijo: str):
        out_df = None
        try:
            # esperamos a que aparezca al menos alguna tabla (si existe)
            try:
                fr.wait_for_selector("table", timeout=6000)
            except Exception:
                # puede no haber tablas en este frame; seguimos igual
                pass

            tablas = fr.locator("table")
            n = 0
            try:
                n = tablas.count()
            except Exception:
                n = 0

            for i in range(n):
                try:
                    outer = tablas.nth(i).evaluate("el => el.outerHTML")
                    fn = f"{prefijo}_table{i}.html"
                    with open(fn, "w", encoding="utf-8") as f:
                        f.write(outer)

                    # Parsear SOLO esa tabla
                    try:
                        dflist = pd.read_html(io.StringIO(outer))
                    except Exception:
                        dflist = []

                    for dfx in dflist:
                        if _df_es_valido(dfx) and not dfx.empty:
                            return dfx
                except Exception:
                    continue
        except Exception:
            pass
        return out_df

    # 2) primero probar el main frame
    df = _probar_frame(page, "main")
    if df is not None and not df.empty:
        return df

    # 3) recorrer todos los frames
    frames = page.frames
    for idx, fr in enumerate(frames):
        try:
            # dump completo del frame para debug
            try:
                h = fr.content()
                with open(f"frame_{idx}.html", "w", encoding="utf-8") as f:
                    f.write(h)
            except Exception:
                pass

            df = _probar_frame(fr, f"frame{idx}")
            if df is not None and not df.empty:
                return df
        except Exception:
            continue

    raise RuntimeError("No se encontró una tabla de Datos en ninguno de los frames.")



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

def obtener_ultimo_costo_por_export(timeout_ms=60000):
    """
    Flujo robusto:
      - Cierra el aviso.
      - Buscar -> pestaña 'Datos'.
      - Usa el buscador global de DataTables para filtrar 'BARRA_BUSCADA'.
      - Parsear la tabla filtrada (evita paginación).
      - Devuelve último registro (por Hora).
    También genera archivos de depuración: step*.png, html_main.html, datos_tabla.html
    """
    from io import StringIO

    def _screenshot(page, name):
        try:
            page.screenshot(path=name)
        except Exception:
            pass

    def _click_possibles(page, textos, timeout=7000):
        # Prueba varios selectores por texto exacto, aproximado y rol
        for t in textos:
            try:
                page.get_by_role("button", name=re.compile(t, re.I)).click(timeout=timeout)
                return True
            except Exception:
                pass
            try:
                page.get_by_text(t, exact=True).click(timeout=timeout)
                return True
            except Exception:
                pass
            try:
                page.locator(f"button:has-text('{t}')").first.click(timeout=timeout)
                return True
            except Exception:
                pass
            try:
                page.locator(f"a:has-text('{t}')").first.click(timeout=timeout)
                return True
            except Exception:
                pass
        return False

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page(viewport={"width": 1366, "height": 900})
        page.goto(URL_COSTOS_TIEMPO_REAL, wait_until="networkidle", timeout=timeout_ms)

        # Guardar HTML inicial para debug
        try:
            with open("html_main.html", "w", encoding="utf-8") as f:
                f.write(page.content())
        except Exception:
            pass
        _screenshot(page, "step1_loaded.png")

        # 1) Cerrar aviso ("Aceptar" o "X")
        _click_possibles(page, [r"Aceptar", r"^Aceptar$", r"×", r"X"])
        page.wait_for_timeout(600)
        _screenshot(page, "step2_modal_closed.png")

        # 2) Click en Buscar
        _click_possibles(page, [r"^Buscar$","Buscar"])
        page.wait_for_load_state("networkidle")
        page.wait_for_timeout(1200)
        _screenshot(page, "step3_clicked_buscar.png")

        # 3) Ir a pestaña Datos
        if not _click_possibles(page, [r"^Datos$", "Datos"]):
            # fallback: una pestaña con aria-controls de datos
            try:
                page.locator("[aria-controls*='Datos'], [data-target*='Datos']").first.click(timeout=5000)
            except Exception:
                pass
        page.wait_for_timeout(900)
        _screenshot(page, "step4_tab_datos.png")

        # 4) Esperar a que DataTables esté listo (wrapper + input de búsqueda)
        try:
            page.wait_for_selector("div.dataTables_wrapper", timeout=15000)
        except Exception:
            _screenshot(page, "step5_no_wrapper.png")
            browser.close()
            raise RuntimeError("No se encontró el contenedor de DataTables.")

        # Localizador ROBUSTO del buscador global de DataTables
        buscador = None
        # a) patrón clásico
        loc = page.locator("div.dataTables_wrapper div.dataTables_filter input[type='search']")
        if loc.count() > 0:
            buscador = loc.first
        # b) variaciones
        if buscador is None:
            loc = page.locator("div.dataTables_filter input")
            if loc.count() > 0:
                buscador = loc.first
        if buscador is None:
            # Buscar el input dentro de la etiqueta del filtro (label: Buscar:)
            loc = page.locator("div.dataTables_filter label").locator("input")
            if loc.count() > 0:
                buscador = loc.first

        if buscador is None:
            _screenshot(page, "step5_no_search.png")
            browser.close()
            raise RuntimeError("No se encontró la caja de búsqueda de DataTables.")

        # 5) Filtrar por la barra objetivo (atraviesa TODA la paginación)
        texto_buscar = BARRA_BUSCADA
        buscador.fill("")               # limpia
        buscador.type(texto_buscar, delay=25)
        _screenshot(page, "step6_typed_filter.png")

        # Espera a que la tabla muestre al menos una fila que contenga la barra
        try:
            page.wait_for_function(
                """(barra) => {
                    const wrap = document.querySelector('div.dataTables_wrapper');
                    if (!wrap) return false;
                    const rows = wrap.querySelectorAll('table tbody tr');
                    if (!rows.length) return false;
                    const upper = barra.toUpperCase();
                    return Array.from(rows).some(r => r.innerText.toUpperCase().includes(upper));
                }""",
                texto_buscar,
                timeout=10000
            )
        except Exception:
            _screenshot(page, "step6_wait_for_filter_timeout.png")
            # seguimos, igual extraemos HTML para ver qué hay


        # 6) Localizar la tabla (header con Barra/Nodo)
        tabla = None
        try:
            tabla = page.locator("div.dataTables_wrapper table").filter(
                has=page.locator("thead")
            ).first
            # asegurar que hay filas
            page.wait_for_selector("div.dataTables_wrapper table tbody tr", timeout=8000)
        except Exception:
            tabla = None

        if not tabla or tabla.count() == 0:
            _screenshot(page, "step7_no_table.png")
            browser.close()
            raise RuntimeError("No se encontró la tabla de 'Datos' después del filtro.")

        # 7) Extraer HTML de la tabla ya filtrada y guardarlo
        html_tabla = tabla.evaluate("el => el.outerHTML")
        try:
            with open("datos_tabla.html", "w", encoding="utf-8") as f:
                f.write(html_tabla)
        except Exception:
            pass

        browser.close()

    # 8) Parsear DataFrame desde la tabla filtrada
    tablas = pd.read_html(StringIO(html_tabla))
    if not tablas:
        raise RuntimeError("No se pudo parsear la tabla de 'Datos' a DataFrame.")

    df = tablas[0].copy()

    # 9) Detectar columnas de forma flexible
    def fcol(pat):
        return next((c for c in df.columns if re.search(pat, str(c), re.I)), None)

    col_hora = fcol(r"\bhora\b")
    col_barra = fcol(r"\b(Barra|Nodo)\b")
    col_cm_en = fcol(r"cm\s*energ")
    col_cm_co = fcol(r"cm\s*conges")
    col_cm_to = fcol(r"cm\s*total")

    if not (col_hora and col_barra and col_cm_to):
        raise RuntimeError("No se encontró una tabla con columnas esperadas (Hora/Barra/CM Total).")

    df = df.rename(columns={
        col_hora: "Hora",
        col_barra: "Barra",
        col_cm_to: "CM_Total",
        **({col_cm_en: "CM_Energia"} if col_cm_en else {}),
        **({col_cm_co: "CM_Congestion"} if col_cm_co else {}),
    })

    # 10) Números
    for c in ["CM_Energia", "CM_Congestion", "CM_Total"]:
        if c in df.columns:
            df[c] = (
                df[c].astype(str)
                .str.replace(",", ".", regex=False)
                .str.extract(r"([-]?\d+(?:\.\d+)?)")[0]
                .astype(float)
            )

    # 11) Hora
    df["ts"] = pd.to_datetime(df["Hora"], dayfirst=True, errors="coerce")

    # 12) Afinar por barra (por si el buscador trajo variantes)
    df = filtrar_barra_robusto(df, BARRA_BUSCADA)
    if df.empty:
        raise RuntimeError(f"No se obtuvo ningún registro para '{BARRA_BUSCADA}' tras filtrar la tabla.")

    # 13) Último registro
    df = df.sort_values("ts")
    row = df.iloc[-1]
    energia = float(row["CM_Energia"]) if "CM_Energia" in df.columns else None
    congestion = float(row["CM_Congestion"]) if "CM_Congestion" in df.columns else None
    total = float(row["CM_Total"])

    ts = row["ts"]
    if ts.tzinfo is None:
        ts = ts.tz_localize(TZ)

    return {"barra": row["Barra"], "ts": ts, "energia": energia, "congestion": congestion, "total": total}


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
