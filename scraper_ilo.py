"""
scraper_ilo.py — Scraper para Labordoc ILO (labordoc.ilo.org).

Estrategia:
  - Labordoc usa Ex Libris Primo VE, que carga con Angular (JavaScript)
  - Se usa Playwright (navegador headless) para renderizar la página
  - Se parsean los resultados con BeautifulSoup
  - Los PDFs se descargan directamente con requests
  
  Alternativa sin JS (si Playwright no está disponible):
  - Se intenta la API interna de Primo VE directamente via requests
"""

import re
import time
import json
from pathlib import Path

import requests
from colorama import Fore, Style
from tqdm import tqdm

from utils import sanitizar_nombre, obtener_dir_temporal

BASE_URL   = "https://labordoc.ilo.org"
SEARCH_URL = f"{BASE_URL}/discovery/search"
VID        = "41ILO_INST:41ILO_V2"
TAB        = "41ILO_V2"
SCOPE      = "41ILO_INST"
TIMEOUT    = 45
HEADERS    = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept"         : "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9,es;q=0.8",
    "Referer"        : BASE_URL,
}

# API interna de Primo VE
PRIMO_API_URL = f"{BASE_URL}/primaws/rest/pub/pnxs"


class ScraperILO:
    """
    Scraper para Labordoc ILO.
    Intenta primero la API interna de Primo VE (rápida).
    Si falla, usa Playwright (navegador headless).
    """

    def __init__(self, palabras_clave: list[str], cantidad_max: int):
        self.palabras_clave = palabras_clave
        self.cantidad_max   = cantidad_max
        self.session        = requests.Session()
        self.session.headers.update(HEADERS)
        self._playwright_disponible = self._verificar_playwright()

    # Verificación de dependencias
    @staticmethod
    def _verificar_playwright() -> bool:
        try:
            from playwright.sync_api import sync_playwright
            return True
        except ImportError:
            return False

    # Punto de entrada
    def ejecutar(self) -> list[dict]:
        """Busca y descarga documentos para todas las palabras clave."""
        resultados_totales = []

        for keyword in self.palabras_clave:
            print(f"\n  {Fore.GREEN}🔍 Labordoc ILO — buscando: {Fore.YELLOW}{keyword}{Style.RESET_ALL}")

            # Intentar primero API interna
            registros = self._buscar_via_api(keyword)

            if not registros and self._playwright_disponible:
                print(f"  {Fore.YELLOW}  → API interna sin resultados. Intentando con navegador...")
                registros = self._buscar_via_playwright(keyword)

            if not registros:
                print(f"  {Fore.RED}  ✘ Sin resultados para '{keyword}' en Labordoc ILO.")
                if not self._playwright_disponible:
                    print(f"  {Fore.YELLOW}  ℹ Para mejores resultados instala Playwright:")
                    print(f"      pip install playwright && playwright install chromium")
                continue

            print(f"  {Fore.CYAN}  → {len(registros)} documento(s) encontrados. Descargando...")

            for registro in tqdm(registros, desc=f"    Descargando", unit="doc",
                                  bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt}"):
                resultado = self._descargar_documento(registro, keyword)
                if resultado:
                    resultados_totales.append(resultado)
                time.sleep(1.0)

        return resultados_totales

    # Método 1: API interna de Primo VE
    def _buscar_via_api(self, keyword: str) -> list[dict]:
        """
        Usa el endpoint interno /primaws/rest/pub/pnxs de Primo VE.
        Este endpoint es usado por el frontend Angular y no requiere API key.
        Soporta paginación con offset + limit.
        """
        registros  = []
        offset     = 0
        limit      = min(25, self.cantidad_max)
        total_api  = None

        while len(registros) < self.cantidad_max:
            limite_actual = min(limit, self.cantidad_max - len(registros))

            params = {
                "blendFacetsSeparately" : False,
                "disableCache"          : False,
                "getMore"               : 0,
                "inst"                  : "41ILO_INST",
                "lang"                  : "en",
                "limit"                 : limite_actual,
                "newspapersActive"      : False,
                "newspapersSearch"      : False,
                "offset"                : offset,
                "otbRanking"            : False,
                "pcAvailability"        : False,
                "q"                     : f"any,contains,{keyword}",
                "qExclude"              : "",
                "qInclude"              : "",
                "rapido"                : False,
                "refEntryActive"        : False,
                "rtaLinks"              : True,
                "scope"                 : SCOPE,
                "skipDelivery"          : "Y",
                "sort"                  : "rank",
                "tab"                   : TAB,
                "vid"                   : VID,
            }

            try:
                resp = self.session.get(
                    PRIMO_API_URL,
                    params=params,
                    timeout=TIMEOUT,
                )
                resp.raise_for_status()
                data = resp.json()
            except (requests.RequestException, json.JSONDecodeError) as e:
                # Silenciar errores de API interna; se usará Playwright como fallback
                return []

            docs = data.get("docs", [])
            if not docs:
                break

            if total_api is None:
                total_api = data.get("info", {}).get("total", 0)

            for doc in docs:
                parsed = self._parsear_doc_primo(doc)
                if parsed:
                    registros.append(parsed)

            offset += len(docs)
            if total_api and offset >= total_api:
                break

            time.sleep(0.8)

        return registros[:self.cantidad_max]

    def _parsear_doc_primo(self, doc: dict) -> dict | None:
        """Extrae metadatos de un documento JSON de Primo VE."""
        pnx = doc.get("pnx", {})

        display  = pnx.get("display", {})
        control  = pnx.get("control", {})
        links    = pnx.get("links", {})
        delivery = doc.get("delivery", {})

        # Título
        titulo_raw = display.get("title", [""])[0] if display.get("title") else ""
        titulo = re.sub(r"<[^>]+>", "", titulo_raw).strip()

        # Autor
        creadores = display.get("creator", []) or display.get("contributor", [])
        autor = creadores[0] if creadores else ""

        # Año
        anio = ""
        fecha_raw = display.get("creationdate", [""])[0] if display.get("creationdate") else ""
        if fecha_raw:
            m = re.search(r"\b(19|20)\d{2}\b", fecha_raw)
            anio = m.group() if m else fecha_raw[:4]

        # Idioma
        idioma_raw = display.get("language", [""])[0] if display.get("language") else ""
        idioma = idioma_raw.upper()[:3]

        # Tipo
        tipo = display.get("type", [""])[0] if display.get("type") else ""

        # ID del registro
        record_id = control.get("recordid", [""])[0] if control.get("recordid") else ""

        # URL de la página del documento
        url_pagina = f"{BASE_URL}/discovery/fulldisplay?vid={VID}&docid={record_id}" if record_id else ""

        # Buscar URL de PDF en los links del documento
        url_pdf = self._extraer_url_pdf(links, delivery, doc)

        if not titulo and not record_id:
            return None

        return {
            "record_id" : record_id,
            "titulo"    : titulo,
            "autor"     : autor,
            "anio"      : anio,
            "idioma"    : idioma,
            "tipo"      : tipo,
            "url_pdf"   : url_pdf,
            "url_pagina": url_pagina,
        }

    def _extraer_url_pdf(self, links: dict, delivery: dict, doc: dict) -> str:
        """Busca URLs de PDF en la estructura del documento Primo VE."""
        # 1. Links directos
        for key in ("linktopdf", "openurlfulltext", "linktorsrc"):
            vals = links.get(key, [])
            for val in vals:
                if isinstance(val, str) and ".pdf" in val.lower():
                    return val

        # 2. Links de entrega
        best_urls = delivery.get("bestlocation", {})
        if isinstance(best_urls, dict):
            url = best_urls.get("urls", {})
            if isinstance(url, dict):
                for u in url.values():
                    if u and ".pdf" in str(u).lower():
                        return str(u)

        # 3. Todos los links disponibles (buscar PDF)
        for key, vals in links.items():
            if isinstance(vals, list):
                for val in vals:
                    if isinstance(val, str) and ("pdf" in val.lower() or "download" in val.lower()):
                        return val

        # 4. Buscar en openURL
        openurl_vals = links.get("openurl", [])
        return openurl_vals[0] if openurl_vals else ""

    # Método 2: Playwright
    def _buscar_via_playwright(self, keyword: str) -> list[dict]:
        """
        Usa un navegador headless (Chromium) para renderizar la página
        y extraer los resultados cargados por JavaScript.
        """
        from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout
        from bs4 import BeautifulSoup

        registros = []

        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            context = browser.new_context(
                user_agent=HEADERS["User-Agent"],
                viewport={"width": 1280, "height": 900},
            )
            page = context.new_page()

            url_busqueda = (
                f"{SEARCH_URL}"
                f"?vid={VID}"
                f"&tab={TAB}"
                f"&scope={SCOPE}"
                f"&query=any,contains,{requests.utils.quote(keyword)}"
                f"&limit={min(25, self.cantidad_max)}"
            )

            try:
                page.goto(url_busqueda, wait_until="networkidle", timeout=60000)
                # Esperar que carguen los resultados de Angular
                page.wait_for_selector("prm-search-result-list", timeout=15000)
                time.sleep(2)
            except PlaywrightTimeout:
                browser.close()
                return []

            html = page.content()
            browser.close()

        soup = BeautifulSoup(html, "lxml")
        items = soup.select("prm-brief-result-container")

        for item in items[:self.cantidad_max]:
            registro = self._parsear_resultado_html(item)
            if registro:
                registros.append(registro)

        return registros

    def _parsear_resultado_html(self, item) -> dict | None:
        """Parsea un ítem HTML de resultado de Primo VE."""
        titulo_el = item.select_one("h3.item-title a, .item-title")
        titulo    = titulo_el.get_text(strip=True) if titulo_el else ""

        autor_el = item.select_one(".item-detail-element:first-child")
        autor    = autor_el.get_text(strip=True) if autor_el else ""

        year_el = item.select_one("[title*='Publication Year']")
        anio    = year_el.get_text(strip=True) if year_el else ""

        link_el = item.select_one("a[href*='fulldisplay']")
        url_pagina = BASE_URL + link_el["href"] if link_el and link_el.get("href") else ""

        pdf_el  = item.select_one("a[href$='.pdf'], a[href*='/media/']")
        url_pdf = pdf_el["href"] if pdf_el and pdf_el.get("href") else ""

        if not titulo:
            return None

        return {
            "record_id" : "",
            "titulo"    : titulo,
            "autor"     : autor,
            "anio"      : anio,
            "idioma"    : "",
            "tipo"      : "",
            "url_pdf"   : url_pdf,
            "url_pagina": url_pagina,
        }

    # Descarga de PDF
    def _descargar_documento(self, registro: dict, keyword: str) -> dict | None:
        """Descarga el PDF y lo guarda localmente."""
        url_pdf = registro.get("url_pdf")

        # Si no hay URL directa, intentar obtenerla desde la página
        if not url_pdf and registro.get("url_pagina"):
            url_pdf = self._extraer_pdf_de_pagina(registro["url_pagina"])

        if not url_pdf:
            return None

        titulo_corto = sanitizar_nombre(registro.get("titulo", "sin_titulo"), max_len=50)
        anio         = registro.get("anio", "")
        record_id    = registro.get("record_id", "unk")
        nombre_pdf   = f"ILO_{record_id}_{titulo_corto}_{anio}.pdf".replace(" ", "_")

        dir_temp  = obtener_dir_temporal()
        dir_tema  = dir_temp / sanitizar_nombre(keyword)
        dir_tema.mkdir(parents=True, exist_ok=True)
        ruta_dest = dir_tema / nombre_pdf

        if not ruta_dest.exists():
            try:
                resp = self.session.get(url_pdf, timeout=TIMEOUT, stream=True)
                resp.raise_for_status()

                content_type = resp.headers.get("Content-Type", "")
                if "pdf" not in content_type.lower() and "octet-stream" not in content_type.lower():
                    if not url_pdf.lower().endswith(".pdf"):
                        return None

                with open(ruta_dest, "wb") as f:
                    for chunk in resp.iter_content(chunk_size=8192):
                        f.write(chunk)

                if ruta_dest.stat().st_size < 1024:
                    ruta_dest.unlink()
                    return None

            except requests.RequestException:
                return None

        return {
            "fuente"        : "Labordoc_ILO",
            "tema"          : keyword,
            "record_id"     : registro.get("record_id", ""),
            "titulo"        : registro.get("titulo", ""),
            "autor"         : registro.get("autor", ""),
            "anio"          : registro.get("anio", ""),
            "idioma"        : registro.get("idioma", ""),
            "url_pdf"       : url_pdf,
            "url_pagina"    : registro.get("url_pagina", ""),
            "archivo_local" : str(ruta_dest),
        }

    def _extraer_pdf_de_pagina(self, url_pagina: str) -> str | None:
        """Navega la página del documento y busca el link de PDF."""
        if not url_pagina:
            return None
        try:
            from bs4 import BeautifulSoup
            resp = self.session.get(url_pagina, timeout=TIMEOUT)
            soup = BeautifulSoup(resp.text, "lxml")

            # Buscar links directos a PDF
            for a in soup.find_all("a", href=True):
                href = a["href"]
                if ".pdf" in href.lower():
                    return href if href.startswith("http") else BASE_URL + href

            # Buscar en los atributos de datos o scripts
            for script in soup.find_all("script"):
                if script.string and ".pdf" in script.string:
                    match = re.search(r'https?://[^\s"\']+\.pdf', script.string)
                    if match:
                        return match.group()

        except Exception:
            pass
        return None
