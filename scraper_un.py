"""
scraper_un.py — Scraper para la UN Digital Library (digitallibrary.un.org).

Estrategia:
  - Usa la API pública de Invenio con output format MARCXML (of=xm)
  - Parsea los registros MARC para extraer metadatos y links a PDFs (campo 856)
  - Descarga los PDFs directamente con requests
  - Paginación automática mediante jrec + rg
"""

import re
import time
from pathlib import Path
from xml.etree import ElementTree as ET

import requests
from tqdm import tqdm
from colorama import Fore, Style

from utils import sanitizar_nombre, obtener_dir_temporal

# Constantes
BASE_URL     = "https://digitallibrary.un.org"
SEARCH_URL   = f"{BASE_URL}/search"
REGISTROS_POR_PAGINA = 25      # máximo seguro para Invenio
TIMEOUT      = 30              # segundos por request
HEADERS      = {
    "User-Agent": (
        "Mozilla/5.0 (compatible; DocumentScraper/1.0; "
        "+https://github.com/usuario/scraper)"
    )
}


class ScraperUN:
    """
    Scraper para UN Digital Library usando la API Invenio (MARCXML).
    """

    def __init__(self, palabras_clave: list[str], cantidad_max: int):
        self.palabras_clave = palabras_clave
        self.cantidad_max   = cantidad_max
        self.session        = requests.Session()
        self.session.headers.update(HEADERS)

    # Punto de entrada
    def ejecutar(self) -> list[dict]:
        """Busca y descarga documentos para todas las palabras clave."""
        resultados_totales = []

        for keyword in self.palabras_clave:
            print(f"\n  {Fore.BLUE}🔍 UN Library — buscando: {Fore.YELLOW}{keyword}{Style.RESET_ALL}")
            registros = self._buscar(keyword)

            if not registros:
                print(f"  {Fore.RED}  ✘ Sin resultados para '{keyword}' en UN Library.")
                continue

            print(f"  {Fore.CYAN}  → {len(registros)} documento(s) encontrados. Descargando...")

            for registro in tqdm(registros, desc=f"    Descargando", unit="doc",
                                  bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt}"):
                resultado = self._descargar_documento(registro, keyword)
                if resultado:
                    resultados_totales.append(resultado)
                time.sleep(0.8)  # cortesía al servidor

        return resultados_totales

    # Búsqueda con paginación
    def _buscar(self, keyword: str) -> list[dict]:
        """
        Llama a la API MARCXML de Invenio y pagina hasta obtener
        cantidad_max registros (o los que haya disponibles).
        """
        registros      = []
        jrec           = 1
        obtenidos      = 0
        total_servidor = None

        while obtenidos < self.cantidad_max:
            por_pagina = min(REGISTROS_POR_PAGINA, self.cantidad_max - obtenidos)

            params = {
                "p"    : keyword,
                "of"   : "xm",          # MARCXML
                "rg"   : por_pagina,
                "jrec" : jrec,
                "c"    : "Documents and Publications",
                "ln"   : "en",
            }

            try:
                resp = self.session.get(SEARCH_URL, params=params, timeout=TIMEOUT)
                resp.raise_for_status()
            except requests.RequestException as e:
                print(f"\n  {Fore.RED}  ✘ Error al consultar UN Library: {e}{Style.RESET_ALL}")
                break

            # Leer total de resultados desde comentario HTML
            if total_servidor is None:
                match = re.search(
                    r"Search-Engine-Total-Number-Of-Results:\s*(\d+)",
                    resp.text
                )
                total_servidor = int(match.group(1)) if match else 0
                if total_servidor == 0:
                    break

            lote = self._parsear_marcxml(resp.text)
            if not lote:
                break

            registros.extend(lote)
            obtenidos += len(lote)
            jrec      += len(lote)

            if jrec > total_servidor:
                break

            time.sleep(1)

        return registros[:self.cantidad_max]

    # Parser MARCXML
    def _parsear_marcxml(self, xml_text: str) -> list[dict]:
        """
        Extrae de cada <record> MARC los campos relevantes:
          - 001       → recid
          - 041 $a    → idioma
          - 100/700 $a→ autor
          - 245 $a    → título
          - 260/269 $c→ año
          - 520 $a    → resumen
          - 856 $u    → URL del PDF
        """
        # El MARCXML puede venir con comentario HTML antes; limpiar
        inicio = xml_text.find("<collection>")
        if inicio == -1:
            return []
        xml_limpio = xml_text[inicio:]

        try:
            root = ET.fromstring(xml_limpio)
        except ET.ParseError:
            return []

        ns = ""  # Invenio no usa namespace
        registros = []

        for record in root.findall("record"):
            datos = {
                "recid"    : "",
                "titulo"   : "",
                "autor"    : "",
                "anio"     : "",
                "idioma"   : "",
                "resumen"  : "",
                "url_pdf"  : "",
                "url_pagina": "",
            }

            # Campo de control 001 → recid
            ctrl = record.find("controlfield[@tag='001']")
            if ctrl is not None:
                datos["recid"] = ctrl.text or ""
                datos["url_pagina"] = f"{BASE_URL}/record/{datos['recid']}"

            for df in record.findall("datafield"):
                tag = df.get("tag", "")

                # Idioma
                if tag == "041":
                    sf = df.find("subfield[@code='a']")
                    if sf is not None:
                        datos["idioma"] = (sf.text or "").upper()

                # Autor principal
                elif tag == "100":
                    sf = df.find("subfield[@code='a']")
                    if sf is not None and not datos["autor"]:
                        datos["autor"] = sf.text or ""

                # Autores secundarios
                elif tag == "700":
                    sf = df.find("subfield[@code='a']")
                    if sf is not None and not datos["autor"]:
                        datos["autor"] = sf.text or ""

                # Título
                elif tag == "245":
                    sf = df.find("subfield[@code='a']")
                    if sf is not None:
                        datos["titulo"] = (sf.text or "").rstrip(" /")

                # Año de publicación
                elif tag in ("260", "269", "264"):
                    sf = df.find("subfield[@code='c']")
                    if sf is not None and not datos["anio"]:
                        anio_match = re.search(r"\b(19|20)\d{2}\b", sf.text or "")
                        if anio_match:
                            datos["anio"] = anio_match.group()

                # Resumen
                elif tag == "520":
                    sf = df.find("subfield[@code='a']")
                    if sf is not None and not datos["resumen"]:
                        datos["resumen"] = (sf.text or "")[:300]

                # URL del archivo (priorizar PDF)
                elif tag == "856":
                    sf_url  = df.find("subfield[@code='u']")
                    sf_tipo = df.find("subfield[@code='q']")
                    if sf_url is not None:
                        url = sf_url.text or ""
                        tipo = (sf_tipo.text or "").lower() if sf_tipo is not None else ""
                        # Priorizar PDF
                        if "pdf" in url.lower() or "pdf" in tipo:
                            datos["url_pdf"] = url
                        elif not datos["url_pdf"] and url:
                            datos["url_pdf"] = url  # guardar lo que haya

            if datos["recid"]:
                registros.append(datos)

        return registros

    # Descarga de PDF
    def _descargar_documento(self, registro: dict, keyword: str) -> dict | None:
        """Descarga el PDF de un registro y lo guarda en el directorio temporal."""
        url_pdf = registro.get("url_pdf")
        if not url_pdf:
            return None  # No hay PDF disponible

        # Construir nombre de archivo
        titulo_corto = sanitizar_nombre(registro.get("titulo", "sin_titulo"), max_len=50)
        anio         = registro.get("anio", "")
        nombre_pdf   = f"UN_{registro['recid']}_{titulo_corto}_{anio}.pdf".replace(" ", "_")

        # Carpeta destino: _temp_documentos/{keyword}/{fuente}/
        dir_temp  = obtener_dir_temporal()
        dir_tema  = dir_temp / sanitizar_nombre(keyword)
        dir_tema.mkdir(parents=True, exist_ok=True)
        ruta_dest = dir_tema / nombre_pdf

        # Descargar si no existe ya
        if not ruta_dest.exists():
            try:
                resp = self.session.get(url_pdf, timeout=TIMEOUT, stream=True)
                resp.raise_for_status()

                # Verificar que sea PDF
                content_type = resp.headers.get("Content-Type", "")
                if "pdf" not in content_type.lower() and not url_pdf.lower().endswith(".pdf"):
                    # Intentar recuperar desde página del record
                    url_pdf_alt = self._buscar_pdf_en_pagina(registro.get("url_pagina", ""))
                    if url_pdf_alt:
                        resp = self.session.get(url_pdf_alt, timeout=TIMEOUT, stream=True)
                        resp.raise_for_status()
                    else:
                        return None

                with open(ruta_dest, "wb") as f:
                    for chunk in resp.iter_content(chunk_size=8192):
                        f.write(chunk)

                # Verificar que el archivo no esté vacío
                if ruta_dest.stat().st_size < 1024:
                    ruta_dest.unlink()
                    return None

            except requests.RequestException:
                return None

        return {
            "fuente"        : "UN_Digital_Library",
            "tema"          : keyword,
            "recid"         : registro["recid"],
            "titulo"        : registro.get("titulo", ""),
            "autor"         : registro.get("autor", ""),
            "anio"          : registro.get("anio", ""),
            "idioma"        : registro.get("idioma", ""),
            "url_pdf"       : url_pdf,
            "url_pagina"    : registro.get("url_pagina", ""),
            "archivo_local" : str(ruta_dest),
        }

    def _buscar_pdf_en_pagina(self, url_pagina: str) -> str | None:
        """
        Fallback: navega la página HTML del registro y busca links a PDF.
        """
        if not url_pagina:
            return None
        try:
            from bs4 import BeautifulSoup
            resp = self.session.get(url_pagina, timeout=TIMEOUT)
            soup = BeautifulSoup(resp.text, "lxml")
            for a in soup.find_all("a", href=True):
                href = a["href"]
                if ".pdf" in href.lower():
                    return href if href.startswith("http") else BASE_URL + href
        except Exception:
            pass
        return None
