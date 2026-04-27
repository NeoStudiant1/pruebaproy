# -*- coding: utf-8 -*-

import os
import re
import sys
import time
import logging
import requests
from typing import List, Optional
from base_scraper import BaseScraper, DocumentoResultado, FiltrosBusqueda

logger = logging.getLogger(__name__)

BASE_URL = "https://digitallibrary.un.org"

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
)

COLLECTION = "Documents and Publications"

MAX_REINTENTOS_BUSQUEDA = 3

BACKOFF_REINTENTOS = [3, 8, 15]

MAPA_IDIOMAS = {
    "es": "ES",
    "en": "EN",
    "fr": "FR",
    "ar": "AR",
    "zh": "ZH",
    "ru": "RU",
}

MAPA_TIPOS_DOCUMENTO = {
    "reporte": "report",
    "resolucion": "resolution",
    "acuerdo": "agreement",
    "decision": "decision",
    "carta": "letter",
}

PATRONES_URL_IGNORADAS = [
    "/thumbnail/",    
    "ignoredefault",  
    ".css", ".js", ".png", ".jpg", ".gif", ".svg", ".ico",
    "google.com", "facebook.com", "twitter.com",
    "piwik.php",      
    "stats.tind.io",  
]

HEADERS_DESCARGA = {
    "User-Agent": USER_AGENT,
}


def _descargar_con_progreso(respuesta, ruta_archivo: str,
                             titulo_doc: str) -> bool:
    titulo_corto = (titulo_doc[:45] + "...") if len(titulo_doc) > 48 else titulo_doc

    try:
        total_bytes = int(respuesta.headers.get("Content-Length", 0))
    except (ValueError, TypeError):
        total_bytes = 0

    bytes_descargados = 0
    ultimo_print_bytes = 0
    ultimo_print_tiempo = time.time()

    try:
        with open(ruta_archivo, "wb") as f:
            for bloque in respuesta.iter_content(chunk_size=8192):
                if not bloque:
                    continue
                f.write(bloque)
                bytes_descargados += len(bloque)

                bytes_desde_ultimo = bytes_descargados - ultimo_print_bytes
                tiempo_desde_ultimo = time.time() - ultimo_print_tiempo
                if bytes_desde_ultimo >= 100 * 1024 or tiempo_desde_ultimo >= 0.25:
                    _imprimir_progreso(
                        titulo_corto, bytes_descargados, total_bytes
                    )
                    ultimo_print_bytes = bytes_descargados
                    ultimo_print_tiempo = time.time()

        _imprimir_progreso(titulo_corto, bytes_descargados, total_bytes)
        sys.stdout.write("\r" + " " * 78 + "\r")
        sys.stdout.flush()
        return True

    except Exception as e:
        sys.stdout.write("\r" + " " * 78 + "\r")
        sys.stdout.flush()
        logger.warning(f"Error escribiendo archivo durante descarga: {e}")
        return False


def _imprimir_progreso(titulo_corto: str, descargados: int, total: int):
    mb_desc = descargados / (1024 * 1024)
    if total > 0:
        mb_total = total / (1024 * 1024)
        sufijo = f" ({mb_desc:.1f} MB / {mb_total:.1f} MB)"
    else:
        sufijo = f" ({mb_desc:.1f} MB)"
    prefijo = "    Descargando: "
    espacio_titulo = 78 - len(prefijo) - len(sufijo)
    if espacio_titulo < 10:
        espacio_titulo = 10
    if len(titulo_corto) > espacio_titulo:
        titulo_mostrado = titulo_corto[:espacio_titulo - 3] + "..."
    else:
        titulo_mostrado = titulo_corto
    texto = (prefijo + titulo_mostrado + sufijo).ljust(78)
    sys.stdout.write("\r" + texto)
    sys.stdout.flush()


class UNDigitalLibraryScraper(BaseScraper):

    REGISTROS_POR_PAGINA = 50

    def __init__(self):
        self.ultima_degradacion_filtro: Optional[dict] = None

    def nombre_fuente(self) -> str:
        return "UN Digital Library"

    def search(self, filtros: FiltrosBusqueda) -> List[DocumentoResultado]:
        self.ultima_degradacion_filtro = None

        query = " ".join(filtros.palabras_clave) if filtros.palabras_clave else ""
        if not query:
            logger.error("No se proporcionaron palabras clave para la busqueda.")
            return []

        logger.info(f"Iniciando busqueda en UN Digital Library: '{query}'")
        logger.info(f"Limite configurado: {filtros.limite} documentos")
        if filtros.tipo_documento:
            logger.info(f"Filtro de tipo solicitado: {filtros.tipo_documento}")
        if filtros.anio_desde or filtros.anio_hasta:
            logger.info(f"Rango de fechas: {filtros.anio_desde or '-'} a "
                        f"{filtros.anio_hasta or '-'}")

        try:
            from playwright.sync_api import sync_playwright
        except ImportError:
            logger.error(
                "Playwright no esta instalado. Ejecuta:\n"
                "  pip install playwright\n"
                "  playwright install chromium"
            )
            return []

        resultados = []

        with sync_playwright() as pw:
            navegador = pw.chromium.launch(headless=True)
            contexto = navegador.new_context(
                user_agent=USER_AGENT,
                viewport={"width": 1280, "height": 900},
                locale="en-US",
            )
            pagina = contexto.new_page()
            pagina.set_default_timeout(45000)

            record_ids = self._buscar_record_ids(
                pagina, query, filtros, usar_filtro_tipo=True
            )

            if not record_ids and filtros.tipo_documento:
                mensaje_degradacion = (
                    f"\n  [!] No se encontraron documentos del tipo "
                    f"'{filtros.tipo_documento}' en la UN Digital Library.\n"
                    f"  [!] Mostrando resultados sin filtro de tipo."
                )
                print(mensaje_degradacion)
                logger.warning(
                    f"Filtro de tipo '{filtros.tipo_documento}' degradado: "
                    "0 resultados con filtro, reintentando sin filtro."
                )
                self.ultima_degradacion_filtro = {
                    "campo": "tipo_documento",
                    "valor_original": filtros.tipo_documento,
                    "razon": "cero_resultados",
                    "fuente": self.nombre_fuente(),
                }
                record_ids = self._buscar_record_ids(
                    pagina, query, filtros, usar_filtro_tipo=False
                )

            if not record_ids:
                logger.info("No se encontraron resultados en la busqueda.")
                navegador.close()
                return []

            logger.info(f"Se encontraron {len(record_ids)} record IDs.")

            for i, recid in enumerate(record_ids):
                if len(resultados) >= filtros.limite:
                    break

                logger.debug(f"Extrayendo metadatos del registro {recid} "
                            f"({i+1}/{len(record_ids)})")

                try:
                    doc = self._extraer_metadatos_registro(pagina, recid, filtros)
                    if doc:
                        resultados.append(doc)
                except Exception as e:
                    logger.warning(
                        f"Error al extraer metadatos del registro {recid}: {e}",
                        exc_info=True
                    )

                if i < len(record_ids) - 1:
                    time.sleep(1.0)

            navegador.close()

        logger.info(f"Busqueda completada. Total de documentos: {len(resultados)}")
        return resultados

    def _construir_query(self, query: str, filtros: FiltrosBusqueda,
                         usar_filtro_tipo: bool) -> str:
        if not usar_filtro_tipo or not filtros.tipo_documento:
            return query

        clave_tipo = filtros.tipo_documento.lower()
        tipo_en = MAPA_TIPOS_DOCUMENTO.get(clave_tipo, filtros.tipo_documento)
        return f"{query} title:{tipo_en}"

    def _construir_url_busqueda(self, query_final: str, filtros: FiltrosBusqueda,
                                 pagina_num: int) -> str:
        jrec = ((pagina_num - 1) * self.REGISTROS_POR_PAGINA) + 1
        rg = min(self.REGISTROS_POR_PAGINA, filtros.limite)

        query_url = query_final.replace(" ", "+")
        coleccion_url = COLLECTION.replace(" ", "+")

        url = (
            f"{BASE_URL}/search?ln=en"
            f"&p={query_url}"
            f"&action_search=Search"
            f"&c={coleccion_url}"
            f"&sf=year&so=d"
            f"&rg={rg}&jrec={jrec}"
            f"&of=hb"
        )

        if filtros.anio_desde:
            url += f"&d1y={filtros.anio_desde}&d1m=01&d1d=01"
        if filtros.anio_hasta:
            url += f"&d2y={filtros.anio_hasta}&d2m=12&d2d=31"
        if filtros.anio_desde or filtros.anio_hasta:
            url += "&dt=c"

        return url

    def _navegar_con_reintentos(self, pagina, url: str,
                                 descripcion: str) -> Optional[str]:
        for intento in range(1, MAX_REINTENTOS_BUSQUEDA + 1):
            try:
                logger.debug(f"[{descripcion}] Intento {intento}/{MAX_REINTENTOS_BUSQUEDA}")

                try:
                    pagina.goto(url, wait_until="domcontentloaded", timeout=30000)
                except Exception as e:
                    logger.warning(
                        f"[{descripcion}] DIAGNOSTICO: timeout en goto() "
                        f"intento {intento}: {type(e).__name__}: {e}"
                    )
                    if intento < MAX_REINTENTOS_BUSQUEDA:
                        time.sleep(BACKOFF_REINTENTOS[intento - 1])
                    continue

                try:
                    pagina.wait_for_selector(
                        'a[href*="/record/"], '
                        '.searchresultsbox, '
                        '.portalboxbody, '
                        '#main-content',
                        timeout=15000,
                        state="attached"  
                    )
                except Exception:
                    logger.debug(
                        f"[{descripcion}] DIAGNOSTICO: selector principal no "
                        f"aparecio en intento {intento}, "
                        "revisando contenido igual"
                    )

                time.sleep(1.5)

                html = pagina.content()
                return html

            except Exception as e:
                logger.warning(
                    f"[{descripcion}] DIAGNOSTICO: error inesperado "
                    f"intento {intento}: {type(e).__name__}: {e}"
                )
                if intento < MAX_REINTENTOS_BUSQUEDA:
                    time.sleep(BACKOFF_REINTENTOS[intento - 1])

        logger.error(f"[{descripcion}] Fallaron los {MAX_REINTENTOS_BUSQUEDA} intentos.")
        return None

    def _buscar_record_ids(self, pagina, query: str,
                            filtros: FiltrosBusqueda,
                            usar_filtro_tipo: bool) -> List[str]:
        record_ids = []
        pagina_num = 1
        ids_necesarios = filtros.limite

        query_final = self._construir_query(query, filtros, usar_filtro_tipo)

        while len(record_ids) < ids_necesarios:
            url = self._construir_url_busqueda(query_final, filtros, pagina_num)

            descripcion = f"UN busqueda pagina {pagina_num}"
            logger.info(f"URL de busqueda ({descripcion}): {url}")

            html = self._navegar_con_reintentos(pagina, url, descripcion)

            if html is None:
                logger.error(
                    f"[{descripcion}] DIAGNOSTICO: no se pudo cargar la "
                    "pagina despues de todos los reintentos. Abortando busqueda."
                )
                break

            ids_pagina = re.findall(r'/record/(\d+)', html)

            ids_unicos = []
            ids_vistos = set(record_ids)
            for rid in ids_pagina:
                if rid not in ids_vistos and rid not in ids_unicos:
                    ids_unicos.append(rid)
                    ids_vistos.add(rid)

            logger.info(
                f"[{descripcion}] DIAGNOSTICO: HTML recibido "
                f"({len(html):,} bytes), {len(ids_unicos)} IDs unicos extraidos"
            )

            if not ids_unicos:
                if self._es_pagina_sin_resultados(html):
                    logger.info(
                        f"[{descripcion}] DIAGNOSTICO: cero resultados "
                        "legitimos (la pagina indica 'no records found')"
                    )
                else:
                    logger.warning(
                        f"[{descripcion}] DIAGNOSTICO: cero IDs pero la "
                        "pagina no indica 'no results'. Posible problema de "
                        "renderizado o cambio en el HTML del servidor."
                    )
                break

            record_ids.extend(ids_unicos)
            logger.info(
                f"[{descripcion}] {len(ids_unicos)} IDs nuevos "
                f"(total acumulado: {len(record_ids)}/{ids_necesarios})"
            )

            if len(ids_unicos) < 5:
                break

            pagina_num += 1
            time.sleep(1.5)

        return record_ids[:ids_necesarios]

    def _es_pagina_sin_resultados(self, html: str) -> bool:
        indicadores_vacio = [
            "No records found",
            "no records matching",
            "found 0 records",
            "Search took",  
        ]
        html_lower = html.lower()
        if "search took" in html_lower:
            return True
        return any(ind.lower() in html_lower for ind in indicadores_vacio[:3])

    def _extraer_metadatos_registro(self, pagina, recid: str,
                                     filtros: FiltrosBusqueda) -> Optional[DocumentoResultado]:
        url = f"{BASE_URL}/record/{recid}"

        html = self._navegar_con_reintentos(
            pagina, url, f"UN registro {recid}"
        )
        if html is None:
            logger.warning(f"No se pudo cargar el registro {recid} tras reintentos.")
            return None

        doc = DocumentoResultado()
        doc.recid = recid
        doc.url_fuente = url

        match_titulo = re.search(r'<h1[^>]*>(.*?)</h1>', html, re.DOTALL)
        if match_titulo:
            titulo = re.sub(r'<[^>]+>', '', match_titulo.group(1)).strip()
            doc.titulo = titulo
        else:
            doc.titulo = f"Documento UN {recid}"

        seccion_autores = re.search(
            r'Authors\s*</[^>]+>\s*(.*?)(?:</(?:div|td|tr)|<(?:div|td|tr)\s)',
            html, re.DOTALL | re.IGNORECASE
        )
        if seccion_autores:
            autores = re.findall(r'>([^<]+)</a>', seccion_autores.group(1))
            doc.autor = "; ".join(a.strip() for a in autores if a.strip())

        match_fecha = re.search(
            r'Date\s*</[^>]+>\s*[^<]*?(\d{4})',
            html, re.DOTALL | re.IGNORECASE
        )
        if match_fecha:
            doc.anio = match_fecha.group(1)

        urls_relativas = re.findall(
            rf'/record/{re.escape(recid)}/files/[^\s"\'<>]+\.pdf',
            html
        )
        urls_pdf = list(set(f"{BASE_URL}{u}" for u in urls_relativas))

        urls_pdf = [
            u for u in urls_pdf
            if not any(patron in u.lower() for patron in PATRONES_URL_IGNORADAS)
        ]

        if filtros.idioma:
            sufijos = [MAPA_IDIOMAS[c] for c in filtros.idioma if c in MAPA_IDIOMAS]
            if sufijos:
                urls_filtradas = [
                    u for u in urls_pdf
                    if any(f"-{s}." in u.upper() or f"-{s}" in u.upper()
                           for s in sufijos)
                ]
                if urls_filtradas:
                    urls_pdf = urls_filtradas

        doc.urls_descarga = urls_pdf

        if doc.urls_descarga:
            primer_pdf = doc.urls_descarga[0].upper()
            for codigo, sufijo in MAPA_IDIOMAS.items():
                if f"-{sufijo}." in primer_pdf or f"-{sufijo}" in primer_pdf:
                    doc.idioma = codigo
                    break

        titulo_lower = doc.titulo.lower()
        for palabra, tipo in [
            ("resolution", "Resolution"),
            ("report", "Report"),
            ("decision", "Decision"),
            ("agreement", "Agreement"),
            ("letter", "Letter"),
            ("note", "Note"),
        ]:
            if palabra in titulo_lower:
                doc.tipo_documento = tipo
                break

        return doc

    def download(self, documento: DocumentoResultado, carpeta_destino: str,
                 intentos_max: int = 3) -> Optional[str]:
        if not documento.urls_descarga:
            logger.warning(f"No se encontraron archivos descargables para: {documento.titulo}")
            return None

        nombre_base = self._nombre_archivo_seguro(documento)
        ruta_archivo = os.path.join(carpeta_destino, nombre_base)

        for url in documento.urls_descarga:
            if any(patron in url.lower() for patron in PATRONES_URL_IGNORADAS):
                logger.debug(f"URL en lista negra, saltando: {url}")
                continue

            for intento in range(1, intentos_max + 1):
                try:
                    logger.debug(f"Intento {intento}/{intentos_max} descargando: {url}")
                    respuesta = requests.get(
                        url,
                        timeout=(10, 120),  
                        stream=True,
                        headers=HEADERS_DESCARGA
                    )
                    respuesta.raise_for_status()

                    content_type = respuesta.headers.get("Content-Type", "")
                    if ("pdf" not in content_type.lower()
                            and "octet-stream" not in content_type.lower()):
                        logger.warning(
                            f"No es PDF (Content-Type: {content_type}). "
                            "Probando siguiente URL."
                        )
                        break

                    ok = _descargar_con_progreso(
                        respuesta, ruta_archivo, documento.titulo
                    )
                    if not ok:
                        continue

                    tamano = os.path.getsize(ruta_archivo)
                    if tamano < 100:
                        logger.warning(f"Archivo muy pequeno ({tamano} bytes).")
                        os.remove(ruta_archivo)
                        continue

                    logger.debug(f"Descarga exitosa: {ruta_archivo} ({tamano:,} bytes)")
                    return ruta_archivo

                except requests.RequestException as e:
                    logger.warning(f"Intento {intento}/{intentos_max} fallido: {e}")
                    if intento < intentos_max:
                        time.sleep(2 * intento)

            logger.error(f"Descarga fallida tras {intentos_max} intentos: {url}")

        return None

    def _nombre_archivo_seguro(self, documento: DocumentoResultado) -> str:
        nombre = documento.titulo[:80] if documento.titulo else "sin_titulo"
        nombre = re.sub(r'[<>:"/\\|?*\x00-\x1f]', '_', nombre)
        nombre = re.sub(r'\s+', '_', nombre)
        nombre = nombre.strip('_.')

        if documento.recid:
            nombre = f"UN_{documento.recid}_{nombre}"
        else:
            nombre = f"UN_{nombre}"

        if not nombre.lower().endswith(".pdf"):
            nombre += ".pdf"

        return nombre
