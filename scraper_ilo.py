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


BASE_URL = "https://labordoc.ilo.org"

VID = "41ILO_INST:41ILO_V2"

SEARCH_SCOPE = "ALL_ILO"
TAB = "ALL_ILO"

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
)

MAX_REINTENTOS_BUSQUEDA = 3

BACKOFF_REINTENTOS = [3, 8, 15]

MAPA_IDIOMAS_ILO = {
    "es": "spa",
    "en": "eng",
    "fr": "fre",
    "ar": "ara",
    "zh": "chi",
    "ru": "rus",
}

MAPA_TIPOS_ILO = {
    "reporte": "reports",
    "resolucion": "government_documents",
    "acuerdo": "government_documents",
    "libro": "books",
    "articulo": "articles",
}

PATRONES_URL_IGNORADAS = [
    "/thumbnail/",      
    "ignoredefault",    
    "/intranet/",       
    ".css", ".js", ".png", ".jpg", ".jpeg", ".gif", ".svg", ".ico",
    "google.com", "facebook.com", "twitter.com", "linkedin.com",
    "analytics", "tracking", "googletagmanager",
]

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


class ILOLabordocScraper(BaseScraper):

    RESULTADOS_POR_PAGINA = 10  

    def __init__(self):
        self.ultima_degradacion_filtro: Optional[dict] = None

    def nombre_fuente(self) -> str:
        return "ILO Labordoc"

    def search(self, filtros: FiltrosBusqueda) -> List[DocumentoResultado]:
        self.ultima_degradacion_filtro = None

        query = " ".join(filtros.palabras_clave) if filtros.palabras_clave else ""
        if not query:
            logger.error("No se proporcionaron palabras clave para la busqueda.")
            return []

        logger.info(f"Iniciando busqueda en ILO Labordoc: '{query}'")
        logger.info(f"Limite configurado: {filtros.limite} documentos")
        logger.info(f"Scope de busqueda: {SEARCH_SCOPE}")
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

            resultados = self._ejecutar_busqueda(
                pagina, query, filtros, usar_filtro_tipo=True
            )

            if not resultados and filtros.tipo_documento:
                mensaje_degradacion = (
                    f"\n  [!] No se encontraron documentos del tipo "
                    f"'{filtros.tipo_documento}' en ILO Labordoc.\n"
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
                resultados = self._ejecutar_busqueda(
                    pagina, query, filtros, usar_filtro_tipo=False
                )

            navegador.close()

        logger.info(f"Busqueda completada. Total de documentos encontrados: {len(resultados)}")
        return resultados

    def _ejecutar_busqueda(self, pagina, query: str,
                           filtros: FiltrosBusqueda,
                           usar_filtro_tipo: bool) -> List[DocumentoResultado]:
        resultados: List[DocumentoResultado] = []
        offset = 0

        while len(resultados) < filtros.limite:
            url_busqueda = self._construir_url_busqueda(
                query, filtros, offset, usar_filtro_tipo
            )
            descripcion = f"ILO busqueda offset={offset}"
            logger.info(f"URL de busqueda ({descripcion}): {url_busqueda}")

            html = self._navegar_busqueda_con_reintentos(
                pagina, url_busqueda, descripcion
            )

            if html is None:
                logger.error(
                    f"[{descripcion}] DIAGNOSTICO: no se pudo cargar la "
                    "pagina tras reintentos. Abortando esta busqueda."
                )
                break

            documentos_pagina = self._extraer_resultados(pagina)

            logger.info(
                f"[{descripcion}] DIAGNOSTICO: HTML recibido "
                f"({len(html):,} bytes), "
                f"{len(documentos_pagina)} enlaces a registros extraidos"
            )

            if not documentos_pagina:
                if self._es_pagina_sin_resultados(pagina, html):
                    logger.info(
                        f"[{descripcion}] DIAGNOSTICO: cero resultados "
                        "legitimos (Primo indica 'No records')"
                    )
                else:
                    logger.warning(
                        f"[{descripcion}] DIAGNOSTICO: cero resultados pero "
                        "Primo no muestra 'No records'. Posible problema de "
                        "renderizado, selector roto o facet mal formado."
                    )
                break

            for doc in documentos_pagina:
                if len(resultados) >= filtros.limite:
                    break
                if doc.url_fuente:
                    urls_pdf = self._obtener_url_pdf(pagina, doc.url_fuente)
                    doc.urls_descarga = urls_pdf
                resultados.append(doc)

            logger.info(
                f"[{descripcion}] {len(documentos_pagina)} resultados de esta "
                f"pagina (total acumulado: {len(resultados)}/{filtros.limite})"
            )

            if len(documentos_pagina) < self.RESULTADOS_POR_PAGINA:
                break

            offset += self.RESULTADOS_POR_PAGINA
            time.sleep(2)

        return resultados[:filtros.limite]

    def _construir_url_busqueda(self, query: str, filtros: FiltrosBusqueda,
                                 offset: int, usar_filtro_tipo: bool) -> str:
        query_codificado = query.replace(" ", "%20")
        query_primo = f"any,contains,{query_codificado}"

        url = (
            f"{BASE_URL}/discovery/search?"
            f"query={query_primo}"
            f"&tab={TAB}"
            f"&search_scope={SEARCH_SCOPE}"
            f"&vid={VID}"
            f"&offset={offset}"
            f"&lang=en"
        )

        posicion_facet = 1

        if filtros.idioma:
            for codigo_idioma in filtros.idioma:
                if codigo_idioma in MAPA_IDIOMAS_ILO:
                    codigo = MAPA_IDIOMAS_ILO[codigo_idioma]
                    url += f"&mfacet=lang,include,{codigo},{posicion_facet}"
                    posicion_facet += 1

        if usar_filtro_tipo and filtros.tipo_documento:
            clave_tipo = filtros.tipo_documento.lower()
            tipo_primo = MAPA_TIPOS_ILO.get(clave_tipo, "")
            if tipo_primo:
                url += f"&mfacet=rtype,include,{tipo_primo},{posicion_facet}"
                posicion_facet += 1

        if filtros.anio_desde or filtros.anio_hasta:
            desde = filtros.anio_desde or 1900
            hasta = filtros.anio_hasta or 2030
            url += (f"&mfacet=searchcreationdate,include,"
                    f"{desde}%7C,%7C{hasta},{posicion_facet}")
            posicion_facet += 1

        return url

    def _navegar_busqueda_con_reintentos(self, pagina, url: str,
                                          descripcion: str) -> Optional[str]:
        for intento in range(1, MAX_REINTENTOS_BUSQUEDA + 1):
            try:
                logger.debug(
                    f"[{descripcion}] Intento {intento}/{MAX_REINTENTOS_BUSQUEDA}"
                )

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
                        'prm-brief-result-container, '
                        '.item-title, '
                        'prm-search-result-list-line-content, '
                        'prm-no-search-result, '
                        '.no-results, '
                        '.empty-results',
                        timeout=20000,
                        state="attached"
                    )
                except Exception:
                    logger.warning(
                        f"[{descripcion}] DIAGNOSTICO: selector de resultados "
                        f"NO aparecio en 20s (intento {intento}). "
                        "Puede ser: render lento, servidor saturado, "
                        "o cambio en el HTML de Primo VE."
                    )
                time.sleep(1)

                html = pagina.content()
                return html

            except Exception as e:
                logger.warning(
                    f"[{descripcion}] DIAGNOSTICO: error inesperado "
                    f"intento {intento}: {type(e).__name__}: {e}"
                )
                if intento < MAX_REINTENTOS_BUSQUEDA:
                    time.sleep(BACKOFF_REINTENTOS[intento - 1])

        logger.error(
            f"[{descripcion}] Fallaron los {MAX_REINTENTOS_BUSQUEDA} intentos."
        )
        return None

    def _es_pagina_sin_resultados(self, pagina, html: str) -> bool:
        try:
            sin_resultados = pagina.query_selector(
                "prm-no-search-result, .no-results, .zero-results"
            )
            if sin_resultados:
                return True
        except Exception:
            pass

        indicadores = [
            "no results found",
            "no records matching",
            "prm-no-search-result",
            "0 results",
        ]
        html_lower = html.lower()
        return any(ind in html_lower for ind in indicadores)

    def _extraer_resultados(self, pagina) -> List[DocumentoResultado]:
        documentos = []
        import html as html_module

        titulos_por_docid: dict = {}

        try:
            contenedores = pagina.query_selector_all(
                "prm-brief-result-container, "
                "prm-search-result-list-line, "
                "prm-search-result-list-line-content, "
                "[class*='list-item-primary-content']"
            )

            for cont in contenedores:
                try:
                    enlaces = cont.query_selector_all("a[href]")
                    for enlace in enlaces:
                        try:
                            href = enlace.get_attribute("href") or ""
                            if "fulldisplay" not in href:
                                continue
                            match_docid = re.search(r'docid=([^&]+)', href)
                            if not match_docid:
                                continue
                            docid = match_docid.group(1)

                            texto = (enlace.inner_text() or "").strip()
                            if texto and len(texto) > 3:
                                if docid not in titulos_por_docid:
                                    titulos_por_docid[docid] = texto
                                break  
                        except Exception:
                            continue
                except Exception:
                    continue
        except Exception as e:
            logger.debug(f"Error extrayendo titulos con Playwright: {e}")

        html = pagina.content()
        html_decoded = html_module.unescape(html)

        enlaces_fulldisplay = re.findall(
            r'(/discovery/fulldisplay\?[^"\'>\s]+)',
            html_decoded
        )

        docids_vistos = set()
        enlaces_unicos = []
        for href in enlaces_fulldisplay:
            match_docid = re.search(r'docid=([^&\s]+)', href)
            if match_docid:
                docid = match_docid.group(1)
                if docid not in docids_vistos:
                    docids_vistos.add(docid)
                    enlaces_unicos.append((href, docid))

        logger.debug(
            f"Enlaces a registros encontrados: {len(enlaces_unicos)} "
            f"(titulos extraidos con Playwright: {len(titulos_por_docid)})"
        )

        for href, docid in enlaces_unicos:
            try:
                doc = DocumentoResultado()
                doc.recid = docid

                if docid in titulos_por_docid:
                    doc.titulo = titulos_por_docid[docid]
                else:
                    doc.titulo = f"Documento ILO {docid}"

                href_limpio = href.replace("&amp;", "&")
                if href_limpio.startswith("/"):
                    doc.url_fuente = f"{BASE_URL}{href_limpio}"
                else:
                    doc.url_fuente = href_limpio

                documentos.append(doc)

            except Exception as e:
                logger.warning(f"Error al extraer resultado: {e}", exc_info=True)

        return documentos

    def _obtener_url_pdf(self, pagina, url_registro: str) -> List[str]:
        urls_pdf = []

        try:
            import html as html_module
            url_limpia = html_module.unescape(url_registro)

            pagina.goto(url_limpia, wait_until="domcontentloaded", timeout=30000)

            selector_pdf_aparecio = False
            inicio_espera = time.time()
            try:
                pagina.wait_for_selector(
                    'a[href*="/view/delivery/"], '
                    'a[href*="/media/"], '
                    'a[href$=".pdf"], '
                    'prm-full-view-service-container, '
                    'prm-gallery-item, '
                    'prm-service-container, '
                    '.full-view-inner-container',
                    timeout=5000,
                    state="attached"
                )
                selector_pdf_aparecio = True
                time.sleep(0.5)
            except Exception:
                tiempo_esperado = time.time() - inicio_espera
                logger.warning(
                    f"DIAGNOSTICO: selector de PDF no aparecio en "
                    f"{tiempo_esperado:.1f}s para {url_limpia}, "
                    "usando fallback best-effort (sleep 3s)"
                )
                time.sleep(3)

            html_contenido = pagina.content()
            html_decodificado = html_module.unescape(html_contenido)

            urls_ilo_media = re.findall(
                r'https?://[^"\'<>\s]*ilo\.org/media/\d+/download',
                html_decodificado
            )
            urls_pdf.extend(urls_ilo_media)

            urls_directas_pdf = re.findall(
                r'https?://[^"\'<>\s]+\.pdf(?:\?[^"\'<>\s]*)?',
                html_decodificado
            )
            urls_pdf.extend(urls_directas_pdf)

            urls_delivery = re.findall(
                r'https?://[^"\'<>\s]*labordoc[^"\'<>\s]*/delivery/[^"\'<>\s]+',
                html_decodificado
            )
            urls_pdf.extend(urls_delivery)

            if not urls_pdf:
                try:
                    enlaces = pagina.query_selector_all("a[href]")
                    for enlace in enlaces:
                        try:
                            href = enlace.get_attribute("href") or ""
                            texto = (enlace.inner_text() or "").strip().lower()

                            es_descarga = (
                                "/download" in href.lower() or
                                href.lower().endswith(".pdf") or
                                "/delivery/" in href.lower() or
                                "pdf" in texto or
                                "full text" in texto or
                                "view online" in texto or
                                "online access" in texto or
                                "texto completo" in texto
                            )

                            if es_descarga and href.startswith("http"):
                                if "javascript:" not in href:
                                    urls_pdf.append(href)
                        except Exception:
                            continue
                except Exception:
                    pass

            urls_unicas = list(dict.fromkeys(urls_pdf))

            urls_filtradas = [
                u for u in urls_unicas
                if not any(patron in u.lower() for patron in PATRONES_URL_IGNORADAS)
            ]

            urls_prioritarias = [
                u for u in urls_filtradas
                if '/view/delivery/' in u or '/media/' in u
            ]
            urls_resto = [u for u in urls_filtradas if u not in urls_prioritarias]
            urls_pdf = urls_prioritarias + urls_resto

        except Exception as e:
            logger.warning(f"Error al obtener URL de PDF desde {url_registro}: {e}")

        logger.debug(f"URLs de PDF encontradas para {url_registro}: {len(urls_pdf)}")
        return urls_pdf[:5]

    def download(self, documento: DocumentoResultado, carpeta_destino: str,
                 intentos_max: int = 3) -> Optional[str]:
        if not documento.urls_descarga:
            logger.warning(
                f"No se encontraron archivos descargables para: {documento.titulo}"
            )
            return None

        nombre_base = self._nombre_archivo_seguro(documento)

        for url in documento.urls_descarga:
            if any(patron in url.lower() for patron in PATRONES_URL_IGNORADAS):
                logger.debug(f"URL en lista negra, saltando: {url}")
                continue

            ruta_archivo = os.path.join(carpeta_destino, nombre_base)

            for intento in range(1, intentos_max + 1):
                try:
                    logger.debug(
                        f"Intento {intento}/{intentos_max} descargando: {url}"
                    )
                    respuesta = requests.get(
                        url,
                        timeout=(10, 120),  
                        stream=True,
                        allow_redirects=True,
                        headers={"User-Agent": USER_AGENT}
                    )
                    respuesta.raise_for_status()

                    content_type = respuesta.headers.get("Content-Type", "")
                    if ("pdf" not in content_type.lower()
                            and "octet-stream" not in content_type.lower()):
                        logger.debug(
                            f"Content-Type no es PDF: {content_type}. "
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
                        logger.warning(f"Archivo demasiado pequeno ({tamano} bytes).")
                        os.remove(ruta_archivo)
                        continue

                    logger.debug(f"Descarga exitosa: {ruta_archivo} ({tamano:,} bytes)")
                    return ruta_archivo

                except requests.RequestException as e:
                    logger.warning(
                        f"Intento {intento}/{intentos_max} fallido para {url}: {e}"
                    )
                    if intento < intentos_max:
                        time.sleep(2 * intento)

            logger.error(f"Descarga fallida despues de {intentos_max} intentos: {url}")

        return None

    def _nombre_archivo_seguro(self, documento: DocumentoResultado) -> str:
        nombre = documento.titulo[:80] if documento.titulo else "sin_titulo"
        nombre = re.sub(r'[<>:"/\\|?*\x00-\x1f]', '_', nombre)
        nombre = re.sub(r'\s+', '_', nombre)
        nombre = nombre.strip('_.')

        if documento.recid:
            recid_corto = documento.recid[:30]
            nombre = f"ILO_{recid_corto}_{nombre}"
        else:
            nombre = f"ILO_{nombre}"

        if not nombre.lower().endswith(".pdf"):
            nombre += ".pdf"

        return nombre
