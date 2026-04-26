# -*- coding: utf-8 -*-

import os
import re
import time
import logging
import requests
from typing import List, Optional
from base_scraper import BaseScraper, DocumentoResultado, FiltrosBusqueda

logger = logging.getLogger(__name__)

VID = "41ILO_INST:41ILO_V2"
TAB = "ILO_DigiColl"
SEARCH_SCOPE = "ILO_DigiColl"
BASE_URL = "https://labordoc.ilo.org"

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


class ILOLabordocScraper(BaseScraper):

    RESULTADOS_POR_PAGINA = 10  

    def nombre_fuente(self) -> str:
        return "ILO Labordoc"

    def search(self, filtros: FiltrosBusqueda) -> List[DocumentoResultado]:
        resultados = []

        query = " ".join(filtros.palabras_clave) if filtros.palabras_clave else ""
        if not query:
            logger.error("No se proporcionaron palabras clave para la busqueda.")
            return []

        logger.info(f"Iniciando busqueda en ILO Labordoc: '{query}'")
        logger.info(f"Limite configurado: {filtros.limite} documentos")

        try:
            from playwright.sync_api import sync_playwright
        except ImportError:
            logger.error(
                "Playwright no esta instalado. Ejecuta:\n"
                "  pip install playwright\n"
                "  playwright install chromium"
            )
            return []

        with sync_playwright() as pw:
            navegador = pw.chromium.launch(headless=True)
            contexto = navegador.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                           "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                viewport={"width": 1280, "height": 800}
            )
            pagina = contexto.new_page()

            pagina.set_default_timeout(45000)

            offset = 0
            documentos_obtenidos = 0

            while documentos_obtenidos < filtros.limite:
                url_busqueda = self._construir_url_busqueda(query, filtros, offset)
                logger.debug(f"Navegando a: {url_busqueda}")

                try:
                    pagina.goto(url_busqueda, wait_until="networkidle", timeout=60000)
                    time.sleep(5)

                    try:
                        pagina.wait_for_selector(
                            ".item-title, .result-item-text, "
                            "prm-brief-result-container .item-title, "
                            "h3.item-title, [class*='result'] a",
                            timeout=10000,
                            state="visible"
                        )
                    except Exception:
                        logger.debug("No se detectaron titulos de resultado visibles.")

                    sin_resultados = pagina.query_selector(".no-results, .zero-results")
                    if sin_resultados:
                        logger.info("No se encontraron mas resultados.")
                        break

                    documentos_pagina = self._extraer_resultados(pagina)

                    if not documentos_pagina:
                        logger.info(f"No se pudieron extraer resultados en offset={offset}.")
                        break

                    for doc in documentos_pagina:
                        if documentos_obtenidos >= filtros.limite:
                            break

                        if doc.url_fuente:
                            urls_pdf = self._obtener_url_pdf(pagina, doc.url_fuente)
                            doc.urls_descarga = urls_pdf

                        resultados.append(doc)
                        documentos_obtenidos += 1

                    logger.info(f"Pagina offset={offset}: {len(documentos_pagina)} resultados "
                                f"(total: {documentos_obtenidos}/{filtros.limite})")

                    if len(documentos_pagina) < self.RESULTADOS_POR_PAGINA:
                        break

                    offset += self.RESULTADOS_POR_PAGINA
                    time.sleep(2)  

                except Exception as e:
                    logger.error(f"Error al procesar pagina offset={offset}: {e}", exc_info=True)
                    break

            navegador.close()

        resultados = resultados[:filtros.limite]
        logger.info(f"Busqueda completada. Total de documentos encontrados: {len(resultados)}")
        return resultados

    def _construir_url_busqueda(self, query: str, filtros: FiltrosBusqueda, offset: int) -> str:
        query_primo = f"any,contains,{query}"

        url = (
            f"{BASE_URL}/discovery/search?"
            f"query={query_primo}"
            f"&tab={TAB}"
            f"&search_scope={SEARCH_SCOPE}"
            f"&vid={VID}"
            f"&offset={offset}"
            f"&lang=en"
        )

        if filtros.idioma:
            for codigo_idioma in filtros.idioma:
                if codigo_idioma in MAPA_IDIOMAS_ILO:
                    codigo = MAPA_IDIOMAS_ILO[codigo_idioma]
                    url += f"&mfacet=lang,include,{codigo},1"

        if filtros.tipo_documento:
            clave_tipo = filtros.tipo_documento.lower()
            tipo_primo = MAPA_TIPOS_ILO.get(clave_tipo, "")
            if tipo_primo:
                url += f"&mfacet=rtype,include,{tipo_primo},1"

        if filtros.anio_desde or filtros.anio_hasta:
            desde = filtros.anio_desde or 1900
            hasta = filtros.anio_hasta or 2030
            url += f"&facet=searchcreationdate,include,[{desde}+TO+{hasta}]"

        return url

    def _extraer_resultados(self, pagina) -> List[DocumentoResultado]:
        documentos = []
        html = pagina.content()

        import html as html_module
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

        logger.debug(f"Enlaces a registros encontrados: {len(enlaces_unicos)}")

        titulos_playwright = []
        try:
            elementos_titulo = pagina.query_selector_all(
                ".item-title a, "
                "h3.item-title a, "
                "prm-brief-result-container .item-title a, "
                "[class*='result'] .item-title a"
            )
            for elem in elementos_titulo:
                try:
                    texto = elem.inner_text().strip()
                    if texto and len(texto) > 3:
                        titulos_playwright.append(texto)
                except Exception:
                    continue
        except Exception:
            pass

        for i, (href, docid) in enumerate(enlaces_unicos):
            try:
                doc = DocumentoResultado()
                doc.recid = docid

                if i < len(titulos_playwright):
                    doc.titulo = titulos_playwright[i]
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

            pagina.goto(url_limpia, wait_until="networkidle", timeout=45000)
            time.sleep(6)

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
                if not any(excl in u.lower() for excl in [
                    '/thumbnail/',     
                    'ignoredefault',     
                    '.css', '.js', '.png', '.jpg', '.gif', '.svg',
                    'google.com', 'facebook.com', 'twitter.com',
                    'analytics', 'tracking',
                ])
            ]

            urls_prioritarias = [u for u in urls_filtradas
                                 if '/view/delivery/' in u or '/media/' in u]
            urls_resto = [u for u in urls_filtradas if u not in urls_prioritarias]
            urls_pdf = urls_prioritarias + urls_resto

        except Exception as e:
            logger.warning(f"Error al obtener URL de PDF desde {url_registro}: {e}")

        logger.debug(f"URLs de PDF encontradas para {url_registro}: {len(urls_pdf)}")
        return urls_pdf[:5]

    def download(self, documento: DocumentoResultado, carpeta_destino: str,
                 intentos_max: int = 3) -> Optional[str]:
        if not documento.urls_descarga:
            logger.warning(f"No se encontraron archivos descargables para: {documento.titulo}")
            return None

        nombre_base = self._nombre_archivo_seguro(documento)

        for url in documento.urls_descarga:
            ruta_archivo = os.path.join(carpeta_destino, nombre_base)

            for intento in range(1, intentos_max + 1):
                try:
                    logger.debug(f"Intento {intento}/{intentos_max} descargando: {url}")
                    respuesta = requests.get(
                        url,
                        timeout=120,
                        stream=True,
                        allow_redirects=True,
                        headers={
                            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                                          "AppleWebKit/537.36 (KHTML, like Gecko) "
                                          "Chrome/120.0.0.0 Safari/537.36"
                        }
                    )
                    respuesta.raise_for_status()

                    content_type = respuesta.headers.get("Content-Type", "")
                    if "pdf" not in content_type.lower() and "octet-stream" not in content_type.lower():
                        logger.debug(f"Content-Type no es PDF: {content_type}. Probando siguiente URL.")
                        break

                    with open(ruta_archivo, "wb") as f:
                        for bloque in respuesta.iter_content(chunk_size=8192):
                            f.write(bloque)

                    tamano = os.path.getsize(ruta_archivo)
                    if tamano < 100:
                        logger.warning(f"Archivo demasiado pequeno ({tamano} bytes).")
                        os.remove(ruta_archivo)
                        continue

                    logger.debug(f"Descarga exitosa: {ruta_archivo} ({tamano:,} bytes)")
                    return ruta_archivo

                except requests.RequestException as e:
                    logger.warning(f"Intento {intento}/{intentos_max} fallido para {url}: {e}")
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
