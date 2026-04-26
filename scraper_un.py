# -*- coding: utf-8 -*-

import os
import re
import time
import logging
import requests
from typing import List, Optional
from base_scraper import BaseScraper, DocumentoResultado, FiltrosBusqueda

logger = logging.getLogger(__name__)

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

BASE_URL = "https://digitallibrary.un.org"

HEADERS_DESCARGA = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
}


class UNDigitalLibraryScraper(BaseScraper):

    REGISTROS_POR_PAGINA = 50

    def nombre_fuente(self) -> str:
        return "UN Digital Library"

    def search(self, filtros: FiltrosBusqueda) -> List[DocumentoResultado]:
        query = " ".join(filtros.palabras_clave) if filtros.palabras_clave else ""
        if not query:
            logger.error("No se proporcionaron palabras clave para la busqueda.")
            return []

        logger.info(f"Iniciando busqueda en UN Digital Library: '{query}'")
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

        resultados = []

        with sync_playwright() as pw:
            navegador = pw.chromium.launch(headless=True)
            contexto = navegador.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                           "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                viewport={"width": 1280, "height": 900}
            )
            pagina = contexto.new_page()
            pagina.set_default_timeout(45000)

            record_ids = self._buscar_record_ids(pagina, query, filtros)

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
                    logger.warning(f"Error al extraer metadatos del registro {recid}: {e}",
                                  exc_info=True)

                if i < len(record_ids) - 1:
                    time.sleep(1.0)

            navegador.close()

        logger.info(f"Busqueda completada. Total de documentos: {len(resultados)}")
        return resultados

    def _buscar_record_ids(self, pagina, query: str,
                            filtros: FiltrosBusqueda) -> List[str]:
        record_ids = []
        pagina_num = 1
        ids_necesarios = filtros.limite

        query_final = query
        if filtros.tipo_documento:
            clave_tipo = filtros.tipo_documento.lower()
            tipo_en = MAPA_TIPOS_DOCUMENTO.get(clave_tipo, filtros.tipo_documento)
            query_final = f"{query} {tipo_en}"

        while len(record_ids) < ids_necesarios:
            jrec = ((pagina_num - 1) * self.REGISTROS_POR_PAGINA) + 1
            rg = min(self.REGISTROS_POR_PAGINA, ids_necesarios - len(record_ids))

            url = (
                f"{BASE_URL}/search?ln=en"
                f"&p={query_final}"
                f"&action_search=Search"
                f"&c=Documents+and+Publications"
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

            try:
                logger.debug(f"Navegando a busqueda pagina {pagina_num}: {url}")
                pagina.goto(url, wait_until="domcontentloaded")

                time.sleep(3)

                contenido = pagina.content()
                ids_pagina = re.findall(r'/record/(\d+)', contenido)

                ids_unicos = []
                ids_vistos = set(record_ids)
                for rid in ids_pagina:
                    if rid not in ids_vistos and rid not in ids_unicos:
                        ids_unicos.append(rid)
                        ids_vistos.add(rid)

                if not ids_unicos:
                    logger.debug(f"No se encontraron nuevos IDs en pagina {pagina_num}.")
                    break

                record_ids.extend(ids_unicos)
                logger.debug(f"Pagina {pagina_num}: {len(ids_unicos)} IDs "
                            f"(total: {len(record_ids)})")

                if len(ids_unicos) < 5:
                    break

                pagina_num += 1
                time.sleep(1.5)

            except Exception as e:
                logger.error(f"Error en busqueda pagina {pagina_num}: {e}", exc_info=True)
                break

        return record_ids[:ids_necesarios]

    def _extraer_metadatos_registro(self, pagina, recid: str,
                                     filtros: FiltrosBusqueda) -> Optional[DocumentoResultado]:
        url = f"{BASE_URL}/record/{recid}"

        try:
            pagina.goto(url, wait_until="domcontentloaded")
            time.sleep(2)
        except Exception as e:
            logger.warning(f"Error al navegar al registro {recid}: {e}")
            return None

        html = pagina.content()
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

        if filtros.idioma and filtros.idioma in MAPA_IDIOMAS:
            sufijo = MAPA_IDIOMAS[filtros.idioma]
            urls_filtradas = [u for u in urls_pdf
                            if f"-{sufijo}." in u.upper() or f"-{sufijo}" in u.upper()]
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
            for intento in range(1, intentos_max + 1):
                try:
                    logger.debug(f"Intento {intento}/{intentos_max} descargando: {url}")
                    respuesta = requests.get(
                        url,
                        timeout=120,
                        stream=True,
                        headers=HEADERS_DESCARGA
                    )
                    respuesta.raise_for_status()

                    content_type = respuesta.headers.get("Content-Type", "")
                    if ("pdf" not in content_type.lower()
                            and "octet-stream" not in content_type.lower()):
                        logger.warning(f"No es PDF (Content-Type: {content_type}). "
                                      "Probando siguiente URL.")
                        break

                    with open(ruta_archivo, "wb") as f:
                        for bloque in respuesta.iter_content(chunk_size=8192):
                            f.write(bloque)

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
