# -*- coding: utf-8 -*-
"""
Scraper para la Biblioteca Digital de las Naciones Unidas
(digitallibrary.un.org).

La obtencion de la lista de resultados y de los metadatos individuales
se hace con Playwright (navegador headless), porque las paginas estan
detras de medidas anti-bot que rechazan clientes HTTP planos. La
descarga final del PDF si se hace con requests, ya que los archivos
binarios se sirven directamente.

Las URLs de PDF siguen el patron:
    https://digitallibrary.un.org/record/XXXXX/files/NOMBRE.pdf
"""

import os
import re
import sys
import time
import logging
import requests
from typing import List, Optional, Set
from base_scraper import BaseScraper, DocumentoResultado, FiltrosBusqueda

logger = logging.getLogger(__name__)

# ============================================================================
# CONSTANTES CONFIGURABLES
# ============================================================================
# Se mantienen al tope del modulo para facilitar ajustes puntuales sin
# tener que recorrer el cuerpo de las funciones. Si en el futuro se
# vuelven realmente parametros del usuario, podrian migrar a
# configuracion.json sin tocar el resto del codigo.

BASE_URL = "https://digitallibrary.un.org"

# Algunos CDNs sirven contenido distinto al User-Agent por defecto de
# Playwright; declarar uno de Chrome reciente evita ese sesgo.
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
)

COLLECTION = "Documents and Publications"

MAX_REINTENTOS_BUSQUEDA = 3

# Backoff creciente para no machacar al servidor si esta caido o lento
BACKOFF_REINTENTOS = [3, 8, 15]

# Mapeo de codigos ISO a los sufijos que la ONU usa en los nombres de
# archivo PDF (ej: A_HRC_34_32-ES.pdf para la version en espanol).
MAPA_IDIOMAS = {
    "es": "ES",
    "en": "EN",
    "fr": "FR",
    "ar": "AR",
    "zh": "ZH",
    "ru": "RU",
}

# Mapeo de tipos de documento en espanol a terminos de busqueda en ingles.
# NOTA: en Invenio el filtro por tipo se aplica via el campo de busqueda
# de Invenio, no concatenando la palabra a la query libre. Ver _construir_query.
MAPA_TIPOS_DOCUMENTO = {
    "reporte": "report",
    "resolucion": "resolution",
    "acuerdo": "agreement",
    "decision": "decision",
    "carta": "letter",
}

# ============================================================================
# LISTA NEGRA DE PATRONES DE URL
# ============================================================================
# Estos patrones aparecen en el HTML de los registros pero no son PDFs
# descargables: son thumbnails, recursos estaticos o endpoints protegidos.
# Agregar nuevos patrones aqui es trivial sin modificar la logica de extraccion.
PATRONES_URL_IGNORADAS = [
    "/thumbnail/",    # Imagenes de portada (devuelven text/html)
    "ignoredefault",  # Parametro comun en URLs de thumbnails
    ".css", ".js", ".png", ".jpg", ".gif", ".svg", ".ico",
    "google.com", "facebook.com", "twitter.com",
    "piwik.php",      # Pixel de tracking
    "stats.tind.io",  # Analytics de Invenio
]

# Headers para la descarga de PDFs (fase 3, con requests)
HEADERS_DESCARGA = {
    "User-Agent": USER_AGENT,
}


# ============================================================================
# FUNCION AUXILIAR: descarga con progreso visible en consola
# ============================================================================
# Esta funcion esta duplicada en scraper_ilo.py intencionalmente: cada scraper
# es autonomo y no depende de los otros. Si en el futuro aparecen 3+ scrapers,
# moverla a un modulo utils.py comun seria el refactor obvio.
def _descargar_con_progreso(respuesta, ruta_archivo: str,
                             titulo_doc: str) -> bool:
    """Escribe la respuesta HTTP en disco mostrando una barra de
    progreso de una sola linea, sobreescrita con \\r. Devuelve True si
    la escritura termina sin excepciones."""
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
    """Imprime la linea de progreso en la misma posicion de la consola."""
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
    """
    Scraper para la Biblioteca Digital de Naciones Unidas.

    Usa Playwright para navegar las paginas de busqueda y registros,
    evitando los bloqueos anti-bot del servidor.
    Usa requests solo para la descarga final de PDFs.

    Atributos post-busqueda:
        ultima_degradacion_filtro: None si la busqueda uso los filtros exactos
            del usuario, o una descripcion de la degradacion si el filtro
            de tipo se desactivo por no arrojar resultados. Disponible despues
            de llamar a search() para que main.py pueda leerlo.
    """

    REGISTROS_POR_PAGINA = 50

    def __init__(self):
        # Cuando el filtro de tipo no produce resultados se relaja y se
        # vuelve a buscar; si eso ocurre, este atributo queda con un dict
        # describiendo el cambio para que main.py pueda informarlo al
        # usuario sin modificar la interfaz publica de BaseScraper.
        self.ultima_degradacion_filtro: Optional[dict] = None

    def nombre_fuente(self) -> str:
        return "UN Digital Library"

    def search(self, filtros: FiltrosBusqueda,
               ids_excluir: Optional[Set[str]] = None) -> List[DocumentoResultado]:
        """Busca documentos en la Biblioteca Digital de la ONU.

        Si el filtro de tipo de documento no devuelve resultados, se
        relaja automaticamente y se vuelve a buscar; el cambio queda
        registrado en self.ultima_degradacion_filtro para que main.py
        pueda informarlo al usuario."""
        self.ultima_degradacion_filtro = None

        # Normalizar el set de exclusion permite tratar None y conjunto
        # vacio del mismo modo en el resto del cuerpo
        ids_excluir = ids_excluir or set()

        query = " ".join(filtros.palabras_clave) if filtros.palabras_clave else ""
        if not query:
            logger.error("No se proporcionaron palabras clave para la busqueda.")
            return []

        logger.info(f"Iniciando busqueda en UN Digital Library: '{query}'")
        logger.info(f"Limite configurado: {filtros.limite} documentos")
        if filtros.tipo_documento:
            logger.info(f"Filtro de tipo solicitado: {filtros.tipo_documento}")
        if filtros.fecha_desde or filtros.fecha_hasta:
            logger.info(f"Rango de fechas: {filtros.fecha_desde or '-'} a "
                        f"{filtros.fecha_hasta or '-'}")

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

            # --- FASE 1: Busqueda con filtro de tipo si se solicito ---
            record_ids = self._buscar_record_ids(
                pagina, query, filtros, usar_filtro_tipo=True,
                ids_excluir=ids_excluir,
            )

            # --- DEGRADACION: si el filtro de tipo dio 0 resultados, reintentar sin el ---
            if not record_ids and filtros.tipo_documento:
                mensaje_degradacion = (
                    f"\n  [!] No se encontraron documentos del tipo "
                    f"'{filtros.tipo_documento}' en la UN Digital Library.\n"
                    f"  [!] Mostrando resultados sin filtro de tipo."
                )
                # Aviso al usuario en consola (no va al log, va a stdout directo)
                print(mensaje_degradacion)
                logger.warning(
                    f"Filtro de tipo '{filtros.tipo_documento}' degradado: "
                    "0 resultados con filtro, reintentando sin filtro."
                )
                # Registrar la degradacion para que main.py pueda leerla
                self.ultima_degradacion_filtro = {
                    "campo": "tipo_documento",
                    "valor_original": filtros.tipo_documento,
                    "razon": "cero_resultados",
                    "fuente": self.nombre_fuente(),
                }
                # Reintentar sin el filtro de tipo
                record_ids = self._buscar_record_ids(
                    pagina, query, filtros, usar_filtro_tipo=False,
                    ids_excluir=ids_excluir,
                )

            if not record_ids:
                logger.info("No se encontraron resultados en la busqueda.")
                navegador.close()
                return []

            logger.info(f"Se encontraron {len(record_ids)} record IDs.")

            # --- FASE 2: Extraer metadatos de cada registro ---
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
        """Construye el parametro 'p' de la URL de busqueda de Invenio.

        El tipo de documento se incorpora como filtro de campo
        ('title:reporte') en lugar de concatenarlo como texto libre. La
        diferencia es importante: el texto libre obligaria a Invenio a
        encontrar la palabra exacta entre las claves de busqueda,
        mientras que el filtro de campo restringe la busqueda al campo
        elegido (titulo) sin obligar al match literal en la query."""
        if not usar_filtro_tipo or not filtros.tipo_documento:
            return query

        clave_tipo = filtros.tipo_documento.lower()
        tipo_en = MAPA_TIPOS_DOCUMENTO.get(clave_tipo, filtros.tipo_documento)
        return f"{query} title:{tipo_en}"

    def _construir_url_busqueda(self, query_final: str, filtros: FiltrosBusqueda,
                                 pagina_num: int) -> str:
        """Arma la URL completa con paginacion, filtros temporales y
        coleccion. El espacio dentro de query se sustituye por '+' por
        convencion de Invenio."""
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

        if filtros.fecha_desde:
            url += f"&d1y={filtros.fecha_desde}&d1m=01&d1d=01"
        if filtros.fecha_hasta:
            url += f"&d2y={filtros.fecha_hasta}&d2m=12&d2d=31"
        if filtros.fecha_desde or filtros.fecha_hasta:
            url += "&dt=c"

        return url

    def _navegar_con_reintentos(self, pagina, url: str,
                                 descripcion: str) -> Optional[str]:
        """Navega a una URL con reintentos espaciados por backoff.

        wait_until='domcontentloaded' es deliberado: las paginas de la
        ONU tienen un pixel de tracking (piwik.php) que mantiene la red
        ocupada indefinidamente, por lo que 'networkidle' nunca se
        cumpliria. El docto se considera listo cuando aparece alguno de
        los selectores tipicos de la pagina de resultados de Invenio.
        Devuelve el HTML, o None si los reintentos se agotaron."""
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

                # state='attached' basta con que el selector exista en
                # el DOM, sin esperar a que sea visible: mas rapido y
                # tolerante con paginas con resultados parciales.
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
                    # Que el selector no aparezca puede significar tanto
                    # 'cero resultados legitimos' como 'pagina rota'; el
                    # caller distingue ambos casos analizando el HTML.
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
                            usar_filtro_tipo: bool,
                            ids_excluir: Optional[Set[str]] = None) -> List[str]:
        """Recorre las paginas de resultados y devuelve la lista de
        record IDs hasta cubrir filtros.limite o agotar la fuente. Si
        ids_excluir se proporciona, los IDs ya conocidos se omiten
        antes de contar contra el limite, de modo que la paginacion
        continua hasta juntar la cantidad solicitada de IDs nuevos."""
        ids_excluir = ids_excluir or set()
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
                # Fallaron todos los reintentos
                logger.error(
                    f"[{descripcion}] DIAGNOSTICO: no se pudo cargar la "
                    "pagina despues de todos los reintentos. Abortando busqueda."
                )
                break

            # Extraer record IDs del contenido de la pagina
            ids_pagina = re.findall(r'/record/(\d+)', html)

            # total_en_pagina cuenta los IDs unicos que el servidor
            # entrego antes de filtrar por exclusion; se usa mas abajo
            # para decidir si hay mas paginas. Mantenerlo separado de
            # ids_unicos evita cortar el bucle prematuramente cuando la
            # pagina viene completa pero casi todo es historico.
            total_en_pagina = 0
            ids_unicos = []
            ids_vistos = set(record_ids)
            excluidos_esta_pagina = 0
            for rid in ids_pagina:
                if rid in ids_vistos:
                    continue
                ids_vistos.add(rid)
                total_en_pagina += 1
                if f"UN:{rid}" in ids_excluir:
                    excluidos_esta_pagina += 1
                    continue
                ids_unicos.append(rid)

            logger.info(
                f"[{descripcion}] DIAGNOSTICO: HTML recibido "
                f"({len(html):,} bytes), {total_en_pagina} IDs unicos en la "
                f"pagina, {excluidos_esta_pagina} excluidos por historial, "
                f"{len(ids_unicos)} a procesar"
            )

            if total_en_pagina == 0:
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

            # Si el servidor devolvio menos IDs de los esperados, casi
            # con certeza ya no hay mas paginas. La condicion se evalua
            # sobre total_en_pagina (antes de excluir) para no
            # interpretar como 'fin' una pagina llena de duplicados.
            if total_en_pagina < 5:
                break

            pagina_num += 1
            time.sleep(1.5)

        return record_ids[:ids_necesarios]

    def _es_pagina_sin_resultados(self, html: str) -> bool:
        """Detecta si una pagina HTML de Invenio corresponde a 'cero
        resultados legitimos' (la query simplemente no encontro nada),
        para distinguirlo de un fallo de carga silencioso."""
        indicadores_vacio = [
            "No records found",
            "no records matching",
            "found 0 records",
            "Search took",
        ]
        html_lower = html.lower()
        # 'search took' aparece siempre en paginas de resultados, asi
        # que su presencia con 0 /record/ implica 'cero resultados'
        if "search took" in html_lower:
            return True
        return any(ind.lower() in html_lower for ind in indicadores_vacio[:3])

    def _extraer_metadatos_registro(self, pagina, recid: str,
                                     filtros: FiltrosBusqueda) -> Optional[DocumentoResultado]:
        """Carga la ficha de un registro individual y extrae titulo,
        autores, fecha, idioma, tipo y URLs de los PDFs adjuntos."""
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

        # --- Titulo (etiqueta <h1>) ---
        match_titulo = re.search(r'<h1[^>]*>(.*?)</h1>', html, re.DOTALL)
        if match_titulo:
            titulo = re.sub(r'<[^>]+>', '', match_titulo.group(1)).strip()
            doc.titulo = titulo
        else:
            doc.titulo = f"Documento UN {recid}"

        # --- Autores ---
        seccion_autores = re.search(
            r'Authors\s*</[^>]+>\s*(.*?)(?:</(?:div|td|tr)|<(?:div|td|tr)\s)',
            html, re.DOTALL | re.IGNORECASE
        )
        if seccion_autores:
            autores = re.findall(r'>([^<]+)</a>', seccion_autores.group(1))
            doc.autor = "; ".join(a.strip() for a in autores if a.strip())

        # --- Fecha / Ano ---
        match_fecha = re.search(
            r'Date\s*</[^>]+>\s*[^<]*?(\d{4})',
            html, re.DOTALL | re.IGNORECASE
        )
        if match_fecha:
            doc.fecha = match_fecha.group(1)

        # --- URLs de PDFs ---
        urls_relativas = re.findall(
            rf'/record/{re.escape(recid)}/files/[^\s"\'<>]+\.pdf',
            html
        )
        urls_pdf = list(set(f"{BASE_URL}{u}" for u in urls_relativas))

        # Aplicar lista negra de patrones
        urls_pdf = [
            u for u in urls_pdf
            if not any(patron in u.lower() for patron in PATRONES_URL_IGNORADAS)
        ]

        # Filtrar por idioma si se especifico
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

        # --- Idioma (inferir del primer PDF) ---
        if doc.urls_descarga:
            primer_pdf = doc.urls_descarga[0].upper()
            for codigo, sufijo in MAPA_IDIOMAS.items():
                if f"-{sufijo}." in primer_pdf or f"-{sufijo}" in primer_pdf:
                    doc.idioma = codigo
                    break

        # --- Tipo de documento (inferir del titulo) ---
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
        """Descarga el PDF al disco con reintentos.

        El timeout es (10, 120): 10 segundos para establecer conexion y
        120 segundos entre bytes recibidos, lo que corta intentos
        bloqueados sin renunciar a archivos grandes que tardan en
        terminar de bajar."""
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
                        timeout=(10, 120),  # (connect, read)
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

                    # Streaming con progreso visible en consola
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
        """Genera un nombre de archivo seguro a partir del titulo y recid."""
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
