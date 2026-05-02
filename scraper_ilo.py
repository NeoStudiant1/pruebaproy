# -*- coding: utf-8 -*-
"""
Scraper para Labordoc de la OIT (labordoc.ilo.org).

Labordoc esta construido sobre Ex Libris Primo VE, una aplicacion
Angular SPA: los resultados se cargan dinamicamente, por lo que las
fases de busqueda y extraccion de metadatos requieren un navegador
real (Playwright). Para resolver el enlace del PDF de cada documento
se prefiere la API REST publica de Primo VE: dos llamadas HTTP cortas
permiten obtener la URL S3 firmada del archivo, evitando navegar a la
pagina del visor.

Sintaxis de facets de Primo VE:
    El formato es propio del producto, no la sintaxis Solr habitual:
        mfacet=NOMBRE_FACET,include,DESDE%7C,%7CHASTA,POSICION
    donde %7C es el pipe (|) codificado y POSICION es la posicion del
    facet dentro del stack (entero, >= 1). Ejemplos:
        Fechas: mfacet=searchcreationdate,include,2020%7C,%7C2026,2
        Idioma: mfacet=lang,include,eng,1
"""

import os
import re
import sys
import time
import logging
import requests
from typing import List, Optional, Set, Tuple
from base_scraper import BaseScraper, DocumentoResultado, FiltrosBusqueda

logger = logging.getLogger(__name__)

# ============================================================================
# CONSTANTES CONFIGURABLES
# ============================================================================
# Se mantienen al tope como variables sueltas para facilitar ajustes sin
# tener que recorrer el cuerpo de las funciones; SEARCH_SCOPE y TAB
# pueden ademas sobreescribirse desde configuracion.json.

BASE_URL = "https://labordoc.ilo.org"

# Vista de Primo VE de la OIT
VID = "41ILO_INST:41ILO_V2"

# Scope: ALL_ILO incluye todo el catalogo; ILO_DigiColl solo las
# colecciones digitalizadas (mas restrictivo, menos resultados).
def _leer_config_ilo():
    """Devuelve los valores ilo_search_scope e ilo_tab de
    configuracion.json, con caida a los defaults si el archivo no
    existe o no se puede leer."""
    import json as _json
    _defaults = {"ilo_search_scope": "ALL_ILO", "ilo_tab": "ALL_ILO"}
    _ruta = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                          "configuracion.json")
    try:
        with open(_ruta, "r", encoding="utf-8") as _f:
            _datos = _json.load(_f)
        return {
            "scope": _datos.get("ilo_search_scope", _defaults["ilo_search_scope"]),
            "tab": _datos.get("ilo_tab", _defaults["ilo_tab"]),
        }
    except Exception:
        return {"scope": _defaults["ilo_search_scope"],
                "tab": _defaults["ilo_tab"]}

_CFG_ILO = _leer_config_ilo()
SEARCH_SCOPE = _CFG_ILO["scope"]
TAB = _CFG_ILO["tab"]

# Algunos CDNs sirven contenido distinto al User-Agent por defecto de
# Playwright; declarar uno de Chrome reciente evita ese sesgo.
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
)

# Numero maximo de reintentos para navegaciones de busqueda
MAX_REINTENTOS_BUSQUEDA = 3

# Backoff exponencial entre reintentos (segundos)
BACKOFF_REINTENTOS = [3, 8, 15]

# Mapeo de idiomas a codigos MARC-21 usados en filtros de Primo VE
MAPA_IDIOMAS_ILO = {
    "es": "spa",
    "en": "eng",
    "fr": "fre",
    "ar": "ara",
    "zh": "chi",
    "ru": "rus",
    "de": "ger",
    "pt": "por",
}

# Nombres de idioma en distintas ortografias que aparecen en los campos
# packageName (edelivery) y label (representationInfo) de la API de Primo
# VE. Cada nombre se mapea al codigo corto que usamos internamente.
# Lista deliberadamente conservadora: solo entradas que son inequivocas.
_NOMBRES_IDIOMA_A_CODIGO = {
    "english": "en",
    "spanish": "es",
    "espanol": "es",
    "espa\u00f1ol": "es",
    "french": "fr",
    "francais": "fr",
    "fran\u00e7ais": "fr",
    "arabic": "ar",
    "chinese": "zh",
    "russian": "ru",
}


def _inferir_idioma_desde_texto(texto: Optional[str]) -> Optional[str]:
    """Intenta deducir el codigo de idioma desde un texto descriptivo.

    La API de Primo VE entrega el idioma del PDF en campos de texto libre
    como 'English - Full text' (en representationInfo.data.files[].label) o
    'Francais' (en edelivery.electronicServices[].packageName), no como un
    codigo estructurado. Esta funcion mapea esas variantes a los codigos
    cortos del proyecto ('en', 'es', 'fr', 'ar', 'zh', 'ru').

    El criterio es deliberadamente conservador: si el texto es vacio,
    ambiguo (ej. 'Digital Version') o no contiene un nombre de idioma
    reconocible, devuelve None. La logica de filtrado superior trata None
    como 'no se puede determinar, dejar pasar', para no descartar
    documentos legitimos por ausencia de metadata.
    """
    if not texto:
        return None

    # Normalizar: minusculas y comparacion como substring sobre cada
    # nombre conocido. Se usa substring (no igualdad) porque los labels
    # vienen en formato compuesto: "English - Full text", "Francais
    # (resume)", etc.
    texto_lower = texto.lower()
    for nombre, codigo in _NOMBRES_IDIOMA_A_CODIGO.items():
        if nombre in texto_lower:
            return codigo
    return None


# Mapeo desde valores que aparecen en pnx.display.type y pnx.display.genre
# del PNX de Primo VE hacia el enum de tipo_documento del proyecto. Cuando
# un valor no encaja en ninguna categoria conocida se guarda como 'otro'
# para mantener un dato util (la cadena cruda queda en metadatos_extra).
# La comparacion siempre se hace en minusculas y eliminando guiones bajos.
_MAPA_TIPO_PNX = {
    # Reportes
    "report": "reporte",
    "reports": "reporte",
    # Resoluciones
    "resolution": "resolucion",
    "resolutions": "resolucion",
    # Acuerdos / tratados
    "agreement": "acuerdo",
    "agreements": "acuerdo",
    "treaty": "acuerdo",
    "treaties": "acuerdo",
    # Libros y capitulos
    "book": "libro",
    "books": "libro",
    "book chapter": "libro",
    "book chapters": "libro",
    # Articulos
    "article": "articulo",
    "articles": "articulo",
    "journal article": "articulo",
}


def _normalizar_tipo(valor: str) -> str:
    """Lowercase y reemplazo de '_' por espacio para alinear con la
    convencion de _MAPA_TIPO_PNX (que guarda formas humanas)."""
    return valor.replace("_", " ").strip().lower()


def _mapear_tipo_documento_pnx(pnx_display: dict) -> Optional[str]:
    """Determina el tipo_documento del proyecto a partir de pnx.display.

    Lee primero pnx.display.type (que en Primo VE es un valor codificado:
    'ResearchPaper', 'book_chapters', 'conference_proceeding', etc.) y,
    si no encaja en el enum, prueba con pnx.display.genre (que trae
    texto humano: 'research paper', 'resolution', 'ILO pub', etc.).

    Devuelve el valor del enum (reporte/resolucion/acuerdo/libro/
    articulo) o 'otro' si reconocio el valor pero queda fuera del enum,
    o None si no encontro ningun campo legible.
    """
    # Recolectar candidatos
    candidatos: List[str] = []
    for clave in ("type", "genre"):
        bruto = pnx_display.get(clave)
        if isinstance(bruto, list):
            for item in bruto:
                if isinstance(item, str) and item.strip():
                    candidatos.append(_normalizar_tipo(item))
        elif isinstance(bruto, str) and bruto.strip():
            candidatos.append(_normalizar_tipo(bruto))

    if not candidatos:
        return None

    # Intentar mapeo directo a enum
    for cand in candidatos:
        if cand in _MAPA_TIPO_PNX:
            return _MAPA_TIPO_PNX[cand]

    # Reconocido pero fuera de enum (research paper, proceedings, etc.)
    return "otro"


def _mapear_idiomas_pnx(pnx_display: dict) -> List[str]:
    """Lee pnx.display.language y devuelve la lista de codigos cortos
    del proyecto (en/es/fr/...). El campo viene como una lista con un
    unico string que puede tener uno o varios codigos MARC separados por
    ';'. Por ejemplo: ['eng'] o ['eng;spa;por'] para registros
    multilingues (caso comun en Labordoc).

    Codigos MARC desconocidos se ignoran silenciosamente. Devuelve lista
    vacia cuando el campo esta ausente o no contiene codigos
    reconocibles, lo que el caller debe interpretar como 'sin
    informacion' y no como 'idioma vacio'."""
    bruto = pnx_display.get("language")
    if not bruto:
        return []
    # Normalizar a string unico
    if isinstance(bruto, list):
        if not bruto:
            return []
    texto = bruto[0] if isinstance(bruto, list) else bruto
    if not isinstance(texto, str):
        return []

    # Mapa inverso MARC -> codigo corto
    inverso = {v: k for k, v in MAPA_IDIOMAS_ILO.items()}

    codigos_marc = [c.strip().lower() for c in texto.split(";")]
    cortos: List[str] = []
    for marc in codigos_marc:
        if marc in inverso:
            cortos.append(inverso[marc])
    # Deduplicar manteniendo orden
    return list(dict.fromkeys(cortos))


def _limpiar_autor_pnx(valor: object) -> str:
    """Saca un autor presentable de un campo PNX que puede venir con
    los marcadores internos de Primo VE.

    Primo expone los autores en pnx.display.contributor / .creator con
    el formato 'Nombre$$QNombre' (donde la parte despues de $$Q es la
    forma para indizar). Tambien existen campos auxiliares con
    sub-marcadores ($$C, $$V, $$T, $$X, $$Z). Esta funcion corta todo a
    partir del primer '$$' y normaliza espacios.

    El campo puede venir como string o lista. Cuando es lista, se
    devuelven los nombres unicos unidos por '; ', mismo separador que
    usa scraper_un para autores multiples."""
    if valor is None:
        return ""
    items: List[str] = []
    if isinstance(valor, list):
        for item in valor:
            if isinstance(item, str):
                items.append(item)
    elif isinstance(valor, str):
        items.append(valor)

    limpios: List[str] = []
    for raw in items:
        # Cortar en el primer separador interno de Primo
        cortado = raw.split("$$")[0]
        cortado = cortado.strip().rstrip(",;")
        if cortado:
            limpios.append(cortado)

    # Deduplicar manteniendo orden
    unicos = list(dict.fromkeys(limpios))
    return "; ".join(unicos)


def _extraer_anio_pnx(valor: object) -> str:
    """Extrae el ano (4 digitos) del campo pnx.display.creationdate.

    Primo lo suele entregar como ['2023'] o, en algunos casos,
    ['2023-06-01']. Devolvemos solo el ano para alinearnos con el
    comportamiento del scraper UN."""
    if valor is None:
        return ""
    texto = ""
    if isinstance(valor, list) and valor:
        primero = valor[0]
        if isinstance(primero, str):
            texto = primero
    elif isinstance(valor, str):
        texto = valor
    m = re.search(r"\b(\d{4})\b", texto)
    return m.group(1) if m else ""


# Mapeo de tipos de documento a valores de rtype en Primo VE
MAPA_TIPOS_ILO = {
    "reporte": "reports",
    "resolucion": "government_documents",
    "acuerdo": "government_documents",
    "libro": "books",
    "articulo": "articles",
}

# ============================================================================
# LISTA NEGRA DE PATRONES DE URL
# ============================================================================
# Estos patrones aparecen en el HTML de los registros pero no son PDFs
# descargables. Agregar nuevos patrones aqui es trivial sin tocar la logica
# de extraccion.
PATRONES_URL_IGNORADAS = [
    "/thumbnail/",      # Imagenes de portada de Primo (devuelven text/html)
    "ignoredefault",    # Parametro de thumbnails
    "/intranet/",       # Endpoint interno de la OIT (devuelve 403 Forbidden)
    ".css", ".js", ".png", ".jpg", ".jpeg", ".gif", ".svg", ".ico",
    "google.com", "facebook.com", "twitter.com", "linkedin.com",
    "analytics", "tracking", "googletagmanager",
]


# ============================================================================
# FUNCION AUXILIAR: descarga con progreso visible en consola
# ============================================================================
def _descargar_con_progreso(respuesta, ruta_archivo: str,
                             titulo_doc: str) -> bool:
    """
    Escribe el contenido de una respuesta HTTP en disco mostrando el progreso
    en una sola linea de la consola (usando \\r para sobreescribir).

    El usuario ve algo como:
        Descargando: Report on child labour... (2.3 MB / 6.4 MB)
    y la linea se actualiza cada ~100 KB o ~0.25s, lo que ocurra primero.
    Al completar, se limpia la linea para que el siguiente print no quede
    pegado a restos del contador.

    Args:
        respuesta: objeto Response de requests con stream=True.
        ruta_archivo: ruta donde guardar el PDF.
        titulo_doc: titulo del documento para mostrar al usuario.

    Returns:
        True si la descarga se escribio correctamente, False si hubo error.
    """
    # Titulo truncado para que quepa en una linea de terminal (~80 cols)
    titulo_corto = (titulo_doc[:45] + "...") if len(titulo_doc) > 48 else titulo_doc

    # Intentar obtener el tamano total del archivo desde Content-Length
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

                # Actualizar el progreso cada ~100 KB o cada ~0.25s
                bytes_desde_ultimo = bytes_descargados - ultimo_print_bytes
                tiempo_desde_ultimo = time.time() - ultimo_print_tiempo
                if bytes_desde_ultimo >= 100 * 1024 or tiempo_desde_ultimo >= 0.25:
                    _imprimir_progreso(
                        titulo_corto, bytes_descargados, total_bytes
                    )
                    ultimo_print_bytes = bytes_descargados
                    ultimo_print_tiempo = time.time()

        # Impresion final (100%) y salto de linea para cerrar la linea
        _imprimir_progreso(titulo_corto, bytes_descargados, total_bytes)
        # Limpiar la linea de progreso para que el siguiente mensaje empiece limpio
        sys.stdout.write("\r" + " " * 78 + "\r")
        sys.stdout.flush()
        return True

    except Exception as e:
        # Limpiar linea de progreso antes de loguear el error
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
        # No conocemos el tamano total (servidor no mando Content-Length)
        sufijo = f" ({mb_desc:.1f} MB)"
    # Reservar 78 cols. Prefijo = "    Descargando: ", sufijo tiene el progreso.
    # El titulo se trunca para que no haya overflow.
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
    """
    Scraper para Labordoc (OIT/ILO) usando Playwright.

    Atributos post-busqueda:
        ultima_degradacion_filtro: None si la busqueda uso los filtros exactos
            del usuario, o un dict describiendo la degradacion si algun filtro
            se desactivo por no arrojar resultados.
    """

    RESULTADOS_POR_PAGINA = 10  # Primo VE muestra 10 resultados por pagina

    def __init__(self):
        # Cuando el filtro de tipo no produce resultados se relaja y se
        # vuelve a buscar; el dict resultante deja constancia para que
        # main.py pueda informar al usuario sin tocar la interfaz base.
        self.ultima_degradacion_filtro: Optional[dict] = None

        # Contadores que alimentan el resumen agregado al final de cada
        # busqueda. Se reinician al comienzo de cada search().
        self.diag_total_visitados: int = 0
        self.diag_con_pdf_primer_intento: int = 0
        self.diag_pdf_via_api_rest: int = 0       # URLs via /primaws/edelivery
        self.diag_pdf_sin_scroll: int = 0         # encontradas antes del scroll
        self.diag_rescatados_por_scroll: int = 0  # render disparado por scroll
        self.diag_con_pdf_segundo_intento: int = 0  # AJAX tardio (post-scroll)
        self.diag_sin_pdf_explicito: int = 0      # mensaje explicito de Primo
        self.diag_sin_pdf_tras_reintento: int = 0  # ceros sin razon clara
        self.diag_error_navegacion: int = 0       # timeout / error de red

        # JWT de invitado de Primo VE: se obtiene una vez al inicio de la
        # sesion y se reusa hasta su vencimiento (~23h). Ante un 401 se
        # vuelve a pedir automaticamente.
        self._jwt_invitado: Optional[str] = None
        self._jwt_obtenido_ts: float = 0.0

    def nombre_fuente(self) -> str:
        return "ILO Labordoc"

    def search(self, filtros: FiltrosBusqueda,
               ids_excluir: Optional[Set[str]] = None) -> List[DocumentoResultado]:
        """Busca documentos en Labordoc.

        Si el filtro de tipo de documento no devuelve resultados, se
        relaja automaticamente; el cambio queda registrado en
        self.ultima_degradacion_filtro para que main.py pueda informar
        al usuario."""
        self.ultima_degradacion_filtro = None

        ids_excluir = ids_excluir or set()

        self.diag_total_visitados = 0
        self.diag_con_pdf_primer_intento = 0
        self.diag_pdf_via_api_rest = 0
        self.diag_pdf_sin_scroll = 0
        self.diag_rescatados_por_scroll = 0
        self.diag_con_pdf_segundo_intento = 0
        self.diag_sin_pdf_explicito = 0
        self.diag_sin_pdf_tras_reintento = 0
        self.diag_error_navegacion = 0
        # El JWT no se resetea: se reusa entre busquedas mientras siga
        # vigente, lo que ahorra una llamada HTTP por sesion.

        query = " ".join(filtros.palabras_clave) if filtros.palabras_clave else ""
        if not query:
            logger.error("No se proporcionaron palabras clave para la busqueda.")
            return []

        logger.info(f"Iniciando busqueda en ILO Labordoc: '{query}'")
        logger.info(f"Limite configurado: {filtros.limite} documentos")
        logger.info(f"Scope de busqueda: {SEARCH_SCOPE}")
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

            # --- FASE 1: busqueda con el filtro de tipo si se solicito ---
            resultados = self._ejecutar_busqueda(
                pagina, query, filtros, usar_filtro_tipo=True,
                ids_excluir=ids_excluir,
            )

            # --- DEGRADACION: si se uso filtro de tipo y dio 0, reintentar sin el ---
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
                    pagina, query, filtros, usar_filtro_tipo=False,
                    ids_excluir=ids_excluir,
                )

            navegador.close()

        logger.info(f"Busqueda completada. Total de documentos encontrados: {len(resultados)}")

        # Resumen agregado de instrumentacion: ratio de exito de _obtener_url_pdf
        # y desglose de causas. Se imprime tanto al log como a consola para que
        # el usuario lo vea sin abrir errores.log.
        con_pdf = self.diag_con_pdf_primer_intento + self.diag_con_pdf_segundo_intento
        sin_pdf = (self.diag_sin_pdf_explicito + self.diag_sin_pdf_tras_reintento
                   + self.diag_error_navegacion)
        resumen = (
            f"Resumen ILO: {self.diag_total_visitados} encontrados | "
            f"{con_pdf} con PDF "
            f"({self.diag_pdf_via_api_rest} via API REST + "
            f"{self.diag_pdf_sin_scroll} sin scroll + "
            f"{self.diag_rescatados_por_scroll} rescatados por scroll + "
            f"{self.diag_con_pdf_segundo_intento} AJAX tardio) | "
            f"{sin_pdf} sin PDF "
            f"({self.diag_sin_pdf_explicito} legitimos por mensaje explicito, "
            f"{self.diag_sin_pdf_tras_reintento} vacios tras reintento, "
            f"{self.diag_error_navegacion} errores de navegacion)"
        )
        logger.info(resumen)
        print(f"\n  {resumen}\n")

        return resultados

    def _ejecutar_busqueda(self, pagina, query: str,
                           filtros: FiltrosBusqueda,
                           usar_filtro_tipo: bool,
                           ids_excluir: Optional[Set[str]] = None) -> List[DocumentoResultado]:
        """
        Ejecuta el loop de busqueda paginada sobre Primo VE.

        Metodo interno extraido para permitir el reintento de degradacion
        del filtro de tipo sin duplicar logica.

        Si ids_excluir se pasa, los documentos cuyo recid prefijado con "ILO:"
        esten en el set se saltan silenciosamente ANTES de llamar a la API REST
        para obtener URLs de descarga, para no desperdiciar llamadas.
        """
        ids_excluir = ids_excluir or set()
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
                # Fallaron todos los reintentos
                logger.error(
                    f"[{descripcion}] DIAGNOSTICO: no se pudo cargar la "
                    "pagina tras reintentos. Abortando esta busqueda."
                )
                break

            # Extraer resultados de la pagina
            documentos_pagina = self._extraer_resultados(pagina)

            # `total_en_pagina` cuenta lo que el servidor devolvio en esta
            # pagina, antes de filtrar. Se usa para decidir fin de paginacion.
            total_en_pagina = len(documentos_pagina)

            # Filtrar los documentos ya existentes en el historial. Se hace
            # ANTES de llamar a _obtener_url_pdf (que es costoso: implica
            # llamadas a la API REST para resolver S3 URLs).
            docs_a_procesar = []
            excluidos_esta_pagina = 0
            for doc in documentos_pagina:
                if doc.recid and f"ILO:{doc.recid}" in ids_excluir:
                    excluidos_esta_pagina += 1
                    continue
                docs_a_procesar.append(doc)

            logger.info(
                f"[{descripcion}] DIAGNOSTICO: HTML recibido "
                f"({len(html):,} bytes), {total_en_pagina} docs en la pagina, "
                f"{excluidos_esta_pagina} excluidos por historial, "
                f"{len(docs_a_procesar)} a procesar"
            )

            if total_en_pagina == 0:
                # Distinguir 0 resultados legitimo de problema de renderizado
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

            # Para cada documento NO excluido, consultar PNX para
            # rellenar metadatos bibliograficos (autor, fecha, idioma,
            # tipo) e intentar obtener las URLs de descarga.
            for doc in docs_a_procesar:
                if len(resultados) >= filtros.limite:
                    break

                # ── Paso 1: consultar PNX siempre que tengamos docid ──
                # El PNX expone metadatos que no estan disponibles en la
                # cadena edelivery -> representationInfo. La consulta es
                # tolerante a errores: si falla, devuelve dict vacio y el
                # documento queda con los campos vacios como hoy.
                meta_pnx: dict = {}
                docid_doc = self._extraer_docid(doc.url_fuente or "")
                if docid_doc:
                    meta_pnx = self._consultar_pnx_para_metadatos(docid_doc)
                    if meta_pnx:
                        if meta_pnx.get("autor"):
                            doc.autor = meta_pnx["autor"]
                        if meta_pnx.get("fecha"):
                            doc.fecha = meta_pnx["fecha"]
                        if meta_pnx.get("tipo_documento"):
                            doc.tipo_documento = meta_pnx["tipo_documento"]
                        # Idioma: lista de codigos cortos. Para serializar
                        # al CSV/JSON usamos el separador ';' que ya emplea
                        # el propio PNX en el campo MARC original.
                        idiomas_codigo = meta_pnx.get("idiomas_codigo") or []
                        if idiomas_codigo:
                            doc.idioma = ";".join(idiomas_codigo)

                # ── Paso 2: filtro de idioma reforzado por PNX ──
                # Linea de defensa adicional sobre el filtro existente
                # de packageName/label, util sobre todo para documentos
                # que entren por el camino Playwright (donde no hay otra
                # validacion de idioma). Politica conservadora:
                #   - Si filtros.idioma esta vacio: no aplica.
                #   - Si el PNX no devolvio idiomas: no descartar (puede
                #     ser un docid valido cuyo PNX falla esporadicamente
                #     o un registro sin etiqueta MARC; preferimos colar a
                #     descartar legitimos).
                #   - Si el PNX dio una lista y NINGUN codigo coincide
                #     con los pedidos: descartar silenciosamente. La
                #     paginacion traera otro candidato.
                #   - Registros multilingues (eng;spa;por) pasan en
                #     cuanto uno solo de sus idiomas coincida; el filtro
                #     de URL existente ya se encarga de elegir el PDF
                #     correcto entre los disponibles.
                if filtros.idioma and meta_pnx:
                    idiomas_pnx = meta_pnx.get("idiomas_codigo") or []
                    if idiomas_pnx:
                        pedidos = set(filtros.idioma)
                        if not (set(idiomas_pnx) & pedidos):
                            logger.info(
                                f"DIAGNOSTICO: doc descartado por idioma PNX "
                                f"docid={docid_doc} pnx={idiomas_pnx} "
                                f"pedidos={sorted(pedidos)}"
                            )
                            continue

                # ── Paso 3: obtener URLs de descarga (sin cambios) ──
                if doc.url_fuente:
                    urls_pdf = self._obtener_url_pdf(
                        pagina, doc.url_fuente, filtros.idioma
                    )
                    # Cuando el usuario pidio un filtro de idioma y el
                    # documento no expone ningun PDF en los idiomas
                    # solicitados, lo descartamos silenciosamente y
                    # dejamos que la paginacion traiga otro candidato.
                    # Mismo patron que el descarte por historial de la
                    # Tanda 3: el bucle `while len(resultados) <
                    # filtros.limite` ya pagina extra automaticamente.
                    # Cuando filtros.idioma esta vacio, esta condicion
                    # nunca se cumple y el comportamiento es identico al
                    # previo (se preserva el doc aunque no tenga URLs
                    # para que el download lo registre como fallido).
                    if filtros.idioma and not urls_pdf:
                        continue
                    doc.urls_descarga = urls_pdf
                resultados.append(doc)

            logger.info(
                f"[{descripcion}] {total_en_pagina} resultados de esta "
                f"pagina (total acumulado: {len(resultados)}/{filtros.limite})"
            )

            # Corte de paginacion: el servidor devolvio pocos docs ORIGINALES
            # (antes de excluir). Si total_en_pagina < RESULTADOS_POR_PAGINA
            # probablemente es la ultima pagina real y no hay mas resultados
            # en el backend, aunque tengamos el limite sin alcanzar.
            if total_en_pagina < self.RESULTADOS_POR_PAGINA:
                break

            offset += self.RESULTADOS_POR_PAGINA
            time.sleep(2)

        return resultados[:filtros.limite]

    def _construir_url_busqueda(self, query: str, filtros: FiltrosBusqueda,
                                 offset: int, usar_filtro_tipo: bool) -> str:
        """
        Construye la URL de busqueda para Primo VE con todos los filtros.

        Formato de query en Primo VE: campo,operador,valor
        Ejemplo: any,contains,child labour

        IMPORTANTE - sintaxis de facets:
            ANTES (bug): &facet=searchcreationdate,include,[2020+TO+2030]
            AHORA:       &mfacet=searchcreationdate,include,2020%7C,%7C2030,POS

        Primo VE acepta la sintaxis vieja sin error pero devuelve 0 hits
        silenciosamente. Este era el bug principal que hacia que las
        busquedas con fechas dieran cero resultados.
        """
        # Codificar el query: espacios como %20
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

        # Contador de posicion para los facets (Primo VE requiere posicion)
        # La posicion determina el orden en el stack de facets del usuario.
        posicion_facet = 1

        # Filtro de idioma: mfacet=lang,include,eng,POS
        if filtros.idioma:
            for codigo_idioma in filtros.idioma:
                if codigo_idioma in MAPA_IDIOMAS_ILO:
                    codigo = MAPA_IDIOMAS_ILO[codigo_idioma]
                    url += f"&mfacet=lang,include,{codigo},{posicion_facet}"
                    posicion_facet += 1

        # Filtro de tipo de documento: mfacet=rtype,include,reports,POS
        if usar_filtro_tipo and filtros.tipo_documento:
            clave_tipo = filtros.tipo_documento.lower()
            tipo_primo = MAPA_TIPOS_ILO.get(clave_tipo, "")
            if tipo_primo:
                url += f"&mfacet=rtype,include,{tipo_primo},{posicion_facet}"
                posicion_facet += 1

        # Filtro de fechas: mfacet=searchcreationdate,include,DESDE%7C,%7CHASTA,POS
        # %7C es el pipe (|) codificado para URL
        if filtros.fecha_desde or filtros.fecha_hasta:
            desde = filtros.fecha_desde or 1900
            hasta = filtros.fecha_hasta or 2030
            url += (f"&mfacet=searchcreationdate,include,"
                    f"{desde}%7C,%7C{hasta},{posicion_facet}")
            posicion_facet += 1

        return url

    def _navegar_busqueda_con_reintentos(self, pagina, url: str,
                                          descripcion: str) -> Optional[str]:
        """Carga una pagina de resultados de Primo VE con reintentos y
        backoff.

        wait_until='domcontentloaded' es deliberado: Primo VE inyecta
        scripts de analytics que mantienen la red activa
        indefinidamente, asi que 'networkidle' nunca completa antes del
        timeout. La pagina se considera lista cuando aparece alguno de
        los selectores que Angular renderiza al terminar de mostrar
        resultados (o el mensaje de 'no results')."""
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
                    # Si el selector no aparece dejamos seguir y que el
                    # caller inspeccione el HTML; podria haber contenido
                    # parcial util pese al fallo del selector.
                    logger.warning(
                        f"[{descripcion}] DIAGNOSTICO: selector de resultados "
                        f"NO aparecio en 20s (intento {intento}). "
                        "Puede ser: render lento, servidor saturado, "
                        "o cambio en el HTML de Primo VE."
                    )

                # Margen para el ultimo ciclo de digest de Angular
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
        """Distingue una pagina con cero resultados legitimos de un
        fallo de carga silencioso, comprobando los textos y selectores
        que Primo VE muestra cuando no hay coincidencias."""
        # Primo VE usa prm-no-search-result cuando no hay hits
        try:
            sin_resultados = pagina.query_selector(
                "prm-no-search-result, .no-results, .zero-results"
            )
            if sin_resultados:
                return True
        except Exception:
            pass

        # Tambien revisar en el HTML
        indicadores = [
            "no results found",
            "no records matching",
            "prm-no-search-result",
            "0 results",
        ]
        html_lower = html.lower()
        return any(ind in html_lower for ind in indicadores)

    def _extraer_resultados(self, pagina) -> List[DocumentoResultado]:
        """Extrae titulos, docids y URLs de los registros visibles en la
        pagina actual.

        El emparejamiento titulo-docid se hace dentro del mismo elemento
        del DOM (no por orden de aparicion) porque las dos extracciones
        independientes pueden desalinearse si algun resultado no tiene
        titulo. Cuando Playwright no ve elementos visibles, se recurre a
        un parseo del HTML como respaldo.

        Limitacion conocida: el regex que detecta enlaces
        '/discovery/fulldisplay?...' captura tambien resultados del panel
        lateral 'Featured Results' de Primo VE. En busquedas con scopes
        configurables, esto puede inflar la cantidad de documentos
        devueltos respecto al limite. Filtrar por scope o por
        contenedor padre (descartando lo que cuelga de
        'prm-explore-main-results') seria el arreglo natural."""
        documentos = []
        import html as html_module

        # Diccionario {docid: titulo} construido con Playwright sobre los
        # elementos visibles. Esto garantiza que el titulo corresponde al
        # docid correcto, no al indice.
        titulos_por_docid: dict = {}

        try:
            # Primo VE renderiza cada resultado dentro de un contenedor.
            # Probamos varios selectores porque Primo ha cambiado nombres
            # entre versiones.
            contenedores = pagina.query_selector_all(
                "prm-brief-result-container, "
                "prm-search-result-list-line, "
                "prm-search-result-list-line-content, "
                "[class*='list-item-primary-content']"
            )

            for cont in contenedores:
                try:
                    # Dentro del contenedor, buscar el enlace que tiene
                    # href con fulldisplay y docid. Ese es el enlace del titulo.
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

                            # El texto de este enlace es el titulo, salvo que
                            # sea vacio o muy corto (en cuyo caso puede ser un
                            # enlace "Ver" o similar)
                            texto = (enlace.inner_text() or "").strip()
                            if texto and len(texto) > 3:
                                # Solo guardar el primer titulo util por docid
                                if docid not in titulos_por_docid:
                                    titulos_por_docid[docid] = texto
                                break  # siguiente contenedor
                        except Exception:
                            continue
                except Exception:
                    continue
        except Exception as e:
            logger.debug(f"Error extrayendo titulos con Playwright: {e}")

        # Ahora extraer TODOS los docids del HTML (mas completo que Playwright,
        # porque detecta enlaces que pudieron no estar en contenedores visibles)
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

                # Pareo por docid (no por indice): garantiza alineacion correcta
                if docid in titulos_por_docid:
                    doc.titulo = titulos_por_docid[docid]
                else:
                    # Fallback: titulo generico (ultimo recurso, preservado
                    # para no crashear ni dejar titulos vacios en el CSV)
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

    def _extraer_docid(self, url_registro: str) -> Optional[str]:
        """
        Extrae el docid (ej: 'alma995339593202676') de una URL de registro
        de Primo VE. Devuelve None si no se puede extraer.
        """
        match = re.search(r'docid=([^&\s]+)', url_registro)
        return match.group(1) if match else None

    def _obtener_jwt_invitado(self) -> Optional[str]:
        """
        Obtiene un JWT de invitado anonimo de Primo VE para autenticar
        las llamadas a /primaws/. Lo cachea en memoria por hasta 23h
        (el JWT expira a las 24h pero refrescamos antes por margen).

        Returns:
            El JWT como string (sin comillas, listo para Bearer), o None
            si no se pudo obtener.
        """
        # Reusar JWT cacheado si tiene menos de 23h
        ahora = time.time()
        if (self._jwt_invitado is not None
                and (ahora - self._jwt_obtenido_ts) < 23 * 3600):
            return self._jwt_invitado

        try:
            url_jwt = (
                f"{BASE_URL}/primaws/rest/pub/institution/"
                f"{VID.split(':')[0]}/guestJwt"
                f"?isGuest=true&lang=en&viewId={VID}"
            )
            respuesta = requests.get(
                url_jwt,
                timeout=(10, 20),
                headers={
                    "User-Agent": USER_AGENT,
                    "Accept": "application/json, text/plain, */*",
                    "Referer": f"{BASE_URL}/discovery/search?vid={VID}",
                }
            )
            respuesta.raise_for_status()
            # La respuesta es un string JSON con comillas: "eyJraWQ..."
            # Lo parseamos y nos quedamos con el contenido sin comillas.
            jwt_raw = respuesta.text.strip()
            if jwt_raw.startswith('"') and jwt_raw.endswith('"'):
                jwt = jwt_raw[1:-1]
            else:
                jwt = jwt_raw
            if not jwt:
                logger.warning(
                    "DIAGNOSTICO: respuesta vacia del endpoint guestJwt"
                )
                return None
            self._jwt_invitado = jwt
            self._jwt_obtenido_ts = ahora
            logger.info(
                f"JWT de invitado obtenido (longitud: {len(jwt)} chars)"
            )
            return jwt
        except Exception as e:
            logger.warning(
                f"DIAGNOSTICO: no se pudo obtener JWT de invitado: "
                f"{type(e).__name__}: {e}"
            )
            return None

    def _extraer_urls_via_api_rest(self, docid: str,
                                    idiomas_pedidos: Optional[List[str]] = None
                                    ) -> List[str]:
        """Devuelve las URLs S3 firmadas de los PDFs asociados al
        documento, consultando la API REST publica de Primo VE.

        El procedimiento usa tres llamadas:
            1. POST /primaws/rest/pub/edelivery/{docid} para enumerar los
               servicios electronicos del documento.
            2. GET /primaws/rest/priv/delivery/representationInfo por
               cada ilsApiId, lo que devuelve la URL S3 firmada con
               vigencia de aproximadamente una hora.
            3. La descarga final se realiza con requests sobre la URL S3,
               sin headers adicionales (la firma autentica la peticion).

        Devuelve lista vacia si la API falla o el documento no tiene
        archivos descargables. El JWT guest se reutiliza entre llamadas
        y se renueva automaticamente si el servidor responde 401.

        Filtro de idioma (cuando idiomas_pedidos no es vacio):
            Primo VE expone el idioma del PDF en campos de texto libre,
            no en un campo estructurado. Usamos dos fuentes ya
            disponibles en la cadena REST, por orden de prioridad:
            (1) packageName de cada electronicService, y (2) label de
            cada archivo dentro de representationInfo. Solo se descartan
            archivos cuando alguno de esos textos identifica
            inequivocamente un idioma distinto al pedido. Si ninguno de
            los dos textos permite inferir un idioma reconocible, el
            archivo pasa el filtro: la politica conservadora prefiere
            colar algun documento sin metadata de idioma a descartar
            documentos legitimos por ausencia de etiqueta.

            Cuando idiomas_pedidos es None o vacio, el comportamiento es
            identico al previo (sin filtrado, devuelve todo)."""
        jwt = self._obtener_jwt_invitado()
        if jwt is None:
            return []

        inst = VID.split(":")[0]  # '41ILO_INST'

        servicios = self._llamar_edelivery(docid, jwt)
        if servicios is None:
            # JWT vencio: invalidamos el cache, lo renovamos y reintentamos
            logger.info("DIAGNOSTICO: JWT refrescado tras 401 en edelivery")
            self._jwt_invitado = None
            self._jwt_obtenido_ts = 0.0
            jwt = self._obtener_jwt_invitado()
            if jwt is None:
                return []
            servicios = self._llamar_edelivery(docid, jwt)

        if not servicios:
            return []

        # Normalizar la lista de idiomas pedidos a un set de codigos cortos
        # para hacer comparaciones O(1).
        idiomas_set = set(idiomas_pedidos) if idiomas_pedidos else set()

        urls_descarga: List[str] = []
        descartados_por_idioma = 0
        for ils_id, package_name in servicios:
            # Filtro temprano: si el packageName del servicio identifica un
            # idioma y no coincide con el pedido, evitamos la llamada a
            # representationInfo. Esto ahorra requests para documentos cuyo
            # idioma esta declarado a nivel de servicio (caso comun en los
            # registros multilingues de Labordoc).
            if idiomas_set:
                idioma_pkg = _inferir_idioma_desde_texto(package_name)
                if idioma_pkg is not None and idioma_pkg not in idiomas_set:
                    descartados_por_idioma += 1
                    continue

            archivos = self._llamar_representation_info(ils_id, jwt, inst)
            for download_url, label in archivos:
                # Filtro tardio: si el label del archivo individual
                # identifica un idioma y no coincide, descartar el archivo.
                # Si el label esta vacio o es ambiguo, dejar pasar.
                if idiomas_set:
                    idioma_lbl = _inferir_idioma_desde_texto(label)
                    if idioma_lbl is not None and idioma_lbl not in idiomas_set:
                        descartados_por_idioma += 1
                        continue
                urls_descarga.append(download_url)

        if descartados_por_idioma > 0:
            logger.info(
                f"DIAGNOSTICO: {descartados_por_idioma} URL(s) descartada(s) "
                f"por filtro de idioma en docid={docid} "
                f"(pedidos: {sorted(idiomas_set)})"
            )

        # Deduplicar manteniendo el orden de aparicion
        urls_unicas = list(dict.fromkeys(urls_descarga))

        # Aplicar lista negra
        urls_filtradas = [
            u for u in urls_unicas
            if not any(patron in u.lower() for patron in PATRONES_URL_IGNORADAS)
        ]

        return urls_filtradas

    def _llamar_edelivery(self, docid: str, jwt: str
                          ) -> Optional[List[Tuple[str, str]]]:
        """
        POST a /primaws/rest/pub/edelivery/{docid}.
        Devuelve lista de tuplas (ilsApiId, packageName) para los servicios
        PDF descargables. El packageName se preserva porque la API de Primo
        VE usa ese campo de texto como portador del idioma del PDF
        (ej. 'English - Full text', 'Francais'); el caller lo necesita para
        filtrar por idioma sin emitir requests adicionales.

        Devuelve None si recibe 401 (senal de JWT vencido para el caller).
        Devuelve lista vacia si no hay servicios o hay otro error.
        """
        try:
            url_endpoint = (
                f"{BASE_URL}/primaws/rest/pub/edelivery/{docid}"
                f"?vid={VID}&lang=en&googleScholar=false"
            )
            respuesta = requests.post(
                url_endpoint,
                timeout=(10, 20),
                headers={
                    "User-Agent": USER_AGENT,
                    "Accept": "application/json, text/plain, */*",
                    "Authorization": f"Bearer {jwt}",
                    "Content-Type": "application/json",
                    "Referer": (
                        f"{BASE_URL}/discovery/fulldisplay"
                        f"?docid={docid}&vid={VID}&lang=en"
                    ),
                },
                data="{}",
            )

            if respuesta.status_code == 401:
                return None  # senal para el caller de refresh JWT

            respuesta.raise_for_status()
            datos = respuesta.json()
        except Exception as e:
            logger.debug(
                f"edelivery fallo para {docid}: {type(e).__name__}: {e}"
            )
            return []

        servicios = datos.get("electronicServices", []) if isinstance(datos, dict) else []
        if not servicios:
            return []

        items: List[Tuple[str, str]] = []
        for svc in servicios:
            if not isinstance(svc, dict):
                continue
            tipo_servicio = (svc.get("serviceType") or "").upper()
            tipo_archivo = (svc.get("fileType") or "").lower()
            tiene_acceso = svc.get("hasAccess", True)

            if tipo_servicio != "DIGITAL":
                continue
            if not tiene_acceso:
                continue
            if tipo_archivo and tipo_archivo != "pdf":
                continue

            ils_id = svc.get("ilsApiId")
            if ils_id:
                package_name = svc.get("packageName") or ""
                items.append((str(ils_id), package_name))

        return items

    def _llamar_representation_info(self, ils_api_id: str, jwt: str,
                                     inst: str) -> List[Tuple[str, str]]:
        """
        GET a /primaws/rest/priv/delivery/representationInfo?pid={ilsApiId}.
        Devuelve lista de tuplas (downloadUrl, label) del array data.files.
        El label es texto descriptivo que tipicamente incluye el idioma
        del PDF (ej. 'English - Full text', 'Espanol'); se preserva para
        que el caller pueda filtrar por idioma sin requests adicionales.
        Cuando el archivo no expone label se devuelve cadena vacia.
        """
        try:
            url_rep = (
                f"{BASE_URL}/primaws/rest/priv/delivery/representationInfo"
                f"?inst={inst}&lang=en&mmsId=&pid={ils_api_id}"
            )
            respuesta = requests.get(
                url_rep,
                timeout=(10, 20),
                headers={
                    "User-Agent": USER_AGENT,
                    "Accept": "application/json, text/plain, */*",
                    "Authorization": f"Bearer {jwt}",
                    "Referer": f"{BASE_URL}/discovery/search?vid={VID}",
                },
            )
            respuesta.raise_for_status()
            datos = respuesta.json()
        except Exception as e:
            logger.debug(
                f"representationInfo fallo para pid={ils_api_id}: "
                f"{type(e).__name__}: {e}"
            )
            return []

        # Extraer data.files[*].downloadUrl
        data = datos.get("data", {})
        if not isinstance(data, dict):
            return []

        archivos = data.get("files", [])
        if not isinstance(archivos, list):
            return []

        urls: List[Tuple[str, str]] = []
        for archivo in archivos:
            if not isinstance(archivo, dict):
                continue
            # Verificar que es PDF y tiene acceso
            ct = (archivo.get("contentType") or "").lower()
            acceso = archivo.get("isAccessRightsOk", True)
            if not acceso:
                continue
            if ct and "pdf" not in ct:
                continue

            download_url = archivo.get("downloadUrl") or ""
            if download_url and download_url.startswith("http"):
                label = archivo.get("label") or ""
                urls.append((download_url, label))

        return urls

    def _consultar_pnx_para_metadatos(self, docid: str) -> dict:
        """Consulta el endpoint PNX y devuelve los metadatos
        bibliograficos del documento ya parseados al formato del proyecto.

        El PNX (Primo Normalized XML, en JSON) es la fuente mas limpia y
        estable para los campos autor, fecha, idioma y tipo_documento que
        no expone la cadena edelivery -> representationInfo. Se consulta
        en GET /primaws/rest/pub/pnxs/L/{docid} reusando el JWT guest
        cacheado.

        Devuelve un diccionario con cuatro claves: 'autor', 'fecha',
        'idiomas_codigo' (lista de codigos cortos como ['en','es']),
        'tipo_documento'. Los campos no encontrados quedan en cadena
        vacia o lista vacia, segun corresponda. Si la peticion falla por
        cualquier motivo (red, 401, 404, JSON malformado), devuelve un
        dict vacio sin lanzar excepcion: el caller debe interpretarlo
        como 'sin metadatos PNX' y no como error.

        El idioma se entrega como lista para preservar registros
        multilingues (un mismo documento puede aparecer marcado como
        'eng;spa;por' en MARC). El caller decide como serializarlo."""
        jwt = self._obtener_jwt_invitado()
        if jwt is None:
            return {}

        url = (
            f"{BASE_URL}/primaws/rest/pub/pnxs/L/{docid}"
            f"?vid={VID}&lang=en"
        )
        try:
            respuesta = requests.get(
                url,
                timeout=(10, 20),
                headers={
                    "User-Agent": USER_AGENT,
                    "Accept": "application/json, text/plain, */*",
                    "Authorization": f"Bearer {jwt}",
                    "Referer": (
                        f"{BASE_URL}/discovery/fulldisplay"
                        f"?docid={docid}&vid={VID}&lang=en"
                    ),
                },
            )

            # 401 -> JWT vencido. Renovar una vez y reintentar; si sigue
            # fallando devolvemos vacio sin escalar el error.
            if respuesta.status_code == 401:
                self._jwt_invitado = None
                self._jwt_obtenido_ts = 0.0
                jwt = self._obtener_jwt_invitado()
                if jwt is None:
                    return {}
                respuesta = requests.get(
                    url,
                    timeout=(10, 20),
                    headers={
                        "User-Agent": USER_AGENT,
                        "Accept": "application/json, text/plain, */*",
                        "Authorization": f"Bearer {jwt}",
                        "Referer": (
                            f"{BASE_URL}/discovery/fulldisplay"
                            f"?docid={docid}&vid={VID}&lang=en"
                        ),
                    },
                )

            if respuesta.status_code != 200:
                return {}
            datos = respuesta.json()
        except Exception as e:
            logger.debug(
                f"PNX fallo para {docid}: {type(e).__name__}: {e}"
            )
            return {}

        # El PNX viene en el primer nivel de la respuesta. Las secciones
        # display y facets contienen los campos bibliograficos limpios.
        pnx = datos.get("pnx") if isinstance(datos, dict) else None
        if not isinstance(pnx, dict):
            return {}
        display = pnx.get("display") if isinstance(pnx, dict) else None
        if not isinstance(display, dict):
            display = {}
        sort_section = pnx.get("sort") if isinstance(pnx, dict) else None
        if not isinstance(sort_section, dict):
            sort_section = {}

        # Autor: preferimos sort.author cuando existe (texto limpio sin
        # marcadores Primo); cae a display.creator y luego a
        # display.contributor, ambos limpiados con _limpiar_autor_pnx.
        autor = ""
        sort_author = sort_section.get("author")
        if sort_author:
            autor = _limpiar_autor_pnx(sort_author)
        if not autor:
            autor = _limpiar_autor_pnx(display.get("creator"))
        if not autor:
            autor = _limpiar_autor_pnx(display.get("contributor"))

        return {
            "autor": autor,
            "fecha": _extraer_anio_pnx(display.get("creationdate")),
            "idiomas_codigo": _mapear_idiomas_pnx(display),
            "tipo_documento": _mapear_tipo_documento_pnx(display) or "",
        }

    def _obtener_url_pdf(self, pagina, url_registro: str,
                          idiomas_pedidos: Optional[List[str]] = None
                          ) -> List[str]:
        """Devuelve la lista de URLs de PDF asociadas a un documento.

        El camino preferente es la API REST de Primo VE (mas rapida y
        directa, ~300 ms por documento). Cuando esa via no devuelve
        nada, se cae al camino de DOM con Playwright: navegacion al
        fulldisplay, extraccion en frio, scroll dirigido a los
        contenedores con la directiva 'prm-digest-when-in-view' (Primo
        VE realiza render diferido segun viewport), y por ultimo un
        reintento tras esperar a posibles AJAX tardios. Cada via
        incrementa contadores diagnosticos diferentes que alimentan el
        resumen agregado de la busqueda.

        El parametro idiomas_pedidos se propaga unicamente al camino API
        REST, que es donde se dispone de la metadata de idioma del PDF
        sin requests adicionales. El camino Playwright extrae enlaces
        del DOM y no recibe filtrado de idioma, ya que esos resultados
        suelen ser excepcionales (la API REST cubre el caso del 93 al
        100 por ciento) y los registros pasados por ese fallback se
        validan con el filtro de URL de Primo VE."""
        self.diag_total_visitados += 1

        # ── PASO 0: API REST de Primo VE (Opcion 2a, camino principal) ──
        # Extraer el docid de la URL del registro y consultar el endpoint
        # /primaws/rest/pub/edelivery directamente. Si funciona, evitamos
        # toda la danza de Playwright + scroll.
        docid = self._extraer_docid(url_registro)
        if docid:
            urls_api = self._extraer_urls_via_api_rest(docid, idiomas_pedidos)
            if urls_api:
                self.diag_con_pdf_primer_intento += 1
                self.diag_pdf_via_api_rest += 1
                logger.info(
                    f"DIAGNOSTICO: URLs via API REST para {url_registro}: "
                    f"{len(urls_api)}"
                )
                return urls_api[:5]
            # Si la API REST no devolvio URLs, caemos al flujo Playwright.
            # No es necesariamente fallo del API: puede ser que el documento
            # legitimamente no tenga PDFs (en cuyo caso el flujo Playwright
            # detectara mensaje explicito "no PDF" y lo registrara correcto).

        try:
            import html as html_module
            url_limpia = html_module.unescape(url_registro)

            # ── PASO 1: navegar + wait contenedor rapido ──
            pagina.goto(url_limpia, wait_until="domcontentloaded", timeout=30000)

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
                time.sleep(0.3)
            except Exception:
                tiempo_esperado = time.time() - inicio_espera
                logger.warning(
                    f"DIAGNOSTICO: selector de PDF no aparecio en "
                    f"{tiempo_esperado:.1f}s para {url_limpia}, "
                    "usando fallback best-effort (sleep 3s)"
                )
                time.sleep(3)

            # ── PASO 2: extraccion fria (sin scroll) ──
            urls_pdf = self._extraer_urls_pdf_del_dom(pagina)

            if urls_pdf:
                self.diag_con_pdf_primer_intento += 1
                self.diag_pdf_sin_scroll += 1
                logger.debug(
                    f"URLs de PDF (sin scroll) para {url_registro}: "
                    f"{len(urls_pdf)}"
                )
                return urls_pdf[:5]

            # ── PASO 3: mensaje explicito "no PDF" ──
            # Detectar ANTES del scroll: los mensajes de "no full text" se
            # renderizan en el HTML inicial sin lazy loading. Si Primo dice
            # explicitamente que no hay PDF, scrollear es desperdicio.
            if self._tiene_mensaje_no_pdf_explicito(pagina):
                self.diag_sin_pdf_explicito += 1
                logger.info(
                    f"DIAGNOSTICO: documento sin PDF segun mensaje explicito "
                    f"de Primo VE: {url_limpia}"
                )
                return []

            # ── PASO 4: scroll a los elementos con directiva onInView ──
            # CAMBIO: el selector apunta directamente al div con la directiva
            # prm-digest-when-in-view (que es lo que Angular vigila), no a
            # contenedores ancestrales genericos que no tienen la directiva.
            # Los tres patrones cubren las distintas formas en que Angular
            # puede serializar la directiva en el HTML.
            selector_scroll = (
                "[prm-digest-when-in-view], "
                "div[in-view], "
                "div[prm-digest-when-in-view]"
            )
            try:
                contenedores = pagina.query_selector_all(selector_scroll)
                # Limite suave: scrollear a 8 elementos como maximo. Cada
                # scroll_into_view_if_needed dispara un viewport shift y
                # espera al settle, asi que muchos elementos se acumulan.
                for cont in contenedores[:8]:
                    try:
                        cont.scroll_into_view_if_needed()
                    except Exception:
                        pass
                logger.debug(
                    f"Scroll aplicado a {min(len(contenedores), 8)} de "
                    f"{len(contenedores)} elementos con directiva onInView "
                    f"para {url_limpia}"
                )
                if len(contenedores) == 0:
                    # Senal importante: si no hay ningun elemento con la
                    # directiva, el problema NO es el viewport (causa A);
                    # es algo mas profundo (IntersectionObserver en headless
                    # o mecanismo distinto). Loguear para diagnostico.
                    logger.warning(
                        f"DIAGNOSTICO: 0 elementos con [prm-digest-when-in-view] "
                        f"en {url_limpia}. Si el ratio sigue bajo, considerar "
                        "Opcion 2 (leer del modelo Angular)."
                    )
            except Exception as e:
                logger.debug(f"Error en scroll a elementos onInView: {e}")

            # ── PASO 5: micro-wait para digest de Angular ──
            # scroll_into_view_if_needed() dispara onInView() de Angular,
            # pero el ciclo de digest + renderizado toma algunos ms.
            # 300ms es suficiente para el digest sin ser costoso
            # (300ms × 15 docs = 4.5s extra total).
            pagina.wait_for_timeout(300)

            # ── PASO 6: wait_for_selector de enlace real, timeout 6s ──
            # Ahora apuntamos a los ENLACES de descarga (no contenedores):
            # son los hijos que Angular renderiza despues del scroll.
            try:
                pagina.wait_for_selector(
                    'a[href*="/view/delivery/"], '
                    'a[href*="/media/"], '
                    'a[href$=".pdf"]',
                    timeout=6000,
                    state="attached"
                )
                time.sleep(0.3)
            except Exception:
                # Enlace real no aparecio en 6s tras scroll: posible cero
                # legitimo o Angular aun no termino. Seguimos extrayendo
                # por si hay algo.
                logger.debug(
                    f"Enlace real no aparecio en 6s tras scroll: {url_limpia}"
                )

            # ── PASO 7: extraccion post-scroll ──
            urls_pdf = self._extraer_urls_pdf_del_dom(pagina)

            if urls_pdf:
                self.diag_con_pdf_primer_intento += 1
                self.diag_rescatados_por_scroll += 1
                logger.info(
                    f"DIAGNOSTICO: URLs rescatadas por scroll para "
                    f"{url_registro}: {len(urls_pdf)}"
                )
                return urls_pdf[:5]

            # ── PASO 8: reintento AJAX (segunda linea de defensa) ──
            # Si el scroll no fue suficiente, esperar 4s mas por si hay
            # un AJAX tardio adicional. Este camino se mantiene como
            # red de seguridad; si queda en 0 tras varios runs se
            # elimina por limpieza.
            logger.debug(
                f"Post-scroll vacio para {url_limpia}, intentando "
                "reintento AJAX (4s)"
            )
            try:
                pagina.wait_for_selector(
                    'a[href*="/view/delivery/"], '
                    'a[href*="/media/"], '
                    'a[href$=".pdf"]',
                    timeout=4000,
                    state="attached"
                )
                time.sleep(0.3)
            except Exception:
                pass

            urls_pdf = self._extraer_urls_pdf_del_dom(pagina)

            if urls_pdf:
                self.diag_con_pdf_segundo_intento += 1
                logger.info(
                    f"DIAGNOSTICO: enlaces capturados en reintento AJAX "
                    f"post-scroll: {url_limpia}"
                )
                return urls_pdf[:5]

            # ── PASO 9: definitivamente vacio ──
            self.diag_sin_pdf_tras_reintento += 1
            logger.info(
                f"DIAGNOSTICO: sin enlaces extraibles despues de scroll "
                f"y reintento: {url_limpia}"
            )
            self._dump_html_zona_servicios(pagina, url_limpia)
            return []

        except Exception as e:
            self.diag_error_navegacion += 1
            logger.warning(
                f"Error al obtener URL de PDF desde {url_registro}: {e}"
            )
            return []

    def _extraer_urls_pdf_del_dom(self, pagina) -> List[str]:
        """
        Extrae URLs de PDFs del HTML actual de la pagina (sin navegar).

        Funcion pura sobre el DOM: se llama desde _obtener_url_pdf en el
        primer pase y opcionalmente en el segundo pase tras esperar enlaces.
        """
        import html as html_module
        urls_pdf: List[str] = []

        try:
            html_contenido = pagina.content()
            html_decodificado = html_module.unescape(html_contenido)

            # Patron 1: ilo.org/media/XXXXX/download
            urls_pdf.extend(re.findall(
                r'https?://[^"\'<>\s]*ilo\.org/media/\d+/download',
                html_decodificado
            ))

            # Patron 2: URLs directas a PDF
            urls_pdf.extend(re.findall(
                r'https?://[^"\'<>\s]+\.pdf(?:\?[^"\'<>\s]*)?',
                html_decodificado
            ))

            # Patron 3: URLs de delivery de Primo (redirigen a S3)
            urls_pdf.extend(re.findall(
                r'https?://[^"\'<>\s]*labordoc[^"\'<>\s]*/delivery/[^"\'<>\s]+',
                html_decodificado
            ))

            # Fallback: buscar con Playwright enlaces visibles
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

            # Eliminar duplicados manteniendo orden
            urls_unicas = list(dict.fromkeys(urls_pdf))

            # Aplicar lista negra centralizada
            urls_filtradas = [
                u for u in urls_unicas
                if not any(patron in u.lower() for patron in PATRONES_URL_IGNORADAS)
            ]

            # Priorizar URLs de delivery de Primo y URLs de ilo.org/media
            urls_prioritarias = [
                u for u in urls_filtradas
                if '/view/delivery/' in u or '/media/' in u
            ]
            urls_resto = [u for u in urls_filtradas if u not in urls_prioritarias]
            return urls_prioritarias + urls_resto

        except Exception as e:
            logger.debug(f"Error extrayendo URLs del DOM: {e}")
            return []

    def _tiene_mensaje_no_pdf_explicito(self, pagina) -> bool:
        """
        Detecta si la pagina muestra un mensaje explicito de "no full text"
        en algun lugar visible. Busca tanto en selectores especificos de Primo
        VE como en el texto plano de los contenedores de servicios.

        Si retorna True, el documento es inequivocamente sin PDF y no
        amerita reintento.
        """
        # Frases que Primo VE usa para indicar sin texto completo
        frases_no_pdf = [
            "no full text available",
            "not available online",
            "online access not available",
            "no online access",
            "no full-text available",
            "full text not available",
        ]

        try:
            # Buscar en los contenedores de servicios donde Primo muestra
            # estos mensajes. Usar inner_text() para obtener texto visible.
            contenedores = pagina.query_selector_all(
                "prm-full-view-service-container, "
                "prm-service-container, "
                ".full-view-inner-container, "
                "prm-no-records, "
                ".no-records-message"
            )
            for cont in contenedores:
                try:
                    texto = (cont.inner_text() or "").strip().lower()
                    if not texto:
                        continue
                    if any(frase in texto for frase in frases_no_pdf):
                        return True
                except Exception:
                    continue
        except Exception:
            pass

        return False

    def _dump_html_zona_servicios(self, pagina, url_limpia: str):
        """
        Dumpea al log (nivel DEBUG, no INFO) los primeros 500 caracteres del
        HTML de los contenedores de servicios de Primo VE. Activado solo
        cuando un documento queda definitivamente sin PDF tras reintento,
        para permitir inspeccion manual del HTML problematico.

        Va en DEBUG porque infla el log; el grep de DIAGNOSTICO sigue siendo
        liviano en corridas normales.
        """
        try:
            contenedores = pagina.query_selector_all(
                "prm-full-view-service-container, "
                "prm-service-container, "
                ".full-view-inner-container"
            )
            if not contenedores:
                logger.debug(
                    f"DUMP HTML para {url_limpia}: ningun contenedor "
                    "de servicios encontrado en el DOM"
                )
                return

            for i, cont in enumerate(contenedores[:2]):  # max 2 contenedores
                try:
                    inner = cont.inner_html() or ""
                    fragmento = inner[:500].replace("\n", " ").strip()
                    logger.debug(
                        f"DUMP HTML para {url_limpia} contenedor #{i}: "
                        f"{fragmento}"
                    )
                except Exception as e:
                    logger.debug(
                        f"DUMP HTML para {url_limpia} contenedor #{i}: "
                        f"error leyendo inner_html: {e}"
                    )
        except Exception as e:
            logger.debug(f"DUMP HTML fallo para {url_limpia}: {e}")

    def download(self, documento: DocumentoResultado, carpeta_destino: str,
                 intentos_max: int = 3) -> Optional[str]:
        """Descarga el PDF al disco con reintentos. El timeout es
        (10, 120): 10 segundos para conectar y 120 entre bytes
        recibidos, lo que corta intentos pegados sin renunciar a
        archivos grandes que tardan en terminar de bajar. Las URLs de
        Labordoc redirigen a S3 con firma corta; requests sigue las
        redirecciones automaticamente."""
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
                        timeout=(10, 120),  # (connect, read)
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

                    # Streaming con progreso visible
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
        """Genera un nombre de archivo seguro a partir del titulo."""
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
