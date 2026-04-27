# -*- coding: utf-8 -*-
"""
Punto de entrada del programa.

Presenta un menu interactivo en consola que guia al usuario para buscar
y descargar documentos de bibliotecas digitales (UN Digital Library e
ILO Labordoc), extraer texto de los PDFs descargados, generar metadatos
en CSV y JSON, y mantener un historial acumulado entre sesiones.

Uso:
    python main.py
"""

import os
import sys
import csv
import time
import logging
import importlib
from datetime import datetime
from typing import List, Optional

from base_scraper import BaseScraper, DocumentoResultado, FiltrosBusqueda

# ===========================================================================
# CONFIGURACION DE LOGGING
# ===========================================================================

# El log se imprime en bloques con cabecera para que sea facil distinguir
# eventos en errores.log al inspeccionarlo manualmente.
FORMATO_LOG = (
    "--- %(asctime)s ---\n"
    "Nivel: %(levelname)s\n"
    "Modulo: %(name)s\n"
    "Mensaje: %(message)s\n"
)

FORMATO_CONSOLA = "%(message)s"


def configurar_logging():
    """Configura dos destinos para el log: archivo errores.log (todo,
    nivel DEBUG) y consola (solo WARNING y superior, formato simple)."""
    logger_raiz = logging.getLogger()
    logger_raiz.setLevel(logging.DEBUG)

    # Limpiar handlers previos por si la funcion se invocara mas de una vez
    logger_raiz.handlers.clear()

    handler_archivo = logging.FileHandler("errores.log", mode="w", encoding="utf-8")
    handler_archivo.setLevel(logging.DEBUG)
    handler_archivo.setFormatter(logging.Formatter(FORMATO_LOG))
    logger_raiz.addHandler(handler_archivo)

    handler_consola = logging.StreamHandler(sys.stdout)
    handler_consola.setLevel(logging.WARNING)
    handler_consola.setFormatter(logging.Formatter(FORMATO_CONSOLA))
    logger_raiz.addHandler(handler_consola)


logger = logging.getLogger(__name__)


# ===========================================================================
# REGISTRO DE SCRAPERS DISPONIBLES
# ===========================================================================
# Para agregar una nueva fuente, basta crear una subclase de BaseScraper y
# registrarla aqui. El menu la lista automaticamente. Las importaciones se
# protegen con try/except para que un scraper roto no impida usar el resto.

def obtener_scrapers_disponibles() -> List[dict]:
    """Retorna la lista de scrapers cargados correctamente. Cada entrada
    es un dict con las claves nombre, descripcion y clase."""
    scrapers = []

    try:
        from scraper_un import UNDigitalLibraryScraper
        scrapers.append({
            "nombre": "UN Digital Library",
            "descripcion": "Biblioteca Digital de las Naciones Unidas (documentos, resoluciones, reportes)",
            "clase": UNDigitalLibraryScraper,
        })
    except ImportError as e:
        logger.warning(f"No se pudo cargar scraper UN: {e}")

    try:
        from scraper_ilo import ILOLabordocScraper
        scrapers.append({
            "nombre": "ILO Labordoc",
            "descripcion": "Repositorio de la OIT (publicaciones sobre trabajo, empleo, derechos laborales)",
            "clase": ILOLabordocScraper,
        })
    except ImportError as e:
        logger.warning(f"No se pudo cargar scraper ILO: {e}")

    return scrapers


# ===========================================================================
# CONFIGURACION DESDE ARCHIVO JSON
# ===========================================================================
# configuracion.json deja editar parametros (rangos de fechas, idiomas
# validos, carpeta por defecto, etc.) sin tocar codigo. Si el archivo no
# existe se crea con valores por defecto; si esta corrupto el programa
# avisa al usuario y continua con los valores por defecto en memoria.

import json

RUTA_CONFIGURACION = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                   "configuracion.json")

CONFIGURACION_POR_DEFECTO = {
    "fecha_minima_permitida": 1945,
    "fecha_maxima_permitida": 2026,
    "carpeta_descarga_por_defecto": "./documentos_descargados",
    "limite_documentos_por_defecto": 50,
    "ultima_carpeta_usada": None,
    "ilo_search_scope": "ALL_ILO",
    "ilo_tab": "ALL_ILO",
    "idiomas_validos": {
        "en": "Ingles",
        "es": "Espanol",
        "fr": "Frances",
        "ar": "Arabe",
        "zh": "Chino",
        "ru": "Ruso",
    },
}


def cargar_configuracion() -> dict:
    """Lee configuracion.json y devuelve sus valores fusionados con los
    defaults. Si el archivo no existe se crea con los defaults; si esta
    corrupto se avisa al usuario y se siguen usando los defaults en
    memoria sin abortar el programa."""
    config = dict(CONFIGURACION_POR_DEFECTO)

    if not os.path.exists(RUTA_CONFIGURACION):
        try:
            with open(RUTA_CONFIGURACION, "w", encoding="utf-8") as f:
                json.dump(CONFIGURACION_POR_DEFECTO, f,
                         indent=4, ensure_ascii=False)
            print(f"  Archivo de configuracion creado: {RUTA_CONFIGURACION}")
        except Exception as e:
            print(f"  No se pudo crear configuracion.json: {e}")
        return config

    try:
        with open(RUTA_CONFIGURACION, "r", encoding="utf-8") as f:
            datos = json.load(f)
        if not isinstance(datos, dict):
            raise ValueError("El contenido no es un objeto JSON valido")
        # Los valores del archivo prevalecen, pero las claves ausentes se
        # rellenan con los defaults para que no falte ningun campo.
        config.update(datos)
    except json.JSONDecodeError as e:
        print()
        print("  " + "=" * 56)
        print("  ERROR AL LEER configuracion.json")
        print("  " + "=" * 56)
        print(f"  El archivo tiene un error de formato en la linea {e.lineno}:")
        print(f"    {e.msg}")
        print()
        print("  Para arreglarlo:")
        print(f"    1. Abre el archivo con el Bloc de notas:")
        print(f"       {RUTA_CONFIGURACION}")
        print("    2. Revisa que todas las comas, comillas y llaves esten bien.")
        print("    3. Si no puedes arreglarlo, borra el archivo y el programa")
        print("       lo creara de nuevo con los valores por defecto.")
        print()
        print("  Por ahora se usaran los valores por defecto.")
        print("  " + "=" * 56)
        print()
    except Exception as e:
        print(f"  Error leyendo configuracion.json: {e}")
        print("  Se usaran los valores por defecto.")

    return config


def guardar_configuracion(config: dict):
    """Persiste el dict de configuracion al archivo JSON."""
    try:
        with open(RUTA_CONFIGURACION, "w", encoding="utf-8") as f:
            json.dump(config, f, indent=4, ensure_ascii=False)
    except Exception as e:
        logger.warning(f"No se pudo guardar configuracion.json: {e}")


CONFIG = cargar_configuracion()

CARPETA_DESCARGA = CONFIG.get("carpeta_descarga_por_defecto",
                               "./documentos_descargados")


# ===========================================================================
# HISTORIAL DE DESCARGAS (DETECCION DE DUPLICADOS ENTRE SESIONES)
# ===========================================================================
# historial_descargas.json acumula informacion de cada documento que el
# programa intento descargar, sea con exito o fallo. Sirve para dos fines:
# saltar archivos ya descargados en sesiones previas, y no reintentar
# descargas que ya fallaron antes (a menudo por documentos sin PDF
# disponible). La identidad de un documento es su recid prefijado con la
# fuente, p.ej. 'ILO:alma995...' o 'UN:4012345', para evitar colisiones
# entre numeraciones de scrapers distintos.

RUTA_HISTORIAL = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "historial_descargas.json"
)

ESTADO_EXITOSO = "exitoso"
ESTADO_FALLIDO = "fallido"


def cargar_historial() -> dict:
    """Lee historial_descargas.json y devuelve su contenido. Si no existe
    devuelve una estructura vacia; si esta corrupto avisa al usuario y
    devuelve la estructura vacia, de modo que el programa pueda seguir
    operando aunque sin deteccion de duplicados hasta que el archivo se
    arregle o se borre.

    La estructura es {version, actualizado, descargas{id_unico: {...}}};
    cada entrada de descargas contiene fuente, titulo, url_fuente,
    fecha_publicacion, fecha_descarga, ruta_archivo y estado."""
    historial_vacio = {
        "version": 1,
        "actualizado": None,
        "descargas": {},
    }

    if not os.path.exists(RUTA_HISTORIAL):
        return historial_vacio

    try:
        with open(RUTA_HISTORIAL, "r", encoding="utf-8") as f:
            datos = json.load(f)
        if not isinstance(datos, dict):
            raise ValueError("El contenido no es un objeto JSON valido")
        # Tolerancia con archivos de versiones anteriores
        if "descargas" not in datos or not isinstance(datos["descargas"], dict):
            datos["descargas"] = {}
        if "version" not in datos:
            datos["version"] = 1
        return datos
    except json.JSONDecodeError as e:
        print()
        print("  " + "=" * 56)
        print("  ERROR AL LEER historial_descargas.json")
        print("  " + "=" * 56)
        print(f"  El archivo tiene un error de formato en la linea {e.lineno}:")
        print(f"    {e.msg}")
        print()
        print("  El programa funcionara igual, pero sin la deteccion de")
        print("  duplicados hasta que arregles el archivo o lo borres.")
        print("  " + "=" * 56)
        print()
        return historial_vacio
    except Exception as e:
        logger.warning(f"Error leyendo historial_descargas.json: {e}")
        return historial_vacio


def guardar_historial(historial: dict):
    """Persiste el historial al disco con timestamp actualizado."""
    try:
        historial["actualizado"] = datetime.now().replace(microsecond=0).isoformat()
        with open(RUTA_HISTORIAL, "w", encoding="utf-8") as f:
            json.dump(historial, f, indent=2, ensure_ascii=False)
        logger.debug(f"Historial guardado: {RUTA_HISTORIAL} "
                     f"({len(historial.get('descargas', {}))} registros)")
    except Exception as e:
        logger.warning(f"No se pudo guardar historial_descargas.json: {e}")


def ids_excluir_desde_historial(historial: dict) -> set:
    """Conjunto de identificadores a saltar en la proxima busqueda.

    Incluye registros exitosos y fallidos: una descarga que fallo en una
    sesion previa probablemente seguira fallando (p. ej., porque el
    documento no tiene PDF), por lo que no se reintenta."""
    descargas = historial.get("descargas", {})
    if not isinstance(descargas, dict):
        return set()
    return set(descargas.keys())


def registrar_en_historial(historial: dict, id_unico: str, registro: dict):
    """Agrega una entrada al historial en memoria.

    El volcado a disco lo realiza guardar_historial() al cerrar la
    sesion; mantenerlo en memoria evita el coste de I/O por cada
    descarga individual."""
    if "descargas" not in historial:
        historial["descargas"] = {}
    historial["descargas"][id_unico] = registro


def construir_id_unico(fuente: str, recid: str) -> Optional[str]:
    """Devuelve el identificador prefijado por fuente, o None si el recid
    es vacio. El prefijo se infiere del nombre legible de la fuente: 'ILO'
    para Labordoc, 'UN' para UN Digital Library, y las tres primeras
    letras en mayusculas para fuentes futuras no contempladas aqui."""
    if not recid:
        return None
    prefijo_upper = fuente.upper()
    if "ILO" in prefijo_upper or "LABORDOC" in prefijo_upper:
        prefijo = "ILO"
    elif "UN " in prefijo_upper or "NACIONES" in prefijo_upper or prefijo_upper.startswith("UN"):
        prefijo = "UN"
    else:
        prefijo = prefijo_upper[:3].replace(" ", "")
    return f"{prefijo}:{recid}"


# ===========================================================================
# FUNCIONES DEL MENU INTERACTIVO
# ===========================================================================

def limpiar_pantalla():
    """Limpia la pantalla de la consola."""
    os.system("cls" if os.name == "nt" else "clear")


def mostrar_encabezado():
    """Muestra el titulo del programa."""
    print("=" * 65)
    print("  DESCARGADOR DE DOCUMENTOS - BIBLIOTECAS DIGITALES")
    print("  UN Digital Library | ILO Labordoc")
    print("=" * 65)
    print()


def mostrar_menu_principal():
    """Muestra el menu principal y retorna la opcion seleccionada."""
    print("  Opciones disponibles:")
    print()
    print("  [1] Buscar y descargar documentos")
    print("  [2] Diagnostico de dependencias")
    print("  [3] Salir")
    print()

    while True:
        opcion = input("  Selecciona una opcion (1-3): ").strip()
        if opcion in ("1", "2", "3"):
            return opcion
        print("  Opcion no valida. Escribe 1, 2 o 3.")


def seleccionar_fuente(scrapers: List[dict]) -> Optional[BaseScraper]:
    """Pide al usuario que elija una fuente del listado y devuelve una
    instancia del scraper correspondiente. Devuelve None si el usuario
    decide volver al menu principal."""
    print()
    print("-" * 50)
    print("  PASO 1: Selecciona la fuente de datos")
    print("-" * 50)
    print()

    for i, scraper in enumerate(scrapers, 1):
        print(f"  [{i}] {scraper['nombre']}")
        print(f"      {scraper['descripcion']}")
        print()

    print(f"  [0] Volver al menu principal")
    print()

    while True:
        opcion = input(f"  Selecciona una opcion (0-{len(scrapers)}): ").strip()
        if opcion == "0":
            return None
        try:
            indice = int(opcion) - 1
            if 0 <= indice < len(scrapers):
                print(f"\n  Fuente seleccionada: {scrapers[indice]['nombre']}")
                return scrapers[indice]["clase"]()
        except ValueError:
            pass
        print(f"  Opcion no valida. Escribe un numero entre 0 y {len(scrapers)}.")


def configurar_filtros() -> Optional[FiltrosBusqueda]:
    """Guia al usuario por el formulario de filtros y devuelve el objeto
    FiltrosBusqueda construido. Cualquier filtro distinto de la palabra
    clave puede dejarse en blanco para omitirlo."""
    filtros = FiltrosBusqueda()

    print()
    print("-" * 50)
    print("  PASO 2: Configura los filtros de busqueda")
    print("-" * 50)
    print("  (Deja en blanco para omitir un filtro)")
    print()

    # --- Palabras clave (obligatorio) ---
    while True:
        entrada = input("  Palabra clave (Max 1 palabra): ").strip()
        if entrada:
            filtros.palabras_clave = [p.strip() for p in entrada.split(",") if p.strip()]
            break
        print("  La palabra clave es obligatoria. Escribe un termino para buscar.")

    # --- Rango de fechas ---
    fecha_min = CONFIG.get("fecha_minima_permitida", 1945)
    fecha_max = CONFIG.get("fecha_maxima_permitida", 2026)
    print()
    fecha_desde = input(f"  Fecha desde ({fecha_min}-{fecha_max}): ").strip()
    if fecha_desde:
        try:
            valor = int(fecha_desde)
            if valor < fecha_min or valor > fecha_max:
                print(f"  Valor fuera del rango permitido ({fecha_min}-{fecha_max}). "
                      "Se omite el filtro.")
            else:
                filtros.fecha_desde = valor
        except ValueError:
            print("  Valor no valido. Se omite el filtro de fecha inicial.")

    fecha_hasta = input(f"  Fecha hasta ({fecha_min}-{fecha_max}): ").strip()
    if fecha_hasta:
        try:
            valor = int(fecha_hasta)
            if valor < fecha_min or valor > fecha_max:
                print(f"  Valor fuera del rango permitido ({fecha_min}-{fecha_max}). "
                      "Se omite el filtro.")
            else:
                filtros.fecha_hasta = valor
        except ValueError:
            print("  Valor no valido. Se omite el filtro de fecha final.")

    # --- Idioma ---
    idiomas_cfg = CONFIG.get("idiomas_validos", {})
    print()
    print("  Idiomas disponibles (Max 2 idiomas, separados por coma):")
    pares = [f"    {cod} = {nombre}" for cod, nombre in idiomas_cfg.items()]
    for par in pares:
        print(par)
    idioma_input = input("  Codigo(s) de idioma (ej: es, en): ").strip().lower()
    if idioma_input:
        idiomas = [c.strip() for c in idioma_input.split(",") if c.strip()]
        idiomas_ok = []
        idiomas_rechazados = []
        for c in idiomas:
            if c in idiomas_cfg:
                idiomas_ok.append(c)
            else:
                idiomas_rechazados.append(c)
        if idiomas_rechazados:
            print(f"  Codigo(s) no reconocido(s): {', '.join(idiomas_rechazados)}. "
                  "Se ignoran.")
        if idiomas_ok:
            filtros.idioma = idiomas_ok
        else:
            print("  Ningun codigo reconocido. Se buscara en todos los idiomas.")

    # --- Tipo de documento ---
    print()
    print("  Tipos de documento:")
    print("    reporte | resolucion | acuerdo | decision | carta")
    tipo = input("  Tipo de documento (o deja en blanco para cualquiera): ").strip().lower()
    if tipo:
        filtros.tipo_documento = tipo

    # --- Limite de documentos ---
    limite_default = CONFIG.get("limite_documentos_por_defecto", 50)
    print()
    limite_str = input(f"  Numero de documentos a descargar (Max 100, default: {limite_default}): ").strip()
    if limite_str:
        try:
            limite = int(limite_str)
            if limite <= 0:
                print(f"  Valor no valido. Se usara el limite por defecto ({limite_default}).")
                limite = limite_default

            if limite > 100:
                print()
                print(f"  ATENCION: el limite maximo es 100 documentos.")
                print(f"  Se usara el limite maximo (100) en vez del valor ingresado ({limite}).")
                limite = 100

            filtros.limite = limite
        except ValueError:
            print(f"  Valor no valido. Se usara el limite por defecto ({limite_default}).")
            filtros.limite = limite_default
    else:
        filtros.limite = limite_default

    # --- Carpeta de destino (selector grafico con tkinter) ---
    carpeta_default = CONFIG.get("ultima_carpeta_usada") or CARPETA_DESCARGA
    print()
    print(f"  Carpeta de descarga actual: {carpeta_default}")
    cambiar = input("  Deseas cambiar la carpeta de descarga? (s/n): ").strip().lower()

    if cambiar == "s":
        carpeta = _seleccionar_carpeta_grafica(carpeta_default)
        if carpeta:
            # Guardar la ultima carpeta elegida en configuracion.json
            CONFIG["ultima_carpeta_usada"] = carpeta
            guardar_configuracion(CONFIG)
        else:
            carpeta = carpeta_default
    else:
        carpeta = carpeta_default

    return filtros, carpeta


def _seleccionar_carpeta_grafica(carpeta_inicial: str) -> Optional[str]:
    """Abre el dialogo nativo de seleccion de carpeta (tkinter). Si el
    entorno no tiene interfaz grafica o el dialogo falla, recurre a
    pedir la ruta por teclado. Devuelve la ruta elegida, o None si el
    usuario cancela."""
    try:
        import tkinter as tk
        from tkinter import filedialog

        # Ventana raiz oculta para que solo aparezca el dialogo
        root = tk.Tk()
        root.withdraw()

        directorio_inicio = carpeta_inicial if os.path.isdir(carpeta_inicial) else "."

        carpeta = filedialog.askdirectory(
            title="Selecciona la carpeta de descarga",
            initialdir=directorio_inicio,
        )

        root.destroy()

        if carpeta:
            print(f"  Carpeta seleccionada: {carpeta}")
            return carpeta
        else:
            print("  Seleccion cancelada. Se usara la carpeta por defecto.")
            return None

    except ImportError:
        print("  El selector grafico no esta disponible en este sistema.")
        print("  Escribe la ruta de la carpeta manualmente.")
    except Exception as e:
        print(f"  No se pudo abrir el selector grafico: {e}")
        print("  Escribe la ruta de la carpeta manualmente.")

    ruta = input(f"  Carpeta de descarga (Enter para '{carpeta_inicial}'): ").strip()
    return ruta if ruta else None


def confirmar_busqueda(filtros: FiltrosBusqueda, nombre_fuente: str) -> bool:
    """Muestra el resumen de los filtros y pide confirmacion al usuario."""
    print()
    print("-" * 50)
    print("  RESUMEN DE LA BUSQUEDA")
    print("-" * 50)
    print(f"  Fuente:            {nombre_fuente}")
    print(f"  Palabras clave:    {', '.join(filtros.palabras_clave)}")
    print(f"  Fecha desde:       {filtros.fecha_desde or 'Sin limite'}")
    print(f"  Fecha hasta:       {filtros.fecha_hasta or 'Sin limite'}")
    print(f"  Idioma:            {', '.join(filtros.idioma) if filtros.idioma else 'Todos'}")
    print(f"  Tipo de documento: {filtros.tipo_documento or 'Cualquiera'}")
    print(f"  Limite:            {filtros.limite} documentos")
    print("-" * 50)
    print()

    confirmacion = input("  Iniciar busqueda? (s/n): ").strip().lower()
    return confirmacion == "s"


def ejecutar_busqueda_y_descarga(scraper: BaseScraper, filtros: FiltrosBusqueda,
                                  carpeta_destino: str):
    """Orquesta la busqueda, descarga, extraccion de texto y volcado de
    metadatos. Carga el historial al inicio para excluir duplicados,
    extiende el historial con los resultados de la sesion y lo persiste
    al final."""
    os.makedirs(carpeta_destino, exist_ok=True)

    # --- FASE 1: Busqueda ---
    print()
    print("  Buscando documentos...")
    print("  (Esto puede tomar unos segundos dependiendo de la fuente)")
    print()

    historial = cargar_historial()
    ids_excluir = ids_excluir_desde_historial(historial)
    if ids_excluir:
        logger.info(f"Historial cargado: {len(ids_excluir)} documentos ya "
                    f"registrados, se excluiran de esta busqueda.")
        print(f"  (Historial: {len(ids_excluir)} documentos previos se "
              "saltaran automaticamente)")
        print()

    inicio_busqueda = time.time()
    resultados = scraper.search(filtros, ids_excluir=ids_excluir)
    tiempo_busqueda = time.time() - inicio_busqueda

    if not resultados:
        print("  No se encontraron documentos nuevos con los filtros especificados.")
        if ids_excluir:
            print("  (Todos los documentos que coinciden ya estan en el historial,")
            print("   o no hay mas resultados disponibles en la fuente.)")
        print("  Sugerencias:")
        print("    - Verifica que las palabras clave sean correctas")
        print("    - Ampliar el rango de fechas")
        print("    - Probar sin filtro de idioma o tipo de documento")
        return

    print(f"  Se encontraron {len(resultados)} documentos en {tiempo_busqueda:.1f} segundos.")
    print()

    # --- FASE 2: Descarga ---
    print("  Iniciando descarga de PDFs...")
    print()

    exitosos = 0
    fallidos = 0
    archivos_descargados = []
    inicio_descarga = time.time()

    nombre_fuente = scraper.nombre_fuente()

    for i, doc in enumerate(resultados, 1):
        print(f"  Descargando documento {i} de {len(resultados)}: {doc.titulo[:60]}...")

        ruta = scraper.download(doc, carpeta_destino)

        # Sello temporal en cuanto termina la descarga, exitosa o no
        fecha_descarga_iso = datetime.now().replace(microsecond=0).isoformat()

        if ruta:
            exitosos += 1
            archivo_local = os.path.basename(ruta)
            ruta_absoluta = os.path.abspath(ruta)
            texto_extraido = extraer_texto_pdf(ruta)
            estado_descarga = ESTADO_EXITOSO
        else:
            fallidos += 1
            archivo_local = "DESCARGA_FALLIDA"
            ruta_absoluta = ""
            texto_extraido = TEXTO_NO_DESCARGADO
            estado_descarga = ESTADO_FALLIDO

        # Un solo punto de construccion para que CSV, JSON y archivo
        # consolidado reciban exactamente los mismos campos.
        archivos_descargados.append({
            "titulo": doc.titulo,
            "autor": doc.autor,
            "fecha": doc.fecha,
            "idioma": doc.idioma,
            "tipo_documento": doc.tipo_documento,
            "url_fuente": doc.url_fuente,
            "archivo_local": archivo_local,
            "fecha_descarga": fecha_descarga_iso,
            "texto_extraido": texto_extraido,
        })

        # Si el recid esta vacio el documento no se puede identificar
        # univocamente, asi que no entra al historial; podria volver a
        # aparecer en sesiones futuras pero esto es preferible a usar un
        # ID falso que entrarie en colision con otros.
        id_unico = construir_id_unico(nombre_fuente, doc.recid)
        if id_unico:
            registrar_en_historial(historial, id_unico, {
                "fuente": nombre_fuente,
                "titulo": doc.titulo,
                "url_fuente": doc.url_fuente,
                "fecha_publicacion": doc.fecha,
                "fecha_descarga": fecha_descarga_iso,
                "ruta_archivo": ruta_absoluta,
                "estado": estado_descarga,
            })

        # Pausa entre descargas para ser respetuoso con el servidor
        if i < len(resultados):
            time.sleep(1)

    tiempo_descarga = time.time() - inicio_descarga

    # --- FASE 3: Generar archivos de salida (CSV + JSON + textos consolidados) ---
    ruta_csv = os.path.join(carpeta_destino, "metadata.csv")
    ruta_json = os.path.join(carpeta_destino, "metadata.json")
    ruta_textos = os.path.join(carpeta_destino, "textos_extraidos.txt")
    generar_csv_metadatos(archivos_descargados, ruta_csv)
    generar_json_metadatos(archivos_descargados, ruta_json)
    generar_archivo_textos_consolidado(archivos_descargados, ruta_textos)

    # Persistir al final en lugar de tras cada descarga: ahorra I/O en
    # el bucle. En caso de crash se pierde solo el historial de la
    # sesion actual; el de sesiones previas no se ve afectado.
    guardar_historial(historial)

    # --- FASE 4: Resumen final ---
    print()
    print("=" * 50)
    print("  RESUMEN DE LA SESION")
    print("=" * 50)
    print(f"  Documentos encontrados:          {len(resultados)}")
    print(f"  Descargados correctamente:       {exitosos}")
    print(f"  Descargas fallidas:              {fallidos}")
    print(f"  Tiempo de busqueda:              {tiempo_busqueda:.1f}s")
    print(f"  Tiempo de descarga:              {tiempo_descarga:.1f}s")
    print(f"  Archivos guardados en:           {os.path.abspath(carpeta_destino)}")
    print(f"  Metadatos CSV:                   {os.path.abspath(ruta_csv)}")
    print(f"  Metadatos JSON:                  {os.path.abspath(ruta_json)}")
    print(f"  Textos extraidos:                {os.path.abspath(ruta_textos)}")
    print(f"  Historial actualizado:           {os.path.abspath(RUTA_HISTORIAL)}")
    total_historial = len(historial.get("descargas", {}))
    print(f"  Total en historial acumulado:    {total_historial} documentos")
    print("=" * 50)

    if fallidos > 0:
        print()
        print(f"  Se encontraron {fallidos} errores durante la descarga.")
        print("  Para obtener ayuda, comparte el archivo errores.log")
        print(f"  Ubicacion: {os.path.abspath('errores.log')}")


def generar_csv_metadatos(datos: List[dict], ruta_csv: str):
    """Vuelca los metadatos a un archivo CSV abrible directamente en Excel.

    Usa pandas como camino principal y csv.DictWriter como respaldo si
    la dependencia no esta instalada. El campo texto_extraido se trunca
    a MAX_CHARS_TEXTO_EN_CSV caracteres para no convertir las celdas en
    bloques ilegibles; el texto integro queda en metadata.json y en
    textos_extraidos.txt."""
    if not datos:
        return

    campos = ["titulo", "autor", "fecha", "idioma", "tipo_documento",
              "url_fuente", "archivo_local", "fecha_descarga", "texto_extraido"]

    # Trabajamos sobre copia para no mutar la lista original que tambien
    # alimenta el JSON y el archivo consolidado, donde el texto va completo.
    datos_para_csv = []
    for registro in datos:
        copia = dict(registro)
        copia["texto_extraido"] = truncar_texto_para_csv(
            registro.get("texto_extraido", "")
        )
        datos_para_csv.append(copia)

    try:
        # encoding utf-8-sig agrega BOM para que Excel no rompa los acentos
        import pandas as pd
        df = pd.DataFrame(datos_para_csv, columns=campos)
        df.to_csv(ruta_csv, index=False, encoding="utf-8-sig")
        logger.info(f"CSV de metadatos generado con pandas: {ruta_csv}")
    except ImportError:
        logger.warning("pandas no disponible, usando csv.DictWriter como fallback")
        try:
            with open(ruta_csv, "w", newline="", encoding="utf-8-sig") as f:
                escritor = csv.DictWriter(f, fieldnames=campos)
                escritor.writeheader()
                for fila in datos_para_csv:
                    escritor.writerow(fila)
            logger.info(f"CSV de metadatos generado (fallback): {ruta_csv}")
        except Exception as e:
            logger.error(f"Error al generar CSV: {e}", exc_info=True)
            print(f"  Error al generar el archivo de metadatos CSV: {e}")
    except Exception as e:
        logger.error(f"Error al generar CSV con pandas: {e}", exc_info=True)
        print(f"  Error al generar el archivo de metadatos CSV: {e}")


def generar_json_metadatos(datos: List[dict], ruta_json: str):
    """Vuelca los metadatos a JSON conservando el texto integro de cada
    documento. Es la fuente canonica del contenido textual cuando se
    quiere procesar la salida con otros programas."""
    if not datos:
        return

    try:
        with open(ruta_json, "w", encoding="utf-8") as f:
            json.dump(datos, f, indent=2, ensure_ascii=False)
        logger.info(f"JSON de metadatos generado: {ruta_json}")
    except Exception as e:
        logger.error(f"Error al generar JSON de metadatos: {e}", exc_info=True)
        print(f"  Error al generar el archivo de metadatos JSON: {e}")


# ===========================================================================
# EXTRACCION DE TEXTO DE PDFs
# ===========================================================================
# El texto se obtiene con pypdf. Si el PDF no tiene capa de texto (caso
# tipico de documentos escaneados como imagenes), se marca como
# 'OCR REQUERIDO' en lugar de intentar reconocimiento optico, que
# requeriria dependencias externas (Tesseract).

MAX_CHARS_TEXTO_EN_CSV = 500

TEXTO_OCR_REQUERIDO = "[PDF SIN CAPA DE TEXTO - OCR REQUERIDO]"
TEXTO_VACIO = "[PDF VACIO O SIN CONTENIDO TEXTUAL]"
TEXTO_ERROR_LECTURA = "[ERROR AL LEER EL PDF]"
TEXTO_NO_DESCARGADO = "[ARCHIVO NO DESCARGADO]"


def extraer_texto_pdf(ruta_pdf: str) -> str:
    """Devuelve el texto contenido en un PDF, o un marcador entre
    corchetes si la extraccion no es posible (PDF escaneado, corrupto,
    archivo inexistente). pypdf se importa de forma perezosa para que la
    ausencia de la dependencia no impida usar el resto del programa."""
    try:
        from pypdf import PdfReader
    except ImportError:
        logger.warning("pypdf no esta instalado. No se extraera texto.")
        return TEXTO_ERROR_LECTURA

    if not os.path.isfile(ruta_pdf):
        return TEXTO_NO_DESCARGADO

    try:
        reader = PdfReader(ruta_pdf)
        # Algunos PDFs vienen marcados como cifrados con clave vacia; el
        # decrypt('') desbloquea ese caso comun sin penalizar al resto.
        if reader.is_encrypted:
            try:
                reader.decrypt("")
            except Exception:
                pass

        partes_texto = []
        for pagina in reader.pages:
            try:
                texto_pagina = pagina.extract_text() or ""
                if texto_pagina.strip():
                    partes_texto.append(texto_pagina)
            except Exception as e:
                # Una pagina ilegible no debe abortar la extraccion del resto
                logger.debug(f"Pagina ilegible en {ruta_pdf}: {e}")
                continue

        texto_total = "\n".join(partes_texto).strip()

        if not texto_total:
            # Sin texto = casi siempre escaneado sin OCR previo
            return TEXTO_OCR_REQUERIDO

        return texto_total

    except Exception as e:
        logger.warning(f"Error extrayendo texto de {ruta_pdf}: {e}")
        return TEXTO_ERROR_LECTURA


def truncar_texto_para_csv(texto: str, limite: int = MAX_CHARS_TEXTO_EN_CSV) -> str:
    """Devuelve el texto cortado al limite de caracteres y con un sufijo
    explicativo cuando hubo truncamiento. Los marcadores entre corchetes
    se devuelven sin tocar (ya son cortos por diseno)."""
    if not texto:
        return ""
    if texto.startswith("[") and texto.endswith("]"):
        return texto
    if len(texto) <= limite:
        return texto
    return texto[:limite].rstrip() + f"... [TRUNCADO - texto completo en metadata.json, {len(texto)} caracteres totales]"


def generar_archivo_textos_consolidado(datos: List[dict], ruta_archivo: str):
    """Escribe en un unico .txt el texto de todos los PDFs descargados.

    Cada bloque comienza con un encabezado que identifica el archivo,
    titulo y fechas, de modo que el resultado se pueda leer en cualquier
    editor de texto plano sin necesidad de procesarlo programaticamente."""
    if not datos:
        return

    try:
        with open(ruta_archivo, "w", encoding="utf-8") as f:
            for i, registro in enumerate(datos, 1):
                separador = "=" * 70
                f.write(separador + "\n")
                f.write(f"DOCUMENTO {i} de {len(datos)}\n")
                f.write(f"ARCHIVO:           {registro.get('archivo_local', '?')}\n")
                f.write(f"TITULO:            {registro.get('titulo', '?')}\n")
                f.write(f"FECHA PUBLICACION: {registro.get('fecha', '?')}\n")
                f.write(f"FECHA DESCARGA:    {registro.get('fecha_descarga', '?')}\n")
                f.write(separador + "\n\n")
                texto = registro.get("texto_extraido", "") or ""
                f.write(texto)
                f.write("\n\n\n")
        logger.info(f"Archivo de textos consolidado generado: {ruta_archivo}")
    except Exception as e:
        logger.error(f"Error al generar archivo de textos: {e}", exc_info=True)
        print(f"  Error al generar el archivo de textos consolidado: {e}")


# ===========================================================================
# DIAGNOSTICO DE DEPENDENCIAS
# ===========================================================================

def diagnostico():
    """Comprueba el entorno de ejecucion y muestra el resultado al
    usuario: dependencias Python, conectividad con las fuentes y estado
    del historial."""
    print()
    print("-" * 50)
    print("  DIAGNOSTICO DE DEPENDENCIAS")
    print("-" * 50)
    print()

    todo_ok = True

    # Python
    version_python = sys.version.split()[0]
    print(f"  [OK] Python {version_python}")

    # requests
    try:
        import requests
        print(f"  [OK] requests {requests.__version__}")
    except ImportError:
        print("  [ERROR] requests no esta instalado")
        print("          Ejecuta: pip install requests")
        todo_ok = False

    # lxml
    try:
        from lxml import etree
        print(f"  [OK] lxml {etree.__version__}")
    except ImportError:
        print("  [ERROR] lxml no esta instalado")
        print("          Ejecuta: pip install lxml")
        todo_ok = False

    # pandas (para CSV de metadata)
    try:
        import pandas as pd
        print(f"  [OK] pandas {pd.__version__}")
    except ImportError:
        print("  [AVISO] pandas no esta instalado")
        print("          Sin pandas el CSV se genera con un fallback de la stdlib.")
        print("          Para mejor manejo de datos: pip install pandas")

    # pypdf (para extraccion de texto de PDFs)
    try:
        import pypdf
        print(f"  [OK] pypdf {pypdf.__version__}")
    except ImportError:
        print("  [AVISO] pypdf no esta instalado")
        print("          Sin pypdf no se podra extraer texto de los PDFs.")
        print("          Ejecuta: pip install pypdf")

    # playwright
    try:
        import playwright
        # Obtener version de forma segura (no todas las versiones exponen __version__)
        try:
            from importlib.metadata import version as pkg_version
            version_pw = pkg_version("playwright")
        except Exception:
            version_pw = "(version desconocida)"
        print(f"  [OK] playwright {version_pw}")

        # Verificar que los navegadores esten instalados
        try:
            from playwright.sync_api import sync_playwright
            with sync_playwright() as pw:
                navegador = pw.chromium.launch(headless=True)
                navegador.close()
            print("  [OK] Navegador Chromium de Playwright disponible")
        except Exception as e:
            print("  [AVISO] Playwright instalado pero falta el navegador")
            print("          Ejecuta: playwright install chromium")
            todo_ok = False

    except ImportError:
        print("  [ERROR] playwright no esta instalado")
        print("          Ejecuta: pip install playwright")
        print("          Luego:   playwright install chromium")
        todo_ok = False

    # Verificar acceso a red (UN Digital Library)
    print()
    print("  Verificando conectividad...")
    try:
        import requests
        r = requests.get("https://digitallibrary.un.org/", timeout=10)
        print(f"  [OK] Conexion a UN Digital Library (HTTP {r.status_code})")
    except Exception:
        print("  [ERROR] No se pudo conectar a UN Digital Library")
        todo_ok = False

    try:
        import requests
        r = requests.get("https://labordoc.ilo.org/", timeout=10)
        print(f"  [OK] Conexion a ILO Labordoc (HTTP {r.status_code})")
    except Exception:
        print("  [ERROR] No se pudo conectar a ILO Labordoc")
        todo_ok = False

    # Informacion del historial acumulado (no bloqueante)
    print()
    print("  Historial de descargas:")
    try:
        hist = cargar_historial()
        descargas = hist.get("descargas", {})
        total = len(descargas)
        if total == 0:
            print("  [INFO] No hay registros en el historial todavia.")
        else:
            exitosos = sum(1 for r in descargas.values()
                           if isinstance(r, dict) and r.get("estado") == ESTADO_EXITOSO)
            fallidos = sum(1 for r in descargas.values()
                           if isinstance(r, dict) and r.get("estado") == ESTADO_FALLIDO)
            print(f"  [OK] {total} documentos en el historial "
                  f"({exitosos} exitosos, {fallidos} fallidos)")
            ultima = hist.get("actualizado")
            if ultima:
                print(f"  [OK] Ultima actualizacion: {ultima}")
    except Exception as e:
        print(f"  [AVISO] No se pudo leer el historial: {e}")

    # Resumen
    print()
    if todo_ok:
        print("  Todo esta correctamente configurado.")
        print("  Puedes usar todas las fuentes de datos.")
    else:
        print("  Se encontraron problemas. Revisa los errores indicados arriba")
        print("  e instala las dependencias faltantes antes de continuar.")

    print()
    input("  Presiona Enter para volver al menu principal...")


# ===========================================================================
# BUCLE PRINCIPAL
# ===========================================================================

def main():
    """Punto de entrada principal del programa."""
    configurar_logging()
    logger.info("Programa iniciado")

    scrapers = obtener_scrapers_disponibles()

    if not scrapers:
        print("  Error: No se pudieron cargar los scrapers.")
        print("  Verifica que los archivos scraper_un.py y scraper_ilo.py existan.")
        return

    while True:
        limpiar_pantalla()
        mostrar_encabezado()
        opcion = mostrar_menu_principal()

        if opcion == "1":
            scraper = seleccionar_fuente(scrapers)
            if scraper is None:
                continue

            resultado = configurar_filtros()
            if resultado is None:
                continue

            filtros, carpeta = resultado

            if confirmar_busqueda(filtros, scraper.nombre_fuente()):
                ejecutar_busqueda_y_descarga(scraper, filtros, carpeta)
                print()
                input("  Presiona Enter para volver al menu principal...")

        elif opcion == "2":
            diagnostico()

        elif opcion == "3":
            print()
            print("  Hasta luego.")
            logger.info("Programa finalizado por el usuario")
            break


if __name__ == "__main__":
    main()


# ===========================================================================
# COMO AGREGAR UNA NUEVA FUENTE
# ===========================================================================
#
# Sigue estos 5 pasos para integrar una nueva biblioteca digital:
#
# PASO 1: Crea un nuevo archivo (ej: scraper_nueva_fuente.py)
#   - Importa BaseScraper, DocumentoResultado y FiltrosBusqueda desde base_scraper.py
#   - Crea una clase que herede de BaseScraper
#
# PASO 2: Implementa los 3 metodos obligatorios:
#   - nombre_fuente() -> str
#       Retorna el nombre legible de la fuente (ej: "Nueva Biblioteca")
#
#   - search(filtros: FiltrosBusqueda) -> List[DocumentoResultado]
#       Realiza la busqueda y retorna una lista de DocumentoResultado.
#       Cada resultado debe tener al menos: titulo, url_fuente, urls_descarga.
#       Usa logging para registrar errores y progreso.
#
#   - download(documento, carpeta_destino, intentos_max=3) -> Optional[str]
#       Descarga el PDF al disco. Retorna la ruta del archivo o None si falla.
#       Implementa reintentos (ver scraper_un.py como ejemplo).
#
# PASO 3: Registra el nuevo scraper en main.py
#   - Ve a la funcion obtener_scrapers_disponibles()
#   - Agrega un bloque try/except similar a los existentes:
#
#       try:
#           from scraper_nueva_fuente import NuevaFuenteScraper
#           scrapers.append({
#               "nombre": "Nueva Biblioteca",
#               "descripcion": "Descripcion breve de la fuente",
#               "clase": NuevaFuenteScraper,
#           })
#       except ImportError as e:
#           logger.warning(f"No se pudo cargar scraper nueva fuente: {e}")
#
# PASO 4: Prueba tu scraper
#   - Ejecuta el diagnostico (opcion 2 del menu) para verificar dependencias
#   - Haz una busqueda simple con 5-10 documentos para validar
#   - Revisa errores.log para detectar problemas
#
# PASO 5: Documenta tu scraper
#   - Agrega docstrings en la clase y metodos explicando la estrategia
#   - Documenta los endpoints/APIs que utiliza
#   - Indica si requiere dependencias adicionales en requirements.txt
#
# Ejemplo minimo de un scraper:
#
#   from base_scraper import BaseScraper, DocumentoResultado, FiltrosBusqueda
#   from typing import List, Optional
#
#   class NuevaFuenteScraper(BaseScraper):
#       def nombre_fuente(self) -> str:
#           return "Nueva Fuente"
#
#       def search(self, filtros: FiltrosBusqueda) -> List[DocumentoResultado]:
#           # Tu logica de busqueda aqui
#           return []
#
#       def download(self, documento: DocumentoResultado, carpeta_destino: str,
#                    intentos_max: int = 3) -> Optional[str]:
#           # Tu logica de descarga aqui
#           return None
