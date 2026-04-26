# -*- coding: utf-8 -*-
import os
import sys
import csv
import time
import logging
import importlib
from datetime import datetime
from typing import List, Optional

from base_scraper import BaseScraper, DocumentoResultado, FiltrosBusqueda

FORMATO_LOG = (
    "--- %(asctime)s ---\n"
    "Nivel: %(levelname)s\n"
    "Modulo: %(name)s\n"
    "Mensaje: %(message)s\n"
)

FORMATO_CONSOLA = "%(message)s"


def configurar_logging():
    logger_raiz = logging.getLogger()
    logger_raiz.setLevel(logging.DEBUG)

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


def obtener_scrapers_disponibles() -> List[dict]:
    
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

CARPETA_DESCARGA = "./documentos_descargados"


def limpiar_pantalla():
    os.system("cls" if os.name == "nt" else "clear")


def mostrar_encabezado():
    print("=" * 65)
    print("  DESCARGADOR DE DOCUMENTOS - BIBLIOTECAS DIGITALES")
    print("  UN Digital Library | ILO Labordoc")
    print("=" * 65)
    print()


def mostrar_menu_principal():
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
    filtros = FiltrosBusqueda()

    print()
    print("-" * 50)
    print("  PASO 2: Configura los filtros de busqueda")
    print("-" * 50)
    print("  (Deja en blanco para omitir un filtro)")
    print()

    while True:
        entrada = input("  Palabras clave (separadas por coma): ").strip()
        if entrada:
            filtros.palabras_clave = [p.strip() for p in entrada.split(",") if p.strip()]
            break
        print("  Las palabras clave son obligatorias. Escribe al menos un termino.")

    print()
    anio_desde = input("  Anio desde (ej: 2015): ").strip()
    if anio_desde:
        try:
            filtros.anio_desde = int(anio_desde)
        except ValueError:
            print("  Valor no valido. Se omite el filtro de anio inicial.")

    anio_hasta = input("  Anio hasta (ej: 2024): ").strip()
    if anio_hasta:
        try:
            filtros.anio_hasta = int(anio_hasta)
        except ValueError:
            print("  Valor no valido. Se omite el filtro de anio final.")

    print()
    print("  Idiomas disponibles (puedes elegir varios separados por coma):")
    print("    en = Ingles | es = Espanol | fr = Frances")
    print("    ar = Arabe  | zh = Chino   | ru = Ruso")
    idioma_input = input("  Codigo(s) de idioma (ej: es, en): ").strip().lower()
    if idioma_input:
        codigos_validos = {"en", "es", "fr", "ar", "zh", "ru"}
        idiomas = [c.strip() for c in idioma_input.split(",") if c.strip()]
        idiomas_ok = [c for c in idiomas if c in codigos_validos]
        if idiomas_ok:
            filtros.idioma = idiomas_ok
        else:
            print("  Ningun codigo reconocido. Se buscara en todos los idiomas.")

    print()
    print("  Tipos de documento:")
    print("    reporte | resolucion | acuerdo | decision | carta")
    tipo = input("  Tipo de documento (o deja en blanco para cualquiera): ").strip().lower()
    if tipo:
        filtros.tipo_documento = tipo

    print()
    limite_str = input(f"  Numero maximo de documentos a descargar (default: 50): ").strip()
    if limite_str:
        try:
            limite = int(limite_str)
            if limite <= 0:
                print("  Valor no valido. Se usara el limite por defecto (50).")
                limite = 50

            if limite > 200:
                print()
                print(f"  ATENCION: Descargar {limite} documentos puede tardar varios minutos")
                print("  y generar un volumen considerable de trafico de red.")
                confirmacion = input("  Deseas continuar con este limite? (s/n): ").strip().lower()
                if confirmacion != "s":
                    print("  Se usara el limite de 200 documentos.")
                    limite = 200

            filtros.limite = limite
        except ValueError:
            print("  Valor no valido. Se usara el limite por defecto (50).")

    print()
    carpeta = input(f"  Carpeta de descarga (default: {CARPETA_DESCARGA}): ").strip()
    if not carpeta:
        carpeta = CARPETA_DESCARGA

    return filtros, carpeta


def confirmar_busqueda(filtros: FiltrosBusqueda, nombre_fuente: str) -> bool:
    print()
    print("-" * 50)
    print("  RESUMEN DE LA BUSQUEDA")
    print("-" * 50)
    print(f"  Fuente:            {nombre_fuente}")
    print(f"  Palabras clave:    {', '.join(filtros.palabras_clave)}")
    print(f"  Anio desde:        {filtros.anio_desde or 'Sin limite'}")
    print(f"  Anio hasta:        {filtros.anio_hasta or 'Sin limite'}")
    print(f"  Idioma:            {', '.join(filtros.idioma) if filtros.idioma else 'Todos'}")
    print(f"  Tipo de documento: {filtros.tipo_documento or 'Cualquiera'}")
    print(f"  Limite:            {filtros.limite} documentos")
    print("-" * 50)
    print()

    confirmacion = input("  Iniciar busqueda? (s/n): ").strip().lower()
    return confirmacion == "s"


def ejecutar_busqueda_y_descarga(scraper: BaseScraper, filtros: FiltrosBusqueda,
                                  carpeta_destino: str):
    os.makedirs(carpeta_destino, exist_ok=True)

    print()
    print("  Buscando documentos...")
    print("  (Esto puede tomar unos segundos dependiendo de la fuente)")
    print()

    inicio_busqueda = time.time()
    resultados = scraper.search(filtros)
    tiempo_busqueda = time.time() - inicio_busqueda

    if not resultados:
        print("  No se encontraron documentos con los filtros especificados.")
        print("  Sugerencias:")
        print("    - Verifica que las palabras clave sean correctas")
        print("    - Ampliar el rango de fechas")
        print("    - Probar sin filtro de idioma o tipo de documento")
        return

    print(f"  Se encontraron {len(resultados)} documentos en {tiempo_busqueda:.1f} segundos.")
    print()

    print("  Iniciando descarga de PDFs...")
    print()

    exitosos = 0
    fallidos = 0
    archivos_descargados = []  
    inicio_descarga = time.time()

    for i, doc in enumerate(resultados, 1):
        print(f"  Descargando documento {i} de {len(resultados)}: {doc.titulo[:60]}...")

        ruta = scraper.download(doc, carpeta_destino)

        if ruta:
            exitosos += 1
            archivos_descargados.append({
                "titulo": doc.titulo,
                "autor": doc.autor,
                "anio": doc.anio,
                "idioma": doc.idioma,
                "tipo_documento": doc.tipo_documento,
                "url_fuente": doc.url_fuente,
                "archivo_local": os.path.basename(ruta),
            })
        else:
            fallidos += 1
            archivos_descargados.append({
                "titulo": doc.titulo,
                "autor": doc.autor,
                "anio": doc.anio,
                "idioma": doc.idioma,
                "tipo_documento": doc.tipo_documento,
                "url_fuente": doc.url_fuente,
                "archivo_local": "DESCARGA_FALLIDA",
            })
        if i < len(resultados):
            time.sleep(1)

    tiempo_descarga = time.time() - inicio_descarga

    ruta_csv = os.path.join(carpeta_destino, "metadata.csv")
    generar_csv_metadatos(archivos_descargados, ruta_csv)

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
    print(f"  Metadatos exportados en:         {os.path.abspath(ruta_csv)}")
    print("=" * 50)

    if fallidos > 0:
        print()
        print(f"  Se encontraron {fallidos} errores durante la descarga.")
        print("  Para obtener ayuda, comparte el archivo errores.log")
        print(f"  Ubicacion: {os.path.abspath('errores.log')}")


def generar_csv_metadatos(datos: List[dict], ruta_csv: str):
    if not datos:
        return

    campos = ["titulo", "autor", "anio", "idioma", "tipo_documento", "url_fuente", "archivo_local"]

    try:
        with open(ruta_csv, "w", newline="", encoding="utf-8-sig") as f:
            escritor = csv.DictWriter(f, fieldnames=campos)
            escritor.writeheader()
            for fila in datos:
                escritor.writerow(fila)
        logger.info(f"CSV de metadatos generado: {ruta_csv}")
    except Exception as e:
        logger.error(f"Error al generar CSV de metadatos: {e}", exc_info=True)
        print(f"  Error al generar el archivo de metadatos: {e}")



def diagnostico():
    print()
    print("-" * 50)
    print("  DIAGNOSTICO DE DEPENDENCIAS")
    print("-" * 50)
    print()

    todo_ok = True

    version_python = sys.version.split()[0]
    print(f"  [OK] Python {version_python}")

    try:
        import requests
        print(f"  [OK] requests {requests.__version__}")
    except ImportError:
        print("  [ERROR] requests no esta instalado")
        print("          Ejecuta: pip install requests")
        todo_ok = False

    try:
        from lxml import etree
        print(f"  [OK] lxml {etree.__version__}")
    except ImportError:
        print("  [ERROR] lxml no esta instalado")
        print("          Ejecuta: pip install lxml")
        todo_ok = False

    try:
        import playwright
        try:
            from importlib.metadata import version as pkg_version
            version_pw = pkg_version("playwright")
        except Exception:
            version_pw = "(version desconocida)"
        print(f"  [OK] playwright {version_pw}")

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

    print()
    if todo_ok:
        print("  Todo esta correctamente configurado.")
        print("  Puedes usar todas las fuentes de datos.")
    else:
        print("  Se encontraron problemas. Revisa los errores indicados arriba")
        print("  e instala las dependencias faltantes antes de continuar.")

    print()
    input("  Presiona Enter para volver al menu principal...")


def main():
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