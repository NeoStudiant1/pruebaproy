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
    try:
        with open(RUTA_CONFIGURACION, "w", encoding="utf-8") as f:
            json.dump(config, f, indent=4, ensure_ascii=False)
    except Exception as e:
        logger.warning(f"No se pudo guardar configuracion.json: {e}")


CONFIG = cargar_configuracion()

CARPETA_DESCARGA = CONFIG.get("carpeta_descarga_por_defecto",
                               "./documentos_descargados")


RUTA_HISTORIAL = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "historial_descargas.json"
)

ESTADO_EXITOSO = "exitoso"
ESTADO_FALLIDO = "fallido"


def cargar_historial() -> dict:
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
    try:
        historial["actualizado"] = datetime.now().replace(microsecond=0).isoformat()
        with open(RUTA_HISTORIAL, "w", encoding="utf-8") as f:
            json.dump(historial, f, indent=2, ensure_ascii=False)
        logger.debug(f"Historial guardado: {RUTA_HISTORIAL} "
                     f"({len(historial.get('descargas', {}))} registros)")
    except Exception as e:
        logger.warning(f"No se pudo guardar historial_descargas.json: {e}")


def ids_excluir_desde_historial(historial: dict) -> set:
    descargas = historial.get("descargas", {})
    if not isinstance(descargas, dict):
        return set()
    return set(descargas.keys())


def registrar_en_historial(historial: dict, id_unico: str, registro: dict):
    if "descargas" not in historial:
        historial["descargas"] = {}
    historial["descargas"][id_unico] = registro


def construir_id_unico(fuente: str, recid: str) -> Optional[str]:
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

    idiomas_cfg = CONFIG.get("idiomas_validos", {})
    print()
    print("  Idiomas disponibles (puedes elegir varios separados por coma):")
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

    print()
    print("  Tipos de documento:")
    print("    reporte | resolucion | acuerdo | decision | carta")
    tipo = input("  Tipo de documento (o deja en blanco para cualquiera): ").strip().lower()
    if tipo:
        filtros.tipo_documento = tipo

    limite_default = CONFIG.get("limite_documentos_por_defecto", 50)
    print()
    limite_str = input(f"  Numero maximo de documentos a descargar (default: {limite_default}): ").strip()
    if limite_str:
        try:
            limite = int(limite_str)
            if limite <= 0:
                print(f"  Valor no valido. Se usara el limite por defecto ({limite_default}).")
                limite = limite_default

            if limite > 100:
                print()
                print(f"  ATENCION: Descargar {limite} documentos puede tardar varios minutos")
                print("  y generar un volumen considerable de trafico de red.")
                confirmacion = input("  Deseas continuar con este limite? (s/n): ").strip().lower()
                if confirmacion != "s":
                    print("  Se usara el limite de 100 documentos.")
                    limite = 100

            filtros.limite = limite
        except ValueError:
            print(f"  Valor no valido. Se usara el limite por defecto ({limite_default}).")
            filtros.limite = limite_default
    else:
        filtros.limite = limite_default

    carpeta_default = CONFIG.get("ultima_carpeta_usada") or CARPETA_DESCARGA
    print()
    print(f"  Carpeta de descarga actual: {carpeta_default}")
    cambiar = input("  Deseas cambiar la carpeta de descarga? (s/n): ").strip().lower()

    if cambiar == "s":
        carpeta = _seleccionar_carpeta_grafica(carpeta_default)
        if carpeta:
            CONFIG["ultima_carpeta_usada"] = carpeta
            guardar_configuracion(CONFIG)
        else:
            carpeta = carpeta_default
    else:
        carpeta = carpeta_default

    return filtros, carpeta


def _seleccionar_carpeta_grafica(carpeta_inicial: str) -> Optional[str]:
    try:
        import tkinter as tk
        from tkinter import filedialog

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
    os.makedirs(carpeta_destino, exist_ok=True)

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

        if i < len(resultados):
            time.sleep(1)

    tiempo_descarga = time.time() - inicio_descarga

    ruta_csv = os.path.join(carpeta_destino, "metadata.csv")
    ruta_json = os.path.join(carpeta_destino, "metadata.json")
    ruta_textos = os.path.join(carpeta_destino, "textos_extraidos.txt")
    generar_csv_metadatos(archivos_descargados, ruta_csv)
    generar_json_metadatos(archivos_descargados, ruta_json)
    generar_archivo_textos_consolidado(archivos_descargados, ruta_textos)

    guardar_historial(historial)

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
    if not datos:
        return

    campos = ["titulo", "autor", "fecha", "idioma", "tipo_documento",
              "url_fuente", "archivo_local", "fecha_descarga", "texto_extraido"]

    datos_para_csv = []
    for registro in datos:
        copia = dict(registro)
        copia["texto_extraido"] = truncar_texto_para_csv(
            registro.get("texto_extraido", "")
        )
        datos_para_csv.append(copia)

    try:
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
    if not datos:
        return

    try:
        with open(ruta_json, "w", encoding="utf-8") as f:
            json.dump(datos, f, indent=2, ensure_ascii=False)
        logger.info(f"JSON de metadatos generado: {ruta_json}")
    except Exception as e:
        logger.error(f"Error al generar JSON de metadatos: {e}", exc_info=True)
        print(f"  Error al generar el archivo de metadatos JSON: {e}")


MAX_CHARS_TEXTO_EN_CSV = 500

TEXTO_OCR_REQUERIDO = "[PDF SIN CAPA DE TEXTO - OCR REQUERIDO]"
TEXTO_VACIO = "[PDF VACIO O SIN CONTENIDO TEXTUAL]"
TEXTO_ERROR_LECTURA = "[ERROR AL LEER EL PDF]"
TEXTO_NO_DESCARGADO = "[ARCHIVO NO DESCARGADO]"


def extraer_texto_pdf(ruta_pdf: str) -> str:
    try:
        from pypdf import PdfReader
    except ImportError:
        logger.warning("pypdf no esta instalado. No se extraera texto.")
        return TEXTO_ERROR_LECTURA

    if not os.path.isfile(ruta_pdf):
        return TEXTO_NO_DESCARGADO

    try:
        reader = PdfReader(ruta_pdf)
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
                logger.debug(f"Pagina ilegible en {ruta_pdf}: {e}")
                continue

        texto_total = "\n".join(partes_texto).strip()

        if not texto_total:
            return TEXTO_OCR_REQUERIDO

        return texto_total

    except Exception as e:
        logger.warning(f"Error extrayendo texto de {ruta_pdf}: {e}")
        return TEXTO_ERROR_LECTURA


def truncar_texto_para_csv(texto: str, limite: int = MAX_CHARS_TEXTO_EN_CSV) -> str:
    if not texto:
        return ""
    if texto.startswith("[") and texto.endswith("]"):
        return texto
    if len(texto) <= limite:
        return texto
    return texto[:limite].rstrip() + f"... [TRUNCADO - texto completo en metadata.json, {len(texto)} caracteres totales]"


def generar_archivo_textos_consolidado(datos: List[dict], ruta_archivo: str):
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
        import pandas as pd
        print(f"  [OK] pandas {pd.__version__}")
    except ImportError:
        print("  [AVISO] pandas no esta instalado")
        print("          Sin pandas el CSV se genera con un fallback de la stdlib.")
        print("          Para mejor manejo de datos: pip install pandas")

    try:
        import pypdf
        print(f"  [OK] pypdf {pypdf.__version__}")
    except ImportError:
        print("  [AVISO] pypdf no esta instalado")
        print("          Sin pypdf no se podra extraer texto de los PDFs.")
        print("          Ejecuta: pip install pypdf")

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