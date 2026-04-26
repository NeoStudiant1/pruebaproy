import os
import re
import zipfile
import shutil
from datetime import datetime
from pathlib import Path

from colorama import init, Fore, Style
from tqdm import tqdm

init(autoreset=True)


DIR_TEMP = Path("_temp_documentos")

def imprimir_banner():
    banner = f"""
{Fore.CYAN}{Style.BRIGHT}
╔══════════════════════════════════════════════════════════════════════╗
║          SCRAPER DE DOCUMENTOS — UN Library & Labordoc ILO          ║
║                       Descarga inteligente de PDFs                  ║
╚══════════════════════════════════════════════════════════════════════╝
{Style.RESET_ALL}
  Fuentes disponibles:
    UN Digital Library  → digitallibrary.un.org
    Labordoc ILO        → labordoc.ilo.org
"""
    print(banner)


def imprimir_menu():
    print(f"""
{Fore.YELLOW}  ┌─────────────────────────────────────┐
  │  ¿De dónde descargar documentos?    │
  │                                     │
  │   1 → UN Digital Library           │
  │   2 → Labordoc ILO                 │
  │   3 → Ambas fuentes                │
  └─────────────────────────────────────┘{Style.RESET_ALL}""")


def solicitar_palabras_clave() -> list[str]:
    """Solicita las palabras clave y retorna una lista de términos."""
    print(f"{Fore.WHITE}{'─'*60}")
    print(f"{Fore.CYAN}📝 PALABRAS CLAVE")
    print(f"{Fore.WHITE}   Ingresa los temas a buscar separados por coma.")
    print(f"   Ejemplo: {Fore.YELLOW}climate change, sustainable development, human rights")
    print(f"{Fore.WHITE}{'─'*60}")

    while True:
        entrada = input(f"\n{Fore.GREEN}➤ Palabras clave: {Style.RESET_ALL}").strip()
        if entrada:
            palabras = [p.strip() for p in entrada.split(",") if p.strip()]
            if palabras:
                print(f"\n{Fore.CYAN}  ✔ Se buscarán {len(palabras)} tema(s):")
                for i, p in enumerate(palabras, 1):
                    print(f"    {i}. {Fore.YELLOW}{p}")
                return palabras
        print(f"{Fore.RED}  ⚠ Debes ingresar al menos una palabra clave.")


def solicitar_cantidad() -> int:
    """Solicita la cantidad máxima de documentos por tema."""
    print(f"\n{Fore.WHITE}{'─'*60}")
    print(f"{Fore.CYAN}📦 CANTIDAD DE DOCUMENTOS")
    print(f"{Fore.WHITE}   Máximo de documentos a descargar por tema.")
    print(f"   Recomendado: 10–20 para pruebas. Puede ser mayor para producción.")
    print(f"{Fore.WHITE}{'─'*60}")

    while True:
        try:
            entrada = input(f"\n{Fore.GREEN}➤ Cantidad máxima por tema [20]: {Style.RESET_ALL}").strip()
            if not entrada:
                return 20
            cantidad = int(entrada)
            if cantidad < 1:
                print(f"{Fore.RED}  ⚠ Debe ser al menos 1.")
            else:
                print(f"\n{Fore.CYAN}  ✔ Se descargarán hasta {Fore.YELLOW}{cantidad}{Fore.CYAN} documentos por tema.")
                return cantidad
        except ValueError:
            print(f"{Fore.RED}  ⚠ Ingresa un número entero válido.")


def solicitar_fuente() -> str:
    """Solicita la fuente de búsqueda."""
    imprimir_menu()
    while True:
        entrada = input(f"\n{Fore.GREEN}➤ Selecciona fuente [1/2/3]: {Style.RESET_ALL}").strip()
        if entrada in ("1", "2", "3"):
            fuentes = {"1": "UN Digital Library", "2": "Labordoc ILO", "3": "Ambas fuentes"}
            print(f"\n{Fore.CYAN}  ✔ Fuente seleccionada: {Fore.YELLOW}{fuentes[entrada]}")
            return entrada
        print(f"{Fore.RED}  ⚠ Opción inválida. Ingresa 1, 2 o 3.")


def sanitizar_nombre(nombre: str, max_len: int = 60) -> str:
    """Convierte un texto en nombre de archivo/carpeta válido."""
    nombre = re.sub(r'[\\/*?:"<>|]', "", nombre)
    nombre = re.sub(r'\s+', "_", nombre.strip())
    return nombre[:max_len]


def obtener_dir_temporal() -> Path:
    DIR_TEMP.mkdir(exist_ok=True)
    return DIR_TEMP


def crear_archivo_comprimido(resultados: list[dict], palabras_clave: list[str]) -> str:
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    nombre_zip = f"documentos_{timestamp}.zip"

    print(f"\n{Fore.CYAN}📦 Creando archivo comprimido: {Fore.YELLOW}{nombre_zip}")

    with zipfile.ZipFile(nombre_zip, "w", zipfile.ZIP_DEFLATED) as zf:
        for resultado in tqdm(resultados, desc="  Comprimiendo", unit="archivo",
                              bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt}"):
            if not resultado.get("archivo_local"):
                continue

            ruta_archivo = Path(resultado["archivo_local"])
            if not ruta_archivo.exists():
                continue

            tema_dir   = sanitizar_nombre(resultado.get("tema", "sin_tema"))
            fuente_dir = sanitizar_nombre(resultado.get("fuente", "desconocida"))
            nombre_pdf = ruta_archivo.name

            ruta_en_zip = f"{tema_dir}/{fuente_dir}/{nombre_pdf}"
            zf.write(ruta_archivo, ruta_en_zip)

        indice_csv = _generar_indice_csv(resultados)
        zf.writestr("_indice_documentos.csv", indice_csv)

    print(f"  {Fore.GREEN}✔ Archivo creado: {Fore.YELLOW}{nombre_zip}")
    return nombre_zip


def _generar_indice_csv(resultados: list[dict]) -> str:
    lineas = ["TEMA,FUENTE,TÍTULO,AUTOR,AÑO,IDIOMA,URL_ORIGINAL,ARCHIVO"]
    for r in resultados:
        if not r.get("archivo_local"):
            continue
        fila = [
            r.get("tema", ""),
            r.get("fuente", ""),
            r.get("titulo", "").replace(",", ";"),
            r.get("autor", "").replace(",", ";"),
            r.get("anio", ""),
            r.get("idioma", ""),
            r.get("url_pdf", ""),
            Path(r.get("archivo_local", "")).name,
        ]
        lineas.append(",".join(str(c) for c in fila))
    return "\n".join(lineas)


def imprimir_resumen(resultados: list[dict], archivo_zip: str):
    total     = len(resultados)
    exitosos  = sum(1 for r in resultados if r.get("archivo_local"))
    fallidos  = total - exitosos

    temas = {}
    for r in resultados:
        if r.get("archivo_local"):
            tema = r.get("tema", "desconocido")
            temas[tema] = temas.get(tema, 0) + 1

    print(f"""
{Fore.CYAN}{Style.BRIGHT}
╔══════════════════════════════════════════════════════╗
║                   RESUMEN FINAL                      ║
╚══════════════════════════════════════════════════════╝{Style.RESET_ALL}
  Documentos encontrados : {Fore.WHITE}{total}
  {Fore.GREEN}✔  Descargados con éxito  : {exitosos}
  {Fore.RED}✘  Fallidos / sin PDF     : {fallidos}{Style.RESET_ALL}

  Documentos por tema:""")

    for tema, cantidad in temas.items():
        print(f"     • {Fore.YELLOW}{tema:<40}{Style.RESET_ALL} → {cantidad} archivo(s)")

    print(f"""
  📦 Archivo generado: {Fore.YELLOW}{archivo_zip}{Style.RESET_ALL}
""")


def limpiar_directorio_temporal():
    if DIR_TEMP.exists():
        shutil.rmtree(DIR_TEMP)
        print(f"{Fore.WHITE}  🧹 Archivos temporales eliminados.{Style.RESET_ALL}")
