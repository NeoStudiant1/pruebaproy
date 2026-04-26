import sys
import os

# Verificar dependencias antes de continuar
DEPENDENCIAS = {
    "requests": "requests",
    "bs4": "beautifulsoup4",
    "lxml": "lxml",
    "tqdm": "tqdm",
    "colorama": "colorama",
}

faltantes = []
for modulo, paquete in DEPENDENCIAS.items():
    try:
        __import__(modulo)
    except ImportError:
        faltantes.append(paquete)

if faltantes:
    print("\n❌ Faltan dependencias. Instálalas con:\n")
    print(f"   pip install {' '.join(faltantes)}\n")
    print("Para ILO Labordoc (sitio con JavaScript) también necesitas:")
    print("   pip install playwright")
    print("   playwright install chromium\n")
    sys.exit(1)

from scraper_un import ScraperUN
from scraper_ilo import ScraperILO
from utils import (
    imprimir_banner,
    imprimir_menu,
    solicitar_palabras_clave,
    solicitar_cantidad,
    solicitar_fuente,
    crear_archivo_comprimido,
    imprimir_resumen,
    limpiar_directorio_temporal,
)


def main():
    imprimir_banner()

    # Solicitar inputs al usuario
    palabras_clave = solicitar_palabras_clave()
    cantidad_max   = solicitar_cantidad()
    fuente         = solicitar_fuente()

    print()

    todos_los_resultados = []

    # Ejecutar scraping según la fuente elegida
    if fuente in ("1", "3"):
        print("\nIniciando búsqueda en UN Digital Library...")
        scraper_un = ScraperUN(palabras_clave, cantidad_max)
        resultados_un = scraper_un.ejecutar()
        todos_los_resultados.extend(resultados_un)

    if fuente in ("2", "3"):
        print("\nIniciando búsqueda en Labordoc ILO...")
        scraper_ilo = ScraperILO(palabras_clave, cantidad_max)
        resultados_ilo = scraper_ilo.ejecutar()
        todos_los_resultados.extend(resultados_ilo)

    # Comprimir y organizar
    if todos_los_resultados:
        archivo_zip = crear_archivo_comprimido(todos_los_resultados, palabras_clave)
        imprimir_resumen(todos_los_resultados, archivo_zip)
    else:
        print("\n No se encontraron documentos con los criterios ingresados.")

    # Limpiar archivos temporales
    limpiar_directorio_temporal()


if __name__ == "__main__":
    main()
